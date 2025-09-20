use crate::google::datastore::import_export::datastore_v3::{
    EntityProto, PropertyValue, Reference, property_value::ReferenceValue,
};
use crate::google::datastore::v1::key::PathElement;
use tracing;

use crate::google::datastore::import_export::dsbackups::ExportMetadata;
use crate::google::datastore::import_export::dsbackups::OverallExportMetadata;
use crate::google::datastore::v1::filter::FilterType;
use crate::google::datastore::v1::key::path_element::IdType;
use crate::google::datastore::v1::query_result_batch::MoreResultsType;
use crate::google::datastore::v1::value::ValueType;
use crate::google::datastore::v1::{
    ArrayValue, Filter, LatLng, PartitionId, Projection, PropertyOrder, Value, property_order,
};
use chrono::DateTime;
use prost::Message;
use rayon::iter::ParallelIterator;
use rayon::prelude::*;
use std::cmp::Ordering;

use crate::google::datastore::v1::{Entity, EntityResult, Key, Mutation};
use crate::leveldb::LogReader; // Added to resolve error E0433
use bincode;
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error, ser::SerializeStruct};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::io::{Read, Write}; // For file I/O
use std::time::SystemTime; // Importing LogReader to read log files

const GRPC_MAX_MESSAGE_SIZE_BYTES: usize = 4 * 1024 * 1024; // 4 MiB
const MAX_ENTITIES_PAYLOAD_BYTES: usize = (GRPC_MAX_MESSAGE_SIZE_BYTES as f64 * 0.8) as usize; // 80% of gRPC max message size
const METADATA_KINDS: [&str; 3] = ["__kind__", "__namespace__", "__property__"];

fn get_representation_for_value(value: &Value) -> Vec<&'static str> {
    let mut representations = Vec::new();
    if let Some(value_type) = &value.value_type {
        match value_type {
            ValueType::NullValue(_) => representations.push("NULL"),
            ValueType::BooleanValue(_) => representations.push("BOOLEAN"),
            ValueType::IntegerValue(_) => representations.push("INT64"),
            ValueType::DoubleValue(_) => representations.push("DOUBLE"),
            ValueType::TimestampValue(_) => representations.push("INT64"),
            ValueType::KeyValue(_) => representations.push("REFERENCE"),
            ValueType::StringValue(_) => representations.push("STRING"),
            ValueType::BlobValue(_) => representations.push("STRING"),
            ValueType::GeoPointValue(_) => representations.push("POINT"),
            ValueType::EntityValue(_) => representations.push("STRING"),
            ValueType::ArrayValue(array_value) => {
                for v in &array_value.values {
                    representations.extend(get_representation_for_value(v));
                }
            }
        }
    }
    representations
}

fn compare_values(a: &Value, b: &Value) -> Ordering {
    match (&a.value_type, &b.value_type) {
        (Some(ValueType::NullValue(_)), Some(ValueType::NullValue(_))) => Ordering::Equal,
        (Some(ValueType::IntegerValue(av)), Some(ValueType::IntegerValue(bv))) => av.cmp(bv),
        (Some(ValueType::DoubleValue(av)), Some(ValueType::DoubleValue(bv))) => {
            av.partial_cmp(bv).unwrap_or(Ordering::Equal)
        }
        (Some(ValueType::StringValue(av)), Some(ValueType::StringValue(bv))) => av.cmp(bv),
        (Some(ValueType::BooleanValue(av)), Some(ValueType::BooleanValue(bv))) => av.cmp(bv),
        (Some(ValueType::TimestampValue(av)), Some(ValueType::TimestampValue(bv))) => av
            .seconds
            .cmp(&bv.seconds)
            .then_with(|| av.nanos.cmp(&bv.nanos)),
        (Some(ValueType::KeyValue(av)), Some(ValueType::KeyValue(bv))) => {
            KeyStruct::from_datastore_key(av).cmp(&KeyStruct::from_datastore_key(bv))
        }
        (Some(ValueType::GeoPointValue(av)), Some(ValueType::GeoPointValue(bv))) => av
            .latitude
            .partial_cmp(&bv.latitude)
            .unwrap_or(Ordering::Equal)
            .then_with(|| {
                av.longitude
                    .partial_cmp(&bv.longitude)
                    .unwrap_or(Ordering::Equal)
            }),
        // Add other types if necessary, and handle type mismatches
        // For now, unequal types are considered equal for sorting purposes, which is a simplification.
        _ => Ordering::Equal,
    }
}

fn get_indexable_strings_for_value(value: &Value) -> Vec<String> {
    let mut values = Vec::new();
    if let Some(value_type) = &value.value_type {
        let mut process_value = |v_type: &ValueType| match v_type {
            ValueType::StringValue(s) => values.push(s.clone()),
            ValueType::IntegerValue(i) => values.push(i.to_string()),
            ValueType::DoubleValue(d) => values.push(d.to_string()),
            ValueType::BooleanValue(b) => values.push(b.to_string()),
            ValueType::TimestampValue(t) => {
                if let Some(dt) = DateTime::from_timestamp(t.seconds, t.nanos as u32) {
                    values.push(dt.to_rfc3339())
                }
            }
            ValueType::KeyValue(k) => {
                values.push(format!("{:?}", KeyStruct::from_datastore_key(k)))
            }
            _ => {} // Other types not indexed
        };

        match value_type {
            ValueType::ArrayValue(array_value) => {
                for v in &array_value.values {
                    if let Some(inner_value_type) = &v.value_type {
                        process_value(inner_value_type);
                    }
                }
            }
            other => process_value(other),
        }
    }
    values
}

fn convert_property_value(prop_val: &PropertyValue) -> Option<ValueType> {
    if let Some(v) = prop_val.int64_value {
        return Some(ValueType::IntegerValue(v));
    }
    if let Some(v) = prop_val.boolean_value {
        return Some(ValueType::BooleanValue(v));
    }
    if let Some(v) = &prop_val.string_value {
        return Some(ValueType::StringValue(v.clone()));
    }
    if let Some(v) = prop_val.double_value {
        return Some(ValueType::DoubleValue(v));
    }
    if let Some(v) = &prop_val.pointvalue {
        return Some(ValueType::GeoPointValue(LatLng {
            latitude: v.x,
            longitude: v.y,
        }));
    }
    if let Some(v) = &prop_val.referencevalue {
        let key = convert_reference_value_to_key(v);
        return Some(ValueType::KeyValue(key));
    }
    // Note: list_value and user_value are not directly handled here as they are
    // part of the Property structure itself or deprecated.
    None
}

fn convert_reference_value_to_key(reference_value: &ReferenceValue) -> Key {
    let path = reference_value
        .pathelement
        .iter()
        .map(|el| {
            let id_type = if let Some(id) = el.id {
                Some(IdType::Id(id))
            } else {
                el.name.as_ref().map(|n| IdType::Name(n.clone()))
            };
            crate::google::datastore::v1::key::PathElement {
                kind: el.r#type.clone(),
                id_type,
            }
        })
        .collect();

    Key {
        partition_id: Some(PartitionId {
            project_id: reference_value.app.clone(),
            namespace_id: reference_value.name_space.clone().unwrap_or_default(),
            database_id: "".to_string(),
        }),
        path,
    }
}

fn convert_reference_to_key(reference: &Reference) -> Key {
    let path = reference
        .path
        .element
        .iter()
        .map(|el| {
            let id_type = if let Some(id) = el.id {
                Some(IdType::Id(id))
            } else {
                el.name.as_ref().map(|n| IdType::Name(n.clone()))
            };
            crate::google::datastore::v1::key::PathElement {
                kind: el.r#type.clone(),
                id_type,
            }
        })
        .collect();

    Key {
        partition_id: Some(PartitionId {
            project_id: reference.app.clone(),
            namespace_id: reference.name_space.clone().unwrap_or_default(),
            database_id: "".to_string(),
        }),
        path,
    }
}

pub fn converter_dump(dump_entities: Vec<EntityProto>) -> Vec<EntityWithMetadata> {
    dump_entities
        .into_par_iter()
        .map(|entity_proto| {
            let v1_key = Some(convert_reference_to_key(&entity_proto.key));

            // Group properties by name to handle multi-valued properties
            let mut grouped_props: HashMap<String, (Vec<Value>, bool)> = HashMap::new();

            for prop in &entity_proto.property {
                let prop_val = &prop.value;
                if let Some(v1_value_type) = convert_property_value(prop_val) {
                    let entry = grouped_props
                        .entry(prop.name.clone())
                        .or_insert((vec![], false));
                    entry.0.push(Value {
                        value_type: Some(v1_value_type),
                        ..Default::default()
                    });
                }
            }

            for prop in &entity_proto.raw_property {
                let prop_val = &prop.value;
                if let Some(v1_value_type) = convert_property_value(prop_val) {
                    let entry = grouped_props
                        .entry(prop.name.clone())
                        .or_insert((vec![], true));
                    entry.0.push(Value {
                        value_type: Some(v1_value_type),
                        ..Default::default()
                    });
                    entry.1 = true; // Mark as exclude_from_indexes
                }
            }

            let mut properties: HashMap<String, Value> = HashMap::new();
            for (name, (mut values, exclude)) in grouped_props {
                if values.len() > 1 {
                    // It's an array
                    properties.insert(
                        name,
                        Value {
                            value_type: Some(ValueType::ArrayValue(ArrayValue { values })),
                            exclude_from_indexes: exclude,
                            ..Default::default()
                        },
                    );
                } else if let Some(mut single_value) = values.pop() {
                    // It's a single value
                    single_value.exclude_from_indexes = exclude;
                    properties.insert(name, single_value);
                }
            }

            let v1_entity = Entity {
                key: v1_key,
                properties,
            };

            let timestamp_now = prost_types::Timestamp {
                seconds: SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs() as i64,
                nanos: SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos() as i32
                    % 1_000_000_000,
            };

            EntityWithMetadata {
                entity: v1_entity,
                version: 1,
                create_time: timestamp_now.clone(),
                update_time: timestamp_now,
            }
        })
        .collect()
}

pub fn read_overall_metadata(export_dir: &str) -> Option<OverallExportMetadata> {
    let mut metadata_file = None;
    let path_to_search = std::path::Path::new(export_dir).join("exports");
    for entry in std::fs::read_dir(path_to_search).expect("Failed to read export directory") {
        let entry = entry.expect("Failed to read directory entry");
        if entry
            .file_name()
            .to_string_lossy()
            .ends_with(".overall_export_metadata")
        {
            metadata_file = Some(entry.path());
            break;
        }
    }

    let _metadata_file = metadata_file.expect("Could not find .overall_export_metadata file.");
    tracing::debug!("Found metadata file: {}", _metadata_file.display());
    // let reader = LogReader::new(_metadata_file.clone())
    //     .expect("Failed to create LogReader for metadata file");
    // dbg!("LogReader created for metadata file:", reader);
    match LogReader::new(_metadata_file.clone()) {
        Ok(reader) => {
            for (i, record_result) in reader.enumerate() {
                match record_result {
                    Ok(record) => {
                        match OverallExportMetadata::decode(&record[..]) {
                            Ok(metadata) => {
                                // Now you have the decoded 'metadata'.
                                // You might want to process or store it.
                                return Some(metadata); // Returning the metadata
                            }
                            Err(decode_err) => {
                                tracing::error!(
                                    "Failed to decode OverallExportMetadata: {}",
                                    decode_err
                                );
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!("Error reading record {}: {}", i + 1, e); // Translated eprintln
                        break;
                    }
                }
            }
        }
        Err(e) => {
            tracing::error!("Error opening file: {}", e);
        }
    }
    None // Returning None as we are not returning the metadata here
}

pub fn read_dump(export_dir: &str) -> Vec<EntityProto> {
    tracing::info!("Reading datastore export from: {}", export_dir);
    let overall_metadata = read_overall_metadata(export_dir);
    if let Some(metadata) = overall_metadata {
        let entity_protos: Vec<EntityProto> = metadata
            .exports
            .into_par_iter()
            .flat_map(|export_entry| {
                let kind_name = export_entry
                    .kind
                    .as_ref()
                    .map_or_else(String::new, |k| k.kind.clone());
                tracing::info!("Processing kind: {}", kind_name);
                let metadata_path = std::path::PathBuf::from(export_dir)
                    .join("exports")
                    .join(&export_entry.path);

                if !metadata_path.exists() {
                    tracing::error!(
                        "Metadata file for kind {} not found at: {}",
                        kind_name,
                        metadata_path.display()
                    );
                    return Vec::new().into_par_iter();
                }

                let export_metadata = match std::fs::read(&metadata_path)
                    .map_err(|e| {
                        tracing::error!(
                            "Failed to read metadata file {}: {}",
                            metadata_path.display(),
                            e
                        );
                        e
                    })
                    .and_then(|data| {
                        ExportMetadata::decode(&data[..]).map_err(|e| {
                            tracing::error!(
                                "Failed to decode ExportMetadata for {}: {}",
                                kind_name,
                                e
                            );
                            std::io::Error::new(std::io::ErrorKind::InvalidData, e)
                        })
                    }) {
                    Ok(meta) => meta,
                    Err(_) => return Vec::new().into_par_iter(),
                };

                let output_file_names = export_metadata.items.unwrap_or_default().outputs;
                let parent_dir_for_output_files = metadata_path
                    .parent()
                    .expect("Metadata path should have a parent directory")
                    .to_path_buf();

                let protos_for_this_export: Vec<EntityProto> = output_file_names
                    .into_par_iter()
                    .flat_map(move |output_file_name_str| {
                        let output_file_path =
                            parent_dir_for_output_files.join(output_file_name_str);
                        let mut protos_in_file = Vec::new();
                        if output_file_path.exists() {
                            if let Ok(reader) = LogReader::new(output_file_path.clone()) {
                                for (i, record_result) in reader.enumerate() {
                                    match record_result {
                                        Ok(record) => {
                                            if let Ok(entity_proto) =
                                                EntityProto::decode(&record[..])
                                            {
                                                protos_in_file.push(entity_proto);
                                            } else {
                                                tracing::error!(
                                                    "Failed to decode EntityProto from file {}",
                                                    output_file_path.display()
                                                );
                                            }
                                        }
                                        Err(e) => {
                                            tracing::error!(
                                                "Error reading record {} from file {}: {}",
                                                i + 1,
                                                output_file_path.display(),
                                                e
                                            );
                                            break;
                                        }
                                    }
                                }
                            } else {
                                tracing::error!(
                                    "Error opening file: {}",
                                    output_file_path.display()
                                );
                            }
                        } else {
                            tracing::error!(
                                "Output file {} does not exist.",
                                output_file_path.display()
                            );
                        }
                        protos_in_file
                    })
                    .collect();

                protos_for_this_export.into_par_iter()
            })
            .collect();

        tracing::info!("Finished reading datastore export.");
        entity_protos
    } else {
        tracing::warn!("No overall metadata found in the export directory.");
        Vec::new()
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub enum KeyId {
    IntId(i64),
    StringId(String),
}

// Custom key structure for efficient indexing
#[derive(serde::Serialize, serde::Deserialize, Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct KeyStruct {
    pub project_id: String,
    pub namespace: String,
    pub path_elements: Vec<(String, KeyId)>, // (kind, id/name)
}

impl KeyStruct {
    pub fn from_datastore_key(key: &Key) -> Self {
        let mut path_elements = Vec::new();
        for path_element in &key.path {
            let kind = path_element.kind.clone();
            let id_type = match &path_element.id_type {
                Some(IdType::Id(id)) => KeyId::IntId(*id),
                Some(IdType::Name(name)) => KeyId::StringId(name.clone()),
                None => continue,
            };
            path_elements.push((kind, id_type));
        }
        Self {
            project_id: key
                .partition_id
                .as_ref()
                .map_or_else(|| "".to_string(), |p| p.project_id.clone()),
            namespace: key
                .partition_id
                .as_ref()
                .map_or_else(|| "".to_string(), |p| p.namespace_id.clone()),
            path_elements,
        }
    }
}

#[derive(Default, Debug, Clone)]
pub struct EntityWithMetadata {
    pub entity: Entity,
    pub version: u64,
    pub create_time: prost_types::Timestamp,
    pub update_time: prost_types::Timestamp,
}

#[derive(Deserialize)]
struct SerializableEntityWithMetadata {
    entity_bytes: Vec<u8>,
    version: u64,
    create_time_bytes: Vec<u8>,
    update_time_bytes: Vec<u8>,
}

impl Serialize for EntityWithMetadata {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("EntityWithMetadata", 4)?;
        state.serialize_field("entity_bytes", &self.entity.encode_to_vec())?;
        state.serialize_field("version", &self.version)?;
        state.serialize_field("create_time_bytes", &self.create_time.encode_to_vec())?;
        state.serialize_field("update_time_bytes", &self.update_time.encode_to_vec())?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for EntityWithMetadata {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = SerializableEntityWithMetadata::deserialize(deserializer)?;
        let entity = Entity::decode(&s.entity_bytes[..]).map_err(Error::custom)?;
        let create_time =
            prost_types::Timestamp::decode(&s.create_time_bytes[..]).map_err(Error::custom)?;
        let update_time =
            prost_types::Timestamp::decode(&s.update_time_bytes[..]).map_err(Error::custom)?;

        Ok(EntityWithMetadata {
            entity,
            version: s.version,
            create_time,
            update_time,
        })
    }
}

// Transaction state
#[derive(Debug)]
pub struct TransactionState {
    pub mutations: Vec<Mutation>,
    pub snapshot: HashMap<KeyStruct, EntityWithMetadata>,
    pub timestamp: prost_types::Timestamp,
    pub read_only: bool,
}

#[derive(Default, Debug)]
pub struct DatastoreStorage {
    // Stores using BTreeMap for ordered storage, mapping a full KeyStruct to its EntityWithMetadata
    pub entities: BTreeMap<KeyStruct, EntityWithMetadata>,
    // Indexes for efficient queries
    pub indexes: HashMap<(String, String, String), BTreeSet<KeyStruct>>,
    // Active transactions
    pub transactions: HashMap<String, TransactionState>,
    // Per-(project, namespace, kind) counters for auto ID allocation
    pub entity_id_counters: HashMap<(String, String, String), i64>,
    // Counter used for transaction IDs
    pub transaction_counter: i64,
}

impl DatastoreStorage {
    fn index_lookup_for_filter(
        &self,
        project_id: &str,
        kind_name: &str,
        filter_type: &FilterType,
    ) -> Option<BTreeSet<KeyStruct>> {
        match filter_type {
            FilterType::PropertyFilter(property_filter) => {
                let property = property_filter.property.as_ref()?;
                let filter_value = property_filter.value.as_ref()?;

                if property.name == "__key__" {
                    match property_filter.op {
                        5 => {
                            if let Some(ValueType::KeyValue(key)) = &filter_value.value_type {
                                let key_struct = KeyStruct::from_datastore_key(key);
                                if key_struct.project_id == project_id
                                    && key_struct
                                        .path_elements
                                        .last()
                                        .is_some_and(|(k, _)| k == kind_name)
                                {
                                    let mut set = BTreeSet::new();
                                    set.insert(key_struct);
                                    return Some(set);
                                }
                                return Some(BTreeSet::new());
                            }
                            None
                        }
                        6 => {
                            if let Some(ValueType::ArrayValue(array)) = &filter_value.value_type {
                                let mut result = BTreeSet::new();
                                for value in &array.values {
                                    if let Some(ValueType::KeyValue(key)) = &value.value_type {
                                        let key_struct = KeyStruct::from_datastore_key(key);
                                        if key_struct.project_id == project_id
                                            && key_struct
                                                .path_elements
                                                .last()
                                                .is_some_and(|(k, _)| k == kind_name)
                                        {
                                            result.insert(key_struct);
                                        }
                                    }
                                }
                                return Some(result);
                            }
                            None
                        }
                        _ => None,
                    }
                } else {
                    let indexed_values = get_indexable_strings_for_value(filter_value);
                    if indexed_values.is_empty() {
                        return None;
                    }

                    let kind_key = kind_name.to_string();
                    let property_key = property.name.clone();

                    match property_filter.op {
                        5 => {
                            let mut result: Option<BTreeSet<KeyStruct>> = None;
                            for value_str in indexed_values.iter() {
                                if let Some(keys) = self.indexes.get(&(
                                    kind_key.clone(),
                                    property_key.clone(),
                                    value_str.clone(),
                                )) {
                                    let filtered: BTreeSet<KeyStruct> = keys
                                        .iter()
                                        .filter(|key_struct| key_struct.project_id == project_id)
                                        .cloned()
                                        .collect();
                                    result = Some(match result {
                                        Some(mut acc) => {
                                            acc.extend(filtered);
                                            acc
                                        }
                                        None => filtered,
                                    });
                                }
                            }
                            result.or(Some(BTreeSet::new()))
                        }
                        6 => {
                            let mut acc = BTreeSet::new();
                            for value_str in indexed_values.iter() {
                                if let Some(keys) = self.indexes.get(&(
                                    kind_key.clone(),
                                    property_key.clone(),
                                    value_str.clone(),
                                )) {
                                    acc.extend(
                                        keys.iter()
                                            .filter(|key_struct| {
                                                key_struct.project_id == project_id
                                            })
                                            .cloned(),
                                    );
                                }
                            }
                            Some(acc)
                        }
                        _ => None,
                    }
                }
            }
            FilterType::CompositeFilter(composite) => {
                let mut child_sets = Vec::new();
                for filter in &composite.filters {
                    if let Some(filter_type) = filter.filter_type.as_ref() {
                        if let Some(child) =
                            self.index_lookup_for_filter(project_id, kind_name, filter_type)
                        {
                            child_sets.push(child);
                        } else {
                            return None;
                        }
                    } else {
                        return None;
                    }
                }

                if child_sets.is_empty() {
                    return Some(BTreeSet::new());
                }

                match composite.op {
                    1 => {
                        // AND
                        child_sets.sort_by_key(|set| set.len());
                        let mut iter = child_sets.into_iter();
                        let mut acc = iter.next().unwrap();
                        for set in iter {
                            acc = acc
                                .intersection(&set)
                                .cloned()
                                .collect::<BTreeSet<KeyStruct>>();
                            if acc.is_empty() {
                                break;
                            }
                        }
                        Some(acc)
                    }
                    2 => {
                        // OR
                        let mut acc = BTreeSet::new();
                        for set in child_sets {
                            acc.extend(set);
                        }
                        Some(acc)
                    }
                    _ => None,
                }
            }
        }
    }

    fn counter_scope_from_key(key: &Key) -> Option<(String, String, String)> {
        let partition = key.partition_id.as_ref();
        let project = partition.map(|p| p.project_id.clone()).unwrap_or_default();
        let namespace = partition
            .map(|p| p.namespace_id.clone())
            .unwrap_or_default();
        let kind = key.path.last()?.kind.clone();
        Some((project, namespace, kind))
    }

    pub(crate) fn observe_key_id(&mut self, key: &Key) {
        if let Some((project, namespace, kind)) = Self::counter_scope_from_key(key)
            && let Some(path_element) = key.path.last()
            && let Some(IdType::Id(id)) = path_element.id_type
        {
            self.entity_id_counters
                .entry((project, namespace, kind))
                .and_modify(|counter| {
                    if *counter < id {
                        *counter = id;
                    }
                })
                .or_insert(id);
        }
    }

    pub fn next_auto_id(&mut self, key: &Key) -> Result<i64, tonic::Status> {
        let scope = Self::counter_scope_from_key(key).ok_or_else(|| {
            tonic::Status::invalid_argument(
                "The entity key has no path elements to determine the 'kind' for ID generation.",
            )
        })?;
        let counter = self.entity_id_counters.entry(scope).or_insert(0);
        *counter += 1;
        Ok(*counter)
    }

    fn rebuild_entity_id_counters(&mut self) {
        self.entity_id_counters.clear();
        let keys: Vec<Key> = self
            .entities
            .values()
            .filter_map(|metadata| metadata.entity.key.clone())
            .collect();
        for key in keys {
            self.observe_key_id(&key);
        }
    }

    // Add this new method to load data from disk
    pub fn load_from_disk(&mut self, path: &str) -> std::io::Result<()> {
        // Try to open the file. If it doesn't exist, just return Ok without doing anything.
        let start_time = SystemTime::now();
        let mut file = match std::fs::File::open(path) {
            Ok(file) => file,
            Err(ref e) if e.kind() == std::io::ErrorKind::NotFound => {
                tracing::info!(
                    "Data file '{}' not found. Starting with an empty store.",
                    path
                );
                return Ok(());
            }
            Err(e) => return Err(e),
        };

        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;

        if buffer.is_empty() {
            tracing::info!(
                "Data file '{}' is empty. Starting with an empty store.",
                path
            );
            return Ok(());
        }

        // Deserialize the buffer directly into the BTreeMap
        let (entities, _): (BTreeMap<KeyStruct, EntityWithMetadata>, _) =
            bincode::serde::decode_from_slice(&buffer, bincode::config::standard())
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

        tracing::info!("Loading {} entities from '{}'...", entities.len(), path);
        self.entities = entities;

        // Bulk rebuild indexes
        tracing::info!("Rebuilding indexes for {} entities...", self.entities.len());
        let mut new_indexes: HashMap<(String, String, String), BTreeSet<KeyStruct>> =
            HashMap::new();
        for (key_struct, metadata) in &self.entities {
            for (prop_name, prop_value) in &metadata.entity.properties {
                if !prop_value.exclude_from_indexes
                    && let Some((kind, _)) = key_struct.path_elements.last()
                {
                    for value_str in get_indexable_strings_for_value(prop_value) {
                        let index_key = (kind.clone(), prop_name.clone(), value_str);
                        new_indexes
                            .entry(index_key)
                            .or_default()
                            .insert(key_struct.clone());
                    }
                }
            }
        }
        self.indexes = new_indexes;
        self.rebuild_entity_id_counters();

        let end_time = SystemTime::now();
        let diff = end_time.duration_since(start_time).unwrap_or_default();
        tracing::info!("Load complete in {} seconds.", diff.as_secs_f64());
        Ok(())
    }

    // Add this new method to save data to disk
    pub fn save_to_disk(&self, path: &str) -> std::io::Result<()> {
        tracing::info!("Saving {} entities to '{}'...", self.entities.len(), path);

        // Serialize the whole BTreeMap
        let buffer = bincode::serde::encode_to_vec(&self.entities, bincode::config::standard())
            .map_err(std::io::Error::other)?;

        // Write the serialized buffer to the file
        let mut file = std::fs::File::create(path)?;
        file.write_all(&buffer)?;
        tracing::info!("Data saved successfully.");
        Ok(())
    }

    pub fn clean_transaction(&mut self, transaction_id: &str) {
        // clean up the transaction state
        self.transactions.remove(transaction_id);
    }

    pub fn import_dump(&mut self, path: &str) -> Result<(), tonic::Status> {
        let dump_entities = read_dump(path);
        let entities_with_metadata = converter_dump(dump_entities);
        tracing::info!(
            "Importing {} entities from dump at {}",
            entities_with_metadata.len(),
            path
        );
        for entity_metadata in entities_with_metadata {
            if let Some(_key) = &entity_metadata.entity.key {
                self.insert_entity(&entity_metadata.entity)?;
            } else {
                return Err(tonic::Status::invalid_argument(
                    "Entity missing key during import",
                ));
            }
        }
        Ok(())
    }

    pub fn insert_entity(
        &mut self,
        entity: &Entity,
    ) -> Result<(Key, EntityWithMetadata), tonic::Status> {
        let original_key = match entity.key {
            Some(ref key) => key.clone(),
            None => return Err(tonic::Status::invalid_argument("Entity missing key")),
        };

        let mut key_with_new_id = original_key.clone();

        let needs_auto_id = original_key
            .path
            .iter()
            .any(|path_element| path_element.id_type.is_none());
        let mut new_id_value = None;
        if needs_auto_id {
            new_id_value = Some(self.next_auto_id(&original_key)?);
        }

        // Apply the new ID to any PathElement in the key that is without an ID.
        for path_element in key_with_new_id.path.iter_mut() {
            if path_element.id_type.is_none()
                && let Some(id_value) = new_id_value
            {
                path_element.id_type = Some(IdType::Id(id_value));
            }
        }

        let final_key_struct = KeyStruct::from_datastore_key(&key_with_new_id);

        let mut db_entity = entity.clone();
        db_entity.key = Some(key_with_new_id.clone());

        let timestamp_now = prost_types::Timestamp {
            // Placeholder, consider real time
            seconds: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as i64,
            nanos: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as i32
                % 1_000_000_000,
        };

        let entity_metadata = EntityWithMetadata {
            entity: db_entity.clone(),
            version: 1, // Initial version
            create_time: timestamp_now.clone(),
            update_time: timestamp_now.clone(),
        };

        // Insert into the BTreeMap<KeyStruct, EntityWithMetadata>
        self.entities
            .insert(final_key_struct.clone(), entity_metadata.clone());
        self.update_indexes(&final_key_struct, &db_entity);
        if let Some(ref key) = db_entity.key {
            self.observe_key_id(key);
        }

        Ok((key_with_new_id, entity_metadata))
    }
    pub fn apply_filter(entity_metadata: &EntityWithMetadata, filter: &FilterType) -> bool {
        match filter {
            FilterType::PropertyFilter(property_filter) => {
                if let Some(ref property) = property_filter.property {
                    if let Some(ref filter_value) = property_filter.value {
                        // Renamed `value` to `filter_value`
                        // Special handling for HAS_ANCESTOR as it operates on keys, not properties
                        if property.name == "__key__" && property_filter.op == 11 {
                            // HAS_ANCESTOR = 11
                            if let Some(ValueType::KeyValue(ancestor_key_value)) =
                                &filter_value.value_type
                                && let Some(entity_key) = &entity_metadata.entity.key
                            {
                                let entity_partition_id_obj = entity_key.partition_id.as_ref();
                                let ancestor_partition_id_obj =
                                    ancestor_key_value.partition_id.as_ref();

                                let partitions_match =
                                    match (entity_partition_id_obj, ancestor_partition_id_obj) {
                                        (Some(ep), Some(ap)) => {
                                            ep.project_id == ap.project_id
                                                && ep.namespace_id == ap.namespace_id
                                        }
                                        (None, None) => true,
                                        _ => false,
                                    };
                                if !partitions_match {
                                    return false;
                                }

                                if entity_key.path.len() > ancestor_key_value.path.len() {
                                    for (i, ancestor_path_element) in
                                        ancestor_key_value.path.iter().enumerate()
                                    {
                                        let entity_path_element = &entity_key.path[i];
                                        if entity_path_element.kind != ancestor_path_element.kind
                                            || entity_path_element.id_type
                                                != ancestor_path_element.id_type
                                        {
                                            return false;
                                        }
                                    }
                                    return true;
                                }
                            }
                            false
                        }
                        // Regular property filters
                        else {
                            let entity_value_opt = if property.name == "__key__" {
                                entity_metadata.entity.key.as_ref().map(|k| Value {
                                    value_type: Some(ValueType::KeyValue(k.clone())),
                                    ..Default::default()
                                })
                            } else {
                                entity_metadata
                                    .entity
                                    .properties
                                    .get(&property.name)
                                    .cloned()
                            };

                            if let Some(entity_value) = entity_value_opt {
                                match property_filter.op {
                                    0 => true, // OPERATOR_UNSPECIFIED
                                    1 => {
                                        // LESS_THAN
                                        compare_values(&entity_value, filter_value).is_lt()
                                    }
                                    2 => {
                                        // LESS_THAN_OR_EQUAL
                                        compare_values(&entity_value, filter_value).is_le()
                                    }
                                    3 => {
                                        // GREATER_THAN
                                        compare_values(&entity_value, filter_value).is_gt()
                                    }
                                    4 => {
                                        // GREATER_THAN_OR_EQUAL
                                        compare_values(&entity_value, filter_value).is_ge()
                                    }
                                    5 => entity_value.value_type == filter_value.value_type, // EQUAL
                                    6 => {
                                        // IN
                                        if let Some(ValueType::ArrayValue(array_value)) =
                                            &filter_value.value_type
                                        {
                                            return array_value
                                                .values
                                                .iter()
                                                .any(|v| v.value_type == entity_value.value_type);
                                        }
                                        false
                                    }
                                    9 => entity_value.value_type != filter_value.value_type, // NOT_EQUAL
                                    // Case 11 (HAS_ANCESTOR) is handled above
                                    13 => {
                                        // NOT_IN
                                        if let Some(ValueType::ArrayValue(array_value)) =
                                            &filter_value.value_type
                                        {
                                            return !array_value
                                                .values
                                                .iter()
                                                .any(|v| v.value_type == entity_value.value_type);
                                        }
                                        true
                                    }
                                    _ => false, // Unsupported operator for property or op combination
                                }
                            } else {
                                // Property not found in entity for regular filters (and not HAS_ANCESTOR)
                                false
                            }
                        }
                    } else {
                        // Filter value is missing
                        false
                    }
                } else {
                    // Property reference is missing
                    false
                }
            }
            FilterType::CompositeFilter(composity_filter) => {
                let mut filter_results = Vec::new();
                for filter in composity_filter.filters.clone() {
                    // Apply filter recursively
                    if let Some(filter_type) = filter.filter_type {
                        filter_results.push(DatastoreStorage::apply_filter(
                            entity_metadata,
                            &filter_type,
                        ));
                    }
                }
                // Combine results based on the composite filter operator
                // AND = 1, OR = 2
                match composity_filter.op {
                    1 => {
                        // AND we can translate to ALL are true
                        filter_results.iter().all(|&result| result)
                    }
                    2 => {
                        // Or we can translate to ANY is true
                        filter_results.iter().any(|&result| result)
                    }
                    _ => {
                        // OPERATOR_UNSPECIFIED
                        true
                    }
                }
            }
        }
        // The code below was unreachable because all match arms returned explicitly.
        // If none of the match arms are hit (which shouldn't happen with a valid FilterType),
        // the default behavior would be not to filter, i.e., return true.
        // However, the current match logic covers all FilterType cases (PropertyFilter, CompositeFilter).
        // If FilterType is None, the filter is not applied in the previous call.
        // Therefore, removing the final `true` is safe, as the match should be exhaustive for valid FilterType.
    }

    pub fn get_entity(&self, key: &Key) -> Option<EntityWithMetadata> {
        let key_struct = KeyStruct::from_datastore_key(key);

        //Debug print items (optional, can be removed or adjusted)
        // for (k_struct, entity_meta) in self.entities.iter() {
        //     println!(
        //         "Stored KeyStruct: {:?}, Entity Path: {:?}",
        //         k_struct,
        //         entity_meta.entity.key.as_ref().map(|k| &k.path)
        //     );
        // }

        self.entities.get(&key_struct).cloned()
    }
    fn get_metadata(&self, metadata_key: &str, project_id: &str) -> Vec<EntityWithMetadata> {
        let mut results = Vec::new();

        match metadata_key {
            "__kind__" => {
                let mut unique_kinds = HashSet::new();
                for key_struct in self.entities.keys() {
                    // TODO: This should also filter by namespace from the query
                    if key_struct.project_id == project_id
                        && let Some((kind, _)) = key_struct.path_elements.last()
                    {
                        unique_kinds.insert((
                            kind.clone(),
                            key_struct.project_id.clone(),
                            key_struct.namespace.clone(),
                        ));
                    }
                }

                results = unique_kinds
                    .into_iter()
                    .map(|(kind, project_id, namespace)| EntityWithMetadata {
                        version: 1,
                        create_time: prost_types::Timestamp::default(),
                        update_time: prost_types::Timestamp::default(),
                        entity: Entity {
                            properties: HashMap::new(),
                            key: Some(Key {
                                partition_id: Some(PartitionId {
                                    project_id,
                                    namespace_id: namespace,
                                    database_id: "".to_string(),
                                }),
                                path: vec![PathElement {
                                    kind: "__kind__".to_string(),
                                    id_type: Some(IdType::Name(kind)),
                                }],
                            }),
                        },
                    })
                    .collect();
            }
            "__namespace__" => {
                let mut unique_namespaces = HashSet::new();
                for key_struct in self.entities.keys() {
                    if key_struct.project_id == project_id {
                        unique_namespaces.insert(key_struct.namespace.clone());
                    }
                }

                results = unique_namespaces
                    .into_iter()
                    .map(|namespace| {
                        let id_type = if namespace.is_empty() {
                            IdType::Id(1)
                        } else {
                            IdType::Name(namespace)
                        };
                        EntityWithMetadata {
                            version: 1,
                            create_time: prost_types::Timestamp::default(),
                            update_time: prost_types::Timestamp::default(),
                            entity: Entity {
                                properties: HashMap::new(),
                                key: Some(Key {
                                    partition_id: Some(PartitionId {
                                        project_id: project_id.to_string(),
                                        namespace_id: "".to_string(),
                                        database_id: "".to_string(),
                                    }),
                                    path: vec![PathElement {
                                        kind: "__namespace__".to_string(),
                                        id_type: Some(id_type),
                                    }],
                                }),
                            },
                        }
                    })
                    .collect();
            }
            "__property__" => {
                // map from (kind, property_name, namespace) to set of representations
                let mut props_by_kind: HashMap<(String, String, String), HashSet<&'static str>> =
                    HashMap::new();

                for (key_struct, entity_meta) in &self.entities {
                    if key_struct.project_id != project_id {
                        continue;
                    }
                    // TODO: This should be filtered by the query's namespace.
                    if let Some((kind, _)) = key_struct.path_elements.last() {
                        for (prop_name, prop_value) in &entity_meta.entity.properties {
                            if !prop_value.exclude_from_indexes {
                                let representations = get_representation_for_value(prop_value);
                                let entry = props_by_kind
                                    .entry((
                                        kind.clone(),
                                        prop_name.clone(),
                                        key_struct.namespace.clone(),
                                    ))
                                    .or_default();
                                entry.extend(representations);
                            }
                        }
                    }
                }

                results = props_by_kind
                    .into_iter()
                    .map(|((kind, prop_name, namespace), representations)| {
                        let property_key = Key {
                            partition_id: Some(PartitionId {
                                project_id: project_id.to_string(),
                                namespace_id: namespace,
                                database_id: "".to_string(),
                            }),
                            path: vec![
                                PathElement {
                                    kind: "__kind__".to_string(),
                                    id_type: Some(IdType::Name(kind)),
                                },
                                PathElement {
                                    kind: "__property__".to_string(),
                                    id_type: Some(IdType::Name(prop_name)),
                                },
                            ],
                        };

                        let mut properties = HashMap::new();
                        let array_values: Vec<Value> = representations
                            .into_iter()
                            .map(|rep| Value {
                                value_type: Some(ValueType::StringValue(rep.to_string())),
                                ..Default::default()
                            })
                            .collect();
                        properties.insert(
                            "property_representation".to_string(),
                            Value {
                                value_type: Some(ValueType::ArrayValue(ArrayValue {
                                    values: array_values,
                                })),
                                ..Default::default()
                            },
                        );

                        EntityWithMetadata {
                            version: 1,
                            create_time: prost_types::Timestamp::default(),
                            update_time: prost_types::Timestamp::default(),
                            entity: Entity {
                                key: Some(property_key),
                                properties,
                            },
                        }
                    })
                    .collect();
            }
            _ => {
                // For other metadata kinds, return an empty result set
                tracing::warn!("Unsupported metadata kind: {}", metadata_key);
            }
        }
        results
    }
    pub fn get_entities(
        &self,
        project_id_filter: String,
        kind_name: String,
        filter: Option<Filter>,
        limit: Option<i32>,
        start_cursor: Vec<u8>,
        projection: Vec<Projection>,
        order: Vec<PropertyOrder>,
    ) -> crate::google::datastore::v1::QueryResultBatch {
        let metadata_holder: Vec<EntityWithMetadata>;
        let filter_type = filter.as_ref().and_then(|f| f.filter_type.clone());
        let matches_filter = |entity_metadata: &EntityWithMetadata| -> bool {
            if let Some(ref filter_type) = filter_type {
                DatastoreStorage::apply_filter(entity_metadata, filter_type)
            } else {
                true
            }
        };

        let mut filtered_entities: Vec<&EntityWithMetadata> =
            if METADATA_KINDS.contains(&kind_name.as_str()) {
                metadata_holder = self.get_metadata(&kind_name, &project_id_filter);
                metadata_holder
                    .iter()
                    .filter(|entity_metadata| matches_filter(entity_metadata))
                    .collect()
            } else if let Some(ref filter_type) = filter_type {
                if let Some(candidate_keys) =
                    self.index_lookup_for_filter(&project_id_filter, &kind_name, filter_type)
                {
                    candidate_keys
                        .into_iter()
                        .filter(|key_struct| {
                            key_struct.project_id == project_id_filter
                                && key_struct
                                    .path_elements
                                    .last()
                                    .is_some_and(|(k, _)| k == &kind_name)
                        })
                        .filter_map(|key_struct| self.entities.get(&key_struct))
                        .filter(|entity_metadata| matches_filter(entity_metadata))
                        .collect()
                } else {
                    self.entities
                        .iter()
                        .filter(|(k, _)| {
                            k.project_id == project_id_filter
                                && k.path_elements.last().is_some_and(|(k, _)| k == &kind_name)
                        })
                        .filter_map(|(_, entity_metadata)| {
                            if matches_filter(entity_metadata) {
                                Some(entity_metadata)
                            } else {
                                None
                            }
                        })
                        .collect()
                }
            } else {
                self.entities
                    .iter()
                    .filter(|(k, _)| {
                        k.project_id == project_id_filter
                            && k.path_elements.last().is_some_and(|(k, _)| k == &kind_name)
                    })
                    .map(|(_, entity_metadata)| entity_metadata)
                    .collect()
            };

        // 2. Sort entities
        if !order.is_empty() {
            filtered_entities.sort_by(|a_meta, b_meta| {
                let mut final_ordering = Ordering::Equal;
                for order_by in &order {
                    if final_ordering != Ordering::Equal {
                        break; // Already decided by a previous order property
                    }

                    let prop_name = &order_by.property.as_ref().unwrap().name;

                    let a_val = if prop_name == "__key__" {
                        a_meta.entity.key.as_ref().map(|k| Value {
                            value_type: Some(ValueType::KeyValue(k.clone())),
                            ..Default::default()
                        })
                    } else {
                        a_meta.entity.properties.get(prop_name).cloned()
                    };
                    let b_val = if prop_name == "__key__" {
                        b_meta.entity.key.as_ref().map(|k| Value {
                            value_type: Some(ValueType::KeyValue(k.clone())),
                            ..Default::default()
                        })
                    } else {
                        b_meta.entity.properties.get(prop_name).cloned()
                    };

                    let ordering = match (a_val.as_ref(), b_val.as_ref()) {
                        (Some(a), Some(b)) => compare_values(a, b),
                        (Some(_), None) => Ordering::Greater,
                        (None, Some(_)) => Ordering::Less,
                        (None, None) => Ordering::Equal,
                    };

                    final_ordering =
                        if order_by.direction == property_order::Direction::Descending as i32 {
                            ordering.reverse()
                        } else {
                            ordering
                        };
                }
                final_ordering
            });
        }

        // 3. Paginate and apply projections
        let mut results = Vec::new();
        let mut start = 0;
        if !start_cursor.is_empty() {
            start = u32::from_be_bytes(
                start_cursor
                    .get(0..4)
                    .and_then(|bytes| bytes.try_into().ok())
                    .unwrap_or([0, 0, 0, 0]),
            ) as usize;
        }

        let db_entities_count = filtered_entities.len();
        let mut current_entities_payload_size: usize = 0;
        let mut new_more_results_state = MoreResultsType::NoMoreResults;
        let mut items_processed_from_start = 0;

        let is_keys_only = !projection.is_empty()
            && projection
                .iter()
                .all(|p| p.property.as_ref().is_some_and(|pr| pr.name == "__key__"));

        for entity_metadata in filtered_entities.iter().skip(start) {
            items_processed_from_start += 1;

            // Apply limit and payload size checks
            let entity_size_bytes = entity_metadata.entity.encoded_len(); // This might need adjustment for projections
            if !results.is_empty()
                && (current_entities_payload_size + entity_size_bytes > MAX_ENTITIES_PAYLOAD_BYTES)
            {
                new_more_results_state = MoreResultsType::NotFinished;
                items_processed_from_start -= 1;
                break;
            }

            if let Some(limit_value) = limit
                && results.len() >= limit_value as usize
            {
                new_more_results_state = MoreResultsType::MoreResultsAfterLimit;
                items_processed_from_start -= 1;
                break;
            }

            // Apply projection
            let result_entity = if is_keys_only {
                Entity {
                    key: entity_metadata.entity.key.clone(),
                    properties: HashMap::new(),
                }
            } else if !projection.is_empty() {
                let mut projected_properties = HashMap::new();
                for p in &projection {
                    if let Some(prop_ref) = &p.property
                        && let Some(value) = entity_metadata.entity.properties.get(&prop_ref.name)
                    {
                        projected_properties.insert(prop_ref.name.clone(), value.clone());
                    }
                }
                Entity {
                    key: entity_metadata.entity.key.clone(),
                    properties: projected_properties,
                }
            } else {
                entity_metadata.entity.clone()
            };

            results.push(EntityResult {
                entity: Some(result_entity),
                create_time: Some(entity_metadata.create_time.clone()),
                update_time: Some(entity_metadata.update_time.clone()),
                cursor: vec![],
                version: entity_metadata.version as i64,
            });
            current_entities_payload_size += entity_metadata.entity.encoded_len(); // Use original size for payload calculation
        }

        let final_cursor_offset = start + items_processed_from_start;

        let end_cursor = {
            if final_cursor_offset >= db_entities_count {
                vec![]
            } else {
                (final_cursor_offset as u32).to_be_bytes().to_vec()
            }
        };

        if new_more_results_state == MoreResultsType::NoMoreResults
            && final_cursor_offset < db_entities_count
        {
            new_more_results_state = MoreResultsType::NotFinished;
        }

        let entity_result_type = if is_keys_only || !projection.is_empty() {
            2 // PROJECTION
        } else {
            1 // FULL
        };

        crate::google::datastore::v1::QueryResultBatch {
            entity_result_type,
            skipped_results: start as i32,
            read_time: None,
            skipped_cursor: vec![],
            snapshot_version: 0,
            entity_results: results,
            more_results: new_more_results_state as i32,
            end_cursor,
        }
    }

    pub fn update_indexes(&mut self, key_struct: &KeyStruct, entity: &Entity) {
        // For each property of an entity, creates an index
        for (prop_name, prop_value) in &entity.properties {
            if !prop_value.exclude_from_indexes
                && let Some((kind, _)) = key_struct.path_elements.last()
            {
                for value_str in get_indexable_strings_for_value(prop_value) {
                    let index_key = (kind.clone(), prop_name.clone(), value_str);
                    self.indexes
                        .entry(index_key)
                        .or_default()
                        .insert(key_struct.clone());
                }
            }
        }
    }

    pub fn remove_from_indexes(&mut self, key_struct: &KeyStruct, entity: &Entity) {
        // For each property of an entity, remove its KeyStruct from the index
        for (prop_name, prop_value) in &entity.properties {
            if !prop_value.exclude_from_indexes
                && let Some((kind, _)) = key_struct.path_elements.last()
            {
                for value_str in get_indexable_strings_for_value(prop_value) {
                    let index_key = (kind.clone(), prop_name.clone(), value_str);
                    if let Some(indexed_keys_set) = self.indexes.get_mut(&index_key) {
                        indexed_keys_set.remove(key_struct);
                        // Optional: if indexed_keys_set is empty after removal,
                        // we could remove the index_key itself from self.indexes.
                        // if indexed_keys_set.is_empty() {
                        //     self.indexes.remove(&index_key);
                        // }
                    }
                }
            }
        }
    }

    pub fn delete_entity(&mut self, key_to_delete: &Key) -> Option<EntityWithMetadata> {
        let key_struct_to_delete = KeyStruct::from_datastore_key(key_to_delete);

        if let Some(removed_entity_metadata) = self.entities.remove(&key_struct_to_delete) {
            // If entity was removed, also remove it from indexes
            self.remove_from_indexes(&key_struct_to_delete, &removed_entity_metadata.entity);
            Some(removed_entity_metadata)
        } else {
            None
        }
    }
}
