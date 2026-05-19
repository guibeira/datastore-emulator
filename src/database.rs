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
    ArrayValue, Filter, LatLng, PartitionId, Projection, PropertyOrder, PropertyReference, Value,
    property_order,
};
use chrono::DateTime;
use prost::Message;
use rayon::iter::ParallelIterator;
use rayon::prelude::*;
use std::cmp::Ordering;
use std::sync::{Arc, atomic::AtomicI64};

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
    fn cmp_int_double(i: i64, d: f64) -> Ordering {
        if d.is_nan() {
            return Ordering::Equal;
        }
        if d == f64::INFINITY {
            return Ordering::Less;
        }
        if d == f64::NEG_INFINITY {
            return Ordering::Greater;
        }

        const I64_MIN_AS_F64: f64 = i64::MIN as f64;
        const I64_MAX_PLUS_ONE_AS_F64: f64 = 9_223_372_036_854_775_808.0;

        if d < I64_MIN_AS_F64 {
            return Ordering::Greater;
        }
        if d >= I64_MAX_PLUS_ONE_AS_F64 {
            return Ordering::Less;
        }
        if d.fract() == 0.0 {
            return i.cmp(&(d as i64));
        }

        let floor = d.floor() as i64;
        let ceil = d.ceil() as i64;
        if i <= floor {
            Ordering::Less
        } else if i >= ceil {
            Ordering::Greater
        } else {
            Ordering::Equal
        }
    }

    // Datastore canonical cross-type ordering:
    // null < number (int/double) < timestamp < boolean < string < blob <
    // geopoint < key < entity < array.
    fn type_rank(v: &Value) -> u8 {
        match &v.value_type {
            None => 0,
            Some(ValueType::NullValue(_)) => 1,
            Some(ValueType::IntegerValue(_)) | Some(ValueType::DoubleValue(_)) => 2,
            Some(ValueType::TimestampValue(_)) => 3,
            Some(ValueType::BooleanValue(_)) => 4,
            Some(ValueType::StringValue(_)) => 5,
            Some(ValueType::BlobValue(_)) => 6,
            Some(ValueType::GeoPointValue(_)) => 7,
            Some(ValueType::KeyValue(_)) => 8,
            Some(ValueType::EntityValue(_)) => 9,
            Some(ValueType::ArrayValue(_)) => 10,
        }
    }

    let rank_ord = type_rank(a).cmp(&type_rank(b));
    if rank_ord != Ordering::Equal {
        return rank_ord;
    }

    match (&a.value_type, &b.value_type) {
        (Some(ValueType::NullValue(_)), Some(ValueType::NullValue(_))) => Ordering::Equal,
        // Integers and doubles share rank 2 and must compare as numbers.
        (Some(ValueType::IntegerValue(av)), Some(ValueType::IntegerValue(bv))) => av.cmp(bv),
        (Some(ValueType::DoubleValue(av)), Some(ValueType::DoubleValue(bv))) => {
            av.partial_cmp(bv).unwrap_or(Ordering::Equal)
        }
        (Some(ValueType::IntegerValue(av)), Some(ValueType::DoubleValue(bv))) => {
            cmp_int_double(*av, *bv)
        }
        (Some(ValueType::DoubleValue(av)), Some(ValueType::IntegerValue(bv))) => {
            cmp_int_double(*bv, *av).reverse()
        }
        (Some(ValueType::TimestampValue(av)), Some(ValueType::TimestampValue(bv))) => av
            .seconds
            .cmp(&bv.seconds)
            .then_with(|| av.nanos.cmp(&bv.nanos)),
        (Some(ValueType::BooleanValue(av)), Some(ValueType::BooleanValue(bv))) => av.cmp(bv),
        (Some(ValueType::StringValue(av)), Some(ValueType::StringValue(bv))) => av.cmp(bv),
        (Some(ValueType::BlobValue(av)), Some(ValueType::BlobValue(bv))) => av.cmp(bv),
        (Some(ValueType::GeoPointValue(av)), Some(ValueType::GeoPointValue(bv))) => av
            .latitude
            .partial_cmp(&bv.latitude)
            .unwrap_or(Ordering::Equal)
            .then_with(|| {
                av.longitude
                    .partial_cmp(&bv.longitude)
                    .unwrap_or(Ordering::Equal)
            }),
        (Some(ValueType::KeyValue(av)), Some(ValueType::KeyValue(bv))) => {
            KeyStruct::from_datastore_key(av).cmp(&KeyStruct::from_datastore_key(bv))
        }
        // Same-rank but unsupported deeper compare (e.g. entity/array): treat as equal.
        _ => Ordering::Equal,
    }
}

fn sort_value(value: &Value, descending: bool) -> Option<&Value> {
    if value.exclude_from_indexes {
        return None;
    }

    match &value.value_type {
        Some(ValueType::ArrayValue(array)) => {
            let indexed_values = array.values.iter().filter(|v| !v.exclude_from_indexes);
            if descending {
                indexed_values.max_by(|a, b| compare_values(a, b))
            } else {
                indexed_values.min_by(|a, b| compare_values(a, b))
            }
        }
        _ => Some(value),
    }
}

/// Resolves a nested property path (e.g., "organizations.key.consistentId") against an entity's properties.
/// Returns all values found at the path (handles arrays at any level).
fn resolve_nested_property<'a>(
    properties: &'a HashMap<String, Value>,
    property_path: &str,
) -> Vec<&'a Value> {
    let parts: Vec<&str> = property_path.split('.').collect();
    if parts.is_empty() {
        return Vec::new();
    }

    resolve_property_path_recursive(properties, &parts)
}

/// Recursively resolve a property path through nested entities and arrays.
fn resolve_property_path_recursive<'a>(
    properties: &'a HashMap<String, Value>,
    path_parts: &[&str],
) -> Vec<&'a Value> {
    if path_parts.is_empty() {
        return Vec::new();
    }

    let current_part = path_parts[0];
    let remaining_parts = &path_parts[1..];

    // Get the value for the current property name
    let Some(value) = properties.get(current_part) else {
        return Vec::new();
    };

    // If this is the last part of the path, return the value(s)
    if remaining_parts.is_empty() {
        return match &value.value_type {
            Some(ValueType::ArrayValue(array)) => array.values.iter().collect(),
            _ => vec![value],
        };
    }

    // Otherwise, we need to traverse deeper
    match &value.value_type {
        Some(ValueType::EntityValue(entity)) => {
            // Single embedded entity - recurse into its properties
            resolve_property_path_recursive(&entity.properties, remaining_parts)
        }
        Some(ValueType::ArrayValue(array)) => {
            // Array of values - for each element that is an entity, recurse and collect results
            let mut results = Vec::with_capacity(array.values.len());
            for item in &array.values {
                if let Some(ValueType::EntityValue(entity)) = &item.value_type {
                    results.extend(resolve_property_path_recursive(
                        &entity.properties,
                        remaining_parts,
                    ));
                }
            }
            results
        }
        _ => {
            // Value is not an entity/array, can't traverse further
            Vec::new()
        }
    }
}

/// Collects all indexable property paths from an entity, including nested paths.
/// Returns tuples of (property_path, indexable_values).
/// For example, for "organizations.key.consistentId", this would return:
/// [("organizations.key.consistentId", vec!["value1", "value2", ...])]
fn collect_nested_indexable_paths(
    properties: &HashMap<String, Value>,
    prefix: &str,
) -> Vec<(String, Vec<String>)> {
    let mut results = Vec::with_capacity(properties.len());

    for (prop_name, prop_value) in properties {
        if prop_value.exclude_from_indexes {
            continue;
        }

        let full_path = if prefix.is_empty() {
            prop_name.clone()
        } else {
            let mut s = String::with_capacity(prefix.len() + 1 + prop_name.len());
            s.push_str(prefix);
            s.push('.');
            s.push_str(prop_name);
            s
        };

        // Get indexable values for this property at its current level
        let indexable_values = get_indexable_strings_for_value(prop_value);
        if !indexable_values.is_empty() {
            results.push((full_path.clone(), indexable_values));
        }

        // Recurse into nested entities
        match &prop_value.value_type {
            Some(ValueType::EntityValue(entity)) => {
                results.extend(collect_nested_indexable_paths(
                    &entity.properties,
                    &full_path,
                ));
            }
            Some(ValueType::ArrayValue(array)) => {
                // For arrays, collect nested properties from each entity element
                for item in &array.values {
                    if let Some(ValueType::EntityValue(entity)) = &item.value_type {
                        results.extend(collect_nested_indexable_paths(
                            &entity.properties,
                            &full_path,
                        ));
                    }
                }
            }
            _ => {}
        }
    }

    results
}

fn get_indexable_strings_for_value(value: &Value) -> Vec<String> {
    let capacity = match &value.value_type {
        Some(ValueType::ArrayValue(array)) => array.values.len(),
        _ => 1,
    };
    let mut values = Vec::with_capacity(capacity);
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

enum PreparedKeyFilter {
    Compare(i32, KeyStruct),
    In(Vec<KeyStruct>),
    NotIn(Vec<KeyStruct>),
}

fn try_prepare_key_filter(filter: &FilterType) -> Option<PreparedKeyFilter> {
    let FilterType::PropertyFilter(pf) = filter else {
        return None;
    };
    if pf.op == 11 {
        return None;
    }
    let property = pf.property.as_ref()?;
    if property.name != "__key__" {
        return None;
    }
    let value = pf.value.as_ref()?;

    match (pf.op, &value.value_type) {
        (op @ (1 | 2 | 3 | 4 | 5 | 9), Some(ValueType::KeyValue(k))) => Some(
            PreparedKeyFilter::Compare(op, KeyStruct::from_datastore_key(k)),
        ),
        (6, Some(ValueType::ArrayValue(arr))) => {
            let keys = arr
                .values
                .iter()
                .filter_map(|v| match &v.value_type {
                    Some(ValueType::KeyValue(k)) => Some(KeyStruct::from_datastore_key(k)),
                    _ => None,
                })
                .collect();
            Some(PreparedKeyFilter::In(keys))
        }
        (13, Some(ValueType::ArrayValue(arr))) => {
            let keys = arr
                .values
                .iter()
                .filter_map(|v| match &v.value_type {
                    Some(ValueType::KeyValue(k)) => Some(KeyStruct::from_datastore_key(k)),
                    _ => None,
                })
                .collect();
            Some(PreparedKeyFilter::NotIn(keys))
        }
        _ => None,
    }
}

fn match_prepared_key_filter(key_struct: &KeyStruct, kind: &PreparedKeyFilter) -> bool {
    match kind {
        PreparedKeyFilter::Compare(op, target) => {
            let ord = key_struct.cmp(target);
            match op {
                1 => ord.is_lt(),
                2 => ord.is_le(),
                3 => ord.is_gt(),
                4 => ord.is_ge(),
                5 => ord.is_eq(),
                9 => !ord.is_eq(),
                _ => false,
            }
        }
        PreparedKeyFilter::In(keys) => keys.iter().any(|k| k == key_struct),
        PreparedKeyFilter::NotIn(keys) => !keys.iter().any(|k| k == key_struct),
    }
}

fn match_key_against_filter(entity_key: &Key, filter_value: &Value, op: i32) -> bool {
    let entity_key_struct = KeyStruct::from_datastore_key(entity_key);

    let compare_with = |other: &Key| entity_key_struct.cmp(&KeyStruct::from_datastore_key(other));

    match op {
        0 => true,
        1 | 2 | 3 | 4 | 5 | 9 => {
            let Some(ValueType::KeyValue(filter_key)) = &filter_value.value_type else {
                return false;
            };
            let ord = compare_with(filter_key);
            match op {
                1 => ord.is_lt(),
                2 => ord.is_le(),
                3 => ord.is_gt(),
                4 => ord.is_ge(),
                5 => ord.is_eq(),
                9 => !ord.is_eq(),
                _ => false,
            }
        }
        6 => {
            let Some(ValueType::ArrayValue(arr)) = &filter_value.value_type else {
                return false;
            };
            arr.values.iter().any(|v| match &v.value_type {
                Some(ValueType::KeyValue(k)) => compare_with(k).is_eq(),
                _ => false,
            })
        }
        13 => {
            let Some(ValueType::ArrayValue(arr)) = &filter_value.value_type else {
                return true;
            };
            !arr.values.iter().any(|v| match &v.value_type {
                Some(ValueType::KeyValue(k)) => compare_with(k).is_eq(),
                _ => false,
            })
        }
        _ => false,
    }
}

fn values_match_filter<'a>(
    entity_values: impl IntoIterator<Item = &'a Value>,
    filter_value: &Value,
    op: i32,
) -> bool {
    let mut entity_values = entity_values.into_iter();

    match op {
        0 => entity_values.next().is_some(), // OPERATOR_UNSPECIFIED
        1 => entity_values.any(|v| compare_values(v, filter_value).is_lt()),
        2 => entity_values.any(|v| compare_values(v, filter_value).is_le()),
        3 => entity_values.any(|v| compare_values(v, filter_value).is_gt()),
        4 => entity_values.any(|v| compare_values(v, filter_value).is_ge()),
        5 => entity_values.any(|v| v.value_type == filter_value.value_type),
        6 => {
            if let Some(ValueType::ArrayValue(array_value)) = &filter_value.value_type {
                entity_values.any(|v| {
                    array_value
                        .values
                        .iter()
                        .any(|candidate| candidate.value_type == v.value_type)
                })
            } else {
                false
            }
        }
        9 => {
            let mut saw_value = false;
            for value in entity_values {
                saw_value = true;
                if value.value_type == filter_value.value_type {
                    return false;
                }
            }
            saw_value
        }
        13 => {
            if let Some(ValueType::ArrayValue(array_value)) = &filter_value.value_type {
                let mut saw_value = false;
                for value in entity_values {
                    saw_value = true;
                    if array_value
                        .values
                        .iter()
                        .any(|candidate| candidate.value_type == value.value_type)
                    {
                        return false;
                    }
                }
                saw_value
            } else {
                entity_values.next().is_some()
            }
        }
        _ => false,
    }
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

            let now = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default();
            let timestamp_now = pbjson_types::Timestamp {
                seconds: now.as_secs() as i64,
                nanos: (now.as_nanos() % 1_000_000_000) as i32,
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

#[derive(serde::Serialize, serde::Deserialize, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub enum KeyId {
    IntId(i64),
    StringId(String),
}

// Custom key structure for efficient indexing
#[derive(serde::Serialize, serde::Deserialize, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
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
    pub create_time: pbjson_types::Timestamp,
    pub update_time: pbjson_types::Timestamp,
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
            pbjson_types::Timestamp::decode(&s.create_time_bytes[..]).map_err(Error::custom)?;
        let update_time =
            pbjson_types::Timestamp::decode(&s.update_time_bytes[..]).map_err(Error::custom)?;

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
    pub timestamp: pbjson_types::Timestamp,
    pub read_only: bool,
}

#[derive(Clone)]
pub(crate) struct QueryCandidate {
    pub(crate) key_struct: Option<KeyStruct>,
    pub(crate) entity_metadata: Arc<EntityWithMetadata>,
}

impl QueryCandidate {
    fn from_metadata(entity_metadata: Arc<EntityWithMetadata>) -> Self {
        let key_struct = entity_metadata
            .entity
            .key
            .as_ref()
            .map(KeyStruct::from_datastore_key);
        Self {
            key_struct,
            entity_metadata,
        }
    }

    fn from_stored_entity(key_struct: KeyStruct, entity_metadata: Arc<EntityWithMetadata>) -> Self {
        Self {
            key_struct: Some(key_struct),
            entity_metadata,
        }
    }
}

#[derive(Default, Debug)]
pub struct DatastoreStorage {
    // Stores using BTreeMap for ordered storage, mapping a full KeyStruct to its EntityWithMetadata
    pub entities: BTreeMap<KeyStruct, Arc<EntityWithMetadata>>,
    // Indexes for efficient queries
    pub indexes: HashMap<(String, String, String), BTreeSet<KeyStruct>>,
    // Active transactions
    pub transactions: HashMap<String, TransactionState>,
    // Per-(project, namespace, kind) counters for auto ID allocation
    pub entity_id_counters: HashMap<(String, String, String), AtomicI64>,
    // Counter used for transaction IDs
    pub transaction_counter: AtomicI64,
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
                                    // project_id and kind filtering happens again in
                                    // query_candidates, so skip the redundant per-key
                                    // filter+clone here and bulk-clone the index set.
                                    result = Some(match result {
                                        Some(mut acc) => {
                                            acc.extend(keys.iter().cloned());
                                            acc
                                        }
                                        None => keys.clone(),
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
                                    acc.extend(keys.iter().cloned());
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
                    let current = counter.load(std::sync::atomic::Ordering::SeqCst);
                    if current < id {
                        counter.store(id, std::sync::atomic::Ordering::SeqCst);
                    }
                })
                .or_insert_with(|| AtomicI64::new(id));
        }
    }

    pub fn next_auto_id(&mut self, key: &Key) -> Result<i64, tonic::Status> {
        let scope = Self::counter_scope_from_key(key).ok_or_else(|| {
            tonic::Status::invalid_argument(
                "The entity key has no path elements to determine the 'kind' for ID generation.",
            )
        })?;
        let counter = self
            .entity_id_counters
            .entry(scope)
            .or_insert_with(|| AtomicI64::new(0));
        let next = counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
        Ok(next)
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
        self.entities = entities
            .into_iter()
            .map(|(key, metadata)| (key, Arc::new(metadata)))
            .collect();

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
        let entities: BTreeMap<KeyStruct, EntityWithMetadata> = self
            .entities
            .iter()
            .map(|(key, metadata)| (key.clone(), metadata.as_ref().clone()))
            .collect();

        let buffer = bincode::serde::encode_to_vec(&entities, bincode::config::standard())
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
    ) -> Result<(Key, Arc<EntityWithMetadata>), tonic::Status> {
        let Some(original_key) = entity.key.as_ref() else {
            return Err(tonic::Status::invalid_argument("Entity missing key"));
        };

        let needs_auto_id = original_key
            .path
            .iter()
            .any(|path_element| path_element.id_type.is_none());
        let new_id_value = if needs_auto_id {
            Some(self.next_auto_id(original_key)?)
        } else {
            None
        };

        let mut key_with_new_id = original_key.clone();
        if let Some(id_value) = new_id_value {
            for path_element in key_with_new_id.path.iter_mut() {
                if path_element.id_type.is_none() {
                    path_element.id_type = Some(IdType::Id(id_value));
                }
            }
        }

        let final_key_struct = KeyStruct::from_datastore_key(&key_with_new_id);

        let mut db_entity = entity.clone();
        db_entity.key = Some(key_with_new_id.clone());

        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default();
        let timestamp_now = pbjson_types::Timestamp {
            seconds: now.as_secs() as i64,
            nanos: (now.as_nanos() % 1_000_000_000) as i32,
        };

        let entity_metadata = Arc::new(EntityWithMetadata {
            entity: db_entity,
            version: 1, // Initial version
            create_time: timestamp_now.clone(),
            update_time: timestamp_now,
        });

        self.entities
            .insert(final_key_struct.clone(), Arc::clone(&entity_metadata));
        self.update_indexes(&final_key_struct, &entity_metadata.entity);
        if let Some(ref key) = entity_metadata.entity.key {
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
                            // Check if this is a nested property path (contains dots)
                            if property.name == "__key__" {
                                if let Some(key) = entity_metadata.entity.key.as_ref() {
                                    match_key_against_filter(key, filter_value, property_filter.op)
                                } else {
                                    false
                                }
                            } else if property.name.contains('.') {
                                // Nested property path - use recursive resolution
                                let entity_values = resolve_nested_property(
                                    &entity_metadata.entity.properties,
                                    &property.name,
                                );
                                values_match_filter(
                                    entity_values
                                        .into_iter()
                                        .filter(|v| !v.exclude_from_indexes),
                                    filter_value,
                                    property_filter.op,
                                )
                            } else {
                                // Simple property lookup
                                if let Some(value) =
                                    entity_metadata.entity.properties.get(&property.name)
                                {
                                    if value.exclude_from_indexes {
                                        false
                                    } else {
                                        match &value.value_type {
                                            Some(ValueType::ArrayValue(array)) => {
                                                values_match_filter(
                                                    array.values.iter().filter(|inner| {
                                                        !inner.exclude_from_indexes
                                                    }),
                                                    filter_value,
                                                    property_filter.op,
                                                )
                                            }
                                            _ => values_match_filter(
                                                std::iter::once(value),
                                                filter_value,
                                                property_filter.op,
                                            ),
                                        }
                                    }
                                } else {
                                    false
                                }
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

    pub fn get_entity(&self, key: &Key) -> Option<Arc<EntityWithMetadata>> {
        let key_struct = KeyStruct::from_datastore_key(key);
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
                        create_time: pbjson_types::Timestamp::default(),
                        update_time: pbjson_types::Timestamp::default(),
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
                            create_time: pbjson_types::Timestamp::default(),
                            update_time: pbjson_types::Timestamp::default(),
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
                            create_time: pbjson_types::Timestamp::default(),
                            update_time: pbjson_types::Timestamp::default(),
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
    pub(crate) fn query_candidates(
        &self,
        project_id_filter: &str,
        kind_name: &str,
        filter_type: Option<&FilterType>,
    ) -> (Vec<QueryCandidate>, bool) {
        if METADATA_KINDS.contains(&kind_name) {
            return (
                self.get_metadata(kind_name, project_id_filter)
                    .into_iter()
                    .map(|metadata| QueryCandidate::from_metadata(Arc::new(metadata)))
                    .collect(),
                false,
            );
        }

        if let Some(filter_type) = filter_type
            && let Some(candidate_keys) =
                self.index_lookup_for_filter(project_id_filter, kind_name, filter_type)
        {
            return (
                candidate_keys
                    .into_iter()
                    .filter(|key_struct| {
                        key_struct.project_id == project_id_filter
                            && key_struct
                                .path_elements
                                .last()
                                .is_some_and(|(k, _)| k == kind_name)
                    })
                    .filter_map(|key_struct| {
                        self.entities.get(&key_struct).map(|entity_metadata| {
                            QueryCandidate::from_stored_entity(
                                key_struct,
                                Arc::clone(entity_metadata),
                            )
                        })
                    })
                    .collect(),
                false,
            );
        }

        let prepared_key_filter = filter_type.and_then(try_prepare_key_filter);

        let candidates = self
            .entities
            .iter()
            .filter(|(key_struct, _)| {
                key_struct.project_id == project_id_filter
                    && key_struct
                        .path_elements
                        .last()
                        .is_some_and(|(k, _)| k == kind_name)
            })
            .filter(|(key_struct, entity_metadata)| {
                if let Some(ref pkf) = prepared_key_filter {
                    match_prepared_key_filter(key_struct, pkf)
                } else {
                    filter_type.is_none_or(|filter_type| {
                        DatastoreStorage::apply_filter(entity_metadata.as_ref(), filter_type)
                    })
                }
            })
            .map(|(key_struct, entity_metadata)| {
                QueryCandidate::from_stored_entity(key_struct.clone(), Arc::clone(entity_metadata))
            })
            .collect();

        (candidates, filter_type.is_some())
    }

    pub(crate) fn get_entities_from_candidates(
        candidates: Vec<QueryCandidate>,
        filter_type: Option<FilterType>,
        candidates_prefiltered: bool,
        limit: Option<i32>,
        offset: i32,
        start_cursor: Vec<u8>,
        projection: Vec<Projection>,
        distinct_on: Vec<PropertyReference>,
        order: Vec<PropertyOrder>,
    ) -> crate::google::datastore::v1::QueryResultBatch {
        let prepared_key_filter = filter_type.as_ref().and_then(try_prepare_key_filter);

        let matches_filter = |candidate: &QueryCandidate| -> bool {
            if candidates_prefiltered {
                return true;
            }
            if let Some(ref pkf) = prepared_key_filter {
                let Some(ks) = candidate.key_struct.as_ref() else {
                    return false;
                };
                return match_prepared_key_filter(ks, pkf);
            }
            if let Some(ref filter_type) = filter_type {
                DatastoreStorage::apply_filter(candidate.entity_metadata.as_ref(), filter_type)
            } else {
                true
            }
        };

        let limit = limit.filter(|value| *value > 0);

        let mut filtered_entities: Vec<QueryCandidate> = candidates
            .into_iter()
            .filter(|candidate| matches_filter(candidate))
            .collect();

        // 2. Sort entities
        if !order.is_empty() {
            filtered_entities.sort_unstable_by(|a, b| {
                let mut final_ordering = Ordering::Equal;
                for order_by in &order {
                    if final_ordering != Ordering::Equal {
                        break; // Already decided by a previous order property
                    }

                    let prop_name = &order_by.property.as_ref().unwrap().name;
                    let descending =
                        order_by.direction == property_order::Direction::Descending as i32;

                    if prop_name == "__key__" {
                        let ordering = match (a.key_struct.as_ref(), b.key_struct.as_ref()) {
                            (Some(a), Some(b)) => a.cmp(b),
                            (Some(_), None) => Ordering::Greater,
                            (None, Some(_)) => Ordering::Less,
                            (None, None) => Ordering::Equal,
                        };

                        final_ordering = if descending {
                            ordering.reverse()
                        } else {
                            ordering
                        };
                        continue;
                    }

                    let a_val = a
                        .entity_metadata
                        .entity
                        .properties
                        .get(prop_name)
                        .and_then(|v| sort_value(v, descending));
                    let b_val = b
                        .entity_metadata
                        .entity
                        .properties
                        .get(prop_name)
                        .and_then(|v| sort_value(v, descending));

                    let ordering = match (a_val, b_val) {
                        (Some(a), Some(b)) => compare_values(a, b),
                        (Some(_), None) => Ordering::Greater,
                        (None, Some(_)) => Ordering::Less,
                        (None, None) => Ordering::Equal,
                    };

                    final_ordering = if descending {
                        ordering.reverse()
                    } else {
                        ordering
                    };
                }
                final_ordering
            });
        }

        if !distinct_on.is_empty() {
            use std::hash::{Hash, Hasher};

            let mut seen_signatures: HashSet<u64> = HashSet::new();
            let mut distinct_entities = Vec::new();
            let mut scratch: Vec<u8> = Vec::new();

            for candidate in filtered_entities.into_iter() {
                let mut hasher = std::collections::hash_map::DefaultHasher::new();

                for prop in &distinct_on {
                    if prop.name == "__key__" {
                        hasher.write_u8(1);
                        if let Some(key_struct) = candidate.key_struct.as_ref() {
                            key_struct.hash(&mut hasher);
                        }
                    } else if let Some(value) =
                        candidate.entity_metadata.entity.properties.get(&prop.name)
                    {
                        hasher.write_u8(2);
                        match &value.value_type {
                            Some(ValueType::StringValue(s)) => {
                                hasher.write_u8(b's');
                                hasher.write(s.as_bytes());
                            }
                            Some(ValueType::IntegerValue(i)) => {
                                hasher.write_u8(b'i');
                                hasher.write_i64(*i);
                            }
                            Some(ValueType::BooleanValue(b)) => {
                                hasher.write_u8(b'b');
                                hasher.write_u8(*b as u8);
                            }
                            _ => {
                                hasher.write_u8(b'?');
                                scratch.clear();
                                value
                                    .encode_length_delimited(&mut scratch)
                                    .expect("encode distinct value signature");
                                hasher.write(&scratch);
                            }
                        }
                    } else {
                        hasher.write_u8(3);
                        hasher.write(prop.name.as_bytes());
                    }
                    hasher.write_u8(0xFF);
                }

                if seen_signatures.insert(hasher.finish()) {
                    distinct_entities.push(candidate);
                }
            }

            filtered_entities = distinct_entities;
        }

        // 3. Paginate and apply projections
        let mut results = Vec::new();
        let mut start_index = 0usize;
        if !start_cursor.is_empty() {
            let decode_result = bincode::serde::decode_from_slice::<KeyStruct, _>(
                &start_cursor,
                bincode::config::standard(),
            );
            if let Ok((cursor_key, _)) = decode_result {
                let mut matched = false;
                for (idx, candidate) in filtered_entities.iter().enumerate() {
                    if let Some(key_struct) = candidate.key_struct.as_ref() {
                        if key_struct == &cursor_key {
                            start_index = idx + 1;
                            matched = true;
                            break;
                        }
                    }
                }
                if !matched {
                    let mut index = 0usize;
                    for candidate in &filtered_entities {
                        match candidate.key_struct.as_ref() {
                            Some(key_struct) if key_struct <= &cursor_key => index += 1,
                            _ => break,
                        }
                    }
                    start_index = index;
                }
            } else if start_cursor.len() >= 4 {
                start_index = u32::from_be_bytes(
                    start_cursor
                        .get(0..4)
                        .and_then(|bytes| bytes.try_into().ok())
                        .unwrap_or([0, 0, 0, 0]),
                ) as usize;
            }
        }

        // Apply Query.offset on top of any cursor-derived start_index.
        if offset > 0 {
            start_index = start_index.saturating_add(offset as usize);
            if start_index > filtered_entities.len() {
                start_index = filtered_entities.len();
            }
        }

        let db_entities_count = filtered_entities.len();
        let mut current_entities_payload_size: usize = 0;
        let mut new_more_results_state = MoreResultsType::NoMoreResults;
        let mut next_offset = start_index;
        let mut last_cursor_bytes: Option<Vec<u8>> = None;

        let is_keys_only = !projection.is_empty()
            && projection
                .iter()
                .all(|p| p.property.as_ref().is_some_and(|pr| pr.name == "__key__"));

        for candidate in filtered_entities.iter().skip(start_index) {
            let entity_metadata = candidate.entity_metadata.as_ref();
            // Apply limit and payload size checks
            let entity_size_bytes = entity_metadata.entity.encoded_len(); // This might need adjustment for projections
            if !results.is_empty()
                && (current_entities_payload_size + entity_size_bytes > MAX_ENTITIES_PAYLOAD_BYTES)
            {
                new_more_results_state = MoreResultsType::NotFinished;
                break;
            }

            if let Some(limit_value) = limit
                && results.len() >= limit_value as usize
            {
                new_more_results_state = MoreResultsType::MoreResultsAfterLimit;
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

            let cursor_value = if let Some(key_struct) = candidate.key_struct.as_ref() {
                bincode::serde::encode_to_vec(key_struct, bincode::config::standard())
                    .unwrap_or_else(|_| (next_offset as u32 + 1).to_be_bytes().to_vec())
            } else {
                (next_offset as u32 + 1).to_be_bytes().to_vec()
            };

            let cursor_clone = cursor_value.clone();

            results.push(EntityResult {
                entity: Some(result_entity),
                create_time: Some(entity_metadata.create_time.clone()),
                update_time: Some(entity_metadata.update_time.clone()),
                cursor: cursor_value,
                version: entity_metadata.version as i64,
            });
            current_entities_payload_size += entity_size_bytes;
            next_offset += 1;
            last_cursor_bytes = Some(cursor_clone);
        }

        let final_cursor_offset = next_offset;

        let end_cursor = {
            if let Some(cursor_bytes) = last_cursor_bytes {
                cursor_bytes
            } else if final_cursor_offset >= db_entities_count {
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

        tracing::debug!(
            "QueryResultBatch: start={} returned={} more_results={:?} final_offset={} total={} end_cursor_len={}",
            start_index,
            results.len(),
            new_more_results_state,
            final_cursor_offset,
            db_entities_count,
            end_cursor.len()
        );

        let entity_result_type = if is_keys_only {
            3 // KEY_ONLY
        } else if !projection.is_empty() {
            2 // PROJECTION
        } else {
            1 // FULL
        };

        crate::google::datastore::v1::QueryResultBatch {
            entity_result_type,
            skipped_results: start_index as i32,
            read_time: None,
            skipped_cursor: vec![],
            snapshot_version: 0,
            entity_results: results,
            more_results: new_more_results_state as i32,
            end_cursor,
        }
    }

    pub fn get_entities(
        &self,
        project_id_filter: String,
        kind_name: String,
        filter: Option<Filter>,
        limit: Option<i32>,
        offset: i32,
        start_cursor: Vec<u8>,
        projection: Vec<Projection>,
        distinct_on: Vec<PropertyReference>,
        order: Vec<PropertyOrder>,
    ) -> crate::google::datastore::v1::QueryResultBatch {
        let filter_type = filter.as_ref().and_then(|f| f.filter_type.clone());
        let (candidates, candidates_prefiltered) =
            self.query_candidates(&project_id_filter, &kind_name, filter_type.as_ref());
        Self::get_entities_from_candidates(
            candidates,
            filter_type,
            candidates_prefiltered,
            limit,
            offset,
            start_cursor,
            projection,
            distinct_on,
            order,
        )
    }

    pub fn update_indexes(&mut self, key_struct: &KeyStruct, entity: &Entity) {
        let Some((kind, _)) = key_struct.path_elements.last() else {
            return;
        };

        // Collect all indexable property paths including nested ones
        let nested_paths = collect_nested_indexable_paths(&entity.properties, "");

        for (prop_path, indexable_values) in nested_paths {
            for value_str in indexable_values {
                let index_key = (kind.clone(), prop_path.clone(), value_str);
                self.indexes
                    .entry(index_key)
                    .or_default()
                    .insert(key_struct.clone());
            }
        }
    }

    pub fn remove_from_indexes(&mut self, key_struct: &KeyStruct, entity: &Entity) {
        let Some((kind, _)) = key_struct.path_elements.last() else {
            return;
        };

        // Collect all indexable property paths including nested ones
        let nested_paths = collect_nested_indexable_paths(&entity.properties, "");

        for (prop_path, indexable_values) in nested_paths {
            for value_str in indexable_values {
                let index_key = (kind.clone(), prop_path.clone(), value_str);
                if let Some(indexed_keys_set) = self.indexes.get_mut(&index_key) {
                    indexed_keys_set.remove(key_struct);
                }
            }
        }
    }

    pub fn delete_entity(&mut self, key_to_delete: &Key) -> Option<EntityWithMetadata> {
        let key_struct_to_delete = KeyStruct::from_datastore_key(key_to_delete);

        if let Some(removed_entity_metadata) = self.entities.remove(&key_struct_to_delete) {
            // If entity was removed, also remove it from indexes
            self.remove_from_indexes(&key_struct_to_delete, &removed_entity_metadata.entity);
            Some(removed_entity_metadata.as_ref().clone())
        } else {
            None
        }
    }
}
