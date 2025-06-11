use crate::google::datastore::import_export::datastore_v3::EntityProto;
use crate::google::datastore::import_export::dsbackups::ExportMetadata;
use crate::google::datastore::import_export::dsbackups::OverallExportMetadata;
use crate::google::datastore::v1::key::path_element::IdType;
use rayon::prelude::*;

use crate::google::datastore::v1::Filter;
use crate::google::datastore::v1::filter::FilterType;
use crate::google::datastore::v1::value::ValueType;
use prost::Message;
use std::sync::{Arc, Mutex};

use crate::google::datastore::v1::{Entity, EntityResult, Key, Mutation};
use crate::leveldb::LogReader; // Added to resolve error E0433
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::time::SystemTime; // Importing LogReader to read log files

pub fn read_overall_metadata(export_dir: &str) -> Option<OverallExportMetadata> {
    let mut metadata_file = None;
    for entry in std::fs::read_dir(export_dir).expect("Failed to read export directory") {
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
    dbg!("Found metadata file:", _metadata_file.display());
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
                                eprintln!("Failed to decode OverallExportMetadata: {}", decode_err);
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("Error reading record {}: {}", i + 1, e); // Translated eprintln
                        break;
                    }
                }
            }
        }
        Err(e) => {
            eprintln!("Erro ao abrir o arquivo: {}", e);
        }
    }
    None // Returning None as we are not returning the metadata here
}

pub fn read_dump(export_dir: &str) -> Vec<EntityProto> {
    let entity_dump_path = std::path::PathBuf::from(export_dir).join("datastore_dump_output.json");

    println!(
        "Reading datastore export from: {}",
        entity_dump_path.display()
    );
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
                println!("Processing kind: {}", kind_name);
                let metadata_path = std::path::PathBuf::from(export_dir).join(&export_entry.path);

                if !metadata_path.exists() {
                    eprintln!(
                        "Metadata file for kind {} not found at: {}",
                        kind_name,
                        metadata_path.display()
                    );
                    return Vec::new().into_par_iter();
                }

                let export_metadata = match std::fs::read(&metadata_path)
                    .map_err(|e| {
                        eprintln!(
                            "Failed to read metadata file {}: {}",
                            metadata_path.display(),
                            e
                        );
                        e
                    })
                    .and_then(|data| {
                        ExportMetadata::decode(&data[..]).map_err(|e| {
                            eprintln!("Failed to decode ExportMetadata for {}: {}", kind_name, e);
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
                                                eprintln!(
                                                    "Failed to decode EntityProto from file {}",
                                                    output_file_path.display()
                                                );
                                            }
                                        }
                                        Err(e) => {
                                            eprintln!(
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
                                eprintln!("Error opening file: {}", output_file_path.display());
                            }
                        } else {
                            eprintln!("Output file {} does not exist.", output_file_path.display());
                        }
                        protos_in_file
                    })
                    .collect();

                protos_for_this_export.into_par_iter()
            })
            .collect();

        println!("Finished reading datastore export.");
        return entity_protos;
    } else {
        eprintln!("No overall metadata found in the export directory.");
        return Vec::new();
    }
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub enum KeyId {
    IntId(i64),
    StringId(String),
}

// Custom key structure for efficient indexing
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
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
    // This is a simple counter for generating unique IDs
    pub id_counter: Arc<Mutex<i64>>,
}

impl DatastoreStorage {
    pub fn clean_transaction(&mut self, transaction_id: &str) {
        // clean up the transaction state
        self.transactions.remove(transaction_id);
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

        // Determine the "kind" of the entity for ID generation.
        // This is based on the last PathElement of the original entity key.
        let entity_kind_for_id_gen = match original_key.path.last() {
            Some(pe) => &pe.kind,
            None => {
                // This case would occur if the entity key had no PathElements.
                return Err(tonic::Status::invalid_argument(
                    "The entity key has no path elements to determine the 'kind' for ID generation.",
                ));
            }
        };

        // Calculate the new ID based on the count of entities of the same "kind".
        // This count is done once before the loop, replicating the old logic.
        let count_for_entity_kind = self
            .entities
            .keys()
            .filter(|stored_key_struct| {
                stored_key_struct
                    .path_elements
                    .last()
                    .map_or_else(|| false, |(k, _)| k == entity_kind_for_id_gen)
            })
            .count();
        let new_id_value = count_for_entity_kind as i64 + 1;

        // Apply the new_id_value to any PathElement in the key that is without an ID.
        for path_element in key_with_new_id.path.iter_mut() {
            if path_element.id_type.is_none() {
                path_element.id_type = Some(IdType::Id(new_id_value));
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

        if let Some(k) = &entity_metadata.entity.key {
            dbg!("Inserting entity with key (from DatastoreStorage)", &k.path);
        }

        // Insert into the BTreeMap<KeyStruct, EntityWithMetadata>
        self.entities
            .insert(final_key_struct.clone(), entity_metadata.clone());
        self.update_indexes(&final_key_struct, &db_entity);

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
                            dbg!(
                                "Applying HAS_ANCESTOR filter",
                                &entity_metadata.entity.key,
                                &filter_value
                            );
                            if let Some(ValueType::KeyValue(ancestor_key_value)) =
                                &filter_value.value_type
                            {
                                if let Some(entity_key) = &entity_metadata.entity.key {
                                    dbg!("Entity Key for HAS_ANCESTOR", entity_key);
                                    dbg!("Ancestor Key Value for HAS_ANCESTOR", ancestor_key_value);

                                    let entity_partition_id_obj = entity_key.partition_id.as_ref();
                                    let ancestor_partition_id_obj =
                                        ancestor_key_value.partition_id.as_ref();

                                    let partitions_match = match (
                                        entity_partition_id_obj,
                                        ancestor_partition_id_obj,
                                    ) {
                                        (Some(ep), Some(ap)) => {
                                            ep.project_id == ap.project_id
                                                && ep.namespace_id == ap.namespace_id
                                        }
                                        (None, None) => true,
                                        _ => false,
                                    };
                                    if !partitions_match {
                                        dbg!("HAS_ANCESTOR: Partition mismatch. Returning false.");
                                        return false;
                                    }

                                    if entity_key.path.len() > ancestor_key_value.path.len() {
                                        for (i, ancestor_path_element) in
                                            ancestor_key_value.path.iter().enumerate()
                                        {
                                            let entity_path_element = &entity_key.path[i];
                                            if entity_path_element.kind
                                                != ancestor_path_element.kind
                                                || entity_path_element.id_type
                                                    != ancestor_path_element.id_type
                                            {
                                                dbg!(
                                                    "HAS_ANCESTOR: Path element mismatch.",
                                                    &entity_path_element,
                                                    &ancestor_path_element
                                                );
                                                return false;
                                            }
                                        }
                                        dbg!(
                                            "HAS_ANCESTOR: Path prefix matches and length is greater. Returning true."
                                        );
                                        return true;
                                    } else {
                                        dbg!(
                                            "HAS_ANCESTOR: Path length condition not met.",
                                            entity_key.path.len(),
                                            ancestor_key_value.path.len()
                                        );
                                    }
                                } else {
                                    dbg!("HAS_ANCESTOR: Entity has no key.");
                                }
                            } else {
                                dbg!("HAS_ANCESTOR: Filter value is not a KeyValue.");
                            }
                            dbg!("HAS_ANCESTOR: Defaulting to false.");
                            return false;
                        }
                        // Regular property filters
                        else if let Some(entity_value) =
                            entity_metadata.entity.properties.get(&property.name)
                        {
                            match property_filter.op {
                                0 => return true, // OPERATOR_UNSPECIFIED
                                1 => {
                                    // LESS_THAN
                                    match (&entity_value.value_type, &filter_value.value_type) {
                                        (
                                            Some(ValueType::IntegerValue(ev)),
                                            Some(ValueType::IntegerValue(fv)),
                                        ) => return ev < fv,
                                        (
                                            Some(ValueType::DoubleValue(ev)),
                                            Some(ValueType::DoubleValue(fv)),
                                        ) => return ev < fv,
                                        (
                                            Some(ValueType::StringValue(ev)),
                                            Some(ValueType::StringValue(fv)),
                                        ) => return ev < fv,
                                        (
                                            Some(ValueType::TimestampValue(ev)),
                                            Some(ValueType::TimestampValue(fv)),
                                        ) => {
                                            return ev.seconds < fv.seconds
                                                || (ev.seconds == fv.seconds
                                                    && ev.nanos < fv.nanos);
                                        }
                                        _ => return false, // Type mismatch or unsupported for comparison
                                    }
                                }
                                2 => {
                                    // LESS_THAN_OR_EQUAL
                                    match (&entity_value.value_type, &filter_value.value_type) {
                                        (
                                            Some(ValueType::IntegerValue(ev)),
                                            Some(ValueType::IntegerValue(fv)),
                                        ) => return ev <= fv,
                                        (
                                            Some(ValueType::DoubleValue(ev)),
                                            Some(ValueType::DoubleValue(fv)),
                                        ) => return ev <= fv,
                                        (
                                            Some(ValueType::StringValue(ev)),
                                            Some(ValueType::StringValue(fv)),
                                        ) => return ev <= fv,
                                        (
                                            Some(ValueType::TimestampValue(ev)),
                                            Some(ValueType::TimestampValue(fv)),
                                        ) => {
                                            return ev.seconds < fv.seconds
                                                || (ev.seconds == fv.seconds
                                                    && ev.nanos <= fv.nanos);
                                        }
                                        _ => return false,
                                    }
                                }
                                3 => {
                                    // GREATER_THAN
                                    match (&entity_value.value_type, &filter_value.value_type) {
                                        (
                                            Some(ValueType::IntegerValue(ev)),
                                            Some(ValueType::IntegerValue(fv)),
                                        ) => return ev > fv,
                                        (
                                            Some(ValueType::DoubleValue(ev)),
                                            Some(ValueType::DoubleValue(fv)),
                                        ) => return ev > fv,
                                        (
                                            Some(ValueType::StringValue(ev)),
                                            Some(ValueType::StringValue(fv)),
                                        ) => return ev > fv,
                                        (
                                            Some(ValueType::TimestampValue(ev)),
                                            Some(ValueType::TimestampValue(fv)),
                                        ) => {
                                            return ev.seconds > fv.seconds
                                                || (ev.seconds == fv.seconds
                                                    && ev.nanos > fv.nanos);
                                        }
                                        _ => return false,
                                    }
                                }
                                4 => {
                                    // GREATER_THAN_OR_EQUAL
                                    match (&entity_value.value_type, &filter_value.value_type) {
                                        (
                                            Some(ValueType::IntegerValue(ev)),
                                            Some(ValueType::IntegerValue(fv)),
                                        ) => return ev >= fv,
                                        (
                                            Some(ValueType::DoubleValue(ev)),
                                            Some(ValueType::DoubleValue(fv)),
                                        ) => return ev >= fv,
                                        (
                                            Some(ValueType::StringValue(ev)),
                                            Some(ValueType::StringValue(fv)),
                                        ) => return ev >= fv,
                                        (
                                            Some(ValueType::TimestampValue(ev)),
                                            Some(ValueType::TimestampValue(fv)),
                                        ) => {
                                            return ev.seconds > fv.seconds
                                                || (ev.seconds == fv.seconds
                                                    && ev.nanos >= fv.nanos);
                                        }
                                        _ => return false,
                                    }
                                }
                                5 => return entity_value.value_type == filter_value.value_type, // EQUAL
                                6 => {
                                    /* IN - todo */
                                    return false;
                                } // Defaulting to false for unimplemented IN
                                9 => return entity_value.value_type != filter_value.value_type, // NOT_EQUAL
                                // Case 11 (HAS_ANCESTOR) is handled above
                                13 => {
                                    /* NOT_IN - todo */
                                    return false;
                                } // Defaulting to false for unimplemented NOT_IN
                                _ => return false, // Unsupported operator for property or op combination
                            }
                        } else {
                            // Property not found in entity for regular filters (and not HAS_ANCESTOR)
                            return false;
                        }
                    } else {
                        // Filter value is missing
                        return false;
                    }
                } else {
                    // Property reference is missing
                    return false;
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
                        return filter_results.iter().all(|&result| result);
                    }
                    2 => {
                        // Or we can translate to ANY is true
                        return filter_results.iter().any(|&result| result);
                    }
                    _ => {
                        // OPERATOR_UNSPECIFIED
                        return true;
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

        // Debug print items (optional, can be removed or adjusted)
        // for (k_struct, entity_meta) in self.entities.iter() {
        //     println!("Stored KeyStruct: {:?}, Entity Path: {:?}", k_struct, entity_meta.entity.key.as_ref().map(|k| &k.path));
        // }

        self.entities.get(&key_struct).cloned()
    }

    pub fn get_entities(
        &self,
        project_id_filter: String,
        kind_name: String,
        filter: Option<Filter>,
    ) -> Vec<EntityResult> {
        let mut results = Vec::new();

        // Debug print (optional)
        // println!("Getting entities for kind: {}", kind_name);
        // for (key_s, entity_meta) in self.entities.iter() {
        //     if let Some(k) = &entity_meta.entity.key {
        //         println!("  Checking entity with key: {:?}", k.path);
        //     }
        // }

        for (key_struct, entity_metadata) in self.entities.iter() {
            // Filter by project_id first
            if key_struct.project_id != project_id_filter {
                continue;
            }

            // Check if the last path element's kind matches kind_name
            if key_struct
                .path_elements
                .last()
                .map_or(false, |(k, _)| k == &kind_name)
            {
                dbg!("Kind matches for entity:", &entity_metadata.entity.key);
                let mut passes_all_filters = true;
                if let Some(ref filter_obj) = filter {
                    if let Some(filter_type) = &filter_obj.filter_type {
                        if !DatastoreStorage::apply_filter(entity_metadata, filter_type) {
                            passes_all_filters = false; // Skip if filter doesn't match
                            dbg!("Entity FAILED filter:", &entity_metadata.entity.key);
                        } else {
                            dbg!("Entity PASSED filter:", &entity_metadata.entity.key);
                        }
                    }
                }

                if passes_all_filters {
                    results.push(EntityResult {
                        entity: Some(entity_metadata.entity.clone()),
                        create_time: Some(entity_metadata.create_time.clone()),
                        update_time: Some(entity_metadata.update_time.clone()),
                        cursor: vec![],
                        version: entity_metadata.version as i64,
                    });
                }
            }
        }
        results
    }

    pub fn update_indexes(&mut self, key_struct: &KeyStruct, entity: &Entity) {
        // For each property of an entity, creates an index
        for (prop_name, prop_value) in &entity.properties {
            if let Some(value_type) = &prop_value.value_type {
                // Extract a value as string for indexing
                let value_str = match value_type {
                    crate::google::datastore::v1::value::ValueType::StringValue(s) => s.clone(),
                    crate::google::datastore::v1::value::ValueType::IntegerValue(i) => {
                        i.to_string()
                    }
                    // todo: implement other types
                    _ => continue,
                };

                // Get the kind of the entity
                if let Some((kind, _)) = key_struct.path_elements.last() {
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
            if let Some(value_type) = &prop_value.value_type {
                // Extract a value as string for indexing
                let value_str = match value_type {
                    crate::google::datastore::v1::value::ValueType::StringValue(s) => s.clone(),
                    crate::google::datastore::v1::value::ValueType::IntegerValue(i) => {
                        i.to_string()
                    }
                    // todo: implement other types
                    _ => continue,
                };

                // Get the kind of the entity
                if let Some((kind, _)) = key_struct.path_elements.last() {
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
