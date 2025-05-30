use crate::google::datastore::v1::key::path_element::IdType;

use crate::google::datastore::v1::filter::FilterType;
use crate::google::datastore::v1::value::ValueType;
use crate::google::datastore::v1::Filter;
use std::sync::{Arc, Mutex};

use crate::google::datastore::v1::{
    Entity, EntityResult, Key, Mutation,
};
use std::collections::{BTreeMap, BTreeSet, HashMap};

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub enum KeyId {
    IntId(i64),
    StringId(String),
}

// Custom key structure for efficient indexing
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct KeyStruct {
    pub namespace: String,
    pub path_elements: Vec<(String, KeyId)>, // (kind, id/name)
}

impl KeyStruct {
    pub fn from_datastore_to_string(key: &Key) -> String {
        // this will be used as a key for get entityes

        let mut path_elements = Vec::new();
        for path_element in &key.path {
            let kind = path_element.kind.clone();
            // let id_type = match &path_element.id_type {
            //     Some(IdType::Id(id)) => format!("id: {}", id),
            //     Some(IdType::Name(name)) => format!("name: {}", name),
            //     None => continue,
            // };
            path_elements.push(kind.to_string());
        }
        //return the last string
        path_elements
            .last()
            .map_or_else(|| "".to_string(), |s| s.to_string())
        //path_elements.join(", ").to_string();
    }

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
    // Stores using BTreeMap for ordered storage
    pub entities: BTreeMap<String, Vec<EntityWithMetadata>>,
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

    pub fn apply_filter(entity_metadata: &EntityWithMetadata, filter: &FilterType) -> bool {
        match filter {
            FilterType::PropertyFilter(property_filter) => {
                if let Some(ref property) = property_filter.property {
                    if let Some(ref value) = property_filter.value {
                        // check if the property exists in the entity
                        if let Some(entity_value) =
                            entity_metadata.entity.properties.get(&property.name)
                        {
                            // Comparing the values based on the operator
                            // OPERATOR_UNSPECIFIED = 0;
                            // LESS_THAN = 1;
                            // LESS_THAN_OR_EQUAL = 2;
                            // GREATER_THAN = 3;
                            // GREATER_THAN_OR_EQUAL = 4;
                            // EQUAL = 5;
                            // IN = 6;
                            // NOT_EQUAL = 9;
                            // HAS_ANCESTOR = 11;
                            // NOT_IN = 13;
                            match property_filter.op {
                                0 => {
                                    // OPERATOR_UNSPECIFIED
                                    return true;
                                }
                                1 => {
                                    // LESS_THAN = 1;
                                    match (&entity_value.value_type, &value.value_type) {
                                        (
                                            Some(ValueType::IntegerValue(entity_val)),
                                            Some(ValueType::IntegerValue(filter_val)),
                                        ) => {
                                            return entity_val < filter_val;
                                        }
                                        (
                                            Some(ValueType::DoubleValue(entity_val)),
                                            Some(ValueType::DoubleValue(filter_val)),
                                        ) => {
                                            return entity_val < filter_val;
                                        }
                                        (
                                            Some(ValueType::StringValue(entity_val)),
                                            Some(ValueType::StringValue(filter_val)),
                                        ) => {
                                            return entity_val < filter_val;
                                        }
                                        (
                                            Some(ValueType::TimestampValue(entity_val)),
                                            Some(ValueType::TimestampValue(filter_val)),
                                        ) => {
                                            return entity_val.seconds < filter_val.seconds
                                                || (entity_val.seconds == filter_val.seconds
                                                    && entity_val.nanos < filter_val.nanos);
                                        }
                                        _ => {
                                            return true;
                                        }
                                    }
                                }
                                2 => {
                                    // LESS_THAN_OR_EQUAL = 2;
                                    match (&entity_value.value_type, &value.value_type) {
                                        (
                                            Some(ValueType::IntegerValue(entity_val)),
                                            Some(ValueType::IntegerValue(filter_val)),
                                        ) => {
                                            return entity_val <= filter_val;
                                        }
                                        (
                                            Some(ValueType::DoubleValue(entity_val)),
                                            Some(ValueType::DoubleValue(filter_val)),
                                        ) => {
                                            return entity_val <= filter_val;
                                        }
                                        (
                                            Some(ValueType::StringValue(entity_val)),
                                            Some(ValueType::StringValue(filter_val)),
                                        ) => {
                                            return entity_val <= filter_val;
                                        }
                                        (
                                            Some(ValueType::TimestampValue(entity_val)),
                                            Some(ValueType::TimestampValue(filter_val)),
                                        ) => {
                                            return entity_val.seconds < filter_val.seconds
                                                || (entity_val.seconds == filter_val.seconds
                                                    && entity_val.nanos <= filter_val.nanos);
                                        }
                                        _ => {
                                            return true;
                                        }
                                    }
                                }
                                3 => {
                                    // GREATER_THAN = 3;
                                    match (&entity_value.value_type, &value.value_type) {
                                        (
                                            Some(ValueType::IntegerValue(entity_val)),
                                            Some(ValueType::IntegerValue(filter_val)),
                                        ) => {
                                            return entity_val > filter_val;
                                        }
                                        (
                                            Some(ValueType::DoubleValue(entity_val)),
                                            Some(ValueType::DoubleValue(filter_val)),
                                        ) => {
                                            return entity_val > filter_val;
                                        }
                                        (
                                            Some(ValueType::StringValue(entity_val)),
                                            Some(ValueType::StringValue(filter_val)),
                                        ) => {
                                            return entity_val > filter_val;
                                        }
                                        (
                                            Some(ValueType::TimestampValue(entity_val)),
                                            Some(ValueType::TimestampValue(filter_val)),
                                        ) => {
                                            return entity_val.seconds > filter_val.seconds
                                                || (entity_val.seconds == filter_val.seconds
                                                    && entity_val.nanos > filter_val.nanos);
                                        }
                                        _ => {
                                            return true;
                                        }
                                    }
                                }
                                4 => {
                                    // GREATER_THAN_OR_EQUAL = 4;
                                    match (&entity_value.value_type, &value.value_type) {
                                        (
                                            Some(ValueType::IntegerValue(entity_val)),
                                            Some(ValueType::IntegerValue(filter_val)),
                                        ) => {
                                            return entity_val >= filter_val;
                                        }
                                        (
                                            Some(ValueType::DoubleValue(entity_val)),
                                            Some(ValueType::DoubleValue(filter_val)),
                                        ) => {
                                            return entity_val >= filter_val;
                                        }
                                        (
                                            Some(ValueType::StringValue(entity_val)),
                                            Some(ValueType::StringValue(filter_val)),
                                        ) => {
                                            return entity_val >= filter_val;
                                        }
                                        (
                                            Some(ValueType::TimestampValue(entity_val)),
                                            Some(ValueType::TimestampValue(filter_val)),
                                        ) => {
                                            return entity_val.seconds > filter_val.seconds
                                                || (entity_val.seconds == filter_val.seconds
                                                    && entity_val.nanos >= filter_val.nanos);
                                        }
                                        _ => {
                                            return true;
                                        }
                                    }
                                }
                                5 => {
                                    // EQUAL = 5;
                                    return entity_value.value_type == value.value_type;
                                }
                                6 => { // IN = 6;
                                     // todo: implement this
                                }
                                9 => {
                                    // NOT_EQUAL = 9;
                                    return entity_value.value_type != value.value_type;
                                }
                                11 => {
                                    // HAS_ANCESTOR = 11;
                                    dbg!("Has ancestor filter", &entity_value, &property_filter);
                                    return true;
                                }
                                13 => {
                                    // NOT_IN = 13;
                                    //  todo: implement this
                                    return true;
                                }
                                _ => {
                                    // Unsupported operator
                                    return true;
                                }
                            }
                        }
                    }
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
        true
    }

    pub fn get_entity(&self, key: &Key) -> Option<EntityWithMetadata> {
        // Search for the entity in the storage
        let key_as_string = KeyStruct::from_datastore_to_string(key);
        // debug print items
        for (db_name, entities) in self.entities.iter() {
            println!("db_name: {}", db_name);
            for entity in entities.iter() {
                if let Some(ref key) = entity.entity.key {
                    println!("entity: {:?}", key.path);
                }
            }
        }
        if let Some(entities) = self.entities.get(&key_as_string) {
            if entities.is_empty() {
                return None;
            } else {
                for entity in entities.iter() {
                    if entity.entity.key == Some(key.clone()) {
                        return Some(entity.clone());
                    }
                }
            }
        } else {
            return None;
        }
        None
    }

    pub fn get_entities(&self, key_db: String, filter: Option<Filter>) -> Vec<EntityResult> {
        // Search for the entity in the storage
        let mut results = Vec::new();
        for (db_name, entities) in self.entities.iter() {
            println!("db_name: {}", db_name);
            for entity in entities.iter() {
                if let Some(ref key) = entity.entity.key {
                    println!("entity: {:?}", key.path);
                }
            }
        }
        if let Some(entities) = self.entities.get(&key_db) {
            for entity in entities.iter() {
                if let Some(ref filter) = filter {
                    if let Some(filter_type) = &filter.filter_type {
                        // Check if the entity matches the filter
                        if !DatastoreStorage::apply_filter(entity, filter_type) {
                            continue; // Skip this entity if it doesn't match the filter
                        }
                    }
                    results.push(EntityResult {
                        entity: Some(entity.entity.clone()),
                        create_time: Some(entity.create_time.clone()),
                        update_time: Some(entity.update_time.clone()),
                        cursor: vec![],
                        version: entity.version as i64,
                    });
                } else {
                    results.push(EntityResult {
                        entity: Some(entity.entity.clone()),
                        create_time: Some(entity.create_time.clone()),
                        update_time: Some(entity.update_time.clone()),
                        cursor: vec![],
                        version: entity.version as i64,
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
}
