use google::datastore::v1::commit_request::TransactionSelector;
use google::datastore::v1::filter::FilterType;
use google::datastore::v1::key::path_element::IdType;
use google::datastore::v1::key::PathElement;
use google::datastore::v1::mutation::Operation;
use google::datastore::v1::value::ValueType;
use google::datastore::v1::Filter;
use prost_types::value::Kind;
use prost_types::{Duration, Struct, Value as ValueProps};
use std::collections::{BTreeMap, BTreeSet, HashMap};

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;
use tonic::{transport::Server, Request, Response, Status};

// Import the generated code
pub mod google {
    pub mod datastore {
        pub mod v1 {
            tonic::include_proto!("google.datastore.v1");
        }
    }
}

use google::datastore::v1::aggregation_query::aggregation::Operator as AggregationOperator;
use google::datastore::v1::datastore_server::{Datastore as DatastoreService, DatastoreServer};
use google::datastore::v1::{
    AggregationResultBatch, AllocateIdsRequest, AllocateIdsResponse,
    BeginTransactionRequest, BeginTransactionResponse, CommitRequest, CommitResponse, Entity,
    EntityResult, ExecutionStats, ExplainMetrics, Key, LookupRequest, LookupResponse, Mutation,
    PingRequest, PingResponse, PlanSummary, PropertyReference, 
    ReserveIdsRequest, ReserveIdsResponse, RollbackRequest, RollbackResponse,
    RunAggregationQueryRequest, RunAggregationQueryResponse, RunQueryRequest, RunQueryResponse,
};

#[derive(Default, Debug)]
struct DatastoreStorage {
    // Armazenamento principal usando BTreeMap para entidades
    entities: BTreeMap<String, Vec<EntityWithMetadata>>,
    // Índices para consultas eficientes
    indexes: HashMap<(String, String, String), BTreeSet<KeyStruct>>,
    // Transações ativas
    transactions: HashMap<String, TransactionState>,
    // Contador para geração de IDs automáticos
    id_counter: Arc<Mutex<i64>>,
}

impl DatastoreStorage {
    fn clean_transaction(&mut self, transaction_id: &str) {
        // Limpar transações
        self.transactions.remove(transaction_id);
    }

    fn get_entity(&self, key: &Key) -> Option<EntityWithMetadata> {
        // Search for the entity in the storage
        let key_as_string = KeyStruct::from_datastore_to_string(key);
        if let Some(entities) = self.entities.get(&key_as_string) {
            if entities.is_empty() {
                return None;
            }else{
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

    fn update_indexes(&mut self, key_struct: &KeyStruct, entity: &Entity) {
        // Para cada propriedade da entidade, criar índices
        for (prop_name, prop_value) in &entity.properties {
            if let Some(value_type) = &prop_value.value_type {
                // Extrair valor como string para indexação
                let value_str = match value_type {
                    google::datastore::v1::value::ValueType::StringValue(s) => s.clone(),
                    google::datastore::v1::value::ValueType::IntegerValue(i) => i.to_string(),
                    // Implementar outros tipos conforme necessário
                    _ => continue,
                };

                // Obter o kind da entidade
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

// Entidade com metadados
#[derive(Default, Debug, Clone)]
struct EntityWithMetadata {
    entity: Entity,
    version: u64,
    create_time: prost_types::Timestamp,
    update_time: prost_types::Timestamp,
}

// Estrutura chave personalizada para indexação eficiente
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
struct KeyStruct {
    namespace: String,
    path_elements: Vec<(String, KeyId)>, // (kind, id/name)
}

impl KeyStruct {
    fn from_datastore_to_string(key: &Key) -> String {
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
        path_elements.join(", ").to_string()
    }

    fn from_datastore_key(key: &Key) -> Self {
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

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
enum KeyId {
    IntId(i64),
    StringId(String),
}

// Estado de uma transação
#[derive(Default, Debug)]
struct TransactionState {
    mutations: Vec<Mutation>,
    snapshot: HashMap<KeyStruct, EntityWithMetadata>,
    timestamp: prost_types::Timestamp,
    read_only: bool,
}

#[derive(Debug)]
struct DatastoreEmulator {
    storage: Arc<Mutex<DatastoreStorage>>,
}

impl Default for DatastoreEmulator {
    fn default() -> Self {
        Self {
            storage: Arc::new(Mutex::new(DatastoreStorage::default())),
        }
    }
}

fn apply_filter(entity_metadata: &EntityWithMetadata, filter: &FilterType) -> bool {
    match filter {
        FilterType::PropertyFilter(property_filter) => {
            if let Some(ref property) = property_filter.property {
                if let Some(ref value) = property_filter.value {
                    // check if the property exists in the entity
                    if let Some(entity_value) =
                        entity_metadata.entity.properties.get(&property.name)
                    {
                        // Comparar os valores com base no operador
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
                        //
                        // TYPES
                        // NullValue(i32),
                        // #[prost(bool, tag = "1")]
                        // BooleanValue(bool),
                        // #[prost(int64, tag = "2")]
                        // IntegerValue(i64),
                        // #[prost(double, tag = "3")]
                        // DoubleValue(f64),
                        // #[prost(message, tag = "10")]
                        // TimestampValue(::prost_types::Timestamp),
                        // #[prost(message, tag = "5")]
                        // KeyValue(super::Key),
                        // #[prost(string, tag = "17")]
                        // StringValue(::prost::alloc::string::String),
                        // #[prost(bytes, tag = "18")]
                        // BlobValue(::prost::alloc::vec::Vec<u8>),
                        // #[prost(message, tag = "8")]
                        // GeoPointValue(super::LatLng),
                        // #[prost(message, tag = "6")]
                        // EntityValue(super::Entity),
                        // #[prost(message, tag = "9")]
                        // ArrayValue(super::ArrayValue),
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
                                // todo: implement this
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
                    filter_results.push(apply_filter(entity_metadata, &filter_type));
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

#[tonic::async_trait]
impl DatastoreService for DatastoreEmulator {
    async fn ping(&self, request: Request<PingRequest>) -> Result<Response<PingResponse>, Status> {
        let req = request.into_inner();
        let now = SystemTime::now();
        let timestamp = prost_types::Timestamp {
            seconds: now
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as i64,
            nanos: 0,
        };

        let response = PingResponse {
            message: format!("Hello {}! Datastore emulator is running.", req.message),
            server_time: Some(timestamp),
        };

        Ok(Response::new(response))
    }

    async fn lookup(
        &self,
        request: Request<LookupRequest>,
    ) -> Result<Response<LookupResponse>, Status> {
        let req = request.into_inner();
        let storage = self.storage.lock().unwrap();
        let mut found = Vec::new();

        // process each key in the request
        for key in &req.keys {
            let result_entity = storage.get_entity(key);
            if let Some(entity) = result_entity {
                    found.push(EntityResult {
                        entity: Some(entity.entity.clone()),
                        create_time: Some(entity.create_time.clone()),
                        update_time: Some(entity.update_time.clone()),
                        cursor: vec![],
                        version: entity.version as i64,
                    });
            }
        }

        // Get current time for read_time
        let now = SystemTime::now();
        let read_time = prost_types::Timestamp {
            seconds: now
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as i64,
            nanos: 0,
        };

        Ok(Response::new(LookupResponse {
            found,
            missing: vec![], // do we really need to check for missing keys?
            deferred: vec![],
            transaction: Vec::new(),
            read_time: Some(read_time),
        }))
    }

    async fn run_query(
        &self,
        request: Request<RunQueryRequest>,
    ) -> Result<Response<RunQueryResponse>, Status> {
        let req = request.into_inner();
        let storage = self.storage.lock().unwrap();
        let mut results = Vec::new();
        // Extract the aggregation query from the request
        let aggregation_query = match req.query_type {
            Some(google::datastore::v1::run_query_request::QueryType::Query(query)) => query,
            _ => {
                // implement other query types if needed
                return Err(Status::invalid_argument(
                    "Missing or invalid aggregation query",
                ));
            }
        };

        let kind_name = aggregation_query.kind[0].name.clone();
        if let Some(entities) = storage.entities.get(&kind_name) {
            for entity in entities {
                // Check if the entity matches the filter
                if let Some(ref filter) = aggregation_query.filter {
                    if let Some(filter_type) = &filter.filter_type {
                        if !apply_filter(entity, filter_type) {
                            continue; // Skip this entity if it doesn't match the filter
                        }
                    }
                }

                // Add the entity to the results
                results.push(EntityResult {
                    entity: Some(entity.entity.clone()),
                    create_time: Some(entity.create_time.clone()),
                    update_time: Some(entity.update_time.clone()),
                    cursor: vec![],
                    version: entity.version as i64,
                });
            }
        }

        let batch = google::datastore::v1::QueryResultBatch {
            entity_result_type: 1,
            skipped_results: 0,
            read_time: None,
            skipped_cursor: vec![],
            snapshot_version: 0,
            entity_results: results,
            more_results: 0, // NO_MORE_RESULTS
            end_cursor: Vec::new(),
        };

        let mut fields = BTreeMap::new();
        fields.insert(
            "Some key".to_string(),
            ValueProps {
                kind: Some(Kind::StringValue("Some value".to_string())),
            },
        );
        let debug_stats = Struct {
            fields: fields.clone(),
        };
        Ok(Response::new(RunQueryResponse {
            transaction: vec![1],
            query: Some(aggregation_query),
            batch: Some(batch),
            // todo: create real metrics
            explain_metrics: Some(ExplainMetrics {
                plan_summary: Some(PlanSummary {
                    indexes_used: vec![Struct {
                        fields: fields.clone(),
                    }],
                }),
                execution_stats: Some(ExecutionStats {
                    results_returned: 10,
                    execution_duration: Some(Duration {
                        seconds: 10,
                        nanos: 0,
                    }),
                    read_operations: 10,
                    debug_stats: Some(debug_stats),
                }),
            }),
        }))
    }

    async fn commit(
        &self,
        request: Request<CommitRequest>,
    ) -> Result<Response<CommitResponse>, Status> {
        let req = request.into_inner();
        let mut storage = self.storage.lock().unwrap();
        let mut mutation_results = Vec::new();

        // Handle transaction if present
        let transaction_id = if let Some(transaction_selector) = req.transaction_selector {
            match transaction_selector {
                TransactionSelector::Transaction(tx_bytes) => {
                    // Convert transaction ID bytes to string
                    match String::from_utf8(tx_bytes.clone()) {
                        Ok(id) => Some(id),
                        Err(_) => {
                            return Err(Status::invalid_argument("Invalid transaction ID format"));
                        }
                    }
                }
                TransactionSelector::SingleUseTransaction(_) => None, // No existing transaction, just commit directly
            }
        } else {
            None // No transaction specified, just commit directly
        };

        // Check if this is a transactional commit
        if let Some(tx_id) = transaction_id.clone() {
            // Verify the transaction exists and is not read-only
            if let Some(tx_state) = storage.transactions.get(&tx_id) {
                if tx_state.read_only {
                    return Err(Status::failed_precondition(
                        "Cannot commit mutations in a read-only transaction",
                    ));
                }

                // In a real implementation, we would apply the transaction's mutations here
                // For now, we'll just remove the transaction after committing
                println!("Committing transaction: {}", tx_id);
            } else {
                return Err(Status::not_found(format!(
                    "Transaction {} not found",
                    tx_id
                )));
            }
        }

        // Process mutations
        for mutation in req.mutations {
            if let Some(ref mutation) = mutation.operation {
                match mutation {
                    Operation::Insert(entity) => {
                        let key = match entity.key {
                            Some(ref key) => key.clone(),
                            None => return Err(Status::invalid_argument("Entity missing key")),
                        };
                        let key_struct = KeyStruct::from_datastore_key(&key);
                        let key_as_string = KeyStruct::from_datastore_to_string(&key);

                        let now = SystemTime::now();
                        let timestamp = prost_types::Timestamp {
                            seconds: now
                                .duration_since(SystemTime::UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_secs() as i64,
                            nanos: 0,
                        };

                        let entity_metadata = EntityWithMetadata {
                            entity: entity.clone(),
                            version: 1,
                            create_time: timestamp.clone(),
                            update_time: timestamp,
                        };

                        let entity_list = storage.entities.entry(key_as_string).or_default();
                        entity_list.push(entity_metadata);

                        storage.update_indexes(&key_struct, entity);

                        mutation_results.push(google::datastore::v1::MutationResult {
                            key: Some(key),
                            ..Default::default()
                        });
                    }
                    Operation::Update(entity) => {
                        let key = match entity.key {
                            Some(ref key) => key.clone(),
                            None => return Err(Status::invalid_argument("Entity missing key")),
                        };
                        let key_as_string = KeyStruct::from_datastore_to_string(&key);

                        if let Some(entity_list) = storage.entities.get_mut(&key_as_string) {
                            for entity_metadata in entity_list {
                                entity_metadata.entity = entity.clone();
                                entity_metadata.update_time = prost_types::Timestamp {
                                    seconds: SystemTime::now()
                                        .duration_since(SystemTime::UNIX_EPOCH)
                                        .unwrap_or_default()
                                        .as_secs()
                                        as i64,
                                    nanos: 0,
                                };
                            }
                            mutation_results.push(google::datastore::v1::MutationResult {
                                key: Some(key),
                                ..Default::default()
                            });
                        } else {
                            // do we really need return 404,
                            // todo: check doc about not found items
                            //return Err(Status::not_found("Entity not found"));
                        }
                    }
                    Operation::Upsert(entity) => {
                        let key = match entity.key {
                            Some(ref key) => key.clone(),
                            None => return Err(Status::invalid_argument("Entity missing key")),
                        };
                        let key_struct = KeyStruct::from_datastore_key(&key);
                        let key_as_string = KeyStruct::from_datastore_to_string(&key);

                        let now = SystemTime::now();
                        let timestamp = prost_types::Timestamp {
                            seconds: now
                                .duration_since(SystemTime::UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_secs() as i64,
                            nanos: 0,
                        };

                        let entity_metadata = EntityWithMetadata {
                            entity: entity.clone(),
                            version: 1,
                            create_time: timestamp.clone(),
                            update_time: timestamp,
                        };

                        let entity_list = storage.entities.entry(key_as_string).or_default();
                        entity_list.push(entity_metadata);

                        storage.update_indexes(&key_struct, entity);

                        mutation_results.push(google::datastore::v1::MutationResult {
                            key: Some(key),
                            ..Default::default()
                        });
                    }
                    Operation::Delete(key) => {
                        let key_as_string = KeyStruct::from_datastore_to_string(key);
                        if let Some(entity_list) = storage.entities.get_mut(&key_as_string) {
                            // Remove the entity from the list
                            let removed_itens = entity_list
                                .extract_if(.., |entity| entity.entity.key == Some(key.clone()))
                                .collect::<Vec<_>>();
                            if removed_itens.is_empty() {
                                // do we really need return 404,
                                // todo: check doc about not found items
                                //return Err(Status::not_found("Entity not found"));
                            }
                            for entity_metadata in removed_itens {
                                // Clean up indexes if needed
                                let key_struct = KeyStruct::from_datastore_key(key);
                                storage.update_indexes(&key_struct, &entity_metadata.entity);
                                mutation_results.push(google::datastore::v1::MutationResult {
                                    key: Some(key.clone()),
                                });
                            }
                        } else {
                            // do we really need return 404,
                            // todo: check doc about not found items
                            //return Err(Status::not_found("Entity not found"));
                        }
                    }
                }
            } else {
                //  What should return if there is not mutation?
            }
        }

        let index_updates = mutation_results.len() as i32;
        // Clean up the transaction if it was used
        if let Some(tx_id) = transaction_id {
            storage.clean_transaction(&tx_id);
        }

        // Get current time for the commit timestamp
        let now = SystemTime::now();
        let commit_timestamp = prost_types::Timestamp {
            seconds: now
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as i64,
            nanos: 0,
        };

        Ok(Response::new(CommitResponse {
            mutation_results,
            index_updates,
            commit_time: Some(commit_timestamp),
        }))
    }

    async fn begin_transaction(
        &self,
        request: Request<BeginTransactionRequest>,
    ) -> Result<Response<BeginTransactionResponse>, Status> {
        let req = request.into_inner();

        // Get current time for the transaction
        let now = SystemTime::now();
        let duration_since_epoch = now
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default();

        let timestamp = prost_types::Timestamp {
            seconds: duration_since_epoch.as_secs() as i64,
            nanos: 0,
        };

        // Generate a unique transaction ID and create transaction state
        let transaction_id;
        {
            let mut storage = self.storage.lock().unwrap();

            // Generate transaction ID using timestamp and current counter
            let counter = {
                let mut counter_guard = storage.id_counter.lock().unwrap();
                let current = *counter_guard;
                *counter_guard += 1;
                current
            };
            transaction_id = format!("tx-{}-{}", timestamp.seconds, counter);

            // Check if the transaction is read-only
            let transaction_options = req.transaction_options.unwrap_or_default();
            let read_only = if let Some(mode) = transaction_options.mode {
                // Set read-only flag
                match mode {
                    google::datastore::v1::transaction_options::Mode::ReadOnly(_) => true,
                    google::datastore::v1::transaction_options::Mode::ReadWrite(_) => false,
                }
            } else {
                // Default to read-write if no mode is specified
                false
            };
            // Create a new transaction state
            let transaction_state = TransactionState {
                mutations: Vec::new(),
                snapshot: HashMap::new(), // Will be populated for read operations
                timestamp: timestamp.clone(),
                read_only,
            };

            // Add the transaction to storage
            storage
                .transactions
                .insert(transaction_id.clone(), transaction_state);
        }

        // Return the transaction ID as bytes
        let transaction_bytes = transaction_id.into_bytes();

        Ok(Response::new(BeginTransactionResponse {
            transaction: transaction_bytes,
        }))
    }

    async fn rollback(
        &self,
        request: Request<RollbackRequest>,
    ) -> Result<Response<RollbackResponse>, Status> {
        let req = request.into_inner();

        // Convert transaction ID bytes to string
        let transaction_id = match String::from_utf8(req.transaction.clone()) {
            Ok(id) => id,
            Err(_) => return Err(Status::invalid_argument("Invalid transaction ID format")),
        };

        // Remove the transaction and any pending changes from storage
        {
            let mut storage = self.storage.lock().unwrap();

            // Check if the transaction exists
            if storage.transactions.contains_key(&transaction_id) {
                // Clean up the transaction (removes it from storage)
                storage.clean_transaction(&transaction_id);
                println!("Transaction {} rolled back successfully", transaction_id);
            } else {
                println!(
                    "Warning: Attempted to rollback non-existent transaction: {}",
                    transaction_id
                );
                // We still return success even if transaction doesn't exist
                // This matches Datastore behavior which is idempotent for rollbacks
            }
        }

        Ok(Response::new(RollbackResponse {}))
    }

    async fn allocate_ids(
        &self,
        request: Request<AllocateIdsRequest>,
    ) -> Result<Response<AllocateIdsResponse>, Status> {
        let req = request.into_inner();
        let storage = self.storage.lock().unwrap();
        let mut allocated_keys = Vec::new();

        // Process each incomplete key in the request
        for incomplete_key in req.keys {
            // Verify the key is incomplete (missing ID or name)
            if incomplete_key.path.is_empty() {
                return Err(Status::invalid_argument("Key path cannot be empty"));
            }

            // Create a new key with the same structure but with allocated IDs
            let mut new_key = incomplete_key.clone();
            let mut path_elements = Vec::new();

            for path_element in incomplete_key.path {
                // Check if this path element needs an ID
                if path_element.id_type.is_none() {
                    // Allocate a new ID
                    let new_id = {
                        let mut counter = storage.id_counter.lock().unwrap();
                        *counter += 1;
                        *counter
                    };

                    // Create a new path element with the allocated ID
                    let new_path_element = PathElement {
                        kind: path_element.kind,
                        id_type: Some(IdType::Id(new_id)),
                    };

                    path_elements.push(new_path_element);
                } else {
                    // This path element already has an ID or name, keep it as is
                    path_elements.push(path_element);
                }
            }

            // Update the key with the new path elements
            new_key.path = path_elements;

            // Add the allocated key to the result
            allocated_keys.push(new_key);
        }

        Ok(Response::new(AllocateIdsResponse {
            keys: allocated_keys,
        }))
    }

    async fn reserve_ids(
        &self,
        request: Request<ReserveIdsRequest>,
    ) -> Result<Response<ReserveIdsResponse>, Status> {
        let req = request.into_inner();
        let storage = self.storage.lock().unwrap();

        // Process each key in the request
        for key in &req.keys {
            // Validate the key
            if key.path.is_empty() {
                return Err(Status::invalid_argument("Key path cannot be empty"));
            }

            // For each path element with an ID, reserve that ID
            for path_element in &key.path {
                if let Some(IdType::Id(id)) = path_element.id_type {
                    // Reserve this ID by ensuring our ID counter is greater than it
                    let mut counter = storage.id_counter.lock().unwrap();
                    if *counter <= id {
                        *counter = id + 1;
                    }
                }
            }

            // We could store reserved keys in a separate collection if needed
            // For now, we just ensure the ID counter is updated
        }

        // Return success response
        Ok(Response::new(ReserveIdsResponse {}))
    }

    async fn run_aggregation_query(
        &self,
        request: Request<RunAggregationQueryRequest>,
    ) -> Result<Response<RunAggregationQueryResponse>, Status> {
        let req = request.into_inner();
        let storage = self.storage.lock().unwrap();

        // Extract the aggregation query from the request
        let aggregation_query = match req.query_type {
            Some(
                google::datastore::v1::run_aggregation_query_request::QueryType::AggregationQuery(
                    query,
                ),
            ) => query,
            _ => {
                return Err(Status::invalid_argument(
                    "Missing or invalid aggregation query",
                ));
            }
        };

        // Get the base query if it exists
        let base_query = match aggregation_query.clone().query_type {
            Some(google::datastore::v1::aggregation_query::QueryType::NestedQuery(query)) => {
                Some(query)
            }
            _ => {
                // todo: Handle other query types if needed
                None
            }
        };

        // Get current time for read_time
        let now = SystemTime::now();
        let read_time = prost_types::Timestamp {
            seconds: now
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as i64,
            nanos: 0,
        };

        // Process each aggregation
        let mut aggregation_results = Vec::new();
        let mut matching_entities = Vec::new();

        if let Some(query) = &base_query {
            // Get entities matching the kind filter
            let filters = query
                .filter
                .as_ref()
                .unwrap_or(&Filter { filter_type: None });
            for kind in &query.kind {
                // Find all entities of this kind
                let values_from_kind = storage.entities.get(&kind.name);
                if let Some(values_from_kind) = values_from_kind {
                    for entity_metadata in values_from_kind {
                        // Check if the entity is found
                        if entity_metadata.entity.key.is_none() {
                            // Corrupted data? how to handle this?
                            return Err(Status::not_found("Entity not found"));
                        }
                        // Apply filters if present
                        if let Some(filter) = &filters.filter_type {
                            if apply_filter(entity_metadata, filter) {
                                matching_entities.push(entity_metadata);
                            }
                        } else {
                            matching_entities.push(entity_metadata);
                        }
                    }
                } else {
                    //there is no kind in the storage
                }
            }
        } else {
            // todo: what we should do if there is no base query?
        }

        for aggregation in &aggregation_query.aggregations {
            if let Some(aggregation_operator) = &aggregation.operator {
                // Check if the aggregation operator is supported
                match aggregation_operator {
                    AggregationOperator::Count(_count) => {
                        let count_value = matching_entities.len() as i64;
                        // Create the aggregation result
                        let result_value = google::datastore::v1::Value {
                            exclude_from_indexes: false,
                            meaning: 0,
                            value_type: Some(
                                google::datastore::v1::value::ValueType::IntegerValue(count_value),
                            ),
                        };

                        aggregation_results.push(google::datastore::v1::AggregationResult {
                            //alias: aggregation.alias.clone(),
                            aggregate_properties: {
                                let mut props = HashMap::new();
                                props.insert(aggregation.alias.clone(), result_value);
                                props
                            },
                        });
                    }
                    AggregationOperator::Sum(sum) => {
                        // Implement SUM aggregation
                        let mut sum_value = 0.0;

                        // Calculate the sum for the specified property
                        for entity_metadata in &matching_entities {
                            if let Some(property) = entity_metadata.entity.properties.get(
                                &<std::option::Option<PropertyReference> as Clone>::clone(
                                    &sum.property,
                                )
                                .unwrap()
                                .name,
                            ) {
                                if let Some(
                                    google::datastore::v1::value::ValueType::IntegerValue(value),
                                ) = &property.value_type
                                {
                                    sum_value += *value as f64;
                                }
                            }
                        }

                        // Create the aggregation result
                        let result_value = google::datastore::v1::Value {
                            exclude_from_indexes: false,
                            meaning: 0,
                            value_type: Some(google::datastore::v1::value::ValueType::DoubleValue(
                                sum_value,
                            )),
                        };

                        aggregation_results.push(google::datastore::v1::AggregationResult {
                            aggregate_properties: {
                                let mut props = HashMap::new();
                                props.insert(aggregation.alias.clone(), result_value);
                                props
                            },
                        });
                    }
                    AggregationOperator::Avg(avg) => {
                        // Implement AVERAGE aggregation
                        let mut sum_value = 0.0;
                        let mut count = 0;

                        // Calculate the sum and count for the average
                        for entity_metadata in &matching_entities {
                            if let Some(property) = entity_metadata.entity.properties.get(
                                &<std::option::Option<PropertyReference> as Clone>::clone(
                                    &avg.property,
                                )
                                .unwrap()
                                .name,
                            ) {
                                if let Some(
                                    google::datastore::v1::value::ValueType::IntegerValue(value),
                                ) = &property.value_type
                                {
                                    sum_value += *value as f64;
                                    count += 1;
                                }
                            }
                        }

                        // Calculate the average
                        let average_value = if count > 0 {
                            sum_value / count as f64
                        } else {
                            0.0
                        };

                        // Create the aggregation result
                        let result_value = google::datastore::v1::Value {
                            exclude_from_indexes: false,
                            meaning: 0,
                            value_type: Some(google::datastore::v1::value::ValueType::DoubleValue(
                                average_value,
                            )),
                        };

                        aggregation_results.push(google::datastore::v1::AggregationResult {
                            aggregate_properties: {
                                let mut props = HashMap::new();
                                props.insert(aggregation.alias.clone(), result_value);
                                props
                            },
                        });
                    }
                }
            }
        }

        // Create the result batch
        let batch = AggregationResultBatch {
            aggregation_results,
            more_results: 3, // NO_MORE_RESULTS
            read_time: Some(read_time.clone()),
        };
        let total_results = batch.aggregation_results.len() as i64;
        // Create execution metrics
        let mut fields = BTreeMap::new();
        fields.insert(
            "query_type".to_string(),
            ValueProps {
                kind: Some(Kind::StringValue("aggregation".to_string())),
            },
        );
        let debug_stats = Struct {
            fields: fields.clone(),
        };

        Ok(Response::new(RunAggregationQueryResponse {
            batch: Some(batch),
            query: Some(aggregation_query),
            transaction: Vec::new(),
            explain_metrics: Some(ExplainMetrics {
                plan_summary: Some(PlanSummary {
                    indexes_used: vec![],
                }),
                execution_stats: Some(ExecutionStats {
                    results_returned: total_results,
                    execution_duration: Some(Duration {
                        seconds: 0,
                        nanos: 1000000, // 1ms fake data
                    }),
                    read_operations: storage.entities.len() as i64,
                    debug_stats: Some(debug_stats),
                }),
            }),
        }))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let addr = SocketAddr::from(([127, 0, 0, 1], 8080));
    let emulator = DatastoreEmulator::default();

    println!("Datastore emulator listening on {}", addr);

    Server::builder()
        .add_service(DatastoreServer::new(emulator))
        .serve(addr)
        .await?;

    Ok(())
}
