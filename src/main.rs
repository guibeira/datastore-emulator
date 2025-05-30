use google::datastore::v1::commit_request::TransactionSelector;
use google::datastore::v1::key::path_element::IdType;
use google::datastore::v1::key::PathElement;
use google::datastore::v1::mutation::Operation;
use google::datastore::v1::Filter;
use prost_types::value::Kind;
use prost_types::{Duration, Struct, Value as ValueProps};
use std::collections::{BTreeMap, HashMap};

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;
use tonic::{transport::Server, Request, Response, Status};

pub mod database;
use database::{DatastoreStorage, EntityWithMetadata, KeyStruct, TransactionState};
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
    AggregationResultBatch, AllocateIdsRequest, AllocateIdsResponse, BeginTransactionRequest,
    BeginTransactionResponse, CommitRequest, CommitResponse, EntityResult, ExecutionStats,
    ExplainMetrics, LookupRequest, LookupResponse, PingRequest, PingResponse,
    PlanSummary, PropertyReference, ReserveIdsRequest, ReserveIdsResponse, RollbackRequest,
    RollbackResponse, RunAggregationQueryRequest, RunAggregationQueryResponse, RunQueryRequest,
    RunQueryResponse,
};

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

#[tonic::async_trait]
impl DatastoreService for DatastoreEmulator {
    async fn ping(&self, request: Request<PingRequest>) -> Result<Response<PingResponse>, Status> {
        let req = request.into_inner();
        let timestamp = prost_types::Timestamp {
            seconds: 0,
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
        let start_time = SystemTime::now();
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
        let end_time = SystemTime::now();
        let total_time_duration = end_time.duration_since(start_time).unwrap_or_default();

        let read_time = prost_types::Timestamp {
            seconds: total_time_duration.as_secs() as i64,
            nanos: total_time_duration.as_nanos() as i32,
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
        let start_time = SystemTime::now();
        let req = request.into_inner();
        let storage = self.storage.lock().unwrap();
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

        let results = storage.get_entities(kind_name, aggregation_query.filter.clone());
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

        let amount_results = batch.entity_results.len() as i64;

        fields.insert(
            "Some key".to_string(),
            ValueProps {
                kind: Some(Kind::StringValue("Some value".to_string())),
            },
        );
        let debug_stats = Struct {
            fields: fields.clone(),
        };

        let end_time = SystemTime::now();
        let total_time_duration = end_time
            .duration_since(start_time)
            .expect("Clock may have gone backwards");

        Ok(Response::new(RunQueryResponse {
            transaction: vec![],
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
                    results_returned: amount_results,
                    execution_duration: Some(Duration {
                        seconds: total_time_duration.as_secs() as i64,
                        nanos: total_time_duration.as_nanos() as i32,
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
        let start_time = SystemTime::now();
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
            if let Some(_tx_state) = storage.transactions.get(&tx_id) {
                // if tx_state.read_only {
                //     return Err(Status::failed_precondition(
                //         "Cannot commit mutations in a read-only transaction blabla",
                //     ));
                // }

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
                        dbg!("Inserting entity", &entity);
                        let key = match entity.key {
                            Some(ref key) => key.clone(),
                            None => return Err(Status::invalid_argument("Entity missing key")),
                        };
                        let key_struct = KeyStruct::from_datastore_key(&key);
                        let key_as_string = KeyStruct::from_datastore_to_string(&key);

                        let timestamp = prost_types::Timestamp {
                            seconds: 0,
                            nanos: 10,
                        };

                        let entity_list = storage.entities.entry(key_as_string).or_default();
                        let mut db_entity = entity.clone();
                        let mut clone_key = key.clone();
                        for path in clone_key.path.iter_mut() {
                            // Check if the path element has an ID
                            if path.id_type.is_none() {
                                // Generate a new ID for the entity
                                let new_id = entity_list.len() as i64 + 1; // Simple ID generation
                                                                           // logic
                                path.id_type = Some(IdType::Id(new_id));
                            }
                        }
                        db_entity.key = Some(clone_key.clone());
                        let entity_metadata = EntityWithMetadata {
                            entity: db_entity.clone(),
                            version: 1,
                            create_time: timestamp.clone(),
                            update_time: timestamp,
                        };

                        if let Some(key) = &entity_metadata.entity.key {
                            dbg!("Inserting entity", &key.path);
                        };
                        entity_list.push(entity_metadata);

                        storage.update_indexes(&key_struct, entity);

                        // todo: return real data instead fake ones
                        let timestamp = prost_types::Timestamp {
                            seconds: 0,
                            nanos: 0,
                        };

                        mutation_results.push(google::datastore::v1::MutationResult {
                            key: Some(clone_key),
                            version: 1,
                            create_time: Some(timestamp.clone()),
                            update_time: Some(timestamp),
                            conflict_detected: false,
                            transform_results: vec![],
                        });
                    }
                    Operation::Update(entity) => {
                        dbg!("Updating entity", &entity);
                        let key = match entity.key {
                            Some(ref key) => key.clone(),
                            None => return Err(Status::invalid_argument("Entity missing key")),
                        };
                        let key_as_string = KeyStruct::from_datastore_to_string(&key);

                        if let Some(entity_db) = storage.get_entity(&key) {
                            if let Some(key) = &&entity_db.entity.key {
                                dbg!("Updating entity", &key.path);
                            };
                            // Update the entity in the storage
                            let mut entity_metadata = entity_db.clone();
                            entity_metadata.entity = entity.clone();
                            entity_metadata.update_time = prost_types::Timestamp {
                                seconds: 0,
                                nanos: 0,
                            };
                            // Update the entity in the list
                            let updated_entity = EntityWithMetadata {
                                entity: entity.clone(),
                                version: entity_metadata.version + 1,
                                // todo: return real data instead fake ones
                                create_time: entity_metadata.create_time.clone(),
                                update_time: prost_types::Timestamp {
                                    seconds: 0,
                                    nanos: 0,
                                },
                            };

                            if let Some(entity_list) = storage.entities.get_mut(&key_as_string) {
                                for entity_metadata in entity_list {
                                    if entity_metadata.entity.key == Some(key.clone()) {
                                        *entity_metadata = updated_entity.clone();
                                    }
                                }
                            }

                            // Clean up indexes if needed
                            let key_struct = KeyStruct::from_datastore_key(&key);
                            storage.update_indexes(&key_struct, &updated_entity.entity);

                            // todo: return real data instead fake ones
                            let timestamp = prost_types::Timestamp {
                                seconds: 0,
                                nanos: 0,
                            };
                            mutation_results.push(google::datastore::v1::MutationResult {
                                key: entity.key.clone(),
                                version: 1,
                                create_time: Some(timestamp.clone()),
                                update_time: Some(timestamp),
                                conflict_detected: false,
                                transform_results: vec![],
                            });
                        } else {
                            // do we really need return 404,
                            // todo: check doc about not found items
                            //return Err(Status::not_found("Entity not found"));
                        }
                    }
                    Operation::Upsert(entity) => {
                        dbg!("Upserting entity", &entity);
                        let key = match entity.key {
                            Some(ref key) => key.clone(),
                            None => return Err(Status::invalid_argument("Entity missing key")),
                        };
                        let key_struct = KeyStruct::from_datastore_key(&key);
                        let key_as_string = KeyStruct::from_datastore_to_string(&key);
                        let timestamp = prost_types::Timestamp {
                            seconds: 0,
                            nanos: 0,
                        };

                        let entity_metadata = EntityWithMetadata {
                            entity: entity.clone(),
                            version: 1,
                            create_time: timestamp.clone(),
                            update_time: timestamp,
                        };
                        let updated_entity = EntityWithMetadata {
                            entity: entity.clone(),
                            version: entity_metadata.version + 1,
                            // todo: return real data instead fake ones
                            create_time: entity_metadata.create_time.clone(),
                            update_time: prost_types::Timestamp {
                                seconds: 0,
                                nanos: 0,
                            },
                        };

                        let mut item_was_inserted = false;
                        if let Some(entity_list) = storage.entities.get_mut(&key_as_string) {
                            for entity_metadata in &mut *entity_list {
                                if entity_metadata.entity.key == Some(key.clone()) {
                                    dbg!("Upserting entity", &key.path);
                                    *entity_metadata = updated_entity.clone();
                                    item_was_inserted = true;
                                }
                            }
                            if !item_was_inserted {
                                dbg!("Inserting upserting entity", &key.path);
                                entity_list.push(entity_metadata);
                            }
                        } else {
                            dbg!("No entity list found for key", &key_as_string);
                            storage
                                .entities
                                .insert(key_as_string.clone(), vec![entity_metadata.clone()]);
                        }

                        storage.update_indexes(&key_struct, entity);

                        let timestamp = prost_types::Timestamp {
                            seconds: 0,
                            nanos: 0,
                        };
                        mutation_results.push(google::datastore::v1::MutationResult {
                            key: Some(key),
                            version: 1,
                            create_time: Some(timestamp.clone()),
                            update_time: Some(timestamp),
                            conflict_detected: false,
                            transform_results: vec![],
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
                                println!("Entity not found for deletion");
                                // do we really need return 404,
                                // todo: check doc about not found items
                                //return Err(Status::not_found("Entity not found"));
                            }
                            for entity_metadata in removed_itens {
                                // Clean up indexes if needed
                                let key_struct = KeyStruct::from_datastore_key(key);
                                storage.update_indexes(&key_struct, &entity_metadata.entity);

                                if let Some(key) = &&entity_metadata.entity.key {
                                    dbg!("Deleting entity", &key.path);
                                };
                                // todo: return real data instead fake ones
                                let timestamp = prost_types::Timestamp {
                                    seconds: 0,
                                    nanos: 0,
                                };
                                mutation_results.push(google::datastore::v1::MutationResult {
                                    key: Some(key.clone()),
                                    version: 1,
                                    create_time: Some(timestamp.clone()),
                                    update_time: Some(timestamp),
                                    conflict_detected: false,
                                    transform_results: vec![],
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
        let end_time = SystemTime::now();
        let total_time_duration = end_time.duration_since(start_time).unwrap_or_default();
        let total_duration = prost_types::Timestamp {
            seconds: total_time_duration.as_secs() as i64,
            nanos: total_time_duration.as_nanos() as i32,
        };

        let response = Response::new(CommitResponse {
            mutation_results,
            index_updates,
            commit_time: Some(total_duration),
        });

        Ok(response)
    }

    async fn begin_transaction(
        &self,
        request: Request<BeginTransactionRequest>,
    ) -> Result<Response<BeginTransactionResponse>, Status> {
        let start = SystemTime::now();
        let req = request.into_inner();
        let duration_since_epoch = start
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default();

        let timestamp = prost_types::Timestamp {
            seconds: duration_since_epoch.as_secs() as i64,
            nanos: duration_since_epoch.as_nanos() as i32,
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
                timestamp,
                read_only,
            };

            // Add the transaction to storage
            storage
                .transactions
                .insert(transaction_id.clone(), transaction_state);
        }

        // Return the transaction ID as bytes
        let transaction_bytes = transaction_id.into_bytes();
        let transaction_response = BeginTransactionResponse {
            transaction: transaction_bytes,
        };
        Ok(Response::new(transaction_response))
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
        let start_time = SystemTime::now();
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
        let read_time = prost_types::Timestamp {
            seconds: 0,
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
                            if DatastoreStorage::apply_filter(entity_metadata, filter) {
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

        let end_time = SystemTime::now();
        let total_time_duration = end_time.duration_since(start_time).unwrap_or_default();

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
                        seconds: total_time_duration.as_secs() as i64,
                        nanos: total_time_duration.as_nanos() as i32,
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

    // Create a Server with increased message size limits for Tonic 1.0
    Server::builder()
        // In Tonic 1.0, we use max_frame_size to control message size
        .max_frame_size(10 * 1024 * 1024) // 10MB frame size (applies to both send/receive)
        // Register the service properly using the generated DatastoreServer
        .add_service(DatastoreServer::new(emulator))
        .serve(addr)
        .await?;

    Ok(())
}
