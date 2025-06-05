use google::datastore::v1::Filter;
use google::datastore::v1::commit_request::TransactionSelector;
use google::datastore::v1::key::PathElement;
use google::datastore::v1::key::path_element::IdType;
use google::datastore::v1::mutation::Operation;
use prost_types::value::Kind;
use prost_types::{Duration, Struct, Value as ValueProps};
use std::collections::{BTreeMap, HashMap};

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;
use tonic::{Request, Response, Status, transport::Server};

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
    BeginTransactionResponse, CommitRequest, CommitResponse, Entity, EntityResult, ExecutionStats,
    ExplainMetrics, LookupRequest, LookupResponse, PingRequest, PingResponse, PlanSummary,
    PropertyReference, ReserveIdsRequest, ReserveIdsResponse, RollbackRequest, RollbackResponse,
    RunAggregationQueryRequest, RunAggregationQueryResponse, RunQueryRequest, RunQueryResponse,
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
                        dbg!("Attempting to insert entity (from main.rs)", &entity);
                        match storage.insert_entity(entity) {
                            Ok((final_key, metadata)) => {
                                mutation_results.push(google::datastore::v1::MutationResult {
                                    key: Some(final_key),
                                    version: metadata.version as i64,
                                    create_time: Some(metadata.create_time.clone()),
                                    update_time: Some(metadata.update_time.clone()),
                                    conflict_detected: false,
                                    transform_results: vec![],
                                });
                            }
                            Err(status) => {
                                return Err(status);
                            }
                        }
                    }
                    Operation::Update(entity) => {
                        dbg!("Updating entity", &entity);
                        let key = match entity.key {
                            Some(ref key) => key.clone(),
                            None => {
                                return Err(Status::invalid_argument(
                                    "Entity missing key for update",
                                ));
                            }
                        };
                        let key_struct = KeyStruct::from_datastore_key(&key);

                        // Option to store data extracted from the mutable borrow if entity is found
                        let mut opt_updated_data: Option<(
                            /*entity_for_index_update*/ Entity,
                            /*version_for_result*/ u64,
                            /*create_time_for_result*/ prost_types::Timestamp,
                            /*update_time_for_result*/ prost_types::Timestamp,
                        )> = None;

                        if let Some(existing_entity_metadata) =
                            storage.entities.get_mut(&key_struct)
                        {
                            if let Some(k) = &existing_entity_metadata.entity.key {
                                dbg!("Updating entity", &k.path);
                            }

                            let timestamp_now = prost_types::Timestamp {
                                // Placeholder
                                seconds: 0,
                                nanos: 0,
                            };

                            existing_entity_metadata.entity = entity.clone(); // `entity` is from the request
                            existing_entity_metadata.version += 1;
                            existing_entity_metadata.update_time = timestamp_now.clone();

                            // Store all data needed after this block, then the borrow of existing_entity_metadata ends
                            opt_updated_data = Some((
                                existing_entity_metadata.entity.clone(), // For update_indexes
                                existing_entity_metadata.version,        // For MutationResult
                                existing_entity_metadata.create_time.clone(), // For MutationResult
                                timestamp_now, // For MutationResult (it's existing_entity_metadata.update_time)
                            ));
                        } // Mutable borrow of storage.entities (existing_entity_metadata) ends here.

                        // Now, call storage.update_indexes and push to mutation_results if data was updated
                        if let Some((entity_for_index, version, create_time, update_time)) =
                            opt_updated_data
                        {
                            storage.update_indexes(&key_struct, &entity_for_index);

                            mutation_results.push(google::datastore::v1::MutationResult {
                                key: entity.key.clone(), // Key from the original request entity
                                version: version as i64,
                                create_time: Some(create_time),
                                update_time: Some(update_time),
                                conflict_detected: false,
                                transform_results: vec![],
                            });
                        }
                    } // Closes Operation::Update(entity) arm
                    // Removed duplicate Operation::Update block
                    Operation::Upsert(entity) => {
                        dbg!("Upserting entity", &entity);
                        let key = match entity.key {
                            Some(ref key) => key.clone(),
                            None => {
                                return Err(Status::invalid_argument(
                                    "Entity missing key for upsert",
                                ));
                            }
                        };
                        // Note: Upsert might generate an ID if the key is incomplete.
                        // This logic assumes the key provided to upsert is complete or will be treated as such.
                        // If ID generation is needed for upsert like in Insert, it should be added here.
                        // For now, we assume `key` is complete for `KeyStruct` creation.
                        let key_struct = KeyStruct::from_datastore_key(&key);

                        let timestamp_now = prost_types::Timestamp {
                            // Placeholder
                            seconds: 0,
                            nanos: 0,
                        };

                        let entry = storage.entities.entry(key_struct.clone());
                        let version;
                        let create_time;
                        let update_time = timestamp_now.clone();

                        match entry {
                            std::collections::btree_map::Entry::Occupied(mut occupied_entry) => {
                                dbg!("Upserting (update path) entity", &key.path);
                                let metadata = occupied_entry.get_mut();
                                metadata.entity = entity.clone();
                                metadata.version += 1;
                                metadata.update_time = update_time.clone();
                                version = metadata.version;
                                create_time = metadata.create_time.clone();
                            }
                            std::collections::btree_map::Entry::Vacant(vacant_entry) => {
                                dbg!("Upserting (insert path) entity", &key.path);
                                let new_metadata = EntityWithMetadata {
                                    entity: entity.clone(),
                                    version: 1,
                                    create_time: timestamp_now.clone(),
                                    update_time: update_time.clone(),
                                };
                                version = new_metadata.version;
                                create_time = new_metadata.create_time.clone();
                                vacant_entry.insert(new_metadata);
                            }
                        }

                        storage.update_indexes(&key_struct, entity);

                        // Use the determined version, create_time, and update_time for the result
                        mutation_results.push(google::datastore::v1::MutationResult {
                            key: Some(key),
                            version: version as i64,
                            create_time: Some(create_time.clone()),
                            update_time: Some(update_time.clone()),
                            conflict_detected: false,
                            transform_results: vec![],
                        });
                    }
                    Operation::Delete(key_to_delete) => {
                        if let Some(removed_entity_metadata) = storage.delete_entity(key_to_delete)
                        {
                            if let Some(k) = &removed_entity_metadata.entity.key {
                                dbg!("Deleting entity", &k.path);
                            }

                            let timestamp_now = prost_types::Timestamp {
                                // Placeholder
                                seconds: 0,
                                nanos: 0,
                            };
                            mutation_results.push(google::datastore::v1::MutationResult {
                                key: Some(key_to_delete.clone()), // Return the original key from request
                                version: removed_entity_metadata.version as i64, // Use actual version
                                create_time: Some(removed_entity_metadata.create_time.clone()),
                                update_time: Some(timestamp_now), // Deletion time could be now
                                conflict_detected: false,
                                transform_results: vec![],
                            });
                        } else {
                            println!(
                                "Entity not found for deletion with key: {:?}",
                                key_to_delete.path
                            );
                            // Datastore typically doesn't error if deleting a non-existent entity.
                            // It returns a MutationResult with no key or version, or an empty result list.
                            // For simplicity, we can add a result indicating nothing happened or skip.
                            // Adding a result with the key but perhaps version 0 or specific times.
                            mutation_results.push(google::datastore::v1::MutationResult {
                                key: Some(key_to_delete.clone()),
                                version: 0, // Indicate not found or already deleted
                                create_time: None,
                                update_time: None,
                                conflict_detected: false,
                                transform_results: vec![],
                            });
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

            // Iterate over all entities and filter by kind and other filters
            for (key_struct, entity_metadata) in storage.entities.iter() {
                // Check if the entity's kind matches any of the kinds in the query
                let entity_kind = key_struct.path_elements.last().map(|(k, _)| k.as_str());
                if entity_kind.is_none()
                    || !query
                        .kind
                        .iter()
                        .any(|k_filter| Some(k_filter.name.as_str()) == entity_kind)
                {
                    continue; // Skip if kind doesn't match
                }

                // Check if the entity is found (key should always be present in EntityWithMetadata)
                if entity_metadata.entity.key.is_none() {
                    // This case should ideally not happen if data is consistent
                    eprintln!(
                        "Warning: Entity found in storage without a key in its Entity struct."
                    );
                    continue;
                }

                // Apply filters if present
                if let Some(filter_type) = &filters.filter_type {
                    if DatastoreStorage::apply_filter(entity_metadata, filter_type) {
                        matching_entities.push(entity_metadata);
                    }
                } else {
                    matching_entities.push(entity_metadata); // No filter, include if kind matches
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

#[cfg(test)]
mod tests {
    use super::*; // Imports items from the parent module (main.rs)
    use crate::google::datastore::v1::{
        datastore_client::DatastoreClient,
        Key, Value, Mutation, CommitRequest, LookupRequest, PingRequest, RunQueryRequest, Query,
        PartitionId, Filter, PropertyFilter, CompositeFilter, PropertyReference,
        filter::FilterType as GrpcFilterType, // Alias for gRPC FilterType
        property_filter::Operator as PropertyFilterOp,
        composite_filter::Operator as CompositeFilterOp,
        key::PathElement, // Corrected import for PathElement
        key::path_element::IdType as GrpcIdType, // Alias for gRPC IdType
        value::ValueType as GrpcValueType,     // Alias for gRPC ValueType
        commit_request::Mode as CommitMode,
        mutation::Operation as MutationOperation, // Alias for gRPC Mutation Operation
        KindExpression, // Import KindExpression directly
    };
    use std::collections::HashMap; // For entity properties
    use tonic::transport::Channel;

    const TEST_SERVER_ADDR: &str = "127.0.0.1:50051"; // Fixed port for tests

    async fn setup_test_client() -> DatastoreClient<Channel> {
        // Spawn the server in a background task.
        // Note: This simple setup might lead to port conflicts if tests run in parallel
        // without `--test-threads=1`. A new DatastoreEmulator is created for each call,
        // meaning if the server could run on different ports, state would be isolated.
        // With a fixed port, only one server instance will bind; subsequent attempts will fail
        // to start a new server but might connect to the existing one if not careful.
        tokio::spawn(async {
            let addr = TEST_SERVER_ADDR.parse().unwrap();
            let emulator = DatastoreEmulator::default();
            eprintln!("Test Datastore emulator listening on {}", addr);
            Server::builder()
                .add_service(DatastoreServer::new(emulator))
                .serve(addr)
                .await
                .unwrap_or_else(|e| eprintln!("Test server failed to start: {:?}", e));
        });
        // Give server a moment to start. This is not ideal but often works for local tests.
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        DatastoreClient::connect(format!("http://{}", TEST_SERVER_ADDR))
            .await
            .expect("Failed to connect to test server")
    }

    #[tokio::test]
    async fn test_ping_server() {
        let mut client = setup_test_client().await;
        let request = tonic::Request::new(PingRequest {
            message: "test".to_string(),
            // project_id field is not part of the standard PingRequest proto for datastore v1
        });
        let response = client.ping(request).await;
        assert!(response.is_ok(), "Ping request failed: {:?}", response.err());
        let response_inner = response.unwrap().into_inner();
        assert!(response_inner.message.contains("Hello test!"));
        assert!(response_inner.server_time.is_some());
    }

    #[tokio::test]
    async fn test_commit_insert_and_lookup() {
        let mut client = setup_test_client().await;

        // 1. Prepare an entity for insertion
        let project_id = "test-project-insert-lookup".to_string();
        let kind_name = "TestKindInsertLookup".to_string(); // Unique kind for this test
        let entity_id_name = "testEntity1".to_string();

        let mut properties = HashMap::new();
        properties.insert(
            "description".to_string(),
            Value { value_type: Some(GrpcValueType::StringValue("This is a test entity".to_string())), ..Default::default() },
        );
        properties.insert(
            "count".to_string(),
            Value { value_type: Some(GrpcValueType::IntegerValue(123)), ..Default::default() },
        );

        let key = Key {
            partition_id: Some(PartitionId {
                project_id: project_id.clone(),
                namespace_id: "".to_string(), // Default namespace
                ..Default::default()
            }),
            path: vec![PathElement {
                kind: kind_name.clone(),
                id_type: Some(GrpcIdType::Name(entity_id_name.clone())),
            }],
        };

        let entity_to_insert = Entity {
            key: Some(key.clone()),
            properties,
        };

        // 2. Create CommitRequest for insertion
        let mutation = Mutation {
            operation: Some(MutationOperation::Insert(entity_to_insert.clone())),
            ..Default::default()
        };
        let commit_request = CommitRequest {
            project_id: project_id.clone(),
            mode: CommitMode::NonTransactional as i32,
            mutations: vec![mutation],
            transaction_selector: None,
            database_id: "".to_string(), // Default database
        };

        // 3. Perform the commit
        let commit_response = client.commit(tonic::Request::new(commit_request)).await;
        assert!(commit_response.is_ok(), "Commit failed: {:?}", commit_response.err());
        let commit_response_inner = commit_response.unwrap().into_inner();
        assert_eq!(commit_response_inner.mutation_results.len(), 1, "Expected one mutation result");
        assert!(commit_response_inner.mutation_results[0].key.is_some(), "Mutation result should have a key");

        // 4. Prepare LookupRequest
        let lookup_request = LookupRequest {
            project_id: project_id.clone(),
            keys: vec![key.clone()],
            read_options: None,
            database_id: "".to_string(), // Default database
            property_mask: None, // Fetch all properties
        };

        // 5. Perform the lookup
        let lookup_response = client.lookup(tonic::Request::new(lookup_request)).await;
        assert!(lookup_response.is_ok(), "Lookup failed: {:?}", lookup_response.err());
        let lookup_response_inner = lookup_response.unwrap().into_inner();

        // 6. Verify the lookup result
        assert_eq!(lookup_response_inner.found.len(), 1, "Expected to find one entity");
        assert_eq!(lookup_response_inner.missing.len(), 0, "Expected no missing entities");

        let found_entity_result = &lookup_response_inner.found[0];
        assert!(found_entity_result.entity.is_some(), "Found entity result should contain an entity");
        let found_entity = found_entity_result.entity.as_ref().unwrap();

        assert_eq!(found_entity.key.as_ref(), Some(&key), "Found entity key does not match");
        
        let desc_prop = found_entity.properties.get("description").expect("Description property missing");
        assert_eq!(desc_prop.value_type,
                   Some(GrpcValueType::StringValue("This is a test entity".to_string())), "Description property mismatch");
        
        let count_prop = found_entity.properties.get("count").expect("Count property missing");
        assert_eq!(count_prop.value_type,
                   Some(GrpcValueType::IntegerValue(123)), "Count property mismatch");
    }

    #[tokio::test]
    async fn test_run_query_with_property_filter() {
        let mut client = setup_test_client().await;
        let project_id = "test-project-prop-filter".to_string();
        let kind_name = "TestKindPropFilter".to_string();

        // Insert two entities
        let entity1_key = Key {
            partition_id: Some(PartitionId { project_id: project_id.clone(), namespace_id: "".to_string(), ..Default::default() }),
            path: vec![PathElement { kind: kind_name.clone(), id_type: Some(GrpcIdType::Name("entity1".to_string())) }],
        };
        let mut props1 = HashMap::new();
        props1.insert("status".to_string(), Value { value_type: Some(GrpcValueType::StringValue("active".to_string())), ..Default::default() });
        let entity1 = Entity { key: Some(entity1_key.clone()), properties: props1 };

        let entity2_key = Key {
            partition_id: Some(PartitionId { project_id: project_id.clone(), namespace_id: "".to_string(), ..Default::default() }),
            path: vec![PathElement { kind: kind_name.clone(), id_type: Some(GrpcIdType::Name("entity2".to_string())) }],
        };
        let mut props2 = HashMap::new();
        props2.insert("status".to_string(), Value { value_type: Some(GrpcValueType::StringValue("inactive".to_string())), ..Default::default() });
        let entity2 = Entity { key: Some(entity2_key.clone()), properties: props2 };

        let commit_req = CommitRequest {
            project_id: project_id.clone(),
            mode: CommitMode::NonTransactional as i32,
            mutations: vec![
                Mutation { operation: Some(MutationOperation::Insert(entity1.clone())), ..Default::default() },
                Mutation { operation: Some(MutationOperation::Insert(entity2.clone())), ..Default::default() },
            ],
            database_id: "".to_string(),
            ..Default::default()
        };
        client.commit(tonic::Request::new(commit_req)).await.expect("Commit failed");

        // Query for entities with status "active"
        let query = Query {
            kind: vec![KindExpression { name: kind_name.clone() }], // Use KindExpression
            filter: Some(Filter {
                filter_type: Some(GrpcFilterType::PropertyFilter(PropertyFilter {
                    property: Some(PropertyReference { name: "status".to_string() }),
                    op: PropertyFilterOp::Equal as i32,
                    value: Some(Value { value_type: Some(GrpcValueType::StringValue("active".to_string())), ..Default::default() }),
                })),
            }),
            ..Default::default()
        };

        let run_query_req = RunQueryRequest {
            project_id: project_id.clone(),
            database_id: "".to_string(),
            query_type: Some(crate::google::datastore::v1::run_query_request::QueryType::Query(query)),
            ..Default::default()
        };

        let response = client.run_query(tonic::Request::new(run_query_req)).await.expect("RunQuery failed");
        let batch = response.into_inner().batch.expect("Query result batch is missing");

        assert_eq!(batch.entity_results.len(), 1, "Expected one entity with status 'active'");
        let found_entity = batch.entity_results[0].entity.as_ref().unwrap();
        assert_eq!(found_entity.key.as_ref(), Some(&entity1_key));
        assert_eq!(found_entity.properties.get("status").unwrap().value_type, Some(GrpcValueType::StringValue("active".to_string())));
    }

    #[tokio::test]
    async fn test_run_query_with_composite_filter() {
        let mut client = setup_test_client().await;
        let project_id = "test-project-comp-filter".to_string();
        let kind_name = "TestKindCompFilter".to_string();

        // Insert three entities
        let entity1_key = Key {
            partition_id: Some(PartitionId { project_id: project_id.clone(), namespace_id: "".to_string(), ..Default::default() }),
            path: vec![PathElement { kind: kind_name.clone(), id_type: Some(GrpcIdType::Name("entityA".to_string())) }],
        };
        let mut propsA = HashMap::new();
        propsA.insert("category".to_string(), Value { value_type: Some(GrpcValueType::StringValue("electronics".to_string())), ..Default::default() });
        propsA.insert("stock".to_string(), Value { value_type: Some(GrpcValueType::IntegerValue(10)), ..Default::default() });
        let entityA = Entity { key: Some(entity1_key.clone()), properties: propsA };

        let entity2_key = Key {
            partition_id: Some(PartitionId { project_id: project_id.clone(), namespace_id: "".to_string(), ..Default::default() }),
            path: vec![PathElement { kind: kind_name.clone(), id_type: Some(GrpcIdType::Name("entityB".to_string())) }],
        };
        let mut propsB = HashMap::new();
        propsB.insert("category".to_string(), Value { value_type: Some(GrpcValueType::StringValue("books".to_string())), ..Default::default() });
        propsB.insert("stock".to_string(), Value { value_type: Some(GrpcValueType::IntegerValue(5)), ..Default::default() });
        let entityB = Entity { key: Some(entity2_key.clone()), properties: propsB };
        
        let entity3_key = Key {
            partition_id: Some(PartitionId { project_id: project_id.clone(), namespace_id: "".to_string(), ..Default::default() }),
            path: vec![PathElement { kind: kind_name.clone(), id_type: Some(GrpcIdType::Name("entityC".to_string())) }],
        };
        let mut propsC = HashMap::new();
        propsC.insert("category".to_string(), Value { value_type: Some(GrpcValueType::StringValue("electronics".to_string())), ..Default::default() });
        propsC.insert("stock".to_string(), Value { value_type: Some(GrpcValueType::IntegerValue(3)), ..Default::default() });
        let entityC = Entity { key: Some(entity3_key.clone()), properties: propsC };


        let commit_req = CommitRequest {
            project_id: project_id.clone(),
            mode: CommitMode::NonTransactional as i32,
            mutations: vec![
                Mutation { operation: Some(MutationOperation::Insert(entityA.clone())), ..Default::default() },
                Mutation { operation: Some(MutationOperation::Insert(entityB.clone())), ..Default::default() },
                Mutation { operation: Some(MutationOperation::Insert(entityC.clone())), ..Default::default() },
            ],
            database_id: "".to_string(),
            ..Default::default()
        };
        client.commit(tonic::Request::new(commit_req)).await.expect("Commit failed");

        // Query for entities with category "electronics" AND stock > 5
        let filter1 = Filter {
            filter_type: Some(GrpcFilterType::PropertyFilter(PropertyFilter {
                property: Some(PropertyReference { name: "category".to_string() }),
                op: PropertyFilterOp::Equal as i32,
                value: Some(Value { value_type: Some(GrpcValueType::StringValue("electronics".to_string())), ..Default::default() }),
            })),
        };
        let filter2 = Filter {
            filter_type: Some(GrpcFilterType::PropertyFilter(PropertyFilter {
                property: Some(PropertyReference { name: "stock".to_string() }),
                op: PropertyFilterOp::GreaterThan as i32,
                value: Some(Value { value_type: Some(GrpcValueType::IntegerValue(5)), ..Default::default() }),
            })),
        };

        let composite_filter = Filter {
            filter_type: Some(GrpcFilterType::CompositeFilter(CompositeFilter {
                op: CompositeFilterOp::And as i32,
                filters: vec![filter1, filter2],
            })),
        };

        let query = Query {
            kind: vec![KindExpression { name: kind_name.clone() }], // Use KindExpression
            filter: Some(composite_filter),
            ..Default::default()
        };

        let run_query_req = RunQueryRequest {
            project_id: project_id.clone(),
            database_id: "".to_string(),
            query_type: Some(crate::google::datastore::v1::run_query_request::QueryType::Query(query)),
            ..Default::default()
        };

        let response = client.run_query(tonic::Request::new(run_query_req)).await.expect("RunQuery failed");
        let batch = response.into_inner().batch.expect("Query result batch is missing");

        assert_eq!(batch.entity_results.len(), 1, "Expected one entity matching composite filter");
        let found_entity = batch.entity_results[0].entity.as_ref().unwrap();
        assert_eq!(found_entity.key.as_ref(), Some(&entity1_key)); // Entity A
        assert_eq!(found_entity.properties.get("category").unwrap().value_type, Some(GrpcValueType::StringValue("electronics".to_string())));
        assert_eq!(found_entity.properties.get("stock").unwrap().value_type, Some(GrpcValueType::IntegerValue(10)));
    }
}
