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
                        dbg!("Inserting entity", &entity);
                        let original_key = match entity.key {
                            Some(ref key) => key.clone(),
                            None => return Err(Status::invalid_argument("Entity missing key")),
                        };

                        let mut key_with_new_id = original_key.clone();
                        // let mut id_generated = false; // Unused variable - a flag to track if an ID was generated is not necessary for this logic.

                        // Determine the "kind" of the entity for ID generation.
                        // This is based on the last PathElement of the original entity key.
                        let entity_kind_for_id_gen = match original_key.path.last() {
                            Some(pe) => &pe.kind,
                            None => {
                                // This case would occur if the entity key had no PathElements.
                                return Err(Status::invalid_argument(
                                    "The entity key has no path elements to determine the 'kind' for ID generation.",
                                ));
                            }
                        };

                        // Calculate the new ID based on the count of entities of the same "kind".
                        // This count is done once before the loop, replicating the old logic.
                        let count_for_entity_kind = storage
                            .entities
                            .keys()
                            .filter(|stored_key_struct| {
                                stored_key_struct
                                    .path_elements
                                    .last()
                                    .map_or(false, |(k, _)| k == entity_kind_for_id_gen)
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
                            seconds: 0, // SystemTime::now().duration_since(UNIX_EPOCH)...
                            nanos: 10,
                        };

                        let entity_metadata = EntityWithMetadata {
                            entity: db_entity.clone(),
                            version: 1, // Initial version
                            create_time: timestamp_now.clone(),
                            update_time: timestamp_now.clone(),
                        };

                        if let Some(k) = &entity_metadata.entity.key {
                            dbg!("Inserting entity with key", &k.path);
                        }

                        // Insert into the BTreeMap<KeyStruct, EntityWithMetadata>
                        // Clone entity_metadata for insertion, so the original can be used for MutationResult
                        storage
                            .entities
                            .insert(final_key_struct.clone(), entity_metadata.clone());
                        storage.update_indexes(&final_key_struct, &db_entity);

                        // Removed unused local timestamp variable

                        mutation_results.push(google::datastore::v1::MutationResult {
                            key: Some(key_with_new_id.clone()),
                            version: entity_metadata.version as i64, // Use actual version from metadata
                            create_time: Some(timestamp_now.clone()),
                            update_time: Some(timestamp_now.clone()),
                            conflict_detected: false,
                            transform_results: vec![],
                        });
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
                        } else {
                            // Entity not found for update.
                            // Datastore typically doesn't error; the mutation just has no effect.
                            // So, we don't add a MutationResult, which is fine.
                            println!("Entity not found for update with key: {:?}", key.path);
                        }
                    }
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
                        let key_struct_to_delete = KeyStruct::from_datastore_key(key_to_delete);

                        if let Some(removed_entity_metadata) =
                            storage.entities.remove(&key_struct_to_delete)
                        {
                            if let Some(k) = &removed_entity_metadata.entity.key {
                                dbg!("Deleting entity", &k.path);
                            }

                            storage.update_indexes(
                                &key_struct_to_delete,
                                &removed_entity_metadata.entity,
                            );

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
