use crate::DatastoreEmulator;
use crate::database::{DatastoreStorage, EntityWithMetadata, KeyStruct, TransactionState};
use crate::google::datastore::v1::aggregation_query::aggregation::Operator as AggregationOperator;
use crate::google::datastore::v1::datastore_server::Datastore as DatastoreService;
use crate::google::datastore::v1::{
    AggregationResultBatch, AllocateIdsRequest, AllocateIdsResponse, BeginTransactionRequest,
    BeginTransactionResponse, CommitRequest, CommitResponse, Entity, EntityResult, ExecutionStats,
    ExplainMetrics, Filter, LookupRequest, LookupResponse, PingRequest, PingResponse, PlanSummary,
    PropertyReference, ReserveIdsRequest, ReserveIdsResponse, RollbackRequest, RollbackResponse,
    RunAggregationQueryRequest, RunAggregationQueryResponse, RunQueryRequest, RunQueryResponse,
    commit_request::TransactionSelector, key::path_element::IdType, mutation::Operation,
};
use prost_types::value::Kind;
use prost_types::{Duration, Struct, Value as ValueProps};
use std::collections::{BTreeMap, HashMap};
use std::time::{Instant, SystemTime};
use tonic::{Request, Response, Status};
use tracing;

fn system_time_to_timestamp(time: SystemTime) -> prost_types::Timestamp {
    match time.duration_since(SystemTime::UNIX_EPOCH) {
        Ok(duration) => prost_types::Timestamp {
            seconds: duration.as_secs() as i64,
            nanos: duration.subsec_nanos() as i32,
        },
        Err(_) => prost_types::Timestamp::default(),
    }
}

fn to_prost_duration(std_duration: std::time::Duration) -> Duration {
    Duration {
        seconds: std_duration.as_secs() as i64,
        nanos: std_duration.subsec_nanos() as i32,
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
        let req = request.into_inner();
        let storage = self.storage.read().await;
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
        let read_time = system_time_to_timestamp(SystemTime::now());

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
        let start = Instant::now();
        let req = request.into_inner();
        let storage = self.storage.read().await;
        // Extract the query from the request
        let query_obj = match req.query_type {
            Some(crate::google::datastore::v1::run_query_request::QueryType::Query(query)) => query,
            _ => {
                // implement other query types if needed
                return Err(Status::invalid_argument("Missing or invalid query"));
            }
        };
        let kind_name = query_obj
            .kind
            .first()
            .map(|k| k.name.clone())
            .ok_or_else(|| Status::invalid_argument("Query must specify a kind"))?;
        let batch = storage.get_entities(
            req.project_id.clone(),
            kind_name,
            query_obj.filter.clone(),
            query_obj.limit,
            query_obj.start_cursor.clone(),
            query_obj.projection.clone(),
            query_obj.order.clone(),
        );
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

        let execution_duration = start.elapsed();

        Ok(Response::new(RunQueryResponse {
            transaction: vec![],
            query: Some(query_obj),
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
                    execution_duration: Some(to_prost_duration(execution_duration)),
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
        let mut storage = self.storage.write().await;
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
                tracing::info!("Committing transaction: {}", tx_id);
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
                        //dbg!("Attempting to insert entity (from main.rs)", &entity);
                        match storage.insert_entity(entity) {
                            Ok((final_key, metadata)) => {
                                mutation_results.push(
                                    crate::google::datastore::v1::MutationResult {
                                        key: Some(final_key),
                                        version: metadata.version as i64,
                                        create_time: Some(metadata.create_time.clone()),
                                        update_time: Some(metadata.update_time.clone()),
                                        conflict_detected: false,
                                        transform_results: vec![],
                                    },
                                );
                            }
                            Err(status) => {
                                return Err(status);
                            }
                        }
                    }
                    Operation::Update(entity) => {
                        //dbg!("Updating entity", &entity);
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
                            if let Some(_k) = &existing_entity_metadata.entity.key {
                                //dbg!("Updating entity", &k.path);
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

                            mutation_results.push(crate::google::datastore::v1::MutationResult {
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
                        //dbg!("Upserting entity", &entity);
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
                                //dbg!("Upserting (update path) entity", &key.path);
                                let metadata = occupied_entry.get_mut();
                                metadata.entity = entity.clone();
                                metadata.version += 1;
                                metadata.update_time = update_time.clone();
                                version = metadata.version;
                                create_time = metadata.create_time.clone();
                            }
                            std::collections::btree_map::Entry::Vacant(vacant_entry) => {
                                //dbg!("Upserting (insert path) entity", &key.path);
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
                        mutation_results.push(crate::google::datastore::v1::MutationResult {
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
                            if let Some(_k) = &removed_entity_metadata.entity.key {
                                //dbg!("Deleting entity", &k.path);
                            }

                            let timestamp_now = prost_types::Timestamp {
                                // Placeholder
                                seconds: 0,
                                nanos: 0,
                            };
                            mutation_results.push(crate::google::datastore::v1::MutationResult {
                                key: Some(key_to_delete.clone()), // Return the original key from request
                                version: removed_entity_metadata.version as i64, // Use actual version
                                create_time: Some(removed_entity_metadata.create_time.clone()),
                                update_time: Some(timestamp_now), // Deletion time could be now
                                conflict_detected: false,
                                transform_results: vec![],
                            });
                        } else {
                            tracing::warn!(
                                "Entity not found for deletion with key: {:?}",
                                key_to_delete.path
                            );
                            // Datastore typically doesn't error if deleting a non-existent entity.
                            // It returns a MutationResult with no key or version, or an empty result list.
                            // For simplicity, we can add a result indicating nothing happened or skip.
                            // Adding a result with the key but perhaps version 0 or specific times.
                            mutation_results.push(crate::google::datastore::v1::MutationResult {
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
        let commit_time = system_time_to_timestamp(SystemTime::now());

        let response = Response::new(CommitResponse {
            mutation_results,
            index_updates,
            commit_time: Some(commit_time),
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
            let mut storage = self.storage.write().await;

            // Generate transaction ID using timestamp and current counter
            let counter = storage.transaction_counter;
            storage.transaction_counter += 1;
            transaction_id = format!("tx-{}-{}", timestamp.seconds, counter);

            // Check if the transaction is read-only
            let transaction_options = req.transaction_options.unwrap_or_default();
            let read_only = if let Some(mode) = transaction_options.mode {
                // Set read-only flag
                match mode {
                    crate::google::datastore::v1::transaction_options::Mode::ReadOnly(_) => true,
                    crate::google::datastore::v1::transaction_options::Mode::ReadWrite(_) => false,
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
            let mut storage = self.storage.write().await;

            // Check if the transaction exists
            if storage.transactions.contains_key(&transaction_id) {
                // Clean up the transaction (removes it from storage)
                storage.clean_transaction(&transaction_id);
                tracing::info!("Transaction {} rolled back successfully", transaction_id);
            } else {
                tracing::warn!(
                    "Attempted to rollback non-existent transaction: {}",
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
        let mut storage = self.storage.write().await;
        let mut allocated_keys = Vec::new();

        // Process each incomplete key in the request
        for incomplete_key in req.keys {
            // Verify the key is incomplete (missing ID or name)
            if incomplete_key.path.is_empty() {
                return Err(Status::invalid_argument("Key path cannot be empty"));
            }

            // Create a new key with the same structure but with allocated IDs
            let mut new_key = incomplete_key.clone();
            let mut allocated_id: Option<i64> = None;

            for path_element in new_key.path.iter_mut() {
                if path_element.id_type.is_none() {
                    if allocated_id.is_none() {
                        allocated_id = Some(storage.next_auto_id(&incomplete_key)?);
                    }
                    path_element.id_type = allocated_id.map(IdType::Id);
                }
            }

            // Add the allocated key to the result
            storage.observe_key_id(&new_key);

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
        let mut storage = self.storage.write().await;

        // Process each key in the request
        for key in &req.keys {
            // Validate the key
            if key.path.is_empty() {
                return Err(Status::invalid_argument("Key path cannot be empty"));
            }

            // For each path element with an ID, reserve that ID
            storage.observe_key_id(key);

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
        let start = Instant::now();
        let req = request.into_inner();
        let storage = self.storage.read().await;

        // Extract the aggregation query from the request
        let aggregation_query = match req.query_type {
            Some(
                crate::google::datastore::v1::run_aggregation_query_request::QueryType::AggregationQuery(
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
            Some(crate::google::datastore::v1::aggregation_query::QueryType::NestedQuery(
                query,
            )) => Some(query),
            _ => {
                // todo: Handle other query types if needed
                None
            }
        };

        // Get current time for read_time
        let read_time = system_time_to_timestamp(SystemTime::now());

        // Process each aggregation
        let mut matching_entities = Vec::new();

        if let Some(query) = &base_query {
            // Get entities matching the kind filter
            let filters = query
                .filter
                .as_ref()
                .unwrap_or(&Filter { filter_type: None });

            // Iterate over all entities and filter by project_id, kind and other filters
            for (key_struct, entity_metadata) in storage.entities.iter() {
                // Filter by project_id first
                if key_struct.project_id != req.project_id {
                    continue;
                }

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
                    tracing::warn!("Entity found in storage without a key in its Entity struct.");
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

        let mut aggregate_properties = HashMap::new();

        for aggregation in &aggregation_query.aggregations {
            if let Some(aggregation_operator) = &aggregation.operator {
                // Check if the aggregation operator is supported
                match aggregation_operator {
                    AggregationOperator::Count(_count) => {
                        let count_value = matching_entities.len() as i64;
                        // Create the aggregation result
                        let result_value = crate::google::datastore::v1::Value {
                            exclude_from_indexes: false,
                            meaning: 0,
                            value_type: Some(
                                crate::google::datastore::v1::value::ValueType::IntegerValue(
                                    count_value,
                                ),
                            ),
                        };
                        aggregate_properties.insert(aggregation.alias.clone(), result_value);
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
                                    crate::google::datastore::v1::value::ValueType::IntegerValue(
                                        value,
                                    ),
                                ) = &property.value_type
                                {
                                    sum_value += *value as f64;
                                } else if let Some(
                                    crate::google::datastore::v1::value::ValueType::DoubleValue(
                                        value,
                                    ),
                                ) = &property.value_type
                                {
                                    sum_value += *value;
                                }
                            }
                        }

                        // Create the aggregation result
                        let result_value = crate::google::datastore::v1::Value {
                            exclude_from_indexes: false,
                            meaning: 0,
                            value_type: Some(
                                crate::google::datastore::v1::value::ValueType::DoubleValue(
                                    sum_value,
                                ),
                            ),
                        };
                        aggregate_properties.insert(aggregation.alias.clone(), result_value);
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
                                    crate::google::datastore::v1::value::ValueType::IntegerValue(
                                        value,
                                    ),
                                ) = &property.value_type
                                {
                                    sum_value += *value as f64;
                                    count += 1;
                                } else if let Some(
                                    crate::google::datastore::v1::value::ValueType::DoubleValue(
                                        value,
                                    ),
                                ) = &property.value_type
                                {
                                    sum_value += *value;
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
                        let result_value = crate::google::datastore::v1::Value {
                            exclude_from_indexes: false,
                            meaning: 0,
                            value_type: Some(
                                crate::google::datastore::v1::value::ValueType::DoubleValue(
                                    average_value,
                                ),
                            ),
                        };
                        aggregate_properties.insert(aggregation.alias.clone(), result_value);
                    }
                }
            }
        }
        // Create a single AggregationResult with all properties
        let final_aggregation_result = crate::google::datastore::v1::AggregationResult {
            aggregate_properties,
        };

        // Create the result batch
        let batch = AggregationResultBatch {
            aggregation_results: vec![final_aggregation_result],
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

        let execution_duration = start.elapsed();
        let response = Response::new(RunAggregationQueryResponse {
            batch: Some(batch),
            query: Some(aggregation_query),
            transaction: Vec::new(),
            explain_metrics: Some(ExplainMetrics {
                plan_summary: Some(PlanSummary {
                    indexes_used: vec![],
                }),
                execution_stats: Some(ExecutionStats {
                    results_returned: total_results,
                    execution_duration: Some(to_prost_duration(execution_duration)),
                    read_operations: storage.entities.len() as i64,
                    debug_stats: Some(debug_stats),
                }),
            }),
        });
        Ok(response)
    }
}
