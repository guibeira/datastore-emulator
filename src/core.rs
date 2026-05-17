use crate::database::{DatastoreStorage, EntityWithMetadata, KeyStruct, TransactionState};
use crate::google::datastore::v1::{
    AggregationResult, AggregationResultBatch, AllocateIdsRequest, AllocateIdsResponse,
    BeginTransactionRequest, BeginTransactionResponse, CommitRequest, CommitResponse, Entity,
    EntityResult, ExecutionStats, ExplainMetrics, Filter, LookupRequest, LookupResponse,
    MutationResult, PlanSummary, PropertyOrder, PropertyReference, ReserveIdsRequest,
    ReserveIdsResponse, RollbackRequest, RollbackResponse, RunAggregationQueryRequest,
    RunAggregationQueryResponse, RunQueryRequest, RunQueryResponse, Value,
    aggregation_query::aggregation::Operator as AggregationOperator,
    commit_request::TransactionSelector, filter::FilterType, key::path_element::IdType,
    mutation::Operation, property_order, value::ValueType,
};
use pbjson_types::{Duration, Struct, Value as ValueProps, value::Kind};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Instant, SystemTime};
use tokio::sync::RwLock;
use tonic::Status;

fn to_pbjson_duration(std_duration: std::time::Duration) -> Duration {
    Duration {
        seconds: std_duration.as_secs() as i64,
        nanos: std_duration.subsec_nanos() as i32,
    }
}

pub(crate) fn system_time_to_timestamp(time: SystemTime) -> pbjson_types::Timestamp {
    match time.duration_since(SystemTime::UNIX_EPOCH) {
        Ok(duration) => pbjson_types::Timestamp {
            seconds: duration.as_secs() as i64,
            nanos: duration.subsec_nanos() as i32,
        },
        Err(_) => pbjson_types::Timestamp::default(),
    }
}

/// Walk a filter and collect property names referenced by any inequality
/// operator (LESS_THAN=1, LESS_THAN_OR_EQUAL=2, GREATER_THAN=3,
/// GREATER_THAN_OR_EQUAL=4, NOT_EQUAL=9). Datastore requires that the first
/// sort order match the inequality property; when no explicit order is
/// supplied we inject one implicitly.
fn collect_inequality_properties(filter: &Filter) -> Vec<String> {
    fn walk(filter_type: &FilterType, out: &mut Vec<String>) {
        match filter_type {
            FilterType::PropertyFilter(pf) => {
                let is_inequality = matches!(pf.op, 1 | 2 | 3 | 4 | 9);
                if is_inequality
                    && let Some(prop) = pf.property.as_ref()
                    && !out.contains(&prop.name)
                {
                    out.push(prop.name.clone());
                }
            }
            FilterType::CompositeFilter(cf) => {
                for sub in &cf.filters {
                    if let Some(ft) = sub.filter_type.as_ref() {
                        walk(ft, out);
                    }
                }
            }
        }
    }

    let mut out = Vec::new();
    if let Some(ft) = filter.filter_type.as_ref() {
        walk(ft, &mut out);
    }
    out
}

pub async fn lookup(
    storage: &Arc<RwLock<DatastoreStorage>>,
    req: LookupRequest,
) -> Result<LookupResponse, Status> {
    tracing::debug!(
        "Lookup request received: keys={:?} read_options={:?}",
        req.keys,
        req.read_options
    );
    let storage = storage.read().await;
    let mut found = Vec::new();
    let mut missing = Vec::new();

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
        } else {
            let entity = Entity {
                key: Some(key.clone()),
                properties: HashMap::new(),
            };
            missing.push(EntityResult {
                entity: Some(entity),
                create_time: None,
                update_time: None,
                cursor: vec![],
                version: 0,
            });
        }
    }
    let read_time = system_time_to_timestamp(SystemTime::now());

    Ok(LookupResponse {
        found,
        missing,
        deferred: vec![],
        transaction: Vec::new(),
        read_time: Some(read_time),
    })
}

pub async fn run_query(
    storage: &Arc<RwLock<DatastoreStorage>>,
    req: RunQueryRequest,
) -> Result<RunQueryResponse, Status> {
    let start = Instant::now();
    let storage = storage.read().await;
    let query_obj = match req.query_type {
        Some(crate::google::datastore::v1::run_query_request::QueryType::Query(query)) => query,
        _ => return Err(Status::invalid_argument("Missing or invalid query")),
    };
    let kind_name = query_obj
        .kind
        .first()
        .map(|k| k.name.clone())
        .ok_or_else(|| Status::invalid_argument("Query must specify a kind"))?;

    // Datastore requires inequality filters to sort by the inequality
    // property first. If the client did not supply an explicit order,
    // inject an ascending implicit order for each inequality property.
    let order: Vec<PropertyOrder> = if query_obj.order.is_empty() {
        query_obj
            .filter
            .as_ref()
            .map(collect_inequality_properties)
            .unwrap_or_default()
            .into_iter()
            .map(|name| PropertyOrder {
                property: Some(PropertyReference { name }),
                direction: property_order::Direction::Ascending as i32,
            })
            .collect()
    } else {
        query_obj.order.clone()
    };

    let batch = storage.get_entities(
        req.project_id.clone(),
        kind_name,
        query_obj.filter.clone(),
        query_obj.limit.as_ref().map(|v| v.value),
        query_obj.offset,
        query_obj.start_cursor.clone(),
        query_obj.projection.clone(),
        query_obj.distinct_on.clone(),
        order,
    );
    let mut fields = HashMap::new();
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

    Ok(RunQueryResponse {
        transaction: vec![],
        query: Some(query_obj),
        batch: Some(batch),
        explain_metrics: Some(ExplainMetrics {
            plan_summary: Some(PlanSummary {
                indexes_used: vec![Struct {
                    fields: fields.clone(),
                }],
            }),
            execution_stats: Some(ExecutionStats {
                results_returned: amount_results,
                execution_duration: Some(to_pbjson_duration(execution_duration)),
                read_operations: 10,
                debug_stats: Some(debug_stats),
            }),
        }),
    })
}

pub async fn commit(
    storage: &Arc<RwLock<DatastoreStorage>>,
    req: CommitRequest,
) -> Result<CommitResponse, Status> {
    let mut storage = storage.write().await;
    let mut mutation_results = Vec::new();

    // Handle transaction if present
    let transaction_id = if let Some(transaction_selector) = req.transaction_selector {
        match transaction_selector {
            TransactionSelector::Transaction(tx_bytes) => {
                match String::from_utf8(tx_bytes.clone()) {
                    Ok(id) => Some(id),
                    Err(_) => {
                        return Err(Status::invalid_argument("Invalid transaction ID format"));
                    }
                }
            }
            TransactionSelector::SingleUseTransaction(_) => None,
        }
    } else {
        None
    };

    if let Some(tx_id) = transaction_id.clone() {
        if storage.transactions.get(&tx_id).is_some() {
            tracing::info!("Committing transaction: {}", tx_id);
        } else {
            return Err(Status::not_found(format!(
                "Transaction {} not found",
                tx_id
            )));
        }
    }

    for mutation in req.mutations {
        if let Some(ref mutation) = mutation.operation {
            match mutation {
                Operation::Insert(entity) => match storage.insert_entity(entity) {
                    Ok((final_key, metadata)) => {
                        mutation_results.push(MutationResult {
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
                },
                Operation::Update(entity) => {
                    let key = match entity.key {
                        Some(ref key) => key.clone(),
                        None => {
                            return Err(Status::invalid_argument(
                                "Entity missing key for update",
                            ));
                        }
                    };
                    let key_struct = KeyStruct::from_datastore_key(&key);

                    let Some(existing_entity_metadata) = storage.entities.get_mut(&key_struct) else {
                        return Err(Status::not_found("Entity not found for update"));
                    };

                    let timestamp_now = system_time_to_timestamp(SystemTime::now());
                    let previous_entity = existing_entity_metadata.entity.clone();
                    existing_entity_metadata.entity = entity.clone();
                    existing_entity_metadata.version += 1;
                    existing_entity_metadata.update_time = timestamp_now.clone();

                    let entity_for_index = existing_entity_metadata.entity.clone();
                    let version = existing_entity_metadata.version;
                    let create_time = existing_entity_metadata.create_time.clone();
                    let update_time = timestamp_now;

                    storage.remove_from_indexes(&key_struct, &previous_entity);
                    storage.update_indexes(&key_struct, &entity_for_index);

                    mutation_results.push(MutationResult {
                        key: entity.key.clone(),
                        version: version as i64,
                        create_time: Some(create_time),
                        update_time: Some(update_time),
                        conflict_detected: false,
                        transform_results: vec![],
                    });
                }
                Operation::Upsert(entity) => {
                    let key = match entity.key {
                        Some(ref key) => key.clone(),
                        None => {
                            return Err(Status::invalid_argument(
                                "Entity missing key for upsert",
                            ));
                        }
                    };

                    let key_is_incomplete = key
                        .path
                        .iter()
                        .any(|path_element| path_element.id_type.is_none());

                    if key_is_incomplete {
                        let (final_key, metadata) = storage.insert_entity(entity)?;
                        mutation_results.push(MutationResult {
                            key: Some(final_key),
                            version: metadata.version as i64,
                            create_time: Some(metadata.create_time.clone()),
                            update_time: Some(metadata.update_time.clone()),
                            conflict_detected: false,
                            transform_results: vec![],
                        });
                    } else {
                        let key_struct = KeyStruct::from_datastore_key(&key);

                        let timestamp_now = system_time_to_timestamp(SystemTime::now());

                        let mut observe_key_counters = false;
                        let entry = storage.entities.entry(key_struct.clone());
                        let version;
                        let create_time;
                        let update_time = timestamp_now.clone();
                        let mut previous_entity: Option<Entity> = None;

                        match entry {
                            std::collections::btree_map::Entry::Occupied(
                                mut occupied_entry,
                            ) => {
                                let metadata = occupied_entry.get_mut();
                                previous_entity = Some(metadata.entity.clone());
                                metadata.entity = entity.clone();
                                metadata.version += 1;
                                metadata.update_time = update_time.clone();
                                version = metadata.version;
                                create_time = metadata.create_time.clone();
                            }
                            std::collections::btree_map::Entry::Vacant(vacant_entry) => {
                                let new_metadata = EntityWithMetadata {
                                    entity: entity.clone(),
                                    version: 1,
                                    create_time: timestamp_now.clone(),
                                    update_time: update_time.clone(),
                                };
                                version = new_metadata.version;
                                create_time = new_metadata.create_time.clone();
                                observe_key_counters = true;
                                vacant_entry.insert(new_metadata);
                            }
                        }

                        if observe_key_counters {
                            storage.observe_key_id(&key);
                        }

                        if let Some(prev) = previous_entity {
                            storage.remove_from_indexes(&key_struct, &prev);
                        }
                        storage.update_indexes(&key_struct, entity);

                        mutation_results.push(MutationResult {
                            key: Some(key),
                            version: version as i64,
                            create_time: Some(create_time.clone()),
                            update_time: Some(update_time.clone()),
                            conflict_detected: false,
                            transform_results: vec![],
                        });
                    }
                }
                Operation::Delete(key_to_delete) => {
                    if let Some(removed_entity_metadata) = storage.delete_entity(key_to_delete)
                    {
                        let timestamp_now = system_time_to_timestamp(SystemTime::now());
                        mutation_results.push(MutationResult {
                            key: Some(key_to_delete.clone()),
                            version: removed_entity_metadata.version as i64,
                            create_time: Some(removed_entity_metadata.create_time.clone()),
                            update_time: Some(timestamp_now),
                            conflict_detected: false,
                            transform_results: vec![],
                        });
                    } else {
                        tracing::warn!(
                            "Entity not found for deletion with key: {:?}",
                            key_to_delete.path
                        );
                        mutation_results.push(MutationResult {
                            key: Some(key_to_delete.clone()),
                            version: 0,
                            create_time: None,
                            update_time: None,
                            conflict_detected: false,
                            transform_results: vec![],
                        });
                    }
                }
            }
        }
    }

    let index_updates = mutation_results.len() as i32;
    if let Some(tx_id) = transaction_id {
        storage.clean_transaction(&tx_id);
    }
    let commit_time = system_time_to_timestamp(SystemTime::now());

    Ok(CommitResponse {
        mutation_results,
        index_updates,
        commit_time: Some(commit_time),
    })
}

pub async fn begin_transaction(
    storage: &Arc<RwLock<DatastoreStorage>>,
    req: BeginTransactionRequest,
) -> Result<BeginTransactionResponse, Status> {
    tracing::debug!("Received BeginTransactionRequest: {:?}", req);
    let start = SystemTime::now();
    let duration_since_epoch = start
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default();

    let timestamp = pbjson_types::Timestamp {
        seconds: duration_since_epoch.as_secs() as i64,
        nanos: (duration_since_epoch.as_nanos() % 1_000_000_000) as i32,
    };

    // Generate a unique transaction ID and create transaction state
    let transaction_id;
    {
        let mut storage = storage.write().await;

        // Generate transaction ID using timestamp and current counter
        let counter = storage
            .transaction_counter
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        transaction_id = format!("tx-{}-{}", timestamp.seconds, counter);

        // Check if the transaction is read-only
        let transaction_options = req.transaction_options.unwrap_or_default();
        let read_only = if let Some(mode) = transaction_options.mode {
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
            snapshot: HashMap::new(),
            timestamp,
            read_only,
        };

        storage
            .transactions
            .insert(transaction_id.clone(), transaction_state);
    }

    let transaction_bytes = transaction_id.into_bytes();
    let transaction_response = BeginTransactionResponse {
        transaction: transaction_bytes,
    };
    tracing::debug!(
        "Began transaction with ID: {:?}",
        String::from_utf8_lossy(&transaction_response.transaction)
    );
    Ok(transaction_response)
}

pub async fn rollback(
    storage: &Arc<RwLock<DatastoreStorage>>,
    req: RollbackRequest,
) -> Result<RollbackResponse, Status> {
    let transaction_id = match String::from_utf8(req.transaction.clone()) {
        Ok(id) => id,
        Err(_) => return Err(Status::invalid_argument("Invalid transaction ID format")),
    };

    let mut storage = storage.write().await;
    if storage.transactions.contains_key(&transaction_id) {
        storage.clean_transaction(&transaction_id);
        tracing::info!("Transaction {} rolled back successfully", transaction_id);
    } else {
        tracing::warn!(
            "Attempted to rollback non-existent transaction: {}",
            transaction_id
        );
    }

    Ok(RollbackResponse {})
}

pub async fn allocate_ids(
    storage: &Arc<RwLock<DatastoreStorage>>,
    req: AllocateIdsRequest,
) -> Result<AllocateIdsResponse, Status> {
    let mut storage = storage.write().await;
    let mut allocated_keys = Vec::new();

    for incomplete_key in req.keys {
        if incomplete_key.path.is_empty() {
            return Err(Status::invalid_argument("Key path cannot be empty"));
        }

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

        storage.observe_key_id(&new_key);
        allocated_keys.push(new_key);
    }

    Ok(AllocateIdsResponse {
        keys: allocated_keys,
    })
}

pub async fn reserve_ids(
    storage: &Arc<RwLock<DatastoreStorage>>,
    req: ReserveIdsRequest,
) -> Result<ReserveIdsResponse, Status> {
    let mut storage = storage.write().await;
    for key in &req.keys {
        if key.path.is_empty() {
            return Err(Status::invalid_argument("Key path cannot be empty"));
        }
        storage.observe_key_id(key);
    }
    Ok(ReserveIdsResponse {})
}

pub async fn run_aggregation_query(
    storage: &Arc<RwLock<DatastoreStorage>>,
    req: RunAggregationQueryRequest,
) -> Result<RunAggregationQueryResponse, Status> {
    let start = Instant::now();
    let storage = storage.read().await;

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
                    let result_value = Value {
                        exclude_from_indexes: false,
                        meaning: 0,
                        value_type: Some(ValueType::IntegerValue(count_value)),
                    };
                    aggregate_properties.insert(aggregation.alias.clone(), result_value);
                }
                AggregationOperator::Sum(sum) => {
                    // Implement SUM aggregation
                    let mut sum_value = 0.0;

                    let prop_name = match &sum.property {
                        Some(p) => &p.name,
                        None => {
                            return Err(Status::invalid_argument(
                                "Sum aggregation requires a property",
                            ));
                        }
                    };

                    // Calculate the sum for the specified property
                    for entity_metadata in &matching_entities {
                        if let Some(property) = entity_metadata.entity.properties.get(prop_name) {
                            if let Some(ValueType::IntegerValue(value)) = &property.value_type {
                                sum_value += *value as f64;
                            } else if let Some(ValueType::DoubleValue(value)) =
                                &property.value_type
                            {
                                sum_value += *value;
                            }
                        }
                    }

                    // Create the aggregation result
                    let result_value = Value {
                        exclude_from_indexes: false,
                        meaning: 0,
                        value_type: Some(ValueType::DoubleValue(sum_value)),
                    };
                    aggregate_properties.insert(aggregation.alias.clone(), result_value);
                }
                AggregationOperator::Avg(avg) => {
                    // Implement AVERAGE aggregation
                    let mut sum_value = 0.0;
                    let mut count = 0;

                    let prop_name = match &avg.property {
                        Some(p) => &p.name,
                        None => {
                            return Err(Status::invalid_argument(
                                "Avg aggregation requires a property",
                            ));
                        }
                    };

                    // Calculate the sum and count for the average
                    for entity_metadata in &matching_entities {
                        if let Some(property) = entity_metadata.entity.properties.get(prop_name) {
                            if let Some(ValueType::IntegerValue(value)) = &property.value_type {
                                sum_value += *value as f64;
                                count += 1;
                            } else if let Some(ValueType::DoubleValue(value)) =
                                &property.value_type
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
                    let result_value = Value {
                        exclude_from_indexes: false,
                        meaning: 0,
                        value_type: Some(ValueType::DoubleValue(average_value)),
                    };
                    aggregate_properties.insert(aggregation.alias.clone(), result_value);
                }
            }
        }
    }
    // Create a single AggregationResult with all properties
    let final_aggregation_result = AggregationResult {
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
    let mut fields = HashMap::new();
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
    Ok(RunAggregationQueryResponse {
        batch: Some(batch),
        query: Some(aggregation_query),
        transaction: Vec::new(),
        explain_metrics: Some(ExplainMetrics {
            plan_summary: Some(PlanSummary {
                indexes_used: vec![],
            }),
            execution_stats: Some(ExecutionStats {
                results_returned: total_results,
                execution_duration: Some(to_pbjson_duration(execution_duration)),
                read_operations: storage.entities.len() as i64,
                debug_stats: Some(debug_stats),
            }),
        }),
    })
}
