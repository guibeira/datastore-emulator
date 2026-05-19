use crate::database::{DatastoreStorage, EntityWithMetadata, KeyStruct, TransactionState};
use crate::google::datastore::v1::{
    AggregationResult, AggregationResultBatch, AllocateIdsRequest, AllocateIdsResponse,
    BeginTransactionRequest, BeginTransactionResponse, CommitRequest, CommitResponse, Entity,
    EntityResult, ExecutionStats, ExplainMetrics, Filter, Key, LookupRequest, LookupResponse,
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
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{Instant, SystemTime};
use tokio::sync::RwLock;
use tonic::Status;

static TRANSACTION_ID_COUNTER: AtomicI64 = AtomicI64::new(0);
/// Offset cursors are stored as a big-endian u32. Longer cursors are encoded
/// KeyStruct values and need key retention to resume ordered scans correctly.
const OFFSET_CURSOR_BYTE_LEN: usize = 4;

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

fn next_transaction_id(prefix: &str) -> String {
    let timestamp = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let counter = TRANSACTION_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}-{timestamp}-{counter}")
}

fn key_is_incomplete(key: &Key) -> bool {
    key.path
        .iter()
        .any(|path_element| path_element.id_type.is_none())
}

fn commit_mutation_result(key: Option<Key>, version: i64) -> MutationResult {
    MutationResult {
        key,
        version,
        create_time: None,
        update_time: None,
        conflict_detected: false,
        transform_results: Vec::new(),
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
    Ok(LookupResponse {
        found,
        missing,
        deferred: vec![],
        transaction: Vec::new(),
        read_time: None,
    })
}

pub async fn run_query(
    storage: &Arc<RwLock<DatastoreStorage>>,
    req: RunQueryRequest,
) -> Result<RunQueryResponse, Status> {
    let explain_requested = req.explain_options.is_some();
    let start = explain_requested.then(Instant::now);
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

    let filter_type = query_obj
        .filter
        .as_ref()
        .and_then(|f| f.filter_type.clone());
    let retain_key_struct = query_obj.start_cursor.len() > OFFSET_CURSOR_BYTE_LEN
        || query_obj.distinct_on.iter().any(|p| p.name == "__key__")
        || order
            .iter()
            .any(|p| p.property.as_ref().is_some_and(|pr| pr.name == "__key__"));
    let (candidates, candidates_prefiltered) = {
        let storage = storage.read().await;
        storage.query_candidates(
            &req.project_id,
            &kind_name,
            filter_type.as_ref(),
            retain_key_struct,
        )
    };

    let batch = DatastoreStorage::get_entities_from_candidates(
        candidates,
        filter_type,
        candidates_prefiltered,
        query_obj.limit.as_ref().map(|v| v.value),
        query_obj.offset,
        query_obj.start_cursor.clone(),
        query_obj.projection.clone(),
        query_obj.distinct_on.clone(),
        order,
    );
    let explain_metrics = if let Some(start) = start {
        let mut fields = HashMap::new();
        let amount_results = batch.entity_results.len() as i64;

        // TODO: Populate emulator explain metrics with real planner/index details.
        fields.insert(
            "placeholder_plan".to_string(),
            ValueProps {
                kind: Some(Kind::StringValue("not_implemented".to_string())),
            },
        );
        let debug_stats = Struct {
            fields: fields.clone(),
        };
        let execution_duration = start.elapsed();

        Some(ExplainMetrics {
            plan_summary: Some(PlanSummary {
                indexes_used: vec![Struct {
                    fields: fields.clone(),
                }],
            }),
            execution_stats: Some(ExecutionStats {
                results_returned: amount_results,
                execution_duration: Some(to_pbjson_duration(execution_duration)),
                read_operations: amount_results,
                debug_stats: Some(debug_stats),
            }),
        })
    } else {
        None
    };

    Ok(RunQueryResponse {
        transaction: vec![],
        query: None,
        batch: Some(batch),
        explain_metrics,
    })
}

pub async fn commit(
    storage: &Arc<RwLock<DatastoreStorage>>,
    req: CommitRequest,
) -> Result<CommitResponse, Status> {
    let mutation_count = req.mutations.len();
    let mut storage = storage.write().await;
    let mut mutation_results = Vec::with_capacity(mutation_count);
    let commit_time = system_time_to_timestamp(SystemTime::now());

    // Handle transaction if present
    let transaction_id = if let Some(transaction_selector) = req.transaction_selector {
        match transaction_selector {
            TransactionSelector::Transaction(tx_bytes) => match String::from_utf8(tx_bytes) {
                Ok(id) => Some(id),
                Err(_) => {
                    return Err(Status::invalid_argument("Invalid transaction ID format"));
                }
            },
            TransactionSelector::SingleUseTransaction(_) => None,
        }
    } else {
        None
    };

    if let Some(tx_id) = transaction_id.as_ref() {
        if storage.transactions.get(tx_id).is_some() {
            tracing::info!("Committing transaction: {}", tx_id);
        } else {
            return Err(Status::not_found(format!(
                "Transaction {} not found",
                tx_id
            )));
        }
    }

    for mutation in req.mutations {
        if let Some(mutation) = mutation.operation {
            match mutation {
                Operation::Insert(entity) => {
                    let allocates_key = entity.key.as_ref().is_some_and(key_is_incomplete);
                    match storage.insert_owned_entity_at(entity, commit_time.clone()) {
                        Ok((final_key, metadata)) => {
                            mutation_results.push(commit_mutation_result(
                                allocates_key.then_some(final_key),
                                metadata.version as i64,
                            ));
                        }
                        Err(status) => {
                            return Err(status);
                        }
                    }
                }
                Operation::Update(entity) => {
                    let Some(key) = entity.key.as_ref() else {
                        return Err(Status::invalid_argument("Entity missing key for update"));
                    };
                    let key_struct = KeyStruct::from_datastore_key(key);

                    let Some(existing_entity_metadata) = storage.entities.get(&key_struct).cloned()
                    else {
                        return Err(Status::not_found("Entity not found for update"));
                    };

                    let create_time = existing_entity_metadata.create_time.clone();
                    let version = existing_entity_metadata.version + 1;
                    let updated_metadata = Arc::new(EntityWithMetadata {
                        entity,
                        version,
                        create_time,
                        update_time: commit_time.clone(),
                    });

                    storage
                        .entities
                        .insert(key_struct.clone(), Arc::clone(&updated_metadata));
                    storage.upsert_scoped_entity(&key_struct, Arc::clone(&updated_metadata));

                    storage.replace_indexes(
                        &key_struct,
                        &existing_entity_metadata.entity,
                        &updated_metadata.entity,
                    );

                    mutation_results.push(commit_mutation_result(None, version as i64));
                }
                Operation::Upsert(entity) => {
                    let Some(key) = entity.key.as_ref() else {
                        return Err(Status::invalid_argument("Entity missing key for upsert"));
                    };

                    let allocates_key = key_is_incomplete(key);

                    if allocates_key {
                        let (final_key, metadata) =
                            storage.insert_owned_entity_at(entity, commit_time.clone())?;
                        mutation_results.push(commit_mutation_result(
                            Some(final_key),
                            metadata.version as i64,
                        ));
                    } else {
                        let key_struct = KeyStruct::from_datastore_key(key);

                        let previous_metadata = storage.entities.get(&key_struct).cloned();
                        let observe_key_counters = previous_metadata.is_none();

                        let version = previous_metadata
                            .as_ref()
                            .map_or(1, |previous| previous.version + 1);
                        let create_time = previous_metadata.as_ref().map_or_else(
                            || commit_time.clone(),
                            |previous| previous.create_time.clone(),
                        );
                        if observe_key_counters {
                            storage.observe_key_id(key);
                            storage.add_ancestor_indexes(&key_struct);
                        }

                        let metadata = Arc::new(EntityWithMetadata {
                            entity,
                            version,
                            create_time,
                            update_time: commit_time.clone(),
                        });

                        storage
                            .entities
                            .insert(key_struct.clone(), Arc::clone(&metadata));
                        storage.upsert_scoped_entity(&key_struct, Arc::clone(&metadata));

                        if let Some(previous_metadata) = previous_metadata {
                            storage.replace_indexes(
                                &key_struct,
                                &previous_metadata.entity,
                                &metadata.entity,
                            );
                        } else {
                            storage.update_indexes(&key_struct, &metadata.entity);
                        }

                        mutation_results.push(commit_mutation_result(None, version as i64));
                    }
                }
                Operation::Delete(key_to_delete) => {
                    if storage.delete_entity(&key_to_delete).is_some() {
                        mutation_results.push(commit_mutation_result(None, 0));
                    } else {
                        tracing::warn!(
                            "Entity not found for deletion with key: {:?}",
                            key_to_delete.path
                        );
                        mutation_results.push(commit_mutation_result(None, 0));
                    }
                }
            }
        }
    }

    let index_updates = mutation_results.len() as i32;
    if let Some(tx_id) = transaction_id {
        storage.clean_transaction(&tx_id);
    }

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
    let transaction_id = next_transaction_id("tx");

    let transaction_bytes = transaction_id.as_bytes().to_vec();
    {
        let mut storage = storage.write().await;
        storage
            .transactions
            .insert(transaction_id, TransactionState);
    }

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
    let transaction_id = match String::from_utf8(req.transaction) {
        Ok(id) => id,
        Err(_) => return Err(Status::invalid_argument("Invalid transaction ID format")),
    };

    let mut storage = storage.write().await;
    if storage.transactions.remove(&transaction_id).is_some() {
        tracing::debug!("Transaction {} rolled back successfully", transaction_id);
    } else {
        tracing::debug!(
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
    let explain_requested = req.explain_options.is_some();
    let start = explain_requested.then(Instant::now);
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
    let base_query = match aggregation_query.query_type.as_ref() {
        Some(crate::google::datastore::v1::aggregation_query::QueryType::NestedQuery(query)) => {
            Some(query)
        }
        _ => {
            // todo: Handle other query types if needed
            None
        }
    };

    // Get current time for read_time
    let read_time = system_time_to_timestamp(SystemTime::now());

    // Process each aggregation
    let mut matching_entities: Vec<Arc<EntityWithMetadata>> = Vec::new();

    if let Some(query) = base_query {
        let filter_type = query.filter.as_ref().and_then(|f| f.filter_type.as_ref());

        // Reuse the query candidate pipeline so index lookups apply to aggregation
        // queries too. Multi-kind aggregations fall back to a scan because
        // query_candidates is per-kind.
        if query.kind.len() == 1 {
            let kind_name = query.kind[0].name.as_str();
            let (candidates, _) =
                storage.query_candidates(&req.project_id, kind_name, filter_type, false);
            matching_entities = candidates
                .into_iter()
                .filter(|c| c.entity_metadata.entity.key.is_some())
                .map(|c| c.entity_metadata)
                .collect();
        } else {
            for (key_struct, entity_metadata) in storage.entities.iter() {
                if key_struct.project_id != req.project_id {
                    continue;
                }

                let entity_kind = key_struct.path_elements.last().map(|(k, _)| k.as_str());
                if entity_kind.is_none()
                    || !query
                        .kind
                        .iter()
                        .any(|k_filter| Some(k_filter.name.as_str()) == entity_kind)
                {
                    continue;
                }

                if entity_metadata.entity.key.is_none() {
                    tracing::warn!("Entity found in storage without a key in its Entity struct.");
                    continue;
                }

                if let Some(filter_type) = filter_type {
                    if DatastoreStorage::apply_filter(entity_metadata, filter_type) {
                        matching_entities.push(Arc::clone(entity_metadata));
                    }
                } else {
                    matching_entities.push(Arc::clone(entity_metadata));
                }
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
                            } else if let Some(ValueType::DoubleValue(value)) = &property.value_type
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
                            } else if let Some(ValueType::DoubleValue(value)) = &property.value_type
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
    let explain_metrics = if let Some(start) = start {
        let total_results = batch.aggregation_results.len() as i64;
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

        Some(ExplainMetrics {
            plan_summary: Some(PlanSummary {
                indexes_used: vec![],
            }),
            execution_stats: Some(ExecutionStats {
                results_returned: total_results,
                execution_duration: Some(to_pbjson_duration(execution_duration)),
                read_operations: storage.entities.len() as i64,
                debug_stats: Some(debug_stats),
            }),
        })
    } else {
        None
    };

    Ok(RunAggregationQueryResponse {
        batch: Some(batch),
        query: None,
        transaction: Vec::new(),
        explain_metrics,
    })
}
