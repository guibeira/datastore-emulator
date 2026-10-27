use crate::database::{DatastoreStorage, EntityWithMetadata, KeyStruct, QueryCandidate};
use crate::google::datastore::v1::{
    AggregationResult, AggregationResultBatch, AllocateIdsRequest, AllocateIdsResponse,
    BeginTransactionRequest, BeginTransactionResponse, CommitRequest, CommitResponse, Entity,
    EntityResult, ExecutionStats, ExplainMetrics, Filter, Key, LookupRequest, LookupResponse,
    Mutation, MutationResult, PlanSummary, PropertyOrder, PropertyReference, Query,
    ReserveIdsRequest, ReserveIdsResponse, RollbackRequest, RollbackResponse,
    RunAggregationQueryRequest, RunAggregationQueryResponse, RunQueryRequest, RunQueryResponse,
    Value,
    aggregation_query::aggregation::Operator as AggregationOperator,
    commit_request::{Mode, TransactionSelector},
    filter::FilterType,
    key::path_element::IdType,
    mutation::Operation,
    property_order,
    property_transform::TransformType,
    transaction_options,
    value::ValueType,
};
use parking_lot::RwLock;
use pbjson_types::{Duration, Struct, Value as ValueProps, value::Kind};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{Instant, SystemTime};
use tonic::Status;

static TRANSACTION_ID_COUNTER: AtomicI64 = AtomicI64::new(0);

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
            nanos: (duration.subsec_nanos() / 1000 * 1000) as i32,
        },
        Err(_) => pbjson_types::Timestamp::default(),
    }
}

pub(crate) fn next_transaction_id(prefix: &str) -> String {
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

fn invalid_transaction() -> Status {
    Status::invalid_argument("Invalid transaction.")
}

fn normalize_key_project(key: &mut Key, project: &str) {
    // Fully empty keys are exempt from the API's project normalization rule.
    if key.path.is_empty()
        && key.partition_id.as_ref().is_none_or(|partition| {
            partition.project_id.is_empty()
                && partition.database_id.is_empty()
                && partition.namespace_id.is_empty()
        })
    {
        return;
    }
    let partition = key.partition_id.get_or_insert_with(Default::default);
    if partition.project_id.is_empty() {
        project.clone_into(&mut partition.project_id);
    }
}

fn normalize_entity_projects(entity: &mut Entity, project: &str) {
    if let Some(key) = entity.key.as_mut() {
        normalize_key_project(key, project);
    }
    for value in entity.properties.values_mut() {
        normalize_value_projects(value, project);
    }
}

fn normalize_value_projects(value: &mut Value, project: &str) {
    match value.value_type.as_mut() {
        Some(ValueType::KeyValue(key)) => normalize_key_project(key, project),
        Some(ValueType::EntityValue(entity)) => normalize_entity_projects(entity, project),
        Some(ValueType::ArrayValue(array)) => {
            for value in &mut array.values {
                normalize_value_projects(value, project);
            }
        }
        _ => {}
    }
}

fn normalize_filter_projects(filter: &mut Filter, project: &str) {
    match filter.filter_type.as_mut() {
        Some(FilterType::PropertyFilter(filter)) => {
            if let Some(value) = filter.value.as_mut() {
                normalize_value_projects(value, project);
            }
        }
        Some(FilterType::CompositeFilter(filter)) => {
            for filter in &mut filter.filters {
                normalize_filter_projects(filter, project);
            }
        }
        None => {}
    }
}

fn validate_mutation_key(key: &Key) -> Result<(), Status> {
    if key.path.is_empty() {
        return Err(Status::invalid_argument("Key path is empty."));
    }
    if key.path.iter().any(|element| element.kind.is_empty()) {
        return Err(Status::invalid_argument("The kind is the empty string."));
    }
    Ok(())
}

/// Names matching `^__.*__$` are reserved by Datastore.
fn is_reserved_property_name(name: &str) -> bool {
    name.len() >= 4 && name.starts_with("__") && name.ends_with("__")
}

fn validate_entity_property_names(entity: &Entity) -> Result<(), Status> {
    for (name, value) in &entity.properties {
        if is_reserved_property_name(name) {
            return Err(Status::invalid_argument(format!(
                "The property.name \"{name}\" is reserved."
            )));
        }
        validate_value_property_names(value)?;
    }
    Ok(())
}

fn oversized_indexed_property(entity: &Entity) -> Option<&str> {
    entity
        .properties
        .iter()
        .find_map(|(name, value)| oversized_indexed_value(value, name))
}

fn oversized_indexed_value<'a>(value: &'a Value, name: &'a str) -> Option<&'a str> {
    match value.value_type.as_ref() {
        Some(ValueType::StringValue(s)) if !value.exclude_from_indexes && s.len() > 1500 => {
            Some(name)
        }
        Some(ValueType::BlobValue(b)) if !value.exclude_from_indexes && b.len() > 1500 => {
            Some(name)
        }
        Some(ValueType::EntityValue(entity)) => oversized_indexed_property(entity),
        Some(ValueType::ArrayValue(array)) => array
            .values
            .iter()
            .find_map(|element| oversized_indexed_value(element, name)),
        _ => None,
    }
}

pub(crate) fn normalize_entity_values(entity: &mut Entity) {
    for value in entity.properties.values_mut() {
        normalize_value(value);
    }
}

fn normalize_value(value: &mut Value) {
    match value.value_type.as_mut() {
        Some(ValueType::TimestampValue(timestamp))
            if (0..1_000_000_000).contains(&timestamp.nanos) =>
        {
            timestamp.nanos = timestamp.nanos / 1000 * 1000;
        }
        Some(ValueType::EntityValue(entity)) => normalize_entity_values(entity),
        Some(ValueType::ArrayValue(array)) => {
            for element in &mut array.values {
                normalize_value(element);
            }
            // The Google emulator stores indexed elements before unindexed elements.
            array
                .values
                .sort_by_key(|element| element.exclude_from_indexes);
        }
        _ => {}
    }
}

fn validate_value_property_names(value: &Value) -> Result<(), Status> {
    match value.value_type.as_ref() {
        Some(ValueType::EntityValue(entity)) => validate_entity_property_names(entity),
        Some(ValueType::ArrayValue(array)) => array
            .values
            .iter()
            .try_for_each(validate_value_property_names),
        _ => Ok(()),
    }
}

fn mutation_key(operation: &Operation) -> Option<&Key> {
    match operation {
        Operation::Insert(entity) | Operation::Update(entity) | Operation::Upsert(entity) => {
            entity.key.as_ref()
        }
        Operation::Delete(key) => Some(key),
    }
}

fn validate_mutation(operation: &Operation) -> Result<(), Status> {
    if let Some(key) = mutation_key(operation) {
        validate_mutation_key(key)?;
    }
    match operation {
        Operation::Insert(entity) | Operation::Update(entity) | Operation::Upsert(entity) => {
            validate_entity_property_names(entity)?;
            if let Some(name) = oversized_indexed_property(entity) {
                return Err(Status::invalid_argument(format!(
                    "The value of property \"{name}\" is longer than 1500 bytes."
                )));
            }
            Ok(())
        }
        Operation::Delete(_) => Ok(()),
    }
}

fn prepare_mutation(
    mutation: &mut Mutation,
    previous: Option<&Entity>,
    time: &pbjson_types::Timestamp,
) -> Result<Vec<Value>, Status> {
    if let Some(Operation::Insert(entity) | Operation::Update(entity) | Operation::Upsert(entity)) =
        mutation.operation.as_mut()
    {
        normalize_entity_values(entity);
    }
    for transform in &mut mutation.property_transforms {
        if let Some(
            TransformType::AppendMissingElements(array) | TransformType::RemoveAllFromArray(array),
        ) = transform.transform_type.as_mut()
        {
            for value in &mut array.values {
                normalize_value(value);
            }
        }
    }
    let results = crate::mutation::prepare(mutation, previous, time)?;
    if let Some(operation) = mutation.operation.as_mut() {
        match operation {
            Operation::Insert(entity) | Operation::Update(entity) | Operation::Upsert(entity) => {
                normalize_entity_values(entity)
            }
            Operation::Delete(_) => {}
        }
        validate_mutation(operation)?;
    }
    Ok(results)
}

/// Transactional commits are all-or-nothing, so the preconditions the apply
/// loop would hit (insert of an existing key, update of a missing key) are
/// checked up front. Earlier mutations of the same request count: a key
/// inserted or upserted exists afterwards, a deleted key does not.
fn check_transactional_preconditions(
    storage: &DatastoreStorage,
    mutations: &[Mutation],
) -> Result<(), Status> {
    let mut exists_after: HashMap<KeyStruct, bool> = HashMap::new();
    for operation in mutations.iter().filter_map(|m| m.operation.as_ref()) {
        let Some(key) = mutation_key(operation) else {
            continue;
        };
        if key_is_incomplete(key) {
            continue;
        }
        let key_struct = KeyStruct::from_datastore_key(key);
        let exists = exists_after
            .get(&key_struct)
            .copied()
            .unwrap_or_else(|| storage.entities.contains_key(&key_struct));
        match operation {
            Operation::Insert(_) if exists => {
                return Err(Status::already_exists("entity already exists"));
            }
            Operation::Update(_) if !exists => {
                return Err(Status::not_found("Entity not found for update"));
            }
            Operation::Insert(_) | Operation::Update(_) | Operation::Upsert(_) => {
                exists_after.insert(key_struct, true);
            }
            Operation::Delete(_) => {
                exists_after.insert(key_struct, false);
            }
        }
    }
    Ok(())
}

/// Every key and partition in a request must name the request's database,
/// like Cloud Datastore.
fn check_request_database(
    request_database: &str,
    partition: Option<&crate::google::datastore::v1::PartitionId>,
) -> Result<(), Status> {
    let database = partition.map_or("", |p| p.database_id.as_str());
    if database == request_database {
        return Ok(());
    }
    Err(Status::invalid_argument(format!(
        "mismatched databases within request: {request_database:?} vs. {database:?}"
    )))
}

fn has_duplicate_complete_key(mutations: &[Mutation]) -> bool {
    let mut seen = HashSet::new();
    mutations
        .iter()
        .filter_map(|mutation| mutation.operation.as_ref().and_then(mutation_key))
        .filter(|key| !key_is_incomplete(key))
        .any(|key| !seen.insert(KeyStruct::from_datastore_key(key)))
}

fn validate_allocate_ids_path(key: &Key) -> Result<(), Status> {
    let last_index = key.path.len().saturating_sub(1);
    for (index, element) in key.path.iter().enumerate() {
        match (&element.id_type, index == last_index) {
            (Some(id_type), true) => {
                let id = match id_type {
                    IdType::Id(id) => id.to_string(),
                    IdType::Name(name) => name.clone(),
                };
                return Err(Status::invalid_argument(format!(
                    "Key path element must not be complete: [{}: {}]",
                    element.kind, id
                )));
            }
            (None, false) => {
                return Err(Status::invalid_argument(format!(
                    "Key path element must not be incomplete: [{}]",
                    element.kind
                )));
            }
            _ => {}
        }
    }
    Ok(())
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

/// The query's explicit order, or the implicit ascending order that
/// inequality properties require when no order is supplied.
fn effective_query_order(query: &Query) -> Vec<PropertyOrder> {
    if !query.order.is_empty() {
        return query.order.clone();
    }
    query
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
}

pub async fn lookup(
    storage: &Arc<RwLock<DatastoreStorage>>,
    mut req: LookupRequest,
) -> Result<LookupResponse, Status> {
    tracing::debug!(
        "Lookup request received: keys={:?} read_options={:?}",
        req.keys,
        req.read_options
    );
    for key in &mut req.keys {
        normalize_key_project(key, &req.project_id);
        check_request_database(&req.database_id, key.partition_id.as_ref())?;
    }
    let context = crate::read::context(
        storage,
        req.read_options.as_ref(),
        &req.project_id,
        &req.database_id,
    )?;
    let mut response = if let Some(id) = &context.id {
        let mut storage = storage.write();
        if !storage.transactions.contains_key(id) {
            return Err(invalid_transaction());
        }
        lookup_response(&req.keys, |key| {
            let key = KeyStruct::from_datastore_key(key);
            let entity = context.time.as_ref().map_or_else(
                || storage.entities.get(&key).cloned(),
                |time| storage.entity_at(&key, time),
            );
            let changed = match (storage.entities.get(&key), &entity) {
                (Some(current), Some(snapshot)) => !Arc::ptr_eq(current, snapshot),
                (None, None) => false,
                _ => true,
            };
            let transaction = storage
                .transactions
                .get_mut(id)
                .expect("validated transaction");
            if !transaction.read_only && changed {
                transaction.conflicted = true;
            }
            transaction.reads.entry(key).or_insert(entity).clone()
        })
    } else {
        let storage = storage.read();
        lookup_response(&req.keys, |key| {
            context.time.as_ref().map_or_else(
                || storage.get_entity(key),
                |time| storage.entity_at(&KeyStruct::from_datastore_key(key), time),
            )
        })
    };
    response.transaction = context.transaction;
    response.read_time = Some(
        context
            .time
            .unwrap_or_else(|| system_time_to_timestamp(SystemTime::now())),
    );
    Ok(response)
}

fn lookup_response(
    keys: &[Key],
    mut get_entity: impl FnMut(&Key) -> Option<Arc<EntityWithMetadata>>,
) -> LookupResponse {
    let mut found = Vec::new();
    let mut missing = Vec::new();

    for key in keys {
        let result_entity = get_entity(key);
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
    LookupResponse {
        found,
        missing,
        deferred: vec![],
        transaction: Vec::new(),
        read_time: None,
    }
}

/// Kindless queries may only order by ascending `__key__` and filter on `__key__`.
fn validate_kindless_query(query: &Query) -> Result<(), Status> {
    if query.order.iter().any(|order| {
        order.property.as_ref().is_none_or(|p| p.name != "__key__")
            || order.direction == property_order::Direction::Descending as i32
    }) {
        return Err(Status::invalid_argument(
            "kind is required for all orders except __key__ ascending",
        ));
    }
    if query.filter.as_ref().is_some_and(|f| !only_key_filters(f)) {
        return Err(Status::invalid_argument(
            "kind is required for non-__key__ filters",
        ));
    }
    Ok(())
}

fn only_key_filters(filter: &Filter) -> bool {
    match filter.filter_type.as_ref() {
        Some(FilterType::PropertyFilter(pf)) => {
            pf.property.as_ref().is_none_or(|p| p.name == "__key__")
        }
        Some(FilterType::CompositeFilter(cf)) => cf.filters.iter().all(only_key_filters),
        None => true,
    }
}

pub async fn run_query(
    storage: &Arc<RwLock<DatastoreStorage>>,
    req: RunQueryRequest,
) -> Result<RunQueryResponse, Status> {
    let explain_requested = req.explain_options.is_some();
    let start = explain_requested.then(Instant::now);
    let mut query_obj = match req.query_type {
        Some(crate::google::datastore::v1::run_query_request::QueryType::Query(query)) => query,
        _ => return Err(Status::invalid_argument("Missing or invalid query")),
    };
    if let Some(filter) = query_obj.filter.as_mut() {
        normalize_filter_projects(filter, &req.project_id);
    }
    // Datastore requires inequality filters to sort by the inequality
    // property first. If the client did not supply an explicit order,
    // inject an ascending implicit order for each inequality property.
    let order = effective_query_order(&query_obj);

    let filter_type = query_obj
        .filter
        .as_ref()
        .and_then(|f| f.filter_type.clone());
    check_request_database(&req.database_id, req.partition_id.as_ref())?;
    let namespace = req
        .partition_id
        .as_ref()
        .map_or("", |p| p.namespace_id.as_str());
    if query_obj.kind.is_empty() {
        validate_kindless_query(&query_obj)?;
    }

    let context = crate::read::context(
        storage,
        req.read_options.as_ref(),
        &req.project_id,
        &req.database_id,
    )?;
    if let Some(id) = &context.id {
        storage.write().observe_query(
            id,
            &req.project_id,
            &req.database_id,
            namespace,
            query_obj.kind.first().map(|kind| kind.name.as_str()),
        )?;
    }
    let storage = storage.read();
    let (candidates, candidates_prefiltered): (
        Box<dyn Iterator<Item = QueryCandidate> + '_>,
        bool,
    ) = if let Some(time) = context.time.as_ref().filter(|_| {
        query_obj
            .kind
            .first()
            .is_none_or(|kind| !crate::database::METADATA_KINDS.contains(&kind.name.as_str()))
    }) {
        (
            Box::new(storage.historical_candidates(
                &req.project_id,
                &req.database_id,
                namespace,
                query_obj.kind.first().map(|kind| kind.name.as_str()),
                time,
            )),
            false,
        )
    } else {
        match query_obj.kind.first() {
            Some(kind) => storage.query_candidates(
                &req.project_id,
                &kind.name,
                &req.database_id,
                namespace,
                filter_type.as_ref(),
            ),
            None => (
                Box::new(storage.kindless_candidates(&req.project_id, &req.database_id, namespace)),
                false,
            ),
        }
    };
    let mut batch = DatastoreStorage::get_entities_from_candidates(
        candidates,
        filter_type.as_ref(),
        candidates_prefiltered,
        query_obj.limit.as_ref().map(|v| v.value),
        query_obj.offset,
        query_obj.start_cursor.clone(),
        query_obj.end_cursor.clone(),
        query_obj.projection.clone(),
        query_obj.distinct_on.clone(),
        order,
    )?;
    batch.read_time = Some(
        context
            .time
            .unwrap_or_else(|| system_time_to_timestamp(SystemTime::now())),
    );
    drop(storage);
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
        transaction: context.transaction,
        query: None,
        batch: Some(batch),
        explain_metrics,
    })
}

pub async fn commit(
    storage: &Arc<RwLock<DatastoreStorage>>,
    mut req: CommitRequest,
) -> Result<CommitResponse, Status> {
    let is_transactional = req.mode == Mode::Transactional as i32;
    let is_non_transactional = req.mode == Mode::NonTransactional as i32;
    let has_transaction = matches!(
        req.transaction_selector,
        Some(TransactionSelector::Transaction(_))
    );
    // A single-use transaction selector also satisfies TRANSACTIONAL mode.
    if is_transactional && req.transaction_selector.is_none() {
        return Err(Status::invalid_argument(
            "transactional commit requires a transaction",
        ));
    }
    if is_non_transactional && has_transaction {
        return Err(Status::invalid_argument(
            "non-transaction commit cannot specify a transaction",
        ));
    }

    // Handle transaction if present
    let transaction_id = if let Some(transaction_selector) = req.transaction_selector {
        match transaction_selector {
            TransactionSelector::Transaction(tx_bytes) => match String::from_utf8(tx_bytes) {
                Ok(id) => Some(id),
                Err(_) => {
                    return Err(Status::invalid_argument("Invalid transaction ID format"));
                }
            },
            TransactionSelector::SingleUseTransaction(options) => {
                if matches!(options.mode, Some(transaction_options::Mode::ReadOnly(_))) {
                    return Err(Status::invalid_argument(
                        "A single-use commit transaction must be read-write.",
                    ));
                }
                None
            }
        }
    } else {
        None
    };

    for mutation in &mut req.mutations {
        for transform in &mut mutation.property_transforms {
            match transform.transform_type.as_mut() {
                Some(
                    TransformType::Increment(value)
                    | TransformType::Maximum(value)
                    | TransformType::Minimum(value),
                ) => normalize_value_projects(value, &req.project_id),
                Some(
                    TransformType::AppendMissingElements(array)
                    | TransformType::RemoveAllFromArray(array),
                ) => {
                    for value in &mut array.values {
                        normalize_value_projects(value, &req.project_id);
                    }
                }
                _ => {}
            }
        }
        if let Some(operation) = mutation.operation.as_mut() {
            match operation {
                Operation::Insert(entity)
                | Operation::Update(entity)
                | Operation::Upsert(entity) => {
                    normalize_entity_projects(entity, &req.project_id);
                }
                Operation::Delete(key) => normalize_key_project(key, &req.project_id),
            }
            validate_mutation(operation)?;
            let partition = mutation_key(operation).and_then(|key| key.partition_id.as_ref());
            check_request_database(&req.database_id, partition)?;
        }
    }
    if is_non_transactional && has_duplicate_complete_key(&req.mutations) {
        return Err(Status::invalid_argument(
            "A non-transactional commit may not contain multiple mutations affecting the same entity.",
        ));
    }

    // Everything above needs no storage; the write lock starts here.
    let mut storage = storage.write();
    if let Some(tx_id) = transaction_id.as_ref() {
        let transaction = storage
            .transactions
            .get(tx_id)
            .ok_or_else(invalid_transaction)?;
        if transaction.project_id != req.project_id || transaction.database_id != req.database_id {
            return Err(invalid_transaction());
        }
        if transaction.read_only && !req.mutations.is_empty() {
            return Err(Status::invalid_argument(
                "Cannot perform mutations in a read-only transaction.",
            ));
        }
        if transaction.conflicted {
            storage.clean_transaction(tx_id);
            return Err(Status::aborted(
                "too much contention on these datastore entities",
            ));
        }
        tracing::info!("Committing transaction: {}", tx_id);
    }
    // Non-transactional commits may apply partially, like Datastore.
    if is_transactional || transaction_id.is_some() {
        check_transactional_preconditions(&storage, &req.mutations)?;
    }

    let mut mutation_results = Vec::with_capacity(req.mutations.len());
    let commit_time = system_time_to_timestamp(SystemTime::now());

    // Finish validation before a transactional write can change any index.
    let transactional = is_transactional || transaction_id.is_some();
    let mut prepared_results = vec![Vec::new(); req.mutations.len()];
    if transactional
        && req.mutations.iter().any(|mutation| {
            mutation.property_mask.is_some()
                || !mutation.property_transforms.is_empty()
                || mutation.operation.is_none()
        })
    {
        let mut staged: HashMap<KeyStruct, Option<Entity>> = HashMap::new();
        for (index, mutation) in req.mutations.iter_mut().enumerate() {
            let key = mutation
                .operation
                .as_ref()
                .and_then(mutation_key)
                .filter(|key| !key_is_incomplete(key))
                .map(KeyStruct::from_datastore_key);
            let previous = key.as_ref().and_then(|key| {
                staged.get(key).cloned().unwrap_or_else(|| {
                    storage
                        .entities
                        .get(key)
                        .map(|metadata| metadata.entity.clone())
                })
            });
            prepared_results[index] = prepare_mutation(mutation, previous.as_ref(), &commit_time)?;
            if let Some(key) = key {
                let entity = match &mutation.operation {
                    Some(
                        Operation::Insert(entity)
                        | Operation::Update(entity)
                        | Operation::Upsert(entity),
                    ) => Some(entity.clone()),
                    _ => None,
                };
                staged.insert(key, entity);
            }
        }
    }

    for (index, mut mutation) in req.mutations.into_iter().enumerate() {
        let transform_results = if transactional {
            std::mem::take(&mut prepared_results[index])
        } else {
            let previous = mutation
                .operation
                .as_ref()
                .and_then(mutation_key)
                .filter(|key| !key_is_incomplete(key))
                .and_then(|key| storage.get_entity(key));
            prepare_mutation(
                &mut mutation,
                previous.as_ref().map(|metadata| &metadata.entity),
                &commit_time,
            )?
        };
        if let Some(mutation) = mutation.operation {
            match mutation {
                Operation::Insert(mut entity) => {
                    normalize_entity_values(&mut entity);
                    let allocates_key = entity.key.as_ref().is_some_and(key_is_incomplete);
                    if !allocates_key
                        && let Some(key) = entity.key.as_ref()
                        && storage
                            .entities
                            .contains_key(&KeyStruct::from_datastore_key(key))
                    {
                        return Err(Status::already_exists("entity already exists"));
                    }
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
                Operation::Update(mut entity) => {
                    normalize_entity_values(&mut entity);
                    let Some(key) = entity.key.as_ref() else {
                        return Err(Status::invalid_argument("Entity missing key for update"));
                    };
                    let Some(existing_entity_metadata) = storage.get_entity(key) else {
                        return Err(Status::not_found("Entity not found for update"));
                    };

                    let create_time = existing_entity_metadata.create_time.clone();
                    let version = existing_entity_metadata.version + 1;
                    storage.put_entity(Arc::new(EntityWithMetadata {
                        entity,
                        version,
                        create_time,
                        update_time: commit_time.clone(),
                    }));

                    mutation_results.push(commit_mutation_result(None, version as i64));
                }
                Operation::Upsert(mut entity) => {
                    normalize_entity_values(&mut entity);
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
                        let previous_metadata = storage.get_entity(key);
                        let version = previous_metadata
                            .as_ref()
                            .map_or(1, |previous| previous.version + 1);
                        let create_time = previous_metadata.map_or_else(
                            || commit_time.clone(),
                            |previous| previous.create_time.clone(),
                        );
                        storage.put_entity(Arc::new(EntityWithMetadata {
                            entity,
                            version,
                            create_time,
                            update_time: commit_time.clone(),
                        }));

                        mutation_results.push(commit_mutation_result(None, version as i64));
                    }
                }
                Operation::Delete(key_to_delete) => {
                    let version =
                        match storage.delete_entity_at(&key_to_delete, commit_time.clone()) {
                            Some(removed) => removed.version as i64 + 1,
                            None => {
                                tracing::warn!(
                                    "Entity not found for deletion with key: {:?}",
                                    key_to_delete.path
                                );
                                1
                            }
                        };
                    mutation_results.push(commit_mutation_result(None, version));
                }
            }
            if let Some(result) = mutation_results.last_mut() {
                result.transform_results = transform_results;
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
    let transaction_bytes = crate::read::start_transaction(
        &mut storage.write(),
        req.transaction_options.as_ref(),
        &req.project_id,
        &req.database_id,
    )?;

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

    let mut storage = storage.write();
    if storage
        .transactions
        .get(&transaction_id)
        .is_none_or(|transaction| {
            transaction.project_id != req.project_id || transaction.database_id != req.database_id
        })
    {
        return Err(invalid_transaction());
    }
    storage.clean_transaction(&transaction_id);
    tracing::debug!("Transaction {} rolled back successfully", transaction_id);

    Ok(RollbackResponse {})
}

pub async fn allocate_ids(
    storage: &Arc<RwLock<DatastoreStorage>>,
    mut req: AllocateIdsRequest,
) -> Result<AllocateIdsResponse, Status> {
    for key in &mut req.keys {
        normalize_key_project(key, &req.project_id);
    }
    let mut storage = storage.write();
    let mut allocated_keys = Vec::new();

    for incomplete_key in req.keys {
        if incomplete_key.path.is_empty() {
            return Err(Status::invalid_argument("Key path cannot be empty"));
        }
        validate_allocate_ids_path(&incomplete_key)?;
        check_request_database(&req.database_id, incomplete_key.partition_id.as_ref())?;

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
    mut req: ReserveIdsRequest,
) -> Result<ReserveIdsResponse, Status> {
    for key in &mut req.keys {
        normalize_key_project(key, &req.project_id);
    }
    let mut storage = storage.write();
    for key in &req.keys {
        if key.path.is_empty() {
            return Err(Status::invalid_argument("Key path cannot be empty"));
        }
        check_request_database(&req.database_id, key.partition_id.as_ref())?;
        storage.observe_key_id(key);
    }
    Ok(ReserveIdsResponse {})
}

#[derive(Clone, Copy)]
enum Number {
    Int(i64),
    Double(f64),
}

impl Number {
    fn as_f64(self) -> f64 {
        match self {
            Number::Int(i) => i as f64,
            Number::Double(d) => d,
        }
    }
}

/// Indexed integer and double values of `prop`; array elements are expanded
/// and entities without the property contribute nothing.
fn indexed_numbers(entities: &[QueryCandidate], prop: &str) -> Vec<Number> {
    let mut numbers = Vec::new();
    for candidate in entities {
        let Some(value) = candidate.entity_metadata.entity.properties.get(prop) else {
            continue;
        };
        if value.exclude_from_indexes {
            continue;
        }
        let elements = match &value.value_type {
            Some(ValueType::ArrayValue(array)) => array.values.as_slice(),
            _ => std::slice::from_ref(value),
        };
        for element in elements.iter().filter(|v| !v.exclude_from_indexes) {
            match &element.value_type {
                Some(ValueType::IntegerValue(i)) => numbers.push(Number::Int(*i)),
                Some(ValueType::DoubleValue(d)) => numbers.push(Number::Double(*d)),
                _ => {}
            }
        }
    }
    numbers
}

/// Integer when every value is an integer and the sum fits in i64, else double.
fn sum_numbers(numbers: &[Number]) -> ValueType {
    let exact = numbers.iter().try_fold(0i64, |acc, n| match n {
        Number::Int(i) => acc.checked_add(*i),
        Number::Double(_) => None,
    });
    match exact {
        Some(total) => ValueType::IntegerValue(total),
        None => ValueType::DoubleValue(numbers.iter().map(|n| n.as_f64()).sum()),
    }
}

pub async fn run_aggregation_query(
    storage: &Arc<RwLock<DatastoreStorage>>,
    req: RunAggregationQueryRequest,
) -> Result<RunAggregationQueryResponse, Status> {
    let explain_requested = req.explain_options.is_some();
    let start = explain_requested.then(Instant::now);
    check_request_database(&req.database_id, req.partition_id.as_ref())?;
    let namespace = req
        .partition_id
        .as_ref()
        .map_or("", |p| p.namespace_id.as_str());
    // Extract the aggregation query from the request
    let mut aggregation_query = match req.query_type {
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

    if let Some(crate::google::datastore::v1::aggregation_query::QueryType::NestedQuery(query)) =
        aggregation_query.query_type.as_mut()
        && let Some(filter) = query.filter.as_mut()
    {
        normalize_filter_projects(filter, &req.project_id);
    }

    // Empty aliases default to `property_N` (1-based position); duplicates are rejected.
    let mut seen_aliases = HashSet::new();
    let mut aliases = Vec::with_capacity(aggregation_query.aggregations.len());
    for (index, aggregation) in aggregation_query.aggregations.iter().enumerate() {
        let alias = if aggregation.alias.is_empty() {
            format!("property_{}", index + 1)
        } else {
            aggregation.alias.clone()
        };
        if !seen_aliases.insert(alias.clone()) {
            return Err(Status::invalid_argument(format!(
                "Aggregation aliases contain duplicate alias: {alias}."
            )));
        }
        aliases.push(alias);
    }

    let context = crate::read::context(
        storage,
        req.read_options.as_ref(),
        &req.project_id,
        &req.database_id,
    )?;
    if let Some(id) = &context.id
        && let Some(crate::google::datastore::v1::aggregation_query::QueryType::NestedQuery(query)) =
            &aggregation_query.query_type
    {
        storage.write().observe_query(
            id,
            &req.project_id,
            &req.database_id,
            namespace,
            query.kind.first().map(|kind| kind.name.as_str()),
        )?;
    }
    let storage = storage.read();
    let read_time = context
        .time
        .unwrap_or_else(|| system_time_to_timestamp(SystemTime::now()));

    let matching_entities: Vec<QueryCandidate> = match aggregation_query.query_type.as_ref() {
        Some(crate::google::datastore::v1::aggregation_query::QueryType::NestedQuery(query)) => {
            let order = effective_query_order(query);
            let filter_type = query.filter.as_ref().and_then(|f| f.filter_type.clone());

            let (candidates, candidates_prefiltered): (
                Box<dyn Iterator<Item = QueryCandidate> + '_>,
                bool,
            ) = if let Some(time) = context.time.as_ref().filter(|_| {
                query.kind.first().is_none_or(|kind| {
                    !crate::database::METADATA_KINDS.contains(&kind.name.as_str())
                })
            }) {
                (
                    Box::new(storage.historical_candidates(
                        &req.project_id,
                        &req.database_id,
                        namespace,
                        query.kind.first().map(|kind| kind.name.as_str()),
                        time,
                    )),
                    false,
                )
            } else {
                match query.kind.first() {
                    Some(kind) => storage.query_candidates(
                        &req.project_id,
                        &kind.name,
                        &req.database_id,
                        namespace,
                        filter_type.as_ref(),
                    ),
                    // An empty kind list is a kindless query over the whole namespace.
                    None => (
                        Box::new(storage.kindless_candidates(
                            &req.project_id,
                            &req.database_id,
                            namespace,
                        )),
                        false,
                    ),
                }
            };
            let candidates = candidates.filter(|c| c.entity_metadata.entity.key.is_some());

            DatastoreStorage::aggregation_candidates(
                candidates,
                filter_type.as_ref(),
                candidates_prefiltered,
                query.limit.as_ref().map(|v| v.value),
                query.offset,
                query.distinct_on.clone(),
                order,
            )
        }
        // todo: Handle other query types if needed
        _ => Vec::new(),
    };

    let mut aggregate_properties = HashMap::new();

    for (aggregation, alias) in aggregation_query.aggregations.iter().zip(aliases) {
        let Some(aggregation_operator) = &aggregation.operator else {
            continue;
        };
        let value_type = match aggregation_operator {
            AggregationOperator::Count(_count) => {
                ValueType::IntegerValue(matching_entities.len() as i64)
            }
            AggregationOperator::Sum(sum) => {
                let prop_name =
                    sum.property
                        .as_ref()
                        .map(|p| p.name.as_str())
                        .ok_or_else(|| {
                            Status::invalid_argument("Sum aggregation requires a property")
                        })?;
                if prop_name == "__key__" {
                    return Err(Status::invalid_argument(
                        "Aggregations are not supported for the property __key__",
                    ));
                }
                sum_numbers(&indexed_numbers(&matching_entities, prop_name))
            }
            AggregationOperator::Avg(avg) => {
                let prop_name =
                    avg.property
                        .as_ref()
                        .map(|p| p.name.as_str())
                        .ok_or_else(|| {
                            Status::invalid_argument("Avg aggregation requires a property")
                        })?;
                if prop_name == "__key__" {
                    return Err(Status::invalid_argument(
                        "Aggregations are not supported for the property __key__",
                    ));
                }
                let numbers = indexed_numbers(&matching_entities, prop_name);
                if numbers.is_empty() {
                    ValueType::NullValue(0)
                } else {
                    let total: f64 = numbers.iter().map(|n| n.as_f64()).sum();
                    ValueType::DoubleValue(total / numbers.len() as f64)
                }
            }
        };
        aggregate_properties.insert(
            alias,
            Value {
                exclude_from_indexes: false,
                meaning: 0,
                value_type: Some(value_type),
            },
        );
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
        transaction: context.transaction,
        explain_metrics,
    })
}
