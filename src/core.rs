use crate::database::DatastoreStorage;
use crate::google::datastore::v1::{
    Entity, EntityResult, ExecutionStats, ExplainMetrics, LookupRequest, LookupResponse,
    PlanSummary, RunQueryRequest, RunQueryResponse,
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
    let batch = storage.get_entities(
        req.project_id.clone(),
        kind_name,
        query_obj.filter.clone(),
        query_obj.limit.as_ref().map(|v| v.value),
        query_obj.start_cursor.clone(),
        query_obj.projection.clone(),
        query_obj.distinct_on.clone(),
        query_obj.order.clone(),
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
