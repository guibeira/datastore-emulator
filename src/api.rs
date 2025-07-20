use crate::google::datastore::v1::{ExplainMetrics, Query, QueryResultBatch};
use crate::import::bg_import_data;
use crate::operation::{OperationState, OperationStatus};
use crate::state::AppState;
use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
    routing::post,
};
use base64::Engine;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub struct WelcomeRequest {
    pub name: String,
}

#[derive(Serialize)]
pub struct WelcomeResponse {
    pub msg: String,
}

#[derive(Deserialize)]
pub struct ImportRequest {
    #[serde(rename = "inputUrl")]
    pub input_url: String,
}

#[derive(Serialize)]
pub struct ImportResponse {
    pub name: String,
    pub metadata: ImportMetadata,
}

#[derive(Serialize)]
pub struct ImportMetadata {
    #[serde(rename = "@type")]
    pub type_url: String,
    pub common: CommonMetadata,
    #[serde(rename = "entityFilter")]
    pub entity_filter: serde_json::Value,
    #[serde(rename = "inputUrl")]
    pub input_url: String,
}

#[derive(Serialize)]
pub struct CommonMetadata {
    #[serde(rename = "startTime")]
    pub start_time: String,
    #[serde(rename = "operationType")]
    pub operation_type: String,
    pub state: String,
}

pub async fn import_handler(
    State(state): State<AppState>,
    Path(project_id_with_operation): Path<String>,
    Json(payload): Json<ImportRequest>,
) -> Json<ImportResponse> {
    let mut path_parameters = project_id_with_operation.split(":");
    let project_id = path_parameters
        .next()
        .unwrap_or("default_project")
        .to_string();
    let _action_parameter = path_parameters.next().unwrap_or("import").to_string();
    let operation_id = Uuid::new_v4().to_string();
    let operation_state = OperationState {
        status: OperationStatus::Processing,
        start_time: Utc::now(),
        end_time: None,
        error: None,
    };
    state
        .operations
        .lock()
        .unwrap()
        .insert(operation_id.clone(), operation_state);
    tokio::spawn(bg_import_data(
        state.storage.clone(),
        state.operations.clone(),
        operation_id.clone(),
        payload.input_url.clone(),
    ));
    let response = ImportResponse {
        name: format!("projects/{}/operations/{}", project_id, operation_id),
        metadata: ImportMetadata {
            type_url: "type.googleapis.com/google.datastore.admin.v1.ImportEntitiesMetadata"
                .to_string(),
            common: CommonMetadata {
                start_time: Utc::now().to_rfc3339(),
                operation_type: "IMPORT_ENTITIES".to_string(),
                state: "PROCESSING".to_string(),
            },
            entity_filter: serde_json::json!({}),
            input_url: payload.input_url,
        },
    };
    Json(response)
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct HttpPathElement {
    pub kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct HttpKey {
    pub partition_id: Option<crate::google::datastore::v1::PartitionId>,
    pub path: Vec<HttpPathElement>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct HttpEntity {
    pub key: Option<HttpKey>,
    pub properties: std::collections::HashMap<String, crate::google::datastore::v1::Value>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct HttpEntityResult {
    pub entity: Option<HttpEntity>,
    pub version: i64,
    pub cursor: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub update_time: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub create_time: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct RunQueryHttpResponse {
    pub batch: Option<QueryResultBatchHttpResponse>,
    pub query: Option<Query>,
    pub transaction: Option<String>,
    pub explain_metrics: Option<ExplainMetrics>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct QueryResultBatchHttpResponse {
    pub skipped_results: i32,
    pub skipped_cursor: String,
    pub entity_result_type: String,
    pub entity_results: Vec<HttpEntityResult>,
    pub end_cursor: String,
    pub more_results: String,
    pub snapshot_version: String,
    pub read_time: Option<String>,
}

pub async fn run_query_handler(
    State(state): State<AppState>,
    Path(project_id): Path<String>,
    Json(payload): Json<serde_json::Value>,
) -> Response {
    let query_payload = match payload.get("query") {
        Some(q) => q.clone(),
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": "Missing query object in request body" })),
            )
                .into_response();
        }
    };

    let query: Query = match serde_json::from_value(query_payload) {
        Ok(q) => q,
        Err(e) => {
            tracing::error!("Failed to deserialize Query: {}", e);
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": format!("Invalid query object: {}", e) })),
            )
                .into_response();
        }
    };

    let kind_name = query.kind.first().map(|k| k.name.clone());

    if kind_name.is_none() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "Missing kind in query" })),
        )
            .into_response();
    }
    let kind_name = kind_name.unwrap();
    let storage = state.storage.lock().unwrap();

    let batch: QueryResultBatch = storage.get_entities(
        project_id,
        kind_name,
        query.filter.clone(),
        query.limit,
        query.start_cursor.clone(),
        query.projection.clone(),
        query.order.clone(),
    );

    // Extract borrows before moving from `batch` to avoid partial move errors.
    let skipped_cursor =
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&batch.skipped_cursor);
    let entity_result_type = batch.entity_result_type().as_str_name().to_string();
    let end_cursor = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&batch.end_cursor);
    let more_results = batch.more_results().as_str_name().to_string();
    let snapshot_version = batch.snapshot_version.to_string();
    let read_time = batch
        .read_time
        .as_ref()
        .and_then(|t| DateTime::from_timestamp(t.seconds, t.nanos as u32))
        .map(|dt| dt.to_rfc3339());

    let http_entity_results: Vec<HttpEntityResult> = batch
        .entity_results
        .into_iter()
        .map(|er| {
            let entity = er.entity.map(|e| {
                let key = e.key.map(|k| {
                    let path = k
                        .path
                        .into_iter()
                        .map(|pe| {
                            let (id, name) = match pe.id_type {
                                Some(
                                    crate::google::datastore::v1::key::path_element::IdType::Id(i),
                                ) => (Some(i.to_string()), None),
                                Some(
                                    crate::google::datastore::v1::key::path_element::IdType::Name(
                                        n,
                                    ),
                                ) => (None, Some(n)),
                                None => (None, None),
                            };
                            HttpPathElement {
                                kind: pe.kind,
                                id,
                                name,
                            }
                        })
                        .collect();
                    HttpKey {
                        partition_id: k.partition_id,
                        path,
                    }
                });
                HttpEntity {
                    key,
                    properties: e.properties,
                }
            });
            HttpEntityResult {
                entity,
                version: er.version,
                cursor: base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&er.cursor),
                update_time: er
                    .update_time
                    .as_ref()
                    .and_then(|t| DateTime::from_timestamp(t.seconds, t.nanos as u32))
                    .map(|dt| dt.to_rfc3339()),
                create_time: er
                    .create_time
                    .as_ref()
                    .and_then(|t| DateTime::from_timestamp(t.seconds, t.nanos as u32))
                    .map(|dt| dt.to_rfc3339()),
            }
        })
        .collect();

    let http_batch = QueryResultBatchHttpResponse {
        skipped_results: batch.skipped_results,
        skipped_cursor,
        entity_result_type,
        entity_results: http_entity_results,
        end_cursor,
        more_results,
        snapshot_version,
        read_time,
    };

    axum::Json(RunQueryHttpResponse {
        batch: Some(http_batch),
        query: Some(query),
        transaction: None,
        explain_metrics: None,
    })
    .into_response()
}

pub async fn match_handler(
    State(state): State<AppState>,
    Path(project_id_with_operation): Path<String>,
    Json(payload): Json<serde_json::Value>,
) -> Response {
    let mut path_parameters = project_id_with_operation.split(":");
    let project_id = path_parameters
        .next()
        .unwrap_or("default_project")
        .to_string();
    let action_parameter = path_parameters.next().unwrap_or("import").to_string();
    match action_parameter.as_str() {
        "import" => {
            let import_payload: ImportRequest = match serde_json::from_value(payload) {
                Ok(p) => p,
                Err(e) => {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(serde_json::json!({ "error": e.to_string() })),
                    )
                        .into_response();
                }
            };
            import_handler(
                State(state),
                Path(project_id_with_operation),
                Json(import_payload),
            )
            .await
            .into_response()
        }
        "runQuery" => run_query_handler(State(state), Path(project_id), Json(payload)).await,
        _ => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "Unknown action" })),
        )
            .into_response(),
    }
}

pub async fn get_operation_status(
    State(state): State<AppState>,
    Path((_project_id, operation_id)): Path<(String, String)>,
) -> impl IntoResponse {
    let operations = state.operations.lock().unwrap();
    if let Some(state) = operations.get(&operation_id) {
        (StatusCode::OK, Json(state.clone())).into_response()
    } else {
        (StatusCode::NOT_FOUND, "Operation not found").into_response()
    }
}

pub fn create_router(state: AppState) -> Router {
    Router::new()
        .route(
            "/v1/projects/:project_id_with_operation",
            post(match_handler),
        )
        .route(
            "/v1/projects/:project_id/operations/:operation_id",
            get(get_operation_status),
        )
        .with_state(state)
}
