use crate::core;
use crate::google::datastore::v1::{
    AllocateIdsRequest, BeginTransactionRequest, CommitRequest, LookupRequest, ReserveIdsRequest,
    RollbackRequest, RunAggregationQueryRequest, RunQueryRequest,
};
use crate::rest_error::{
    PROTOBUF_CONTENT_TYPE, bad_request, not_found, status_to_protobuf_response, status_to_response,
};
use crate::state::AppState;
use axum::{
    Json,
    body::Bytes,
    extract::{Path, State},
    http::{HeaderMap, StatusCode, header::CONTENT_TYPE},
    response::{IntoResponse, Response},
};
use prost::Message;
use serde::Serialize;
use serde::de::DeserializeOwned;

/// How a request body and its response are encoded. The Google clients use
/// JSON through the REST API and protobuf (`application/x-protobuf`) when
/// gRPC is disabled.
#[derive(Clone, Copy)]
enum Wire {
    Json,
    Protobuf,
}

impl Wire {
    fn from_headers(headers: &HeaderMap) -> Self {
        let is_protobuf = headers
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.starts_with(PROTOBUF_CONTENT_TYPE));
        if is_protobuf {
            Wire::Protobuf
        } else {
            Wire::Json
        }
    }

    fn error(self, status: tonic::Status) -> Response {
        match self {
            Wire::Json => status_to_response(status),
            Wire::Protobuf => status_to_protobuf_response(status),
        }
    }
}

/// Requests with a top-level `project_id`. The URL project is authoritative
/// and clients (dsadmin among them) commonly omit it from the body.
trait HasProjectId {
    fn project_id(&self) -> &str;
    fn set_project_id(&mut self, project_id: &str);
}

macro_rules! has_project_id {
    ($($request:ty),* $(,)?) => {
        $(impl HasProjectId for $request {
            fn project_id(&self) -> &str {
                &self.project_id
            }
            fn set_project_id(&mut self, project_id: &str) {
                self.project_id = project_id.to_string();
            }
        })*
    };
}

has_project_id!(
    LookupRequest,
    RunQueryRequest,
    RunAggregationQueryRequest,
    CommitRequest,
    BeginTransactionRequest,
    RollbackRequest,
    AllocateIdsRequest,
    ReserveIdsRequest,
);

/// Decodes the body once, fills in the URL project when the body has none,
/// runs `op` and encodes the result on the same wire.
async fn call<Req, Resp, F, Fut>(wire: Wire, body: Bytes, project_id: &str, op: F) -> Response
where
    Req: DeserializeOwned + Message + Default + HasProjectId,
    Resp: Serialize + Message,
    F: FnOnce(Req) -> Fut,
    Fut: std::future::Future<Output = Result<Resp, tonic::Status>>,
{
    let decoded = match wire {
        Wire::Json => {
            let body: &[u8] = if body.is_empty() { b"{}" } else { &body };
            serde_json::from_slice::<Req>(body).map_err(|e| format!("invalid JSON: {e}"))
        }
        Wire::Protobuf => Req::decode(body).map_err(|e| format!("invalid protobuf: {e}")),
    };
    let mut req = match decoded {
        Ok(req) => req,
        Err(message) => return wire.error(tonic::Status::invalid_argument(message)),
    };

    if req.project_id().is_empty() {
        req.set_project_id(project_id);
    } else if req.project_id() != project_id {
        return wire.error(tonic::Status::invalid_argument(format!(
            "projectId in body ('{}') must match URL project ('{project_id}')",
            req.project_id()
        )));
    }

    match op(req).await {
        Ok(resp) => match wire {
            Wire::Json => (StatusCode::OK, Json(resp)).into_response(),
            Wire::Protobuf => (
                StatusCode::OK,
                [(CONTENT_TYPE, PROTOBUF_CONTENT_TYPE)],
                resp.encode_to_vec(),
            )
                .into_response(),
        },
        Err(status) => wire.error(status),
    }
}

pub async fn datastore_method_handler(
    State(state): State<AppState>,
    Path(project_method): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let (project, method) = match project_method.split_once(':') {
        Some(p) => p,
        None => return bad_request("missing :method suffix on /v1/projects/{project}:{method}"),
    };
    let wire = Wire::from_headers(&headers);
    let storage = &state.storage;

    match method {
        "lookup" => call(wire, body, project, |r| core::lookup(storage, r)).await,
        "runQuery" => call(wire, body, project, |r| core::run_query(storage, r)).await,
        "commit" => call(wire, body, project, |r| core::commit(storage, r)).await,
        "beginTransaction" => {
            call(wire, body, project, |r| core::begin_transaction(storage, r)).await
        }
        "rollback" => call(wire, body, project, |r| core::rollback(storage, r)).await,
        "allocateIds" => call(wire, body, project, |r| core::allocate_ids(storage, r)).await,
        "reserveIds" => call(wire, body, project, |r| core::reserve_ids(storage, r)).await,
        "runAggregationQuery" => {
            call(wire, body, project, |r| {
                core::run_aggregation_query(storage, r)
            })
            .await
        }
        "import" => import_handler(state.clone(), project.to_string(), body).await,
        "export" => export_handler().await,
        other => not_found(&format!("unknown Datastore method: {other}")),
    }
}

async fn export_handler() -> Response {
    status_to_response(tonic::Status::unimplemented(
        "ExportEntities not yet implemented in datastore-emulator",
    ))
}

use crate::import::bg_import_data;
use crate::operation::{OperationState, OperationStatus};
use chrono::Utc;
use serde::Deserialize;
use uuid::Uuid;

#[derive(Deserialize)]
pub struct ImportRequest {
    #[serde(rename = "inputUrl", alias = "input_url")]
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

pub async fn import_handler(state: AppState, project_id: String, body: Bytes) -> Response {
    let payload: ImportRequest = match serde_json::from_slice(&body) {
        Ok(r) => r,
        Err(e) => return bad_request(&format!("invalid JSON: {e}")),
    };

    let operation_id = Uuid::new_v4().to_string();
    let operation_start_time = Utc::now();
    let operation_state = OperationState {
        status: OperationStatus::Processing,
        start_time: operation_start_time,
        end_time: None,
        error: None,
    };
    state
        .operations
        .write()
        .insert(operation_id.clone(), operation_state);

    bg_import_data(
        state.storage.clone(),
        state.operations.clone(),
        operation_id.clone(),
        payload.input_url.clone(),
        Some(project_id.clone()),
    )
    .await;

    let (final_state, start_time) = state
        .operations
        .read()
        .get(&operation_id)
        .map(|s| {
            let state = match s.status {
                OperationStatus::Successful => "SUCCESSFUL",
                OperationStatus::Failed => "FAILED",
                OperationStatus::Processing => "PROCESSING",
            };
            (state.to_string(), s.start_time.to_rfc3339())
        })
        .unwrap_or_else(|| ("PROCESSING".to_string(), operation_start_time.to_rfc3339()));

    let response = ImportResponse {
        name: format!("projects/{}/operations/{}", project_id, operation_id),
        metadata: ImportMetadata {
            type_url: "type.googleapis.com/google.datastore.admin.v1.ImportEntitiesMetadata"
                .to_string(),
            common: CommonMetadata {
                start_time,
                operation_type: "IMPORT_ENTITIES".to_string(),
                state: final_state,
            },
            entity_filter: serde_json::json!({}),
            input_url: payload.input_url,
        },
    };

    (StatusCode::OK, Json(response)).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::create_router;
    use crate::database::{DatastoreStorage, EntityWithMetadata};
    use crate::google::datastore::v1::key::PathElement;
    use crate::google::datastore::v1::key::path_element::IdType;
    use crate::google::datastore::v1::{Entity, Key, PartitionId};
    use axum::body::Body;
    use axum::http::Request;
    use parking_lot::RwLock;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tower::ServiceExt;

    fn seeded_state(project: &str, kind: &str, name: &str) -> AppState {
        let mut storage = DatastoreStorage::default();
        let datastore_key = Key {
            partition_id: Some(PartitionId {
                project_id: project.to_string(),
                database_id: String::new(),
                namespace_id: String::new(),
            }),
            path: vec![PathElement {
                kind: kind.to_string(),
                id_type: Some(IdType::Name(name.to_string())),
            }],
        };
        storage.put_entity(
            EntityWithMetadata {
                entity: Entity {
                    key: Some(datastore_key),
                    properties: HashMap::new(),
                },
                version: 1,
                create_time: pbjson_types::Timestamp::default(),
                update_time: pbjson_types::Timestamp::default(),
            }
            .into(),
        );
        AppState {
            storage: Arc::new(RwLock::new(storage)),
            operations: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    #[tokio::test]
    async fn project_id_mismatch_between_url_and_body_returns_400() {
        let state = seeded_state("p1", "Task", "abc");
        let app = create_router(state);

        let body = serde_json::json!({
            "projectId": "p2",
            "keys": [{
                "partitionId": { "projectId": "p2" },
                "path": [{ "kind": "Task", "name": "abc" }]
            }]
        });

        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/projects/p1:lookup")
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let bytes = hyper::body::to_bytes(resp.into_body()).await.unwrap();
        let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert!(
            v["error"]["message"]
                .as_str()
                .unwrap()
                .contains("must match URL project")
        );
    }

    #[tokio::test]
    async fn lookup_accepts_and_answers_protobuf() {
        use crate::google::datastore::v1::{LookupRequest, LookupResponse};
        use prost::Message;

        let state = seeded_state("p1", "Task", "abc");
        let app = create_router(state);
        let request = LookupRequest {
            project_id: "p1".to_string(),
            keys: vec![Key {
                partition_id: Some(PartitionId {
                    project_id: "p1".to_string(),
                    ..Default::default()
                }),
                path: vec![PathElement {
                    kind: "Task".to_string(),
                    id_type: Some(IdType::Name("abc".to_string())),
                }],
            }],
            ..Default::default()
        };

        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/projects/p1:lookup")
                    .header("content-type", "application/x-protobuf")
                    .body(Body::from(request.encode_to_vec()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.headers()["content-type"], "application/x-protobuf");
        let bytes = hyper::body::to_bytes(resp.into_body()).await.unwrap();
        let decoded = LookupResponse::decode(bytes).unwrap();
        assert_eq!(decoded.found.len(), 1);
        let key = decoded.found[0]
            .entity
            .as_ref()
            .unwrap()
            .key
            .as_ref()
            .unwrap();
        assert_eq!(key.path[0].kind, "Task");
    }

    #[tokio::test]
    async fn protobuf_error_body_is_an_rpc_status() {
        use crate::google::datastore::v1::RollbackRequest;
        use prost::Message;

        let state = seeded_state("p1", "Task", "abc");
        let app = create_router(state);
        let request = RollbackRequest {
            project_id: "p1".to_string(),
            transaction: b"nope".to_vec(),
            ..Default::default()
        };

        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/projects/p1:rollback")
                    .header("content-type", "application/x-protobuf")
                    .body(Body::from(request.encode_to_vec()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        assert_eq!(resp.headers()["content-type"], "application/x-protobuf");
        let bytes = hyper::body::to_bytes(resp.into_body()).await.unwrap();
        let status = crate::rest_error::RpcStatus::decode(bytes).unwrap();
        assert_eq!(status.code, tonic::Code::InvalidArgument as i32);
        assert!(!status.message.is_empty());
    }

    #[tokio::test]
    async fn lookup_returns_found_entity_in_camelcase_json() {
        let state = seeded_state("p1", "Task", "abc");
        let app = create_router(state);

        let body = serde_json::json!({
            "keys": [{
                "partitionId": { "projectId": "p1" },
                "path": [{ "kind": "Task", "name": "abc" }]
            }]
        });

        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/projects/p1:lookup")
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = hyper::body::to_bytes(resp.into_body()).await.unwrap();
        let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert!(v["found"].is_array());
        assert_eq!(v["found"][0]["entity"]["key"]["path"][0]["kind"], "Task");
        assert_eq!(v["found"][0]["entity"]["key"]["path"][0]["name"], "abc");
    }

    #[tokio::test]
    async fn run_query_kind_returns_entities() {
        let state = seeded_state("p1", "Task", "abc");
        let app = create_router(state);

        let body = serde_json::json!({
            "projectId": "p1",
            "query": { "kind": [{ "name": "Task" }] }
        });

        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/projects/p1:runQuery")
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = hyper::body::to_bytes(resp.into_body()).await.unwrap();
        let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert!(v["batch"].is_object());
    }

    #[tokio::test]
    async fn commit_insert_creates_entity() {
        let state = AppState {
            storage: Arc::new(RwLock::new(DatastoreStorage::default())),
            operations: Arc::new(RwLock::new(HashMap::new())),
        };
        let app = create_router(state.clone());

        let body = serde_json::json!({
            "mode": "NON_TRANSACTIONAL",
            "mutations": [{
                "insert": {
                    "key": {
                        "partitionId": { "projectId": "p1" },
                        "path": [{ "kind": "Task", "name": "abc" }]
                    },
                    "properties": {
                        "title": { "stringValue": "hello" }
                    }
                }
            }]
        });

        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/projects/p1:commit")
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = hyper::body::to_bytes(resp.into_body()).await.unwrap();
        let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert!(v["mutationResults"].is_array());
        assert_eq!(state.storage.read().entities.len(), 1);
    }

    #[tokio::test]
    async fn begin_transaction_returns_transaction_id() {
        let state = AppState {
            storage: Arc::new(RwLock::new(DatastoreStorage::default())),
            operations: Arc::new(RwLock::new(HashMap::new())),
        };
        let app = create_router(state);

        let body = serde_json::json!({});

        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/projects/p1:beginTransaction")
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = hyper::body::to_bytes(resp.into_body()).await.unwrap();
        let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert!(v["transaction"].as_str().unwrap().len() > 0);
    }

    #[tokio::test]
    async fn rollback_returns_bad_request_for_unknown_tx() {
        use base64::Engine;
        let state = AppState {
            storage: Arc::new(RwLock::new(DatastoreStorage::default())),
            operations: Arc::new(RwLock::new(HashMap::new())),
        };
        let app = create_router(state);

        let body = serde_json::json!({
            "transaction": base64::engine::general_purpose::STANDARD.encode("tx-0-0")
        });

        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/projects/p1:rollback")
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn allocate_ids_fills_incomplete_keys() {
        let state = AppState {
            storage: Arc::new(RwLock::new(DatastoreStorage::default())),
            operations: Arc::new(RwLock::new(HashMap::new())),
        };
        let app = create_router(state);

        let body = serde_json::json!({
            "keys": [{
                "partitionId": { "projectId": "p1" },
                "path": [{ "kind": "Task" }]
            }]
        });

        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/projects/p1:allocateIds")
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = hyper::body::to_bytes(resp.into_body()).await.unwrap();
        let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert!(
            v["keys"][0]["path"][0]["id"]
                .as_str()
                .unwrap()
                .parse::<i64>()
                .is_ok()
        );
    }

    #[tokio::test]
    async fn reserve_ids_accepts_explicit_keys() {
        let state = AppState {
            storage: Arc::new(RwLock::new(DatastoreStorage::default())),
            operations: Arc::new(RwLock::new(HashMap::new())),
        };
        let app = create_router(state);

        let body = serde_json::json!({
            "keys": [{
                "partitionId": { "projectId": "p1" },
                "path": [{ "kind": "Task", "id": "42" }]
            }]
        });

        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/projects/p1:reserveIds")
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn run_aggregation_query_count_returns_total() {
        let state = seeded_state("p1", "Task", "abc");
        let app = create_router(state);

        let body = serde_json::json!({
            "projectId": "p1",
            "aggregationQuery": {
                "nestedQuery": { "kind": [{ "name": "Task" }] },
                "aggregations": [{ "count": {}, "alias": "total" }]
            }
        });

        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/projects/p1:runAggregationQuery")
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = hyper::body::to_bytes(resp.into_body()).await.unwrap();
        let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            v["batch"]["aggregationResults"][0]["aggregateProperties"]["total"]["integerValue"],
            "1"
        );
    }

    #[tokio::test]
    async fn lookup_returns_missing_for_unknown_key() {
        let state = seeded_state("p1", "Task", "abc");
        let app = create_router(state);

        let body = serde_json::json!({
            "keys": [{
                "partitionId": { "projectId": "p1" },
                "path": [{ "kind": "Task", "name": "missing" }]
            }]
        });

        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/projects/p1:lookup")
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = hyper::body::to_bytes(resp.into_body()).await.unwrap();
        let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert!(v["missing"].is_array());
        assert!(!v["missing"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn import_accepts_camel_case() {
        let state = AppState {
            storage: Arc::new(RwLock::new(DatastoreStorage::default())),
            operations: Arc::new(RwLock::new(HashMap::new())),
        };
        let app = create_router(state.clone());

        let body = serde_json::json!({ "inputUrl": "/nonexistent/path.zip" });

        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/projects/p1:import")
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = hyper::body::to_bytes(resp.into_body()).await.unwrap();
        let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(v["metadata"]["common"]["state"], "FAILED");

        let operations = state.operations.read();
        assert_eq!(operations.len(), 1);
        let operation = operations.values().next().unwrap();
        assert_eq!(
            v["metadata"]["common"]["startTime"],
            operation.start_time.to_rfc3339()
        );
        assert!(matches!(operation.status.clone(), OperationStatus::Failed));
        assert!(
            operation
                .error
                .as_deref()
                .unwrap_or_default()
                .contains("File not found")
        );
    }

    #[tokio::test]
    async fn import_accepts_snake_case_alias() {
        let state = AppState {
            storage: Arc::new(RwLock::new(DatastoreStorage::default())),
            operations: Arc::new(RwLock::new(HashMap::new())),
        };
        let app = create_router(state.clone());

        let body = serde_json::json!({ "input_url": "/nonexistent/path.zip" });

        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/projects/p1:import")
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(state.operations.read().len(), 1);
    }

    #[tokio::test]
    async fn export_returns_501_unimplemented() {
        let state = AppState {
            storage: Arc::new(RwLock::new(DatastoreStorage::default())),
            operations: Arc::new(RwLock::new(HashMap::new())),
        };
        let app = create_router(state);

        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/projects/p1:export")
                    .header("content-type", "application/json")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_IMPLEMENTED);
    }

    #[tokio::test]
    async fn unknown_method_returns_404() {
        let state = AppState {
            storage: Arc::new(RwLock::new(DatastoreStorage::default())),
            operations: Arc::new(RwLock::new(HashMap::new())),
        };
        let app = create_router(state);

        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/projects/p1:bogus")
                    .header("content-type", "application/json")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        let bytes = hyper::body::to_bytes(resp.into_body()).await.unwrap();
        let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(v["error"]["code"], 404);
        assert!(v["error"]["message"].as_str().unwrap().contains("bogus"));
    }

    #[tokio::test]
    async fn missing_method_suffix_returns_400() {
        let state = AppState {
            storage: Arc::new(RwLock::new(DatastoreStorage::default())),
            operations: Arc::new(RwLock::new(HashMap::new())),
        };
        let app = create_router(state);

        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/projects/p1")
                    .header("content-type", "application/json")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn invalid_json_returns_400() {
        let state = AppState {
            storage: Arc::new(RwLock::new(DatastoreStorage::default())),
            operations: Arc::new(RwLock::new(HashMap::new())),
        };
        let app = create_router(state);

        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/projects/p1:lookup")
                    .header("content-type", "application/json")
                    .body(Body::from("not json"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn cors_preflight_succeeds() {
        let state = AppState {
            storage: Arc::new(RwLock::new(DatastoreStorage::default())),
            operations: Arc::new(RwLock::new(HashMap::new())),
        };
        let app = create_router(state);

        let resp = app
            .oneshot(
                Request::builder()
                    .method("OPTIONS")
                    .uri("/v1/projects/p1:lookup")
                    .header("origin", "http://localhost:3000")
                    .header("access-control-request-method", "POST")
                    .header("access-control-request-headers", "content-type")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(resp.status().is_success());
        assert_eq!(
            resp.headers()
                .get("access-control-allow-origin")
                .map(|v| v.to_str().unwrap_or("")),
            Some("*")
        );
    }

    #[tokio::test]
    async fn cors_actual_post_has_allow_origin_header() {
        let state = seeded_state("p1", "Task", "abc");
        let app = create_router(state);

        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/projects/p1:lookup")
                    .header("origin", "http://localhost:3000")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"keys":[]}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(resp.headers().get("access-control-allow-origin").is_some());
    }
}
