use crate::core;
use crate::rest_error::{bad_request, not_found, status_to_response};
use crate::state::AppState;
use axum::{
    Json,
    body::Bytes,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::Serialize;
use serde::de::DeserializeOwned;

async fn json_call<Req, Resp, F, Fut>(body: Bytes, op: F) -> Response
where
    Req: DeserializeOwned,
    Resp: Serialize,
    F: FnOnce(Req) -> Fut,
    Fut: std::future::Future<Output = Result<Resp, tonic::Status>>,
{
    let req: Req = match serde_json::from_slice(&body) {
        Ok(r) => r,
        Err(e) => return bad_request(&format!("invalid JSON: {e}")),
    };
    match op(req).await {
        Ok(resp) => (StatusCode::OK, Json(resp)).into_response(),
        Err(status) => status_to_response(status),
    }
}

/// Inject `projectId` from the URL path into the JSON body if absent.
/// Google's Datastore REST API treats the URL `project` as authoritative; clients
/// (including dsadmin) commonly omit it from the body. Our core handlers read
/// `req.project_id`, so we copy the URL segment in when the client didn't supply one.
/// Empty body is treated as `{}`.
fn inject_project_id(body: &Bytes, project_id: &str) -> Result<Bytes, String> {
    let mut v: serde_json::Value = if body.is_empty() {
        serde_json::Value::Object(Default::default())
    } else {
        serde_json::from_slice(body).map_err(|e| format!("invalid JSON: {e}"))?
    };
    if let serde_json::Value::Object(map) = &mut v {
        match map.get("projectId").and_then(|v| v.as_str()) {
            Some(existing) if existing != project_id => {
                return Err(format!(
                    "projectId in body ('{existing}') must match URL project ('{project_id}')"
                ));
            }
            Some(_) => {}
            None => {
                map.insert(
                    "projectId".to_string(),
                    serde_json::Value::String(project_id.to_string()),
                );
            }
        }
    }
    Ok(Bytes::from(serde_json::to_vec(&v).unwrap()))
}

pub async fn datastore_method_handler(
    State(state): State<AppState>,
    Path(project_method): Path<String>,
    body: Bytes,
) -> Response {
    let (project_id, method) = match project_method.split_once(':') {
        Some(p) => p,
        None => return bad_request("missing :method suffix on /v1/projects/{project}:{method}"),
    };

    // Methods carrying a top-level projectId in their proto. Inject from URL when missing.
    let body = match method {
        "lookup" | "runQuery" | "runAggregationQuery" | "commit" | "beginTransaction"
        | "rollback" | "allocateIds" | "reserveIds" => match inject_project_id(&body, project_id) {
            Ok(b) => b,
            Err(e) => return bad_request(&e),
        },
        _ => body,
    };

    match method {
        "lookup" => json_call(body, |r| core::lookup(&state.storage, r)).await,
        "runQuery" => json_call(body, |r| core::run_query(&state.storage, r)).await,
        "commit" => json_call(body, |r| core::commit(&state.storage, r)).await,
        "beginTransaction" => json_call(body, |r| core::begin_transaction(&state.storage, r)).await,
        "rollback" => json_call(body, |r| core::rollback(&state.storage, r)).await,
        "allocateIds" => json_call(body, |r| core::allocate_ids(&state.storage, r)).await,
        "reserveIds" => json_call(body, |r| core::reserve_ids(&state.storage, r)).await,
        "runAggregationQuery" => json_call(body, |r| core::run_aggregation_query(&state.storage, r)).await,
        "import" => import_handler(state, project_id.to_string(), body).await,
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
        start_time: operation_start_time.clone(),
        end_time: None,
        error: None,
    };
    state
        .operations
        .write()
        .await
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
        .await
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
    use crate::database::{DatastoreStorage, EntityWithMetadata, KeyId, KeyStruct};
    use crate::google::datastore::v1::key::path_element::IdType;
    use crate::google::datastore::v1::key::PathElement;
    use crate::google::datastore::v1::{Entity, Key, PartitionId};
    use axum::body::Body;
    use axum::http::Request;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::RwLock;
    use tower::ServiceExt;

    fn seeded_state(project: &str, kind: &str, name: &str) -> AppState {
        let mut storage = DatastoreStorage::default();
        let key_struct = KeyStruct {
            project_id: project.to_string(),
            namespace: String::new(),
            path_elements: vec![(kind.to_string(), KeyId::StringId(name.to_string()))],
        };
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
        storage.entities.insert(
            key_struct,
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
        assert!(v["error"]["message"]
            .as_str()
            .unwrap()
            .contains("must match URL project"));
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
        assert_eq!(state.storage.read().await.entities.len(), 1);
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
    async fn rollback_returns_ok_for_unknown_tx() {
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
        assert_eq!(resp.status(), StatusCode::OK);
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
        assert!(v["keys"][0]["path"][0]["id"].as_str().unwrap().parse::<i64>().is_ok());
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

        let operations = state.operations.read().await;
        assert_eq!(operations.len(), 1);
        let operation = operations.values().next().unwrap();
        assert_eq!(
            v["metadata"]["common"]["startTime"],
            operation.start_time.to_rfc3339()
        );
        assert!(matches!(operation.status.clone(), OperationStatus::Failed));
        assert!(operation
            .error
            .as_deref()
            .unwrap_or_default()
            .contains("File not found"));
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
        assert_eq!(state.operations.read().await.len(), 1);
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
