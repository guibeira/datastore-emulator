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

pub async fn datastore_method_handler(
    State(state): State<AppState>,
    Path(project_method): Path<String>,
    body: Bytes,
) -> Response {
    let (_project_id, method) = match project_method.split_once(':') {
        Some(p) => p,
        None => return bad_request("missing :method suffix on /v1/projects/{project}:{method}"),
    };

    match method {
        "lookup" => json_call(body, |r| core::lookup(&state.storage, r)).await,
        other => not_found(&format!("unknown Datastore method: {other}")),
    }
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
            },
        );
        AppState {
            storage: Arc::new(RwLock::new(storage)),
            operations: Arc::new(RwLock::new(HashMap::new())),
        }
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
}
