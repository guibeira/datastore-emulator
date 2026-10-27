use axum::body::Body;
use axum::http::Request;
use datastore_emulator::{
    AppState, DatastoreStorage,
    api::create_router,
    core,
    google::datastore::v1::{
        Entity, Key, LookupRequest, PartitionId,
        key::{PathElement, path_element::IdType},
    },
};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use tower::ServiceExt;

fn key(project: &str, kind: &str, name: &str) -> Key {
    Key {
        partition_id: Some(PartitionId {
            project_id: project.to_string(),
            database_id: String::new(),
            namespace_id: String::new(),
        }),
        path: vec![PathElement {
            kind: kind.to_string(),
            id_type: Some(IdType::Name(name.to_string())),
        }],
    }
}

#[tokio::test]
async fn grpc_and_rest_lookup_return_same_data() {
    use datastore_emulator::database::EntityWithMetadata;

    let mut storage = DatastoreStorage::default();
    storage.put_entity(
        EntityWithMetadata {
            entity: Entity {
                key: Some(key("p1", "Task", "abc")),
                properties: HashMap::new(),
            },
            version: 7,
            create_time: pbjson_types::Timestamp::default(),
            update_time: pbjson_types::Timestamp::default(),
        }
        .into(),
    );
    let storage = Arc::new(RwLock::new(storage));

    let core_resp = core::lookup(
        &storage,
        LookupRequest {
            project_id: "p1".to_string(),
            keys: vec![key("p1", "Task", "abc")],
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(core_resp.found.len(), 1);
    assert_eq!(core_resp.found[0].version, 7);

    let state = AppState {
        storage: storage.clone(),
        operations: Arc::new(RwLock::new(HashMap::new())),
    };
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
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(v["found"][0]["version"], "7");
    assert_eq!(v["found"][0]["entity"]["key"]["path"][0]["kind"], "Task");
    assert_eq!(v["found"][0]["entity"]["key"]["path"][0]["name"], "abc");
}

#[tokio::test]
async fn rest_commit_and_lookup_normalize_missing_key_projects_from_the_url() {
    let state = AppState {
        storage: Arc::new(RwLock::new(DatastoreStorage::default())),
        operations: Default::default(),
    };
    let app = create_router(state);
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/projects/p1:commit")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "mode": "NON_TRANSACTIONAL",
                        "mutations": [{"upsert": {
                            "key": {"path": [{"kind": "Task", "name": "bare"}]}
                        }}]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(response.status().is_success());

    for partition in [
        serde_json::Value::Null,
        serde_json::json!({"projectId": "p1"}),
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/projects/p1:lookup")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "keys": [{
                                "partitionId": partition,
                                "path": [{"kind": "Task", "name": "bare"}]
                            }]
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(response.status().is_success());
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let data: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            data["found"][0]["entity"]["key"]["partitionId"]["projectId"],
            "p1"
        );
    }
}
