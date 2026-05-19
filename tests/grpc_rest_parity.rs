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
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
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
    use datastore_emulator::database::{EntityWithMetadata, KeyId, KeyStruct};

    let mut storage = DatastoreStorage::default();
    let key_struct = KeyStruct {
        project_id: "p1".to_string(),
        namespace: String::new(),
        path_elements: vec![("Task".to_string(), KeyId::StringId("abc".to_string()))],
    };
    storage.entities.insert(
        key_struct,
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
    let bytes = hyper::body::to_bytes(resp.into_body()).await.unwrap();
    let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(v["found"][0]["version"], "7");
    assert_eq!(v["found"][0]["entity"]["key"]["path"][0]["kind"], "Task");
    assert_eq!(v["found"][0]["entity"]["key"]["path"][0]["name"], "abc");
}
