use crate::state::AppState;
use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::get,
    routing::post,
};

pub async fn get_operation_status(
    State(state): State<AppState>,
    Path((_project_id, operation_id)): Path<(String, String)>,
) -> impl IntoResponse {
    let operations = state.operations.read().await;
    if let Some(state) = operations.get(&operation_id) {
        (StatusCode::OK, Json(state.clone())).into_response()
    } else {
        (StatusCode::NOT_FOUND, "Operation not found").into_response()
    }
}

pub async fn healthcheck_handler() -> impl IntoResponse {
    StatusCode::OK
}

pub async fn reset_handler(State(state): State<AppState>) -> impl IntoResponse {
    {
        let mut storage = state.storage.write().await;
        *storage = crate::database::DatastoreStorage::default();
    }
    state.operations.write().await.clear();
    StatusCode::OK
}

pub fn create_router(state: AppState) -> Router {
    Router::new()
        .route("/", get(healthcheck_handler))
        .route("/reset", post(reset_handler))
        .route(
            "/v1/projects/:project_method",
            post(crate::rest::datastore_method_handler),
        )
        .route(
            "/v1/projects/:project_id/operations/:operation_id",
            get(get_operation_status),
        )
        .layer(tower_http::cors::CorsLayer::permissive())
        .with_state(state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::{DatastoreStorage, EntityWithMetadata, KeyId, KeyStruct};
    use crate::google::datastore::v1::Entity;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::RwLock;
    use tower::ServiceExt;

    fn test_state() -> AppState {
        AppState {
            storage: Arc::new(RwLock::new(DatastoreStorage::default())),
            operations: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    #[tokio::test]
    async fn root_healthcheck_returns_ok() {
        let app = create_router(test_state());

        let response = app
            .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn reset_clears_datastore_storage() {
        let state = test_state();
        {
            let mut storage = state.storage.write().await;
            storage.entities.insert(
                KeyStruct {
                    project_id: "test-project".to_string(),
                    namespace: String::new(),
                    path_elements: vec![("Task".to_string(), KeyId::StringId("one".to_string()))],
                },
                EntityWithMetadata {
                    entity: Entity::default(),
                    version: 1,
                    create_time: pbjson_types::Timestamp::default(),
                    update_time: pbjson_types::Timestamp::default(),
                }
                .into(),
            );
        }

        let app = create_router(state.clone());
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/reset")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert!(state.storage.read().await.entities.is_empty());
    }
}
