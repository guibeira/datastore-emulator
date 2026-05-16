use crate::import::bg_import_data;
use crate::operation::{OperationState, OperationStatus};
use crate::state::AppState;
use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::get,
    routing::post,
};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Deserialize)]
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
    Path(project_id): Path<String>,
    Json(payload): Json<ImportRequest>,
) -> Json<ImportResponse> {
    let mut path_parameters = project_id.split(":");
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
        .write()
        .await
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
        .route("/v1/projects/:project_id", post(import_handler))
        .route(
            "/v1/projects/:project_id/operations/:operation_id",
            get(get_operation_status),
        )
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
                },
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
