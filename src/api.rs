use crate::database::DatastoreStorage;
use crate::import::bg_import_data;
use axum::{
    Json, Router,
    extract::{Path, State},
    routing::post,
};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};
use uuid::Uuid;

#[derive(Deserialize)]
pub struct WelcomeRequest {
    pub name: String,
}

#[derive(Serialize)]
pub struct WelcomeResponse {
    pub msg: String,
}

pub async fn welcome_handler(Json(payload): Json<WelcomeRequest>) -> Json<WelcomeResponse> {
    let response = WelcomeResponse {
        msg: format!("welcome {}", payload.name),
    };
    Json(response)
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
    State(storage): State<Arc<Mutex<DatastoreStorage>>>,
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
    tokio::spawn(bg_import_data(storage.clone(), payload.input_url.clone())); // Start background import task
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

pub fn create_router(storage: Arc<Mutex<DatastoreStorage>>) -> Router {
    Router::new()
        .route("/", post(welcome_handler))
        .route("/v1/projects/:project_id", post(import_handler))
        .with_state(storage)
}
