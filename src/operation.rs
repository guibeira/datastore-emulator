use chrono::{DateTime, Utc};
use serde::Serialize;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Serialize, Clone)]
pub enum OperationStatus {
    Processing,
    Successful,
    Failed,
}

#[derive(Serialize, Clone)]
pub struct OperationState {
    pub status: OperationStatus,
    pub start_time: DateTime<Utc>,
    pub end_time: Option<DateTime<Utc>>,
    pub error: Option<String>,
}

pub type Operations = Arc<RwLock<HashMap<String, OperationState>>>;
