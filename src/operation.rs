use chrono::{DateTime, Utc};
use serde::Serialize;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

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

pub type Operations = Arc<Mutex<HashMap<String, OperationState>>>;
