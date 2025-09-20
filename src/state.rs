use crate::database::DatastoreStorage;
use crate::operation::Operations;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct AppState {
    pub storage: Arc<RwLock<DatastoreStorage>>,
    pub operations: Operations,
}
