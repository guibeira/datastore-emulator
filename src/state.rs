use crate::database::DatastoreStorage;
use crate::operation::Operations;
use parking_lot::RwLock;
use std::sync::Arc;

#[derive(Clone)]
pub struct AppState {
    pub storage: Arc<RwLock<DatastoreStorage>>,
    pub operations: Operations,
}
