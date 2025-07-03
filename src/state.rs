use crate::database::DatastoreStorage;
use crate::operation::Operations;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct AppState {
    pub storage: Arc<Mutex<DatastoreStorage>>,
    pub operations: Operations,
}
