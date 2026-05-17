pub mod api;
pub mod core;
pub mod database;
pub mod gcp;
pub mod google {
    pub mod datastore {
        pub mod import_export {
            pub mod dsbackups {
                tonic::include_proto!("dsbackups");
            }
            pub mod datastore_v3 {
                tonic::include_proto!("appengine");
            }
        }
        pub mod v1 {
            tonic::include_proto!("google.datastore.v1");
            include!(concat!(env!("OUT_DIR"), "/google.datastore.v1.serde.rs"));
        }
    }
}
pub mod import;
pub mod leveldb;
pub mod operation;
pub mod rest;
pub mod rest_error;
pub mod state;

pub use crate::database::DatastoreStorage;
pub use crate::state::AppState;

use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug)]
pub struct DatastoreEmulator {
    pub storage: Arc<RwLock<DatastoreStorage>>,
}

impl DatastoreEmulator {
    pub fn new(store_on_disk: bool) -> Self {
        tracing::info!("Initializing Datastore Emulator...");
        let mut storage = DatastoreStorage::default();
        if store_on_disk {
            if let Err(e) = storage.load_from_disk("datastore.bin") {
                tracing::warn!("Could not load data from disk: {}", e);
            }
        } else {
            tracing::info!("Running in-memory only. No data will be read from or saved to disk.");
        }
        Self {
            storage: Arc::new(RwLock::new(storage)),
        }
    }
}
