use crate::api::create_router;
use crate::google::datastore::v1::datastore_server::DatastoreServer;
use crate::state::AppState;
use axum::{
    body::{Body, boxed},
    http::Request,
};
use clap::Parser;
use database::DatastoreStorage;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::{signal, sync::RwLock}; // Import the tokio signal module
use tonic::server::NamedService;
use tracing_subscriber::EnvFilter;

pub mod api;
pub mod database;
pub mod gcp;
pub mod import;
pub mod leveldb;
pub mod operation;
pub mod state;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// The host:port for both Datastore gRPC and local HTTP endpoints.
    #[arg(long, default_value = "127.0.0.1:8042")]
    host_port: String,

    /// Do not store data on disk
    #[arg(long)]
    no_store_on_disk: bool,
}

// This is the single, canonical definition of the `google` module for the entire crate.
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

#[derive(Debug)]
pub struct DatastoreEmulator {
    pub storage: Arc<RwLock<DatastoreStorage>>,
}

impl DatastoreEmulator {
    fn new(store_on_disk: bool) -> Self {
        tracing::info!("Initializing Datastore Emulator...");
        let mut storage = DatastoreStorage::default();
        if store_on_disk {
            // Attempt to load data from disk on startup
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info,h2=off"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let cli = Cli::parse();
    let store_on_disk = !cli.no_store_on_disk;

    let listen_addr: SocketAddr = cli.host_port.parse().unwrap_or_else(|e| {
        tracing::error!(
            "Invalid --host-port '{}', error: {}. Using default 127.0.0.1:8042.",
            cli.host_port,
            e
        );
        SocketAddr::from(([127, 0, 0, 1], 8042))
    });

    let emulator = DatastoreEmulator::new(store_on_disk);
    let app_state = AppState {
        storage: emulator.storage.clone(),
        operations: Arc::new(RwLock::new(HashMap::new())),
    };
    let storage_for_shutdown = emulator.storage.clone(); // Clone for the shutdown handler

    let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter
        .set_serving::<DatastoreServer<DatastoreEmulator>>()
        .await;

    let datastore_service = DatastoreServer::new(emulator)
        .max_encoding_message_size(16 * 1024 * 1024) // 16 MB
        .max_decoding_message_size(16 * 1024 * 1024); // 16 MB
    let grpc_path = format!("/{}/*rest", DatastoreServer::<DatastoreEmulator>::NAME);
    let datastore_service =
        tower::ServiceExt::<Request<Body>>::map_response(datastore_service, |res| res.map(boxed));
    let health_service =
        tower::ServiceExt::<Request<Body>>::map_response(health_service, |res| res.map(boxed));

    let app = create_router(app_state.clone())
        .route_service(&grpc_path, datastore_service)
        .route_service("/grpc.health.v1.Health/*rest", health_service);

    tracing::info!(
        "Datastore emulator HTTP and gRPC listening on {}",
        listen_addr
    );

    let server = axum::Server::bind(&listen_addr).serve(app.into_make_service());

    // --- Run the server with graceful shutdown ---
    tokio::select! {
        res = async {
            server
                .await
                .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { e.into() })
        } => {
            if let Err(e) = res {
                tracing::error!("Server failed: {}", e);
            }
        },

        // Branch 2: Wait for the shutdown signal (Ctrl+C)
        _ = signal::ctrl_c() => {
            if store_on_disk {
                tracing::info!("
    Shutdown signal received. Saving data to disk...");
                let storage = storage_for_shutdown.read().await;
                if let Err(e) = storage.save_to_disk("datastore.bin") {
                    tracing::error!("Failed to save data to disk: {}", e);
                }
            } else {
                tracing::info!("
    Shutdown signal received. Not saving data to disk as --no-store-on-disk is enabled.");
            }
        },
    }

    tracing::info!("Shutdown complete.");
    Ok(())
}
