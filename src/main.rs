use crate::api::create_router;
use crate::google::datastore::v1::datastore_server::DatastoreServer;
use crate::state::AppState;
use clap::Parser;
use database::DatastoreStorage;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tokio::signal; // Import the tokio signal module
use tonic::transport::Server;
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
    /// The host for the gRPC server to listen on
    #[arg(long, default_value = "127.0.0.1")]
    grpc_host: String,

    /// The port for the gRPC server to listen on
    #[arg(long, default_value_t = 8042)]
    grpc_port: u16,

    /// The host for the HTTP server to listen on
    #[arg(long, default_value = "127.0.0.1")]
    http_host: String,

    /// The port for the HTTP server to listen on
    #[arg(long, default_value_t = 8043)]
    http_port: u16,

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
        }
    }
}

#[derive(Debug)]
pub struct DatastoreEmulator {
    pub storage: Arc<Mutex<DatastoreStorage>>,
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
            storage: Arc::new(Mutex::new(storage)),
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("debug,h2=off"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let cli = Cli::parse();
    let store_on_disk = !cli.no_store_on_disk;

    // --- gRPC Server Setup ---
    let grpc_addr: SocketAddr = format!("{}:{}", cli.grpc_host, cli.grpc_port)
        .parse()
        .unwrap_or_else(|e| {
            tracing::error!(
                "Invalid gRPC address format '{}:{}', error: {}. Using default 127.0.0.1:8042.",
                cli.grpc_host,
                cli.grpc_port,
                e
            );
            SocketAddr::from(([127, 0, 0, 1], 8042))
        });

    let emulator = DatastoreEmulator::new(store_on_disk);
    let app_state = AppState {
        storage: emulator.storage.clone(),
        operations: Arc::new(Mutex::new(HashMap::new())),
    };
    let storage_for_shutdown = emulator.storage.clone(); // Clone for the shutdown handler

    let (health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter
        .set_serving::<DatastoreServer<DatastoreEmulator>>()
        .await;

    // --- HTTP Server Setup ---
    let http_addr: SocketAddr = format!("{}:{}", cli.http_host, cli.http_port).parse()?;
    let http_router = create_router(app_state.clone());
    let http_server = axum::Server::bind(&http_addr).serve(http_router.into_make_service());

    tracing::info!("Datastore emulator (gRPC) listening on {}", grpc_addr);
    tracing::info!("HTTP server listening on {}", http_addr);

    let datastore_service = DatastoreServer::new(emulator)
        .max_encoding_message_size(16 * 1024 * 1024) // 16 MB
        .max_decoding_message_size(16 * 1024 * 1024); // 16 MB
    let grpc_server = Server::builder()
        .add_service(datastore_service)
        .add_service(health_service)
        .serve(grpc_addr);

    // --- Run both servers concurrently with graceful shutdown ---
    tokio::select! {
        // Branch 1: Run the servers. This is now a single future.
        res = async {
            tokio::try_join!(
                async {
                    grpc_server
                        .await
                        .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { e.into() })
                },
                async {
                    http_server
                        .await
                        .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { e.into() })
                }
            )
        } => {
            if let Err(e) = res {
                tracing::error!("One of the servers failed: {}", e);
            }
        },

        // Branch 2: Wait for the shutdown signal (Ctrl+C)
        _ = signal::ctrl_c() => {
            if store_on_disk {
                tracing::info!("
    Shutdown signal received. Saving data to disk...");
                let storage = storage_for_shutdown.lock().unwrap();
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
