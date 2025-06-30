use crate::api::create_router;
use crate::google::datastore::v1::datastore_server::DatastoreServer;
use database::DatastoreStorage;
use std::env;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tokio::signal; // Import the tokio signal module
use tonic::transport::Server;
use tracing;
use tracing_subscriber::EnvFilter;

pub mod api;
pub mod database;
pub mod gcp;
pub mod import;
pub mod leveldb;

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

impl Default for DatastoreEmulator {
    fn default() -> Self {
        tracing::info!("Initializing Datastore Emulator...");
        let mut storage = DatastoreStorage::default();
        // Attempt to load data from disk on startup
        if let Err(e) = storage.load_from_disk("datastore.bin") {
            tracing::warn!("Could not load data from disk: {}", e);
        }
        Self {
            storage: Arc::new(Mutex::new(storage)),
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info,h2=off"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    // --- gRPC Server Setup ---
    let args: Vec<String> = env::args().collect();
    let grpc_host = args.get(1).map_or("127.0.0.1", |s| s.as_str());
    let grpc_port_str = args.get(2).map_or("8042", |s| s.as_str());
    let grpc_port: u16 = grpc_port_str.parse().unwrap_or_else(|_| {
        tracing::error!(
            "Invalid gRPC port number '{}', using default 8042.",
            grpc_port_str
        );
        8042
    });

    let grpc_addr: SocketAddr = format!("{}:{}", grpc_host, grpc_port)
        .parse()
        .unwrap_or_else(|e| {
            tracing::error!(
                "Invalid gRPC address format '{}:{}', error: {}. Using default 127.0.0.1:8042.",
                grpc_host,
                grpc_port,
                e
            );
            SocketAddr::from(([127, 0, 0, 1], 8042))
        });

    let emulator = DatastoreEmulator::default();
    let storage_for_http = emulator.storage.clone();
    let storage_for_shutdown = emulator.storage.clone(); // Clone for the shutdown handler

    let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter
        .set_serving::<DatastoreServer<DatastoreEmulator>>()
        .await;

    // --- HTTP Server Setup ---
    let http_addr: SocketAddr = "127.0.0.1:8043".parse()?;
    let http_router = create_router(storage_for_http);
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
            tracing::info!("\nShutdown signal received. Saving data to disk...");
            let storage = storage_for_shutdown.lock().unwrap();
            if let Err(e) = storage.save_to_disk("datastore.bin") {
                tracing::error!("Failed to save data to disk: {}", e);
            }
        },
    }

    tracing::info!("Shutdown complete.");
    Ok(())
}
