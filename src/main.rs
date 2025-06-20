use crate::api::create_router;
use crate::google::datastore::v1::datastore_server::DatastoreServer;
use database::DatastoreStorage;
use std::env;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tonic::transport::Server;

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
        Self {
            storage: Arc::new(Mutex::new(DatastoreStorage::default())),
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    env_logger::init();

    // --- gRPC Server Setup ---
    let args: Vec<String> = env::args().collect();
    let grpc_host = args.get(1).map_or("127.0.0.1", |s| s.as_str());
    let grpc_port_str = args.get(2).map_or("8042", |s| s.as_str());
    let grpc_port: u16 = grpc_port_str.parse().unwrap_or_else(|_| {
        eprintln!(
            "Invalid gRPC port number '{}', using default 8042.",
            grpc_port_str
        );
        8042
    });

    let grpc_addr: SocketAddr = format!("{}:{}", grpc_host, grpc_port)
        .parse()
        .unwrap_or_else(|e| {
            eprintln!(
                "Invalid gRPC address format '{}:{}', error: {}. Using default 127.0.0.1:8042.",
                grpc_host, grpc_port, e
            );
            SocketAddr::from(([127, 0, 0, 1], 8042))
        });

    let emulator = DatastoreEmulator::default();
    let storage_clone = emulator.storage.clone();

    let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter
        .set_serving::<DatastoreServer<DatastoreEmulator>>()
        .await;

    // --- HTTP Server Setup ---
    let http_addr: SocketAddr = "127.0.0.1:8043".parse()?;
    let http_router = create_router(storage_clone);
    let http_server = axum::Server::bind(&http_addr).serve(http_router.into_make_service());

    println!("Datastore emulator (gRPC) listening on {}", grpc_addr);
    println!("HTTP server listening on {}", http_addr);

    let datastore_service = DatastoreServer::new(emulator);
    let grpc_server = Server::builder()
        .add_service(datastore_service)
        .add_service(health_service)
        .serve(grpc_addr);

    // --- Run both servers concurrently ---
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
    )?;

    Ok(())
}
