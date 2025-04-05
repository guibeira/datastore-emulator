use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;
use tokio::sync::mpsc;
use tonic::{transport::Server, Request, Response, Status};
use uuid::Uuid;

// Import the generated code
pub mod google {
    pub mod datastore {
        pub mod v1 {
            tonic::include_proto!("google.datastore.v1");
        }
    }
}

use google::datastore::v1::datastore_server::{Datastore as DatastoreService, DatastoreServer};
use google::datastore::v1::{
    BeginTransactionRequest, BeginTransactionResponse,
    CommitRequest, CommitResponse,
    LookupRequest, LookupResponse,
    PingRequest, PingResponse,
    RollbackRequest, RollbackResponse,
    RunQueryRequest, RunQueryResponse,
};

// The in-memory storage for our emulator
#[derive(Default, Debug)]
struct DatastoreStorage {
    entities: HashMap<String, google::datastore::v1::Entity>,
    transactions: HashMap<String, Vec<google::datastore::v1::Mutation>>,
}

#[derive(Debug)]
struct DatastoreEmulator {
    storage: Arc<Mutex<DatastoreStorage>>,
}

impl Default for DatastoreEmulator {
    fn default() -> Self {
        Self {
            storage: Arc::new(Mutex::new(DatastoreStorage::default())),
        }
    }
}

#[tonic::async_trait]
impl DatastoreService for DatastoreEmulator {
    async fn ping(
        &self,
        request: Request<PingRequest>,
    ) -> Result<Response<PingResponse>, Status> {
        let req = request.into_inner();
        let now = SystemTime::now();
        let timestamp = prost_types::Timestamp {
            seconds: now.duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as i64,
            nanos: 0,
        };
        
        let response = PingResponse {
            message: format!("Hello {}! Datastore emulator is running.", req.message),
            server_time: Some(timestamp),
        };
        
        Ok(Response::new(response))
    }

    async fn begin_transaction(
        &self,
        request: Request<BeginTransactionRequest>,
    ) -> Result<Response<BeginTransactionResponse>, Status> {
        let req = request.into_inner();
        println!("Beginning transaction for project: {}", req.project_id);
        
        // Generate a unique transaction ID
        let transaction_id = Uuid::new_v4().to_string();
        let transaction_bytes = transaction_id.clone().into_bytes();
        
        // Store the transaction
        {
            let mut storage = self.storage.lock().unwrap();
            storage.transactions.insert(transaction_id, Vec::new());
        }
        
        Ok(Response::new(BeginTransactionResponse {
            transaction: transaction_bytes,
        }))
    }

    async fn lookup(
        &self,
        request: Request<LookupRequest>,
    ) -> Result<Response<LookupResponse>, Status> {
        let req = request.into_inner();
        println!("Lookup request for project: {}", req.project_id);
        
        // This is just a placeholder implementation that returns an empty response
        // We'll implement the actual lookup logic later
        
        Ok(Response::new(LookupResponse {
            found: Vec::new(),
            missing: Vec::new(),
            deferred: Vec::new(),
            // transaction: Vec::new(),
            // read_time: None,
        }))
    }

    async fn run_query(
        &self,
        request: Request<RunQueryRequest>,
    ) -> Result<Response<RunQueryResponse>, Status> {
        let req = request.into_inner();
        println!("RunQuery request for project: {}", req.project_id);
        
        // Return an empty result batch for now
        let batch = google::datastore::v1::QueryResultBatch {
            entity_results: Vec::new(),
            more_results: 3, // NO_MORE_RESULTS
            end_cursor: Vec::new(),
            // skipped_results: 0,
            // skipped_cursor: Vec::new(),
            // entity_result_type: 0,
            // snapshot_version: 0,
            // read_time: None,
        };
        
        Ok(Response::new(RunQueryResponse {
            batch: Some(batch),
            // query: None,
            // transaction: Vec::new(),
            // explain_metrics: None,
        }))
    }

    async fn commit(
        &self,
        request: Request<CommitRequest>,
    ) -> Result<Response<CommitResponse>, Status> {
        let req = request.into_inner();
        println!("Commit request for project: {}", req.project_id);
        
        // For now, just acknowledge the mutations without actually processing them
        let mutation_results = req.mutations.iter().map(|_| {
            google::datastore::v1::MutationResult {
                key: None,
            }
        }).collect();
        
        Ok(Response::new(CommitResponse {
            mutation_results,
            index_updates: 0,
        }))
    }

    async fn rollback(
        &self,
        request: Request<RollbackRequest>,
    ) -> Result<Response<RollbackResponse>, Status> {
        let req = request.into_inner();
        println!("Rollback request for project: {}", req.project_id);
        
        // Convert transaction ID bytes back to string
        let transaction_id = String::from_utf8_lossy(&req.transaction);
        
        // Remove the transaction from storage
        {
            let mut storage = self.storage.lock().unwrap();
            storage.transactions.remove(&transaction_id.to_string());
        }
        
        Ok(Response::new(RollbackResponse {}))
    }

    // We'll implement these other methods later
    // async fn allocate_ids(
    //     &self,
    //     _request: Request<google::datastore::v1::AllocateIdsRequest>,
    // ) -> Result<Response<google::datastore::v1::AllocateIdsResponse>, Status> {
    //     Err(Status::unimplemented("Not yet implemented"))
    // }

    // async fn reserve_ids(
    //     &self,
    //     _request: Request<google::datastore::v1::ReserveIdsRequest>,
    // ) -> Result<Response<google::datastore::v1::ReserveIdsResponse>, Status> {
    //     Err(Status::unimplemented("Not yet implemented"))
    // }

    // async fn run_aggregation_query(
    //     &self,
    //     _request: Request<google::datastore::v1::RunAggregationQueryRequest>,
    // ) -> Result<Response<google::datastore::v1::RunAggregationQueryResponse>, Status> {
    //     Err(Status::unimplemented("Not yet implemented"))
    // }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let addr = SocketAddr::from(([127, 0, 0, 1], 8080));
    let emulator = DatastoreEmulator::default();
    
    println!("Datastore emulator listening on {}", addr);
    
    Server::builder()
        .add_service(DatastoreServer::new(emulator))
        .serve(addr)                           
        .await?;
    
    Ok(())
}
