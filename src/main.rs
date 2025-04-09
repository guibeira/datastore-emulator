use google::datastore::v1::aggregation_query::aggregation::Count;
use google::datastore::v1::aggregation_query::{Aggregation, QueryType};
use google::datastore::v1::key::path_element::IdType;
use google::datastore::v1::key::PathElement;
use prost_types::value::Kind;
use prost_types::{Duration, Struct, Value as ValueProps};
use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;
use tokio::sync::mpsc;
use tonic::{Request, Response, Status, transport::Server};
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
    AggregationQuery, AllocateIdsRequest, AllocateIdsResponse, BeginTransactionRequest, BeginTransactionResponse, CommitRequest, CommitResponse, Entity, EntityResult, ExecutionStats, ExplainMetrics, Key, LookupRequest, LookupResponse, PartitionId, PingRequest, PingResponse, PlanSummary, Query, RollbackRequest, RollbackResponse, RunAggregationQueryRequest, RunAggregationQueryResponse, RunQueryRequest, RunQueryResponse
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
    async fn ping(&self, request: Request<PingRequest>) -> Result<Response<PingResponse>, Status> {
        let req = request.into_inner();
        let now = SystemTime::now();
        let timestamp = prost_types::Timestamp {
            seconds: now
                .duration_since(SystemTime::UNIX_EPOCH)
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


    async fn lookup(
        &self,
        request: Request<LookupRequest>,
    ) -> Result<Response<LookupResponse>, Status> {
        let req = request.into_inner();

        // This is just a placeholder implementation that returns an empty response
        // We'll implement the actual lookup logic later

        let mut properties = HashMap::new();
        properties.insert(
            "name".to_string(),
            google::datastore::v1::Value {
                exclude_from_indexes: false,
                meaning: 0,
                value_type: Some(google::datastore::v1::value::ValueType::StringValue(
                    "example_name".to_string(),
                )),
            },
        );


        let partition_id = PartitionId {
            database_id: req.database_id.clone(),
            project_id: "something".to_string(),
            namespace_id: "".to_string(), 
        };


        let key = Key {
            partition_id: Some(partition_id),
            path: vec![
                    PathElement{kind: "Task".to_string(), id_type: Some(IdType::Id(12345))}
            ],
        };

        let results = vec![
            EntityResult {
                entity: Some(Entity {
                    key: Some(key.clone()),
                    properties: properties.clone(),
                }),
                create_time: Some(prost_types::Timestamp {
                    seconds: 10,
                    nanos: 0,
                }),
                update_time: Some(prost_types::Timestamp {
                    seconds: 10,
                    nanos: 0,
                }),
                cursor: vec![],
                version: 0,

            },
        ];
        Ok(Response::new(LookupResponse {
            found: results,
            missing: Vec::new(),
            deferred: vec![],
            transaction: Vec::new(), 
            read_time: Some(prost_types::Timestamp {
                seconds: 10,
                nanos: 0,
            }),
        }))
    }

    async fn run_query(
        &self,
        request: Request<RunQueryRequest>,
    ) -> Result<Response<RunQueryResponse>, Status> {
        let req = request.into_inner();

        // Return an empty result batch for now
        let mut properties = HashMap::new();
        properties.insert(
            "name".to_string(),
            google::datastore::v1::Value {
                exclude_from_indexes: false,
                meaning: 0,
                value_type: Some(google::datastore::v1::value::ValueType::StringValue(
                    "example_name".to_string(),
                )),
            },
        );
        let key = Key {
            partition_id: req.partition_id.clone(),
            path: vec![],
        };
        let results = vec![
            EntityResult {
                entity: Some(Entity {
                    key: Some(key.clone()),
                    properties: properties.clone(),
                }),
                create_time: Some(prost_types::Timestamp {
                    seconds: 10,
                    nanos: 0,
                }),
                update_time: Some(prost_types::Timestamp {
                    seconds: 10,
                    nanos: 0,
                }),
                cursor: vec![],
                version: 1,
            },
            EntityResult {
                entity: Some(Entity {
                    key: Some(key.clone()),
                    properties: properties.clone(),
                }),
                create_time: Some(prost_types::Timestamp {
                    seconds: 10,
                    nanos: 0,
                }),
                update_time: Some(prost_types::Timestamp {
                    seconds: 10,
                    nanos: 0,
                }),
                cursor: vec![],
                version: 2,
            },
            EntityResult {
                entity: Some(Entity {
                    key: Some(key.clone()),
                    properties: properties.clone(),
                }),
                create_time: Some(prost_types::Timestamp {
                    seconds: 10,
                    nanos: 0,
                }),
                update_time: Some(prost_types::Timestamp {
                    seconds: 10,
                    nanos: 0,
                }),
                cursor: vec![],
                version: 3,
            },
        ];
        let batch = google::datastore::v1::QueryResultBatch {
            entity_results: results,
            more_results: 3, // NO_MORE_RESULTS
            end_cursor: Vec::new(),
        };

        let mut fields = BTreeMap::new();
        fields.insert(
            "Some key".to_string(),
            ValueProps {
                kind: Some(Kind::StringValue("Some value".to_string())),
            },
        );
        let debug_stats = Struct { fields: fields.clone() };
        let query = Query {
            projection: vec![],
            kind: vec![],
            filter: None,
            order: vec![],
            distinct_on: vec![],
            start_cursor: Vec::new(),
            end_cursor: Vec::new(),
            offset: 1,
            limit: Some(2),
            find_nearest: None,
        };
        Ok(Response::new(RunQueryResponse {
            transaction: vec![],
            query: Some(query),
            batch: Some(batch),
            explain_metrics: Some(ExplainMetrics {
                plan_summary: Some(PlanSummary {
                    indexes_used: vec![
                        Struct {
                            fields: fields.clone(),
                        }
                    ]
                }),
                execution_stats: Some(ExecutionStats {
                    results_returned: 10,
                    execution_duration: Some(Duration {
                        seconds: 10,
                        nanos: 0,
                    }),
                    read_operations: 10,
                    debug_stats: Some(debug_stats),
                }),
            }),
        }))
    }

    async fn commit(
        &self,
        request: Request<CommitRequest>,
    ) -> Result<Response<CommitResponse>, Status> {
        let req = request.into_inner();
        dbg!(&req);
        // println!("---");
        //For now, just acknowledge the mutations without actually processing them
        let mutation_results = req
            .mutations
            .iter()
            .map(|_| google::datastore::v1::MutationResult { key: None })
            .collect();

        Ok(Response::new(CommitResponse {
            mutation_results,
            index_updates: 0,
        }))
    }

    async fn begin_transaction(
        &self,
        request: Request<BeginTransactionRequest>,
    ) -> Result<Response<BeginTransactionResponse>, Status> {
        let req = request.into_inner();
        dbg!(&req);

        // Generate a unique transaction ID
        let transaction_id = 1;
        let transaction_bytes = transaction_id.to_string().into_bytes();
        // wait 1 second
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        // Store the transaction
        // {
        //     let mut storage = self.storage.lock().unwrap();
        //     storage.transactions.insert(transaction_id, Vec::new());
        // }

        println!("End beggining transaction");
        Ok(Response::new(BeginTransactionResponse {
            transaction: transaction_bytes,
        }))
    }
    
    async fn rollback(
        &self,
        request: Request<RollbackRequest>,
    ) -> Result<Response<RollbackResponse>, Status> {
        let req = request.into_inner();
        dbg!(&req);
        let transaction_id = req.transaction.clone();
        // Convert transaction ID bytes to string
        let transaction_id = String::from_utf8_lossy(&transaction_id);
        println!("Transaction ID: {}", transaction_id);

        // Convert transaction ID bytes back to string
        let transaction_id = String::from_utf8_lossy(&req.transaction);

        // Remove the transaction from storage
        {
            let mut storage = self.storage.lock().unwrap();
            storage.transactions.remove(&transaction_id.to_string());
        }

        println!("End rollback transaction");
        Ok(Response::new(RollbackResponse {}))
    }

    async fn allocate_ids(
        &self,
        request: Request<AllocateIdsRequest>,
    ) -> Result<Response<AllocateIdsResponse>, Status> {
        dbg!(request);

        Ok(Response::new(AllocateIdsResponse {
            keys: vec![],
        }))
    }

    // async fn reserve_ids(
    //     &self,
    //     _request: Request<google::datastore::v1::ReserveIdsRequest>,
    // ) -> Result<Response<google::datastore::v1::ReserveIdsResponse>, Status> {
    //     Err(Status::unimplemented("Not yet implemented"))
    // }

    async fn run_aggregation_query(
        &self,
        request: Request<RunAggregationQueryRequest>,
    ) -> Result<Response<RunAggregationQueryResponse>, Status> {
        let req = request.into_inner();
        dbg!(&req);

        // Return an empty result batch for now
        let batch = google::datastore::v1::AggregationResultBatch {
            aggregation_results: Vec::new(),
            more_results: 0, // NO_MORE_RESULTS
            read_time: Some(prost_types::Timestamp {
                seconds: 10,
                nanos: 0,
            }),
        };

        let transaction_with_fake_data = vec![0; 0];
        let query_with_fake_data = AggregationQuery {
            aggregations: vec![Aggregation {
                alias: "fake_alias".to_string(),
                operator: Some(
                    google::datastore::v1::aggregation_query::aggregation::Operator::Count(Count {
                        up_to: Some(200_i64),
                    }),
                ),
            }],
            query_type: None,
        };

        let mut fields = BTreeMap::new();
        fields.insert(
            "Some key".to_string(),
            ValueProps {
                kind: Some(Kind::StringValue("Some value".to_string())),
            },
        );
        let debug_stats = Struct { fields: fields.clone() };
        Ok(Response::new(RunAggregationQueryResponse {
            batch: Some(batch),
            query: Some(query_with_fake_data),
            transaction: transaction_with_fake_data,
            explain_metrics: Some(ExplainMetrics {
                plan_summary: Some(PlanSummary {
                    indexes_used: vec![
                        Struct {
                            fields: fields.clone(),
                        }
                    ]
                }),
                execution_stats: Some(ExecutionStats {
                    results_returned: 10,
                    execution_duration: Some(Duration {
                        seconds: 10,
                        nanos: 0,
                    }),
                    read_operations: 10,
                    debug_stats: Some(debug_stats),
                }),
            }),
        }))
    }
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
