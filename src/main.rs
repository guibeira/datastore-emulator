use google::datastore::v1::aggregation_query::Aggregation;
use google::datastore::v1::aggregation_query::aggregation::Count;
use google::datastore::v1::key::PathElement;
use google::datastore::v1::key::path_element::IdType;
use google::datastore::v1::mutation::Operation;
use prost_types::value::Kind;
use prost_types::{Duration, Struct, Value as ValueProps};
use std::collections::{BTreeMap, BTreeSet, HashMap};

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;
use tonic::{Request, Response, Status, transport::Server};

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
    AggregationQuery, AggregationResultBatch, AllocateIdsRequest, AllocateIdsResponse,
    BeginTransactionRequest, BeginTransactionResponse, CommitRequest, CommitResponse, Entity,
    EntityResult, ExecutionStats, ExplainMetrics, Key, LookupRequest, LookupResponse, Mutation,
    PartitionId, PingRequest, PingResponse, PlanSummary, Query, ReserveIdsRequest,
    ReserveIdsResponse, RollbackRequest, RollbackResponse, RunAggregationQueryRequest,
    RunAggregationQueryResponse, RunQueryRequest, RunQueryResponse,
};

#[derive(Default, Debug)]
struct DatastoreStorage {
    // Armazenamento principal usando BTreeMap para entidades
    entities: BTreeMap<KeyStruct, EntityWithMetadata>,
    // Índices para consultas eficientes
    indexes: HashMap<(String, String, String), BTreeSet<KeyStruct>>,
    // Transações ativas
    transactions: HashMap<String, TransactionState>,
    // Contador para geração de IDs automáticos
    id_counter: Arc<Mutex<i64>>,
}

impl DatastoreStorage {
    fn update_indexes(&mut self, key_struct: &KeyStruct, entity: &Entity) {
        // Para cada propriedade da entidade, criar índices
        for (prop_name, prop_value) in &entity.properties {
            if let Some(value_type) = &prop_value.value_type {
                // Extrair valor como string para indexação
                let value_str = match value_type {
                    google::datastore::v1::value::ValueType::StringValue(s) => s.clone(),
                    google::datastore::v1::value::ValueType::IntegerValue(i) => i.to_string(),
                    // Implementar outros tipos conforme necessário
                    _ => continue,
                };

                // Obter o kind da entidade
                if let Some((kind, _)) = key_struct.path_elements.last() {
                    let index_key = (kind.clone(), prop_name.clone(), value_str);
                    self.indexes
                        .entry(index_key)
                        .or_insert_with(BTreeSet::new)
                        .insert(key_struct.clone());
                }
            }
        }
    }
}

// Entidade com metadados
#[derive(Default, Debug)]
struct EntityWithMetadata {
    entity: Entity,
    version: u64,
    create_time: prost_types::Timestamp,
    update_time: prost_types::Timestamp,
}

// Estrutura chave personalizada para indexação eficiente
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
struct KeyStruct {
    namespace: String,
    path_elements: Vec<(String, KeyId)>, // (kind, id/name)
}

impl KeyStruct {
    fn from_datastore_key(key: &Key) -> Self {
        let mut path_elements = Vec::new();
        for path_element in &key.path {
            let kind = path_element.kind.clone();
            let id_type = match &path_element.id_type {
                Some(IdType::Id(id)) => KeyId::IntId(*id),
                Some(IdType::Name(name)) => KeyId::StringId(name.clone()),
                None => continue,
            };
            path_elements.push((kind, id_type));
        }
        Self {
            namespace: key
                .partition_id
                .as_ref()
                .map_or_else(|| "".to_string(), |p| p.namespace_id.clone()),
            path_elements,
        }
    }
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
enum KeyId {
    IntId(i64),
    StringId(String),
}

// Estado de uma transação
#[derive(Default, Debug)]
struct TransactionState {
    mutations: Vec<Mutation>,
    snapshot: HashMap<KeyStruct, EntityWithMetadata>,
    timestamp: prost_types::Timestamp,
    read_only: bool,
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
        let storage = self.storage.lock().unwrap();

        let mut found = Vec::new();
        let mut missing = Vec::new();

        // Process each key in the request
        for key in &req.keys {
            let key_struct = KeyStruct::from_datastore_key(key);

            // Look up the key in storage
            if let Some(entity_metadata) = storage.entities.get(&key_struct) {
                // Entity found, add to results
                found.push(EntityResult {
                    entity: Some(entity_metadata.entity.clone()),
                    create_time: Some(entity_metadata.create_time.clone()),
                    update_time: Some(entity_metadata.update_time.clone()),
                    cursor: vec![],
                    version: entity_metadata.version as i64,
                });
            } else {
                // Entity not found, add to missing
                missing.push(EntityResult {
                    entity: Some(Entity {
                        key: Some(key.clone()),
                        properties: HashMap::new(),
                    }),
                    create_time: None,
                    update_time: None,
                    cursor: vec![],
                    version: 0,
                });
            }
        }

        // Get current time for read_time
        let now = SystemTime::now();
        let read_time = prost_types::Timestamp {
            seconds: now
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as i64,
            nanos: 0,
        };

        Ok(Response::new(LookupResponse {
            found,
            missing,
            deferred: vec![],
            transaction: Vec::new(),
            read_time: Some(read_time),
        }))
    }

    async fn run_query(
        &self,
        request: Request<RunQueryRequest>,
    ) -> Result<Response<RunQueryResponse>, Status> {
        let req = request.into_inner();

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
        let debug_stats = Struct {
            fields: fields.clone(),
        };
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
                    indexes_used: vec![Struct {
                        fields: fields.clone(),
                    }],
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
        let mut storage = self.storage.lock().unwrap();
        let mut mutation_results = Vec::new();

        for mutation in req.mutations {
            if let Some(Operation::Insert(entity) | Operation::Upsert(entity)) = mutation.operation
            {
                let key = match entity.key {
                    Some(ref key) => key.clone(),
                    None => return Err(Status::invalid_argument("Entity missing key")),
                };

                let key_struct = KeyStruct::from_datastore_key(&key);

                let now = SystemTime::now();
                let timestamp = prost_types::Timestamp {
                    seconds: now
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs() as i64,
                    nanos: 0,
                };

                let entity_metadata = EntityWithMetadata {
                    entity: entity.clone(),
                    version: 1, // Primeira versão
                    create_time: timestamp.clone(),
                    update_time: timestamp,
                };

                storage.entities.insert(key_struct.clone(), entity_metadata);

                storage.update_indexes(&key_struct, &entity);

                mutation_results.push(google::datastore::v1::MutationResult {
                    key: Some(key),
                    ..Default::default()
                });
            }
            // another types of mutation
        }
        dbg!(&storage);

        let index_updates = mutation_results.len() as i32;
        Ok(Response::new(CommitResponse {
            mutation_results,
            index_updates,
        }))
    }

    async fn begin_transaction(
        &self,
        request: Request<BeginTransactionRequest>,
    ) -> Result<Response<BeginTransactionResponse>, Status> {
        let req = request.into_inner();
        //dbg!(&req);

        let transaction_id = 1;
        let transaction_bytes = transaction_id.to_string().into_bytes();

        Ok(Response::new(BeginTransactionResponse {
            transaction: transaction_bytes,
        }))
    }

    async fn rollback(
        &self,
        request: Request<RollbackRequest>,
    ) -> Result<Response<RollbackResponse>, Status> {
        let req = request.into_inner();
        // dbg!(&req);
        let transaction_id = req.transaction.clone();
        // Convert transaction ID bytes to string
        let transaction_id = String::from_utf8_lossy(&transaction_id);
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
        //dbg!(request);

        Ok(Response::new(AllocateIdsResponse {
            keys: vec![Key {
                partition_id: Some(PartitionId {
                    database_id: "database_id".to_string(),
                    project_id: "something".to_string(),
                    namespace_id: "".to_string(),
                }),
                path: vec![PathElement {
                    kind: "Task".to_string(),
                    id_type: Some(IdType::Id(12345)),
                }],
            }],
        }))
    }

    async fn reserve_ids(
        &self,
        request: Request<ReserveIdsRequest>,
    ) -> Result<Response<ReserveIdsResponse>, Status> {
        //dbg!(request);
        Ok(Response::new(ReserveIdsResponse {}))
    }

    async fn run_aggregation_query(
        &self,
        request: Request<RunAggregationQueryRequest>,
    ) -> Result<Response<RunAggregationQueryResponse>, Status> {
        let req = request.into_inner();
        //dbg!(&req);

        // Return an empty result batch for now
        let batch = AggregationResultBatch {
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
        let debug_stats = Struct {
            fields: fields.clone(),
        };
        Ok(Response::new(RunAggregationQueryResponse {
            batch: Some(batch),
            query: Some(query_with_fake_data),
            transaction: transaction_with_fake_data,
            explain_metrics: Some(ExplainMetrics {
                plan_summary: Some(PlanSummary {
                    indexes_used: vec![Struct {
                        fields: fields.clone(),
                    }],
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
