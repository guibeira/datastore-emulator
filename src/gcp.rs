use crate::DatastoreEmulator;
use crate::database::DatastoreStorage;
use crate::google::datastore::v1::aggregation_query::aggregation::Operator as AggregationOperator;
use crate::google::datastore::v1::datastore_server::Datastore as DatastoreService;
use crate::google::datastore::v1::{
    AggregationResultBatch, AllocateIdsRequest, AllocateIdsResponse, BeginTransactionRequest,
    BeginTransactionResponse, CommitRequest, CommitResponse, ExecutionStats, ExplainMetrics,
    Filter, LookupRequest, LookupResponse, PingRequest, PingResponse, PlanSummary,
    PropertyReference, ReserveIdsRequest, ReserveIdsResponse, RollbackRequest, RollbackResponse,
    RunAggregationQueryRequest, RunAggregationQueryResponse, RunQueryRequest, RunQueryResponse,
};
use pbjson_types::value::Kind;
use pbjson_types::{Duration, Struct, Value as ValueProps};
use std::collections::HashMap;
use std::time::{Instant, SystemTime};
use tonic::{Request, Response, Status};
use tracing;

fn system_time_to_timestamp(time: SystemTime) -> pbjson_types::Timestamp {
    match time.duration_since(SystemTime::UNIX_EPOCH) {
        Ok(duration) => pbjson_types::Timestamp {
            seconds: duration.as_secs() as i64,
            nanos: duration.subsec_nanos() as i32,
        },
        Err(_) => pbjson_types::Timestamp::default(),
    }
}

fn to_prost_duration(std_duration: std::time::Duration) -> Duration {
    Duration {
        seconds: std_duration.as_secs() as i64,
        nanos: std_duration.subsec_nanos() as i32,
    }
}

#[tonic::async_trait]
impl DatastoreService for DatastoreEmulator {
    async fn ping(&self, request: Request<PingRequest>) -> Result<Response<PingResponse>, Status> {
        let req = request.into_inner();
        let timestamp = pbjson_types::Timestamp {
            seconds: 0,
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
        let resp = crate::core::lookup(&self.storage, request.into_inner()).await?;
        Ok(Response::new(resp))
    }

    async fn run_query(
        &self,
        request: Request<RunQueryRequest>,
    ) -> Result<Response<RunQueryResponse>, Status> {
        let resp = crate::core::run_query(&self.storage, request.into_inner()).await?;
        Ok(Response::new(resp))
    }

    async fn commit(
        &self,
        request: Request<CommitRequest>,
    ) -> Result<Response<CommitResponse>, Status> {
        let resp = crate::core::commit(&self.storage, request.into_inner()).await?;
        Ok(Response::new(resp))
    }

    async fn begin_transaction(
        &self,
        request: Request<BeginTransactionRequest>,
    ) -> Result<Response<BeginTransactionResponse>, Status> {
        let resp = crate::core::begin_transaction(&self.storage, request.into_inner()).await?;
        Ok(Response::new(resp))
    }

    async fn rollback(
        &self,
        request: Request<RollbackRequest>,
    ) -> Result<Response<RollbackResponse>, Status> {
        let resp = crate::core::rollback(&self.storage, request.into_inner()).await?;
        Ok(Response::new(resp))
    }

    async fn allocate_ids(
        &self,
        request: Request<AllocateIdsRequest>,
    ) -> Result<Response<AllocateIdsResponse>, Status> {
        let resp = crate::core::allocate_ids(&self.storage, request.into_inner()).await?;
        Ok(Response::new(resp))
    }

    async fn reserve_ids(
        &self,
        request: Request<ReserveIdsRequest>,
    ) -> Result<Response<ReserveIdsResponse>, Status> {
        let resp = crate::core::reserve_ids(&self.storage, request.into_inner()).await?;
        Ok(Response::new(resp))
    }

    async fn run_aggregation_query(
        &self,
        request: Request<RunAggregationQueryRequest>,
    ) -> Result<Response<RunAggregationQueryResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();
        let storage = self.storage.read().await;

        // Extract the aggregation query from the request
        let aggregation_query = match req.query_type {
            Some(
                crate::google::datastore::v1::run_aggregation_query_request::QueryType::AggregationQuery(
                    query,
                ),
            ) => query,
            _ => {
                return Err(Status::invalid_argument(
                    "Missing or invalid aggregation query",
                ));
            }
        };

        // Get the base query if it exists
        let base_query = match aggregation_query.clone().query_type {
            Some(crate::google::datastore::v1::aggregation_query::QueryType::NestedQuery(
                query,
            )) => Some(query),
            _ => {
                // todo: Handle other query types if needed
                None
            }
        };

        // Get current time for read_time
        let read_time = system_time_to_timestamp(SystemTime::now());

        // Process each aggregation
        let mut matching_entities = Vec::new();

        if let Some(query) = &base_query {
            // Get entities matching the kind filter
            let filters = query
                .filter
                .as_ref()
                .unwrap_or(&Filter { filter_type: None });

            // Iterate over all entities and filter by project_id, kind and other filters
            for (key_struct, entity_metadata) in storage.entities.iter() {
                // Filter by project_id first
                if key_struct.project_id != req.project_id {
                    continue;
                }

                // Check if the entity's kind matches any of the kinds in the query
                let entity_kind = key_struct.path_elements.last().map(|(k, _)| k.as_str());
                if entity_kind.is_none()
                    || !query
                        .kind
                        .iter()
                        .any(|k_filter| Some(k_filter.name.as_str()) == entity_kind)
                {
                    continue; // Skip if kind doesn't match
                }

                // Check if the entity is found (key should always be present in EntityWithMetadata)
                if entity_metadata.entity.key.is_none() {
                    // This case should ideally not happen if data is consistent
                    tracing::warn!("Entity found in storage without a key in its Entity struct.");
                    continue;
                }

                // Apply filters if present
                if let Some(filter_type) = &filters.filter_type {
                    if DatastoreStorage::apply_filter(entity_metadata, filter_type) {
                        matching_entities.push(entity_metadata);
                    }
                } else {
                    matching_entities.push(entity_metadata); // No filter, include if kind matches
                }
            }
        } else {
            // todo: what we should do if there is no base query?
        }

        let mut aggregate_properties = HashMap::new();

        for aggregation in &aggregation_query.aggregations {
            if let Some(aggregation_operator) = &aggregation.operator {
                // Check if the aggregation operator is supported
                match aggregation_operator {
                    AggregationOperator::Count(_count) => {
                        let count_value = matching_entities.len() as i64;
                        // Create the aggregation result
                        let result_value = crate::google::datastore::v1::Value {
                            exclude_from_indexes: false,
                            meaning: 0,
                            value_type: Some(
                                crate::google::datastore::v1::value::ValueType::IntegerValue(
                                    count_value,
                                ),
                            ),
                        };
                        aggregate_properties.insert(aggregation.alias.clone(), result_value);
                    }
                    AggregationOperator::Sum(sum) => {
                        // Implement SUM aggregation
                        let mut sum_value = 0.0;

                        // Calculate the sum for the specified property
                        for entity_metadata in &matching_entities {
                            if let Some(property) = entity_metadata.entity.properties.get(
                                &<std::option::Option<PropertyReference> as Clone>::clone(
                                    &sum.property,
                                )
                                .unwrap()
                                .name,
                            ) {
                                if let Some(
                                    crate::google::datastore::v1::value::ValueType::IntegerValue(
                                        value,
                                    ),
                                ) = &property.value_type
                                {
                                    sum_value += *value as f64;
                                } else if let Some(
                                    crate::google::datastore::v1::value::ValueType::DoubleValue(
                                        value,
                                    ),
                                ) = &property.value_type
                                {
                                    sum_value += *value;
                                }
                            }
                        }

                        // Create the aggregation result
                        let result_value = crate::google::datastore::v1::Value {
                            exclude_from_indexes: false,
                            meaning: 0,
                            value_type: Some(
                                crate::google::datastore::v1::value::ValueType::DoubleValue(
                                    sum_value,
                                ),
                            ),
                        };
                        aggregate_properties.insert(aggregation.alias.clone(), result_value);
                    }
                    AggregationOperator::Avg(avg) => {
                        // Implement AVERAGE aggregation
                        let mut sum_value = 0.0;
                        let mut count = 0;

                        // Calculate the sum and count for the average
                        for entity_metadata in &matching_entities {
                            if let Some(property) = entity_metadata.entity.properties.get(
                                &<std::option::Option<PropertyReference> as Clone>::clone(
                                    &avg.property,
                                )
                                .unwrap()
                                .name,
                            ) {
                                if let Some(
                                    crate::google::datastore::v1::value::ValueType::IntegerValue(
                                        value,
                                    ),
                                ) = &property.value_type
                                {
                                    sum_value += *value as f64;
                                    count += 1;
                                } else if let Some(
                                    crate::google::datastore::v1::value::ValueType::DoubleValue(
                                        value,
                                    ),
                                ) = &property.value_type
                                {
                                    sum_value += *value;
                                    count += 1;
                                }
                            }
                        }

                        // Calculate the average
                        let average_value = if count > 0 {
                            sum_value / count as f64
                        } else {
                            0.0
                        };

                        // Create the aggregation result
                        let result_value = crate::google::datastore::v1::Value {
                            exclude_from_indexes: false,
                            meaning: 0,
                            value_type: Some(
                                crate::google::datastore::v1::value::ValueType::DoubleValue(
                                    average_value,
                                ),
                            ),
                        };
                        aggregate_properties.insert(aggregation.alias.clone(), result_value);
                    }
                }
            }
        }
        // Create a single AggregationResult with all properties
        let final_aggregation_result = crate::google::datastore::v1::AggregationResult {
            aggregate_properties,
        };

        // Create the result batch
        let batch = AggregationResultBatch {
            aggregation_results: vec![final_aggregation_result],
            more_results: 3, // NO_MORE_RESULTS
            read_time: Some(read_time.clone()),
        };
        let total_results = batch.aggregation_results.len() as i64;
        // Create execution metrics
        let mut fields = HashMap::new();
        fields.insert(
            "query_type".to_string(),
            ValueProps {
                kind: Some(Kind::StringValue("aggregation".to_string())),
            },
        );

        let debug_stats = Struct {
            fields: fields.clone(),
        };

        let execution_duration = start.elapsed();
        let response = Response::new(RunAggregationQueryResponse {
            batch: Some(batch),
            query: Some(aggregation_query),
            transaction: Vec::new(),
            explain_metrics: Some(ExplainMetrics {
                plan_summary: Some(PlanSummary {
                    indexes_used: vec![],
                }),
                execution_stats: Some(ExecutionStats {
                    results_returned: total_results,
                    execution_duration: Some(to_prost_duration(execution_duration)),
                    read_operations: storage.entities.len() as i64,
                    debug_stats: Some(debug_stats),
                }),
            }),
        });
        Ok(response)
    }
}
