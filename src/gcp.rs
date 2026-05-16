use crate::DatastoreEmulator;
use crate::google::datastore::v1::datastore_server::Datastore as DatastoreService;
use crate::google::datastore::v1::{
    AllocateIdsRequest, AllocateIdsResponse, BeginTransactionRequest, BeginTransactionResponse,
    CommitRequest, CommitResponse, LookupRequest, LookupResponse, PingRequest, PingResponse,
    ReserveIdsRequest, ReserveIdsResponse, RollbackRequest, RollbackResponse,
    RunAggregationQueryRequest, RunAggregationQueryResponse, RunQueryRequest, RunQueryResponse,
};
use tonic::{Request, Response, Status};

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
        let resp = crate::core::run_aggregation_query(&self.storage, request.into_inner()).await?;
        Ok(Response::new(resp))
    }
}
