//! Load generator for the benchmark.
//!
//! A gRPC client on the emulator's generated `DatastoreClient`: a call costs
//! microseconds on the client side, so the emulators, not the client, are
//! what gets measured. Same scenarios, settings, summary and JSON as
//! benchmark/test_benchmark.py; merge_results.py reads the output unchanged.
//!
//!     cargo run --release --features loadgen --bin loadgen -- --target rust --json-out out/rust.json

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::{Parser, ValueEnum};
use datastore_emulator::google::datastore::v1::{
    BeginTransactionRequest, CommitRequest, Entity, Filter, Key, KindExpression, LookupRequest,
    Mutation, PartitionId, Projection, PropertyFilter, PropertyOrder, PropertyReference, Query,
    QueryResultBatch, ReadOptions, RollbackRequest, RunQueryRequest, Value, commit_request,
    datastore_client::DatastoreClient, filter, key, mutation, property_filter, property_order,
    query_result_batch, read_options, run_query_request, value,
};
use serde::Serialize;
use tokio::sync::{Barrier, watch};
use tonic::Status;
use tonic::transport::{Channel, Endpoint};
use uuid::Uuid;

const KIND: &str = "PerfTest";
const GET_MULTI_KEYS: usize = 100;
const QUERY_LIMIT: i32 = 20;
// Shared dataset: SEED_GROUPS groups of SEED_TOTAL / SEED_GROUPS entities; the
// cursor walk pages through one group PAGE_SIZE at a time.
const SEED_TOTAL: usize = 5000;
const SEED_GROUPS: usize = 10;
const PAGE_SIZE: usize = 100;
// Per-request deadline, so a hung emulator fails the run instead of stalling it.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);

type Client = DatastoreClient<Channel>;

// --- proto helpers -------------------------------------------------------

fn partition(project: &str) -> PartitionId {
    PartitionId {
        project_id: project.to_string(),
        database_id: String::new(),
        namespace_id: String::new(),
    }
}

fn name_key(project: &str, kind: &str, name: &str) -> Key {
    Key {
        partition_id: Some(partition(project)),
        path: vec![key::PathElement {
            kind: kind.to_string(),
            id_type: Some(key::path_element::IdType::Name(name.to_string())),
        }],
    }
}

fn str_value(text: &str) -> Value {
    Value {
        meaning: 0,
        exclude_from_indexes: false,
        value_type: Some(value::ValueType::StringValue(text.to_string())),
    }
}

fn int_value(number: i64) -> Value {
    Value {
        meaning: 0,
        exclude_from_indexes: false,
        value_type: Some(value::ValueType::IntegerValue(number)),
    }
}

fn int_of(entity: &Entity, name: &str) -> i64 {
    match entity
        .properties
        .get(name)
        .and_then(|v| v.value_type.as_ref())
    {
        Some(value::ValueType::IntegerValue(number)) => *number,
        _ => 0,
    }
}

fn entity(key: Key, properties: Vec<(&str, Value)>) -> Entity {
    Entity {
        key: Some(key),
        properties: properties
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect(),
    }
}

fn upsert(entity: Entity) -> Mutation {
    Mutation {
        operation: Some(mutation::Operation::Upsert(entity)),
        ..Default::default()
    }
}

fn delete(key: Key) -> Mutation {
    Mutation {
        operation: Some(mutation::Operation::Delete(key)),
        ..Default::default()
    }
}

fn equals(property: &str, value: Value) -> Filter {
    Filter {
        filter_type: Some(filter::FilterType::PropertyFilter(PropertyFilter {
            property: Some(PropertyReference {
                name: property.to_string(),
            }),
            op: property_filter::Operator::Equal as i32,
            value: Some(value),
        })),
    }
}

fn query(
    kind: &str,
    filter: Option<Filter>,
    order_desc: Option<&str>,
    limit: Option<i32>,
) -> Query {
    Query {
        kind: vec![KindExpression {
            name: kind.to_string(),
        }],
        filter,
        order: order_desc
            .map(|name| {
                vec![PropertyOrder {
                    property: Some(PropertyReference {
                        name: name.to_string(),
                    }),
                    direction: property_order::Direction::Descending as i32,
                }]
            })
            .unwrap_or_default(),
        limit: limit.map(|value| pbjson_types::Int32Value { value }),
        ..Default::default()
    }
}

/// Bounds a call on the client side. tonic's `Endpoint::timeout` only covers
/// the request once the connection is ready; a server that accepts TCP but
/// never finishes the HTTP/2 handshake would hang the first call forever.
async fn timed<T>(
    deadline: Duration,
    call: impl Future<Output = Result<T, Status>>,
) -> Result<T, Status> {
    match tokio::time::timeout(deadline, call).await {
        Ok(outcome) => outcome,
        Err(_) => Err(Status::deadline_exceeded(format!(
            "no answer within {}s",
            deadline.as_secs_f64()
        ))),
    }
}

async fn connect_with_timeout(endpoint: &str, timeout: Duration) -> Result<Client, Status> {
    let channel = Endpoint::from_shared(endpoint.to_string())
        .map_err(|e| Status::invalid_argument(e.to_string()))?
        .connect_timeout(timeout)
        .timeout(timeout)
        .connect()
        .await
        .map_err(|e| Status::unavailable(e.to_string()))?;
    Ok(DatastoreClient::new(channel))
}

async fn connect(endpoint: &str) -> Result<Client, Status> {
    connect_with_timeout(endpoint, REQUEST_TIMEOUT).await
}

async fn commit(
    client: &mut Client,
    project: &str,
    mutations: Vec<Mutation>,
    transaction: Option<Vec<u8>>,
) -> Result<(), Status> {
    let (mode, selector) = match transaction {
        Some(id) => (
            commit_request::Mode::Transactional,
            Some(commit_request::TransactionSelector::Transaction(id)),
        ),
        None => (commit_request::Mode::NonTransactional, None),
    };
    timed(
        REQUEST_TIMEOUT,
        client.commit(CommitRequest {
            project_id: project.to_string(),
            database_id: String::new(),
            mode: mode as i32,
            mutations,
            transaction_selector: selector,
        }),
    )
    .await?;
    Ok(())
}

async fn lookup(
    client: &mut Client,
    project: &str,
    keys: Vec<Key>,
    transaction: Option<Vec<u8>>,
) -> Result<Vec<Entity>, Status> {
    let read_options = transaction.map(|id| ReadOptions {
        consistency_type: Some(read_options::ConsistencyType::Transaction(id)),
    });
    let response = timed(
        REQUEST_TIMEOUT,
        client.lookup(LookupRequest {
            project_id: project.to_string(),
            database_id: String::new(),
            read_options,
            keys,
            property_mask: None,
        }),
    )
    .await?
    .into_inner();
    Ok(response
        .found
        .into_iter()
        .filter_map(|result| result.entity)
        .collect())
}

async fn run_query(
    client: &mut Client,
    project: &str,
    query: Query,
) -> Result<QueryResultBatch, Status> {
    let response = timed(
        REQUEST_TIMEOUT,
        client.run_query(RunQueryRequest {
            project_id: project.to_string(),
            database_id: String::new(),
            partition_id: Some(partition(project)),
            read_options: None,
            property_mask: None,
            explain_options: None,
            query_type: Some(run_query_request::QueryType::Query(query)),
        }),
    )
    .await?
    .into_inner();
    Ok(response.batch.unwrap_or_default())
}

fn batch_keys(batch: QueryResultBatch) -> Vec<Key> {
    batch
        .entity_results
        .into_iter()
        .filter_map(|result| result.entity.and_then(|entity| entity.key))
        .collect()
}

async fn put_in_chunks(
    client: &mut Client,
    project: &str,
    entities: Vec<Entity>,
) -> Result<(), Status> {
    for chunk in entities.chunks(500) {
        commit(
            client,
            project,
            chunk.iter().cloned().map(upsert).collect(),
            None,
        )
        .await?;
    }
    Ok(())
}

/// Deletes every entity of `kind` matching `filter`, page by page.
async fn delete_kind(
    client: &mut Client,
    project: &str,
    kind: &str,
    filter: Option<Filter>,
) -> Result<usize, Status> {
    let mut keys_only = query(kind, filter, None, None);
    keys_only.projection = vec![Projection {
        property: Some(PropertyReference {
            name: "__key__".to_string(),
        }),
    }];
    let mut keys = Vec::new();
    loop {
        let batch = run_query(client, project, keys_only.clone()).await?;
        let more = batch.more_results == query_result_batch::MoreResultsType::NotFinished as i32
            || batch.more_results
                == query_result_batch::MoreResultsType::MoreResultsAfterCursor as i32;
        let end_cursor = batch.end_cursor.clone();
        let page = batch_keys(batch);
        let exhausted = page.is_empty() || !more || end_cursor.is_empty();
        keys.extend(page);
        if exhausted {
            break;
        }
        keys_only.start_cursor = end_cursor;
    }
    for chunk in keys.chunks(500) {
        commit(
            client,
            project,
            chunk.iter().cloned().map(delete).collect(),
            None,
        )
        .await?;
    }
    Ok(keys.len())
}

// --- data ---------------------------------------------------------------

/// The shared dataset one target's clients query: `kind` holds `groups`
/// groups of `per_group` entities with a random `score`.
#[derive(Clone, Debug)]
struct Seed {
    kind: String,
    groups: usize,
    per_group: usize,
    page_size: usize,
}

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
}

/// (name, group, score) rows, the same for every run.
fn seed_rows(total: usize, groups: usize) -> Vec<(String, i64, i64)> {
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    (0..total)
        .map(|i| {
            (
                format!("item-{i}"),
                (i % groups) as i64,
                (rng.next() % 1_000_000) as i64,
            )
        })
        .collect()
}

async fn seed_dataset(
    client: &mut Client,
    project: &str,
    total: usize,
    groups: usize,
    page_size: usize,
) -> Result<Seed, Status> {
    let seed = Seed {
        kind: format!("PerfSeed_{}", Uuid::new_v4().simple()),
        groups,
        per_group: total / groups,
        page_size,
    };
    let entities = seed_rows(total, groups)
        .into_iter()
        .map(|(name, group, score)| {
            entity(
                name_key(project, &seed.kind, &name),
                vec![
                    ("name", str_value(&name)),
                    ("group", int_value(group)),
                    ("score", int_value(score)),
                ],
            )
        })
        .collect();
    if let Err(error) = put_in_chunks(client, project, entities).await {
        // Commits are per chunk: drop what the earlier chunks left behind.
        let _ = cleanup_seed(client, project, &seed).await;
        return Err(error);
    }
    Ok(seed)
}

async fn cleanup_seed(client: &mut Client, project: &str, seed: &Seed) -> Result<usize, Status> {
    delete_kind(client, project, &seed.kind, None).await
}

/// Deletes what setup_client_run and the scenarios wrote for `run_id`.
async fn cleanup_data(client: &mut Client, project: &str, run_id: &str) -> Result<usize, Status> {
    delete_kind(
        client,
        project,
        KIND,
        Some(equals("test_id", str_value(run_id))),
    )
    .await
}

/// What one client's scenarios work on: its own entity (get, update,
/// transaction), GET_MULTI_KEYS entities for get_multi, and the seed group
/// it queries.
struct ClientRun {
    client: Client,
    project: String,
    run_id: String,
    own_key: Key,
    multi_keys: Vec<Key>,
    seed: Seed,
    group: i64,
}

async fn setup_client_run(
    mut client: Client,
    project: &str,
    index: usize,
    seed: Seed,
    run_id: String,
) -> Result<ClientRun, Status> {
    let own_key = name_key(project, KIND, &format!("own-{run_id}"));
    let own = entity(
        own_key.clone(),
        vec![
            ("test_id", str_value(&run_id)),
            ("counter", int_value(0)),
            ("version", int_value(0)),
        ],
    );
    let multi_keys: Vec<Key> = (0..GET_MULTI_KEYS)
        .map(|i| name_key(project, KIND, &format!("multi-{run_id}-{i}")))
        .collect();
    let mut entities = vec![own];
    entities.extend(multi_keys.iter().enumerate().map(|(i, key)| {
        entity(
            key.clone(),
            vec![
                ("test_id", str_value(&run_id)),
                ("index", int_value(i as i64)),
            ],
        )
    }));
    put_in_chunks(&mut client, project, entities).await?;
    Ok(ClientRun {
        client,
        project: project.to_string(),
        run_id,
        own_key,
        multi_keys,
        group: (index % seed.groups) as i64,
        seed,
    })
}

// --- scenarios ----------------------------------------------------------

impl ClientRun {
    async fn single_insert(&mut self) -> Result<(), Status> {
        let key = name_key(
            &self.project,
            KIND,
            &format!("task-{}-{}", self.run_id, Uuid::new_v4()),
        );
        let new = entity(
            key,
            vec![
                ("test_id", str_value(&self.run_id)),
                ("value", str_value(&Uuid::new_v4().to_string())),
            ],
        );
        commit(&mut self.client, &self.project, vec![upsert(new)], None).await
    }

    async fn bulk_insert(&mut self) -> Result<(), Status> {
        let batch = Uuid::new_v4();
        let mutations = (0..50)
            .map(|i| {
                let key = name_key(
                    &self.project,
                    KIND,
                    &format!("task-{}-{batch}-{i}", self.run_id),
                );
                upsert(entity(key, vec![("test_id", str_value(&self.run_id))]))
            })
            .collect();
        commit(&mut self.client, &self.project, mutations, None).await
    }

    async fn simple_query(&mut self) -> Result<Vec<Entity>, Status> {
        let batch = run_query(
            &mut self.client,
            &self.project,
            query(
                KIND,
                Some(equals("test_id", str_value(&self.run_id))),
                None,
                Some(10),
            ),
        )
        .await?;
        Ok(batch
            .entity_results
            .into_iter()
            .filter_map(|r| r.entity)
            .collect())
    }

    async fn get(&mut self) -> Result<Option<Entity>, Status> {
        Ok(lookup(
            &mut self.client,
            &self.project,
            vec![self.own_key.clone()],
            None,
        )
        .await?
        .pop())
    }

    async fn get_multi(&mut self) -> Result<Vec<Entity>, Status> {
        lookup(
            &mut self.client,
            &self.project,
            self.multi_keys.clone(),
            None,
        )
        .await
    }

    async fn update(&mut self) -> Result<(), Status> {
        let mut own = self
            .get()
            .await?
            .ok_or_else(|| Status::not_found("own entity"))?;
        let version = int_of(&own, "version") + 1;
        own.properties
            .insert("version".to_string(), int_value(version));
        own.properties.insert(
            "updated".to_string(),
            str_value(&Uuid::new_v4().to_string()),
        );
        commit(&mut self.client, &self.project, vec![upsert(own)], None).await
    }

    fn group_query(&self, limit: i32) -> Query {
        query(
            &self.seed.kind,
            Some(equals("group", int_value(self.group))),
            Some("score"),
            Some(limit),
        )
    }

    async fn query_filter_order(&mut self) -> Result<Vec<Entity>, Status> {
        let group_query = self.group_query(QUERY_LIMIT);
        let batch = run_query(&mut self.client, &self.project, group_query).await?;
        Ok(batch
            .entity_results
            .into_iter()
            .filter_map(|r| r.entity)
            .collect())
    }

    /// Walks the client's seed group page by page through cursors; always
    /// the same number of requests, whatever the emulator says after the last page.
    async fn cursor_pagination(&mut self) -> Result<Vec<Key>, Status> {
        let mut page_query = self.group_query(self.seed.page_size as i32);
        let mut keys = Vec::with_capacity(self.seed.per_group);
        for _ in 0..self.seed.per_group / self.seed.page_size {
            let batch = run_query(&mut self.client, &self.project, page_query.clone()).await?;
            let end_cursor = batch.end_cursor.clone();
            keys.extend(batch_keys(batch));
            if end_cursor.is_empty() {
                break;
            }
            page_query.start_cursor = end_cursor;
        }
        Ok(keys)
    }

    async fn transaction_rmw(&mut self) -> Result<(), Status> {
        let transaction = timed(
            REQUEST_TIMEOUT,
            self.client.begin_transaction(BeginTransactionRequest {
                project_id: self.project.clone(),
                database_id: String::new(),
                transaction_options: None,
            }),
        )
        .await?
        .into_inner()
        .transaction;
        let outcome = async {
            let mut own = lookup(
                &mut self.client,
                &self.project,
                vec![self.own_key.clone()],
                Some(transaction.clone()),
            )
            .await?
            .pop()
            .ok_or_else(|| Status::not_found("own entity"))?;
            let counter = int_of(&own, "counter") + 1;
            own.properties
                .insert("counter".to_string(), int_value(counter));
            commit(
                &mut self.client,
                &self.project,
                vec![upsert(own)],
                Some(transaction.clone()),
            )
            .await
        }
        .await;
        if outcome.is_err() {
            let _ = timed(
                REQUEST_TIMEOUT,
                self.client.rollback(RollbackRequest {
                    project_id: self.project.clone(),
                    database_id: String::new(),
                    transaction,
                }),
            )
            .await;
        }
        outcome
    }

    /// An app-like request: two reads, one update, one small query.
    async fn mixed(&mut self) -> Result<(), Status> {
        self.get().await?;
        self.get_multi().await?;
        self.update().await?;
        self.simple_query().await?;
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Scenario {
    SingleInsert,
    BulkInsert,
    SimpleQuery,
    Get,
    GetMulti,
    Update,
    QueryFilterOrder,
    CursorPagination,
    TransactionRmw,
    Mixed,
}

impl Scenario {
    const ALL: [Scenario; 10] = [
        Scenario::SingleInsert,
        Scenario::BulkInsert,
        Scenario::SimpleQuery,
        Scenario::Get,
        Scenario::GetMulti,
        Scenario::Update,
        Scenario::QueryFilterOrder,
        Scenario::CursorPagination,
        Scenario::TransactionRmw,
        Scenario::Mixed,
    ];

    /// The chart names, shared with the Python harness.
    fn name(self) -> &'static str {
        match self {
            Scenario::SingleInsert => "Single Insert",
            Scenario::BulkInsert => "Bulk Insert (50)",
            Scenario::SimpleQuery => "Simple Query",
            Scenario::Get => "Get",
            Scenario::GetMulti => "Get Multi (100)",
            Scenario::Update => "Update",
            Scenario::QueryFilterOrder => "Query Filter+Order (5k)",
            Scenario::CursorPagination => "Cursor Pagination (5x100)",
            Scenario::TransactionRmw => "Transaction RMW",
            Scenario::Mixed => "Mixed (get, get_multi, update, query)",
        }
    }

    async fn run(self, run: &mut ClientRun) -> Result<(), Status> {
        match self {
            Scenario::SingleInsert => run.single_insert().await,
            Scenario::BulkInsert => run.bulk_insert().await,
            Scenario::SimpleQuery => run.simple_query().await.map(drop),
            Scenario::Get => run.get().await.map(drop),
            Scenario::GetMulti => run.get_multi().await.map(drop),
            Scenario::Update => run.update().await,
            Scenario::QueryFilterOrder => run.query_filter_order().await.map(drop),
            Scenario::CursorPagination => run.cursor_pagination().await.map(drop),
            Scenario::TransactionRmw => run.transaction_rmw().await,
            Scenario::Mixed => run.mixed().await,
        }
    }
}

// --- statistics ---------------------------------------------------------

#[derive(Clone, Copy, Debug, Serialize)]
struct ScenarioResult {
    total: f64,
    p50: f64,
    p95: f64,
}

struct Summary {
    p50: f64,
    p95: f64,
}

fn median(sorted: &[f64]) -> f64 {
    let n = sorted.len();
    if n % 2 == 1 {
        sorted[n / 2]
    } else {
        (sorted[n / 2 - 1] + sorted[n / 2]) / 2.0
    }
}

/// Nearest-rank percentile of an ascending list.
fn percentile(sorted: &[f64], fraction: f64) -> f64 {
    let rank = ((fraction * sorted.len() as f64).ceil() as usize).max(1);
    sorted[rank - 1]
}

fn summarize(timings: &[f64]) -> Summary {
    let mut sorted = timings.to_vec();
    sorted.sort_by(|a, b| a.total_cmp(b));
    Summary {
        p50: median(&sorted),
        p95: percentile(&sorted, 0.95),
    }
}

fn median_of(values: impl Iterator<Item = f64>) -> f64 {
    let mut sorted: Vec<f64> = values.collect();
    sorted.sort_by(|a, b| a.total_cmp(b));
    median(&sorted)
}

fn median_of_results(results: &[ScenarioResult]) -> ScenarioResult {
    ScenarioResult {
        total: median_of(results.iter().map(|r| r.total)),
        p50: median_of(results.iter().map(|r| r.p50)),
        p95: median_of(results.iter().map(|r| r.p95)),
    }
}

// --- orchestration ------------------------------------------------------

/// One client's scenario raised `error`; `released` when the client was let
/// go by another client's failure.
#[derive(Debug)]
struct ScenarioFailed {
    scenario: &'static str,
    error: String,
    released: bool,
}

/// What one client runs per scenario call; the tests substitute fakes.
trait Workload {
    fn call(&mut self, scenario: Scenario) -> impl Future<Output = Result<(), Status>> + Send;
}

impl Workload for ClientRun {
    fn call(&mut self, scenario: Scenario) -> impl Future<Output = Result<(), Status>> + Send {
        scenario.run(self)
    }
}

/// Times every scenario for one client. Clients meet at `barrier` before each
/// scenario, so a slow scenario on one client never overlaps the next
/// scenario's timings on another; a failing client flips `failed` to release
/// the others from the barrier.
async fn run_scenarios(
    workload: &mut impl Workload,
    scenarios: &[Scenario],
    num_runs: usize,
    warmup_runs: usize,
    barrier: Arc<Barrier>,
    failed: watch::Sender<bool>,
    mut released: watch::Receiver<bool>,
) -> Result<HashMap<&'static str, Vec<f64>>, ScenarioFailed> {
    let mut timings = HashMap::new();
    for &scenario in scenarios {
        if std::env::var_os("LOADGEN_TRACE").is_some() {
            eprintln!(
                "[trace] {} waiting at barrier before {:?}",
                std::thread::current().name().unwrap_or("?"),
                scenario
            );
        }
        tokio::select! {
            _ = barrier.wait() => {}
            _ = released.changed() => {
                return Err(ScenarioFailed { scenario: scenario.name(), error: "released by another client's failure".to_string(), released: true });
            }
        }
        let mut timed = Vec::with_capacity(num_runs);
        for i in 0..warmup_runs + num_runs {
            if std::env::var_os("LOADGEN_TRACE").is_some() {
                eprintln!("[trace] {scenario:?} call {i}");
            }
            let start = Instant::now();
            if let Err(error) = workload.call(scenario).await {
                let _ = failed.send(true);
                return Err(ScenarioFailed {
                    scenario: scenario.name(),
                    error: error.to_string(),
                    released: false,
                });
            }
            if i >= warmup_runs {
                timed.push(start.elapsed().as_secs_f64());
            }
        }
        timings.insert(scenario.name(), timed);
    }
    Ok(timings)
}

#[derive(Debug)]
struct EmulatorFailed {
    message: String,
}

impl EmulatorFailed {
    fn new(label: &str, scenario: &str, error: impl fmt::Display) -> Self {
        EmulatorFailed {
            message: format!("{label} emulator failed during {scenario:?}: {error}"),
        }
    }
}

impl fmt::Display for EmulatorFailed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_client(
    index: usize,
    endpoint: &str,
    project: &str,
    seed: Seed,
    barrier: Arc<Barrier>,
    failed: watch::Sender<bool>,
    released: watch::Receiver<bool>,
    num_runs: usize,
    warmup_runs: usize,
) -> Result<HashMap<&'static str, Vec<f64>>, ScenarioFailed> {
    let run_id = format!("perf-{}", Uuid::new_v4());
    let setup = async {
        let client = connect(endpoint).await?;
        setup_client_run(client, project, index, seed, run_id.clone()).await
    }
    .await;
    let outcome = match setup {
        Ok(mut run) => {
            run_scenarios(
                &mut run,
                &Scenario::ALL,
                num_runs,
                warmup_runs,
                barrier,
                failed,
                released,
            )
            .await
        }
        Err(error) => {
            let _ = failed.send(true);
            Err(ScenarioFailed {
                scenario: "setup",
                error: error.to_string(),
                released: false,
            })
        }
    };
    if let Ok(mut client) = connect(endpoint).await {
        let _ = cleanup_data(&mut client, project, &run_id).await;
    }
    outcome
}

/// A client task that panics would leave the others waiting at the barrier
/// forever; turn the panic into a failure that releases them.
async fn guard_panics(
    failed: watch::Sender<bool>,
    client: impl Future<Output = Result<HashMap<&'static str, Vec<f64>>, ScenarioFailed>>,
) -> Result<HashMap<&'static str, Vec<f64>>, ScenarioFailed> {
    use futures::FutureExt;
    match std::panic::AssertUnwindSafe(client).catch_unwind().await {
        Ok(outcome) => outcome,
        Err(panic) => {
            let _ = failed.send(true);
            let message = panic
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| panic.downcast_ref::<&str>().map(|m| m.to_string()))
                .unwrap_or_else(|| "panic".to_string());
            Err(ScenarioFailed {
                scenario: "panic",
                error: message,
                released: false,
            })
        }
    }
}

/// One pass with `num_clients` concurrent clients; returns {scenario: result}.
async fn run_once(
    label: &str,
    endpoint: &str,
    project: &str,
    seed: &Seed,
    num_clients: usize,
    num_runs: usize,
    warmup_runs: usize,
) -> Result<BTreeMap<&'static str, ScenarioResult>, EmulatorFailed> {
    let barrier = Arc::new(Barrier::new(num_clients));
    let (failed, released) = watch::channel(false);
    let handles: Vec<_> = (0..num_clients)
        .map(|index| {
            let (endpoint, project, seed) =
                (endpoint.to_string(), project.to_string(), seed.clone());
            let (barrier, failed, released) = (barrier.clone(), failed.clone(), released.clone());
            tokio::spawn(async move {
                let guard = failed.clone();
                guard_panics(
                    guard,
                    run_client(
                        index,
                        &endpoint,
                        &project,
                        seed,
                        barrier,
                        failed,
                        released,
                        num_runs,
                        warmup_runs,
                    ),
                )
                .await
            })
        })
        .collect();
    drop(released);
    let mut results = Vec::new();
    let mut failures = Vec::new();
    for handle in handles {
        match handle.await.expect("client task panicked") {
            Ok(timings) => results.push(timings),
            Err(failure) => failures.push(failure),
        }
    }
    if !failures.is_empty() {
        // Clients released by the failure only echo it.
        let causes: Vec<&ScenarioFailed> = failures.iter().filter(|f| !f.released).collect();
        let (first, count) = if causes.is_empty() {
            (&failures[0], failures.len())
        } else {
            (causes[0], causes.len())
        };
        return Err(EmulatorFailed::new(
            label,
            first.scenario,
            format!("{} ({count} of {num_clients} clients failed)", first.error),
        ));
    }
    let mut summary = BTreeMap::new();
    for scenario in Scenario::ALL {
        let name = scenario.name();
        let per_client: Vec<f64> = results.iter().map(|r| r[name].iter().sum()).collect();
        let pooled: Vec<f64> = results
            .iter()
            .flat_map(|r| r[name].iter().copied())
            .collect();
        let stats = summarize(&pooled);
        summary.insert(
            name,
            ScenarioResult {
                total: per_client.iter().sum::<f64>() / per_client.len() as f64,
                p50: stats.p50,
                p95: stats.p95,
            },
        );
    }
    Ok(summary)
}

/// A bound port is not readiness: the Java emulator resets connections while
/// it starts. Poll with a lookup until one succeeds.
async fn wait_until_ready(
    label: &str,
    endpoint: &str,
    project: &str,
    timeout: Duration,
) -> Result<(), EmulatorFailed> {
    let deadline = Instant::now() + timeout;
    let mut last_error = "no attempt".to_string();
    while Instant::now() < deadline {
        let probe = timed(Duration::from_secs(5), async {
            let mut client = connect_with_timeout(endpoint, Duration::from_secs(5)).await?;
            lookup(
                &mut client,
                project,
                vec![name_key(project, "Healthcheck", "ping")],
                None,
            )
            .await
        })
        .await;
        match probe {
            Ok(_) => return Ok(()),
            Err(error) => last_error = error.to_string(),
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    Err(EmulatorFailed::new(
        label,
        "startup",
        format!("not ready after {}s: {last_error}", timeout.as_secs()),
    ))
}

#[derive(Clone, Copy, Debug, Serialize)]
struct Settings {
    num_clients: usize,
    num_runs: usize,
    warmup_runs: usize,
    repeat: usize,
}

type TargetResults = BTreeMap<&'static str, ScenarioResult>;

async fn run_target(
    label: &str,
    endpoint: &str,
    project: &str,
    settings: Settings,
) -> Result<TargetResults, EmulatorFailed> {
    println!(
        "\n--- {label} at {endpoint}: {} clients, {} runs/client, {} warm-up, {} repeat(s) ---",
        settings.num_clients, settings.num_runs, settings.warmup_runs, settings.repeat
    );
    wait_until_ready(label, endpoint, project, Duration::from_secs(120)).await?;
    let mut client = connect(endpoint)
        .await
        .map_err(|e| EmulatorFailed::new(label, "seed", e))?;
    let started = Instant::now();
    let seed = seed_dataset(&mut client, project, SEED_TOTAL, SEED_GROUPS, PAGE_SIZE)
        .await
        .map_err(|e| EmulatorFailed::new(label, "seed", e))?;
    println!(
        "  seeded {SEED_TOTAL} entities in {:.1}s",
        started.elapsed().as_secs_f64()
    );
    let mut repeats = Vec::with_capacity(settings.repeat);
    let mut outcome = Ok(());
    for i in 0..settings.repeat {
        let started = Instant::now();
        match run_once(
            label,
            endpoint,
            project,
            &seed,
            settings.num_clients,
            settings.num_runs,
            settings.warmup_runs,
        )
        .await
        {
            Ok(results) => repeats.push(results),
            Err(error) => {
                outcome = Err(error);
                break;
            }
        }
        println!(
            "  repeat {}/{} done in {:.1}s",
            i + 1,
            settings.repeat,
            started.elapsed().as_secs_f64()
        );
    }
    if let Err(error) = cleanup_seed(&mut client, project, &seed).await {
        println!("  seed cleanup failed: {error}");
    }
    outcome?;
    Ok(Scenario::ALL
        .iter()
        .map(|scenario| {
            let name = scenario.name();
            (
                name,
                median_of_results(&repeats.iter().map(|r| r[name]).collect::<Vec<_>>()),
            )
        })
        .collect())
}

#[derive(Serialize)]
struct Output<'a> {
    settings: &'a Settings,
    targets: &'a BTreeMap<String, TargetResults>,
}

fn output_json(settings: &Settings, targets: &BTreeMap<String, TargetResults>) -> String {
    serde_json::to_string_pretty(&Output { settings, targets }).expect("results serialize")
}

fn print_summary(targets: &BTreeMap<String, TargetResults>, settings: Settings) {
    println!("\n--- Benchmark Summary ---");
    for scenario in Scenario::ALL {
        let name = scenario.name();
        println!("\nOperation: {name}");
        for (label, results) in targets {
            let r = results[name];
            println!(
                "  - {label} ({} clients, {} runs each): avg per client {:.4}s, p50 {:.1} ms, p95 {:.1} ms",
                settings.num_clients,
                settings.num_runs,
                r.total,
                r.p50 * 1000.0,
                r.p95 * 1000.0
            );
        }
        if let (Some(rust), Some(java)) = (targets.get("Rust"), targets.get("Java")) {
            println!(
                "  - Verdict: Rust p50 is {:.2}x faster than Java.",
                java[name].p50 / rust[name].p50
            );
        }
    }
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Target {
    Rust,
    Java,
    Both,
}

/// Counts that must be at least one: zero would leave nothing to summarize.
fn at_least_one(text: &str) -> Result<usize, String> {
    match text.parse::<usize>() {
        Ok(0) => Err("must be at least 1".to_string()),
        Ok(value) => Ok(value),
        Err(error) => Err(error.to_string()),
    }
}

/// Benchmark an emulator with N concurrent gRPC clients.
#[derive(Parser, Debug)]
#[command(version, about)]
struct Cli {
    #[arg(long, value_enum, default_value_t = Target::Both)]
    target: Target,
    /// Endpoint override, e.g. http://localhost:8042; needs a single --target.
    #[arg(long)]
    host: Option<String>,
    #[arg(long, default_value_t = String::from("test-project-2"))]
    project: String,
    #[arg(long, default_value_t = 10, value_parser = at_least_one)]
    num_clients: usize,
    /// Timed runs per client per scenario.
    #[arg(long, default_value_t = 20, value_parser = at_least_one)]
    num_runs: usize,
    /// Untimed runs before timing.
    #[arg(long, default_value_t = 5)]
    warmup_runs: usize,
    /// Whole-run repeats; the median is kept.
    #[arg(long, default_value_t = 3, value_parser = at_least_one)]
    repeat: usize,
    /// Write the per-target summaries as JSON for merge_results.py.
    #[arg(long)]
    json_out: Option<PathBuf>,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let targets: Vec<(&str, &str)> = match cli.target {
        Target::Rust => vec![("Rust", "http://localhost:8042")],
        Target::Java => vec![("Java", "http://localhost:8044")],
        Target::Both => vec![
            ("Rust", "http://localhost:8042"),
            ("Java", "http://localhost:8044"),
        ],
    };
    if cli.host.is_some() && targets.len() > 1 {
        eprintln!("--host needs a single --target");
        std::process::exit(1);
    }
    let settings = Settings {
        num_clients: cli.num_clients,
        num_runs: cli.num_runs,
        warmup_runs: cli.warmup_runs,
        repeat: cli.repeat,
    };
    let mut results = BTreeMap::new();
    for (label, default_endpoint) in targets {
        let endpoint = cli
            .host
            .clone()
            .unwrap_or_else(|| default_endpoint.to_string());
        match run_target(label, &endpoint, &cli.project, settings).await {
            Ok(target_results) => {
                results.insert(label.to_string(), target_results);
            }
            Err(error) => {
                println!("\n::error::{error}");
                std::process::exit(2);
            }
        }
    }
    print_summary(&results, settings);
    if let Some(path) = cli.json_out {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("create output directory");
        }
        std::fs::write(&path, output_json(&settings, &results)).expect("write results");
        println!("\nWrote {}", path.display());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datastore_emulator::DatastoreEmulator;
    use datastore_emulator::google::datastore::v1;
    use datastore_emulator::google::datastore::v1::datastore_server::DatastoreServer;
    use std::sync::Mutex;
    use std::time::Duration;
    use tokio::sync::Barrier;
    use tonic::transport::server::TcpIncoming;

    async fn spawn_server() -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let incoming = TcpIncoming::from_listener(listener, true, None).unwrap();
        tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(DatastoreServer::new(DatastoreEmulator::new(false)))
                .serve_with_incoming(incoming),
        );
        format!("http://{addr}")
    }

    const PROJECT: &str = "test-project-2";

    /// Speaks gRPC but never answers a lookup, like a wedged emulator.
    struct Hanging;

    #[tonic::async_trait]
    impl datastore_emulator::google::datastore::v1::datastore_server::Datastore for Hanging {
        async fn ping(
            &self,
            _: tonic::Request<v1::PingRequest>,
        ) -> Result<tonic::Response<v1::PingResponse>, Status> {
            Err(Status::unimplemented("ping"))
        }
        async fn begin_transaction(
            &self,
            _: tonic::Request<v1::BeginTransactionRequest>,
        ) -> Result<tonic::Response<v1::BeginTransactionResponse>, Status> {
            Err(Status::unimplemented("begin_transaction"))
        }
        async fn lookup(
            &self,
            _: tonic::Request<v1::LookupRequest>,
        ) -> Result<tonic::Response<v1::LookupResponse>, Status> {
            std::future::pending().await
        }
        async fn run_query(
            &self,
            _: tonic::Request<v1::RunQueryRequest>,
        ) -> Result<tonic::Response<v1::RunQueryResponse>, Status> {
            Err(Status::unimplemented("run_query"))
        }
        async fn commit(
            &self,
            _: tonic::Request<v1::CommitRequest>,
        ) -> Result<tonic::Response<v1::CommitResponse>, Status> {
            Err(Status::unimplemented("commit"))
        }
        async fn rollback(
            &self,
            _: tonic::Request<v1::RollbackRequest>,
        ) -> Result<tonic::Response<v1::RollbackResponse>, Status> {
            Err(Status::unimplemented("rollback"))
        }
        async fn run_aggregation_query(
            &self,
            _: tonic::Request<v1::RunAggregationQueryRequest>,
        ) -> Result<tonic::Response<v1::RunAggregationQueryResponse>, Status> {
            Err(Status::unimplemented("run_aggregation_query"))
        }
        async fn allocate_ids(
            &self,
            _: tonic::Request<v1::AllocateIdsRequest>,
        ) -> Result<tonic::Response<v1::AllocateIdsResponse>, Status> {
            Err(Status::unimplemented("allocate_ids"))
        }
        async fn reserve_ids(
            &self,
            _: tonic::Request<v1::ReserveIdsRequest>,
        ) -> Result<tonic::Response<v1::ReserveIdsResponse>, Status> {
            Err(Status::unimplemented("reserve_ids"))
        }
    }

    #[tokio::test]
    async fn a_request_the_emulator_never_answers_times_out() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let incoming = TcpIncoming::from_listener(listener, true, None).unwrap();
        tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(DatastoreServer::new(Hanging))
                .serve_with_incoming(incoming),
        );
        let endpoint = format!("http://{addr}");

        let outcome = tokio::time::timeout(Duration::from_secs(5), async {
            let mut client = connect_with_timeout(&endpoint, Duration::from_millis(300))
                .await
                .unwrap();
            lookup(
                &mut client,
                PROJECT,
                vec![name_key(PROJECT, "Healthcheck", "ping")],
                None,
            )
            .await
        })
        .await
        .expect("the request must give up on its own");
        assert!(outcome.is_err());
    }

    #[tokio::test]
    async fn a_connection_the_emulator_never_completes_times_out() {
        // Accepts TCP connections but never speaks HTTP/2, like an emulator wedged after bind.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let mut held = Vec::new();
            loop {
                held.push(listener.accept().await.unwrap());
            }
        });

        let outcome = tokio::time::timeout(Duration::from_secs(5), async {
            let mut client =
                connect_with_timeout(&format!("http://{addr}"), Duration::from_millis(300))
                    .await
                    .unwrap();
            lookup(
                &mut client,
                PROJECT,
                vec![name_key(PROJECT, "Healthcheck", "ping")],
                None,
            )
            .await
        })
        .await
        .expect("the first request must give up on its own");
        assert!(outcome.is_err());
    }

    type Events = Arc<Mutex<Vec<(Scenario, usize, Instant, Instant)>>>;

    /// Sleeps `10 ms * index` on Single Insert and records every call.
    struct Recorder {
        index: usize,
        events: Events,
    }

    impl Workload for Recorder {
        fn call(&mut self, scenario: Scenario) -> impl Future<Output = Result<(), Status>> + Send {
            let (index, events) = (self.index, self.events.clone());
            async move {
                let start = Instant::now();
                if scenario == Scenario::SingleInsert {
                    tokio::time::sleep(Duration::from_millis(10 * index as u64)).await;
                }
                events
                    .lock()
                    .unwrap()
                    .push((scenario, index, start, Instant::now()));
                Ok(())
            }
        }
    }

    struct Failing {
        bad: bool,
    }

    impl Workload for Failing {
        fn call(&mut self, scenario: Scenario) -> impl Future<Output = Result<(), Status>> + Send {
            let fail = self.bad && scenario == Scenario::SingleInsert;
            async move {
                if fail {
                    Err(Status::unavailable("down"))
                } else {
                    Ok(())
                }
            }
        }
    }

    async fn client_run(endpoint: &str, seed: &Seed) -> ClientRun {
        let client = connect(endpoint).await.unwrap();
        setup_client_run(
            client,
            PROJECT,
            1,
            seed.clone(),
            format!("perf-{}", uuid::Uuid::new_v4()),
        )
        .await
        .unwrap()
    }

    async fn small_seed(endpoint: &str) -> (Client, Seed) {
        let mut client = connect(endpoint).await.unwrap();
        let seed = seed_dataset(&mut client, PROJECT, 100, 2, 10)
            .await
            .unwrap();
        (client, seed)
    }

    #[test]
    fn zero_clients_runs_or_repeats_are_rejected_at_the_cli() {
        for flag in ["--num-clients", "--num-runs", "--repeat"] {
            assert!(
                Cli::try_parse_from(["loadgen", flag, "0"]).is_err(),
                "{flag} 0 must be rejected"
            );
            assert!(
                Cli::try_parse_from(["loadgen", flag, "1"]).is_ok(),
                "{flag} 1 must be accepted"
            );
        }
        assert!(Cli::try_parse_from(["loadgen", "--warmup-runs", "0"]).is_ok());
    }

    #[test]
    fn seed_rows_spread_the_groups_evenly() {
        let rows = seed_rows(100, 4);
        assert_eq!(rows.len(), 100);
        let names: std::collections::HashSet<_> = rows.iter().map(|r| r.0.clone()).collect();
        assert_eq!(names.len(), 100);
        for g in 0..4 {
            assert_eq!(rows.iter().filter(|r| r.1 == g).count(), 25);
        }
        assert!(rows.iter().all(|r| (0..1_000_000).contains(&r.2)));
    }

    #[test]
    fn summarize_reports_percentiles() {
        let s = summarize(&[0.3, 0.1, 1.0, 0.4, 0.2]);
        assert!((s.p50 - 0.3).abs() < 1e-9);
        assert!((s.p95 - 1.0).abs() < 1e-9);
        let even = summarize(&[1.0, 2.0, 3.0, 4.0]);
        assert!((even.p50 - 2.5).abs() < 1e-9);
    }

    #[test]
    fn median_of_results_takes_the_median_per_metric() {
        let m = median_of_results(&[
            ScenarioResult {
                total: 1.0,
                p50: 1.0,
                p95: 5.0,
            },
            ScenarioResult {
                total: 2.0,
                p50: 3.0,
                p95: 4.0,
            },
            ScenarioResult {
                total: 3.0,
                p50: 2.0,
                p95: 9.0,
            },
        ]);
        assert_eq!((m.total, m.p50, m.p95), (2.0, 2.0, 5.0));
    }

    #[test]
    fn json_output_matches_the_python_harness() {
        let mut targets = BTreeMap::new();
        let mut rust = BTreeMap::new();
        rust.insert(
            "Get",
            ScenarioResult {
                total: 1.0,
                p50: 0.01,
                p95: 0.02,
            },
        );
        targets.insert("Rust".to_string(), rust);
        let settings = Settings {
            num_clients: 10,
            num_runs: 20,
            warmup_runs: 5,
            repeat: 3,
        };
        let json = output_json(&settings, &targets);
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(value["settings"]["num_clients"], 10);
        assert_eq!(value["settings"]["repeat"], 3);
        assert_eq!(value["targets"]["Rust"]["Get"]["p50"], 0.01);
    }

    #[tokio::test]
    async fn get_returns_the_client_entity() {
        let endpoint = spawn_server().await;
        let (_, seed) = small_seed(&endpoint).await;
        let mut run = client_run(&endpoint, &seed).await;
        let entity = run.get().await.unwrap().expect("own entity");
        assert_eq!(entity.key, Some(run.own_key.clone()));
        assert_eq!(int_of(&entity, "counter"), 0);
    }

    #[tokio::test]
    async fn get_multi_returns_every_key() {
        let endpoint = spawn_server().await;
        let (_, seed) = small_seed(&endpoint).await;
        let mut run = client_run(&endpoint, &seed).await;
        let entities = run.get_multi().await.unwrap();
        assert_eq!(entities.len(), GET_MULTI_KEYS);
    }

    #[tokio::test]
    async fn update_bumps_the_version() {
        let endpoint = spawn_server().await;
        let (_, seed) = small_seed(&endpoint).await;
        let mut run = client_run(&endpoint, &seed).await;
        run.update().await.unwrap();
        run.update().await.unwrap();
        assert_eq!(int_of(&run.get().await.unwrap().unwrap(), "version"), 2);
    }

    #[tokio::test]
    async fn transaction_increments_the_counter() {
        let endpoint = spawn_server().await;
        let (_, seed) = small_seed(&endpoint).await;
        let mut run = client_run(&endpoint, &seed).await;
        run.transaction_rmw().await.unwrap();
        run.transaction_rmw().await.unwrap();
        assert_eq!(int_of(&run.get().await.unwrap().unwrap(), "counter"), 2);
    }

    #[tokio::test]
    async fn query_filter_order_returns_the_group_sorted_by_score() {
        let endpoint = spawn_server().await;
        let (_, seed) = small_seed(&endpoint).await;
        let mut run = client_run(&endpoint, &seed).await;
        let entities = run.query_filter_order().await.unwrap();
        assert_eq!(entities.len(), QUERY_LIMIT as usize);
        assert!(entities.iter().all(|e| int_of(e, "group") == run.group));
        let scores: Vec<i64> = entities.iter().map(|e| int_of(e, "score")).collect();
        let mut sorted = scores.clone();
        sorted.sort_unstable_by(|a, b| b.cmp(a));
        assert_eq!(scores, sorted);
    }

    #[tokio::test]
    async fn cursor_pagination_walks_the_whole_group() {
        let endpoint = spawn_server().await;
        let (_, seed) = small_seed(&endpoint).await;
        let mut run = client_run(&endpoint, &seed).await;
        let keys = run.cursor_pagination().await.unwrap();
        assert_eq!(keys.len(), seed.per_group);
        let distinct: std::collections::HashSet<String> =
            keys.iter().map(|k| format!("{k:?}")).collect();
        assert_eq!(distinct.len(), seed.per_group);
    }

    #[tokio::test]
    async fn mixed_workload_updates_the_client_entity() {
        let endpoint = spawn_server().await;
        let (_, seed) = small_seed(&endpoint).await;
        let mut run = client_run(&endpoint, &seed).await;
        run.mixed().await.unwrap();
        assert_eq!(int_of(&run.get().await.unwrap().unwrap(), "version"), 1);
    }

    #[tokio::test]
    async fn cleanup_removes_the_seed_and_the_client_data() {
        let endpoint = spawn_server().await;
        let (mut client, seed) = small_seed(&endpoint).await;
        let mut run = client_run(&endpoint, &seed).await;
        run.single_insert().await.unwrap();
        cleanup_data(&mut client, PROJECT, &run.run_id)
            .await
            .unwrap();
        cleanup_seed(&mut client, PROJECT, &seed).await.unwrap();
        let mut keys = vec![run.own_key.clone()];
        keys.extend(run.multi_keys.iter().cloned());
        assert!(
            lookup(&mut client, PROJECT, keys, None)
                .await
                .unwrap()
                .is_empty()
        );
        let batch = run_query(&mut client, PROJECT, query(&seed.kind, None, None, Some(1)))
            .await
            .unwrap();
        assert!(batch.entity_results.is_empty());
        let batch = run_query(
            &mut client,
            PROJECT,
            query(
                KIND,
                Some(equals("test_id", str_value(&run.run_id))),
                None,
                Some(1),
            ),
        )
        .await
        .unwrap();
        assert!(batch.entity_results.is_empty());
    }

    #[tokio::test]
    async fn run_once_times_every_scenario_for_every_client() {
        let endpoint = spawn_server().await;
        let (mut client, seed) = small_seed(&endpoint).await;
        let results = run_once("Rust", &endpoint, PROJECT, &seed, 2, 2, 1)
            .await
            .unwrap();
        assert_eq!(results.len(), Scenario::ALL.len());
        for scenario in Scenario::ALL {
            let r = &results[scenario.name()];
            assert!(
                r.total > 0.0 && r.p50 > 0.0 && r.p95 >= r.p50,
                "{}",
                scenario.name()
            );
        }
        cleanup_seed(&mut client, PROJECT, &seed).await.unwrap();
    }

    #[tokio::test]
    async fn run_once_reports_a_dead_emulator() {
        let seed = Seed {
            kind: "PerfSeed_dead".into(),
            groups: 1,
            per_group: 10,
            page_size: 10,
        };
        let error = run_once("Rust", "http://127.0.0.1:1", PROJECT, &seed, 2, 1, 0)
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("Rust emulator failed during"),
            "{error}"
        );
        assert!(
            error.to_string().contains("2 of 2 clients failed"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn run_scenarios_keeps_clients_on_the_same_scenario() {
        let clients = 3;
        let barrier = Arc::new(Barrier::new(clients));
        let (failed, _rx) = tokio::sync::watch::channel(false);
        let events: Events = Arc::default();
        let scenarios = [Scenario::SingleInsert, Scenario::Get];
        let mut handles = vec![];
        for index in 1..=clients {
            let (barrier, rx, failed) = (barrier.clone(), failed.subscribe(), failed.clone());
            let mut recorder = Recorder {
                index,
                events: events.clone(),
            };
            handles.push(tokio::spawn(async move {
                run_scenarios(&mut recorder, &scenarios, 2, 1, barrier, failed, rx).await
            }));
        }
        for handle in handles {
            let timings = handle.await.unwrap().unwrap();
            assert_eq!(timings[Scenario::SingleInsert.name()].len(), 2);
        }
        let events = events.lock().unwrap();
        let slow_end = events
            .iter()
            .filter(|e| e.0 == Scenario::SingleInsert)
            .map(|e| e.3)
            .max()
            .unwrap();
        let fast_start = events
            .iter()
            .filter(|e| e.0 == Scenario::Get)
            .map(|e| e.2)
            .min()
            .unwrap();
        assert!(slow_end <= fast_start);
    }

    #[tokio::test]
    async fn a_panicking_client_releases_the_others() {
        let barrier = Arc::new(Barrier::new(2));
        let (failed, _rx) = tokio::sync::watch::channel(false);
        let scenarios = [Scenario::SingleInsert, Scenario::Get];
        let (good_barrier, good_rx, good_failed) =
            (barrier.clone(), failed.subscribe(), failed.clone());
        let good = tokio::spawn(async move {
            let mut workload = Failing { bad: false };
            run_scenarios(
                &mut workload,
                &scenarios,
                1,
                0,
                good_barrier,
                good_failed,
                good_rx,
            )
            .await
        });
        let bad = tokio::spawn(guard_panics(failed.clone(), async { panic!("boom") }));

        let outcome = tokio::time::timeout(Duration::from_secs(5), async {
            (bad.await.unwrap(), good.await.unwrap())
        })
        .await
        .expect("the other client must not hang");
        let bad = outcome.0.unwrap_err();
        assert_eq!(
            (bad.scenario, bad.error.as_str(), bad.released),
            ("panic", "boom", false)
        );
        assert!(outcome.1.unwrap_err().released);
    }

    #[tokio::test]
    async fn run_scenarios_releases_the_others_when_one_client_fails() {
        let barrier = Arc::new(Barrier::new(2));
        let (failed, _rx) = tokio::sync::watch::channel(false);
        let scenarios = [Scenario::SingleInsert, Scenario::Get];
        let mut handles = vec![];
        for bad in [true, false] {
            let (barrier, rx, failed) = (barrier.clone(), failed.subscribe(), failed.clone());
            let mut workload = Failing { bad };
            handles.push(tokio::spawn(async move {
                run_scenarios(&mut workload, &scenarios, 1, 0, barrier, failed, rx).await
            }));
        }
        let outcome = tokio::time::timeout(Duration::from_secs(5), async {
            let mut errors = vec![];
            for handle in handles {
                errors.push(handle.await.unwrap().unwrap_err());
            }
            errors
        })
        .await
        .expect("the released client must not hang");
        assert_eq!(outcome[0].scenario, Scenario::SingleInsert.name());
        assert!(!outcome[0].released);
        assert!(outcome[1].released);
    }
}
