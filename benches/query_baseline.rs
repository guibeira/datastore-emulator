use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use datastore_emulator::{
    DatastoreStorage, core,
    database::KeyStruct,
    google::datastore::v1::{
        AggregationQuery, ArrayValue, BeginTransactionRequest, CommitRequest, Entity, Filter, Key,
        KindExpression, LookupRequest, Mutation, PartitionId, Projection, PropertyFilter,
        PropertyOrder, PropertyReference, Query, RollbackRequest, RunAggregationQueryRequest,
        RunQueryRequest, TransactionOptions, Value,
        aggregation_query::{
            Aggregation, QueryType as AggregationQueryType,
            aggregation::{Count, Operator as AggOperator},
        },
        commit_request::{Mode, TransactionSelector},
        filter::FilterType,
        key::{PathElement, path_element::IdType},
        mutation::Operation,
        property_filter, property_order,
        run_aggregation_query_request::QueryType as RunAggQueryType,
        run_query_request::QueryType,
        transaction_options,
        value::ValueType,
    },
};
use pbjson_types::Int32Value;
use std::collections::HashMap;
use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Runtime;
use tokio::sync::RwLock;

const PROJECT: &str = "bench-project";
const KIND: &str = "BenchItem";
const SIGNUP_KIND: &str = "SignUpModel";
const SIGNUP_ROW_KIND: &str = "SignUpRow";
const DEFAULT_ENTITY_COUNT: usize = 10_000;
const DEFAULT_MIXED_SCOPE_ENTITIES_PER_KIND: usize = 1_000;
const MIXED_SCOPE_PROJECTS: usize = 5;
const MIXED_SCOPE_KINDS: usize = 5;

struct BenchCase {
    name: &'static str,
    query: Query,
}

fn criterion_benchmark(c: &mut Criterion) {
    let entity_count = env_usize("DATASTORE_QUERY_BENCH_ENTITIES", DEFAULT_ENTITY_COUNT);
    let mixed_scope_entities_per_kind = env_usize(
        "DATASTORE_QUERY_BENCH_MIXED_SCOPE_ENTITIES_PER_KIND",
        DEFAULT_MIXED_SCOPE_ENTITIES_PER_KIND,
    );
    let runtime = Runtime::new().expect("create tokio runtime");
    let storage = Arc::new(RwLock::new(seed_storage(entity_count)));
    let mixed_scope_storage = Arc::new(RwLock::new(seed_mixed_scope_storage(
        mixed_scope_entities_per_kind,
    )));
    let ancestor_storage = Arc::new(RwLock::new(seed_ancestor_storage(500, 20)));
    let missing_kind_keys_only_query = keys_only_query_for_kind("MissingCleanupKind");
    let ancestor_query = ancestor_query_for(250);

    runtime.block_on(async {
        for case in bench_cases() {
            black_box(execute_query(&storage, &case.query).await);
        }
        for case in mixed_scope_bench_cases() {
            black_box(execute_query(&mixed_scope_storage, &case.query).await);
        }
        black_box(execute_query(&mixed_scope_storage, &missing_kind_keys_only_query).await);
        black_box(execute_query(&ancestor_storage, &ancestor_query).await);
    });

    let mut group = c.benchmark_group("query_baseline");
    group.sample_size(env_usize("DATASTORE_QUERY_BENCH_SAMPLES", 50).max(10));
    group.measurement_time(Duration::from_secs(env_usize(
        "DATASTORE_QUERY_BENCH_MEASUREMENT_SECONDS",
        5,
    ) as u64));

    for case in bench_cases() {
        group.bench_function(case.name, |b| {
            let query = case.query.clone();
            b.to_async(&runtime).iter(|| async {
                black_box(execute_query(&storage, black_box(&query)).await);
            });
        });
    }

    for case in mixed_scope_bench_cases() {
        group.bench_function(case.name, |b| {
            let query = case.query.clone();
            b.to_async(&runtime).iter(|| async {
                black_box(execute_query(&mixed_scope_storage, black_box(&query)).await);
            });
        });
    }

    group.bench_function("missing_kind_keys_only_cleanup", |b| {
        let query = missing_kind_keys_only_query.clone();
        b.to_async(&runtime).iter(|| async {
            black_box(execute_query(&mixed_scope_storage, black_box(&query)).await);
        });
    });

    group.bench_function("ancestor_query_rows_20", |b| {
        let query = ancestor_query.clone();
        b.to_async(&runtime).iter(|| async {
            black_box(execute_query(&ancestor_storage, black_box(&query)).await);
        });
    });

    let lookup_keys: Vec<Key> = (0..100).map(|i| key_for(i * 100)).collect();
    group.bench_function("lookup_batch_100", |b| {
        let keys = lookup_keys.clone();
        b.to_async(&runtime).iter(|| async {
            black_box(execute_lookup(&storage, black_box(&keys)).await);
        });
    });

    let insert_template = bench_entity_template();
    group.bench_function("insert_entity_with_nested", |b| {
        b.iter_batched(
            || DatastoreStorage::default(),
            |mut storage| {
                storage
                    .insert_entity(&insert_template)
                    .expect("insert bench entity");
                black_box(storage);
            },
            BatchSize::SmallInput,
        );
    });

    let key_only_template = Entity {
        key: Some(key_for_name("key-only-insert-target")),
        properties: HashMap::new(),
    };
    group.bench_function("insert_entity_key_only", |b| {
        b.iter_batched(
            || DatastoreStorage::default(),
            |mut storage| {
                storage
                    .insert_entity(&key_only_template)
                    .expect("insert key-only bench entity");
                black_box(storage);
            },
            BatchSize::SmallInput,
        );
    });

    let index_template = bench_entity_template();
    let index_key = KeyStruct::from_datastore_key(index_template.key.as_ref().unwrap());
    group.bench_function("index_entity_with_nested", |b| {
        b.iter_batched(
            || DatastoreStorage::default(),
            |mut storage| {
                storage.update_indexes(&index_key, &index_template);
                black_box(storage);
            },
            BatchSize::SmallInput,
        );
    });

    let update_template = bench_entity_template();
    group.bench_function("update_entity_with_nested", |b| {
        b.to_async(&runtime).iter_batched(
            || {
                let mut storage = DatastoreStorage::default();
                storage
                    .insert_entity(&update_template)
                    .expect("seed update bench entity");
                Arc::new(RwLock::new(storage))
            },
            |storage| {
                let req = CommitRequest {
                    project_id: PROJECT.to_string(),
                    mode: Mode::NonTransactional as i32,
                    mutations: vec![Mutation {
                        operation: Some(Operation::Update(update_template.clone())),
                        ..Default::default()
                    }],
                    ..Default::default()
                };
                async move {
                    black_box(core::commit(&storage, req).await.expect("update commit"));
                }
            },
            BatchSize::SmallInput,
        );
    });

    let wide_update_seed = wide_indexed_entity_for_name("wide-update-target", 1);
    let wide_update_template = wide_indexed_entity_for_name("wide-update-target", 2);
    group.bench_function("update_one_field_many_indexes", |b| {
        b.to_async(&runtime).iter_batched(
            || {
                let mut storage = DatastoreStorage::default();
                storage
                    .insert_entity(&wide_update_seed)
                    .expect("seed wide update bench entity");
                Arc::new(RwLock::new(storage))
            },
            |storage| {
                let req = CommitRequest {
                    project_id: PROJECT.to_string(),
                    mode: Mode::NonTransactional as i32,
                    mutations: vec![Mutation {
                        operation: Some(Operation::Update(wide_update_template.clone())),
                        ..Default::default()
                    }],
                    ..Default::default()
                };
                async move {
                    black_box(
                        core::commit(&storage, req)
                            .await
                            .expect("wide update commit"),
                    );
                }
            },
            BatchSize::SmallInput,
        );
    });

    let agg_query = AggregationQuery {
        query_type: Some(AggregationQueryType::NestedQuery(Query {
            kind: kind(),
            filter: Some(property_filter(
                "bucket",
                property_filter::Operator::Equal,
                value(ValueType::StringValue("bucket-42".to_string())),
            )),
            ..Default::default()
        })),
        aggregations: vec![Aggregation {
            alias: "count".to_string(),
            operator: Some(AggOperator::Count(Count { up_to: None })),
        }],
    };
    group.bench_function("aggregation_count_bucket", |b| {
        let aq = agg_query.clone();
        b.to_async(&runtime).iter(|| {
            let req = RunAggregationQueryRequest {
                project_id: PROJECT.to_string(),
                query_type: Some(RunAggQueryType::AggregationQuery(aq.clone())),
                ..Default::default()
            };
            async {
                black_box(
                    core::run_aggregation_query(&storage, req)
                        .await
                        .expect("aggregation"),
                );
            }
        });
    });

    let delete_template = bench_entity_template();
    let delete_key = delete_template.key.clone().unwrap();
    group.bench_function("delete_entity_with_nested", |b| {
        b.to_async(&runtime).iter_batched(
            || {
                let mut storage = DatastoreStorage::default();
                storage
                    .insert_entity(&delete_template)
                    .expect("seed delete bench entity");
                Arc::new(RwLock::new(storage))
            },
            |storage| {
                let req = CommitRequest {
                    project_id: PROJECT.to_string(),
                    mode: Mode::NonTransactional as i32,
                    mutations: vec![Mutation {
                        operation: Some(Operation::Delete(delete_key.clone())),
                        ..Default::default()
                    }],
                    ..Default::default()
                };
                async move {
                    black_box(core::commit(&storage, req).await.expect("delete commit"));
                }
            },
            BatchSize::SmallInput,
        );
    });

    let upsert_template = bench_entity_template();
    group.bench_function("upsert_existing_entity_with_nested", |b| {
        b.to_async(&runtime).iter_batched(
            || {
                let mut storage = DatastoreStorage::default();
                storage
                    .insert_entity(&upsert_template)
                    .expect("seed upsert bench entity");
                Arc::new(RwLock::new(storage))
            },
            |storage| {
                let req = CommitRequest {
                    project_id: PROJECT.to_string(),
                    mode: Mode::NonTransactional as i32,
                    mutations: vec![Mutation {
                        operation: Some(Operation::Upsert(upsert_template.clone())),
                        ..Default::default()
                    }],
                    ..Default::default()
                };
                async move {
                    black_box(core::commit(&storage, req).await.expect("upsert commit"));
                }
            },
            BatchSize::SmallInput,
        );
    });

    let batch_insert_templates = batch_insert_entities("batch-insert");
    group.bench_function("commit_batch_10_inserts", |b| {
        b.to_async(&runtime).iter_batched(
            || Arc::new(RwLock::new(DatastoreStorage::default())),
            |storage| {
                let req = CommitRequest {
                    project_id: PROJECT.to_string(),
                    mode: Mode::NonTransactional as i32,
                    mutations: insert_mutations(&batch_insert_templates),
                    ..Default::default()
                };
                async move {
                    black_box(
                        core::commit(&storage, req)
                            .await
                            .expect("batch insert commit"),
                    );
                }
            },
            BatchSize::SmallInput,
        );
    });

    let transactional_batch_insert_templates = batch_insert_entities("transactional-batch-insert");
    group.bench_function("commit_transactional_batch_10", |b| {
        b.to_async(&runtime).iter_batched(
            || Arc::new(RwLock::new(DatastoreStorage::default())),
            |storage| {
                let mutations = insert_mutations(&transactional_batch_insert_templates);
                async move {
                    let transaction = core::begin_transaction(
                        &storage,
                        BeginTransactionRequest {
                            project_id: PROJECT.to_string(),
                            ..Default::default()
                        },
                    )
                    .await
                    .expect("begin transaction")
                    .transaction;
                    let req = CommitRequest {
                        project_id: PROJECT.to_string(),
                        mode: Mode::Transactional as i32,
                        transaction_selector: Some(TransactionSelector::Transaction(transaction)),
                        mutations,
                        ..Default::default()
                    };
                    black_box(
                        core::commit(&storage, req)
                            .await
                            .expect("transactional batch insert commit"),
                    );
                }
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("read_only_transaction_lifecycle", |b| {
        b.to_async(&runtime).iter_batched(
            || Arc::new(RwLock::new(DatastoreStorage::default())),
            |storage| async move {
                let transaction = core::begin_transaction(
                    &storage,
                    BeginTransactionRequest {
                        project_id: PROJECT.to_string(),
                        transaction_options: Some(TransactionOptions {
                            mode: Some(transaction_options::Mode::ReadOnly(
                                transaction_options::ReadOnly::default(),
                            )),
                        }),
                        ..Default::default()
                    },
                )
                .await
                .expect("begin read-only transaction")
                .transaction;

                black_box(
                    core::rollback(
                        &storage,
                        RollbackRequest {
                            project_id: PROJECT.to_string(),
                            transaction,
                            ..Default::default()
                        },
                    )
                    .await
                    .expect("rollback read-only transaction"),
                );
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

fn bench_entity_template() -> Entity {
    bench_entity_for_name("insert-bench-target")
}

fn batch_insert_entities(prefix: &str) -> Vec<Entity> {
    (0..10)
        .map(|idx| bench_entity_for_name(&format!("{prefix}-{idx}")))
        .collect()
}

fn insert_mutations(entities: &[Entity]) -> Vec<Mutation> {
    entities
        .iter()
        .cloned()
        .map(|entity| Mutation {
            operation: Some(Operation::Insert(entity)),
            ..Default::default()
        })
        .collect()
}

fn bench_entity_for_name(name: &str) -> Entity {
    Entity {
        key: Some(key_for_name(name)),
        properties: HashMap::from([
            (
                "bucket".to_string(),
                value(ValueType::StringValue("bucket-42".to_string())),
            ),
            ("score".to_string(), value(ValueType::IntegerValue(123))),
            (
                "region".to_string(),
                value(ValueType::StringValue("region-3".to_string())),
            ),
            (
                "tags".to_string(),
                value(ValueType::ArrayValue(ArrayValue {
                    values: vec![
                        value(ValueType::StringValue("tag-1".to_string())),
                        value(ValueType::StringValue("tag-2".to_string())),
                        value(ValueType::StringValue("tag-3".to_string())),
                    ],
                })),
            ),
            (
                "profile".to_string(),
                value(ValueType::EntityValue(Entity {
                    key: None,
                    properties: HashMap::from([
                        (
                            "tier".to_string(),
                            value(ValueType::StringValue("tier-2".to_string())),
                        ),
                        (
                            "country".to_string(),
                            value(ValueType::StringValue("BR".to_string())),
                        ),
                    ]),
                })),
            ),
        ]),
    }
}

fn wide_indexed_entity_for_name(name: &str, changed_value: i64) -> Entity {
    let mut properties = HashMap::with_capacity(40);
    for idx in 0..32 {
        properties.insert(
            format!("field_{idx:02}"),
            value(ValueType::StringValue(format!("value-{idx:02}"))),
        );
    }
    properties.insert(
        "changed_field".to_string(),
        value(ValueType::IntegerValue(changed_value)),
    );
    properties.insert(
        "tags".to_string(),
        value(ValueType::ArrayValue(ArrayValue {
            values: vec![
                value(ValueType::StringValue("wide-tag-1".to_string())),
                value(ValueType::StringValue("wide-tag-2".to_string())),
                value(ValueType::StringValue("wide-tag-3".to_string())),
            ],
        })),
    );

    Entity {
        key: Some(key_for_name(name)),
        properties,
    }
}

fn key_for_name(name: &str) -> Key {
    Key {
        partition_id: Some(PartitionId {
            project_id: PROJECT.to_string(),
            database_id: String::new(),
            namespace_id: String::new(),
        }),
        path: vec![PathElement {
            kind: KIND.to_string(),
            id_type: Some(IdType::Name(name.to_string())),
        }],
    }
}

async fn execute_lookup(storage: &Arc<RwLock<DatastoreStorage>>, keys: &[Key]) -> usize {
    let req = LookupRequest {
        project_id: PROJECT.to_string(),
        keys: keys.to_vec(),
        ..Default::default()
    };
    let resp = core::lookup(storage, req).await.expect("lookup");
    resp.found.len()
}

async fn execute_query(storage: &Arc<RwLock<DatastoreStorage>>, query: &Query) -> usize {
    let req = RunQueryRequest {
        project_id: PROJECT.to_string(),
        query_type: Some(QueryType::Query(query.clone())),
        ..Default::default()
    };
    let resp = core::run_query(storage, req).await.expect("run query");
    resp.batch.expect("query batch").entity_results.len()
}

fn seed_storage(entity_count: usize) -> DatastoreStorage {
    let mut storage = DatastoreStorage::default();

    for idx in 0..entity_count {
        let entity = Entity {
            key: Some(key_for(idx)),
            properties: HashMap::from([
                (
                    "bucket".to_string(),
                    value(ValueType::StringValue(format!("bucket-{}", idx % 100))),
                ),
                (
                    "score".to_string(),
                    value(ValueType::IntegerValue((idx % 10_000) as i64)),
                ),
                (
                    "region".to_string(),
                    value(ValueType::StringValue(format!("region-{}", idx % 8))),
                ),
                (
                    "tags".to_string(),
                    value(ValueType::ArrayValue(ArrayValue {
                        values: vec![
                            value(ValueType::StringValue(format!("tag-{}", idx % 16))),
                            value(ValueType::StringValue(format!("tag-{}", (idx + 1) % 16))),
                        ],
                    })),
                ),
                (
                    "profile".to_string(),
                    value(ValueType::EntityValue(Entity {
                        key: None,
                        properties: HashMap::from([(
                            "tier".to_string(),
                            value(ValueType::StringValue(format!("tier-{}", idx % 5))),
                        )]),
                    })),
                ),
            ]),
        };

        storage
            .insert_entity(&entity)
            .expect("seed benchmark entity");
    }

    storage
}

fn seed_mixed_scope_storage(entities_per_kind: usize) -> DatastoreStorage {
    let mut storage = DatastoreStorage::default();

    for project_idx in 0..MIXED_SCOPE_PROJECTS {
        let project = if project_idx == 0 {
            PROJECT.to_string()
        } else {
            format!("{PROJECT}-other-{project_idx}")
        };

        for kind_idx in 0..MIXED_SCOPE_KINDS {
            let kind = if kind_idx == 0 {
                KIND.to_string()
            } else {
                format!("{KIND}Other{kind_idx}")
            };

            for idx in 0..entities_per_kind {
                let score = ((idx * 37 + project_idx * 101 + kind_idx * 17) % 10_000) as i64;
                let entity = Entity {
                    key: Some(key_for_project_kind(&project, &kind, idx)),
                    properties: HashMap::from([
                        (
                            "bucket".to_string(),
                            value(ValueType::StringValue(format!("bucket-{}", idx % 100))),
                        ),
                        ("score".to_string(), value(ValueType::IntegerValue(score))),
                        (
                            "region".to_string(),
                            value(ValueType::StringValue(format!("region-{}", idx % 8))),
                        ),
                    ]),
                };

                storage
                    .insert_entity(&entity)
                    .expect("seed mixed-scope benchmark entity");
            }
        }
    }

    storage
}

fn seed_ancestor_storage(parent_count: usize, rows_per_parent: usize) -> DatastoreStorage {
    let mut storage = DatastoreStorage::default();

    for parent_idx in 0..parent_count {
        let parent = Entity {
            key: Some(signup_key(parent_idx)),
            properties: HashMap::from([(
                "title".to_string(),
                value(ValueType::StringValue(format!("signup-{parent_idx}"))),
            )]),
        };
        storage
            .insert_entity(&parent)
            .expect("seed ancestor parent entity");

        for row_idx in 0..rows_per_parent {
            let row = Entity {
                key: Some(signup_row_key(parent_idx, row_idx)),
                properties: HashMap::from([
                    (
                        "user_id".to_string(),
                        value(ValueType::StringValue(format!("user-{}", row_idx % 5))),
                    ),
                    (
                        "position_index".to_string(),
                        value(ValueType::IntegerValue(row_idx as i64)),
                    ),
                ]),
            };
            storage
                .insert_entity(&row)
                .expect("seed ancestor child entity");
        }
    }

    storage
}

fn key_for(idx: usize) -> Key {
    key_for_project_kind(PROJECT, KIND, idx)
}

fn key_for_project_kind(project: &str, kind: &str, idx: usize) -> Key {
    Key {
        partition_id: Some(PartitionId {
            project_id: project.to_string(),
            database_id: String::new(),
            namespace_id: String::new(),
        }),
        path: vec![PathElement {
            kind: kind.to_string(),
            id_type: Some(IdType::Name(format!("entity-{idx:06}"))),
        }],
    }
}

fn signup_key(idx: usize) -> Key {
    Key {
        partition_id: Some(PartitionId {
            project_id: PROJECT.to_string(),
            database_id: String::new(),
            namespace_id: String::new(),
        }),
        path: vec![PathElement {
            kind: SIGNUP_KIND.to_string(),
            id_type: Some(IdType::Id(idx as i64)),
        }],
    }
}

fn signup_row_key(parent_idx: usize, row_idx: usize) -> Key {
    Key {
        partition_id: Some(PartitionId {
            project_id: PROJECT.to_string(),
            database_id: String::new(),
            namespace_id: String::new(),
        }),
        path: vec![
            PathElement {
                kind: SIGNUP_KIND.to_string(),
                id_type: Some(IdType::Id(parent_idx as i64)),
            },
            PathElement {
                kind: SIGNUP_ROW_KIND.to_string(),
                id_type: Some(IdType::Name(format!("row-{row_idx:03}"))),
            },
        ],
    }
}

fn keys_only_query_for_kind(kind_name: &str) -> Query {
    Query {
        kind: kind_named(kind_name),
        projection: vec![Projection {
            property: Some(PropertyReference {
                name: "__key__".to_string(),
            }),
        }],
        ..Default::default()
    }
}

fn ancestor_query_for(parent_idx: usize) -> Query {
    Query {
        kind: kind_named(SIGNUP_ROW_KIND),
        filter: Some(property_filter(
            "__key__",
            property_filter::Operator::HasAncestor,
            value(ValueType::KeyValue(signup_key(parent_idx))),
        )),
        ..Default::default()
    }
}

fn bench_cases() -> Vec<BenchCase> {
    vec![
        BenchCase {
            name: "indexed_equality_limit_20",
            query: Query {
                kind: kind(),
                filter: Some(property_filter(
                    "bucket",
                    property_filter::Operator::Equal,
                    value(ValueType::StringValue("bucket-42".to_string())),
                )),
                limit: Some(Int32Value { value: 20 }),
                ..Default::default()
            },
        },
        BenchCase {
            name: "indexed_array_in_limit_50",
            query: Query {
                kind: kind(),
                filter: Some(property_filter(
                    "tags",
                    property_filter::Operator::In,
                    value(ValueType::ArrayValue(ArrayValue {
                        values: vec![
                            value(ValueType::StringValue("tag-3".to_string())),
                            value(ValueType::StringValue("tag-7".to_string())),
                        ],
                    })),
                )),
                limit: Some(Int32Value { value: 50 }),
                ..Default::default()
            },
        },
        BenchCase {
            name: "nested_indexed_equality",
            query: Query {
                kind: kind(),
                filter: Some(property_filter(
                    "profile.tier",
                    property_filter::Operator::Equal,
                    value(ValueType::StringValue("tier-3".to_string())),
                )),
                limit: Some(Int32Value { value: 50 }),
                ..Default::default()
            },
        },
        BenchCase {
            name: "inequality_scan_sort_limit_50",
            query: Query {
                kind: kind(),
                filter: Some(property_filter(
                    "score",
                    property_filter::Operator::GreaterThan,
                    value(ValueType::IntegerValue(5_000)),
                )),
                limit: Some(Int32Value { value: 50 }),
                ..Default::default()
            },
        },
        BenchCase {
            name: "full_kind_ordered_limit_50",
            query: Query {
                kind: kind(),
                order: vec![order_by("score")],
                limit: Some(Int32Value { value: 50 }),
                ..Default::default()
            },
        },
        BenchCase {
            name: "keys_only_offset_500",
            query: Query {
                kind: kind(),
                projection: vec![Projection {
                    property: Some(PropertyReference {
                        name: "__key__".to_string(),
                    }),
                }],
                offset: 500,
                limit: Some(Int32Value { value: 50 }),
                ..Default::default()
            },
        },
        BenchCase {
            name: "key_filter_greater_than",
            query: Query {
                kind: kind(),
                filter: Some(property_filter(
                    "__key__",
                    property_filter::Operator::GreaterThan,
                    value(ValueType::KeyValue(key_for(5000))),
                )),
                limit: Some(Int32Value { value: 50 }),
                ..Default::default()
            },
        },
        BenchCase {
            name: "distinct_on_region_bucket",
            query: Query {
                kind: kind(),
                distinct_on: vec![
                    PropertyReference {
                        name: "region".to_string(),
                    },
                    PropertyReference {
                        name: "bucket".to_string(),
                    },
                ],
                order: vec![order_by("region"), order_by("bucket")],
                ..Default::default()
            },
        },
    ]
}

fn mixed_scope_bench_cases() -> Vec<BenchCase> {
    vec![
        BenchCase {
            name: "mixed_scope_full_kind_limit_50",
            query: Query {
                kind: kind(),
                limit: Some(Int32Value { value: 50 }),
                ..Default::default()
            },
        },
        BenchCase {
            name: "mixed_scope_ordered_limit_50",
            query: Query {
                kind: kind(),
                order: vec![order_by("score")],
                limit: Some(Int32Value { value: 50 }),
                ..Default::default()
            },
        },
        BenchCase {
            name: "mixed_scope_inequality_limit_50",
            query: Query {
                kind: kind(),
                filter: Some(property_filter(
                    "score",
                    property_filter::Operator::GreaterThan,
                    value(ValueType::IntegerValue(5_000)),
                )),
                limit: Some(Int32Value { value: 50 }),
                ..Default::default()
            },
        },
    ]
}

fn kind() -> Vec<KindExpression> {
    kind_named(KIND)
}

fn kind_named(kind_name: &str) -> Vec<KindExpression> {
    vec![KindExpression {
        name: kind_name.to_string(),
    }]
}

fn order_by(property: &str) -> PropertyOrder {
    PropertyOrder {
        property: Some(PropertyReference {
            name: property.to_string(),
        }),
        direction: property_order::Direction::Ascending as i32,
    }
}

fn property_filter(name: &str, op: property_filter::Operator, filter_value: Value) -> Filter {
    Filter {
        filter_type: Some(FilterType::PropertyFilter(PropertyFilter {
            property: Some(PropertyReference {
                name: name.to_string(),
            }),
            op: op as i32,
            value: Some(filter_value),
        })),
    }
}

fn value(value_type: ValueType) -> Value {
    Value {
        value_type: Some(value_type),
        ..Default::default()
    }
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
