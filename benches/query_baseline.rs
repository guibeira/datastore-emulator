use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use datastore_emulator::{
    DatastoreStorage, core,
    google::datastore::v1::{
        ArrayValue, Entity, Filter, Key, KindExpression, LookupRequest, PartitionId, Projection,
        PropertyFilter, PropertyOrder, PropertyReference, Query, RunQueryRequest, Value,
        filter::FilterType,
        key::{PathElement, path_element::IdType},
        property_filter, property_order,
        run_query_request::QueryType,
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
const DEFAULT_ENTITY_COUNT: usize = 10_000;

struct BenchCase {
    name: &'static str,
    query: Query,
}

fn criterion_benchmark(c: &mut Criterion) {
    let entity_count = env_usize("DATASTORE_QUERY_BENCH_ENTITIES", DEFAULT_ENTITY_COUNT);
    let runtime = Runtime::new().expect("create tokio runtime");
    let storage = Arc::new(RwLock::new(seed_storage(entity_count)));

    runtime.block_on(async {
        for case in bench_cases() {
            black_box(execute_query(&storage, &case.query).await);
        }
    });

    let mut group = c.benchmark_group("query_baseline");
    group.sample_size(env_usize("DATASTORE_QUERY_BENCH_SAMPLES", 50));
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

    group.finish();
}

fn bench_entity_template() -> Entity {
    Entity {
        key: Some(Key {
            partition_id: Some(PartitionId {
                project_id: PROJECT.to_string(),
                database_id: String::new(),
                namespace_id: String::new(),
            }),
            path: vec![PathElement {
                kind: KIND.to_string(),
                id_type: Some(IdType::Name("insert-bench-target".to_string())),
            }],
        }),
        properties: HashMap::from([
            (
                "bucket".to_string(),
                value(ValueType::StringValue("bucket-42".to_string())),
            ),
            (
                "score".to_string(),
                value(ValueType::IntegerValue(123)),
            ),
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

fn key_for(idx: usize) -> Key {
    Key {
        partition_id: Some(PartitionId {
            project_id: PROJECT.to_string(),
            database_id: String::new(),
            namespace_id: String::new(),
        }),
        path: vec![PathElement {
            kind: KIND.to_string(),
            id_type: Some(IdType::Name(format!("entity-{idx:06}"))),
        }],
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

fn kind() -> Vec<KindExpression> {
    vec![KindExpression {
        name: KIND.to_string(),
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
