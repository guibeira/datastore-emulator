use datastore_emulator::google::datastore::v1::{
    aggregation_query::{self, aggregation},
    commit_request::{Mode, TransactionSelector},
    key::{PathElement, path_element::IdType},
    mutation::Operation,
    property_transform::TransformType,
    read_options::ConsistencyType,
    value::ValueType,
};
use datastore_emulator::{DatastoreStorage, core, google::datastore::v1::*};
use parking_lot::RwLock;
use std::sync::Arc;
use tonic::Code;

type Storage = Arc<RwLock<DatastoreStorage>>;

fn key(name: &str) -> Key {
    Key {
        partition_id: Some(PartitionId {
            project_id: "p".into(),
            ..Default::default()
        }),
        path: vec![PathElement {
            kind: "Item".into(),
            id_type: Some(IdType::Name(name.into())),
        }],
    }
}

fn value(value_type: ValueType) -> Value {
    Value {
        value_type: Some(value_type),
        ..Default::default()
    }
}
fn int(n: i64) -> Value {
    value(ValueType::IntegerValue(n))
}
fn array(values: Vec<Value>) -> Value {
    value(ValueType::ArrayValue(ArrayValue { values }))
}
fn entity(name: &str, props: &[(&str, Value)]) -> Entity {
    Entity {
        key: Some(key(name)),
        properties: props
            .iter()
            .map(|(k, v)| ((*k).into(), v.clone()))
            .collect(),
    }
}
fn upsert(entity: Entity) -> Mutation {
    Mutation {
        operation: Some(Operation::Upsert(entity)),
        ..Default::default()
    }
}
fn transform(property: &str, transform_type: TransformType) -> PropertyTransform {
    PropertyTransform {
        property: property.into(),
        transform_type: Some(transform_type),
    }
}
async fn commit(
    storage: &Storage,
    mutations: Vec<Mutation>,
) -> Result<CommitResponse, tonic::Status> {
    core::commit(
        storage,
        CommitRequest {
            project_id: "p".into(),
            mode: Mode::NonTransactional as i32,
            mutations,
            ..Default::default()
        },
    )
    .await
}
async fn lookup(
    storage: &Storage,
    name: &str,
    consistency_type: Option<ConsistencyType>,
) -> LookupResponse {
    core::lookup(
        storage,
        LookupRequest {
            project_id: "p".into(),
            keys: vec![key(name)],
            read_options: consistency_type.map(|consistency_type| ReadOptions {
                consistency_type: Some(consistency_type),
            }),
            ..Default::default()
        },
    )
    .await
    .unwrap()
}
fn stored(storage: &Storage, name: &str) -> Entity {
    storage
        .read()
        .get_entity(&key(name))
        .unwrap()
        .entity
        .clone()
}

#[tokio::test]
async fn masks_preserve_other_fields_delete_missing_fields_and_apply_transforms_last() {
    let storage = Storage::default();
    let nested = |a, b| {
        value(ValueType::EntityValue(entity(
            "nested",
            &[("a", int(a)), ("b", int(b))],
        )))
    };
    commit(
        &storage,
        vec![upsert(entity(
            "one",
            &[
                ("keep", int(1)),
                ("drop", int(2)),
                ("nested", nested(3, 4)),
                ("a.b", int(5)),
            ],
        ))],
    )
    .await
    .unwrap();
    let mut mutation = upsert(entity(
        "one",
        &[
            ("keep", int(99)),
            ("nested", nested(10, 99)),
            ("a.b", int(6)),
        ],
    ));
    mutation.property_mask = Some(PropertyMask {
        paths: vec!["drop".into(), "nested.a".into(), "a\\.b".into()],
    });
    mutation.property_transforms = vec![transform("nested.a", TransformType::Increment(int(2)))];
    let result = commit(&storage, vec![mutation]).await.unwrap();
    let actual = stored(&storage, "one");
    assert_eq!(actual.properties["keep"], int(1));
    assert!(!actual.properties.contains_key("drop"));
    assert_eq!(actual.properties["a.b"], int(6));
    let Some(ValueType::EntityValue(nested)) = &actual.properties["nested"].value_type else {
        panic!("nested entity")
    };
    assert_eq!(nested.properties["a"], int(12));
    assert_eq!(nested.properties["b"], int(4));
    assert_eq!(result.mutation_results[0].transform_results, vec![int(12)]);
}

#[tokio::test]
async fn numeric_transforms_saturate_preserve_types_and_propagate_nan() {
    let storage = Storage::default();
    let mut mutation = upsert(entity(
        "one",
        &[
            ("high", int(i64::MAX)),
            ("low", int(i64::MIN)),
            ("mixed", int(3)),
            ("precise", int(9_007_199_254_740_993)),
            ("nan", int(1)),
        ],
    ));
    mutation.property_transforms = vec![
        transform("high", TransformType::Increment(int(10))),
        transform("low", TransformType::Increment(int(-10))),
        transform(
            "mixed",
            TransformType::Maximum(value(ValueType::DoubleValue(3.0))),
        ),
        transform(
            "precise",
            TransformType::Minimum(value(ValueType::DoubleValue(9_007_199_254_740_992.0))),
        ),
        transform(
            "nan",
            TransformType::Maximum(value(ValueType::DoubleValue(f64::NAN))),
        ),
        transform(
            "new",
            TransformType::Increment(value(ValueType::DoubleValue(2.5))),
        ),
    ];
    commit(&storage, vec![mutation]).await.unwrap();
    let actual = stored(&storage, "one");
    assert_eq!(actual.properties["high"], int(i64::MAX));
    assert_eq!(actual.properties["low"], int(i64::MIN));
    assert_eq!(actual.properties["mixed"], int(3));
    assert_eq!(
        actual.properties["precise"],
        value(ValueType::DoubleValue(9_007_199_254_740_992.0))
    );
    assert!(
        matches!(actual.properties["nan"].value_type, Some(ValueType::DoubleValue(n)) if n.is_nan())
    );
    assert_eq!(actual.properties["new"], value(ValueType::DoubleValue(2.5)));
}

#[tokio::test]
async fn array_transforms_compare_numbers_and_nan_and_return_null() {
    let storage = Storage::default();
    let nan = value(ValueType::DoubleValue(f64::NAN));
    let mut mutation = upsert(entity(
        "one",
        &[("items", array(vec![int(1), int(1), int(2), nan.clone()]))],
    ));
    mutation.property_transforms = vec![
        transform(
            "items",
            TransformType::RemoveAllFromArray(ArrayValue {
                values: vec![value(ValueType::DoubleValue(1.0)), nan],
            }),
        ),
        transform(
            "items",
            TransformType::AppendMissingElements(ArrayValue {
                values: vec![value(ValueType::DoubleValue(2.0)), int(3), int(3)],
            }),
        ),
    ];
    let response = commit(&storage, vec![mutation]).await.unwrap();
    assert_eq!(
        stored(&storage, "one").properties["items"],
        array(vec![int(2), int(3)])
    );
    assert_eq!(
        response.mutation_results[0].transform_results,
        vec![value(ValueType::NullValue(0)); 2]
    );
}

#[tokio::test]
async fn invalid_later_transform_keeps_transaction_atomic() {
    let storage = Storage::default();
    let mut invalid = upsert(entity("bad", &[]));
    invalid.property_transforms = vec![transform(
        "x",
        TransformType::Increment(value(ValueType::StringValue("bad".into()))),
    )];
    let response = core::commit(
        &storage,
        CommitRequest {
            project_id: "p".into(),
            mode: Mode::Transactional as i32,
            transaction_selector: Some(TransactionSelector::SingleUseTransaction(
                TransactionOptions::default(),
            )),
            mutations: vec![upsert(entity("good", &[("x", int(1))])), invalid],
            ..Default::default()
        },
    )
    .await
    .unwrap_err();
    assert_eq!(response.code(), Code::InvalidArgument);
    assert!(storage.read().entities.is_empty());
}

#[tokio::test]
async fn repeated_transactional_mutations_use_staged_values_and_one_request_time() {
    let storage = Storage::default();
    let mut first = upsert(entity("one", &[("x", int(1))]));
    first.property_transforms = vec![transform("time", TransformType::SetToServerValue(1))];
    let mut second = upsert(entity("one", &[]));
    second.property_mask = Some(PropertyMask::default());
    second.property_transforms = vec![
        transform("x", TransformType::Increment(int(2))),
        transform("time2", TransformType::SetToServerValue(1)),
    ];
    core::commit(
        &storage,
        CommitRequest {
            project_id: "p".into(),
            mode: 1,
            transaction_selector: Some(TransactionSelector::SingleUseTransaction(
                TransactionOptions::default(),
            )),
            mutations: vec![first, second],
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let actual = stored(&storage, "one");
    assert_eq!(actual.properties["x"], int(3));
    assert_eq!(actual.properties["time"], actual.properties["time2"]);
    assert!(
        matches!(&actual.properties["time"].value_type, Some(ValueType::TimestampValue(time)) if time.nanos % 1_000_000 == 0)
    );
}

#[tokio::test]
async fn historical_lookup_survives_updates_deletes_and_recreation() {
    let storage = Storage::default();
    let initial = commit(&storage, vec![upsert(entity("one", &[("x", int(1))]))])
        .await
        .unwrap()
        .commit_time
        .unwrap();
    commit(&storage, vec![upsert(entity("one", &[("x", int(2))]))])
        .await
        .unwrap();
    let deleted = commit(
        &storage,
        vec![Mutation {
            operation: Some(Operation::Delete(key("one"))),
            ..Default::default()
        }],
    )
    .await
    .unwrap()
    .commit_time
    .unwrap();
    commit(&storage, vec![upsert(entity("one", &[("x", int(3))]))])
        .await
        .unwrap();
    let past = lookup(&storage, "one", Some(ConsistencyType::ReadTime(initial))).await;
    assert_eq!(
        past.found[0].entity.as_ref().unwrap().properties["x"],
        int(1)
    );
    assert_eq!(past.read_time, Some(initial));
    assert_eq!(
        lookup(&storage, "one", Some(ConsistencyType::ReadTime(deleted)))
            .await
            .missing
            .len(),
        1
    );
    assert_eq!(
        lookup(&storage, "one", None).await.found[0]
            .entity
            .as_ref()
            .unwrap()
            .properties["x"],
        int(3)
    );
}

fn query() -> Query {
    Query {
        kind: vec![KindExpression {
            name: "Item".into(),
        }],
        ..Default::default()
    }
}
async fn count(storage: &Storage, options: ReadOptions) -> RunAggregationQueryResponse {
    core::run_aggregation_query(
        storage,
        RunAggregationQueryRequest {
            project_id: "p".into(),
            read_options: Some(options),
            query_type: Some(run_aggregation_query_request::QueryType::AggregationQuery(
                AggregationQuery {
                    query_type: Some(aggregation_query::QueryType::NestedQuery(query())),
                    aggregations: vec![aggregation_query::Aggregation {
                        alias: "total".into(),
                        operator: Some(aggregation::Operator::Count(aggregation::Count::default())),
                    }],
                },
            )),
            ..Default::default()
        },
    )
    .await
    .unwrap()
}
fn total(response: &RunAggregationQueryResponse) -> Value {
    response.batch.as_ref().unwrap().aggregation_results[0].aggregate_properties["total"].clone()
}

#[tokio::test]
async fn query_and_aggregation_honor_read_time_and_query_transactions_detect_inserts() {
    let storage = Storage::default();
    let time = commit(&storage, vec![upsert(entity("one", &[("x", int(1))]))])
        .await
        .unwrap()
        .commit_time
        .unwrap();
    commit(
        &storage,
        vec![
            upsert(entity("one", &[("x", int(2))])),
            upsert(entity("two", &[])),
        ],
    )
    .await
    .unwrap();
    let options = ReadOptions {
        consistency_type: Some(ConsistencyType::ReadTime(time)),
    };
    let past = core::run_query(
        &storage,
        RunQueryRequest {
            project_id: "p".into(),
            read_options: Some(options.clone()),
            query_type: Some(run_query_request::QueryType::Query(query())),
            ..Default::default()
        },
    )
    .await
    .unwrap()
    .batch
    .unwrap();
    assert_eq!(past.entity_results.len(), 1);
    assert_eq!(
        past.entity_results[0].entity.as_ref().unwrap().properties["x"],
        int(1)
    );
    assert_eq!(total(&count(&storage, options).await), int(1));
    let transaction = core::run_query(
        &storage,
        RunQueryRequest {
            project_id: "p".into(),
            read_options: Some(ReadOptions {
                consistency_type: Some(ConsistencyType::NewTransaction(
                    TransactionOptions::default(),
                )),
            }),
            query_type: Some(run_query_request::QueryType::Query(query())),
            ..Default::default()
        },
    )
    .await
    .unwrap()
    .transaction;
    assert!(!transaction.is_empty());
    commit(&storage, vec![upsert(entity("three", &[]))])
        .await
        .unwrap();
    assert_eq!(
        total(
            &count(
                &storage,
                ReadOptions {
                    consistency_type: Some(ConsistencyType::Transaction(transaction.clone()))
                }
            )
            .await
        ),
        int(2)
    );
    let error = core::commit(
        &storage,
        CommitRequest {
            project_id: "p".into(),
            mode: 1,
            transaction_selector: Some(TransactionSelector::Transaction(transaction)),
            mutations: vec![upsert(entity("four", &[]))],
            ..Default::default()
        },
    )
    .await
    .unwrap_err();
    assert_eq!(error.code(), Code::Aborted);
    assert!(storage.read().get_entity(&key("four")).is_none());
}

#[tokio::test]
async fn lazy_lookup_transaction_shares_its_snapshot_with_queries_and_aggregations() {
    let storage = Storage::default();
    commit(&storage, vec![upsert(entity("one", &[("x", int(1))]))])
        .await
        .unwrap();
    let response = lookup(
        &storage,
        "one",
        Some(ConsistencyType::NewTransaction(TransactionOptions {
            mode: Some(transaction_options::Mode::ReadOnly(
                transaction_options::ReadOnly::default(),
            )),
        })),
    )
    .await;
    assert!(!response.transaction.is_empty());
    commit(&storage, vec![upsert(entity("two", &[]))])
        .await
        .unwrap();
    let options = ReadOptions {
        consistency_type: Some(ConsistencyType::Transaction(response.transaction.clone())),
    };
    let response = core::run_query(
        &storage,
        RunQueryRequest {
            project_id: "p".into(),
            read_options: Some(options.clone()),
            query_type: Some(run_query_request::QueryType::Query(query())),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(response.batch.unwrap().entity_results.len(), 1);
    assert_eq!(total(&count(&storage, options).await), int(1));
}

#[tokio::test]
async fn empty_aggregation_snapshot_does_not_see_later_inserts() {
    let storage = Storage::default();
    let first = count(
        &storage,
        ReadOptions {
            consistency_type: Some(ConsistencyType::NewTransaction(
                TransactionOptions::default(),
            )),
        },
    )
    .await;
    assert_eq!(total(&first), int(0));
    commit(&storage, vec![upsert(entity("one", &[]))])
        .await
        .unwrap();
    assert_eq!(
        total(
            &count(
                &storage,
                ReadOptions {
                    consistency_type: Some(ConsistencyType::Transaction(first.transaction))
                }
            )
            .await
        ),
        int(0)
    );
}

#[tokio::test]
async fn invalid_read_times_and_cross_database_transactions_are_rejected() {
    let storage = Storage::default();
    for time in [
        pbjson_types::Timestamp {
            seconds: i64::MAX,
            nanos: 0,
        },
        pbjson_types::Timestamp {
            seconds: 1,
            nanos: 0,
        },
        pbjson_types::Timestamp {
            seconds: 1,
            nanos: 1,
        },
    ] {
        let error = core::lookup(
            &storage,
            LookupRequest {
                project_id: "p".into(),
                keys: vec![key("one")],
                read_options: Some(ReadOptions {
                    consistency_type: Some(ConsistencyType::ReadTime(time)),
                }),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
        assert_eq!(error.code(), Code::InvalidArgument);
    }
    let response = lookup(
        &storage,
        "one",
        Some(ConsistencyType::NewTransaction(
            TransactionOptions::default(),
        )),
    )
    .await;
    let error = core::run_query(
        &storage,
        RunQueryRequest {
            project_id: "other".into(),
            read_options: Some(ReadOptions {
                consistency_type: Some(ConsistencyType::Transaction(response.transaction)),
            }),
            query_type: Some(run_query_request::QueryType::Query(query())),
            ..Default::default()
        },
    )
    .await
    .unwrap_err();
    assert_eq!(error.code(), Code::InvalidArgument);
}

#[tokio::test]
async fn sum_and_average_reject_the_key_property() {
    let storage = Storage::default();
    for operator in [
        aggregation::Operator::Sum(aggregation::Sum {
            property: Some(PropertyReference {
                name: "__key__".into(),
            }),
        }),
        aggregation::Operator::Avg(aggregation::Avg {
            property: Some(PropertyReference {
                name: "__key__".into(),
            }),
        }),
    ] {
        let error = core::run_aggregation_query(
            &storage,
            RunAggregationQueryRequest {
                project_id: "p".into(),
                query_type: Some(run_aggregation_query_request::QueryType::AggregationQuery(
                    AggregationQuery {
                        query_type: Some(aggregation_query::QueryType::NestedQuery(query())),
                        aggregations: vec![aggregation_query::Aggregation {
                            alias: "invalid".into(),
                            operator: Some(operator),
                        }],
                    },
                )),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
        assert_eq!(error.code(), Code::InvalidArgument);
    }
}
