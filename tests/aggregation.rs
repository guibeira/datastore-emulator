//! Aggregation query semantics: nested limit/offset/order, SUM/AVG typing,
//! unindexed and array values, default and duplicate aliases, kindless queries.

use datastore_emulator::{
    DatastoreStorage, core,
    google::datastore::v1::{
        AggregationQuery, ArrayValue, Entity, Filter, Key, KindExpression, PartitionId,
        PropertyFilter, PropertyOrder, PropertyReference, Query, RunAggregationQueryRequest, Value,
        aggregation_query::{
            Aggregation, QueryType as AggregationQueryType,
            aggregation::{Avg, Count, Operator as AggOperator, Sum},
        },
        filter::FilterType,
        key::{PathElement, path_element::IdType},
        property_filter, property_order,
        run_aggregation_query_request::QueryType as RunAggQueryType,
        value::ValueType,
    },
};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use tonic::Code;

const PROJECT: &str = "p1";

fn key(kind: &str, name: &str, namespace: &str) -> Key {
    Key {
        partition_id: Some(PartitionId {
            project_id: PROJECT.to_string(),
            namespace_id: namespace.to_string(),
            database_id: String::new(),
        }),
        path: vec![PathElement {
            kind: kind.to_string(),
            id_type: Some(IdType::Name(name.to_string())),
        }],
    }
}

fn val(value_type: ValueType) -> Value {
    Value {
        value_type: Some(value_type),
        ..Default::default()
    }
}

fn unindexed(value_type: ValueType) -> Value {
    Value {
        exclude_from_indexes: true,
        ..val(value_type)
    }
}

fn insert(
    storage: &mut DatastoreStorage,
    kind: &str,
    name: &str,
    namespace: &str,
    properties: Vec<(&str, Value)>,
) {
    let entity = Entity {
        key: Some(key(kind, name, namespace)),
        properties: properties
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect(),
    };
    storage.insert_entity(&entity).expect("insert");
}

fn storage() -> Arc<RwLock<DatastoreStorage>> {
    let mut storage = DatastoreStorage::default();
    for n in 0..5 {
        insert(
            &mut storage,
            "Item",
            &format!("e{n}"),
            "",
            vec![("n", val(ValueType::IntegerValue(n)))],
        );
    }
    Arc::new(RwLock::new(storage))
}

fn hours_storage(entities: Vec<(&str, Value)>) -> Arc<RwLock<DatastoreStorage>> {
    let mut storage = DatastoreStorage::default();
    for (name, hours) in entities {
        insert(&mut storage, "Work", name, "", vec![("hours", hours)]);
    }
    Arc::new(RwLock::new(storage))
}

fn kind_query(kind: &str) -> Query {
    Query {
        kind: vec![KindExpression {
            name: kind.to_string(),
        }],
        ..Default::default()
    }
}

fn order_asc(prop: &str) -> Vec<PropertyOrder> {
    vec![PropertyOrder {
        property: Some(PropertyReference {
            name: prop.to_string(),
        }),
        direction: property_order::Direction::Ascending as i32,
    }]
}

fn count_agg(alias: &str) -> Aggregation {
    Aggregation {
        operator: Some(AggOperator::Count(Count { up_to: None })),
        alias: alias.to_string(),
    }
}

fn sum_agg(alias: &str, prop: &str) -> Aggregation {
    Aggregation {
        operator: Some(AggOperator::Sum(Sum {
            property: Some(PropertyReference {
                name: prop.to_string(),
            }),
        })),
        alias: alias.to_string(),
    }
}

fn avg_agg(alias: &str, prop: &str) -> Aggregation {
    Aggregation {
        operator: Some(AggOperator::Avg(Avg {
            property: Some(PropertyReference {
                name: prop.to_string(),
            }),
        })),
        alias: alias.to_string(),
    }
}

fn prop_filter(prop: &str, op: property_filter::Operator, value: Value) -> Filter {
    Filter {
        filter_type: Some(FilterType::PropertyFilter(PropertyFilter {
            property: Some(PropertyReference {
                name: prop.to_string(),
            }),
            op: op as i32,
            value: Some(value),
        })),
    }
}

async fn run(
    storage: &Arc<RwLock<DatastoreStorage>>,
    namespace: &str,
    query: Query,
    aggregations: Vec<Aggregation>,
) -> Result<HashMap<String, Value>, tonic::Status> {
    let partition_id = Some(PartitionId {
        project_id: PROJECT.to_string(),
        namespace_id: namespace.to_string(),
        database_id: String::new(),
    });
    let req = RunAggregationQueryRequest {
        project_id: PROJECT.to_string(),
        partition_id,
        query_type: Some(RunAggQueryType::AggregationQuery(AggregationQuery {
            query_type: Some(AggregationQueryType::NestedQuery(query)),
            aggregations,
        })),
        ..Default::default()
    };
    let response = core::run_aggregation_query(storage, req).await?;
    let batch = response.batch.expect("batch");
    Ok(batch.aggregation_results[0].aggregate_properties.clone())
}

fn value_of(props: &HashMap<String, Value>, alias: &str) -> Option<ValueType> {
    props.get(alias).and_then(|v| v.value_type.clone())
}

#[tokio::test]
async fn count_honors_nested_limit_and_offset() {
    let storage = storage();

    let mut limited = kind_query("Item");
    limited.order = order_asc("n");
    limited.limit = Some(pbjson_types::Int32Value { value: 3 });
    let props = run(&storage, "", limited, vec![count_agg("c")])
        .await
        .expect("limit");
    assert_eq!(value_of(&props, "c"), Some(ValueType::IntegerValue(3)));

    let mut offset = kind_query("Item");
    offset.order = order_asc("n");
    offset.offset = 2;
    let props = run(&storage, "", offset, vec![count_agg("c")])
        .await
        .expect("offset");
    assert_eq!(value_of(&props, "c"), Some(ValueType::IntegerValue(3)));

    let mut both = kind_query("Item");
    both.order = order_asc("n");
    both.offset = 2;
    both.limit = Some(pbjson_types::Int32Value { value: 2 });
    let props = run(&storage, "", both, vec![count_agg("c")])
        .await
        .expect("offset+limit");
    assert_eq!(value_of(&props, "c"), Some(ValueType::IntegerValue(2)));

    // Ordered asc with limit 2 selects n = 0 and 1.
    let mut ordered = kind_query("Item");
    ordered.order = order_asc("n");
    ordered.limit = Some(pbjson_types::Int32Value { value: 2 });
    let props = run(&storage, "", ordered, vec![sum_agg("s", "n")])
        .await
        .expect("sum");
    assert_eq!(value_of(&props, "s"), Some(ValueType::IntegerValue(1)));
}

#[tokio::test]
async fn count_defaults_projects_in_nested_key_filters() {
    let storage = storage();
    let mut target = key("Item", "e2", "");
    target.partition_id = None;
    let mut query = kind_query("Item");
    query.filter = Some(prop_filter(
        "__key__",
        property_filter::Operator::Equal,
        val(ValueType::KeyValue(target)),
    ));
    let props = run(&storage, "", query, vec![count_agg("c")])
        .await
        .unwrap();
    assert_eq!(value_of(&props, "c"), Some(ValueType::IntegerValue(1)));
}

#[tokio::test]
async fn sum_of_integers_is_integer_and_avg_is_double() {
    let storage = hours_storage(vec![
        ("a", val(ValueType::IntegerValue(5))),
        ("b", val(ValueType::IntegerValue(3))),
        ("c", val(ValueType::IntegerValue(1))),
    ]);
    let props = run(
        &storage,
        "",
        kind_query("Work"),
        vec![sum_agg("s", "hours"), avg_agg("a", "hours")],
    )
    .await
    .expect("aggregate");
    assert_eq!(value_of(&props, "s"), Some(ValueType::IntegerValue(9)));
    assert_eq!(value_of(&props, "a"), Some(ValueType::DoubleValue(3.0)));
}

#[tokio::test]
async fn sum_with_a_double_is_double() {
    let storage = hours_storage(vec![
        ("a", val(ValueType::IntegerValue(5))),
        ("b", val(ValueType::DoubleValue(2.5))),
    ]);
    let props = run(
        &storage,
        "",
        kind_query("Work"),
        vec![sum_agg("s", "hours"), avg_agg("a", "hours")],
    )
    .await
    .expect("aggregate");
    assert_eq!(value_of(&props, "s"), Some(ValueType::DoubleValue(7.5)));
    assert_eq!(value_of(&props, "a"), Some(ValueType::DoubleValue(3.75)));
}

#[tokio::test]
async fn empty_result_set_values() {
    let storage = storage();
    let mut q = kind_query("Item");
    q.filter = Some(prop_filter(
        "n",
        property_filter::Operator::GreaterThan,
        val(ValueType::IntegerValue(100)),
    ));
    let props = run(
        &storage,
        "",
        q,
        vec![count_agg("c"), sum_agg("s", "n"), avg_agg("a", "n")],
    )
    .await
    .expect("aggregate");
    assert_eq!(value_of(&props, "c"), Some(ValueType::IntegerValue(0)));
    assert_eq!(value_of(&props, "s"), Some(ValueType::IntegerValue(0)));
    assert!(matches!(
        value_of(&props, "a"),
        Some(ValueType::NullValue(_))
    ));
}

#[tokio::test]
async fn sum_expands_arrays_and_skips_non_numeric_and_unindexed() {
    let array = |values: Vec<Value>| val(ValueType::ArrayValue(ArrayValue { values }));
    let mut storage = DatastoreStorage::default();
    insert(
        &mut storage,
        "Work",
        "arr",
        "",
        vec![(
            "hours",
            array(vec![
                val(ValueType::IntegerValue(1)),
                val(ValueType::IntegerValue(2)),
            ]),
        )],
    );
    insert(
        &mut storage,
        "Work",
        "three",
        "",
        vec![("hours", val(ValueType::IntegerValue(3)))],
    );
    insert(
        &mut storage,
        "Work",
        "text",
        "",
        vec![("hours", val(ValueType::StringValue("ten".to_string())))],
    );
    insert(
        &mut storage,
        "Work",
        "unindexed",
        "",
        vec![("hours", unindexed(ValueType::IntegerValue(4)))],
    );
    insert(
        &mut storage,
        "Work",
        "none",
        "",
        vec![("other", val(ValueType::IntegerValue(9)))],
    );
    let storage = Arc::new(RwLock::new(storage));

    let props = run(
        &storage,
        "",
        kind_query("Work"),
        vec![sum_agg("s", "hours"), avg_agg("a", "hours"), count_agg("c")],
    )
    .await
    .expect("aggregate");
    assert_eq!(value_of(&props, "s"), Some(ValueType::IntegerValue(6)));
    assert_eq!(value_of(&props, "a"), Some(ValueType::DoubleValue(2.0)));
    assert_eq!(value_of(&props, "c"), Some(ValueType::IntegerValue(5)));
}

#[tokio::test]
async fn empty_alias_defaults_to_property_n() {
    let storage = storage();
    let props = run(
        &storage,
        "",
        kind_query("Item"),
        vec![count_agg(""), sum_agg("", "n"), avg_agg("", "n")],
    )
    .await
    .expect("aggregate");
    let mut keys: Vec<&String> = props.keys().collect();
    keys.sort();
    assert_eq!(keys, vec!["property_1", "property_2", "property_3"]);
}

#[tokio::test]
async fn duplicate_alias_is_rejected() {
    let storage = storage();
    let err = run(
        &storage,
        "",
        kind_query("Item"),
        vec![count_agg("x"), sum_agg("x", "n")],
    )
    .await
    .expect_err("duplicate alias must fail");
    assert_eq!(err.code(), Code::InvalidArgument);
    assert_eq!(
        err.message(),
        "Aggregation aliases contain duplicate alias: x."
    );
}

#[tokio::test]
async fn kindless_count_matches_every_kind_in_namespace() {
    let mut storage = DatastoreStorage::default();
    insert(&mut storage, "A", "a1", "ns1", vec![]);
    insert(&mut storage, "A", "a2", "ns1", vec![]);
    insert(&mut storage, "B", "b1", "ns1", vec![]);
    insert(&mut storage, "A", "a0", "", vec![]);
    let storage = Arc::new(RwLock::new(storage));

    let props = run(&storage, "ns1", Query::default(), vec![count_agg("c")])
        .await
        .expect("aggregate");
    assert_eq!(value_of(&props, "c"), Some(ValueType::IntegerValue(3)));
}
