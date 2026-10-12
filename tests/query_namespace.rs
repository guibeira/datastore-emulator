//! Regression tests for namespace scoping of queries.
//!
//! Before the fix, `RunQueryRequest.partition_id.namespace_id` and the same field
//! on `RunAggregationQueryRequest` were ignored, so queries returned entities
//! from every namespace of the project.

use datastore_emulator::{
    DatastoreStorage, core,
    google::datastore::v1::{
        AggregationQuery, Entity, Filter, Key, KindExpression, PartitionId, PropertyFilter,
        PropertyOrder, PropertyReference, Query, QueryResultBatch, RunAggregationQueryRequest,
        RunAggregationQueryResponse, RunQueryRequest, Value,
        aggregation_query::{
            Aggregation, QueryType as AggregationQueryType,
            aggregation::{Count, Operator as AggOperator},
        },
        filter::FilterType,
        key::{PathElement, path_element::IdType},
        property_filter, property_order,
        run_aggregation_query_request::QueryType as RunAggQueryType,
        run_query_request::QueryType,
        value::ValueType,
    },
};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

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

fn insert(
    storage: &mut DatastoreStorage,
    kind: &str,
    name: &str,
    namespace: &str,
    tag: Option<&str>,
    p: i64,
) {
    let mut properties = HashMap::from([("p".to_string(), val(ValueType::IntegerValue(p)))]);
    if let Some(tag) = tag {
        properties.insert(
            "tag".to_string(),
            val(ValueType::StringValue(tag.to_string())),
        );
    }
    let entity = Entity {
        key: Some(key(kind, name, namespace)),
        properties,
    };
    storage.insert_entity(&entity).expect("insert");
}

/// Kind `Task`: `a` in "", `b` and `b2` in "ns1", `c` in "ns2".
/// Kinds `OnlyDefault` in "" and `OnlyNs1` in "ns1".
fn storage() -> Arc<RwLock<DatastoreStorage>> {
    let mut storage = DatastoreStorage::default();
    insert(&mut storage, "Task", "a", "", Some("x"), 1);
    insert(&mut storage, "Task", "b", "ns1", Some("x"), 2);
    insert(&mut storage, "Task", "b2", "ns1", Some("y"), 3);
    insert(&mut storage, "Task", "c", "ns2", Some("x"), 1);
    insert(&mut storage, "OnlyDefault", "d", "", None, 0);
    insert(&mut storage, "OnlyNs1", "e", "ns1", None, 0);
    Arc::new(RwLock::new(storage))
}

fn query(kind: &str) -> Query {
    Query {
        kind: vec![KindExpression {
            name: kind.to_string(),
        }],
        ..Default::default()
    }
}

fn partition(namespace: &str) -> Option<PartitionId> {
    Some(PartitionId {
        project_id: PROJECT.to_string(),
        namespace_id: namespace.to_string(),
        database_id: String::new(),
    })
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
    namespace: Option<&str>,
    query: Query,
) -> QueryResultBatch {
    let req = RunQueryRequest {
        project_id: PROJECT.to_string(),
        partition_id: namespace.and_then(partition),
        query_type: Some(QueryType::Query(query)),
        ..Default::default()
    };
    core::run_query(storage, req)
        .await
        .expect("run_query")
        .batch
        .expect("batch")
}

async fn names(
    storage: &Arc<RwLock<DatastoreStorage>>,
    namespace: Option<&str>,
    query: Query,
) -> Vec<String> {
    let batch = run(storage, namespace, query).await;
    let mut names: Vec<String> = batch
        .entity_results
        .iter()
        .map(|r| {
            match r
                .entity
                .as_ref()
                .and_then(|e| e.key.as_ref()?.path.last()?.id_type.as_ref())
            {
                Some(IdType::Name(n)) => n.clone(),
                Some(IdType::Id(id)) => id.to_string(),
                None => String::new(),
            }
        })
        .collect();
    names.sort();
    names
}

async fn count(storage: &Arc<RwLock<DatastoreStorage>>, namespace: &str) -> i64 {
    let aggregation_query = AggregationQuery {
        query_type: Some(AggregationQueryType::NestedQuery(query("Task"))),
        aggregations: vec![Aggregation {
            operator: Some(AggOperator::Count(Count { up_to: None })),
            alias: "c".to_string(),
        }],
    };
    let req = RunAggregationQueryRequest {
        project_id: PROJECT.to_string(),
        partition_id: partition(namespace),
        query_type: Some(RunAggQueryType::AggregationQuery(aggregation_query)),
        ..Default::default()
    };
    let response: RunAggregationQueryResponse = core::run_aggregation_query(storage, req)
        .await
        .expect("run_aggregation_query");
    let batch = response.batch.expect("batch");
    match batch.aggregation_results[0].aggregate_properties["c"]
        .value_type
        .as_ref()
    {
        Some(ValueType::IntegerValue(n)) => *n,
        other => panic!("unexpected count value: {other:?}"),
    }
}

#[tokio::test]
async fn query_without_filter_is_scoped_to_namespace() {
    let storage = storage();
    assert_eq!(
        names(&storage, Some("ns1"), query("Task")).await,
        vec!["b", "b2"]
    );
    assert_eq!(names(&storage, Some(""), query("Task")).await, vec!["a"]);
    assert_eq!(names(&storage, None, query("Task")).await, vec!["a"]);
}

#[tokio::test]
async fn equality_filter_is_scoped_to_namespace() {
    let storage = storage();

    let mut by_tag = query("Task");
    by_tag.filter = Some(prop_filter(
        "tag",
        property_filter::Operator::Equal,
        val(ValueType::StringValue("x".to_string())),
    ));
    assert_eq!(names(&storage, Some("ns1"), by_tag).await, vec!["b"]);

    let mut by_p = query("Task");
    by_p.filter = Some(prop_filter(
        "p",
        property_filter::Operator::Equal,
        val(ValueType::IntegerValue(1)),
    ));
    assert!(names(&storage, Some("ns1"), by_p).await.is_empty());
}

#[tokio::test]
async fn order_and_limit_are_scoped_to_namespace() {
    let storage = storage();
    let mut q = query("Task");
    q.order = vec![PropertyOrder {
        property: Some(PropertyReference {
            name: "p".to_string(),
        }),
        direction: property_order::Direction::Ascending as i32,
    }];
    q.limit = Some(pbjson_types::Int32Value { value: 1 });
    let batch = run(&storage, Some("ns1"), q).await;
    let got: Vec<String> = batch
        .entity_results
        .iter()
        .filter_map(|r| {
            match r
                .entity
                .as_ref()?
                .key
                .as_ref()?
                .path
                .last()?
                .id_type
                .as_ref()?
            {
                IdType::Name(n) => Some(n.clone()),
                IdType::Id(id) => Some(id.to_string()),
            }
        })
        .collect();
    assert_eq!(got, vec!["b"]);
}

#[tokio::test]
async fn kind_metadata_is_scoped_to_namespace() {
    let storage = storage();
    assert_eq!(
        names(&storage, Some("ns1"), query("__kind__")).await,
        vec!["OnlyNs1", "Task"]
    );
    assert_eq!(
        names(&storage, Some(""), query("__kind__")).await,
        vec!["OnlyDefault", "Task"]
    );
}

#[tokio::test]
async fn aggregation_count_is_scoped_to_namespace() {
    let storage = storage();
    assert_eq!(count(&storage, "ns1").await, 2);
    assert_eq!(count(&storage, "ns2").await, 1);
    assert_eq!(count(&storage, "").await, 1);
}
