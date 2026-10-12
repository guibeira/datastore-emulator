//! Kindless queries and the HAS_ANCESTOR self-match.
//!
//! Before the fix, `run_query` rejected queries without a kind, and the
//! ancestor filter dropped the ancestor entity itself.

use datastore_emulator::{
    DatastoreStorage, core,
    google::datastore::v1::{
        AggregationQuery, CompositeFilter, Entity, Filter, Key, PartitionId, Projection,
        PropertyFilter, PropertyOrder, PropertyReference, Query, QueryResultBatch,
        RunAggregationQueryRequest, RunQueryRequest, Value,
        aggregation_query::Aggregation,
        aggregation_query::QueryType as AggQueryType,
        aggregation_query::aggregation::{Count, Operator as AggOperator},
        composite_filter,
        filter::FilterType,
        key::{PathElement, path_element::IdType},
        property_filter, property_order,
        query_result_batch::MoreResultsType,
        run_aggregation_query_request::QueryType as RunAggQueryType,
        run_query_request::QueryType,
        value::ValueType,
    },
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::Code;

const PROJECT: &str = "p1";

fn partition(namespace: &str) -> Option<PartitionId> {
    Some(PartitionId {
        project_id: PROJECT.to_string(),
        namespace_id: namespace.to_string(),
        database_id: String::new(),
    })
}

fn key(path: &[(&str, &str)], namespace: &str) -> Key {
    Key {
        partition_id: partition(namespace),
        path: path
            .iter()
            .map(|(kind, name)| PathElement {
                kind: kind.to_string(),
                id_type: Some(IdType::Name(name.to_string())),
            })
            .collect(),
    }
}

fn val(value_type: ValueType) -> Value {
    Value {
        value_type: Some(value_type),
        ..Default::default()
    }
}

fn insert(storage: &mut DatastoreStorage, path: &[(&str, &str)], namespace: &str) {
    let entity = Entity {
        key: Some(key(path, namespace)),
        properties: HashMap::new(),
    };
    storage.insert_entity(&entity).expect("insert");
}

/// Namespace `ns1`: `KParent/p` with children `KindA/a1` (which has child
/// `KindC/c1`), `KindA/a2`, `KindB/b1`, `KindB/b2`, plus unparented `KindA/zz`.
/// Default namespace: `KindA/other`.
fn storage() -> Arc<RwLock<DatastoreStorage>> {
    let mut storage = DatastoreStorage::default();
    let p: &[(&str, &str)] = &[("KParent", "p")];
    insert(&mut storage, p, "ns1");
    insert(&mut storage, &[("KParent", "p"), ("KindA", "a1")], "ns1");
    insert(
        &mut storage,
        &[("KParent", "p"), ("KindA", "a1"), ("KindC", "c1")],
        "ns1",
    );
    insert(&mut storage, &[("KParent", "p"), ("KindA", "a2")], "ns1");
    insert(&mut storage, &[("KParent", "p"), ("KindB", "b1")], "ns1");
    insert(&mut storage, &[("KParent", "p"), ("KindB", "b2")], "ns1");
    insert(&mut storage, &[("KindA", "zz")], "ns1");
    insert(&mut storage, &[("KindA", "other")], "");
    Arc::new(RwLock::new(storage))
}

fn pair(kind: &str, name: &str) -> (String, String) {
    (kind.to_string(), name.to_string())
}

fn expected_ancestor_result() -> Vec<(String, String)> {
    vec![
        pair("KParent", "p"),
        pair("KindA", "a1"),
        pair("KindC", "c1"),
        pair("KindA", "a2"),
        pair("KindB", "b1"),
        pair("KindB", "b2"),
    ]
}

fn has_ancestor(ancestor: Key) -> Filter {
    Filter {
        filter_type: Some(FilterType::PropertyFilter(PropertyFilter {
            property: Some(PropertyReference {
                name: "__key__".to_string(),
            }),
            op: property_filter::Operator::HasAncestor as i32,
            value: Some(val(ValueType::KeyValue(ancestor))),
        })),
    }
}

fn key_filter(op: property_filter::Operator, key: Key) -> Filter {
    Filter {
        filter_type: Some(FilterType::PropertyFilter(PropertyFilter {
            property: Some(PropertyReference {
                name: "__key__".to_string(),
            }),
            op: op as i32,
            value: Some(val(ValueType::KeyValue(key))),
        })),
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

fn and(filters: Vec<Filter>) -> Filter {
    Filter {
        filter_type: Some(FilterType::CompositeFilter(CompositeFilter {
            op: composite_filter::Operator::And as i32,
            filters,
        })),
    }
}

fn kindless(filter: Option<Filter>) -> Query {
    Query {
        filter,
        ..Default::default()
    }
}

fn order(prop: &str, direction: property_order::Direction) -> PropertyOrder {
    PropertyOrder {
        property: Some(PropertyReference {
            name: prop.to_string(),
        }),
        direction: direction as i32,
    }
}

fn limited(mut query: Query, limit: i32) -> Query {
    query.limit = Some(pbjson_types::Int32Value { value: limit });
    query
}

async fn run(
    storage: &Arc<RwLock<DatastoreStorage>>,
    namespace: &str,
    query: Query,
) -> Result<QueryResultBatch, tonic::Status> {
    let req = RunQueryRequest {
        project_id: PROJECT.to_string(),
        partition_id: partition(namespace),
        query_type: Some(QueryType::Query(query)),
        ..Default::default()
    };
    Ok(core::run_query(storage, req).await?.batch.expect("batch"))
}

/// `(kind, name)` of the last path element, in returned order.
fn pairs(batch: &QueryResultBatch) -> Vec<(String, String)> {
    batch
        .entity_results
        .iter()
        .map(|r| {
            let path = &r
                .entity
                .as_ref()
                .expect("entity")
                .key
                .as_ref()
                .expect("key")
                .path;
            let last = path.last().expect("path");
            let name = match &last.id_type {
                Some(IdType::Name(n)) => n.clone(),
                Some(IdType::Id(id)) => id.to_string(),
                None => String::new(),
            };
            (last.kind.clone(), name)
        })
        .collect()
}

async fn run_pairs(
    storage: &Arc<RwLock<DatastoreStorage>>,
    namespace: &str,
    query: Query,
) -> Vec<(String, String)> {
    pairs(&run(storage, namespace, query).await.expect("run_query"))
}

fn more_results(batch: &QueryResultBatch) -> MoreResultsType {
    MoreResultsType::from_i32(batch.more_results).expect("more_results")
}

/// Pages with `end_cursor` like google-cloud-ndb `fetch_page`.
async fn page_through(
    storage: &Arc<RwLock<DatastoreStorage>>,
    namespace: &str,
    query: Query,
) -> Vec<Vec<(String, String)>> {
    let mut pages = Vec::new();
    let mut cursor = Vec::new();
    for _ in 0..10 {
        let mut q = query.clone();
        q.start_cursor = cursor;
        let batch = run(storage, namespace, q).await.expect("run_query");
        assert!(
            batch.entity_results.iter().all(|r| !r.cursor.is_empty()),
            "every EntityResult needs a cursor"
        );
        assert!(!batch.end_cursor.is_empty(), "end_cursor must be set");
        cursor = batch.end_cursor.clone();
        let page = pairs(&batch);
        let done = page.is_empty() || more_results(&batch) == MoreResultsType::NoMoreResults;
        pages.push(page);
        if done {
            return pages;
        }
    }
    panic!("pagination did not stop: {pages:?}");
}

fn parent_key() -> Key {
    key(&[("KParent", "p")], "ns1")
}

#[tokio::test]
async fn kindless_ancestor_query_returns_all_kinds_in_key_order() {
    let storage = storage();
    let query = kindless(Some(has_ancestor(parent_key())));
    assert_eq!(
        run_pairs(&storage, "ns1", query).await,
        expected_ancestor_result()
    );
}

#[tokio::test]
async fn kindless_query_without_ancestor_returns_whole_namespace() {
    let storage = storage();
    assert_eq!(
        run_pairs(&storage, "ns1", kindless(None)).await,
        vec![
            pair("KParent", "p"),
            pair("KindA", "a1"),
            pair("KindC", "c1"),
            pair("KindA", "a2"),
            pair("KindB", "b1"),
            pair("KindB", "b2"),
            pair("KindA", "zz"),
        ]
    );
}

#[tokio::test]
async fn kindless_keys_only_and_limit_paging() {
    let storage = storage();
    let mut query = limited(kindless(Some(has_ancestor(parent_key()))), 2);
    query.projection = vec![Projection {
        property: Some(PropertyReference {
            name: "__key__".to_string(),
        }),
    }];
    let pages = page_through(&storage, "ns1", query).await;
    assert!(pages.len() > 1, "expected multiple pages: {pages:?}");
    let flat: Vec<(String, String)> = pages.into_iter().flatten().collect();
    assert_eq!(flat, expected_ancestor_result());
}

#[tokio::test]
async fn kindless_key_range_filter() {
    let storage = storage();
    let filter = and(vec![
        key_filter(
            property_filter::Operator::GreaterThan,
            key(&[("KParent", "p"), ("KindA", "a1")], "ns1"),
        ),
        key_filter(
            property_filter::Operator::LessThan,
            key(&[("KParent", "p"), ("KindB", "b2")], "ns1"),
        ),
    ]);
    assert_eq!(
        run_pairs(&storage, "ns1", kindless(Some(filter))).await,
        vec![
            pair("KindC", "c1"),
            pair("KindA", "a2"),
            pair("KindB", "b1"),
        ]
    );
}

#[tokio::test]
async fn kindless_rejects_property_filter_and_non_key_order() {
    let storage = storage();

    let flag = prop_filter(
        "flag",
        property_filter::Operator::Equal,
        val(ValueType::BooleanValue(true)),
    );
    let err = run(&storage, "ns1", kindless(Some(flag)))
        .await
        .expect_err("property filter must be rejected");
    assert_eq!(err.code(), Code::InvalidArgument);

    let by_n = Query {
        order: vec![order("n", property_order::Direction::Ascending)],
        ..Default::default()
    };
    let err = run(&storage, "ns1", by_n)
        .await
        .expect_err("non-__key__ order must be rejected");
    assert_eq!(err.code(), Code::InvalidArgument);

    let key_desc = Query {
        order: vec![order("__key__", property_order::Direction::Descending)],
        ..Default::default()
    };
    let err = run(&storage, "ns1", key_desc)
        .await
        .expect_err("descending __key__ order must be rejected");
    assert_eq!(err.code(), Code::InvalidArgument);

    let key_asc = Query {
        order: vec![order("__key__", property_order::Direction::Ascending)],
        ..Default::default()
    };
    run(&storage, "ns1", key_asc)
        .await
        .expect("ascending __key__ order is allowed");
}

fn task_storage(entities: &[(&[(&str, &str)], i64)]) -> Arc<RwLock<DatastoreStorage>> {
    let mut storage = DatastoreStorage::default();
    for (path, n) in entities {
        let entity = Entity {
            key: Some(key(path, "")),
            properties: HashMap::from([("n".to_string(), val(ValueType::IntegerValue(*n)))]),
        };
        storage.insert_entity(&entity).expect("insert");
    }
    Arc::new(RwLock::new(storage))
}

fn task_ancestor_storage() -> Arc<RwLock<DatastoreStorage>> {
    task_storage(&[
        (&[("Task", "p")], 1),
        (&[("Task", "p"), ("Task", "child")], 1),
        (&[("Task", "unrelated")], 1),
    ])
}

fn task_query(filter: Filter) -> Query {
    Query {
        kind: vec![datastore_emulator::google::datastore::v1::KindExpression {
            name: "Task".to_string(),
        }],
        filter: Some(filter),
        ..Default::default()
    }
}

fn task_pair(name: &str) -> (String, String) {
    pair("Task", name)
}

#[tokio::test]
async fn ancestor_query_includes_the_ancestor_itself() {
    let storage = task_ancestor_storage();
    let p = key(&[("Task", "p")], "");
    let expected = vec![task_pair("p"), task_pair("child")];

    // (i) simple index path.
    let simple = task_query(has_ancestor(p.clone()));
    assert_eq!(run_pairs(&storage, "", simple).await, expected);

    // (ii) index lookup path.
    let indexed = task_query(and(vec![
        has_ancestor(p.clone()),
        prop_filter(
            "n",
            property_filter::Operator::Equal,
            val(ValueType::IntegerValue(1)),
        ),
    ]));
    assert_eq!(run_pairs(&storage, "", indexed).await, expected);

    // (iii) scan path through apply_filter.
    let scanned = task_query(and(vec![
        has_ancestor(p),
        prop_filter(
            "n",
            property_filter::Operator::GreaterThanOrEqual,
            val(ValueType::IntegerValue(1)),
        ),
    ]));
    assert_eq!(run_pairs(&storage, "", scanned).await, expected);
}

#[tokio::test]
async fn kindless_count_includes_ancestor() {
    let storage = storage();
    let aggregation_query = AggregationQuery {
        query_type: Some(AggQueryType::NestedQuery(kindless(Some(has_ancestor(
            parent_key(),
        ))))),
        aggregations: vec![Aggregation {
            operator: Some(AggOperator::Count(Count { up_to: None })),
            alias: "c".to_string(),
        }],
    };
    let req = RunAggregationQueryRequest {
        project_id: PROJECT.to_string(),
        partition_id: partition("ns1"),
        query_type: Some(RunAggQueryType::AggregationQuery(aggregation_query)),
        ..Default::default()
    };
    let response = core::run_aggregation_query(&storage, req)
        .await
        .expect("run_aggregation_query");
    let batch = response.batch.expect("batch");
    match batch.aggregation_results[0].aggregate_properties["c"]
        .value_type
        .as_ref()
    {
        Some(ValueType::IntegerValue(n)) => assert_eq!(*n, 6),
        other => panic!("unexpected count value: {other:?}"),
    }
}
