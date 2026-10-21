//! Regression tests for query cursors, auto IDs and dotted property names.
//!
//! Before the fix, `EntityResult.cursor` was empty and `start_cursor` was
//! ignored, so the google-cloud-ndb `fetch_page` loop got page 1 forever.

use datastore_emulator::{
    DatastoreStorage, core,
    google::datastore::v1::{
        AllocateIdsRequest, CompositeFilter, Entity, Filter, Key, KindExpression, PartitionId,
        Projection, PropertyFilter, PropertyOrder, PropertyReference, Query, QueryResultBatch,
        RunQueryRequest, Value, composite_filter,
        filter::FilterType,
        key::{PathElement, path_element::IdType},
        property_filter, property_order,
        query_result_batch::MoreResultsType,
        run_query_request::QueryType,
        value::ValueType,
    },
};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use tonic::Code;

const PROJECT: &str = "p1";
const KIND: &str = "Thing";
const N: usize = 25;

fn key(kind: &str, id_type: Option<IdType>) -> Key {
    Key {
        partition_id: Some(PartitionId {
            project_id: PROJECT.to_string(),
            database_id: String::new(),
            namespace_id: String::new(),
        }),
        path: vec![PathElement {
            kind: kind.to_string(),
            id_type,
        }],
    }
}

fn val(value_type: ValueType) -> Value {
    Value {
        value_type: Some(value_type),
        ..Default::default()
    }
}

fn name(i: usize) -> String {
    format!("t{i:03}")
}

fn names(range: std::ops::Range<usize>) -> Vec<String> {
    range.map(name).collect()
}

/// `t000..t024`, `updated` grows with the index, `n = i % 7` has ties.
fn storage_with_things() -> Arc<RwLock<DatastoreStorage>> {
    let mut storage = DatastoreStorage::default();
    for i in 0..N {
        insert_thing(&mut storage, &name(i), i as i64);
    }
    Arc::new(RwLock::new(storage))
}

fn insert_thing(storage: &mut DatastoreStorage, entity_name: &str, i: i64) {
    let entity = Entity {
        key: Some(key(KIND, Some(IdType::Name(entity_name.to_string())))),
        properties: HashMap::from([
            (
                "updated".to_string(),
                val(ValueType::IntegerValue(1000 + i)),
            ),
            ("n".to_string(), val(ValueType::IntegerValue(i % 7))),
        ]),
    };
    storage.insert_entity(&entity).expect("insert");
}

fn query() -> Query {
    Query {
        kind: vec![KindExpression {
            name: KIND.to_string(),
        }],
        ..Default::default()
    }
}

fn limited(mut query: Query, limit: i32) -> Query {
    query.limit = Some(pbjson_types::Int32Value { value: limit });
    query
}

fn order(prop: &str, direction: property_order::Direction) -> PropertyOrder {
    PropertyOrder {
        property: Some(PropertyReference {
            name: prop.to_string(),
        }),
        direction: direction as i32,
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

/// `updated >= 1000 AND updated < 2000`: matches all, implicit sort on `updated`.
fn range_filter() -> Filter {
    Filter {
        filter_type: Some(FilterType::CompositeFilter(CompositeFilter {
            op: composite_filter::Operator::And as i32,
            filters: vec![
                prop_filter(
                    "updated",
                    property_filter::Operator::GreaterThanOrEqual,
                    val(ValueType::IntegerValue(1000)),
                ),
                prop_filter(
                    "updated",
                    property_filter::Operator::LessThan,
                    val(ValueType::IntegerValue(2000)),
                ),
            ],
        })),
    }
}

async fn run(
    storage: &Arc<RwLock<DatastoreStorage>>,
    query: Query,
) -> Result<QueryResultBatch, tonic::Status> {
    let req = RunQueryRequest {
        project_id: PROJECT.to_string(),
        query_type: Some(QueryType::Query(query)),
        ..Default::default()
    };
    Ok(core::run_query(storage, req).await?.batch.expect("batch"))
}

fn result_names(batch: &QueryResultBatch) -> Vec<String> {
    batch
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
        .collect()
}

fn more_results(batch: &QueryResultBatch) -> MoreResultsType {
    MoreResultsType::try_from(batch.more_results).expect("more_results")
}

/// Pages with `end_cursor` like google-cloud-ndb `fetch_page`, which uses the
/// cursor of the last result. Stops on an empty page, like ndb.
async fn page_through(storage: &Arc<RwLock<DatastoreStorage>>, query: Query) -> Vec<Vec<String>> {
    let mut pages = Vec::new();
    let mut cursor = Vec::new();
    for _ in 0..10 {
        let mut q = query.clone();
        q.start_cursor = cursor;
        let batch = run(storage, q).await.expect("run_query");
        let page = result_names(&batch);
        assert!(
            batch.entity_results.iter().all(|r| !r.cursor.is_empty()),
            "every EntityResult needs a cursor"
        );
        assert!(!batch.end_cursor.is_empty(), "end_cursor must be set");
        if let Some(last) = batch.entity_results.last() {
            assert_eq!(last.cursor, batch.end_cursor);
        }
        cursor = batch.end_cursor.clone();
        let done = page.is_empty() || more_results(&batch) == MoreResultsType::NoMoreResults;
        pages.push(page);
        if done {
            return pages;
        }
    }
    panic!("pagination did not stop: {pages:?}");
}

#[tokio::test]
async fn fetch_page_loop_walks_all_pages_without_filter() {
    let storage = storage_with_things();
    let pages = page_through(&storage, limited(query(), 10)).await;
    assert_eq!(pages, vec![names(0..10), names(10..20), names(20..25)]);
}

#[tokio::test]
async fn fetch_page_loop_walks_all_pages_with_range_filter() {
    let storage = storage_with_things();
    let mut q = limited(query(), 10);
    q.filter = Some(range_filter());
    let pages = page_through(&storage, q).await;
    assert_eq!(pages, vec![names(0..10), names(10..20), names(20..25)]);
}

#[tokio::test]
async fn more_results_reports_limit_and_exhaustion() {
    let storage = storage_with_things();
    let first = run(&storage, limited(query(), 10)).await.unwrap();
    assert_eq!(more_results(&first), MoreResultsType::MoreResultsAfterLimit);

    let mut last = limited(query(), 10);
    last.offset = 20;
    let last = run(&storage, last).await.unwrap();
    assert_eq!(result_names(&last), names(20..25));
    assert_eq!(more_results(&last), MoreResultsType::NoMoreResults);
}

#[tokio::test]
async fn mid_page_entity_cursor_resumes_after_that_entity() {
    let storage = storage_with_things();
    for q in [query(), {
        let mut q = query();
        q.filter = Some(range_filter());
        q
    }] {
        let batch = run(&storage, limited(q.clone(), 10)).await.unwrap();
        for k in [0, 3, 9] {
            let mut next = limited(q.clone(), 3);
            next.start_cursor = batch.entity_results[k].cursor.clone();
            let resumed = run(&storage, next).await.unwrap();
            assert_eq!(
                result_names(&resumed),
                names(k + 1..k + 4),
                "cursor of entity {k}"
            );
        }
    }
}

#[tokio::test]
async fn cursor_works_with_explicit_order_and_ties() {
    let storage = storage_with_things();
    let mut q = query();
    q.order = vec![
        order("n", property_order::Direction::Ascending),
        order("updated", property_order::Direction::Descending),
    ];
    let full = result_names(&run(&storage, q.clone()).await.unwrap());
    assert_eq!(full[..4], ["t021", "t014", "t007", "t000"]);

    let pages: Vec<String> = page_through(&storage, limited(q, 4))
        .await
        .into_iter()
        .flatten()
        .collect();
    assert_eq!(pages, full);

    // Ties on the only sort property are broken by key.
    let mut q = query();
    q.order = vec![order("n", property_order::Direction::Ascending)];
    let full = result_names(&run(&storage, q.clone()).await.unwrap());
    assert_eq!(full[..4], ["t000", "t007", "t014", "t021"]);
    let pages: Vec<String> = page_through(&storage, limited(q, 3))
        .await
        .into_iter()
        .flatten()
        .collect();
    assert_eq!(pages, full);
}

#[tokio::test]
async fn cursor_works_with_keys_only_query() {
    let storage = storage_with_things();
    let mut q = limited(query(), 6);
    q.projection = vec![Projection {
        property: Some(PropertyReference {
            name: "__key__".to_string(),
        }),
    }];
    let pages: Vec<String> = page_through(&storage, q.clone())
        .await
        .into_iter()
        .flatten()
        .collect();
    assert_eq!(pages, names(0..N));

    q.order = vec![order("__key__", property_order::Direction::Descending)];
    let pages: Vec<String> = page_through(&storage, q)
        .await
        .into_iter()
        .flatten()
        .collect();
    assert_eq!(pages, names(0..N).into_iter().rev().collect::<Vec<_>>());
}

#[tokio::test]
async fn empty_batch_has_end_cursor_that_keeps_position() {
    let storage = storage_with_things();
    let mut empty = query();
    empty.filter = Some(prop_filter(
        "n",
        property_filter::Operator::Equal,
        val(ValueType::IntegerValue(99)),
    ));
    let batch = run(&storage, empty).await.unwrap();
    assert!(batch.entity_results.is_empty());
    assert!(!batch.end_cursor.is_empty());

    // A start cursor from an empty batch points to the start.
    let mut from_start = limited(query(), 2);
    from_start.start_cursor = batch.end_cursor;
    assert_eq!(
        result_names(&run(&storage, from_start).await.unwrap()),
        names(0..2)
    );

    // An empty page after the last entity keeps the same end cursor.
    let all = run(&storage, query()).await.unwrap();
    let mut after_end = limited(query(), 5);
    after_end.start_cursor = all.end_cursor.clone();
    let batch = run(&storage, after_end).await.unwrap();
    assert!(batch.entity_results.is_empty());
    assert_eq!(batch.end_cursor, all.end_cursor);
    assert_eq!(more_results(&batch), MoreResultsType::NoMoreResults);
}

#[tokio::test]
async fn end_cursor_and_offset_combine_with_start_cursor() {
    let storage = storage_with_things();
    let all = run(&storage, query()).await.unwrap();
    let cursor_after = |i: usize| all.entity_results[i].cursor.clone();

    let mut window = query();
    window.start_cursor = cursor_after(4);
    window.end_cursor = cursor_after(11);
    let batch = run(&storage, window).await.unwrap();
    assert_eq!(result_names(&batch), names(5..12));
    assert_eq!(
        more_results(&batch),
        MoreResultsType::MoreResultsAfterCursor
    );

    let mut skip = limited(query(), 4);
    skip.start_cursor = cursor_after(4);
    skip.offset = 3;
    let batch = run(&storage, skip).await.unwrap();
    assert_eq!(result_names(&batch), names(8..12));
    assert_eq!(batch.skipped_results, 3);
    let mut from_skipped = limited(query(), 1);
    from_skipped.start_cursor = batch.skipped_cursor;
    assert_eq!(
        result_names(&run(&storage, from_skipped).await.unwrap()),
        names(8..9)
    );
}

#[tokio::test]
async fn cursor_survives_writes_between_pages() {
    let storage = storage_with_things();
    let page = run(&storage, limited(query(), 10)).await.unwrap();
    {
        let mut storage = storage.write();
        storage.delete_entity(&key(KIND, Some(IdType::Name(name(9)))));
        insert_thing(&mut storage, "t0045", 0);
        insert_thing(&mut storage, "t0095", 0);
    }
    let mut next = limited(query(), 3);
    next.start_cursor = page.end_cursor;
    let batch = run(&storage, next).await.unwrap();
    assert_eq!(result_names(&batch), ["t0095", "t010", "t011"]);
}

#[tokio::test]
async fn invalid_cursor_is_rejected() {
    let storage = storage_with_things();
    for cursor in [vec![0, 0, 0, 10], b"garbage".to_vec()] {
        let mut q = query();
        q.start_cursor = cursor;
        let err = run(&storage, q).await.expect_err("invalid cursor");
        assert_eq!(err.code(), Code::InvalidArgument);
    }
}

#[tokio::test]
async fn auto_ids_are_unique_across_kinds() {
    let mut storage = DatastoreStorage::default();
    let mut ids = Vec::new();
    for kind in ["KindA", "KindB", "KindA"] {
        let entity = Entity {
            key: Some(key(kind, None)),
            properties: HashMap::new(),
        };
        let (key, _) = storage.insert_entity(&entity).unwrap();
        match key.path[0].id_type {
            Some(IdType::Id(id)) => ids.push(id),
            ref other => panic!("expected an auto id, got {other:?}"),
        }
    }
    assert_eq!(ids, [1, 2, 3]);

    let storage = Arc::new(RwLock::new(storage));
    let allocated = core::allocate_ids(
        &storage,
        AllocateIdsRequest {
            project_id: PROJECT.to_string(),
            keys: vec![key("KindC", None), key("KindA", None)],
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let allocated: Vec<_> = allocated
        .keys
        .iter()
        .map(|k| k.path[0].id_type.clone())
        .collect();
    assert_eq!(allocated, [Some(IdType::Id(4)), Some(IdType::Id(5))]);
}

#[tokio::test]
async fn inequality_on_dotted_property_name_matches() {
    // A legacy ndb StructuredProperty stores `moved.moved_to` as one property.
    let mut storage = DatastoreStorage::default();
    for (entity_name, props) in [
        (
            "old",
            HashMap::from([(
                "moved.moved_to".to_string(),
                val(ValueType::StringValue("new".to_string())),
            )]),
        ),
        ("plain", HashMap::new()),
    ] {
        let entity = Entity {
            key: Some(key("Tmpl", Some(IdType::Name(entity_name.to_string())))),
            properties: props,
        };
        storage.insert_entity(&entity).unwrap();
    }
    let storage = Arc::new(RwLock::new(storage));

    use property_filter::Operator::*;
    for (op, value) in [
        (GreaterThan, ""),
        (GreaterThanOrEqual, "a"),
        (LessThan, "z"),
        (LessThanOrEqual, "new"),
        (Equal, "new"),
    ] {
        let q = Query {
            kind: vec![KindExpression {
                name: "Tmpl".to_string(),
            }],
            filter: Some(prop_filter(
                "moved.moved_to",
                op,
                val(ValueType::StringValue(value.to_string())),
            )),
            ..Default::default()
        };
        let batch = run(&storage, q).await.unwrap();
        assert_eq!(
            result_names(&batch),
            ["old"],
            "moved.moved_to {op:?} {value:?}"
        );
    }
}

#[tokio::test]
async fn limit_zero_returns_no_results() {
    let storage = storage_with_things();
    let batch = run(&storage, limited(query(), 0)).await.unwrap();
    assert!(result_names(&batch).is_empty());
    assert_eq!(more_results(&batch), MoreResultsType::MoreResultsAfterLimit);
    assert!(!batch.end_cursor.is_empty());
}

#[tokio::test]
async fn limit_zero_with_offset_skips_but_returns_nothing() {
    let storage = storage_with_things();
    let mut q = limited(query(), 0);
    q.offset = 2;
    let batch = run(&storage, q).await.unwrap();
    assert!(result_names(&batch).is_empty());
    assert_eq!(batch.skipped_results, 2);
}
