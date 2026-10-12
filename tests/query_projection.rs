//! Projection queries are index scans: an array property yields one result
//! row per indexed value, and entities without an indexed projected value
//! are not returned.

use datastore_emulator::{
    DatastoreStorage, core,
    google::datastore::v1::{
        ArrayValue, Entity, Key, KindExpression, PartitionId, Projection, PropertyOrder,
        PropertyReference, Query, QueryResultBatch, RunQueryRequest, Value,
        key::{PathElement, path_element::IdType},
        property_order,
        query_result_batch::MoreResultsType,
        run_query_request::QueryType,
        value::ValueType,
    },
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

const PROJECT: &str = "p1";
const KIND: &str = "Thing";

type Row = (String, Vec<(String, Value)>);

fn key(name: &str) -> Key {
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

fn val(value_type: ValueType) -> Value {
    Value {
        value_type: Some(value_type),
        ..Default::default()
    }
}

fn text(s: &str) -> Value {
    val(ValueType::StringValue(s.to_string()))
}

fn int(i: i64) -> Value {
    val(ValueType::IntegerValue(i))
}

fn array(values: Vec<Value>) -> Value {
    val(ValueType::ArrayValue(ArrayValue { values }))
}

fn excluded(mut value: Value) -> Value {
    value.exclude_from_indexes = true;
    value
}

fn null() -> Value {
    val(ValueType::NullValue(0))
}

fn entity(name: &str, props: Vec<(&str, Value)>) -> Entity {
    Entity {
        key: Some(key(name)),
        properties: props
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect::<HashMap<_, _>>(),
    }
}

fn storage_with(entities: Vec<Entity>) -> Arc<RwLock<DatastoreStorage>> {
    let mut storage = DatastoreStorage::default();
    for e in &entities {
        storage.insert_entity(e).expect("insert");
    }
    Arc::new(RwLock::new(storage))
}

/// `tags` data: a=[a], ab=[a,b], p1=[b,a], c=[c].
fn tags_storage() -> Arc<RwLock<DatastoreStorage>> {
    storage_with(vec![
        entity("a", vec![("tags", array(vec![text("a")]))]),
        entity("ab", vec![("tags", array(vec![text("a"), text("b")]))]),
        entity("p1", vec![("tags", array(vec![text("b"), text("a")]))]),
        entity("c", vec![("tags", array(vec![text("c")]))]),
    ])
}

fn query() -> Query {
    Query {
        kind: vec![KindExpression {
            name: KIND.to_string(),
        }],
        ..Default::default()
    }
}

fn projected(names: &[&str]) -> Query {
    let mut q = query();
    q.projection = names
        .iter()
        .map(|n| Projection {
            property: Some(PropertyReference {
                name: n.to_string(),
            }),
        })
        .collect();
    q
}

fn with_order(mut query: Query, prop: &str, direction: property_order::Direction) -> Query {
    query.order = vec![PropertyOrder {
        property: Some(PropertyReference {
            name: prop.to_string(),
        }),
        direction: direction as i32,
    }];
    query
}

fn with_distinct_on(mut query: Query, prop: &str) -> Query {
    query.distinct_on = vec![PropertyReference {
        name: prop.to_string(),
    }];
    query
}

fn limited(mut query: Query, limit: i32) -> Query {
    query.limit = Some(pbjson_types::Int32Value { value: limit });
    query
}

async fn run(storage: &Arc<RwLock<DatastoreStorage>>, query: Query) -> QueryResultBatch {
    let req = RunQueryRequest {
        project_id: PROJECT.to_string(),
        query_type: Some(QueryType::Query(query)),
        ..Default::default()
    };
    core::run_query(storage, req)
        .await
        .expect("run_query")
        .batch
        .expect("batch")
}

/// `(key name, projected properties sorted by name)` in result order.
fn rows(batch: &QueryResultBatch) -> Vec<Row> {
    batch
        .entity_results
        .iter()
        .map(|r| {
            let e = r.entity.as_ref().expect("entity");
            let name = match e.key.as_ref().and_then(|k| k.path.last()?.id_type.as_ref()) {
                Some(IdType::Name(n)) => n.clone(),
                _ => String::new(),
            };
            let mut props: Vec<(String, Value)> = e
                .properties
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            props.sort_by(|a, b| a.0.cmp(&b.0));
            (name, props)
        })
        .collect()
}

fn row(name: &str, props: Vec<(&str, Value)>) -> Row {
    let mut props: Vec<(String, Value)> =
        props.into_iter().map(|(k, v)| (k.to_string(), v)).collect();
    props.sort_by(|a, b| a.0.cmp(&b.0));
    (name.to_string(), props)
}

#[tokio::test]
async fn projection_expands_array_values_in_value_then_key_order() {
    let storage = tags_storage();
    let batch = run(&storage, projected(&["tags"])).await;

    assert_eq!(
        rows(&batch),
        vec![
            row("a", vec![("tags", text("a"))]),
            row("ab", vec![("tags", text("a"))]),
            row("p1", vec![("tags", text("a"))]),
            row("ab", vec![("tags", text("b"))]),
            row("p1", vec![("tags", text("b"))]),
            row("c", vec![("tags", text("c"))]),
        ]
    );
}

#[tokio::test]
async fn projection_of_two_properties_is_the_cross_product() {
    let storage = storage_with(vec![entity(
        "e",
        vec![
            ("tags", array(vec![text("a"), text("b")])),
            ("nums", array(vec![int(1), int(2)])),
        ],
    )]);
    let batch = run(&storage, projected(&["tags", "nums"])).await;

    assert_eq!(
        rows(&batch),
        vec![
            // Rows follow the projected properties in alphabetical order
            // (nums, then tags), like the composite index Datastore recommends.
            row("e", vec![("tags", text("a")), ("nums", int(1))]),
            row("e", vec![("tags", text("b")), ("nums", int(1))]),
            row("e", vec![("tags", text("a")), ("nums", int(2))]),
            row("e", vec![("tags", text("b")), ("nums", int(2))]),
        ]
    );
}

#[tokio::test]
async fn projection_drops_entities_without_an_indexed_value_and_keeps_null() {
    let storage = storage_with(vec![
        entity("has", vec![("score", int(5))]),
        entity("lacks", vec![("other", int(1))]),
        entity("unindexed", vec![("score", excluded(int(7)))]),
        entity("nul", vec![("score", null())]),
    ]);
    let batch = run(&storage, projected(&["score"])).await;

    assert_eq!(
        rows(&batch),
        vec![
            row("nul", vec![("score", null())]),
            row("has", vec![("score", int(5))]),
        ]
    );
}

#[tokio::test]
async fn projection_resolves_dotted_paths() {
    let address = val(ValueType::EntityValue(entity(
        "",
        vec![("city", text("Lisbon"))],
    )));
    let storage = storage_with(vec![entity("e", vec![("address", address)])]);
    let batch = run(&storage, projected(&["address.city"])).await;

    assert_eq!(
        rows(&batch),
        vec![row("e", vec![("address.city", text("Lisbon"))])]
    );
}

/// `e1=[b,d]`, `e2=[a,c]`.
fn ordered_storage() -> Arc<RwLock<DatastoreStorage>> {
    storage_with(vec![
        entity("e1", vec![("tags", array(vec![text("b"), text("d")]))]),
        entity("e2", vec![("tags", array(vec![text("a"), text("c")]))]),
    ])
}

#[tokio::test]
async fn projection_with_explicit_order_sorts_rows() {
    let storage = ordered_storage();

    let asc = run(
        &storage,
        with_order(
            projected(&["tags"]),
            "tags",
            property_order::Direction::Ascending,
        ),
    )
    .await;
    assert_eq!(
        rows(&asc),
        vec![
            row("e2", vec![("tags", text("a"))]),
            row("e1", vec![("tags", text("b"))]),
            row("e2", vec![("tags", text("c"))]),
            row("e1", vec![("tags", text("d"))]),
        ]
    );

    let desc = run(
        &storage,
        with_order(
            projected(&["tags"]),
            "tags",
            property_order::Direction::Descending,
        ),
    )
    .await;
    assert_eq!(
        rows(&desc),
        vec![
            row("e1", vec![("tags", text("d"))]),
            row("e2", vec![("tags", text("c"))]),
            row("e1", vec![("tags", text("b"))]),
            row("e2", vec![("tags", text("a"))]),
        ]
    );
}

#[tokio::test]
async fn distinct_on_applies_to_expanded_rows() {
    let storage = storage_with(vec![
        entity("e1", vec![("tags", array(vec![text("a"), text("b")]))]),
        entity("e2", vec![("tags", array(vec![text("b"), text("c")]))]),
        entity("e3", vec![("tags", array(vec![text("c")]))]),
    ]);
    let batch = run(&storage, with_distinct_on(projected(&["tags"]), "tags")).await;

    assert_eq!(
        rows(&batch),
        vec![
            row("e1", vec![("tags", text("a"))]),
            row("e1", vec![("tags", text("b"))]),
            row("e2", vec![("tags", text("c"))]),
        ]
    );
}

#[tokio::test]
async fn keys_only_is_not_expanded() {
    let storage = storage_with(vec![entity(
        "e",
        vec![("tags", array(vec![text("a"), text("b")]))],
    )]);
    let batch = run(&storage, projected(&["__key__"])).await;

    assert_eq!(batch.entity_result_type, 3);
    assert_eq!(batch.entity_results.len(), 1);
}

#[tokio::test]
async fn projection_rows_page_with_cursors() {
    let storage = tags_storage();
    let mut all = Vec::new();
    let mut cursor = Vec::new();
    for _ in 0..10 {
        let mut q = limited(projected(&["tags"]), 2);
        q.start_cursor = cursor.clone();
        let batch = run(&storage, q).await;

        assert_eq!(batch.entity_result_type, 2);
        all.extend(rows(&batch));
        cursor = batch.end_cursor.clone();
        if MoreResultsType::from_i32(batch.more_results) == Some(MoreResultsType::NoMoreResults) {
            break;
        }
    }

    assert_eq!(
        all,
        vec![
            row("a", vec![("tags", text("a"))]),
            row("ab", vec![("tags", text("a"))]),
            row("p1", vec![("tags", text("a"))]),
            row("ab", vec![("tags", text("b"))]),
            row("p1", vec![("tags", text("b"))]),
            row("c", vec![("tags", text("c"))]),
        ]
    );
}
