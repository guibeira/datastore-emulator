//! Regression tests for query bugs reported in
//! https://github.com/guibeira/datastore-emulator/issues/26
//!
//! Each test reproduces one of the reported issues:
//!   1. Cross-type ordering must follow the documented Datastore order
//!      (null < integer/timestamp < boolean < string < blob < double < ...).
//!   2. An inequality filter without an explicit `order` should implicitly
//!      sort by the inequality property (ascending).
//!   3. Properties marked `exclude_from_indexes` must not match index-backed
//!      filters.
//!   4. The `offset` field on `Query` must be honored.
//!   5. Updating an entity must purge stale index entries for old values.

use datastore_emulator::{
    DatastoreStorage, core,
    database::EntityWithMetadata,
    google::datastore::v1::{
        ArrayValue, CommitRequest, Entity, Filter, Key, Mutation, PartitionId, PropertyOrder,
        PropertyReference, Query, RunQueryRequest, Value,
        commit_request::Mode,
        filter::FilterType,
        key::{PathElement, path_element::IdType},
        mutation::Operation,
        property_filter, property_order,
        run_query_request::QueryType,
        value::ValueType,
    },
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::Code;

const PROJECT: &str = "p1";
const KIND: &str = "Item";

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

fn val_excluded(value_type: ValueType) -> Value {
    Value {
        value_type: Some(value_type),
        exclude_from_indexes: true,
        ..Default::default()
    }
}

fn val_array(values: Vec<Value>) -> Value {
    Value {
        value_type: Some(ValueType::ArrayValue(ArrayValue { values })),
        ..Default::default()
    }
}

fn insert(storage: &mut DatastoreStorage, name: &str, props: HashMap<String, Value>) {
    let entity = Entity {
        key: Some(key(name)),
        properties: props,
    };
    storage.put_entity(
        EntityWithMetadata {
            entity,
            version: 1,
            create_time: pbjson_types::Timestamp::default(),
            update_time: pbjson_types::Timestamp::default(),
        }
        .into(),
    );
}

fn name_of(entity: &Entity) -> String {
    entity
        .key
        .as_ref()
        .and_then(|k| k.path.last())
        .and_then(|p| match p.id_type.as_ref()? {
            IdType::Name(n) => Some(n.clone()),
            _ => None,
        })
        .unwrap_or_default()
}

async fn run(storage: Arc<RwLock<DatastoreStorage>>, query: Query) -> Vec<String> {
    let req = RunQueryRequest {
        project_id: PROJECT.to_string(),
        query_type: Some(QueryType::Query(query)),
        ..Default::default()
    };
    let resp = core::run_query(&storage, req).await.expect("run_query");
    resp.batch
        .expect("batch")
        .entity_results
        .into_iter()
        .map(|r| name_of(&r.entity.unwrap_or_default()))
        .collect()
}

fn order_by_asc(prop: &str) -> PropertyOrder {
    PropertyOrder {
        property: Some(PropertyReference {
            name: prop.to_string(),
        }),
        direction: property_order::Direction::Ascending as i32,
    }
}

fn prop_filter(name: &str, op: property_filter::Operator, value: Value) -> Filter {
    Filter {
        filter_type: Some(FilterType::PropertyFilter(
            datastore_emulator::google::datastore::v1::PropertyFilter {
                property: Some(PropertyReference {
                    name: name.to_string(),
                }),
                op: op as i32,
                value: Some(value),
            },
        )),
    }
}

/// Bug 1: cross-type ordering should follow the documented Datastore order:
/// null < integer/timestamp (compared numerically) < boolean < string < blob.
#[tokio::test]
async fn cross_type_ordering_follows_datastore_spec() {
    let mut storage = DatastoreStorage::default();
    // Insert in random order; "score" property carries values of different types.
    insert(
        &mut storage,
        "str",
        HashMap::from([(
            "score".to_string(),
            val(ValueType::StringValue("a".to_string())),
        )]),
    );
    insert(
        &mut storage,
        "bool",
        HashMap::from([("score".to_string(), val(ValueType::BooleanValue(true)))]),
    );
    insert(
        &mut storage,
        "ts",
        HashMap::from([(
            "score".to_string(),
            val(ValueType::TimestampValue(pbjson_types::Timestamp {
                seconds: 0,
                nanos: 0,
            })),
        )]),
    );
    insert(
        &mut storage,
        "int",
        HashMap::from([("score".to_string(), val(ValueType::IntegerValue(0)))]),
    );
    insert(
        &mut storage,
        "null",
        HashMap::from([("score".to_string(), val(ValueType::NullValue(0)))]),
    );

    let storage = Arc::new(RwLock::new(storage));

    let query = Query {
        kind: vec![datastore_emulator::google::datastore::v1::KindExpression {
            name: KIND.to_string(),
        }],
        order: vec![order_by_asc("score")],
        ..Default::default()
    };

    let names = run(storage, query).await;
    // int 0 and timestamp 1970-01-01T00:00:00Z are both 0 microseconds, so they tie.
    assert_eq!(names.len(), 5, "cross-type ordering mismatch: {names:?}");
    assert_eq!(names[0], "null", "cross-type ordering mismatch: {names:?}");
    let mut tied = names[1..3].to_vec();
    tied.sort();
    assert_eq!(
        tied,
        vec!["int", "ts"],
        "cross-type ordering mismatch: {names:?}"
    );
    assert_eq!(
        names[3..],
        ["bool", "str"],
        "cross-type ordering mismatch: {names:?}"
    );
}

/// Integers and doubles are different ranks: every int sorts before every double,
/// so `>`/`<` filters between them follow the rank, not the numeric value.
#[tokio::test]
async fn integers_never_compare_numerically_with_doubles() {
    let mut storage = DatastoreStorage::default();
    let threshold = 9_007_199_254_740_992.0;
    let int_above_threshold = 9_007_199_254_740_993i64;

    insert(
        &mut storage,
        "double_threshold",
        HashMap::from([("score".to_string(), val(ValueType::DoubleValue(threshold)))]),
    );
    insert(
        &mut storage,
        "int_above",
        HashMap::from([(
            "score".to_string(),
            val(ValueType::IntegerValue(int_above_threshold)),
        )]),
    );
    let storage = Arc::new(RwLock::new(storage));

    let greater_than_double = Query {
        kind: vec![datastore_emulator::google::datastore::v1::KindExpression {
            name: KIND.to_string(),
        }],
        filter: Some(prop_filter(
            "score",
            property_filter::Operator::GreaterThan,
            val(ValueType::DoubleValue(threshold)),
        )),
        order: vec![order_by_asc("score")],
        ..Default::default()
    };
    let names = run(storage.clone(), greater_than_double).await;
    assert_eq!(
        names,
        Vec::<String>::new(),
        "integer 2^53 + 1 ranks before double 2^53, so it must not match `> double`"
    );

    let less_than_int = Query {
        kind: vec![datastore_emulator::google::datastore::v1::KindExpression {
            name: KIND.to_string(),
        }],
        filter: Some(prop_filter(
            "score",
            property_filter::Operator::LessThan,
            val(ValueType::IntegerValue(int_above_threshold)),
        )),
        order: vec![order_by_asc("score")],
        ..Default::default()
    };
    let names = run(storage, less_than_int).await;
    assert_eq!(
        names,
        Vec::<String>::new(),
        "double 2^53 ranks after integer 2^53 + 1, so it must not match `< int`"
    );
}

/// Bug 2: an inequality filter with no explicit `order` should implicitly
/// sort results by the inequality property ascending.
#[tokio::test]
async fn inequality_filter_applies_implicit_order() {
    let mut storage = DatastoreStorage::default();
    // Insert entities so that BTreeMap key order disagrees with `age` order.
    // Names sort a < b < c, but ages are 30, 10, 20 respectively. Without
    // implicit ordering on the inequality property, results come back in
    // key (name) order.
    for (name, age) in [("a", 30i64), ("b", 10), ("c", 20)] {
        insert(
            &mut storage,
            name,
            HashMap::from([("age".to_string(), val(ValueType::IntegerValue(age)))]),
        );
    }
    let storage = Arc::new(RwLock::new(storage));

    let query = Query {
        kind: vec![datastore_emulator::google::datastore::v1::KindExpression {
            name: KIND.to_string(),
        }],
        filter: Some(prop_filter(
            "age",
            property_filter::Operator::GreaterThan,
            val(ValueType::IntegerValue(0)),
        )),
        // No explicit `order` field.
        ..Default::default()
    };

    let names = run(storage, query).await;
    assert_eq!(
        names,
        vec!["b", "c", "a"],
        "inequality filter must implicitly order by the inequality property (b=10 < c=20 < a=30)"
    );
}

/// Bug 3: properties marked `exclude_from_indexes` must not match filters
/// since they are not indexed. Uses an inequality filter to hit the
/// non-index-backed scan path where the check is currently missing.
#[tokio::test]
async fn exclude_from_indexes_skips_filter_matches() {
    let mut storage = DatastoreStorage::default();
    insert(
        &mut storage,
        "indexed",
        HashMap::from([("score".to_string(), val(ValueType::IntegerValue(50)))]),
    );
    insert(
        &mut storage,
        "excluded",
        HashMap::from([(
            "score".to_string(),
            val_excluded(ValueType::IntegerValue(50)),
        )]),
    );
    let storage = Arc::new(RwLock::new(storage));

    let query = Query {
        kind: vec![datastore_emulator::google::datastore::v1::KindExpression {
            name: KIND.to_string(),
        }],
        filter: Some(prop_filter(
            "score",
            property_filter::Operator::GreaterThan,
            val(ValueType::IntegerValue(0)),
        )),
        order: vec![order_by_asc("score")],
        ..Default::default()
    };

    let names = run(storage, query).await;
    assert_eq!(
        names,
        vec!["indexed"],
        "exclude_from_indexes property must not match filters even via scan path"
    );
}

#[tokio::test]
async fn order_by_skips_excluded_array_values() {
    let mut storage = DatastoreStorage::default();
    insert(
        &mut storage,
        "scalar",
        HashMap::from([("score".to_string(), val(ValueType::IntegerValue(10)))]),
    );
    insert(
        &mut storage,
        "array_indexed",
        HashMap::from([(
            "score".to_string(),
            val_array(vec![
                val_excluded(ValueType::IntegerValue(0)),
                val(ValueType::IntegerValue(5)),
            ]),
        )]),
    );
    insert(
        &mut storage,
        "all_excluded",
        HashMap::from([(
            "score".to_string(),
            val_array(vec![val_excluded(ValueType::IntegerValue(1))]),
        )]),
    );
    let storage = Arc::new(RwLock::new(storage));

    let query = Query {
        kind: vec![datastore_emulator::google::datastore::v1::KindExpression {
            name: KIND.to_string(),
        }],
        order: vec![order_by_asc("score")],
        ..Default::default()
    };

    let names = run(storage, query).await;
    assert_eq!(
        names,
        vec!["array_indexed", "scalar"],
        "sort order must ignore excluded array members and drop arrays with no indexed members, like Datastore"
    );
}

/// Bug 4: the `Query.offset` field must skip N results before returning.
#[tokio::test]
async fn query_offset_is_respected() {
    let mut storage = DatastoreStorage::default();
    for (name, score) in [("a", 1i64), ("b", 2), ("c", 3), ("d", 4), ("e", 5)] {
        insert(
            &mut storage,
            name,
            HashMap::from([("score".to_string(), val(ValueType::IntegerValue(score)))]),
        );
    }
    let storage = Arc::new(RwLock::new(storage));

    let query = Query {
        kind: vec![datastore_emulator::google::datastore::v1::KindExpression {
            name: KIND.to_string(),
        }],
        order: vec![order_by_asc("score")],
        offset: 2,
        ..Default::default()
    };

    let req = RunQueryRequest {
        project_id: PROJECT.to_string(),
        query_type: Some(QueryType::Query(query)),
        ..Default::default()
    };
    let resp = core::run_query(&storage, req).await.expect("run_query");
    let batch = resp.batch.expect("batch");

    let names: Vec<String> = batch
        .entity_results
        .iter()
        .map(|r| name_of(r.entity.as_ref().unwrap()))
        .collect();
    assert_eq!(
        names,
        vec!["c", "d", "e"],
        "offset must skip first 2 results"
    );
    assert_eq!(
        batch.skipped_results, 2,
        "skipped_results must report the offset"
    );
}

#[tokio::test]
async fn full_query_results_include_entity_metadata() {
    let mut storage = DatastoreStorage::default();
    let create_time = pbjson_types::Timestamp {
        seconds: 11,
        nanos: 22,
    };
    let update_time = pbjson_types::Timestamp {
        seconds: 33,
        nanos: 44,
    };
    let entity = Entity {
        key: Some(key("with_metadata")),
        properties: HashMap::from([("score".to_string(), val(ValueType::IntegerValue(7)))]),
    };
    storage.put_entity(
        EntityWithMetadata {
            entity,
            version: 42,
            create_time: create_time.clone(),
            update_time: update_time.clone(),
        }
        .into(),
    );
    let storage = Arc::new(RwLock::new(storage));

    let query = Query {
        kind: vec![datastore_emulator::google::datastore::v1::KindExpression {
            name: KIND.to_string(),
        }],
        ..Default::default()
    };
    let req = RunQueryRequest {
        project_id: PROJECT.to_string(),
        query_type: Some(QueryType::Query(query)),
        ..Default::default()
    };
    let resp = core::run_query(&storage, req).await.expect("run_query");
    let batch = resp.batch.expect("batch");
    assert_eq!(
        batch.entity_result_type, 1,
        "query should return FULL results"
    );
    assert_eq!(batch.entity_results.len(), 1);
    let result = &batch.entity_results[0];
    assert_eq!(result.version, 42);
    assert_eq!(result.create_time.as_ref(), Some(&create_time));
    assert_eq!(result.update_time.as_ref(), Some(&update_time));
}

#[tokio::test]
async fn malformed_order_without_property_does_not_panic() {
    let mut storage = DatastoreStorage::default();
    insert(
        &mut storage,
        "x",
        HashMap::from([("score".to_string(), val(ValueType::IntegerValue(1)))]),
    );
    let storage = Arc::new(RwLock::new(storage));

    let query = Query {
        kind: vec![datastore_emulator::google::datastore::v1::KindExpression {
            name: KIND.to_string(),
        }],
        order: vec![PropertyOrder {
            property: None,
            direction: property_order::Direction::Ascending as i32,
        }],
        ..Default::default()
    };

    let names = run(storage, query).await;
    assert_eq!(names, vec!["x"]);
}

/// Bug 5: updating an entity must purge stale index entries for the old
/// property values. Currently `core::commit` calls `update_indexes` on the
/// new entity without first calling `remove_from_indexes` for the previous
/// state, so old values stay indexed and may match equality filters even
/// though the entity no longer holds them.
#[tokio::test]
async fn update_purges_stale_index_entries() {
    let mut storage = DatastoreStorage::default();
    insert(
        &mut storage,
        "x",
        HashMap::from([(
            "tag".to_string(),
            val(ValueType::StringValue("old".to_string())),
        )]),
    );
    let storage = Arc::new(RwLock::new(storage));

    // Update the entity to change `tag` from "old" to "new".
    let updated_entity = Entity {
        key: Some(key("x")),
        properties: HashMap::from([(
            "tag".to_string(),
            val(ValueType::StringValue("new".to_string())),
        )]),
    };
    let commit_req = CommitRequest {
        project_id: PROJECT.to_string(),
        mode: Mode::NonTransactional as i32,
        mutations: vec![Mutation {
            operation: Some(Operation::Update(updated_entity)),
            ..Default::default()
        }],
        ..Default::default()
    };
    core::commit(&storage, commit_req).await.expect("commit");

    // After the update, the indexes map must not contain the stale "old"
    // entry pointing at this entity's key.
    let storage_guard = storage.read().await;
    let old_tag = datastore_emulator::database::index_keys_for_value(
        KIND,
        "tag",
        &val(ValueType::StringValue("old".to_string())),
    );
    let stale = storage_guard.indexes.get(&old_tag[0]);
    let stale_keys: Vec<_> = stale.map(|set| set.iter().collect()).unwrap_or_default();
    assert!(
        stale_keys.is_empty(),
        "stale index entry for old value not cleaned up: {:?}",
        stale_keys
    );

    // Querying by the old value must return no results.
    drop(storage_guard);
    let query = Query {
        kind: vec![datastore_emulator::google::datastore::v1::KindExpression {
            name: KIND.to_string(),
        }],
        filter: Some(prop_filter(
            "tag",
            property_filter::Operator::Equal,
            val(ValueType::StringValue("old".to_string())),
        )),
        ..Default::default()
    };
    let names = run(storage.clone(), query).await;
    assert!(
        names.is_empty(),
        "query for old value must return nothing after update, got {:?}",
        names
    );
}

#[tokio::test]
async fn commit_mutation_result_returns_persisted_version() {
    let storage = Arc::new(RwLock::new(DatastoreStorage::default()));
    let initial_entity = Entity {
        key: Some(key("versioned")),
        properties: HashMap::from([(
            "tag".to_string(),
            val(ValueType::StringValue("initial".to_string())),
        )]),
    };
    let insert_req = CommitRequest {
        project_id: PROJECT.to_string(),
        mode: Mode::NonTransactional as i32,
        mutations: vec![Mutation {
            operation: Some(Operation::Insert(initial_entity)),
            ..Default::default()
        }],
        ..Default::default()
    };
    let insert_resp = core::commit(&storage, insert_req).await.expect("insert");
    assert_eq!(insert_resp.mutation_results[0].version, 1);

    let updated_entity = Entity {
        key: Some(key("versioned")),
        properties: HashMap::from([(
            "tag".to_string(),
            val(ValueType::StringValue("updated".to_string())),
        )]),
    };
    let update_req = CommitRequest {
        project_id: PROJECT.to_string(),
        mode: Mode::NonTransactional as i32,
        mutations: vec![Mutation {
            operation: Some(Operation::Update(updated_entity)),
            ..Default::default()
        }],
        ..Default::default()
    };
    let update_resp = core::commit(&storage, update_req).await.expect("update");
    assert_eq!(update_resp.mutation_results[0].version, 2);
}

#[tokio::test]
async fn update_missing_entity_returns_not_found() {
    let storage = Arc::new(RwLock::new(DatastoreStorage::default()));
    let updated_entity = Entity {
        key: Some(key("missing")),
        properties: HashMap::from([(
            "tag".to_string(),
            val(ValueType::StringValue("new".to_string())),
        )]),
    };
    let commit_req = CommitRequest {
        project_id: PROJECT.to_string(),
        mode: Mode::NonTransactional as i32,
        mutations: vec![Mutation {
            operation: Some(Operation::Update(updated_entity)),
            ..Default::default()
        }],
        ..Default::default()
    };

    let err = core::commit(&storage, commit_req)
        .await
        .expect_err("updating a missing entity must fail");
    assert_eq!(err.code(), Code::NotFound);
    assert_eq!(err.message(), "Entity not found for update");
}

#[tokio::test]
async fn insert_existing_key_returns_already_exists() {
    let storage = Arc::new(RwLock::new(DatastoreStorage::default()));
    let first = Entity {
        key: Some(key("dup")),
        properties: HashMap::from([(
            "tag".to_string(),
            val(ValueType::StringValue("first".to_string())),
        )]),
    };
    let first_req = CommitRequest {
        project_id: PROJECT.to_string(),
        mode: Mode::NonTransactional as i32,
        mutations: vec![Mutation {
            operation: Some(Operation::Insert(first)),
            ..Default::default()
        }],
        ..Default::default()
    };
    core::commit(&storage, first_req)
        .await
        .expect("first insert");

    let second = Entity {
        key: Some(key("dup")),
        properties: HashMap::from([(
            "tag".to_string(),
            val(ValueType::StringValue("second".to_string())),
        )]),
    };
    let second_req = CommitRequest {
        project_id: PROJECT.to_string(),
        mode: Mode::NonTransactional as i32,
        mutations: vec![Mutation {
            operation: Some(Operation::Insert(second)),
            ..Default::default()
        }],
        ..Default::default()
    };
    let err = core::commit(&storage, second_req)
        .await
        .expect_err("inserting an existing key must fail");
    assert_eq!(err.code(), Code::AlreadyExists);

    let guard = storage.read().await;
    let stored = guard.get_entity(&key("dup")).expect("entity still stored");
    assert_eq!(
        stored.entity.properties.get("tag"),
        Some(&val(ValueType::StringValue("first".to_string())))
    );
}

#[tokio::test]
async fn insert_incomplete_key_twice_allocates_two_entities() {
    let storage = Arc::new(RwLock::new(DatastoreStorage::default()));
    let incomplete_key = Key {
        partition_id: Some(PartitionId {
            project_id: PROJECT.to_string(),
            database_id: String::new(),
            namespace_id: String::new(),
        }),
        path: vec![PathElement {
            kind: KIND.to_string(),
            id_type: None,
        }],
    };
    let mut allocated = Vec::new();
    for _ in 0..2 {
        let req = CommitRequest {
            project_id: PROJECT.to_string(),
            mode: Mode::NonTransactional as i32,
            mutations: vec![Mutation {
                operation: Some(Operation::Insert(Entity {
                    key: Some(incomplete_key.clone()),
                    properties: HashMap::new(),
                })),
                ..Default::default()
            }],
            ..Default::default()
        };
        let resp = core::commit(&storage, req)
            .await
            .expect("insert incomplete key");
        allocated.push(resp.mutation_results[0].key.clone().expect("allocated key"));
    }
    assert_eq!(allocated.len(), 2);
    assert_ne!(allocated[0], allocated[1]);
}

/// Bug 6: equality filters must match only the value's own type. Int `1`,
/// double `1.0` and string `"1"` must not collide in the property index.
#[tokio::test]
async fn equality_filter_does_not_match_other_numeric_types() {
    let mut storage = DatastoreStorage::default();
    insert(
        &mut storage,
        "i1",
        HashMap::from([("score".to_string(), val(ValueType::IntegerValue(1)))]),
    );
    insert(
        &mut storage,
        "d1",
        HashMap::from([("score".to_string(), val(ValueType::DoubleValue(1.0)))]),
    );
    insert(
        &mut storage,
        "s1",
        HashMap::from([(
            "score".to_string(),
            val(ValueType::StringValue("1".to_string())),
        )]),
    );
    insert(
        &mut storage,
        "i2",
        HashMap::from([("score".to_string(), val(ValueType::IntegerValue(2)))]),
    );
    let storage = Arc::new(RwLock::new(storage));

    let equal_to = |value: ValueType| Query {
        kind: vec![datastore_emulator::google::datastore::v1::KindExpression {
            name: KIND.to_string(),
        }],
        filter: Some(prop_filter(
            "score",
            property_filter::Operator::Equal,
            val(value),
        )),
        ..Default::default()
    };

    let names = run(storage.clone(), equal_to(ValueType::IntegerValue(1))).await;
    assert_eq!(names, vec!["i1"], "score = 1 must match only the integer");

    let names = run(storage.clone(), equal_to(ValueType::DoubleValue(1.0))).await;
    assert_eq!(names, vec!["d1"], "score = 1.0 must match only the double");

    let names = run(storage, equal_to(ValueType::StringValue("1".to_string()))).await;
    assert_eq!(
        names,
        vec!["s1"],
        "score = \"1\" must match only the string"
    );
}

/// Bug 6: a boolean `true` must not match the string `"true"`.
#[tokio::test]
async fn equality_filter_bool_does_not_match_string() {
    let mut storage = DatastoreStorage::default();
    insert(
        &mut storage,
        "b",
        HashMap::from([("flag".to_string(), val(ValueType::BooleanValue(true)))]),
    );
    insert(
        &mut storage,
        "s",
        HashMap::from([(
            "flag".to_string(),
            val(ValueType::StringValue("true".to_string())),
        )]),
    );
    let storage = Arc::new(RwLock::new(storage));

    let equal_to = |value: ValueType| Query {
        kind: vec![datastore_emulator::google::datastore::v1::KindExpression {
            name: KIND.to_string(),
        }],
        filter: Some(prop_filter(
            "flag",
            property_filter::Operator::Equal,
            val(value),
        )),
        ..Default::default()
    };

    let names = run(storage.clone(), equal_to(ValueType::BooleanValue(true))).await;
    assert_eq!(names, vec!["b"], "flag = true must match only the boolean");

    let names = run(
        storage,
        equal_to(ValueType::StringValue("true".to_string())),
    )
    .await;
    assert_eq!(
        names,
        vec!["s"],
        "flag = \"true\" must match only the string"
    );
}

/// Bug 6: `IN` must be type-strict like `=`.
#[tokio::test]
async fn in_filter_is_type_strict() {
    let mut storage = DatastoreStorage::default();
    insert(
        &mut storage,
        "i1",
        HashMap::from([("score".to_string(), val(ValueType::IntegerValue(1)))]),
    );
    insert(
        &mut storage,
        "d1",
        HashMap::from([("score".to_string(), val(ValueType::DoubleValue(1.0)))]),
    );
    insert(
        &mut storage,
        "s1",
        HashMap::from([(
            "score".to_string(),
            val(ValueType::StringValue("1".to_string())),
        )]),
    );
    insert(
        &mut storage,
        "i2",
        HashMap::from([("score".to_string(), val(ValueType::IntegerValue(2)))]),
    );
    let storage = Arc::new(RwLock::new(storage));

    let query = Query {
        kind: vec![datastore_emulator::google::datastore::v1::KindExpression {
            name: KIND.to_string(),
        }],
        filter: Some(prop_filter(
            "score",
            property_filter::Operator::In,
            val_array(vec![
                val(ValueType::DoubleValue(1.0)),
                val(ValueType::IntegerValue(2)),
            ]),
        )),
        ..Default::default()
    };

    let names = run(storage, query).await;
    assert_eq!(
        names,
        vec!["d1", "i2"],
        "score IN [1.0, 2] must match the double and the integer only"
    );
}

fn order_by_desc(prop: &str) -> PropertyOrder {
    PropertyOrder {
        property: Some(PropertyReference {
            name: prop.to_string(),
        }),
        direction: property_order::Direction::Descending as i32,
    }
}

fn kind_query(order: Vec<PropertyOrder>) -> Query {
    Query {
        kind: vec![datastore_emulator::google::datastore::v1::KindExpression {
            name: KIND.to_string(),
        }],
        order,
        ..Default::default()
    }
}

/// Order by a property is an index scan: entities without the property are excluded.
#[tokio::test]
async fn order_by_excludes_entities_without_the_property() {
    let mut storage = DatastoreStorage::default();
    insert(
        &mut storage,
        "a",
        HashMap::from([("score".to_string(), val(ValueType::IntegerValue(2)))]),
    );
    insert(
        &mut storage,
        "b",
        HashMap::from([("score".to_string(), val(ValueType::IntegerValue(1)))]),
    );
    insert(&mut storage, "c", HashMap::new());
    let storage = Arc::new(RwLock::new(storage));

    let names = run(storage.clone(), kind_query(vec![order_by_asc("score")])).await;
    assert_eq!(
        names,
        vec!["b", "a"],
        "ascending must skip entity without score"
    );

    let names = run(storage, kind_query(vec![order_by_desc("score")])).await;
    assert_eq!(
        names,
        vec!["a", "b"],
        "descending must skip entity without score"
    );
}

/// Excluded-from-indexes values are not in the index, so the entity is excluded from order results.
#[tokio::test]
async fn order_by_excludes_unindexed_property() {
    let mut storage = DatastoreStorage::default();
    insert(
        &mut storage,
        "a",
        HashMap::from([("score".to_string(), val(ValueType::IntegerValue(2)))]),
    );
    insert(
        &mut storage,
        "b",
        HashMap::from([(
            "score".to_string(),
            val_excluded(ValueType::IntegerValue(1)),
        )]),
    );
    let storage = Arc::new(RwLock::new(storage));

    let names = run(storage, kind_query(vec![order_by_asc("score")])).await;
    assert_eq!(
        names,
        vec!["a"],
        "excluded-from-indexes value must not be ordered"
    );
}

/// An explicit null is indexed and sorts first ascending; a missing property is excluded.
#[tokio::test]
async fn order_by_keeps_explicit_null_first() {
    let mut storage = DatastoreStorage::default();
    insert(
        &mut storage,
        "a",
        HashMap::from([("score".to_string(), val(ValueType::IntegerValue(2)))]),
    );
    insert(
        &mut storage,
        "b",
        HashMap::from([("score".to_string(), val(ValueType::NullValue(0)))]),
    );
    insert(&mut storage, "c", HashMap::new());
    let storage = Arc::new(RwLock::new(storage));

    let names = run(storage, kind_query(vec![order_by_asc("score")])).await;
    assert_eq!(
        names,
        vec!["b", "a"],
        "explicit null must be kept and sort first"
    );
}

/// With a limit, the lowest indexed entity must be returned, not the entity missing the property.
#[tokio::test]
async fn order_by_with_limit_returns_lowest_indexed_entity() {
    let mut storage = DatastoreStorage::default();
    insert(
        &mut storage,
        "a",
        HashMap::from([("score".to_string(), val(ValueType::IntegerValue(5)))]),
    );
    insert(&mut storage, "b", HashMap::new());
    let storage = Arc::new(RwLock::new(storage));

    let mut query = kind_query(vec![order_by_asc("score")]);
    query.limit = Some(pbjson_types::Int32Value { value: 1 });
    let names = run(storage, query).await;
    assert_eq!(
        names,
        vec!["a"],
        "limit 1 must return the entity that has the property"
    );
}

/// Every order property must be present and indexed, not just one of them.
#[tokio::test]
async fn order_by_two_properties_requires_both() {
    let mut storage = DatastoreStorage::default();
    insert(
        &mut storage,
        "a",
        HashMap::from([
            (
                "cat".to_string(),
                val(ValueType::StringValue("x".to_string())),
            ),
            ("score".to_string(), val(ValueType::IntegerValue(2))),
        ]),
    );
    insert(
        &mut storage,
        "b",
        HashMap::from([(
            "cat".to_string(),
            val(ValueType::StringValue("x".to_string())),
        )]),
    );
    insert(
        &mut storage,
        "c",
        HashMap::from([
            (
                "cat".to_string(),
                val(ValueType::StringValue("y".to_string())),
            ),
            ("score".to_string(), val(ValueType::IntegerValue(1))),
        ]),
    );
    let storage = Arc::new(RwLock::new(storage));

    let names = run(
        storage,
        kind_query(vec![order_by_asc("cat"), order_by_asc("score")]),
    )
    .await;
    assert_eq!(
        names,
        vec!["a", "c"],
        "entity missing the second order property must be excluded"
    );
}

/// Array properties match `!=` and `NOT_IN` when ANY element satisfies the predicate.
#[tokio::test]
async fn not_equal_on_array_matches_when_any_element_differs() {
    let mut storage = DatastoreStorage::default();
    let tags = |items: &[&str]| {
        val_array(
            items
                .iter()
                .map(|s| val(ValueType::StringValue(s.to_string())))
                .collect(),
        )
    };
    insert(
        &mut storage,
        "ab",
        HashMap::from([("tags".to_string(), tags(&["a", "b"]))]),
    );
    insert(
        &mut storage,
        "a",
        HashMap::from([("tags".to_string(), tags(&["a"]))]),
    );
    insert(
        &mut storage,
        "c",
        HashMap::from([("tags".to_string(), tags(&["c"]))]),
    );
    insert(
        &mut storage,
        "empty",
        HashMap::from([("tags".to_string(), tags(&[]))]),
    );
    insert(&mut storage, "missing", HashMap::new());
    let storage = Arc::new(RwLock::new(storage));

    let query = Query {
        filter: Some(prop_filter(
            "tags",
            property_filter::Operator::NotEqual,
            val(ValueType::StringValue("a".to_string())),
        )),
        ..kind_query(vec![order_by_asc("__key__")])
    };
    let names = run(storage, query).await;
    assert_eq!(names, vec!["ab", "c"]);
}

#[tokio::test]
async fn not_in_on_array_matches_when_any_element_is_outside_the_list() {
    let mut storage = DatastoreStorage::default();
    let tags = |items: &[&str]| {
        val_array(
            items
                .iter()
                .map(|s| val(ValueType::StringValue(s.to_string())))
                .collect(),
        )
    };
    insert(
        &mut storage,
        "ab",
        HashMap::from([("tags".to_string(), tags(&["a", "b"]))]),
    );
    insert(
        &mut storage,
        "a",
        HashMap::from([("tags".to_string(), tags(&["a"]))]),
    );
    insert(
        &mut storage,
        "c",
        HashMap::from([("tags".to_string(), tags(&["c"]))]),
    );
    insert(
        &mut storage,
        "empty",
        HashMap::from([("tags".to_string(), tags(&[]))]),
    );
    insert(&mut storage, "missing", HashMap::new());
    let storage = Arc::new(RwLock::new(storage));

    let list = |items: &[&str]| {
        val_array(
            items
                .iter()
                .map(|s| val(ValueType::StringValue(s.to_string())))
                .collect(),
        )
    };
    let run_with = |op: property_filter::Operator, value: Value| {
        let storage = storage.clone();
        async move {
            let query = Query {
                filter: Some(prop_filter("tags", op, value)),
                ..kind_query(vec![order_by_asc("__key__")])
            };
            run(storage, query).await
        }
    };

    assert_eq!(
        run_with(property_filter::Operator::NotIn, list(&["a"])).await,
        vec!["ab", "c"],
        "NOT_IN [a]"
    );
    assert_eq!(
        run_with(property_filter::Operator::NotIn, list(&["a", "b"])).await,
        vec!["c"],
        "NOT_IN [a, b]"
    );
    assert_eq!(
        run_with(property_filter::Operator::In, list(&["b", "z"])).await,
        vec!["ab"],
        "IN [b, z]"
    );
}

#[tokio::test]
async fn not_equal_and_not_in_on_scalars_keep_null_and_drop_missing() {
    let mut storage = DatastoreStorage::default();
    insert(
        &mut storage,
        "i1",
        HashMap::from([("score".to_string(), val(ValueType::IntegerValue(1)))]),
    );
    insert(
        &mut storage,
        "i2",
        HashMap::from([("score".to_string(), val(ValueType::IntegerValue(2)))]),
    );
    insert(
        &mut storage,
        "none",
        HashMap::from([("score".to_string(), val(ValueType::NullValue(0)))]),
    );
    insert(&mut storage, "missing", HashMap::new());
    let storage = Arc::new(RwLock::new(storage));

    let run_with = |op: property_filter::Operator, value: Value| {
        let storage = storage.clone();
        async move {
            let query = Query {
                filter: Some(prop_filter("score", op, value)),
                ..kind_query(vec![order_by_asc("__key__")])
            };
            run(storage, query).await
        }
    };

    assert_eq!(
        run_with(
            property_filter::Operator::NotEqual,
            val(ValueType::IntegerValue(1))
        )
        .await,
        vec!["i2", "none"],
        "score != 1"
    );
    assert_eq!(
        run_with(
            property_filter::Operator::NotEqual,
            val(ValueType::NullValue(0))
        )
        .await,
        vec!["i1", "i2"],
        "score != None"
    );
    assert_eq!(
        run_with(
            property_filter::Operator::NotIn,
            val_array(vec![
                val(ValueType::IntegerValue(1)),
                val(ValueType::IntegerValue(2)),
            ]),
        )
        .await,
        vec!["none"],
        "score NOT_IN [1, 2]"
    );
}

fn kind_query_with(filter: Option<Filter>, order: Vec<PropertyOrder>) -> Query {
    Query {
        kind: vec![datastore_emulator::google::datastore::v1::KindExpression {
            name: KIND.to_string(),
        }],
        filter,
        order,
        ..Default::default()
    }
}

#[tokio::test]
async fn cross_type_ordering_puts_doubles_after_strings() {
    let mut storage = DatastoreStorage::default();
    insert(
        &mut storage,
        "null",
        HashMap::from([("score".to_string(), val(ValueType::NullValue(0)))]),
    );
    insert(
        &mut storage,
        "i1",
        HashMap::from([("score".to_string(), val(ValueType::IntegerValue(1)))]),
    );
    insert(
        &mut storage,
        "i2",
        HashMap::from([("score".to_string(), val(ValueType::IntegerValue(2)))]),
    );
    insert(
        &mut storage,
        "ts",
        HashMap::from([(
            "score".to_string(),
            val(ValueType::TimestampValue(pbjson_types::Timestamp {
                seconds: 1_577_836_800,
                nanos: 0,
            })),
        )]),
    );
    insert(
        &mut storage,
        "b",
        HashMap::from([("score".to_string(), val(ValueType::BooleanValue(true)))]),
    );
    insert(
        &mut storage,
        "s",
        HashMap::from([(
            "score".to_string(),
            val(ValueType::StringValue("1".to_string())),
        )]),
    );
    insert(
        &mut storage,
        "d1",
        HashMap::from([("score".to_string(), val(ValueType::DoubleValue(1.0)))]),
    );
    let storage = Arc::new(RwLock::new(storage));

    let asc = run(
        storage.clone(),
        kind_query_with(None, vec![order_by_asc("score")]),
    )
    .await;
    assert_eq!(
        asc,
        vec!["null", "i1", "i2", "ts", "b", "s", "d1"],
        "ascending: every double must sort after every string"
    );

    let desc = run(storage, kind_query_with(None, vec![order_by_desc("score")])).await;
    assert_eq!(
        desc,
        vec!["d1", "s", "b", "ts", "i2", "i1", "null"],
        "descending: reverse of the ascending order"
    );
}

#[tokio::test]
async fn inequality_filters_never_compare_int_with_double() {
    let mut storage = DatastoreStorage::default();
    insert(
        &mut storage,
        "null",
        HashMap::from([("score".to_string(), val(ValueType::NullValue(0)))]),
    );
    insert(
        &mut storage,
        "i1",
        HashMap::from([("score".to_string(), val(ValueType::IntegerValue(1)))]),
    );
    insert(
        &mut storage,
        "i2",
        HashMap::from([("score".to_string(), val(ValueType::IntegerValue(2)))]),
    );
    insert(
        &mut storage,
        "ts",
        HashMap::from([(
            "score".to_string(),
            val(ValueType::TimestampValue(pbjson_types::Timestamp {
                seconds: 1_577_836_800,
                nanos: 0,
            })),
        )]),
    );
    insert(
        &mut storage,
        "b",
        HashMap::from([("score".to_string(), val(ValueType::BooleanValue(true)))]),
    );
    insert(
        &mut storage,
        "s",
        HashMap::from([(
            "score".to_string(),
            val(ValueType::StringValue("1".to_string())),
        )]),
    );
    insert(
        &mut storage,
        "d1",
        HashMap::from([("score".to_string(), val(ValueType::DoubleValue(1.0)))]),
    );
    let storage = Arc::new(RwLock::new(storage));

    let filter = |op, value| {
        kind_query_with(
            Some(prop_filter("score", op, value)),
            vec![order_by_asc("score")],
        )
    };

    let mut gt_half = run(
        storage.clone(),
        filter(
            property_filter::Operator::GreaterThan,
            val(ValueType::DoubleValue(0.5)),
        ),
    )
    .await;
    gt_half.sort();
    assert_eq!(
        gt_half,
        vec!["d1"],
        "score > 0.5 must only match the double"
    );

    let mut ge_int = run(
        storage.clone(),
        filter(
            property_filter::Operator::GreaterThanOrEqual,
            val(ValueType::IntegerValue(1)),
        ),
    )
    .await;
    ge_int.sort();
    assert_eq!(
        ge_int,
        vec!["b", "d1", "i1", "i2", "s", "ts"],
        "score >= 1 must match everything except null"
    );

    let mut ge_double = run(
        storage.clone(),
        filter(
            property_filter::Operator::GreaterThanOrEqual,
            val(ValueType::DoubleValue(1.0)),
        ),
    )
    .await;
    ge_double.sort();
    assert_eq!(
        ge_double,
        vec!["d1"],
        "score >= 1.0 must only match the double"
    );

    let mut lt_int = run(
        storage,
        filter(
            property_filter::Operator::LessThan,
            val(ValueType::IntegerValue(2)),
        ),
    )
    .await;
    lt_int.sort();
    assert_eq!(
        lt_int,
        vec!["i1", "null"],
        "score < 2 must match null and int 1"
    );
}

#[tokio::test]
async fn integers_and_timestamps_compare_numerically() {
    let mut storage = DatastoreStorage::default();
    insert(
        &mut storage,
        "small",
        HashMap::from([("score".to_string(), val(ValueType::IntegerValue(5)))]),
    );
    // 1 second after the epoch is 1_000_000 microseconds.
    insert(
        &mut storage,
        "epoch1s",
        HashMap::from([(
            "score".to_string(),
            val(ValueType::TimestampValue(pbjson_types::Timestamp {
                seconds: 1,
                nanos: 0,
            })),
        )]),
    );
    insert(
        &mut storage,
        "big",
        HashMap::from([("score".to_string(), val(ValueType::IntegerValue(2_000_000)))]),
    );
    let storage = Arc::new(RwLock::new(storage));

    let asc = run(
        storage.clone(),
        kind_query_with(None, vec![order_by_asc("score")]),
    )
    .await;
    assert_eq!(
        asc,
        vec!["small", "epoch1s", "big"],
        "integers and timestamps must interleave by microseconds"
    );

    let gt = run(
        storage.clone(),
        kind_query_with(
            Some(prop_filter(
                "score",
                property_filter::Operator::GreaterThan,
                val(ValueType::IntegerValue(1_000_000)),
            )),
            vec![order_by_asc("score")],
        ),
    )
    .await;
    assert_eq!(gt, vec!["big"], "int 1_000_000 must not match timestamp 1s");

    let lt = run(
        storage,
        kind_query_with(
            Some(prop_filter(
                "score",
                property_filter::Operator::LessThan,
                val(ValueType::IntegerValue(1_000_000)),
            )),
            vec![order_by_asc("score")],
        ),
    )
    .await;
    assert_eq!(
        lt,
        vec!["small"],
        "int 1_000_000 must not match timestamp 1s"
    );
}
