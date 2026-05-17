//! Regression tests for query bugs reported in
//! https://github.com/guibeira/datastore-emulator/issues/26
//!
//! Each test reproduces one of the reported issues:
//!   1. Cross-type ordering must follow the documented Datastore order
//!      (null < number < timestamp < boolean < string < ...).
//!   2. An inequality filter without an explicit `order` should implicitly
//!      sort by the inequality property (ascending).
//!   3. Properties marked `exclude_from_indexes` must not match index-backed
//!      filters.
//!   4. The `offset` field on `Query` must be honored.
//!   5. Updating an entity must purge stale index entries for old values.

use datastore_emulator::{
    DatastoreStorage,
    core,
    database::{EntityWithMetadata, KeyId, KeyStruct},
    google::datastore::v1::{
        ArrayValue, CommitRequest, Entity, Filter, Key, Mutation, PartitionId, PropertyOrder,
        PropertyReference, Query, RunQueryRequest, Value,
        commit_request::Mode,
        filter::FilterType,
        key::{PathElement, path_element::IdType},
        mutation::Operation,
        property_filter,
        property_order,
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

fn key_struct_for(name: &str) -> KeyStruct {
    KeyStruct {
        project_id: PROJECT.to_string(),
        namespace: String::new(),
        path_elements: vec![(KIND.to_string(), KeyId::StringId(name.to_string()))],
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
    let key = key(name);
    let key_struct = key_struct_for(name);
    let entity = Entity {
        key: Some(key.clone()),
        properties: props,
    };
    storage.entities.insert(
        key_struct.clone(),
        EntityWithMetadata {
            entity: entity.clone(),
            version: 1,
            create_time: pbjson_types::Timestamp::default(),
            update_time: pbjson_types::Timestamp::default(),
        },
    );
    storage.update_indexes(&key_struct, &entity);
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
/// null < integer/double < timestamp < boolean < string < blob.
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
    assert_eq!(
        names,
        vec!["null", "int", "ts", "bool", "str"],
        "cross-type ordering mismatch"
    );
}

#[tokio::test]
async fn int_double_comparison_preserves_large_integer_precision() {
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
        vec!["int_above"],
        "integer 2^53 + 1 must compare greater than double 2^53"
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
        vec!["double_threshold"],
        "double 2^53 must compare less than integer 2^53 + 1"
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
        vec!["all_excluded", "array_indexed", "scalar"],
        "sort order must ignore excluded array members and treat arrays with no indexed members as missing"
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
    assert_eq!(names, vec!["c", "d", "e"], "offset must skip first 2 results");
    assert_eq!(
        batch.skipped_results, 2,
        "skipped_results must report the offset"
    );
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
    let stale = storage_guard.indexes.get(&(
        KIND.to_string(),
        "tag".to_string(),
        "old".to_string(),
    ));
    let stale_keys: Vec<&KeyStruct> =
        stale.map(|set| set.iter().collect()).unwrap_or_default();
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
