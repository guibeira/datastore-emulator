//! Validation rules for Commit, Rollback and AllocateIds, matching the
//! behaviour of the real Cloud Datastore.

use datastore_emulator::{
    DatastoreStorage, core,
    google::datastore::v1::{
        AllocateIdsRequest, ArrayValue, BeginTransactionRequest, CommitRequest, Entity, Key,
        Mutation, PartitionId, RollbackRequest, Value,
        commit_request::{Mode, TransactionSelector},
        key::{PathElement, path_element::IdType},
        mutation::Operation,
        value::ValueType,
    },
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::Code;

const PROJECT: &str = "p1";
const KIND: &str = "Item";

type Storage = Arc<RwLock<DatastoreStorage>>;

fn new_storage() -> Storage {
    Arc::new(RwLock::new(DatastoreStorage::default()))
}

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

fn string_val(s: &str) -> Value {
    val(ValueType::StringValue(s.to_string()))
}

fn int_val(i: i64) -> Value {
    val(ValueType::IntegerValue(i))
}

fn entity(key: Key, props: Vec<(&str, Value)>) -> Entity {
    Entity {
        key: Some(key),
        properties: props
            .into_iter()
            .map(|(name, value)| (name.to_string(), value))
            .collect::<HashMap<_, _>>(),
    }
}

fn upsert(entity: Entity) -> Mutation {
    Mutation {
        operation: Some(Operation::Upsert(entity)),
        ..Default::default()
    }
}

fn insert(entity: Entity) -> Mutation {
    Mutation {
        operation: Some(Operation::Insert(entity)),
        ..Default::default()
    }
}

fn delete(key: Key) -> Mutation {
    Mutation {
        operation: Some(Operation::Delete(key)),
        ..Default::default()
    }
}

fn update(entity: Entity) -> Mutation {
    Mutation {
        operation: Some(Operation::Update(entity)),
        ..Default::default()
    }
}

async fn commit_non_tx(storage: &Storage, mutations: Vec<Mutation>) -> Result<(), tonic::Status> {
    core::commit(storage, commit_req(Mode::NonTransactional, None, mutations))
        .await
        .map(|_| ())
}

async fn commit_tx(storage: &Storage, mutations: Vec<Mutation>) -> Result<(), tonic::Status> {
    let tx = begin(storage).await;
    core::commit(
        storage,
        commit_req(
            Mode::Transactional,
            Some(TransactionSelector::Transaction(tx)),
            mutations,
        ),
    )
    .await
    .map(|_| ())
}

async fn stored_value(storage: &Storage, k: &Key, prop: &str) -> Option<Value> {
    storage
        .read()
        .await
        .get_entity(k)
        .and_then(|e| e.entity.properties.get(prop).cloned())
}

/// Transactional commits are all-or-nothing: a failing precondition must
/// leave earlier mutations of the same request unapplied.
#[tokio::test]
async fn transactional_commit_applies_nothing_when_insert_hits_existing_key() {
    let storage = new_storage();
    let existing = key("existing");
    commit_non_tx(&storage, vec![upsert(entity(existing.clone(), vec![]))])
        .await
        .expect("seed");

    let a = key("a");
    let err = commit_tx(
        &storage,
        vec![
            upsert(entity(a.clone(), vec![("v", int_val(1))])),
            insert(entity(existing.clone(), vec![])),
        ],
    )
    .await
    .expect_err("insert of an existing key must fail");
    assert_eq!(err.code(), Code::AlreadyExists);
    assert!(
        storage.read().await.get_entity(&a).is_none(),
        "earlier upsert must not be applied"
    );
}

#[tokio::test]
async fn transactional_commit_applies_nothing_when_update_misses() {
    let storage = new_storage();
    let a = key("a");
    let err = commit_tx(
        &storage,
        vec![
            upsert(entity(a.clone(), vec![("v", int_val(1))])),
            update(entity(key("missing"), vec![])),
        ],
    )
    .await
    .expect_err("update of a missing key must fail");
    assert_eq!(err.code(), Code::NotFound);
    assert!(
        storage.read().await.get_entity(&a).is_none(),
        "earlier upsert must not be applied"
    );
}

/// Inside one transaction a key may be inserted and then updated, or deleted
/// and then inserted again.
#[tokio::test]
async fn transactional_commit_allows_insert_then_update_and_delete_then_insert() {
    let storage = new_storage();
    let k = key("k");
    commit_tx(
        &storage,
        vec![
            insert(entity(k.clone(), vec![("v", int_val(1))])),
            update(entity(k.clone(), vec![("v", int_val(2))])),
        ],
    )
    .await
    .expect("insert then update");
    assert_eq!(stored_value(&storage, &k, "v").await, Some(int_val(2)));

    commit_tx(
        &storage,
        vec![
            delete(k.clone()),
            insert(entity(k.clone(), vec![("v", int_val(3))])),
        ],
    )
    .await
    .expect("delete then insert");
    assert_eq!(stored_value(&storage, &k, "v").await, Some(int_val(3)));
}

/// Non-transactional commits are not atomic in Datastore ("the mutations may
/// not apply as all or none"), so earlier mutations stay applied.
#[tokio::test]
async fn non_transactional_commit_keeps_earlier_mutations_on_failure() {
    let storage = new_storage();
    let a = key("a");
    let err = commit_non_tx(
        &storage,
        vec![
            upsert(entity(a.clone(), vec![("v", int_val(1))])),
            update(entity(key("missing"), vec![])),
        ],
    )
    .await
    .expect_err("update of a missing key must fail");
    assert_eq!(err.code(), Code::NotFound);
    assert_eq!(stored_value(&storage, &a, "v").await, Some(int_val(1)));
}

fn commit_req(
    mode: Mode,
    selector: Option<TransactionSelector>,
    mutations: Vec<Mutation>,
) -> CommitRequest {
    CommitRequest {
        project_id: PROJECT.to_string(),
        mode: mode as i32,
        transaction_selector: selector,
        mutations,
        ..Default::default()
    }
}

async fn begin(storage: &Storage) -> Vec<u8> {
    core::begin_transaction(
        storage,
        BeginTransactionRequest {
            project_id: PROJECT.to_string(),
            ..Default::default()
        },
    )
    .await
    .expect("begin transaction")
    .transaction
}

fn assert_invalid(err: &tonic::Status, message: &str) {
    assert_eq!(
        err.code(),
        Code::InvalidArgument,
        "unexpected error: {err:?}"
    );
    assert_eq!(err.message(), message);
}

#[tokio::test]
async fn non_transactional_duplicate_key_is_rejected_before_apply() {
    let storage = new_storage();
    let k = key("dup");
    let req = commit_req(
        Mode::NonTransactional,
        None,
        vec![
            upsert(entity(k.clone(), vec![("v", int_val(1))])),
            upsert(entity(k.clone(), vec![("v", int_val(2))])),
        ],
    );
    let err = core::commit(&storage, req).await.unwrap_err();
    assert_invalid(
        &err,
        "A non-transactional commit may not contain multiple mutations affecting the same entity.",
    );
    assert!(storage.read().await.get_entity(&k).is_none());

    let storage = new_storage();
    let req = commit_req(
        Mode::NonTransactional,
        None,
        vec![
            insert(entity(k.clone(), vec![("v", int_val(1))])),
            delete(k.clone()),
        ],
    );
    let err = core::commit(&storage, req).await.unwrap_err();
    assert_invalid(
        &err,
        "A non-transactional commit may not contain multiple mutations affecting the same entity.",
    );
    assert!(storage.read().await.get_entity(&k).is_none());
}

#[tokio::test]
async fn transactional_duplicate_key_is_allowed() {
    let storage = new_storage();
    let tx = begin(&storage).await;
    let k = key("dup");
    let req = commit_req(
        Mode::Transactional,
        Some(TransactionSelector::Transaction(tx)),
        vec![
            upsert(entity(k.clone(), vec![("v", int_val(1))])),
            upsert(entity(k.clone(), vec![("v", int_val(2))])),
        ],
    );
    core::commit(&storage, req).await.expect("commit");

    let stored = storage.read().await.get_entity(&k).expect("stored");
    assert_eq!(
        stored.entity.properties["v"],
        int_val(2),
        "later mutation must win"
    );
}

#[tokio::test]
async fn mode_and_selector_must_agree() {
    let storage = new_storage();
    let tx = begin(&storage).await;

    let req = commit_req(
        Mode::Transactional,
        None,
        vec![upsert(entity(key("a"), vec![]))],
    );
    let err = core::commit(&storage, req).await.unwrap_err();
    assert_invalid(&err, "transactional commit requires a transaction");

    let req = commit_req(
        Mode::NonTransactional,
        Some(TransactionSelector::Transaction(tx)),
        vec![upsert(entity(key("a"), vec![]))],
    );
    let err = core::commit(&storage, req).await.unwrap_err();
    assert_invalid(&err, "non-transaction commit cannot specify a transaction");
}

#[tokio::test]
async fn unknown_transaction_is_invalid_argument() {
    let storage = new_storage();
    let req = commit_req(
        Mode::Transactional,
        Some(TransactionSelector::Transaction(b"nope".to_vec())),
        vec![upsert(entity(key("a"), vec![]))],
    );
    let err = core::commit(&storage, req).await.unwrap_err();
    assert_invalid(&err, "Invalid transaction.");

    let err = core::rollback(
        &storage,
        RollbackRequest {
            project_id: PROJECT.to_string(),
            transaction: b"nope".to_vec(),
            ..Default::default()
        },
    )
    .await
    .unwrap_err();
    assert_invalid(&err, "Invalid transaction.");
}

#[tokio::test]
async fn empty_key_path_and_empty_kind_are_rejected() {
    let storage = new_storage();

    let empty_path = Key {
        partition_id: key("x").partition_id,
        path: vec![],
    };
    let req = commit_req(
        Mode::NonTransactional,
        None,
        vec![upsert(entity(empty_path.clone(), vec![]))],
    );
    let err = core::commit(&storage, req).await.unwrap_err();
    assert_invalid(&err, "Key path is empty.");

    let req = commit_req(Mode::NonTransactional, None, vec![delete(empty_path)]);
    let err = core::commit(&storage, req).await.unwrap_err();
    assert_invalid(&err, "Key path is empty.");

    let empty_kind = Key {
        partition_id: key("x").partition_id,
        path: vec![PathElement {
            kind: String::new(),
            id_type: Some(IdType::Name("x".to_string())),
        }],
    };
    let req = commit_req(
        Mode::NonTransactional,
        None,
        vec![upsert(entity(empty_kind.clone(), vec![]))],
    );
    let err = core::commit(&storage, req).await.unwrap_err();
    assert_invalid(&err, "The kind is the empty string.");

    let req = commit_req(Mode::NonTransactional, None, vec![delete(empty_kind)]);
    let err = core::commit(&storage, req).await.unwrap_err();
    assert_invalid(&err, "The kind is the empty string.");
}

#[tokio::test]
async fn reserved_property_names_are_rejected_at_any_depth() {
    let storage = new_storage();
    let reserved = |name: &str| format!("The property.name \"{name}\" is reserved.");

    let req = commit_req(
        Mode::NonTransactional,
        None,
        vec![upsert(entity(key("a"), vec![("__foo__", string_val("x"))]))],
    );
    let err = core::commit(&storage, req).await.unwrap_err();
    assert_invalid(&err, &reserved("__foo__"));

    let nested = val(ValueType::EntityValue(entity(
        key("inner"),
        vec![("__bad__", string_val("x"))],
    )));
    let req = commit_req(
        Mode::NonTransactional,
        None,
        vec![upsert(entity(key("b"), vec![("child", nested)]))],
    );
    let err = core::commit(&storage, req).await.unwrap_err();
    assert_invalid(&err, &reserved("__bad__"));

    let in_array = val(ValueType::ArrayValue(ArrayValue {
        values: vec![val(ValueType::EntityValue(entity(
            key("inner"),
            vec![("__arr__", string_val("x"))],
        )))],
    }));
    let req = commit_req(
        Mode::NonTransactional,
        None,
        vec![insert(entity(key("c"), vec![("list", in_array)]))],
    );
    let err = core::commit(&storage, req).await.unwrap_err();
    assert_invalid(&err, &reserved("__arr__"));

    let req = commit_req(
        Mode::NonTransactional,
        None,
        vec![upsert(entity(
            key("ok"),
            vec![("__foo", string_val("x")), ("foo__", string_val("y"))],
        ))],
    );
    core::commit(&storage, req)
        .await
        .expect("__foo and foo__ are allowed");
}

#[tokio::test]
async fn allocate_ids_validates_path_shape() {
    let storage = new_storage();
    let complete = Key {
        partition_id: key("x").partition_id,
        path: vec![PathElement {
            kind: "Kind".to_string(),
            id_type: Some(IdType::Name("name".to_string())),
        }],
    };
    let err = core::allocate_ids(
        &storage,
        AllocateIdsRequest {
            project_id: PROJECT.to_string(),
            keys: vec![complete],
            ..Default::default()
        },
    )
    .await
    .unwrap_err();
    assert_invalid(&err, "Key path element must not be complete: [Kind: name]");

    let parent_incomplete = Key {
        partition_id: key("x").partition_id,
        path: vec![
            PathElement {
                kind: "Parent".to_string(),
                id_type: None,
            },
            PathElement {
                kind: "Child".to_string(),
                id_type: None,
            },
        ],
    };
    let err = core::allocate_ids(
        &storage,
        AllocateIdsRequest {
            project_id: PROJECT.to_string(),
            keys: vec![parent_incomplete],
            ..Default::default()
        },
    )
    .await
    .unwrap_err();
    assert_invalid(&err, "Key path element must not be incomplete: [Parent]");

    let parent_complete = Key {
        partition_id: key("x").partition_id,
        path: vec![
            PathElement {
                kind: "Parent".to_string(),
                id_type: Some(IdType::Id(1)),
            },
            PathElement {
                kind: "Child".to_string(),
                id_type: None,
            },
        ],
    };
    let resp = core::allocate_ids(
        &storage,
        AllocateIdsRequest {
            project_id: PROJECT.to_string(),
            keys: vec![parent_complete],
            ..Default::default()
        },
    )
    .await
    .expect("allocate with complete parent");
    let last = &resp.keys[0].path[1];
    assert!(
        matches!(last.id_type, Some(IdType::Id(_))),
        "last element should be allocated, got {:?}",
        last.id_type
    );
}

#[tokio::test]
async fn delete_returns_non_zero_version() {
    let storage = new_storage();
    let k = key("gone");
    let req = commit_req(
        Mode::NonTransactional,
        None,
        vec![insert(entity(k.clone(), vec![]))],
    );
    core::commit(&storage, req).await.expect("insert");

    let req = commit_req(Mode::NonTransactional, None, vec![delete(k.clone())]);
    let resp = core::commit(&storage, req).await.expect("delete");
    assert_eq!(resp.mutation_results[0].version, 2);

    let req = commit_req(Mode::NonTransactional, None, vec![delete(k)]);
    let resp = core::commit(&storage, req).await.expect("delete missing");
    assert_eq!(resp.mutation_results[0].version, 1);
}
