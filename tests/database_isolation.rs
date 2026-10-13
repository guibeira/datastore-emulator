//! Requests name a database, and every key or partition inside them must
//! name the same one, like Cloud Datastore. Entities of one database are
//! invisible from another.

use datastore_emulator::{
    DatastoreStorage, core,
    google::datastore::v1::{
        AllocateIdsRequest, CommitRequest, Entity, Key, KindExpression, LookupRequest, Mutation,
        PartitionId, Query, RunQueryRequest,
        commit_request::Mode,
        key::{PathElement, path_element::IdType},
        mutation::Operation,
        run_query_request::QueryType,
    },
};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use tonic::Code;

const PROJECT: &str = "p1";
const KIND: &str = "Item";

fn partition(database_id: &str) -> PartitionId {
    PartitionId {
        project_id: PROJECT.to_string(),
        database_id: database_id.to_string(),
        namespace_id: String::new(),
    }
}

fn key(database_id: &str, name: &str) -> Key {
    Key {
        partition_id: Some(partition(database_id)),
        path: vec![PathElement {
            kind: KIND.to_string(),
            id_type: Some(IdType::Name(name.to_string())),
        }],
    }
}

fn entity(database_id: &str, name: &str) -> Entity {
    Entity {
        key: Some(key(database_id, name)),
        properties: HashMap::new(),
    }
}

fn storage() -> Arc<RwLock<DatastoreStorage>> {
    Arc::new(RwLock::new(DatastoreStorage::default()))
}

fn lookup_request(database_id: &str, keys: Vec<Key>) -> LookupRequest {
    LookupRequest {
        project_id: PROJECT.to_string(),
        database_id: database_id.to_string(),
        keys,
        ..Default::default()
    }
}

fn query_request(database_id: &str, partition_database_id: &str) -> RunQueryRequest {
    RunQueryRequest {
        project_id: PROJECT.to_string(),
        database_id: database_id.to_string(),
        partition_id: Some(partition(partition_database_id)),
        query_type: Some(QueryType::Query(Query {
            kind: vec![KindExpression {
                name: KIND.to_string(),
            }],
            ..Default::default()
        })),
        ..Default::default()
    }
}

fn upsert_request(database_id: &str, entities: Vec<Entity>) -> CommitRequest {
    CommitRequest {
        project_id: PROJECT.to_string(),
        database_id: database_id.to_string(),
        mode: Mode::NonTransactional as i32,
        mutations: entities
            .into_iter()
            .map(|e| Mutation {
                operation: Some(Operation::Upsert(e)),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

#[tokio::test]
async fn lookup_rejects_a_key_from_another_database() {
    let storage = storage();
    let err = core::lookup(&storage, lookup_request("db1", vec![key("", "one")]))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);
    assert!(
        err.message().contains("mismatched databases"),
        "{}",
        err.message()
    );
}

#[tokio::test]
async fn run_query_rejects_a_partition_from_another_database() {
    let storage = storage();
    let err = core::run_query(&storage, query_request("db1", ""))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);
}

#[tokio::test]
async fn commit_rejects_a_mutation_from_another_database() {
    let storage = storage();
    let err = core::commit(&storage, upsert_request("db1", vec![entity("", "one")]))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);
    assert!(storage.read().entities.is_empty());
}

#[tokio::test]
async fn allocate_ids_rejects_a_key_from_another_database() {
    let storage = storage();
    let req = AllocateIdsRequest {
        project_id: PROJECT.to_string(),
        database_id: "db1".to_string(),
        keys: vec![Key {
            partition_id: Some(partition("")),
            path: vec![PathElement {
                kind: KIND.to_string(),
                id_type: None,
            }],
        }],
    };
    let err = core::allocate_ids(&storage, req).await.unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);
}

#[tokio::test]
async fn an_entity_committed_to_one_database_is_invisible_from_another() {
    let storage = storage();
    core::commit(&storage, upsert_request("db1", vec![entity("db1", "one")]))
        .await
        .unwrap();

    let in_db1 = core::lookup(&storage, lookup_request("db1", vec![key("db1", "one")]))
        .await
        .unwrap();
    assert_eq!(in_db1.found.len(), 1);

    let in_default = core::lookup(&storage, lookup_request("", vec![key("", "one")]))
        .await
        .unwrap();
    assert_eq!(in_default.found.len(), 0);
    assert_eq!(in_default.missing.len(), 1);

    let batch = core::run_query(&storage, query_request("", ""))
        .await
        .unwrap()
        .batch
        .unwrap();
    assert!(batch.entity_results.is_empty());
}
