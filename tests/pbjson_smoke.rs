use datastore_emulator::google::datastore::v1::{
    Key, PartitionId,
    key::{PathElement, path_element::IdType},
};

#[test]
fn key_round_trips_through_json_camel_case() {
    let key = Key {
        partition_id: Some(PartitionId {
            project_id: "p1".to_string(),
            database_id: String::new(),
            namespace_id: "ns1".to_string(),
        }),
        path: vec![PathElement {
            kind: "Task".to_string(),
            id_type: Some(IdType::Name("abc".to_string())),
        }],
    };

    let json = serde_json::to_value(&key).expect("serialize");
    assert_eq!(json["partitionId"]["projectId"], "p1");
    assert_eq!(json["partitionId"]["namespaceId"], "ns1");
    assert_eq!(json["path"][0]["kind"], "Task");
    assert_eq!(json["path"][0]["name"], "abc");

    let decoded: Key = serde_json::from_value(json).expect("deserialize");
    assert_eq!(decoded.path[0].kind, "Task");
}
