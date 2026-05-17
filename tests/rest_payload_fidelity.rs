use datastore_emulator::google::datastore::v1::{CommitRequest, LookupRequest, RunQueryRequest};

#[test]
fn lookup_payload_round_trips() {
    let raw = r#"{
        "keys": [{
            "partitionId": { "projectId": "p1", "namespaceId": "ns" },
            "path": [{ "kind": "Task", "id": "12345" }]
        }]
    }"#;
    let parsed: LookupRequest = serde_json::from_str(raw).unwrap();
    assert_eq!(parsed.keys.len(), 1);
    let re = serde_json::to_value(&parsed).unwrap();
    assert_eq!(re["keys"][0]["path"][0]["id"], "12345");
    assert_eq!(re["keys"][0]["partitionId"]["projectId"], "p1");
}

#[test]
fn run_query_kind_payload_round_trips() {
    let raw = r#"{
        "query": { "kind": [{ "name": "__kind__" }] }
    }"#;
    let parsed: RunQueryRequest = serde_json::from_str(raw).unwrap();
    let re = serde_json::to_value(&parsed).unwrap();
    assert_eq!(re["query"]["kind"][0]["name"], "__kind__");
}

#[test]
fn commit_insert_round_trips() {
    let raw = r#"{
        "mode": "NON_TRANSACTIONAL",
        "mutations": [{
            "insert": {
                "key": {
                    "partitionId": { "projectId": "p1" },
                    "path": [{ "kind": "Task", "name": "x" }]
                },
                "properties": {
                    "blob": { "blobValue": "aGVsbG8=" },
                    "ts": { "timestampValue": "2024-01-15T10:00:00Z" },
                    "i64": { "integerValue": "9223372036854775807" },
                    "nested": {
                        "entityValue": {
                            "properties": {
                                "s": { "stringValue": "v" }
                            }
                        }
                    }
                }
            }
        }]
    }"#;
    let parsed: CommitRequest = serde_json::from_str(raw).unwrap();
    let re = serde_json::to_value(&parsed).unwrap();
    assert_eq!(re["mode"], "NON_TRANSACTIONAL");
    assert_eq!(
        re["mutations"][0]["insert"]["properties"]["i64"]["integerValue"],
        "9223372036854775807"
    );
    // pbjson-types serializes via chrono which emits `+00:00` instead of `Z`;
    // both are valid ISO-8601 UTC representations per Google's proto-JSON spec.
    let ts = re["mutations"][0]["insert"]["properties"]["ts"]["timestampValue"]
        .as_str()
        .unwrap();
    assert!(ts.starts_with("2024-01-15T10:00:00"));
    assert!(ts.ends_with("Z") || ts.ends_with("+00:00"));
    assert_eq!(
        re["mutations"][0]["insert"]["properties"]["blob"]["blobValue"],
        "aGVsbG8="
    );
}
