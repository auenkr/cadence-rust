//! SDK/wire compatibility checks against fixtures produced by the Go
//! implementation (`go/common/types`, `encoding/json` with Thrift tags).

use cadence_api::{enums::*, tasktoken::*, types::*};

// Fixture: Go `json.Marshal(TaskToken{...})` — keys must match exactly.
#[test]
fn go_task_token_fixture_deserializes() {
    let go_json = r#"{"domainId":"test-domain-uuid","workflowId":"wf-id","workflowType":"wf-type","runId":"run-id","scheduleId":5,"scheduleAttempt":1,"activityId":"act-id","activityType":"act-type"}"#;
    let token: TaskToken = serde_json::from_str(go_json).unwrap();
    assert_eq!(token.domain_id, "test-domain-uuid");
    assert_eq!(token.schedule_id, 5);
    // Re-serialized bytes must keep the same keys so Go can read them back.
    let back = serde_json::to_value(&token).unwrap();
    let again: TaskToken = serde_json::from_value(back).unwrap();
    assert_eq!(again, token);
}

// Fixture: Go WorkflowExecution JSON uses lowercase `workflowId`/`runId`.
#[test]
fn go_workflow_execution_fixture() {
    let go_json = r#"{"workflowId":"w1","runId":"r1"}"#;
    let e: WorkflowExecution = serde_json::from_str(go_json).unwrap();
    assert_eq!(e.workflow_id, "w1");
    let v = serde_json::to_value(&e).unwrap();
    assert_eq!(v["workflowId"], "w1");
}

// Thrift enums travel as ints; string forms are display-only.
#[test]
fn go_enum_int_encoding() {
    // Go `EventTypeDecisionTaskScheduled` == 4.
    let e: EventType = serde_json::from_str("4").unwrap();
    assert_eq!(e, EventType::DecisionTaskScheduled);
    assert_eq!(serde_json::to_string(&TaskListKind::Sticky).unwrap(), "1");
}

// DataBlob keeps Go's PascalCase keys (`EncodingType`, `Data`).
#[test]
fn go_data_blob_fixture() {
    let go_json = r#"{"EncodingType":0,"Data":"aGk="}"#;
    let b: DataBlob = serde_json::from_str(go_json).unwrap();
    assert_eq!(b.encoding_type, Some(EncodingType::ThriftRW));
    assert_eq!(b.data, b"hi");
}
