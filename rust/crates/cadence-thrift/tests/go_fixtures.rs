//! Wire-compatibility tests against fixtures encoded by the Go server's own
//! Thrift stack (`go/cmd/thrift-fixtures`, thriftrw binary protocol).
//!
//! These prove the Rust decoder reads Go-produced bytes and the Rust encoder
//! produces byte-identical output for the same values. Regenerate fixtures
//! with: `cd go && go run ./cmd/thrift-fixtures -out
//! ../rust/crates/cadence-thrift/tests/fixtures`.

use cadence_api::enums::*;
use cadence_api::types::*;
use cadence_thrift::protocol::{MessageType, Reader, Writer};
use cadence_thrift::value::{read_struct, write_struct, FromThrift, Struct, ToThrift};
use std::path::PathBuf;

fn fixture(name: &str) -> Vec<u8> {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures");
    std::fs::read(dir.join(name)).unwrap_or_else(|_| panic!("missing fixture {name}"))
}

#[test]
fn persistence_history_blob_matches_go() {
    use cadence_api::service_types::{HistoryEvent, WorkflowExecutionSignaledEventAttributes};
    use cadence_thrift::history_blob::{decode, encode};
    let events = vec![HistoryEvent {
        event_id: 7,
        timestamp: Some(1_700_000_000_123),
        event_type: Some(EventType::WorkflowExecutionSignaled),
        version: -24,
        task_id: 19,
        workflow_execution_signaled_event_attributes: Some(
            WorkflowExecutionSignaledEventAttributes {
                signal_name: "wake".into(),
                input: vec![0, 255, 42],
                identity: "oracle".into(),
                ..Default::default()
            },
        ),
        ..Default::default()
    }];
    for (name, events) in [
        ("history_blob.bin", events),
        ("history_blob_empty.bin", vec![]),
    ] {
        let blob = encode(events.clone());
        assert_eq!(blob.data, fixture(name));
        assert_eq!(decode(&blob).unwrap(), events);
        let mut corrupt = blob.clone();
        corrupt.data[0] = 0;
        assert!(decode(&corrupt).is_err());
        let mut trailing = blob;
        trailing.data.push(0);
        assert!(decode(&trailing).is_err());
    }
}

/// Decode `(envelope, args-struct)` from a CALL/REPLY fixture.
fn decode_envelope(bytes: &[u8]) -> (String, MessageType, i32, Struct) {
    let mut r = Reader::new(bytes);
    let (name, ty, seq) = r.read_message_begin().unwrap();
    let body = read_struct(&mut r).unwrap();
    assert_eq!(r.remaining(), 0, "trailing bytes in {name}");
    (name, ty, seq, body)
}

#[test]
fn go_start_args_decode() {
    let bytes = fixture("start_args.bin");
    let (name, ty, seq, body) = decode_envelope(&bytes);
    assert_eq!(name, "StartWorkflowExecution");
    assert_eq!(ty, MessageType::Call);
    assert_eq!(seq, 42);
    let args = body.get(1).expect("args field 1");
    let req = match args {
        cadence_thrift::Value::Struct(s) => StartWorkflowExecutionRequest::from_thrift(s).unwrap(),
        _ => panic!("args not a struct"),
    };
    assert_eq!(req.domain, "samples-domain");
    assert_eq!(req.workflow_id, "workflowid123");
    assert_eq!(req.workflow_type.clone().unwrap().name, "workflow_type");
    let tl = req.task_list.clone().unwrap();
    assert_eq!(tl.name, "tasklist-name");
    assert_eq!(tl.kind, Some(TaskListKind::Normal));
    assert_eq!(req.input, vec![0x00, 0xff, 0x01, 0x02]);
    assert_eq!(req.execution_start_to_close_timeout_seconds, Some(11));
    assert_eq!(req.task_start_to_close_timeout_seconds, Some(10));
    assert_eq!(req.request_id, "4D1E4058-6FCF-4BA8-BF16-8FA8B02F9651");
    assert_eq!(
        req.workflow_id_reuse_policy,
        Some(WorkflowIDReusePolicy::RejectDuplicate)
    );
    let retry = req.retry_policy.clone().unwrap();
    assert_eq!(retry.initial_interval_in_seconds, 1);
    assert_eq!(retry.backoff_coefficient, 2.0);
    assert_eq!(retry.maximum_interval_in_seconds, 100);
    assert_eq!(retry.maximum_attempts, 3);
    assert_eq!(retry.non_retriable_error_reasons, vec!["BadInput", "Fatal"]);
    assert_eq!(retry.expiration_interval_in_seconds, 60);
    assert_eq!(retry.jitter_coefficient, 0.1);
    assert_eq!(req.cron_schedule, "0 6 * * *");
    assert_eq!(req.delay_start_seconds, Some(5));
    assert_eq!(req.jitter_start_seconds, Some(2));
    // Round-trip stability: decode -> encode -> identical bytes.
    // Compare at the args-struct level (field 1 wraps the request).
    let mut w = Writer::new();
    write_struct(
        &mut w,
        &Struct {
            fields: vec![(1, cadence_thrift::Value::Struct(req.to_thrift()))],
        },
    );
    let ours = w.into_bytes();
    let mut r = Reader::new(&bytes);
    r.read_message_begin().unwrap();
    let start = bytes.len() - r.remaining();
    let go_struct_bytes = &bytes[start..];
    assert_eq!(ours, go_struct_bytes, "encoder must match thriftrw bytes");
}

#[test]
fn go_start_reply_decode() {
    let bytes = fixture("start_reply.bin");
    let (name, ty, _, body) = decode_envelope(&bytes);
    assert_eq!(name, "StartWorkflowExecution");
    assert_eq!(ty, MessageType::Reply);
    let resp = match body.get(0).expect("result field 0") {
        cadence_thrift::Value::Struct(s) => StartWorkflowExecutionResponse::from_thrift(s).unwrap(),
        _ => panic!("result not a struct"),
    };
    assert_eq!(resp.run_id, "4D1E4058-6FCF-4BA8-BF16-8FA8B02F9651");
    // Encoder agreement on the success struct, wrapped as field 0 like Go.
    let mut w = Writer::new();
    write_struct(
        &mut w,
        &Struct {
            fields: vec![(0, cadence_thrift::Value::Struct(resp.to_thrift()))],
        },
    );
    let mut r = Reader::new(&bytes);
    r.read_message_begin().unwrap();
    let start = bytes.len() - r.remaining();
    assert_eq!(w.into_bytes(), &bytes[start..]);
}

#[test]
fn go_declared_exception_decode() {
    let bytes = fixture("start_bad_request.bin");
    let (_, ty, _, body) = decode_envelope(&bytes);
    assert_eq!(ty, MessageType::Reply);
    assert!(body.get(0).is_none(), "no success field on error");
    let err = match body.get(1).expect("exception field 1") {
        cadence_thrift::Value::Struct(s) => {
            cadence_api::errors::BadRequestError::from_thrift(s).unwrap()
        }
        _ => panic!("exception not a struct"),
    };
    assert_eq!(err.message, "Domain not set on request.");
}

#[test]
fn go_poll_response_decode() {
    use cadence_api::service_types::HistoryEvent;
    use cadence_api::tasks::PollForDecisionTaskResponse;

    let bytes = fixture("poll_resp.bin");
    let (name, ty, _, body) = decode_envelope(&bytes);
    assert_eq!(name, "PollForDecisionTask");
    assert_eq!(ty, MessageType::Reply);
    let resp = match body.get(0).expect("result field 0") {
        cadence_thrift::Value::Struct(s) => PollForDecisionTaskResponse::from_thrift(s).unwrap(),
        _ => panic!("result not a struct"),
    };
    assert_eq!(resp.task_token, b"{\"domainId\":\"d\",\"scheduleId\":2}");
    let exec = resp.workflow_execution.unwrap();
    assert_eq!(exec.workflow_id, "wf-1");
    assert_eq!(exec.run_id, "run-1");
    assert_eq!(resp.started_event_id, 3);
    assert_eq!(resp.attempt, 1);
    assert_eq!(resp.backlog_count_hint, 2);
    let history = resp.history.unwrap();
    assert_eq!(history.events.len(), 4);

    // Event 1: started with memo/search/header + identity.
    let e1 = &history.events[0];
    assert_eq!(e1.event_id, 1);
    assert_eq!(e1.event_type, Some(EventType::WorkflowExecutionStarted));
    assert_eq!(e1.version, 5);
    let started = e1
        .workflow_execution_started_event_attributes
        .as_ref()
        .unwrap();
    assert_eq!(started.identity, "sdk");
    assert_eq!(started.request_id, "req-1");
    assert_eq!(started.attempt, 2);
    assert_eq!(started.cron_schedule, "0 6 * * *");
    assert_eq!(started.retry_policy.as_ref().unwrap().maximum_attempts, 5);
    assert_eq!(
        started.memo.as_ref().unwrap().fields.as_ref().unwrap()["key"],
        b"value".to_vec()
    );
    assert_eq!(
        started
            .search_attributes
            .as_ref()
            .unwrap()
            .indexed_fields
            .as_ref()
            .unwrap()["CustomKeywordField"],
        b"keyword".to_vec()
    );
    assert_eq!(
        started.header.as_ref().unwrap().fields.as_ref().unwrap()["h"],
        b"v".to_vec()
    );

    // Event 2: decision scheduled, attempt 0.
    let e2 = &history.events[1];
    assert_eq!(e2.event_type, Some(EventType::DecisionTaskScheduled));
    assert_eq!(
        e2.decision_task_scheduled_event_attributes
            .as_ref()
            .unwrap()
            .attempt,
        0
    );

    // Event 3: activity scheduled with retry policy + header.
    let e3 = &history.events[2];
    assert_eq!(e3.event_type, Some(EventType::ActivityTaskScheduled));
    let sched = e3
        .activity_task_scheduled_event_attributes
        .as_ref()
        .unwrap();
    assert_eq!(sched.activity_id, "a1");
    assert_eq!(sched.decision_task_completed_event_id, 4);
    let retry = sched.retry_policy.as_ref().unwrap();
    assert_eq!(retry.backoff_coefficient, 1.5);
    assert_eq!(retry.maximum_attempts, 2);

    // Event 4: activity failed with Poll failure category (wire value 0).
    let e4 = &history.events[3];
    assert_eq!(e4.event_type, Some(EventType::ActivityTaskFailed));
    let failed = e4.activity_task_failed_event_attributes.as_ref().unwrap();
    assert_eq!(failed.reason.as_deref(), Some("boom"));
    assert_eq!(failed.scheduled_event_id, 3);
    assert_eq!(failed.started_event_id, 5);
    assert_eq!(
        failed.failure_options.as_ref().unwrap().failure_category,
        Some(FailureCategory::Poll)
    );

    // Full history round-trips through our codec (decode -> encode -> decode).
    let rt: HistoryEvent = {
        let mut w = Writer::new();
        write_struct(&mut w, &e4.to_thrift());
        let b = w.into_bytes();
        let mut r = Reader::new(&b);
        HistoryEvent::from_thrift(&read_struct(&mut r).unwrap()).unwrap()
    };
    assert_eq!(&rt, e4);
}

#[test]
fn go_signal_args_decode() {
    let bytes = fixture("signal_args.bin");
    let (name, ty, _, body) = decode_envelope(&bytes);
    assert_eq!(name, "SignalWorkflowExecution");
    assert_eq!(ty, MessageType::Call);
    let req = match body.get(1).expect("args field 1") {
        cadence_thrift::Value::Struct(s) => SignalWorkflowExecutionRequest::from_thrift(s).unwrap(),
        _ => panic!("args not a struct"),
    };
    assert_eq!(req.domain, "d");
    assert_eq!(req.signal_name, "sig");
    assert_eq!(req.input, b"payload");
    assert_eq!(req.request_id, "4D1E4058-6FCF-4BA8-BF16-8FA8B02F9651");
}

#[test]
fn active_active_zero_version_matches_go_mapper() {
    let active = ActiveClusters {
        // The pinned Go mapper ignores this deprecated wire field.
        active_clusters_by_region: Some(std::collections::HashMap::from([(
            "ignored".into(), ActiveClusterInfo { active_cluster_name: "unknown".into(), failover_version: 999 },
        )])),
        active_clusters_by_cluster_attribute: Some(std::collections::HashMap::from([(
            "city".into(), ClusterAttributeScope { cluster_attributes: Some(std::collections::HashMap::from([(
                "x".into(), ActiveClusterInfo { active_cluster_name: "a".into(), failover_version: 0 },
            )])) },
        )])),
    };
    let mut writer = Writer::new();
    writer.write_u8(0x59);
    write_struct(&mut writer, &active.to_thrift());
    assert_eq!(writer.into_bytes(), fixture("active_clusters_zero.bin"));
}
