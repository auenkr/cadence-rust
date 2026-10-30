use cadence_proto::native::matching;
use prost::Message;

fn unhex(value: &str) -> Vec<u8> {
    value
        .as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect()
}

#[test]
fn native_matching_wire_edges_match_pinned_go_yarpc_decoder() {
    let path = std::env::var("MATCHING_GO_RAW_STRING_ORACLE")
        .expect("CI must provide MATCHING_GO_RAW_STRING_ORACLE from the pinned Go oracle");
    let fixture: serde_json::Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    let cases = fixture[0]["nativeWireEdgeCases"]
        .as_array()
        .expect("Go oracle output must include nativeWireEdgeCases");
    assert!(!cases.is_empty());

    for case in cases {
        let input = unhex(case["inputHex"].as_str().unwrap());
        let go_ok = case["error"].as_str().unwrap().is_empty();
        if case["message"] == "pollDecisionResponse" {
            let decoded = matching::PollForDecisionTaskResponse::decode(input.as_slice());
            assert_eq!(decoded.is_ok(), go_ok, "{}", case["name"]);
            if let Ok(decoded) = decoded {
                assert_eq!(
                    decoded.previous_started_event_id.is_some(),
                    case["previousStartedPresent"].as_bool().unwrap()
                );
                if let Some(value) = decoded.previous_started_event_id {
                    assert_eq!(value, case["previousStartedValue"].as_i64().unwrap());
                }
            }
            continue;
        }
        let mut native = matching::AddDecisionTaskRequest::default();
        let native_result = native.merge(input.as_slice());
        assert_eq!(native_result.is_ok(), go_ok, "{}", case["name"]);
        // Rejected messages never reach a handler. The partially mutated
        // scratch value is not an observable transport contract.
        if !go_ok {
            continue;
        }
        assert_eq!(
            hex(native.domain_id.as_bytes()),
            case["domainIdHex"].as_str().unwrap(),
            "{}",
            case["name"]
        );
        assert_eq!(
            native.workflow_execution.is_some(),
            case["workflowExecutionPresent"].as_bool().unwrap(),
            "{}",
            case["name"]
        );
        if let Some(execution) = native.workflow_execution {
            assert_eq!(hex(execution.workflow_id.as_bytes()), case["workflowIdHex"]);
            assert_eq!(hex(execution.run_id.as_bytes()), case["runIdHex"]);
        }
        assert_eq!(
            native.schedule_to_start_timeout.is_some(),
            case["scheduleToStartTimeoutPresent"].as_bool().unwrap(),
            "{}",
            case["name"]
        );
        let actual_map: Vec<(String, String)> = native
            .partition_config
            .into_iter()
            .map(|(key, value)| (hex(key.as_bytes()), hex(value.as_bytes())))
            .collect();
        let mut expected_map: Vec<(String, String)> = case["partitionConfigHex"]
            .as_array()
            .unwrap()
            .iter()
            .map(|entry| {
                (
                    entry[0].as_str().unwrap().to_owned(),
                    entry[1].as_str().unwrap().to_owned(),
                )
            })
            .collect();
        let mut actual_map = actual_map;
        actual_map.sort();
        expected_map.sort();
        assert_eq!(actual_map, expected_map, "{}", case["name"]);
    }
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        output.push(DIGITS[(byte >> 4) as usize] as char);
        output.push(DIGITS[(byte & 0x0f) as usize] as char);
    }
    output
}

#[test]
fn native_zero_isolation_metrics_keep_explicit_map_value() {
    use cadence_proto::native::api::{IsolationGroupMetrics, TaskListStatus};
    let status = TaskListStatus {
        isolation_group_metrics: std::collections::HashMap::from([(
            "a".to_owned().into(),
            IsolationGroupMetrics::default(),
        )]),
        ..Default::default()
    };
    // Go emits tag 6 entry {key "a", value empty message}; an absent value
    // makes the pinned Go decoder keep nil and protobufjs reject the reply.
    assert_eq!(status.encode_to_vec(), vec![0x32, 5, 0x0a, 1, b'a', 0x12, 0]);
    assert_eq!(status.encoded_len(), 7);
}
