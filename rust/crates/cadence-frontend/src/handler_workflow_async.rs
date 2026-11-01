//! ThriftRW envelope construction for Go's queued asynchronous workflow APIs.

use cadence_thrift::value::{write_struct, ToThrift};
use cadence_thrift::Writer;
use serde_json::Value;
use std::collections::HashMap;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AsyncWorkflowMessage {
    pub domain: String,
    pub partition_key: String,
    /// Go sqlblobs.AsyncRequestType: start=0, signal-with-start=1.
    pub request_type: i32,
    pub headers: HashMap<String, Vec<u8>>,
    pub encoding: &'static str,
    pub payload: Vec<u8>,
}

pub fn encode_async_workflow_request(
    operation: &str,
    request: &Value,
    original_headers: &HashMap<String, String>,
) -> Result<AsyncWorkflowMessage, String> {
    let (domain, partition_key, request_type, payload) = match operation {
        "StartWorkflowExecutionAsync" => {
            let request: cadence_api::types::StartWorkflowExecutionAsyncRequest =
                serde_json::from_value(request.clone()).map_err(|error| error.to_string())?;
            let payload = thrift_payload(&request);
            let start = request.request.as_ref();
            (
                start.map_or_else(String::new, |value| value.domain.clone()),
                start.map_or_else(String::new, |value| value.workflow_id.clone()),
                0,
                payload,
            )
        }
        "SignalWithStartWorkflowExecutionAsync" => {
            let request: cadence_api::service_types::SignalWithStartWorkflowExecutionAsyncRequest =
                serde_json::from_value(request.clone()).map_err(|error| error.to_string())?;
            let payload = thrift_payload(&request);
            let start = request.request.as_ref();
            (
                start.map_or_else(String::new, |value| value.domain.clone()),
                start.map_or_else(String::new, |value| value.workflow_id.clone()),
                1,
                payload,
            )
        }
        _ => return Err(format!("unsupported async workflow operation {operation}")),
    };
    let headers = original_headers
        .iter()
        .map(|(key, value)| (key.clone(), value.as_bytes().to_vec()))
        .collect();
    Ok(AsyncWorkflowMessage {
        domain,
        partition_key,
        request_type,
        headers,
        encoding: "ThriftRW",
        payload,
    })
}

fn thrift_payload<T: ToThrift>(value: &T) -> Vec<u8> {
    let mut writer = Writer::new();
    write_struct(&mut writer, &value.to_thrift());
    // Go common/codec.ThriftRWEncoder prefixes even nested async request
    // payloads with its version-0 marker. The Go consumer rejects raw Thrift.
    let mut payload = vec![0x59];
    payload.extend(writer.into_bytes());
    payload
}

#[cfg(test)]
mod tests {
    use super::*;
    use cadence_thrift::value::{read_struct, FromThrift};
    use cadence_thrift::Reader;

    #[test]
    fn async_payload_has_go_versioned_thriftrw_framing() {
        // Go's version-0 encoder emits 0x59 followed by Thrift STOP for an
        // empty optional-field struct. This checks framing, not validation.
        assert_eq!(
            thrift_payload(&cadence_api::types::StartWorkflowExecutionAsyncRequest::default()),
            [0x59, 0x00]
        );
        assert_eq!(
            thrift_payload(
                &cadence_api::service_types::SignalWithStartWorkflowExecutionAsyncRequest::default()
            ),
            [0x59, 0x00]
        );
    }

    #[test]
    fn async_start_matches_go_queue_envelope_and_keeps_original_headers() {
        let request = serde_json::json!({
            "request": {
                "domain": "shop",
                "workflowId": "order-17",
                "workflowType": {"name": "Order"},
                "taskList": {"name": "orders"},
                "input": "AQI="
            }
        });
        let headers = HashMap::from([("x-cadence-trace".into(), "trace-1".into())]);
        let message =
            encode_async_workflow_request("StartWorkflowExecutionAsync", &request, &headers)
                .unwrap();
        assert_eq!(message.domain, "shop");
        assert_eq!(message.partition_key, "order-17");
        assert_eq!(message.request_type, 0);
        assert_eq!(message.encoding, "ThriftRW");
        assert_eq!(message.headers["x-cadence-trace"], b"trace-1");

        assert_eq!(message.payload[0], 0x59);
        let mut reader = Reader::new(&message.payload[1..]);
        let decoded = cadence_api::types::StartWorkflowExecutionAsyncRequest::from_thrift(
            &read_struct(&mut reader).unwrap(),
        )
        .unwrap();
        assert_eq!(decoded.request.unwrap().workflow_id, "order-17");
    }

    #[test]
    fn async_signal_with_start_uses_go_queue_message_type() {
        let request = serde_json::json!({
            "request": {
                "domain": "shop",
                "workflowId": "order-18",
                "workflowType": {"name": "Order"},
                "taskList": {"name": "orders"},
                "signalName": "refresh"
            }
        });
        let message = encode_async_workflow_request(
            "SignalWithStartWorkflowExecutionAsync",
            &request,
            &HashMap::new(),
        )
        .unwrap();
        assert_eq!(message.request_type, 1);
        assert_eq!(message.partition_key, "order-18");
        assert_eq!(message.payload[0], 0x59);
        let decoded =
            cadence_api::service_types::SignalWithStartWorkflowExecutionAsyncRequest::from_thrift(
                &read_struct(&mut Reader::new(&message.payload[1..])).unwrap(),
            )
            .unwrap();
        assert_eq!(decoded.request.unwrap().workflow_id, "order-18");
    }
}
