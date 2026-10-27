//! Task-token serialization ported from `go/common/taskTokenSerializerInterfaces.go`
//! and `go/common/json_task_token_serializer.go`.
//!
//! JSON field names match Go exactly (`domainId`, `workflowId`, ...).

use cadence_common::go_string::GoString;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskToken {
    #[serde(rename = "domainId")]
    pub domain_id: String,
    #[serde(rename = "workflowId")]
    pub workflow_id: String,
    #[serde(rename = "workflowType")]
    pub workflow_type: String,
    #[serde(rename = "runId")]
    pub run_id: String,
    #[serde(rename = "scheduleId")]
    pub schedule_id: i64,
    #[serde(rename = "scheduleAttempt")]
    pub schedule_attempt: i64,
    #[serde(rename = "activityId")]
    pub activity_id: String,
    #[serde(rename = "activityType")]
    pub activity_type: String,
}

impl TaskToken {
    #[must_use]
    pub fn domain_id(&self) -> &str {
        &self.domain_id
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryTaskToken {
    #[serde(rename = "domainId")]
    pub domain_id: String,
    #[serde(rename = "workflowId")]
    pub workflow_id: String,
    #[serde(rename = "runId")]
    pub run_id: String,
    #[serde(rename = "taskList")]
    pub task_list: String,
    #[serde(rename = "taskId")]
    pub task_id: String,
}

/// Native Go strings before the JSON boundary replaces malformed UTF-8.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TaskTokenBytes {
    pub domain_id: GoString,
    pub workflow_id: GoString,
    pub workflow_type: GoString,
    pub run_id: GoString,
    pub schedule_id: i64,
    pub schedule_attempt: i64,
    pub activity_id: GoString,
    pub activity_type: GoString,
}

impl From<&TaskToken> for TaskTokenBytes {
    fn from(token: &TaskToken) -> Self {
        Self {
            domain_id: token.domain_id.clone().into(),
            workflow_id: token.workflow_id.clone().into(),
            workflow_type: token.workflow_type.clone().into(),
            run_id: token.run_id.clone().into(),
            schedule_id: token.schedule_id,
            schedule_attempt: token.schedule_attempt,
            activity_id: token.activity_id.clone().into(),
            activity_type: token.activity_type.clone().into(),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct QueryTaskTokenBytes {
    pub domain_id: GoString,
    pub workflow_id: GoString,
    pub run_id: GoString,
    pub task_list: GoString,
    pub task_id: GoString,
}

impl From<&QueryTaskToken> for QueryTaskTokenBytes {
    fn from(token: &QueryTaskToken) -> Self {
        Self {
            domain_id: token.domain_id.clone().into(),
            workflow_id: token.workflow_id.clone().into(),
            run_id: token.run_id.clone().into(),
            task_list: token.task_list.clone().into(),
            task_id: token.task_id.clone().into(),
        }
    }
}

/// Mirrors Go `TaskTokenSerializer` (JSON implementation).
pub struct JsonTaskTokenSerializer;

impl JsonTaskTokenSerializer {
    pub fn serialize(token: &TaskToken) -> Result<Vec<u8>, serde_json::Error> {
        Ok(Self::serialize_bytes(&TaskTokenBytes::from(token)))
    }

    pub fn deserialize(data: &[u8]) -> Result<TaskToken, serde_json::Error> {
        serde_json::from_slice(data)
    }

    pub fn serialize_query_token(token: &QueryTaskToken) -> Result<Vec<u8>, serde_json::Error> {
        Ok(Self::serialize_query_token_bytes(
            &QueryTaskTokenBytes::from(token),
        ))
    }

    /// Go `json.Marshal` uses struct declaration order and replaces invalid
    /// bytes only here, without changing the native identifier used for lookup.
    pub fn serialize_bytes(token: &TaskTokenBytes) -> Vec<u8> {
        format!(
            "{{\"domainId\":{},\"workflowId\":{},\"workflowType\":{},\"runId\":{},\"scheduleId\":{},\"scheduleAttempt\":{},\"activityId\":{},\"activityType\":{}}}",
            token.domain_id.go_json_string(),
            token.workflow_id.go_json_string(),
            token.workflow_type.go_json_string(),
            token.run_id.go_json_string(),
            token.schedule_id,
            token.schedule_attempt,
            token.activity_id.go_json_string(),
            token.activity_type.go_json_string(),
        ).into_bytes()
    }

    pub fn serialize_query_token_bytes(token: &QueryTaskTokenBytes) -> Vec<u8> {
        format!(
            "{{\"domainId\":{},\"workflowId\":{},\"runId\":{},\"taskList\":{},\"taskId\":{}}}",
            token.domain_id.go_json_string(),
            token.workflow_id.go_json_string(),
            token.run_id.go_json_string(),
            token.task_list.go_json_string(),
            token.task_id.go_json_string(),
        )
        .into_bytes()
    }

    pub fn deserialize_query_token(data: &[u8]) -> Result<QueryTaskToken, serde_json::Error> {
        serde_json::from_slice(data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_task_tokens_match_the_original_go_serializer() {
        let Ok(path) = std::env::var("MATCHING_GO_RAW_STRING_ORACLE") else {
            return;
        };
        let rows: serde_json::Value =
            serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        for row in rows.as_array().unwrap() {
            let hex = row["hex"].as_str().unwrap();
            let raw = GoString::from(
                (0..hex.len())
                    .step_by(2)
                    .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
                    .collect::<Vec<_>>(),
            );
            let token = TaskTokenBytes {
                domain_id: raw.clone(),
                workflow_id: raw.clone(),
                workflow_type: raw.clone(),
                run_id: raw.clone(),
                schedule_id: -7,
                schedule_attempt: 9,
                activity_id: raw.clone(),
                activity_type: raw.clone(),
            };
            assert_eq!(
                JsonTaskTokenSerializer::serialize_bytes(&token),
                row["taskTokenJSON"].as_str().unwrap().as_bytes(),
                "task token input {hex}"
            );
            let query = QueryTaskTokenBytes {
                domain_id: raw.clone(),
                workflow_id: raw.clone(),
                run_id: raw.clone(),
                task_list: raw.clone(),
                task_id: raw,
            };
            assert_eq!(
                JsonTaskTokenSerializer::serialize_query_token_bytes(&query),
                row["queryTokenJSON"].as_str().unwrap().as_bytes(),
                "query token input {hex}"
            );
        }
    }

    #[test]
    fn task_token_json_replaces_invalid_bytes_only_at_serialization() {
        let token = TaskTokenBytes {
            workflow_id: vec![0xe1, 0x80, 0xff].into(),
            ..Default::default()
        };
        let bytes = JsonTaskTokenSerializer::serialize_bytes(&token);
        assert!(String::from_utf8(bytes.clone())
            .unwrap()
            .contains("\"workflowId\":\"\\ufffd\\ufffd\\ufffd\""));
        assert_eq!(token.workflow_id.as_bytes(), &[0xe1, 0x80, 0xff]);
        assert_eq!(
            JsonTaskTokenSerializer::deserialize(&bytes)
                .unwrap()
                .workflow_id,
            "���"
        );
    }

    #[test]
    fn public_token_serializer_uses_go_html_escaping() {
        let token = TaskToken {
            workflow_id: "<>&\u{2028}\u{2029}".into(),
            ..Default::default()
        };
        let bytes = JsonTaskTokenSerializer::serialize(&token).unwrap();
        assert!(String::from_utf8(bytes.clone())
            .unwrap()
            .contains("\"workflowId\":\"\\u003c\\u003e\\u0026\\u2028\\u2029\""));
        assert_eq!(JsonTaskTokenSerializer::deserialize(&bytes).unwrap(), token);
    }

    fn sample() -> TaskToken {
        TaskToken {
            domain_id: "domain-uuid".into(),
            workflow_id: "wf-id".into(),
            workflow_type: "wf-type".into(),
            run_id: "run-id".into(),
            schedule_id: 5,
            schedule_attempt: 1,
            activity_id: "act-1".into(),
            activity_type: "act-type".into(),
        }
    }

    #[test]
    fn task_token_json_field_names_match_go() {
        let bytes = JsonTaskTokenSerializer::serialize(&sample()).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        for key in [
            "domainId",
            "workflowId",
            "workflowType",
            "runId",
            "scheduleId",
            "scheduleAttempt",
            "activityId",
            "activityType",
        ] {
            assert!(v.get(key).is_some(), "missing key {key}");
        }
    }

    #[test]
    fn task_token_round_trip() {
        let t = sample();
        let bytes = JsonTaskTokenSerializer::serialize(&t).unwrap();
        let back = JsonTaskTokenSerializer::deserialize(&bytes).unwrap();
        assert_eq!(t, back);
    }

    #[test]
    fn query_token_round_trip() {
        let q = QueryTaskToken {
            domain_id: "d".into(),
            workflow_id: "w".into(),
            run_id: "r".into(),
            task_list: "tl".into(),
            task_id: "t".into(),
        };
        let bytes = JsonTaskTokenSerializer::serialize_query_token(&q).unwrap();
        let back = JsonTaskTokenSerializer::deserialize_query_token(&bytes).unwrap();
        assert_eq!(q, back);
    }

    #[test]
    fn go_produced_token_deserializes() {
        // Fixture produced by Go `encoding/json` with the same struct tags.
        let go_json = br#"{"domainId":"uuid-1","workflowId":"wid","workflowType":"wt","runId":"rid","scheduleId":12,"scheduleAttempt":0,"activityId":"aid","activityType":"at"}"#;
        let t = JsonTaskTokenSerializer::deserialize(go_json).unwrap();
        assert_eq!(t.schedule_id, 12);
        assert_eq!(t.workflow_id, "wid");
    }
}
