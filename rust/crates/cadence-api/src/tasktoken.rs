//! Task-token serialization ported from `go/common/taskTokenSerializerInterfaces.go`
//! and `go/common/json_task_token_serializer.go`.
//!
//! JSON field names match Go exactly (`domainId`, `workflowId`, ...).

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

/// Mirrors Go `TaskTokenSerializer` (JSON implementation).
pub struct JsonTaskTokenSerializer;

impl JsonTaskTokenSerializer {
    pub fn serialize(token: &TaskToken) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(token)
    }

    pub fn deserialize(data: &[u8]) -> Result<TaskToken, serde_json::Error> {
        serde_json::from_slice(data)
    }

    pub fn serialize_query_token(token: &QueryTaskToken) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(token)
    }

    pub fn deserialize_query_token(data: &[u8]) -> Result<QueryTaskToken, serde_json::Error> {
        serde_json::from_slice(data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
