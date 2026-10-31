//! Batch system workflow wire names from Go `batcher`.
pub const TASK_LIST: &str = "cadence-sys-batcher-tasklist";
pub const V1_WORKFLOW_TYPE: &str = "cadence-sys-batch-workflow";
pub const V2_WORKFLOW_TYPE: &str = "cadence-sys-batch-workflow-v2";
pub const ACTIVITY_TYPE: &str = "cadence-sys-batch-activity";
pub const V2_ACTIVITY_TYPE: &str = "cadence-sys-batch-activity-v2";
pub const TUNE_SIGNAL: &str = "cadence-sys-batch-tune-signal";
pub const DEFAULT_RPS: i32 = 50;
pub const DEFAULT_CONCURRENCY: i32 = 5;
pub const DEFAULT_PAGE_SIZE: i32 = 1000;
pub const DEFAULT_RETRYABLE_ATTEMPTS: i32 = 50;
pub const DEFAULT_MAX_ACTIVITY_RETRIES: i32 = 4;
pub const DEFAULT_HEARTBEAT_TIMEOUT_NANOS: i64 = 10_000_000_000;

#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(default)]
pub struct TerminateParams {
    #[serde(rename = "TerminateChildren")]
    pub terminate_children: Option<bool>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(default)]
pub struct CancelParams {
    #[serde(rename = "CancelChildren")]
    pub cancel_children: Option<bool>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(default)]
pub struct SignalParams {
    #[serde(rename = "SignalName")]
    pub signal_name: String,
    #[serde(rename = "Input")]
    pub input: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(default)]
pub struct ReplicateParams {
    #[serde(rename = "SourceCluster")]
    pub source_cluster: String,
    #[serde(rename = "TargetCluster")]
    pub target_cluster: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(default)]
pub struct HeartBeatDetails {
    #[serde(rename = "PageToken", with = "base64_bytes")]
    pub page_token: Vec<u8>,
    #[serde(rename = "CurrentPage")]
    pub current_page: i32,
    #[serde(rename = "TotalEstimate")]
    pub total_estimate: i64,
    #[serde(rename = "SuccessCount")]
    pub success_count: i32,
    #[serde(rename = "ErrorCount")]
    pub error_count: i32,
    #[serde(rename = "RPS")]
    pub rps: i32,
    #[serde(rename = "Concurrency")]
    pub concurrency: i32,
}

mod base64_bytes {
    use base64::Engine;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&base64::engine::general_purpose::STANDARD.encode(bytes))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<u8>, D::Error> {
        let value = String::deserialize(deserializer)?;
        base64::engine::general_purpose::STANDARD
            .decode(value)
            .map_err(serde::de::Error::custom)
    }
}

/// Go `BatchParams` JSON field names and nanosecond duration representation.
#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(default)]
pub struct BatchParams {
    #[serde(rename = "DomainName")]
    pub domain_name: String,
    #[serde(rename = "Query")]
    pub query: String,
    #[serde(rename = "Reason")]
    pub reason: String,
    #[serde(rename = "BatchType")]
    pub batch_type: String,
    #[serde(rename = "TerminateParams")]
    pub terminate_params: TerminateParams,
    #[serde(rename = "CancelParams")]
    pub cancel_params: CancelParams,
    #[serde(rename = "SignalParams")]
    pub signal_params: SignalParams,
    #[serde(rename = "ReplicateParams")]
    pub replicate_params: ReplicateParams,
    #[serde(rename = "RPS")]
    pub rps: i32,
    #[serde(rename = "Concurrency")]
    pub concurrency: i32,
    #[serde(rename = "PageSize")]
    pub page_size: i32,
    #[serde(rename = "AttemptsOnRetryableError")]
    pub attempts_on_retryable_error: i32,
    #[serde(rename = "ActivityHeartBeatTimeout")]
    pub activity_heartbeat_timeout_nanos: i64,
    #[serde(rename = "MaxActivityRetries")]
    pub max_activity_retries: i32,
    #[serde(rename = "NonRetryableErrors")]
    pub non_retryable_errors: Option<Vec<String>>,
    #[serde(rename = "Progress")]
    pub progress: Option<HeartBeatDetails>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidatedBatchParams(pub BatchParams);

pub fn validate_and_default(mut params: BatchParams) -> Result<ValidatedBatchParams, String> {
    if params.batch_type.is_empty()
        || params.reason.is_empty()
        || params.domain_name.is_empty()
        || params.query.is_empty()
    {
        return Err("must provide required parameters: BatchType/Reason/DomainName/Query".into());
    }
    match params.batch_type.as_str() {
        "signal" if params.signal_params.signal_name.is_empty() => {
            return Err("must provide signal name".into());
        }
        "replicate" if params.replicate_params.source_cluster.is_empty() => {
            return Err("must provide source cluster".into());
        }
        "replicate" if params.replicate_params.target_cluster.is_empty() => {
            return Err("must provide target cluster".into());
        }
        "terminate" | "cancel" | "signal" | "replicate" | "refresh" => {}
        value => return Err(format!("not supported batch type: {value}")),
    }
    if params.rps <= 0 {
        params.rps = DEFAULT_RPS;
    }
    if params.concurrency <= 0 {
        params.concurrency = DEFAULT_CONCURRENCY;
    }
    if params.page_size <= 0 {
        params.page_size = DEFAULT_PAGE_SIZE;
    }
    if params.attempts_on_retryable_error <= 0 {
        params.attempts_on_retryable_error = DEFAULT_RETRYABLE_ATTEMPTS;
    }
    if params.activity_heartbeat_timeout_nanos <= 0 {
        params.activity_heartbeat_timeout_nanos = DEFAULT_HEARTBEAT_TIMEOUT_NANOS;
    }
    if params.terminate_params.terminate_children.is_none() {
        params.terminate_params.terminate_children = Some(true);
    }
    if params.max_activity_retries < 0 {
        params.max_activity_retries = DEFAULT_MAX_ACTIVITY_RETRIES;
    }
    Ok(ValidatedBatchParams(params))
}

pub fn registrations() -> [crate::runtime::WorkflowRegistration; 2] {
    [
        crate::runtime::WorkflowRegistration {
            workflow_type: V1_WORKFLOW_TYPE,
            task_list: TASK_LIST,
            activities: &[ACTIVITY_TYPE],
        },
        crate::runtime::WorkflowRegistration {
            workflow_type: V2_WORKFLOW_TYPE,
            task_list: TASK_LIST,
            activities: &[V2_ACTIVITY_TYPE],
        },
    ]
}

#[cfg(test)]
mod tests {
    #[test]
    fn preserves_legacy_and_current_workflow_names() {
        assert_eq!(super::TASK_LIST, "cadence-sys-batcher-tasklist");
        assert_eq!(super::V1_WORKFLOW_TYPE, "cadence-sys-batch-workflow");
        assert_eq!(super::V2_WORKFLOW_TYPE, "cadence-sys-batch-workflow-v2");
        assert_eq!(super::ACTIVITY_TYPE, "cadence-sys-batch-activity");
        assert_eq!(super::V2_ACTIVITY_TYPE, "cadence-sys-batch-activity-v2");
        assert_eq!(super::TUNE_SIGNAL, "cadence-sys-batch-tune-signal");
    }

    #[test]
    fn batch_defaults_and_validation_match_go() {
        let result = super::validate_and_default(super::BatchParams {
            batch_type: "refresh".to_owned(),
            reason: "reason".to_owned(),
            domain_name: "domain".to_owned(),
            query: "WorkflowType='x'".to_owned(),
            ..Default::default()
        })
        .unwrap()
        .0;
        assert_eq!(result.rps, 50);
        assert_eq!(result.concurrency, 5);
        assert_eq!(result.page_size, 1000);
        assert_eq!(result.attempts_on_retryable_error, 50);
        assert_eq!(result.activity_heartbeat_timeout_nanos, 10_000_000_000);
        assert_eq!(result.terminate_params.terminate_children, Some(true));
        assert_eq!(result.max_activity_retries, 0);
        assert_eq!(
            super::validate_and_default(super::BatchParams {
                batch_type: "invalid".to_owned(),
                reason: "r".to_owned(),
                domain_name: "d".to_owned(),
                query: "q".to_owned(),
                ..Default::default()
            })
            .unwrap_err(),
            "not supported batch type: invalid"
        );
    }

    #[test]
    fn heartbeat_details_encode_bytes_as_go_base64() {
        let details = super::HeartBeatDetails {
            page_token: vec![1, 2, 3],
            ..Default::default()
        };
        let json = serde_json::to_string(&details).unwrap();
        assert!(json.starts_with(r#"{"PageToken":"AQID","CurrentPage":0"#));
    }
}
