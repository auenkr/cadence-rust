//! Cadence error types ported from `go/common/types/shared.go` error structs
//! and `go/common/types/errors.go` behavior.

use serde::{Deserialize, Serialize};
use thiserror::Error;

// ---------------------------------------------------------------------------
// Error structs (wire shapes match Go json tags)
// ---------------------------------------------------------------------------

macro_rules! message_error {
    ($name:ident) => {
        #[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, Error)]
        #[error("{message}")]
        pub struct $name {
            #[serde(rename = "message", default)]
            pub message: String,
        }
        impl $name {
            #[must_use]
            pub fn new(message: impl Into<String>) -> Self {
                Self {
                    message: message.into(),
                }
            }
        }
    };
}

message_error!(AccessDeniedError);
message_error!(BadRequestError);
message_error!(CancellationAlreadyRequestedError);
message_error!(CurrentBranchChangedError);
message_error!(DomainAlreadyExistsError);
message_error!(WorkflowExecutionAlreadyCompletedError);
message_error!(InternalDataInconsistencyError);
message_error!(InternalServiceError);
message_error!(LimitExceededError);
message_error!(QueryFailedError);
message_error!(RemoteSyncMatchedError);
message_error!(ShardOwnershipLostError);
message_error!(EventAlreadyStartedError);
message_error!(StickyWorkerUnavailableError);
message_error!(ReadOnlyPartitionError);

/// EntityNotExistsError carries optional cluster context plus a required
/// (possibly empty) active-clusters list, per the IDL.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, Error)]
#[error("{message}")]
pub struct EntityNotExistsError {
    #[serde(rename = "message", default)]
    pub message: String,
    #[serde(
        rename = "currentCluster",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub current_cluster: String,
    #[serde(
        rename = "activeCluster",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub active_cluster: String,
    #[serde(
        rename = "activeClusters",
        default,
        skip_serializing_if = "Vec::is_empty"
    )]
    pub active_clusters: Vec<String>,
}

impl EntityNotExistsError {
    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            ..Default::default()
        }
    }
}

/// DomainNotActiveError: all IDL fields are required.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, Error)]
#[error("{message}")]
pub struct DomainNotActiveError {
    #[serde(rename = "message", default)]
    pub message: String,
    #[serde(
        rename = "domainName",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub domain_name: String,
    #[serde(
        rename = "currentCluster",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub current_cluster: String,
    #[serde(
        rename = "activeCluster",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub active_cluster: String,
    #[serde(
        rename = "activeClusters",
        default,
        skip_serializing_if = "Vec::is_empty"
    )]
    pub active_clusters: Vec<String>,
}

/// ServiceBusyError carries an optional machine-readable reason
/// (e.g. workflow-ID rate limiting), per the IDL.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, Error)]
#[error("{message}")]
pub struct ServiceBusyError {
    #[serde(rename = "message", default)]
    pub message: String,
    #[serde(rename = "reason", default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

impl ServiceBusyError {
    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            reason: None,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, Error)]
#[error("client version not supported")]
pub struct ClientVersionNotSupportedError {
    #[serde(
        rename = "featureVersion",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub feature_version: String,
    #[serde(
        rename = "clientImpl",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub client_impl: String,
    #[serde(
        rename = "supportedVersions",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub supported_versions: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, Error)]
#[error("feature not enabled")]
pub struct FeatureNotEnabledError {
    #[serde(
        rename = "featureFlag",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub feature_flag: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, Error)]
#[error("{message}")]
pub struct WorkflowExecutionAlreadyStartedError {
    #[serde(rename = "message", default)]
    pub message: String,
    #[serde(
        rename = "startRequestId",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub start_request_id: String,
    #[serde(rename = "runId", default, skip_serializing_if = "String::is_empty")]
    pub run_id: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, Error)]
#[error("{message}")]
pub struct RetryTaskV2Error {
    #[serde(rename = "message", default)]
    pub message: String,
    #[serde(rename = "domainId", default, skip_serializing_if = "String::is_empty")]
    pub domain_id: String,
    #[serde(
        rename = "workflowId",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub workflow_id: String,
    #[serde(rename = "runId", default, skip_serializing_if = "String::is_empty")]
    pub run_id: String,
    #[serde(
        rename = "startEventId",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub start_event_id: Option<i64>,
    #[serde(
        rename = "startEventVersion",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub start_event_version: Option<i64>,
    #[serde(
        rename = "endEventId",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub end_event_id: Option<i64>,
    #[serde(
        rename = "endEventVersion",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub end_event_version: Option<i64>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, Error)]
#[error("cluster attribute not found: {scope_type}/{attribute_name}")]
pub struct ClusterAttributeNotFoundError {
    #[serde(rename = "scopeType", default)]
    pub scope_type: String,
    #[serde(rename = "attributeName", default)]
    pub attribute_name: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, Error)]
#[error("query rejected")]
pub struct QueryRejected {
    #[serde(
        rename = "closeStatus",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub close_status: Option<crate::enums::WorkflowExecutionCloseStatus>,
}

// ---------------------------------------------------------------------------
// Unified error enum for service handlers
// ---------------------------------------------------------------------------

#[derive(Debug, Error)]
pub enum CadenceError {
    #[error(transparent)]
    BadRequest(#[from] BadRequestError),
    #[error(transparent)]
    Internal(#[from] InternalServiceError),
    #[error(transparent)]
    EntityNotExists(#[from] EntityNotExistsError),
    #[error(transparent)]
    DomainNotActive(#[from] DomainNotActiveError),
    #[error(transparent)]
    WorkflowAlreadyStarted(#[from] WorkflowExecutionAlreadyStartedError),
    #[error(transparent)]
    WorkflowAlreadyCompleted(#[from] WorkflowExecutionAlreadyCompletedError),
    #[error(transparent)]
    DomainAlreadyExists(#[from] DomainAlreadyExistsError),
    #[error(transparent)]
    CancellationAlreadyRequested(#[from] CancellationAlreadyRequestedError),
    #[error(transparent)]
    QueryFailed(#[from] QueryFailedError),
    #[error(transparent)]
    ServiceBusy(#[from] ServiceBusyError),
    #[error(transparent)]
    LimitExceeded(#[from] LimitExceededError),
    #[error(transparent)]
    ShardOwnershipLost(#[from] ShardOwnershipLostError),
    #[error(transparent)]
    RetryTaskV2(#[from] RetryTaskV2Error),
    #[error(transparent)]
    AccessDenied(#[from] AccessDeniedError),
    #[error(transparent)]
    ClientVersionNotSupported(#[from] ClientVersionNotSupportedError),
    #[error("unknown: {0}")]
    Unknown(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_display_matches_go_behavior() {
        let e = BadRequestError::new("bad input");
        assert_eq!(e.to_string(), "bad input");
        let v = ClientVersionNotSupportedError::default();
        assert_eq!(v.to_string(), "client version not supported");
        let f = FeatureNotEnabledError {
            feature_flag: "x".into(),
        };
        assert_eq!(f.to_string(), "feature not enabled");
    }

    #[test]
    fn retry_task_v2_json_round_trip() {
        let e = RetryTaskV2Error {
            message: "retry".into(),
            domain_id: "d".into(),
            start_event_id: Some(5),
            ..Default::default()
        };
        let s = serde_json::to_string(&e).unwrap();
        let back: RetryTaskV2Error = serde_json::from_str(&s).unwrap();
        assert_eq!(e, back);
    }
}
