//! Go-compatible history continuation token JSON (`api/handler.go`).

use cadence_api::errors::{BadRequestError, InternalServiceError};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HistoryContinuationToken {
    #[serde(rename = "runID", alias = "RunID")]
    pub run_id: String,
    #[serde(rename = "firstEventID", alias = "FirstEventID")]
    pub first_event_id: i64,
    #[serde(rename = "nextEventID", alias = "NextEventID")]
    pub next_event_id: i64,
    #[serde(rename = "isWorkflowRunning", alias = "IsWorkflowRunning")]
    pub is_workflow_running: bool,
    #[serde(
        rename = "persistenceToken",
        alias = "PersistenceToken",
        default,
        serialize_with = "cadence_api::b64::serialize_opt_bytes",
        deserialize_with = "cadence_api::b64::deserialize_opt_bytes"
    )]
    pub persistence_token: Option<Vec<u8>>,
    #[serde(rename = "transientDecision", alias = "TransientDecision", default)]
    pub transient_decision: Option<Value>,
    #[serde(
        rename = "branchToken",
        alias = "BranchToken",
        default,
        serialize_with = "cadence_api::b64::serialize_opt_bytes",
        deserialize_with = "cadence_api::b64::deserialize_opt_bytes"
    )]
    pub branch_token: Option<Vec<u8>>,
    #[serde(rename = "versionHistoryItem", alias = "VersionHistoryItem", default)]
    pub version_history_item: Option<Value>,
}

/// A nil Go token serializes as a nil byte slice; a non-nil token always
/// serializes every field, including zero values and null pointers.
pub fn serialize_history_token(
    token: Option<&HistoryContinuationToken>,
) -> Result<Option<Vec<u8>>, serde_json::Error> {
    token.map(serde_json::to_vec).transpose()
}

pub fn deserialize_history_token(
    bytes: &[u8],
) -> Result<HistoryContinuationToken, serde_json::Error> {
    serde_json::from_slice(bytes)
}

pub fn validate_history_run_id(
    request_run_id: &str,
    token: &HistoryContinuationToken,
) -> Result<(), BadRequestError> {
    if !request_run_id.is_empty() && request_run_id != token.run_id {
        return Err(BadRequestError::new(
            "RunID in the request does not match the NextPageToken.",
        ));
    }
    Ok(())
}

/// Verify that a decoded persistence page covers the event interval requested
/// by Frontend. This matches Go's `verifyHistoryIsComplete`, including the
/// lower-bound page-size rule used when the persistence token says more pages
/// remain.
#[allow(clippy::too_many_arguments)]
pub fn verify_history_is_complete(
    first_event_id: Option<i64>,
    last_event_id: Option<i64>,
    event_count: usize,
    expected_first_event_id: i64,
    expected_last_event_id: i64,
    is_first_page: bool,
    is_last_page: bool,
    page_size: usize,
) -> Result<(), InternalServiceError> {
    if event_count == 0 {
        if is_last_page {
            return Ok(());
        }
        return Err(InternalServiceError::new(
            "invalid history: contains zero events",
        ));
    }
    let first = first_event_id.unwrap_or_default();
    let last = last_event_id.unwrap_or_default();
    let mut expected_first = expected_first_event_id;
    if !is_first_page {
        if first <= expected_first_event_id {
            return Err(InternalServiceError::new(format!(
                "invalid history: expected first eventID to be > {expected_first_event_id} but got {first}"
            )));
        }
        expected_first = first;
    }
    let expected_last = if is_last_page {
        expected_last_event_id
    } else {
        expected_first
            .saturating_add(page_size as i64)
            .saturating_sub(1)
    };
    let expected_count = expected_last
        .saturating_sub(expected_first)
        .saturating_add(1);
    let count = event_count as i64;
    if first == expected_first
        && ((is_last_page && last == expected_last && count == expected_count)
            || (!is_last_page && last >= expected_last && count >= expected_count))
    {
        return Ok(());
    }
    Err(InternalServiceError::new(format!(
        "incomplete history: expected events [{expected_first}-{expected_last}] but got events [{first}-{last}] of length {event_count}:isFirstPage={is_first_page},isLastPage={is_last_page},pageSize={page_size}"
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_json_matches_go_struct_field_order_and_byte_encoding() {
        let token = HistoryContinuationToken {
            run_id: "run-1".into(),
            first_event_id: 1,
            next_event_id: 2,
            is_workflow_running: true,
            persistence_token: Some(vec![1, 2]),
            transient_decision: None,
            branch_token: Some(vec![3, 4]),
            version_history_item: Some(serde_json::json!({"eventID": 2, "version": 7})),
        };
        let expected = br#"{"runID":"run-1","firstEventID":1,"nextEventID":2,"isWorkflowRunning":true,"persistenceToken":"AQI=","transientDecision":null,"branchToken":"AwQ=","versionHistoryItem":{"eventID":2,"version":7}}"#;
        assert_eq!(
            serialize_history_token(Some(&token)).unwrap().unwrap(),
            expected
        );
        assert_eq!(deserialize_history_token(expected).unwrap(), token);
        assert_eq!(serialize_history_token(None).unwrap(), None);
    }

    #[test]
    fn history_token_run_id_mismatch_uses_go_error() {
        let token = HistoryContinuationToken {
            run_id: "token-run".into(),
            ..Default::default()
        };
        assert!(validate_history_run_id("", &token).is_ok());
        assert_eq!(
            validate_history_run_id("request-run", &token)
                .unwrap_err()
                .to_string(),
            "RunID in the request does not match the NextPageToken."
        );
    }

    #[test]
    fn history_page_completeness_matches_go_persistence_checks() {
        assert!(verify_history_is_complete(Some(1), Some(3), 3, 1, 3, true, true, 3).is_ok());
        assert!(verify_history_is_complete(Some(1), Some(4), 4, 1, 4, true, false, 3).is_ok());
        assert!(verify_history_is_complete(None, None, 0, 1, 3, false, true, 3).is_ok());
        assert_eq!(
            verify_history_is_complete(None, None, 0, 1, 3, true, false, 3)
                .unwrap_err()
                .to_string(),
            "invalid history: contains zero events"
        );
        assert_eq!(
            verify_history_is_complete(Some(1), Some(2), 2, 1, 3, true, true, 3)
                .unwrap_err()
                .to_string(),
            "incomplete history: expected events [1-3] but got events [1-2] of length 2:isFirstPage=true,isLastPage=true,pageSize=3"
        );
    }
}
