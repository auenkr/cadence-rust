//! Admin raw-history V2 range and cursor rules from Go frontend/admin/handler.go.
use crate::version_history::{new_item, VersionHistoriesExt, VersionHistoryExt};
use cadence_api::{
    admin::GetWorkflowExecutionRawHistoryV2Request as Request,
    errors::BadRequestError,
    types::{VersionHistories, VersionHistory},
};
use serde::{Deserialize, Serialize};

type Result<T> = std::result::Result<T, BadRequestError>;

pub fn validate_request(request: &Request) -> Result<()> {
    let execution = request.execution.as_ref();
    if execution.is_none_or(|e| e.workflow_id.is_empty()) {
        return Err(BadRequestError::new("Invalid WorkflowID."));
    }
    if execution.is_none_or(|e| !is_google_uuid(&e.run_id)) {
        return Err(BadRequestError::new("Invalid RunID."));
    }
    if request.maximum_page_size <= 0 {
        return Err(BadRequestError::new("Invalid PageSize."));
    }
    if request.start_event_id.is_some() != request.start_event_version.is_some() {
        return Err(BadRequestError::new(
            "Invalid start event id and start event version combination.",
        ));
    }
    if request.end_event_id.is_some() != request.end_event_version.is_some() {
        return Err(BadRequestError::new(
            "Invalid end event id and end event version combination.",
        ));
    }
    Ok(())
}

/// Go's `google/uuid.Parse` accepts canonical, compact, braced and URN forms.
fn is_google_uuid(value: &str) -> bool {
    let value = value.strip_prefix("urn:uuid:").unwrap_or(value);
    let value = value
        .strip_prefix('{')
        .and_then(|v| v.strip_suffix('}'))
        .unwrap_or(value);
    uuid::Uuid::parse_str(value).is_ok()
}

pub fn select_history<'a>(
    request: &mut Request,
    histories: &'a VersionHistories,
) -> Result<&'a VersionHistory> {
    let mut target = histories.current_history()?;
    let first = target.first_item()?;
    let last = target.last_item()?;
    if request.start_event_id.is_none() || request.start_event_version.is_none() {
        request.start_event_id = Some(0);
        request.start_event_version = Some(first.version);
    }
    if request.end_event_id.is_none() || request.end_event_version.is_none() {
        request.end_event_id = Some(last.event_id.wrapping_add(1));
        request.end_event_version = Some(last.version);
    }
    let start = request.start_event_id.unwrap();
    if start < 0 {
        return Err(BadRequestError::new(
            "Invalid FirstEventID && NextEventID combination.",
        ));
    }
    let end = request.end_event_id.unwrap();
    let end_version = request.end_event_version.unwrap();
    if end != last.event_id.wrapping_add(1) || end_version != last.version {
        target = histories.find_first_by_item(&new_item(end, end_version))?.1;
    }
    let start_version = request.start_event_version.unwrap();
    let start_item = new_item(start, start_version);
    if (start != 0 || start_version != first.version) && !target.contains_item(&start_item) {
        let start_branch = histories.find_first_by_item(&start_item)?.1;
        let lca = target.find_lca(start_branch)?;
        request.start_event_id = Some(lca.event_id);
        request.start_event_version = Some(lca.version);
    }
    Ok(target)
}

/// Field names and byte encoding match Go's getWorkflowRawHistoryV2Token.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, rename_all = "PascalCase")]
pub struct RawHistoryToken {
    pub domain_name: String,
    #[serde(rename = "WorkflowID")]
    pub workflow_id: String,
    #[serde(rename = "RunID")]
    pub run_id: String,
    #[serde(rename = "StartEventID")]
    pub start_event_id: i64,
    pub start_event_version: i64,
    #[serde(rename = "EndEventID")]
    pub end_event_id: i64,
    pub end_event_version: i64,
    #[serde(
        serialize_with = "cadence_api::b64::serialize_opt_bytes",
        deserialize_with = "cadence_api::b64::deserialize_opt_bytes"
    )]
    pub persistence_token: Option<Vec<u8>>,
    pub version_histories: Option<VersionHistories>,
}

impl RawHistoryToken {
    pub fn new(request: &Request, histories: &VersionHistories) -> Self {
        let execution = request.execution.clone().unwrap_or_default();
        Self {
            domain_name: request.domain.clone(),
            workflow_id: execution.workflow_id,
            run_id: execution.run_id,
            start_event_id: request.start_event_id.unwrap_or(0),
            start_event_version: request.start_event_version.unwrap_or(0),
            end_event_id: request.end_event_id.unwrap_or(0),
            end_event_version: request.end_event_version.unwrap_or(0),
            persistence_token: None,
            version_histories: Some(histories.clone()),
        }
    }

    pub fn to_bytes(&self) -> std::result::Result<Vec<u8>, cadence_api::errors::BadRequestError> {
        serde_json::to_vec(self)
            .map_err(|_| cadence_api::errors::BadRequestError::new("Invalid pagination token."))
    }

    pub fn from_bytes(bytes: &[u8]) -> std::result::Result<Self, cadence_api::errors::BadRequestError> {
        serde_json::from_slice(bytes)
            .map_err(|_| cadence_api::errors::BadRequestError::new("Invalid pagination token."))
    }

    pub fn validate(&self, request: &Request) -> Result<()> {
        let execution = request.execution.clone().unwrap_or_default();
        if request.domain != self.domain_name
            || execution.workflow_id != self.workflow_id
            || execution.run_id != self.run_id
            || request.start_event_id.unwrap_or(0) != self.start_event_id
            || request.start_event_version.unwrap_or(0) != self.start_event_version
            || request.end_event_id.unwrap_or(0) != self.end_event_id
            || request.end_event_version.unwrap_or(0) != self.end_event_version
        {
            return Err(BadRequestError::new("Invalid pagination token."));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cadence_api::types::WorkflowExecution;

    #[test]
    fn raw_history_run_id_accepts_google_uuid_spellings() {
        for run_id in [
            "123e4567-e89b-12d3-a456-426614174000",
            "123e4567e89b12d3a456426614174000",
            "{123e4567-e89b-12d3-a456-426614174000}",
            "urn:uuid:123e4567-e89b-12d3-a456-426614174000",
        ] {
            let request = Request {
                execution: Some(WorkflowExecution::new("workflow", run_id)),
                maximum_page_size: 1,
                ..Default::default()
            };
            assert!(validate_request(&request).is_ok(), "{run_id}");
        }
        let invalid = Request {
            execution: Some(WorkflowExecution::new("workflow", "not-a-uuid")),
            maximum_page_size: 1,
            ..Default::default()
        };
        assert_eq!(validate_request(&invalid).unwrap_err().message, "Invalid RunID.");
    }
}
