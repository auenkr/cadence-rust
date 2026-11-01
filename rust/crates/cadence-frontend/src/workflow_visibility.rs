//! SQL-visibility-backed list operations of the production `WorkflowBackend`
//! (Go `visibilityManagerImpl` over `sqlVisibilityStore`).
#![allow(clippy::result_large_err)]

use std::sync::Arc;

use cadence_api::enums::{WorkflowExecutionCloseStatus, WorkflowExecutionStatus};
use cadence_api::errors::CadenceError;
use cadence_api::service_types as st;
use cadence_api::types as ty;
use cadence_persistence::visibility_reader::{
    decode_memo, list_executions, VisibilityError, VisibilityFilter, VisibilityRow,
    VisibilityStore,
};

use crate::workflow_backend::{bad_request, internal, to_json, ProductionBackend};

fn map_error(error: VisibilityError) -> CadenceError {
    match error {
        VisibilityError::EntityNotExists(m) => {
            CadenceError::EntityNotExists(cadence_api::errors::EntityNotExistsError::new(m))
        }
        VisibilityError::BadRequest(m) => bad_request(m),
        VisibilityError::Internal(m) => internal(m),
    }
}

/// Go `p.ErrVisibilityOperationNotSupported`.
pub(crate) fn not_supported() -> CadenceError {
    bad_request("Operation is not supported")
}

/// Go `convertVisibilityWorkflowExecutionInfo` (timestamps in internal ms).
pub fn row_to_info(row: &VisibilityRow) -> st::WorkflowExecutionInfo {
    let execution_us = if row.execution_time_us == 0 {
        row.start_time_us
    } else {
        row.execution_time_us
    };
    // Go logs and continues when the memo cannot be decoded.
    let memo = decode_memo(&row.memo, &row.encoding).ok().flatten();
    let mut info = st::WorkflowExecutionInfo {
        execution: Some(ty::WorkflowExecution {
            workflow_id: row.workflow_id.clone(),
            run_id: row.run_id.clone(),
        }),
        workflow_type: Some(ty::WorkflowType {
            name: row.workflow_type_name.clone(),
        }),
        start_time: Some(row.start_time_us / 1000),
        execution_time: Some(execution_us / 1000),
        memo,
        task_list: Some(ty::TaskList::default()),
        is_cron: row.is_cron,
        cron_schedule: Some(String::new()),
        execution_status: Some(WorkflowExecutionStatus::Pending),
        ..Default::default()
    };
    if let Some(status) = row.close_status {
        info.close_time = row.close_time_us.map(|us| us / 1000);
        info.close_status = serde_json::from_value(serde_json::Value::from(status)).ok();
        info.history_length = row.history_length.unwrap_or(0);
    }
    info
}

fn str_field(value: &serde_json::Value, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
}

fn time_us(value: &serde_json::Value, key: &str) -> i64 {
    value
        .get(key)
        .and_then(serde_json::Value::as_i64)
        .unwrap_or(0)
        .saturating_mul(1000)
}

impl ProductionBackend {
    pub fn with_visibility_store(mut self, store: Arc<dyn VisibilityStore>) -> Self {
        self.visibility = Some(store);
        self
    }

    fn visibility_store(&self) -> Result<&Arc<dyn VisibilityStore>, CadenceError> {
        self.visibility.as_ref().ok_or_else(not_supported)
    }

    /// `ListOpenWorkflowExecutions` / `ListClosedWorkflowExecutions`.
    pub(crate) async fn list_visibility(
        &self,
        request: &serde_json::Value,
        closed: bool,
    ) -> Result<serde_json::Value, CadenceError> {
        let store = self.visibility_store()?.clone();
        let close_status = match request.get("status") {
            Some(v) if !v.is_null() => {
                let status: WorkflowExecutionCloseStatus = serde_json::from_value(v.clone())
                    .map_err(|e| bad_request(format!("invalid close status: {e}")))?;
                Some(status as i32)
            }
            _ => None,
        };
        let filter = VisibilityFilter {
            domain_id: str_field(request, "domainUUID").unwrap_or_default(),
            closed,
            workflow_id: str_field(request, "workflowId"),
            workflow_type_name: str_field(request, "workflowTypeName"),
            close_status: if closed { close_status } else { None },
            page_size: request.get("pageSize").and_then(serde_json::Value::as_i64).unwrap_or(0),
            ..Default::default()
        };
        let token = match request.get("nextPageToken") {
            Some(v @ serde_json::Value::String(s)) if !s.is_empty() => {
                cadence_api::b64::deserialize_bytes(v.clone())
                    .map_err(|e| bad_request(format!("invalid token: {e}")))?
            }
            _ => Vec::new(),
        };
        let earliest = time_us(request, "earliestTime");
        let latest = time_us(request, "latestTime");
        let page = tokio::task::spawn_blocking(move || {
            list_executions(store.as_ref(), filter, &token, earliest, latest)
        })
        .await
        .map_err(|e| internal(e.to_string()))?
        .map_err(map_error)?;
        let response = st::ListOpenWorkflowExecutionsResponse {
            executions: page.rows.iter().map(row_to_info).collect(),
            next_page_token: page.next_page_token,
        };
        to_json(&response)
    }

    /// Go `GetClosedWorkflowExecution` (used by archival fallbacks).
    pub async fn get_closed_execution(
        &self,
        domain_id: &str,
        workflow_id: &str,
        run_id: &str,
    ) -> Result<st::WorkflowExecutionInfo, CadenceError> {
        let store = self.visibility_store()?.clone();
        let (d, r) = (domain_id.to_string(), run_id.to_string());
        let row = tokio::task::spawn_blocking(move || store.get_closed(&d, &r))
            .await
            .map_err(|e| internal(e.to_string()))?
            .map_err(map_error)?;
        let Some(mut row) = row else {
            return Err(CadenceError::EntityNotExists(
                cadence_api::errors::EntityNotExistsError::new(format!(
                    "Workflow execution not found.  WorkflowId: {workflow_id}, RunId: {run_id}"
                )),
            ));
        };
        row.workflow_id = workflow_id.to_string();
        row.run_id = run_id.to_string();
        Ok(row_to_info(&row))
    }
}
