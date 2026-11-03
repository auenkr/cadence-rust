//! SQL-visibility-backed list operations of the production `WorkflowBackend`
//! (Go `visibilityManagerImpl` over `sqlVisibilityStore`).
#![allow(clippy::result_large_err)]

use std::sync::Arc;

use cadence_api::enums::{WorkflowExecutionCloseStatus, WorkflowExecutionStatus};
use cadence_api::errors::CadenceError;
use cadence_api::service_types as st;
use cadence_api::types as ty;
use cadence_persistence::visibility_reader::{
    decode_memo, list_page, VisibilityError, VisibilityFilter, VisibilityRow, VisibilityStore,
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

/// Go `convertVisibilityWorkflowExecutionInfo` (visibility rows hold microseconds; Cadence API uses nanoseconds).
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
        start_time: Some(row.start_time_us.saturating_mul(1000)),
        execution_time: Some(execution_us.saturating_mul(1000)),
        memo,
        task_list: Some(ty::TaskList::default()),
        is_cron: row.is_cron,
        cron_schedule: Some(String::new()),
        execution_status: Some(WorkflowExecutionStatus::Pending),
        ..Default::default()
    };
    if let Some(status) = row.close_status {
        info.close_time = row.close_time_us.map(|us| us.saturating_mul(1000));
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
        / 1000
}

/// Advanced-visibility (Elasticsearch/OpenSearch) read binding.
#[derive(Clone)]
pub struct EsBinding {
    pub es: Arc<crate::es_visibility::EsVisibility>,
    pub attrs: Arc<
        dyn Fn() -> std::collections::HashMap<String, cadence_api::enums::IndexedValueType>
            + Send
            + Sync,
    >,
    pub validate: Arc<dyn Fn() -> bool + Send + Sync>,
    /// Go `system.readVisibilityStoreName` (per domain name, default `es`).
    pub read_store: Arc<dyn Fn(&str) -> String + Send + Sync>,
}

/// Go `chooseVisibilityManagerForRead`: the first comma-separated store wins;
/// stores that are not available (anything but the configured ES/OpenSearch
/// store) fall back to the DB visibility manager. Returns true for ES.
pub fn read_store_selects_es(configured: &str) -> bool {
    let first = configured.split(',').next().unwrap_or_default();
    matches!(first.trim().to_lowercase().as_str(), "es" | "os")
}

impl ProductionBackend {
    /// The ES binding when this request's domain reads from advanced
    /// visibility, else `None` (DB visibility).
    fn es_for_read(&self, request: &serde_json::Value) -> Option<EsBinding> {
        let binding = self.es.as_ref()?;
        let domain = str_field(request, "domain").unwrap_or_default();
        read_store_selects_es(&(binding.read_store)(&domain)).then(|| binding.clone())
    }
}

#[derive(Clone, Copy)]
pub(crate) enum EsOp {
    List,
    Scan,
    Count,
}

fn map_query_error(e: crate::es_query::QueryError) -> CadenceError {
    match e {
        crate::es_query::QueryError::BadRequest(m) => bad_request(m),
        crate::es_query::QueryError::Internal(m) => internal(m),
    }
}

fn token_bytes(request: &serde_json::Value) -> Result<Vec<u8>, CadenceError> {
    match request.get("nextPageToken") {
        Some(v @ serde_json::Value::String(s)) if !s.is_empty() => {
            cadence_api::b64::deserialize_bytes(v.clone())
                .map_err(|e| bad_request(format!("invalid token: {e}")))
        }
        _ => Ok(Vec::new()),
    }
}

impl ProductionBackend {
    pub fn with_es_visibility(mut self, binding: EsBinding) -> Self {
        self.es = Some(binding);
        self
    }

    /// `ListWorkflowExecutions` / `ScanWorkflowExecutions` / `CountWorkflowExecutions`.
    pub(crate) async fn es_query_op(
        &self,
        request: &serde_json::Value,
        op: EsOp,
    ) -> Result<serde_json::Value, CadenceError> {
        let Some(binding) = self.es_for_read(request) else {
            return Err(not_supported());
        };
        let domain_id = str_field(request, "domainUUID").unwrap_or_default();
        let raw_query = str_field(request, "query").unwrap_or_default();
        let page_size = request
            .get("pageSize")
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(0);
        let token = token_bytes(request)?;
        tokio::task::spawn_blocking(move || -> Result<serde_json::Value, CadenceError> {
            let attrs = (binding.attrs)();
            let query = crate::es_query::validate_query(&raw_query, &attrs, (binding.validate)())
                .map_err(map_query_error)?;
            match op {
                EsOp::Count => {
                    let count = binding
                        .es
                        .count(&domain_id, &query)
                        .map_err(map_query_error)?;
                    to_json(&st::CountWorkflowExecutionsResponse { count })
                }
                EsOp::List | EsOp::Scan => {
                    let (executions, next_page_token) = if matches!(op, EsOp::List) {
                        let types = |n: &str| attrs.get(n).map(|t| t.value());
                        binding
                            .es
                            .list(&domain_id, &query, page_size, &token, &types)
                    } else {
                        binding.es.scan(&domain_id, &query, page_size, &token)
                    }
                    .map_err(map_query_error)?;
                    to_json(&st::ListWorkflowExecutionsResponse {
                        executions,
                        next_page_token,
                    })
                }
            }
        })
        .await
        .map_err(|e| internal(e.to_string()))?
    }

    /// ES-backed `ListOpen/ListClosed(+ByID/ByType/ByStatus)`.
    pub(crate) async fn es_open_closed(
        &self,
        request: &serde_json::Value,
        closed: bool,
    ) -> Result<serde_json::Value, CadenceError> {
        let Some(binding) = self.es_for_read(request) else {
            return Err(not_supported());
        };
        let domain_id = str_field(request, "domainUUID").unwrap_or_default();
        let page_size = request
            .get("pageSize")
            .or_else(|| request.get("maximumPageSize"))
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(0);
        let token = token_bytes(request)?;
        let earliest = request
            .get("earliestTime")
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(0);
        let latest = request
            .get("latestTime")
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(i64::MAX);
        let (name, matcher) = {
            let status = request.get("status").filter(|v| !v.is_null());
            if closed {
                if let Some(v) = status {
                    let st: WorkflowExecutionCloseStatus = serde_json::from_value(v.clone())
                        .map_err(|e| bad_request(format!("invalid close status: {e}")))?;
                    (
                        "ListClosedWorkflowExecutionsByStatus",
                        Some(("CloseStatus".to_string(), serde_json::json!(st as i32))),
                    )
                } else if let Some(t) = str_field(request, "workflowTypeName") {
                    (
                        "ListClosedWorkflowExecutionsByType",
                        Some(("WorkflowType".to_string(), serde_json::json!(t))),
                    )
                } else if let Some(w) = str_field(request, "workflowId") {
                    (
                        "ListClosedWorkflowExecutionsByWorkflowID",
                        Some(("WorkflowID".to_string(), serde_json::json!(w))),
                    )
                } else {
                    ("ListClosedWorkflowExecutions", None)
                }
            } else if let Some(t) = str_field(request, "workflowTypeName") {
                (
                    "ListOpenWorkflowExecutionsByType",
                    Some(("WorkflowType".to_string(), serde_json::json!(t))),
                )
            } else if let Some(w) = str_field(request, "workflowId") {
                (
                    "ListOpenWorkflowExecutionsByWorkflowID",
                    Some(("WorkflowID".to_string(), serde_json::json!(w))),
                )
            } else {
                ("ListOpenWorkflowExecutions", None)
            }
        };
        let (executions, next_page_token) = tokio::task::spawn_blocking(move || {
            binding.es.list_open_closed(
                name, &domain_id, !closed, earliest, latest, page_size, &token, matcher,
            )
        })
        .await
        .map_err(|e| internal(e.to_string()))?
        .map_err(map_query_error)?;
        to_json(&st::ListOpenWorkflowExecutionsResponse {
            executions,
            next_page_token,
        })
    }

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
        if self.es_for_read(request).is_some() {
            return self.es_open_closed(request, closed).await;
        }
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
            page_size: request
                .get("pageSize")
                .and_then(serde_json::Value::as_i64)
                .unwrap_or(0),
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
            list_page(store.as_ref(), filter, &token, earliest, latest)
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
        let (d, w, r) = (
            domain_id.to_string(),
            workflow_id.to_string(),
            run_id.to_string(),
        );
        let row = tokio::task::spawn_blocking(move || store.get_closed_by_workflow(&d, &w, &r))
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

#[cfg(test)]
mod read_store_tests {
    use super::read_store_selects_es;

    #[test]
    fn first_store_decides_like_go_choose_visibility_manager() {
        assert!(read_store_selects_es("es"));
        assert!(read_store_selects_es(" ES , db"));
        assert!(read_store_selects_es("os"));
        assert!(!read_store_selects_es("db"));
        assert!(!read_store_selects_es("db,es"));
        // Unavailable stores fall back to DB visibility.
        assert!(!read_store_selects_es("pinot"));
        assert!(!read_store_selects_es(""));
    }
}
