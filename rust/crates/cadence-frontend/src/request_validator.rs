//! Frontend request validation ported from Go's
//! `service/frontend/api/request_validator.go`.

use cadence_api::{
    errors::BadRequestError,
    service_types::{
        CountWorkflowExecutionsRequest, DeleteDomainRequest, DeprecateDomainRequest,
        DescribeDomainRequest, ListArchivedWorkflowExecutionsRequest,
        ListClosedWorkflowExecutionsRequest, ListOpenWorkflowExecutionsRequest,
        ListWorkflowExecutionsRequest, ResetStickyTaskListRequest, UpdateDomainRequest,
    },
    tasks::{
        DescribeTaskListRequest, GetTaskListsByDomainRequest, ListTaskListPartitionsRequest,
    },
    types::{FailoverDomainRequest, RefreshWorkflowTasksRequest, RegisterDomainRequest},
};
use std::sync::{Arc, Mutex};

/// Typed subset of Frontend configuration consumed by request validation.
/// Values are populated from the canonical Go dynamic-property keys.
#[derive(Debug, Clone)]
pub struct ValidationConfig {
    pub visibility_max_page_size: i32,
    pub visibility_archival_query_max_page_size: i32,
    pub es_index_max_result_window: i32,
    pub advanced_visibility_store: bool,
    pub admin_protection: bool,
    pub admin_operation_token: String,
    pub max_retention_days: i32,
    pub max_failover_timeout_seconds: i32,
    pub lockdown: bool,
    pub max_domain_name_length: usize,
    pub max_task_list_name_length: usize,
    pub required_domain_data_keys: Vec<String>,
}

impl Default for ValidationConfig {
    fn default() -> Self {
        Self {
            visibility_max_page_size: 1000,
            visibility_archival_query_max_page_size: 1000,
            es_index_max_result_window: 10000,
            advanced_visibility_store: false,
            admin_protection: false,
            admin_operation_token: String::new(),
            max_retention_days: 3650,
            max_failover_timeout_seconds: 60,
            lockdown: false,
            max_domain_name_length: 255,
            max_task_list_name_length: 255,
            required_domain_data_keys: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct RequestValidator {
    pub config: ValidationConfig,
    /// Shared with Frontend's dynamic-config handlers. Read on every request,
    /// so admin updates and provider refreshes take effect without rebuilding
    /// the validator, as Go's typed property closures do.
    pub dynamic: Option<Arc<Mutex<cadence_service_core::admin::DynamicConfigOverlay>>>,
}

impl RequestValidator {
    fn bad(message: impl Into<String>) -> BadRequestError {
        BadRequestError::new(message)
    }

    pub fn with_dynamic_config(
        mut self,
        dynamic: Arc<Mutex<cadence_service_core::admin::DynamicConfigOverlay>>,
    ) -> Self {
        self.dynamic = Some(dynamic);
        self
    }

    fn dynamic_integer(&self, key: &str, domain: Option<&str>, default: i64) -> i64 {
        let Some(dynamic) = &self.dynamic else { return default };
        let Ok(dynamic) = dynamic.lock() else { return default };
        cadence_persistence::dynamic_config::integer(dynamic.property_values(key), domain, default)
    }

    fn dynamic_boolean(&self, key: &str, domain: Option<&str>, default: bool) -> bool {
        let Some(dynamic) = &self.dynamic else { return default };
        let Ok(dynamic) = dynamic.lock() else { return default };
        cadence_persistence::dynamic_config::boolean(dynamic.property_values(key), domain, default)
    }

    fn dynamic_string(&self, key: &str, default: &str) -> String {
        let Some(dynamic) = &self.dynamic else { return default.to_owned() };
        let Ok(dynamic) = dynamic.lock() else { return default.to_owned() };
        cadence_persistence::dynamic_config::string(dynamic.property_values(key), default)
    }

    fn dynamic_string_domain(&self, key: &str, domain: &str, default: &str) -> String {
        let Some(dynamic) = &self.dynamic else { return default.to_owned() };
        let Ok(dynamic) = dynamic.lock() else { return default.to_owned() };
        cadence_persistence::dynamic_config::string_domain(dynamic.property_values(key), domain, default)
    }

    fn visibility_max_page_size(&self, domain: &str) -> i32 {
        self.dynamic_integer("frontend.visibilityMaxPageSize", Some(domain), i64::from(self.config.visibility_max_page_size)) as i32
    }

    fn es_index_max_result_window(&self) -> i32 {
        self.dynamic_integer("frontend.esIndexMaxResultWindow", None, i64::from(self.config.es_index_max_result_window)) as i32
    }

    fn domain_missing(domain: &str) -> Result<(), BadRequestError> {
        if domain.is_empty() {
            Err(Self::bad("Domain not set on request."))
        } else {
            Ok(())
        }
    }

    fn page_size_too_large(&self, size: i32, domain: &str) -> bool {
        let domain_uses_advanced = self.dynamic_string_domain("system.readVisibilityStoreName", domain, "db") != "db";
        self.config.advanced_visibility_store
            && domain_uses_advanced
            && size > self.es_index_max_result_window()
    }

    fn check_page_size(&self, size: i32, domain: &str) -> Result<(), BadRequestError> {
        if self.page_size_too_large(size, domain) {
            return Err(Self::bad(format!(
                "Pagesize is larger than allow {}",
                self.es_index_max_result_window()
            )));
        }
        Ok(())
    }

    fn check_permission(&self, token: &str) -> Result<(), BadRequestError> {
        let protected = self.dynamic_boolean("history.enableAdminProtection", None, self.config.admin_protection);
        let required = self.dynamic_string("history.adminOperationToken", &self.config.admin_operation_token);
        if protected && (token.is_empty() || token != required)
        {
            return Err(Self::bad("No permission to do this operation."));
        }
        Ok(())
    }

    fn check_task_list(&self, task_list: Option<&cadence_api::types::TaskList>, domain: &str) -> Result<(), BadRequestError> {
        let Some(task_list) = task_list else {
            return Err(Self::bad("TaskList is not set on request."));
        };
        if task_list.name.is_empty() {
            return Err(Self::bad("TaskList is not set on request."));
        }
        let max_length = self.dynamic_integer("limit.taskListNameLength", Some(domain), self.config.max_task_list_name_length as i64) as usize;
        if task_list.name.len() > max_length {
            return Err(Self::bad("TaskList length exceeds limit."));
        }
        Ok(())
    }

    fn check_time_filter(
        filter: Option<&cadence_api::service_types::StartTimeFilter>,
    ) -> Result<(), BadRequestError> {
        let Some(filter) = filter else {
            return Err(Self::bad("StartTimeFilter is required"));
        };
        let Some(earliest) = filter.earliest_time else {
            return Err(Self::bad("EarliestTime in StartTimeFilter is required"));
        };
        let Some(latest) = filter.latest_time else {
            return Err(Self::bad("LatestTime in StartTimeFilter is required"));
        };
        if earliest > latest {
            return Err(Self::bad(
                "EarliestTime in StartTimeFilter should not be larger than LatestTime",
            ));
        }
        Ok(())
    }

    pub fn validate_refresh_workflow_tasks_request(
        &self,
        request: Option<&RefreshWorkflowTasksRequest>,
    ) -> Result<(), BadRequestError> {
        let request = request.ok_or_else(|| Self::bad("Request is nil."))?;
        cadence_api::validation::check_execution(request.execution.as_ref())
    }

    pub fn validate_describe_task_list_request(
        &self,
        request: Option<&DescribeTaskListRequest>,
    ) -> Result<(), BadRequestError> {
        let request = request.ok_or_else(|| Self::bad("Request is nil."))?;
        Self::domain_missing(&request.domain)?;
        if request.task_list_type.is_none() {
            return Err(Self::bad("TaskListType is not set on request."));
        }
        self.check_task_list(request.task_list.as_ref(), &request.domain)
    }

    pub fn validate_list_task_list_partitions_request(
        &self,
        request: Option<&ListTaskListPartitionsRequest>,
    ) -> Result<(), BadRequestError> {
        let request = request.ok_or_else(|| Self::bad("Request is nil."))?;
        Self::domain_missing(&request.domain)?;
        self.check_task_list(request.task_list.as_ref(), &request.domain)
    }

    pub fn validate_get_task_lists_by_domain_request(
        &self,
        request: Option<&GetTaskListsByDomainRequest>,
    ) -> Result<(), BadRequestError> {
        let request = request.ok_or_else(|| Self::bad("Request is nil."))?;
        Self::domain_missing(&request.domain)
    }

    pub fn validate_reset_sticky_task_list_request(
        &self,
        request: Option<&ResetStickyTaskListRequest>,
    ) -> Result<(), BadRequestError> {
        let request = request.ok_or_else(|| Self::bad("Request is nil."))?;
        Self::domain_missing(&request.domain)?;
        cadence_api::validation::check_execution(request.execution.as_ref())
    }

    pub fn validate_count_workflow_executions_request(
        &self,
        request: Option<&CountWorkflowExecutionsRequest>,
    ) -> Result<(), BadRequestError> {
        let request = request.ok_or_else(|| Self::bad("Request is nil."))?;
        Self::domain_missing(&request.domain)
    }

    pub fn validate_list_workflow_executions_request(
        &self,
        request: Option<&mut ListWorkflowExecutionsRequest>,
    ) -> Result<(), BadRequestError> {
        let request = request.ok_or_else(|| Self::bad("Request is nil."))?;
        Self::domain_missing(&request.domain)?;
        if request.page_size <= 0 {
            request.page_size = self.visibility_max_page_size(&request.domain);
        }
        self.check_page_size(request.page_size, &request.domain)
    }

    pub fn validate_list_open_workflow_executions_request(
        &self,
        request: Option<&mut ListOpenWorkflowExecutionsRequest>,
    ) -> Result<(), BadRequestError> {
        let request = request.ok_or_else(|| Self::bad("Request is nil."))?;
        Self::domain_missing(&request.domain)?;
        Self::check_time_filter(request.start_time_filter.as_ref())?;
        if request.execution_filter.is_some() && request.type_filter.is_some() {
            return Err(Self::bad("Only one of ExecutionFilter or TypeFilter is allowed"));
        }
        if request.maximum_page_size <= 0 {
            request.maximum_page_size = self.visibility_max_page_size(&request.domain);
        }
        self.check_page_size(request.maximum_page_size, &request.domain)
    }

    pub fn validate_list_archived_workflow_executions_request(
        &self,
        request: Option<&mut ListArchivedWorkflowExecutionsRequest>,
    ) -> Result<(), BadRequestError> {
        let request = request.ok_or_else(|| Self::bad("Request is nil."))?;
        Self::domain_missing(&request.domain)?;
        if request.page_size <= 0 {
            request.page_size = self.visibility_max_page_size(&request.domain);
        }
        let max_page_size = self.dynamic_integer("frontend.visibilityArchivalQueryMaxPageSize", None, i64::from(self.config.visibility_archival_query_max_page_size)) as i32;
        if request.page_size > max_page_size {
            return Err(Self::bad(format!(
                "Pagesize is larger than allowed {}",
                max_page_size
            )));
        }
        Ok(())
    }

    pub fn validate_list_closed_workflow_executions_request(
        &self,
        request: Option<&mut ListClosedWorkflowExecutionsRequest>,
    ) -> Result<(), BadRequestError> {
        let request = request.ok_or_else(|| Self::bad("Request is nil."))?;
        Self::domain_missing(&request.domain)?;
        Self::check_time_filter(request.start_time_filter.as_ref())?;
        let filters = usize::from(request.execution_filter.is_some())
            + usize::from(request.type_filter.is_some())
            + usize::from(request.status_filter.is_some());
        if filters > 1 {
            return Err(Self::bad(
                "Only one of ExecutionFilter, TypeFilter or StatusFilter is allowed",
            ));
        }
        if request.maximum_page_size <= 0 {
            request.maximum_page_size = self.visibility_max_page_size(&request.domain);
        }
        self.check_page_size(request.maximum_page_size, &request.domain)
    }

    pub fn validate_register_domain_request(
        &self,
        request: Option<&RegisterDomainRequest>,
    ) -> Result<(), BadRequestError> {
        let request = request.ok_or_else(|| Self::bad("Request is nil."))?;
        if request.name.is_empty() {
            return Err(Self::bad("Domain not set on request."));
        }
        let max_domain_length = self.dynamic_integer("limit.domainNameLength", Some(&request.name), self.config.max_domain_name_length as i64) as usize;
        if request.name.len() > max_domain_length {
            return Err(Self::bad("Domain length exceeds limit."));
        }
        let max_retention = self.dynamic_integer("system.maxRetentionDays", None, i64::from(self.config.max_retention_days)) as i32;
        if request.workflow_execution_retention_period_in_days > max_retention {
            return Err(Self::bad("RetentionDays is invalid."));
        }
        let required_keys = if let Some(dynamic) = &self.dynamic {
            dynamic.lock().ok().map(|dynamic| {
                cadence_persistence::dynamic_config::string_list(dynamic.property_values("system.requiredDomainDataKeys"))
            }).filter(|keys| !keys.is_empty()).unwrap_or_else(|| self.config.required_domain_data_keys.clone())
        } else {
            self.config.required_domain_data_keys.clone()
        };
        for key in &required_keys {
            if !request.data.as_ref().is_some_and(|data| data.contains_key(key)) {
                return Err(Self::bad(format!(
                    "domain data error, missing required key {key} . All required keys: {:?}",
                    required_keys
                )));
            }
        }
        self.check_permission(&request.security_token)
    }

    pub fn validate_describe_domain_request(
        &self,
        request: Option<&DescribeDomainRequest>,
    ) -> Result<(), BadRequestError> {
        let request = request.ok_or_else(|| Self::bad("Request is nil."))?;
        if request.name.as_deref().unwrap_or_default().is_empty()
            && request.uuid.as_deref().unwrap_or_default().is_empty()
        {
            return Err(Self::bad("Domain not set on request."));
        }
        Ok(())
    }

    pub fn validate_update_domain_request(
        &self,
        request: Option<&UpdateDomainRequest>,
    ) -> Result<(), BadRequestError> {
        let request = request.ok_or_else(|| Self::bad("Request is nil."))?;
        Self::domain_missing(&request.name)?;
        if request
            .configuration
            .as_ref()
            .and_then(|config| config.workflow_execution_retention_period_in_days)
            .is_some_and(|days| days > self.dynamic_integer("system.maxRetentionDays", None, i64::from(self.config.max_retention_days)) as i32)
        {
            return Err(Self::bad("RetentionDays is invalid."));
        }
        if request.is_failover() {
            self.check_failover_permission(&request.name)
        } else {
            self.check_permission(&request.security_token)
        }
    }

    pub fn validate_delete_domain_request(
        &self,
        request: Option<&DeleteDomainRequest>,
    ) -> Result<(), BadRequestError> {
        let request = request.ok_or_else(|| Self::bad("Request is nil."))?;
        Self::domain_missing(&request.name)?;
        self.check_permission(&request.security_token)
    }

    pub fn validate_deprecate_domain_request(
        &self,
        request: Option<&DeprecateDomainRequest>,
    ) -> Result<(), BadRequestError> {
        let request = request.ok_or_else(|| Self::bad("Request is nil."))?;
        Self::domain_missing(&request.name)?;
        self.check_permission(&request.security_token)
    }

    pub fn validate_failover_domain_request(
        &self,
        request: Option<&FailoverDomainRequest>,
    ) -> Result<(), BadRequestError> {
        let request = request.ok_or_else(|| Self::bad("Request is nil."))?;
        Self::domain_missing(&request.domain_name)?;
        if request.domain_active_cluster_name.is_none() && request.active_clusters.is_none() {
            return Err(Self::bad(
                "DomainActiveClusterName or ActiveClusters must be provided to failover the domain",
            ));
        }
        if let Some(timeout) = request.failover_timeout_in_seconds {
            if timeout <= 0 {
                return Err(Self::bad("FailoverTimeoutInSeconds must be positive"));
            }
            let max_timeout = self.dynamic_integer("frontend.maxFailoverTimeoutInSeconds", Some(&request.domain_name), i64::from(self.config.max_failover_timeout_seconds)) as i32;
            if timeout > max_timeout {
                return Err(Self::bad(format!(
                    "FailoverTimeoutInSeconds {timeout} exceeds the maximum allowed {}",
                    max_timeout
                )));
            }
        }
        self.check_failover_permission(&request.domain_name)
    }

    fn check_failover_permission(&self, domain: &str) -> Result<(), BadRequestError> {
        if self.dynamic_boolean("system.Lockdown", Some(domain), self.config.lockdown) {
            Err(Self::bad(
                "Domain is not accepting fail overs at this time due to lockdown.",
            ))
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cadence_api::service_types::{StartTimeFilter, WorkflowExecutionFilter, WorkflowTypeFilter};
    use cadence_api::types::{TaskList, WorkflowExecution};

    #[test]
    fn list_page_size_mutation_and_visibility_limit_match_go() {
        let validator = RequestValidator::default();
        let mut request = ListWorkflowExecutionsRequest { domain: "d".into(), ..Default::default() };
        validator.validate_list_workflow_executions_request(Some(&mut request)).unwrap();
        assert_eq!(request.page_size, 1000);

        let mut validator = validator;
        validator.config.advanced_visibility_store = true;
        validator.config.es_index_max_result_window = 100;
        assert!(validator.validate_list_workflow_executions_request(Some(&mut request)).is_err());
    }

    #[test]
    fn task_list_and_time_filter_validation_match_go_order() {
        let validator = RequestValidator::default();
        let err = validator.validate_describe_task_list_request(Some(&DescribeTaskListRequest {
            domain: "d".into(), task_list: Some(TaskList::default()), task_list_type: Some(cadence_api::TaskListType::Decision), ..Default::default()
        })).unwrap_err();
        assert_eq!(err.message, "TaskList is not set on request.");

        let mut request = ListOpenWorkflowExecutionsRequest {
            domain: "d".into(),
            start_time_filter: Some(StartTimeFilter { earliest_time: Some(2), latest_time: Some(1) }),
            execution_filter: Some(WorkflowExecutionFilter::default()),
            type_filter: Some(WorkflowTypeFilter::default()),
            ..Default::default()
        };
        assert_eq!(validator.validate_list_open_workflow_executions_request(Some(&mut request)).unwrap_err().message,
            "EarliestTime in StartTimeFilter should not be larger than LatestTime");
    }

    #[test]
    fn reset_sticky_requires_execution_and_validates_identifiers() {
        let validator = RequestValidator::default();
        let err = validator.validate_reset_sticky_task_list_request(Some(&ResetStickyTaskListRequest {
            domain: "d".into(), execution: Some(WorkflowExecution::default())
        })).unwrap_err();
        assert_eq!(err.message, "WorkflowId is not set on request.");
    }
}
