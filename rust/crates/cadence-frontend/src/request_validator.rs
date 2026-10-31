//! Frontend request validation ported from Go's
//! `service/frontend/api/request_validator.go`.

use cadence_api::{
    errors::BadRequestError,
    service_types::{
        CountWorkflowExecutionsRequest, DeleteDomainRequest, DeprecateDomainRequest,
        DescribeDomainRequest, ListArchivedWorkflowExecutionsRequest,
        ListClosedWorkflowExecutionsRequest, ListOpenWorkflowExecutionsRequest,
        ListWorkflowExecutionsRequest, UpdateDomainRequest,
    },
    tasks::{
        DescribeTaskListRequest, GetTaskListsByDomainRequest, ListTaskListPartitionsRequest,
        ResetStickyTaskListRequest,
    },
    types::{FailoverDomainRequest, RefreshWorkflowTasksRequest, RegisterDomainRequest},
};

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
}

impl RequestValidator {
    fn bad(message: impl Into<String>) -> BadRequestError {
        BadRequestError::new(message)
    }

    fn domain_missing(domain: &str) -> Result<(), BadRequestError> {
        if domain.is_empty() {
            Err(Self::bad("Domain not set on request."))
        } else {
            Ok(())
        }
    }

    fn page_size_too_large(&self, size: i32) -> bool {
        self.config.advanced_visibility_store && size > self.config.es_index_max_result_window
    }

    fn check_page_size(&self, size: i32) -> Result<(), BadRequestError> {
        if self.page_size_too_large(size) {
            return Err(Self::bad(format!(
                "Pagesize is larger than allow {}",
                self.config.es_index_max_result_window
            )));
        }
        Ok(())
    }

    fn check_permission(&self, token: &str) -> Result<(), BadRequestError> {
        if self.config.admin_protection
            && (token.is_empty() || token != self.config.admin_operation_token)
        {
            return Err(Self::bad("No permission to do this operation."));
        }
        Ok(())
    }

    fn check_task_list(&self, task_list: Option<&cadence_api::types::TaskList>) -> Result<(), BadRequestError> {
        let Some(task_list) = task_list else {
            return Err(Self::bad("TaskList is not set on request."));
        };
        if task_list.name.is_empty() {
            return Err(Self::bad("TaskList is not set on request."));
        }
        if task_list.name.chars().count() > self.config.max_task_list_name_length {
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
        self.check_task_list(request.task_list.as_ref())
    }

    pub fn validate_list_task_list_partitions_request(
        &self,
        request: Option<&ListTaskListPartitionsRequest>,
    ) -> Result<(), BadRequestError> {
        let request = request.ok_or_else(|| Self::bad("Request is nil."))?;
        Self::domain_missing(&request.domain)?;
        self.check_task_list(request.task_list.as_ref())
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
            request.page_size = self.config.visibility_max_page_size;
        }
        self.check_page_size(request.page_size)
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
            request.maximum_page_size = self.config.visibility_max_page_size;
        }
        self.check_page_size(request.maximum_page_size)
    }

    pub fn validate_list_archived_workflow_executions_request(
        &self,
        request: Option<&mut ListArchivedWorkflowExecutionsRequest>,
    ) -> Result<(), BadRequestError> {
        let request = request.ok_or_else(|| Self::bad("Request is nil."))?;
        Self::domain_missing(&request.domain)?;
        if request.page_size <= 0 {
            request.page_size = self.config.visibility_max_page_size;
        }
        if request.page_size > self.config.visibility_archival_query_max_page_size {
            return Err(Self::bad(format!(
                "Pagesize is larger than allowed {}",
                self.config.visibility_archival_query_max_page_size
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
            request.maximum_page_size = self.config.visibility_max_page_size;
        }
        self.check_page_size(request.maximum_page_size)
    }

    pub fn validate_register_domain_request(
        &self,
        request: Option<&RegisterDomainRequest>,
    ) -> Result<(), BadRequestError> {
        let request = request.ok_or_else(|| Self::bad("Request is nil."))?;
        if request.name.is_empty() {
            return Err(Self::bad("Domain not set on request."));
        }
        if request.name.chars().count() > self.config.max_domain_name_length {
            return Err(Self::bad("Domain length exceeds limit."));
        }
        if request.workflow_execution_retention_period_in_days > self.config.max_retention_days {
            return Err(Self::bad("RetentionDays is invalid."));
        }
        for key in &self.config.required_domain_data_keys {
            if !request.data.as_ref().is_some_and(|data| data.contains_key(key)) {
                return Err(Self::bad(format!(
                    "domain data error, missing required key {key} . All required keys: {:?}",
                    self.config.required_domain_data_keys
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
            .is_some_and(|days| days > self.config.max_retention_days)
        {
            return Err(Self::bad("RetentionDays is invalid."));
        }
        if request.is_failover() {
            self.check_failover_permission()
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
            if timeout > self.config.max_failover_timeout_seconds {
                return Err(Self::bad(format!(
                    "FailoverTimeoutInSeconds {timeout} exceeds the maximum allowed {}",
                    self.config.max_failover_timeout_seconds
                )));
            }
        }
        self.check_failover_permission()
    }

    fn check_failover_permission(&self) -> Result<(), BadRequestError> {
        if self.config.lockdown {
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
