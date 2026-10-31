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
    tasks::{DescribeTaskListRequest, GetTaskListsByDomainRequest, ListTaskListPartitionsRequest},
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
        let Some(dynamic) = &self.dynamic else {
            return default;
        };
        let Ok(dynamic) = dynamic.lock() else {
            return default;
        };
        cadence_persistence::dynamic_config::integer(dynamic.property_values(key), domain, default)
    }

    fn dynamic_boolean(&self, key: &str, domain: Option<&str>, default: bool) -> bool {
        let Some(dynamic) = &self.dynamic else {
            return default;
        };
        let Ok(dynamic) = dynamic.lock() else {
            return default;
        };
        cadence_persistence::dynamic_config::boolean(dynamic.property_values(key), domain, default)
    }

    fn dynamic_string(&self, key: &str, default: &str) -> String {
        let Some(dynamic) = &self.dynamic else {
            return default.to_owned();
        };
        let Ok(dynamic) = dynamic.lock() else {
            return default.to_owned();
        };
        cadence_persistence::dynamic_config::string(dynamic.property_values(key), default)
    }

    fn dynamic_string_domain(&self, key: &str, domain: &str, default: &str) -> String {
        let Some(dynamic) = &self.dynamic else {
            return default.to_owned();
        };
        let Ok(dynamic) = dynamic.lock() else {
            return default.to_owned();
        };
        cadence_persistence::dynamic_config::string_domain(
            dynamic.property_values(key),
            domain,
            default,
        )
    }

    fn visibility_max_page_size(&self, domain: &str) -> i32 {
        self.dynamic_integer(
            "frontend.visibilityMaxPageSize",
            Some(domain),
            i64::from(self.config.visibility_max_page_size),
        ) as i32
    }

    fn es_index_max_result_window(&self) -> i32 {
        self.dynamic_integer(
            "frontend.esIndexMaxResultWindow",
            None,
            i64::from(self.config.es_index_max_result_window),
        ) as i32
    }

    fn domain_missing(domain: &str) -> Result<(), BadRequestError> {
        if domain.is_empty() {
            Err(Self::bad("Domain not set on request."))
        } else {
            Ok(())
        }
    }

    fn page_size_too_large(&self, size: i32, domain: &str) -> bool {
        let domain_uses_advanced =
            self.dynamic_string_domain("system.readVisibilityStoreName", domain, "db") != "db";
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
        let protected = self.dynamic_boolean(
            "history.enableAdminProtection",
            None,
            self.config.admin_protection,
        );
        let required = self.dynamic_string(
            "history.adminOperationToken",
            &self.config.admin_operation_token,
        );
        if protected && (token.is_empty() || token != required) {
            return Err(Self::bad("No permission to do this operation."));
        }
        Ok(())
    }

    fn check_task_list(
        &self,
        task_list: Option<&cadence_api::types::TaskList>,
        domain: &str,
    ) -> Result<(), BadRequestError> {
        let Some(task_list) = task_list else {
            return Err(Self::bad("TaskList is not set on request."));
        };
        if task_list.name.is_empty() {
            return Err(Self::bad("TaskList is not set on request."));
        }
        let max_length = self.dynamic_integer(
            "limit.taskListNameLength",
            Some(domain),
            self.config.max_task_list_name_length as i64,
        ) as usize;
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
            return Err(Self::bad(
                "Only one of ExecutionFilter or TypeFilter is allowed",
            ));
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
        let max_page_size = self.dynamic_integer(
            "frontend.visibilityArchivalQueryMaxPageSize",
            None,
            i64::from(self.config.visibility_archival_query_max_page_size),
        ) as i32;
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
        let max_domain_length = self.dynamic_integer(
            "limit.domainNameLength",
            Some(&request.name),
            self.config.max_domain_name_length as i64,
        ) as usize;
        if request.name.len() > max_domain_length {
            return Err(Self::bad("Domain length exceeds limit."));
        }
        let max_retention = self.dynamic_integer(
            "system.maxRetentionDays",
            None,
            i64::from(self.config.max_retention_days),
        ) as i32;
        if request.workflow_execution_retention_period_in_days > max_retention {
            return Err(Self::bad("RetentionDays is invalid."));
        }
        let default_required_keys = self
            .config
            .required_domain_data_keys
            .iter()
            .cloned()
            .map(|key| (key, serde_json::Value::Bool(true)))
            .collect::<std::collections::BTreeMap<_, _>>();
        let required_keys = self
            .dynamic
            .as_ref()
            .and_then(|dynamic| dynamic.lock().ok())
            .map(|dynamic| {
                cadence_persistence::dynamic_config::map_property(
                    dynamic.property_values("system.requiredDomainDataKeys"),
                    default_required_keys.clone(),
                )
            })
            .unwrap_or(default_required_keys);
        let required_keys_go_format = format!(
            "map[{}]",
            required_keys
                .iter()
                .map(|(key, value)| {
                    let value = match value {
                        serde_json::Value::String(value) => value.clone(),
                        _ => value.to_string(),
                    };
                    format!("{key}:{value}")
                })
                .collect::<Vec<_>>()
                .join(" ")
        );
        for key in required_keys.keys() {
            if !request
                .data
                .as_ref()
                .is_some_and(|data| data.contains_key(key))
            {
                return Err(Self::bad(format!(
                    "domain data error, missing required key {key} . All required keys: {required_keys_go_format}"
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
            .is_some_and(|days| {
                days > self.dynamic_integer(
                    "system.maxRetentionDays",
                    None,
                    i64::from(self.config.max_retention_days),
                ) as i32
            })
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
            let max_timeout = self.dynamic_integer(
                "frontend.maxFailoverTimeoutInSeconds",
                Some(&request.domain_name),
                i64::from(self.config.max_failover_timeout_seconds),
            ) as i32;
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
    use cadence_api::service_types::{
        StartTimeFilter, WorkflowExecutionFilter, WorkflowTypeFilter,
    };
    use cadence_api::types::{TaskList, WorkflowExecution};
    use cadence_api::{
        admin::{DynamicConfigFilter, DynamicConfigValue},
        enums::EncodingType,
        types::DataBlob,
    };

    #[test]
    fn list_page_size_mutation_and_visibility_limit_match_go() {
        let validator = RequestValidator::default();
        let mut request = ListWorkflowExecutionsRequest {
            domain: "d".into(),
            ..Default::default()
        };
        validator
            .validate_list_workflow_executions_request(Some(&mut request))
            .unwrap();
        assert_eq!(request.page_size, 1000);

        let mut validator = validator;
        validator.config.advanced_visibility_store = true;
        validator.config.es_index_max_result_window = 100;
        validator.dynamic = Some(Arc::new(Mutex::new(
            cadence_service_core::admin::DynamicConfigOverlay::default(),
        )));
        validator.dynamic.as_ref().unwrap().lock().unwrap().update(
            false,
            "system.readVisibilityStoreName",
            vec![DynamicConfigValue {
                value: Some(DataBlob::new(
                    EncodingType::JSON,
                    serde_json::to_vec(&serde_json::json!("es")).unwrap(),
                )),
                filters: vec![DynamicConfigFilter {
                    name: "domainName".into(),
                    value: Some(DataBlob::new(
                        EncodingType::JSON,
                        serde_json::to_vec(&serde_json::json!("d")).unwrap(),
                    )),
                }],
            }],
        );
        assert!(validator
            .validate_list_workflow_executions_request(Some(&mut request))
            .is_err());
    }

    #[test]
    fn task_list_and_time_filter_validation_match_go_order() {
        let validator = RequestValidator::default();
        let err = validator
            .validate_describe_task_list_request(Some(&DescribeTaskListRequest {
                domain: "d".into(),
                task_list: Some(TaskList::default()),
                task_list_type: Some(cadence_api::TaskListType::Decision),
                ..Default::default()
            }))
            .unwrap_err();
        assert_eq!(err.message, "TaskList is not set on request.");

        let mut request = ListOpenWorkflowExecutionsRequest {
            domain: "d".into(),
            start_time_filter: Some(StartTimeFilter {
                earliest_time: Some(2),
                latest_time: Some(1),
            }),
            execution_filter: Some(WorkflowExecutionFilter::default()),
            type_filter: Some(WorkflowTypeFilter::default()),
            ..Default::default()
        };
        assert_eq!(
            validator
                .validate_list_open_workflow_executions_request(Some(&mut request))
                .unwrap_err()
                .message,
            "EarliestTime in StartTimeFilter should not be larger than LatestTime"
        );
    }

    #[test]
    fn reset_sticky_requires_execution_and_validates_identifiers() {
        let validator = RequestValidator::default();
        let err = validator
            .validate_reset_sticky_task_list_request(Some(&ResetStickyTaskListRequest {
                domain: "d".into(),
                execution: Some(WorkflowExecution::default()),
            }))
            .unwrap_err();
        assert_eq!(err.message, "WorkflowId is not set on request.");
    }

    #[test]
    fn every_go_validator_entry_point_rejects_a_nil_request() {
        let validator = RequestValidator::default();
        let errors = [
            validator
                .validate_refresh_workflow_tasks_request(None)
                .unwrap_err(),
            validator
                .validate_describe_task_list_request(None)
                .unwrap_err(),
            validator
                .validate_list_task_list_partitions_request(None)
                .unwrap_err(),
            validator
                .validate_get_task_lists_by_domain_request(None)
                .unwrap_err(),
            validator
                .validate_reset_sticky_task_list_request(None)
                .unwrap_err(),
            validator
                .validate_count_workflow_executions_request(None)
                .unwrap_err(),
            validator
                .validate_list_workflow_executions_request(None)
                .unwrap_err(),
            validator
                .validate_list_open_workflow_executions_request(None)
                .unwrap_err(),
            validator
                .validate_list_archived_workflow_executions_request(None)
                .unwrap_err(),
            validator
                .validate_list_closed_workflow_executions_request(None)
                .unwrap_err(),
            validator
                .validate_register_domain_request(None)
                .unwrap_err(),
            validator
                .validate_describe_domain_request(None)
                .unwrap_err(),
            validator.validate_update_domain_request(None).unwrap_err(),
            validator.validate_delete_domain_request(None).unwrap_err(),
            validator
                .validate_deprecate_domain_request(None)
                .unwrap_err(),
            validator
                .validate_failover_domain_request(None)
                .unwrap_err(),
        ];
        assert!(errors
            .iter()
            .all(|error| error.message == "Request is nil."));
    }

    // These cases mirror the Go table tests in request_validator_test.go.
    // Keep the assertion on the public BadRequest message: callers and SDKs
    // observe this text over both gRPC and Thrift transports.
    #[test]
    fn go_request_validator_error_and_mutation_matrix() {
        let validator = RequestValidator::default();
        let bad = |result: Result<(), BadRequestError>, expected: &str| {
            assert!(result.unwrap_err().message.contains(expected));
        };

        bad(
            validator.validate_refresh_workflow_tasks_request(Some(
                &RefreshWorkflowTasksRequest::default(),
            )),
            "Execution is not set on request.",
        );
        bad(
            validator.validate_describe_task_list_request(Some(&DescribeTaskListRequest {
                domain: String::new(),
                task_list: Some(TaskList {
                    name: "tl".into(),
                    ..Default::default()
                }),
                task_list_type: Some(cadence_api::TaskListType::Decision),
                ..Default::default()
            })),
            "Domain not set on request.",
        );
        bad(
            validator.validate_list_task_list_partitions_request(Some(
                &ListTaskListPartitionsRequest {
                    domain: "d".into(),
                    task_list: None,
                },
            )),
            "TaskList is not set on request.",
        );
        bad(
            validator.validate_list_task_list_partitions_request(Some(
                &ListTaskListPartitionsRequest {
                    domain: "d".into(),
                    task_list: Some(TaskList::default()),
                },
            )),
            "TaskList is not set on request.",
        );
        bad(
            validator.validate_get_task_lists_by_domain_request(Some(
                &GetTaskListsByDomainRequest {
                    domain: String::new(),
                },
            )),
            "Domain not set on request.",
        );
        bad(
            validator.validate_reset_sticky_task_list_request(Some(&ResetStickyTaskListRequest {
                domain: "d".into(),
                execution: Some(WorkflowExecution {
                    workflow_id: "wid".into(),
                    run_id: "bad".into(),
                }),
            })),
            "Invalid RunId.",
        );
        bad(
            validator.validate_count_workflow_executions_request(Some(
                &CountWorkflowExecutionsRequest {
                    domain: String::new(),
                    ..Default::default()
                },
            )),
            "Domain not set on request.",
        );

        let mut list = ListWorkflowExecutionsRequest {
            domain: "d".into(),
            page_size: 0,
            ..Default::default()
        };
        validator
            .validate_list_workflow_executions_request(Some(&mut list))
            .unwrap();
        assert_eq!(list.page_size, 1000);
        let mut open = ListOpenWorkflowExecutionsRequest {
            domain: "d".into(),
            ..Default::default()
        };
        bad(
            validator.validate_list_open_workflow_executions_request(Some(&mut open)),
            "StartTimeFilter is required",
        );
        let mut closed = ListClosedWorkflowExecutionsRequest {
            domain: "d".into(),
            ..Default::default()
        };
        bad(
            validator.validate_list_closed_workflow_executions_request(Some(&mut closed)),
            "StartTimeFilter is required",
        );
        let mut archived = ListArchivedWorkflowExecutionsRequest {
            domain: "d".into(),
            page_size: 0,
            ..Default::default()
        };
        validator
            .validate_list_archived_workflow_executions_request(Some(&mut archived))
            .unwrap();
        assert_eq!(archived.page_size, 1000);

        let mut protected = validator.clone();
        protected.config.admin_protection = true;
        protected.config.admin_operation_token = "secret".into();
        bad(
            protected.validate_register_domain_request(Some(&RegisterDomainRequest {
                name: "d".into(),
                ..Default::default()
            })),
            "No permission to do this operation.",
        );
        assert!(validator
            .validate_describe_domain_request(Some(&DescribeDomainRequest::default()))
            .is_err());
        bad(
            validator.validate_update_domain_request(Some(&UpdateDomainRequest {
                name: String::new(),
                ..Default::default()
            })),
            "Domain not set on request.",
        );
        bad(
            protected.validate_delete_domain_request(Some(&DeleteDomainRequest {
                name: "d".into(),
                ..Default::default()
            })),
            "No permission to do this operation.",
        );
        bad(
            protected.validate_deprecate_domain_request(Some(&DeprecateDomainRequest {
                name: "d".into(),
                ..Default::default()
            })),
            "No permission to do this operation.",
        );
        bad(
            validator.validate_failover_domain_request(Some(&FailoverDomainRequest {
                domain_name: "d".into(),
                ..Default::default()
            })),
            "DomainActiveClusterName or ActiveClusters must be provided",
        );
    }

    #[test]
    fn go_domain_presence_matrix_covers_every_domain_scoped_validator() {
        let validator = RequestValidator::default();
        let mut list = ListWorkflowExecutionsRequest::default();
        let mut open = ListOpenWorkflowExecutionsRequest::default();
        let mut closed = ListClosedWorkflowExecutionsRequest::default();
        let mut archived = ListArchivedWorkflowExecutionsRequest::default();
        let errors = [
            validator
                .validate_describe_task_list_request(Some(&DescribeTaskListRequest::default()))
                .unwrap_err(),
            validator
                .validate_list_task_list_partitions_request(Some(
                    &ListTaskListPartitionsRequest::default(),
                ))
                .unwrap_err(),
            validator
                .validate_get_task_lists_by_domain_request(Some(
                    &GetTaskListsByDomainRequest::default(),
                ))
                .unwrap_err(),
            validator
                .validate_reset_sticky_task_list_request(Some(
                    &ResetStickyTaskListRequest::default(),
                ))
                .unwrap_err(),
            validator
                .validate_count_workflow_executions_request(Some(
                    &CountWorkflowExecutionsRequest::default(),
                ))
                .unwrap_err(),
            validator
                .validate_list_workflow_executions_request(Some(&mut list))
                .unwrap_err(),
            validator
                .validate_list_open_workflow_executions_request(Some(&mut open))
                .unwrap_err(),
            validator
                .validate_list_closed_workflow_executions_request(Some(&mut closed))
                .unwrap_err(),
            validator
                .validate_list_archived_workflow_executions_request(Some(&mut archived))
                .unwrap_err(),
            validator
                .validate_register_domain_request(Some(&RegisterDomainRequest::default()))
                .unwrap_err(),
            validator
                .validate_describe_domain_request(Some(&DescribeDomainRequest::default()))
                .unwrap_err(),
            validator
                .validate_update_domain_request(Some(&UpdateDomainRequest::default()))
                .unwrap_err(),
            validator
                .validate_delete_domain_request(Some(&DeleteDomainRequest::default()))
                .unwrap_err(),
            validator
                .validate_deprecate_domain_request(Some(&DeprecateDomainRequest::default()))
                .unwrap_err(),
            validator
                .validate_failover_domain_request(Some(&FailoverDomainRequest::default()))
                .unwrap_err(),
        ];
        assert!(errors
            .iter()
            .all(|error| error.message == "Domain not set on request."));
    }

    #[test]
    fn go_request_validator_limit_filter_and_lockdown_matrix() {
        let mut validator = RequestValidator::default();
        validator.config.max_domain_name_length = 3;
        validator.config.max_task_list_name_length = 2;
        validator.config.max_retention_days = 5;
        validator.config.max_failover_timeout_seconds = 4;

        assert_eq!(
            validator
                .validate_describe_task_list_request(Some(&DescribeTaskListRequest {
                    domain: "d".into(),
                    task_list: Some(TaskList {
                        name: "long".into(),
                        ..Default::default()
                    }),
                    task_list_type: Some(cadence_api::TaskListType::Decision),
                    ..Default::default()
                }))
                .unwrap_err()
                .message,
            "TaskList length exceeds limit."
        );
        let time_filter = StartTimeFilter {
            earliest_time: Some(1),
            latest_time: Some(2),
        };
        let mut open = ListOpenWorkflowExecutionsRequest {
            domain: "d".into(),
            start_time_filter: Some(time_filter.clone()),
            execution_filter: Some(WorkflowExecutionFilter::default()),
            type_filter: Some(WorkflowTypeFilter::default()),
            ..Default::default()
        };
        assert_eq!(
            validator
                .validate_list_open_workflow_executions_request(Some(&mut open))
                .unwrap_err()
                .message,
            "Only one of ExecutionFilter or TypeFilter is allowed"
        );
        let mut closed = ListClosedWorkflowExecutionsRequest {
            domain: "d".into(),
            start_time_filter: Some(time_filter),
            execution_filter: Some(WorkflowExecutionFilter::default()),
            type_filter: Some(WorkflowTypeFilter::default()),
            ..Default::default()
        };
        assert_eq!(
            validator
                .validate_list_closed_workflow_executions_request(Some(&mut closed))
                .unwrap_err()
                .message,
            "Only one of ExecutionFilter, TypeFilter or StatusFilter is allowed"
        );

        let mut closed = ListClosedWorkflowExecutionsRequest {
            domain: "domain".into(),
            start_time_filter: Some(StartTimeFilter {
                earliest_time: Some(1),
                latest_time: Some(2),
            }),
            type_filter: Some(WorkflowTypeFilter::default()),
            status_filter: Some(cadence_api::enums::WorkflowExecutionCloseStatus::Failed),
            ..Default::default()
        };
        assert_eq!(
            validator
                .validate_list_closed_workflow_executions_request(Some(&mut closed))
                .unwrap_err()
                .message,
            "Only one of ExecutionFilter, TypeFilter or StatusFilter is allowed"
        );
        let mut archived = ListArchivedWorkflowExecutionsRequest {
            domain: "d".into(),
            page_size: 2,
            ..Default::default()
        };
        validator.config.visibility_archival_query_max_page_size = 1;
        assert_eq!(
            validator
                .validate_list_archived_workflow_executions_request(Some(&mut archived))
                .unwrap_err()
                .message,
            "Pagesize is larger than allowed 1"
        );

        assert_eq!(
            validator
                .validate_register_domain_request(Some(&RegisterDomainRequest {
                    name: "four".into(),
                    ..Default::default()
                }))
                .unwrap_err()
                .message,
            "Domain length exceeds limit."
        );
        assert_eq!(
            validator
                .validate_register_domain_request(Some(&RegisterDomainRequest {
                    name: "ok".into(),
                    workflow_execution_retention_period_in_days: 6,
                    ..Default::default()
                }))
                .unwrap_err()
                .message,
            "RetentionDays is invalid."
        );
        assert_eq!(
            validator
                .validate_update_domain_request(Some(&UpdateDomainRequest {
                    name: "d".into(),
                    configuration: Some(cadence_api::service_types::DomainConfigurationUpdate {
                        workflow_execution_retention_period_in_days: Some(6),
                        ..Default::default()
                    }),
                    ..Default::default()
                }))
                .unwrap_err()
                .message,
            "RetentionDays is invalid."
        );
        assert_eq!(
            validator
                .validate_failover_domain_request(Some(&FailoverDomainRequest {
                    domain_name: "d".into(),
                    domain_active_cluster_name: Some("cluster".into()),
                    failover_timeout_in_seconds: Some(5),
                    ..Default::default()
                }))
                .unwrap_err()
                .message,
            "FailoverTimeoutInSeconds 5 exceeds the maximum allowed 4"
        );
        validator.config.lockdown = true;
        assert_eq!(
            validator
                .validate_failover_domain_request(Some(&FailoverDomainRequest {
                    domain_name: "d".into(),
                    domain_active_cluster_name: Some("cluster".into()),
                    ..Default::default()
                }))
                .unwrap_err()
                .message,
            "Domain is not accepting fail overs at this time due to lockdown."
        );
    }

    #[test]
    fn go_request_validator_remaining_table_cases() {
        let mut validator = RequestValidator::default();
        validator.config.visibility_max_page_size = 10;
        validator.config.visibility_archival_query_max_page_size = 100;
        validator.config.es_index_max_result_window = 100;
        validator.config.max_domain_name_length = 10;
        validator.config.max_retention_days = 3;
        validator.config.max_failover_timeout_seconds = 300;
        validator.config.admin_protection = true;
        validator.config.admin_operation_token = "token".into();
        validator.config.required_domain_data_keys = vec!["tier".into()];
        validator.config.max_failover_timeout_seconds = 300;
        let valid_time_filter = || StartTimeFilter {
            earliest_time: Some(1),
            latest_time: Some(2),
        };
        let bad = |result: Result<(), BadRequestError>, expected: &str| {
            assert_eq!(result.unwrap_err().message, expected);
        };

        // Go's open/closed visibility tests cover all three time-filter
        // omissions independently, filter conflicts, and default mutation.
        let mut open = ListOpenWorkflowExecutionsRequest {
            domain: "domain".into(),
            start_time_filter: Some(valid_time_filter()),
            maximum_page_size: 0,
            ..Default::default()
        };
        validator
            .validate_list_open_workflow_executions_request(Some(&mut open))
            .unwrap();
        assert_eq!(open.maximum_page_size, 10);
        for filter in [
            StartTimeFilter::default(),
            StartTimeFilter {
                earliest_time: Some(1),
                ..Default::default()
            },
            StartTimeFilter {
                latest_time: Some(2),
                ..Default::default()
            },
            StartTimeFilter {
                earliest_time: Some(3),
                latest_time: Some(2),
            },
        ] {
            let mut request = ListOpenWorkflowExecutionsRequest {
                domain: "domain".into(),
                start_time_filter: Some(filter.clone()),
                ..Default::default()
            };
            assert!(validator
                .validate_list_open_workflow_executions_request(Some(&mut request))
                .is_err());
        }

        let valid_closed = || ListClosedWorkflowExecutionsRequest {
            domain: "domain".into(),
            start_time_filter: Some(valid_time_filter()),
            ..Default::default()
        };
        let mut closed = valid_closed();
        validator
            .validate_list_closed_workflow_executions_request(Some(&mut closed))
            .unwrap();
        assert_eq!(closed.maximum_page_size, 10);
        let mut closed_conflict = valid_closed();
        closed_conflict.execution_filter = Some(WorkflowExecutionFilter::default());
        closed_conflict.status_filter =
            Some(cadence_api::enums::WorkflowExecutionCloseStatus::Failed);
        bad(
            validator.validate_list_closed_workflow_executions_request(Some(&mut closed_conflict)),
            "Only one of ExecutionFilter, TypeFilter or StatusFilter is allowed",
        );
        let mut closed_conflict = valid_closed();
        closed_conflict.type_filter = Some(WorkflowTypeFilter::default());
        closed_conflict.status_filter =
            Some(cadence_api::enums::WorkflowExecutionCloseStatus::Failed);
        assert!(validator
            .validate_list_closed_workflow_executions_request(Some(&mut closed_conflict))
            .is_err());

        let mut archived = ListArchivedWorkflowExecutionsRequest {
            domain: "domain".into(),
            ..Default::default()
        };
        validator
            .validate_list_archived_workflow_executions_request(Some(&mut archived))
            .unwrap();
        assert_eq!(archived.page_size, 10);
        let mut archived_too_large = ListArchivedWorkflowExecutionsRequest {
            domain: "domain".into(),
            page_size: 101,
            ..Default::default()
        };
        bad(
            validator
                .validate_list_archived_workflow_executions_request(Some(&mut archived_too_large)),
            "Pagesize is larger than allowed 100",
        );

        // Domain tests cover successful identifier alternatives, all guarded
        // operations, required registration data, and failover shape variants.
        validator
            .validate_describe_domain_request(Some(&DescribeDomainRequest {
                name: Some("domain".into()),
                ..Default::default()
            }))
            .unwrap();
        validator
            .validate_describe_domain_request(Some(&DescribeDomainRequest {
                uuid: Some("uuid".into()),
                ..Default::default()
            }))
            .unwrap();
        validator
            .validate_register_domain_request(Some(&RegisterDomainRequest {
                name: "domain".into(),
                security_token: "token".into(),
                data: Some(std::collections::HashMap::from([(
                    "tier".into(),
                    "3".into(),
                )])),
                ..Default::default()
            }))
            .unwrap();
        bad(
            validator.validate_register_domain_request(Some(&RegisterDomainRequest {
                name: "domain".into(),
                security_token: "token".into(),
                ..Default::default()
            })),
            "domain data error, missing required key tier . All required keys: map[tier:true]",
        );
        validator.config.admin_protection = true;
        validator.config.admin_operation_token = "token".into();
        bad(
            validator.validate_register_domain_request(Some(&RegisterDomainRequest {
                name: "domain".into(),
                security_token: "wrong".into(),
                data: Some(std::collections::HashMap::from([(
                    "tier".into(),
                    "3".into(),
                )])),
                ..Default::default()
            })),
            "No permission to do this operation.",
        );
        validator
            .validate_update_domain_request(Some(&UpdateDomainRequest {
                name: "domain".into(),
                security_token: "token".into(),
                ..Default::default()
            }))
            .unwrap();
        bad(
            validator.validate_update_domain_request(Some(&UpdateDomainRequest {
                name: "domain".into(),
                security_token: "wrong".into(),
                ..Default::default()
            })),
            "No permission to do this operation.",
        );
        bad(
            validator.validate_delete_domain_request(Some(&DeleteDomainRequest {
                name: "domain".into(),
                security_token: "wrong".into(),
            })),
            "No permission to do this operation.",
        );
        bad(
            validator.validate_deprecate_domain_request(Some(&DeprecateDomainRequest {
                name: "domain".into(),
                security_token: "wrong".into(),
            })),
            "No permission to do this operation.",
        );
        let active_active = cadence_api::types::ActiveClusters::default();
        validator
            .validate_failover_domain_request(Some(&FailoverDomainRequest {
                domain_name: "domain".into(),
                active_clusters: Some(active_active),
                ..Default::default()
            }))
            .unwrap();
        validator
            .validate_failover_domain_request(Some(&FailoverDomainRequest {
                domain_name: "domain".into(),
                domain_active_cluster_name: Some(String::new()),
                failover_timeout_in_seconds: Some(120),
                ..Default::default()
            }))
            .unwrap();
        bad(
            validator.validate_failover_domain_request(Some(&FailoverDomainRequest {
                domain_name: "domain".into(),
                domain_active_cluster_name: Some("c".into()),
                failover_timeout_in_seconds: Some(0),
                ..Default::default()
            })),
            "FailoverTimeoutInSeconds must be positive",
        );
        bad(
            validator.validate_failover_domain_request(Some(&FailoverDomainRequest {
                domain_name: "domain".into(),
                domain_active_cluster_name: Some("c".into()),
                failover_timeout_in_seconds: Some(-1),
                ..Default::default()
            })),
            "FailoverTimeoutInSeconds must be positive",
        );

        // A configured dynamic overlay is consulted on every call, as Go's
        // dynamic-property closures are, rather than being snapshotted.
        let overlay = Arc::new(Mutex::new(
            cadence_service_core::admin::DynamicConfigOverlay::default(),
        ));
        let dynamic = RequestValidator::default().with_dynamic_config(overlay.clone());
        let mut page = ListWorkflowExecutionsRequest {
            domain: "d".into(),
            page_size: 0,
            ..Default::default()
        };
        dynamic
            .validate_list_workflow_executions_request(Some(&mut page))
            .unwrap();
        assert_eq!(page.page_size, 1000);
        overlay.lock().unwrap().update(
            false,
            "frontend.visibilityMaxPageSize",
            vec![DynamicConfigValue {
                value: Some(DataBlob::new(
                    EncodingType::JSON,
                    serde_json::to_vec(&serde_json::json!(7)).unwrap(),
                )),
                filters: vec![],
            }],
        );
        let mut page = ListWorkflowExecutionsRequest {
            domain: "d".into(),
            page_size: 0,
            ..Default::default()
        };
        dynamic
            .validate_list_workflow_executions_request(Some(&mut page))
            .unwrap();
        assert_eq!(page.page_size, 7);
        overlay.lock().unwrap().update(
            false,
            "system.requiredDomainDataKeys",
            vec![DynamicConfigValue {
                value: Some(DataBlob::new(
                    EncodingType::JSON,
                    serde_json::to_vec(&serde_json::json!({"tier": true})).unwrap(),
                )),
                filters: vec![],
            }],
        );
        assert_eq!(
            dynamic
                .validate_register_domain_request(Some(&RegisterDomainRequest {
                    name: "d".into(),
                    ..Default::default()
                }))
                .unwrap_err()
                .message,
            "domain data error, missing required key tier . All required keys: map[tier:true]"
        );
    }

    #[test]
    fn go_request_validator_success_cases_cover_all_entry_points() {
        let validator = RequestValidator::default();
        let execution = WorkflowExecution {
            workflow_id: "wf".into(),
            run_id: "".into(),
        };
        validator
            .validate_refresh_workflow_tasks_request(Some(&RefreshWorkflowTasksRequest {
                domain: "domain".into(),
                execution: Some(execution.clone()),
            }))
            .unwrap();
        validator
            .validate_describe_task_list_request(Some(&DescribeTaskListRequest {
                domain: "domain".into(),
                task_list: Some(TaskList {
                    name: "tl".into(),
                    ..Default::default()
                }),
                task_list_type: Some(cadence_api::TaskListType::Activity),
                ..Default::default()
            }))
            .unwrap();
        validator
            .validate_list_task_list_partitions_request(Some(&ListTaskListPartitionsRequest {
                domain: "domain".into(),
                task_list: Some(TaskList {
                    name: "tl".into(),
                    ..Default::default()
                }),
            }))
            .unwrap();
        validator
            .validate_get_task_lists_by_domain_request(Some(&GetTaskListsByDomainRequest {
                domain: "domain".into(),
            }))
            .unwrap();
        validator
            .validate_reset_sticky_task_list_request(Some(&ResetStickyTaskListRequest {
                domain: "domain".into(),
                execution: Some(execution),
            }))
            .unwrap();
        validator
            .validate_count_workflow_executions_request(Some(&CountWorkflowExecutionsRequest {
                domain: "domain".into(),
                ..Default::default()
            }))
            .unwrap();
        let mut list = ListWorkflowExecutionsRequest {
            domain: "domain".into(),
            ..Default::default()
        };
        validator
            .validate_list_workflow_executions_request(Some(&mut list))
            .unwrap();
        let mut open = ListOpenWorkflowExecutionsRequest {
            domain: "domain".into(),
            start_time_filter: Some(StartTimeFilter {
                earliest_time: Some(1),
                latest_time: Some(2),
            }),
            ..Default::default()
        };
        validator
            .validate_list_open_workflow_executions_request(Some(&mut open))
            .unwrap();
        let mut closed = ListClosedWorkflowExecutionsRequest {
            domain: "domain".into(),
            start_time_filter: Some(StartTimeFilter {
                earliest_time: Some(1),
                latest_time: Some(2),
            }),
            ..Default::default()
        };
        validator
            .validate_list_closed_workflow_executions_request(Some(&mut closed))
            .unwrap();
        let mut archived = ListArchivedWorkflowExecutionsRequest {
            domain: "domain".into(),
            ..Default::default()
        };
        validator
            .validate_list_archived_workflow_executions_request(Some(&mut archived))
            .unwrap();
        validator
            .validate_register_domain_request(Some(&RegisterDomainRequest {
                name: "domain".into(),
                ..Default::default()
            }))
            .unwrap();
        validator
            .validate_describe_domain_request(Some(&DescribeDomainRequest {
                name: Some("domain".into()),
                ..Default::default()
            }))
            .unwrap();
        validator
            .validate_update_domain_request(Some(&UpdateDomainRequest {
                name: "domain".into(),
                ..Default::default()
            }))
            .unwrap();
        validator
            .validate_delete_domain_request(Some(&DeleteDomainRequest {
                name: "domain".into(),
                ..Default::default()
            }))
            .unwrap();
        validator
            .validate_deprecate_domain_request(Some(&DeprecateDomainRequest {
                name: "domain".into(),
                ..Default::default()
            }))
            .unwrap();
        validator
            .validate_failover_domain_request(Some(&FailoverDomainRequest {
                domain_name: "domain".into(),
                domain_active_cluster_name: Some("cluster0".into()),
                ..Default::default()
            }))
            .unwrap();
    }

    #[test]
    fn go_visibility_matrix_checks_exact_time_filter_and_filter_conflict_errors() {
        let validator = RequestValidator::default();
        let expected = [
            (
                StartTimeFilter::default(),
                "EarliestTime in StartTimeFilter is required",
            ),
            (
                StartTimeFilter {
                    earliest_time: Some(1),
                    ..Default::default()
                },
                "LatestTime in StartTimeFilter is required",
            ),
            (
                StartTimeFilter {
                    latest_time: Some(2),
                    ..Default::default()
                },
                "EarliestTime in StartTimeFilter is required",
            ),
            (
                StartTimeFilter {
                    earliest_time: Some(3),
                    latest_time: Some(2),
                },
                "EarliestTime in StartTimeFilter should not be larger than LatestTime",
            ),
        ];
        for (filter, message) in expected {
            let mut request = ListOpenWorkflowExecutionsRequest {
                domain: "domain".into(),
                start_time_filter: Some(filter.clone()),
                ..Default::default()
            };
            assert_eq!(
                validator
                    .validate_list_open_workflow_executions_request(Some(&mut request))
                    .unwrap_err()
                    .message,
                message
            );
            let mut request = ListClosedWorkflowExecutionsRequest {
                domain: "domain".into(),
                start_time_filter: Some(filter),
                ..Default::default()
            };
            assert_eq!(
                validator
                    .validate_list_closed_workflow_executions_request(Some(&mut request))
                    .unwrap_err()
                    .message,
                message
            );
        }

        let time_filter = StartTimeFilter {
            earliest_time: Some(1),
            latest_time: Some(2),
        };
        let mut open = ListOpenWorkflowExecutionsRequest {
            domain: "domain".into(),
            start_time_filter: Some(time_filter.clone()),
            execution_filter: Some(WorkflowExecutionFilter::default()),
            type_filter: Some(WorkflowTypeFilter::default()),
            ..Default::default()
        };
        assert_eq!(
            validator
                .validate_list_open_workflow_executions_request(Some(&mut open))
                .unwrap_err()
                .message,
            "Only one of ExecutionFilter or TypeFilter is allowed"
        );

        let mut closed = ListClosedWorkflowExecutionsRequest {
            domain: "domain".into(),
            start_time_filter: Some(time_filter),
            execution_filter: Some(WorkflowExecutionFilter::default()),
            status_filter: Some(cadence_api::enums::WorkflowExecutionCloseStatus::Failed),
            ..Default::default()
        };
        assert_eq!(
            validator
                .validate_list_closed_workflow_executions_request(Some(&mut closed))
                .unwrap_err()
                .message,
            "Only one of ExecutionFilter, TypeFilter or StatusFilter is allowed"
        );

        let mut open = ListOpenWorkflowExecutionsRequest::default();
        assert_eq!(
            validator
                .validate_list_open_workflow_executions_request(Some(&mut open))
                .unwrap_err()
                .message,
            "Domain not set on request."
        );
        let mut closed = ListClosedWorkflowExecutionsRequest::default();
        assert_eq!(
            validator
                .validate_list_closed_workflow_executions_request(Some(&mut closed))
                .unwrap_err()
                .message,
            "Domain not set on request."
        );
    }

    #[test]
    fn go_validator_matrix_covers_task_list_reset_and_failover_boundaries() {
        let mut validator = RequestValidator::default();
        validator.config.max_task_list_name_length = 2;

        let mut describe = DescribeTaskListRequest {
            domain: "domain".into(),
            task_list: None,
            task_list_type: None,
            include_task_list_status: false,
        };
        assert_eq!(
            validator
                .validate_describe_task_list_request(Some(&describe))
                .unwrap_err()
                .message,
            "TaskListType is not set on request."
        );
        describe.task_list_type = Some(cadence_api::TaskListType::Decision);
        assert_eq!(
            validator
                .validate_describe_task_list_request(Some(&describe))
                .unwrap_err()
                .message,
            "TaskList is not set on request."
        );
        describe.task_list = Some(TaskList {
            name: "long".into(),
            kind: None,
            base_name: String::new(),
        });
        assert_eq!(
            validator
                .validate_describe_task_list_request(Some(&describe))
                .unwrap_err()
                .message,
            "TaskList length exceeds limit."
        );

        let partitions = ListTaskListPartitionsRequest {
            domain: "domain".into(),
            task_list: Some(TaskList {
                name: "long".into(),
                kind: None,
                base_name: String::new(),
            }),
        };
        assert_eq!(
            validator
                .validate_list_task_list_partitions_request(Some(&partitions))
                .unwrap_err()
                .message,
            "TaskList length exceeds limit."
        );

        let mut reset = ResetStickyTaskListRequest {
            domain: String::new(),
            execution: None,
        };
        assert_eq!(
            validator
                .validate_reset_sticky_task_list_request(Some(&reset))
                .unwrap_err()
                .message,
            "Domain not set on request."
        );
        reset.domain = "domain".into();
        assert_eq!(
            validator
                .validate_reset_sticky_task_list_request(Some(&reset))
                .unwrap_err()
                .message,
            "Execution is not set on request."
        );

        let mut failover = FailoverDomainRequest {
            domain_name: String::new(),
            domain_active_cluster_name: None,
            active_clusters: None,
            reason: None,
            failover_timeout_in_seconds: None,
            skip_destination_cluster_check: false,
        };
        assert_eq!(
            validator
                .validate_failover_domain_request(Some(&failover))
                .unwrap_err()
                .message,
            "Domain not set on request."
        );
        failover.domain_name = "domain".into();
        assert_eq!(
            validator
                .validate_failover_domain_request(Some(&failover))
                .unwrap_err()
                .message,
            "DomainActiveClusterName or ActiveClusters must be provided to failover the domain"
        );
        validator.config.lockdown = true;
        failover.active_clusters = Some(cadence_api::types::ActiveClusters::default());
        assert_eq!(
            validator
                .validate_failover_domain_request(Some(&failover))
                .unwrap_err()
                .message,
            "Domain is not accepting fail overs at this time due to lockdown."
        );
    }

    #[test]
    fn go_table_covers_visibility_limits_and_update_failover_lockdown() {
        let overlay = Arc::new(Mutex::new(
            cadence_service_core::admin::DynamicConfigOverlay::default(),
        ));
        overlay.lock().unwrap().update(
            false,
            "system.readVisibilityStoreName",
            vec![DynamicConfigValue {
                value: Some(DataBlob::new(
                    EncodingType::JSON,
                    serde_json::to_vec(&serde_json::json!("es")).unwrap(),
                )),
                filters: vec![DynamicConfigFilter {
                    name: "domainName".into(),
                    value: Some(DataBlob::new(
                        EncodingType::JSON,
                        serde_json::to_vec(&serde_json::json!("domain")).unwrap(),
                    )),
                }],
            }],
        );
        let mut validator = RequestValidator::default().with_dynamic_config(overlay);
        validator.config.advanced_visibility_store = true;
        validator.config.es_index_max_result_window = 100;

        let expected = "Pagesize is larger than allow 100";
        let mut list = ListWorkflowExecutionsRequest {
            domain: "domain".into(),
            page_size: 101,
            ..Default::default()
        };
        assert_eq!(
            validator
                .validate_list_workflow_executions_request(Some(&mut list))
                .unwrap_err()
                .message,
            expected
        );
        let time_filter = StartTimeFilter {
            earliest_time: Some(1),
            latest_time: Some(2),
        };
        let mut open = ListOpenWorkflowExecutionsRequest {
            domain: "domain".into(),
            start_time_filter: Some(time_filter.clone()),
            maximum_page_size: 101,
            ..Default::default()
        };
        assert_eq!(
            validator
                .validate_list_open_workflow_executions_request(Some(&mut open))
                .unwrap_err()
                .message,
            expected
        );
        let mut closed = ListClosedWorkflowExecutionsRequest {
            domain: "domain".into(),
            start_time_filter: Some(time_filter),
            maximum_page_size: 101,
            ..Default::default()
        };
        assert_eq!(
            validator
                .validate_list_closed_workflow_executions_request(Some(&mut closed))
                .unwrap_err()
                .message,
            expected
        );

        validator.config.admin_protection = true;
        validator.config.lockdown = true;
        let expected_lockdown = "Domain is not accepting fail overs at this time due to lockdown.";
        for replication in [
            cadence_api::service_types::DomainReplicationConfigurationUpdate {
                active_cluster_name: Some("cluster1".into()),
                ..Default::default()
            },
            cadence_api::service_types::DomainReplicationConfigurationUpdate {
                active_clusters: Some(cadence_api::types::ActiveClusters::default()),
                ..Default::default()
            },
        ] {
            assert_eq!(
                validator
                    .validate_update_domain_request(Some(&UpdateDomainRequest {
                        name: "domain".into(),
                        replication_configuration: Some(replication),
                        ..Default::default()
                    }))
                    .unwrap_err()
                    .message,
                expected_lockdown
            );
        }
    }

    #[test]
    fn go_failover_timeout_table_case_600_exceeds_maximum_300() {
        let mut validator = RequestValidator::default();
        validator.config.max_failover_timeout_seconds = 300;
        let error = validator
            .validate_failover_domain_request(Some(&FailoverDomainRequest {
                domain_name: "domain".into(),
                domain_active_cluster_name: Some("cluster0".into()),
                failover_timeout_in_seconds: Some(600),
                ..Default::default()
            }))
            .unwrap_err();
        assert_eq!(
            error.message,
            "FailoverTimeoutInSeconds 600 exceeds the maximum allowed 300"
        );
    }

    #[test]
    fn go_failover_domain_table_success_and_lockdown_rows() {
        let validator = RequestValidator::default();
        let active_active = cadence_api::types::ActiveClusters {
            active_clusters_by_cluster_attribute: Some(std::collections::HashMap::from([(
                "region".to_string(),
                cadence_api::types::ClusterAttributeScope {
                    cluster_attributes: Some(std::collections::HashMap::from([(
                        "region0".to_string(),
                        cadence_api::types::ActiveClusterInfo {
                            active_cluster_name: "cluster0".into(),
                            failover_version: 1,
                        },
                    )])),
                },
            )])),
            ..Default::default()
        };
        for request in [
            FailoverDomainRequest {
                domain_name: "domain".into(),
                domain_active_cluster_name: Some("cluster0".into()),
                ..Default::default()
            },
            FailoverDomainRequest {
                domain_name: "domain".into(),
                active_clusters: Some(active_active.clone()),
                ..Default::default()
            },
            FailoverDomainRequest {
                domain_name: "domain".into(),
                domain_active_cluster_name: Some("cluster0".into()),
                failover_timeout_in_seconds: Some(120),
                ..Default::default()
            },
            FailoverDomainRequest {
                domain_name: "domain".into(),
                domain_active_cluster_name: Some("cluster0".into()),
                failover_timeout_in_seconds: Some(60),
                ..Default::default()
            },
        ] {
            assert!(validator
                .validate_failover_domain_request(Some(&request))
                .is_ok());
        }

        let mut lockdown = validator;
        lockdown.config.lockdown = true;
        for request in [
            FailoverDomainRequest {
                domain_name: "domain".into(),
                domain_active_cluster_name: Some("cluster0".into()),
                ..Default::default()
            },
            FailoverDomainRequest {
                domain_name: "domain".into(),
                active_clusters: Some(active_active),
                ..Default::default()
            },
        ] {
            assert_eq!(
                lockdown
                    .validate_failover_domain_request(Some(&request))
                    .unwrap_err()
                    .message,
                "Domain is not accepting fail overs at this time due to lockdown."
            );
        }
    }
}
