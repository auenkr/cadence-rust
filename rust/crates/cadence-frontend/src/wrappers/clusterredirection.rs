//! Cluster redirection policy constants and routing decisions.

#![allow(clippy::result_large_err)]
//!
//! The Go frontend has three selected API allowlists. Keep the versions
//! separate: they are configuration contracts and have evolved independently.

use std::collections::HashSet;
use std::future::Future;
use std::time::Instant;

use cadence_api::errors::{CadenceError, DomainNotActiveError};

pub const POLICY_DEFAULT: &str = "";
pub const POLICY_NOOP: &str = "noop";
pub const POLICY_SELECTED_APIS_FORWARDING: &str = "selected-apis-forwarding";
pub const POLICY_SELECTED_APIS_FORWARDING_V2: &str = "selected-apis-forwarding-v2";
pub const POLICY_ALL_DOMAIN_APIS_FORWARDING: &str = "all-domain-apis-forwarding";
pub const POLICY_ALL_DOMAIN_APIS_FORWARDING_V2: &str = "all-domain-apis-forwarding-v2";
pub const POLICY_SELECTED_APIS_FORWARDING_V3: &str = "selected-apis-forwarding-v3";
pub const POLICY_ALL_DOMAIN_APIS_FORWARDING_V3: &str = "all-domain-apis-forwarding-v3";

const SELECTED_V1: &[&str] = &[
    "StartWorkflowExecution",
    "SignalWithStartWorkflowExecution",
    "SignalWorkflowExecution",
    "RequestCancelWorkflowExecution",
    "TerminateWorkflowExecution",
    "ResetWorkflowExecution",
    "CreateSchedule",
    "DeleteSchedule",
    "UpdateSchedule",
    "PauseSchedule",
    "UnpauseSchedule",
    "BackfillSchedule",
];

const SELECTED_V2_ADDITIONS: &[&str] = &[
    "RespondActivityTaskCanceled",
    "RespondActivityTaskCanceledByID",
    "RespondActivityTaskCompleted",
    "RespondActivityTaskCompletedByID",
    "RespondActivityTaskFailed",
    "RespondActivityTaskFailedByID",
];

const SELECTED_V3_ADDITIONS: &[&str] = &[
    "StartWorkflowExecutionAsync",
    "SignalWithStartWorkflowExecutionAsync",
];

const ALLOWED_ON_DEPRECATED_DOMAIN: &[&str] = &[
    "ListWorkflowExecutions",
    "CountWorkflowExecutions",
    "ScanWorkflowExecutions",
    "TerminateWorkflowExecution",
];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ForwardingPolicy {
    Noop,
    SelectedV1,
    SelectedV2,
    SelectedV3,
    AllDomainV1,
    AllDomainV2,
    AllDomainV3,
}

impl ForwardingPolicy {
    /// Resolve a Go configuration policy string. Unknown values are a startup
    /// configuration error in Rust rather than Go's generator panic.
    pub fn from_name(value: &str) -> Option<Self> {
        Some(match value {
            POLICY_DEFAULT | POLICY_NOOP => Self::Noop,
            POLICY_SELECTED_APIS_FORWARDING => Self::SelectedV1,
            POLICY_SELECTED_APIS_FORWARDING_V2 => Self::SelectedV2,
            POLICY_SELECTED_APIS_FORWARDING_V3 => Self::SelectedV3,
            POLICY_ALL_DOMAIN_APIS_FORWARDING => Self::AllDomainV1,
            POLICY_ALL_DOMAIN_APIS_FORWARDING_V2 => Self::AllDomainV2,
            POLICY_ALL_DOMAIN_APIS_FORWARDING_V3 => Self::AllDomainV3,
            _ => return None,
        })
    }

    pub fn is_all_domain(self) -> bool {
        matches!(
            self,
            Self::AllDomainV1 | Self::AllDomainV2 | Self::AllDomainV3
        )
    }

    pub fn allows_api(self, api: &str) -> bool {
        match self {
            Self::Noop => false,
            Self::SelectedV1 | Self::AllDomainV1 => SELECTED_V1.contains(&api),
            Self::SelectedV2 | Self::AllDomainV2 => {
                SELECTED_V1.contains(&api) || SELECTED_V2_ADDITIONS.contains(&api)
            }
            Self::SelectedV3 | Self::AllDomainV3 => {
                SELECTED_V1.contains(&api)
                    || SELECTED_V2_ADDITIONS.contains(&api)
                    || SELECTED_V3_ADDITIONS.contains(&api)
            }
        }
    }

    /// Go routes all APIs for all-domain policies when no target cluster is
    /// configured or when the configured target is the active cluster. In
    /// other cases it falls back to the selected API allowlist.
    pub fn routes_to_active_cluster(
        self,
        api: &str,
        strong_consistency: bool,
        active_active_domain: bool,
        configured_target: &str,
        active_cluster: &str,
    ) -> bool {
        if self == Self::Noop {
            return false;
        }
        if active_active_domain && strong_consistency {
            return true;
        }
        if self.is_all_domain()
            && (configured_target.is_empty() || configured_target == active_cluster)
        {
            return true;
        }
        self.allows_api(api)
    }
}

pub fn allowed_on_deprecated_domain(api: &str) -> bool {
    ALLOWED_ON_DEPRECATED_DOMAIN.contains(&api)
}

/// Construct the exact set when call sites need membership lookup repeatedly.
pub fn api_allowlist(policy: ForwardingPolicy) -> HashSet<&'static str> {
    match policy {
        ForwardingPolicy::Noop => HashSet::new(),
        ForwardingPolicy::SelectedV1 | ForwardingPolicy::AllDomainV1 => {
            SELECTED_V1.iter().copied().collect()
        }
        ForwardingPolicy::SelectedV2 | ForwardingPolicy::AllDomainV2 => SELECTED_V1
            .iter()
            .chain(SELECTED_V2_ADDITIONS)
            .copied()
            .collect(),
        ForwardingPolicy::SelectedV3 | ForwardingPolicy::AllDomainV3 => SELECTED_V1
            .iter()
            .chain(SELECTED_V2_ADDITIONS)
            .chain(SELECTED_V3_ADDITIONS)
            .copied()
            .collect(),
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DomainState {
    pub id: String,
    pub name: String,
    pub global: bool,
    pub deprecated_or_deleted: bool,
    pub active_cluster: String,
    pub active_active: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct WorkflowRef {
    pub workflow_id: String,
    pub run_id: String,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum Consistency {
    #[default]
    Eventual,
    Strong,
}

pub trait RuntimeConfig: Send + Sync {
    fn domain_auto_forwarding_enabled(&self, domain: &str) -> bool;
}

pub trait ActiveClusterManager: Send + Sync {
    fn active_cluster_for_workflow(
        &self,
        domain_id: &str,
        workflow_id: &str,
        run_id: &str,
    ) -> Result<String, String>;
    fn active_cluster_for_cluster_attribute(
        &self,
        domain_id: &str,
        cluster_attribute: &str,
    ) -> Result<String, String>;
    fn current_workflow_selection(
        &self,
        domain_id: &str,
        workflow_id: &str,
    ) -> Result<(bool, Option<String>), String>;
}

pub trait RedirectMetrics: Send + Sync {
    fn record_forward(&self, record: &ForwardRecord);
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ForwardRecord {
    pub operation: String,
    pub domain: String,
    pub source_cluster: String,
    pub target_cluster: String,
    /// Set when the active cluster returned by DomainNotActive was attached
    /// to Go's forwarding-policy metric scope.
    pub active_cluster: Option<String>,
    pub active_active: bool,
    pub strong_consistency: bool,
    pub elapsed_micros: u128,
    pub succeeded: bool,
}

/// Stateful policy counterpart to Go `selectedOrAllAPIsForwarding...Redirect`.
/// Lookup failures intentionally route to the current cluster, as Go does.
#[allow(clippy::too_many_arguments)]
pub fn redirect<C, M, F>(
    policy: ForwardingPolicy,
    current_cluster: &str,
    configured_target: &str,
    domain: &DomainState,
    execution: Option<&WorkflowRef>,
    requested_cluster_attribute: Option<&str>,
    operation: &str,
    consistency: Consistency,
    config: &C,
    manager: &M,
    metrics: &dyn RedirectMetrics,
    mut call: F,
) -> Result<(), CadenceError>
where
    C: RuntimeConfig,
    M: ActiveClusterManager,
    F: FnMut(&str) -> Result<(), CadenceError>,
{
    if domain.deprecated_or_deleted && !allowed_on_deprecated_domain(operation) {
        return Err(CadenceError::DomainNotActive(DomainNotActiveError {
            message: "domain is deprecated or deleted.".into(),
            domain_name: domain.name.clone(),
            current_cluster: current_cluster.into(),
            active_cluster: current_cluster.into(),
            active_clusters: Vec::new(),
        }));
    }
    if policy == ForwardingPolicy::Noop {
        return call(current_cluster);
    }
    let auto_forward = domain.global && config.domain_auto_forwarding_enabled(&domain.name);
    let mut target = current_cluster.to_owned();
    let mut should_forward_after_inactive = false;

    if auto_forward {
        let mut active_cluster = domain.active_cluster.clone();
        if domain.active_active
            && (policy.is_all_domain()
                || policy.allows_api(operation)
                || consistency == Consistency::Strong)
        {
            active_cluster = active_active_target(
                manager,
                domain,
                execution,
                requested_cluster_attribute,
                operation,
                current_cluster,
            );
        }

        if (policy.is_all_domain()
            && (configured_target.is_empty() || configured_target == active_cluster))
            || consistency == Consistency::Strong
            || policy.allows_api(operation)
        {
            target = active_cluster;
            should_forward_after_inactive = true;
        }
    }

    let start = Instant::now();
    let mut result = call(&target);
    let mut active_cluster_metric = None;

    if let Err(CadenceError::DomainNotActive(ref error)) = result {
        if should_forward_after_inactive {
            active_cluster_metric = Some(error.active_cluster.clone());
        }
        if should_forward_after_inactive
            && !error.active_cluster.is_empty()
            && error.active_cluster != target
        {
            let fallback = error.active_cluster.clone();
            result = call(&fallback);
        }
    }
    metrics.record_forward(&ForwardRecord {
        operation: operation.into(),
        domain: domain.name.clone(),
        source_cluster: current_cluster.into(),
        target_cluster: target,
        active_cluster: active_cluster_metric,
        active_active: domain.active_active,
        strong_consistency: consistency == Consistency::Strong,
        elapsed_micros: start.elapsed().as_micros(),
        succeeded: result.is_ok(),
    });
    result
}

/// Async counterpart used by the Frontend handler decorator. The forwarding
/// choice, DomainNotActive retry and metrics mirror `redirect`; only the call
/// boundary is asynchronous so generated gRPC clients can be used directly.
#[allow(clippy::too_many_arguments)]
pub async fn redirect_async<T, C, M, F, Fut>(
    policy: ForwardingPolicy,
    current_cluster: &str,
    configured_target: &str,
    domain: &DomainState,
    execution: Option<&WorkflowRef>,
    requested_cluster_attribute: Option<&str>,
    operation: &str,
    consistency: Consistency,
    config: &C,
    manager: &M,
    metrics: &dyn RedirectMetrics,
    mut call: F,
) -> Result<T, CadenceError>
where
    C: RuntimeConfig,
    M: ActiveClusterManager,
    F: FnMut(String) -> Fut,
    Fut: Future<Output = Result<T, CadenceError>>,
{
    if domain.deprecated_or_deleted && !allowed_on_deprecated_domain(operation) {
        return Err(CadenceError::DomainNotActive(DomainNotActiveError {
            message: "domain is deprecated or deleted.".into(),
            domain_name: domain.name.clone(),
            current_cluster: current_cluster.into(),
            active_cluster: current_cluster.into(),
            active_clusters: Vec::new(),
        }));
    }
    if policy == ForwardingPolicy::Noop {
        return call(current_cluster.to_owned()).await;
    }
    let auto_forward = domain.global && config.domain_auto_forwarding_enabled(&domain.name);
    let mut target = current_cluster.to_owned();
    let mut should_forward_after_inactive = false;
    if auto_forward {
        let mut active_cluster = domain.active_cluster.clone();
        if domain.active_active
            && (policy.is_all_domain()
                || policy.allows_api(operation)
                || consistency == Consistency::Strong)
        {
            active_cluster = active_active_target(
                manager,
                domain,
                execution,
                requested_cluster_attribute,
                operation,
                current_cluster,
            );
        }
        if (policy.is_all_domain()
            && (configured_target.is_empty() || configured_target == active_cluster))
            || consistency == Consistency::Strong
            || policy.allows_api(operation)
        {
            target = active_cluster;
            should_forward_after_inactive = true;
        }
    }

    let start = Instant::now();
    let mut result = call(target.clone()).await;
    let mut active_cluster_metric = None;
    if let Err(CadenceError::DomainNotActive(ref error)) = result {
        if should_forward_after_inactive {
            active_cluster_metric = Some(error.active_cluster.clone());
        }
        if should_forward_after_inactive
            && !error.active_cluster.is_empty()
            && error.active_cluster != target
        {
            let fallback = error.active_cluster.clone();
            result = call(fallback).await;
        }
    }
    metrics.record_forward(&ForwardRecord {
        operation: operation.into(),
        domain: domain.name.clone(),
        source_cluster: current_cluster.into(),
        target_cluster: target,
        active_cluster: active_cluster_metric,
        active_active: domain.active_active,
        strong_consistency: consistency == Consistency::Strong,
        elapsed_micros: start.elapsed().as_micros(),
        succeeded: result.is_ok(),
    });
    result
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RedirectRequest {
    pub domain: DomainState,
    pub execution: Option<WorkflowRef>,
    pub requested_cluster_attribute: Option<String>,
    pub consistency: Consistency,
}

/// Runtime adapter for the generated cluster-redirection wrapper. The domain
/// resolver owns domain-cache lookups and extracts execution/cluster-selection
/// attributes using the Go request semantics.
pub trait RedirectRequestResolver: Send + Sync {
    fn resolve(
        &self,
        operation: &str,
        request: &serde_json::Value,
    ) -> Result<Option<RedirectRequest>, CadenceError>;
}

pub struct WorkflowRedirector<C, M, R> {
    pub policy: ForwardingPolicy,
    pub current_cluster: String,
    pub configured_target: String,
    pub config: C,
    pub manager: M,
    pub metrics: std::sync::Arc<dyn RedirectMetrics>,
    pub requests: R,
}

impl<C, M, R> crate::handler_workflow::RequestRedirector for WorkflowRedirector<C, M, R>
where
    C: RuntimeConfig + 'static,
    M: ActiveClusterManager + 'static,
    R: RedirectRequestResolver + 'static,
{
    fn invoke<'a>(
        &'a self,
        operation: crate::handler_workflow::WorkflowOperation,
        request: &'a serde_json::Value,
        mut next: crate::handler_workflow::WorkflowNext<'a>,
    ) -> crate::handler_workflow::WorkflowFuture<'a> {
        Box::pin(async move {
            let Some(route) = self.requests.resolve(operation.name(), request)? else {
                return next(None).await;
            };
            redirect_async(
                self.policy,
                &self.current_cluster,
                &self.configured_target,
                &route.domain,
                route.execution.as_ref(),
                route.requested_cluster_attribute.as_deref(),
                operation.name(),
                route.consistency,
                &self.config,
                &self.manager,
                self.metrics.as_ref(),
                |cluster| next(Some(cluster)),
            )
            .await
        })
    }
}

fn active_active_target<M: ActiveClusterManager>(
    manager: &M,
    domain: &DomainState,
    execution: Option<&WorkflowRef>,
    requested_cluster_attribute: Option<&str>,
    operation: &str,
    current_cluster: &str,
) -> String {
    let is_start = matches!(
        operation,
        "StartWorkflowExecution" | "StartWorkflowExecutionAsync"
    );
    let is_signal_start = matches!(
        operation,
        "SignalWithStartWorkflowExecution" | "SignalWithStartWorkflowExecutionAsync"
    );
    let cluster_from_attribute = || {
        requested_cluster_attribute
            .and_then(|attribute| {
                manager
                    .active_cluster_for_cluster_attribute(&domain.id, attribute)
                    .ok()
            })
            .unwrap_or_else(|| current_cluster.to_owned())
    };

    if is_start {
        return cluster_from_attribute();
    }
    if is_signal_start {
        let workflow_id = execution.map_or("", |workflow| workflow.workflow_id.as_str());
        match manager.current_workflow_selection(&domain.id, workflow_id) {
            Ok((true, existing_attribute)) => {
                return manager
                    .active_cluster_for_cluster_attribute(
                        &domain.id,
                        existing_attribute.as_deref().unwrap_or(""),
                    )
                    .unwrap_or_else(|_| current_cluster.to_owned());
            }
            Err(_) => return current_cluster.to_owned(),
            Ok((false, _)) => {}
        }
        return cluster_from_attribute();
    }

    let Some(execution) = execution.filter(|execution| !execution.workflow_id.is_empty()) else {
        return current_cluster.to_owned();
    };
    manager
        .active_cluster_for_workflow(&domain.id, &execution.workflow_id, &execution.run_id)
        .unwrap_or_else(|_| current_cluster.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    struct Config(bool);
    impl RuntimeConfig for Config {
        fn domain_auto_forwarding_enabled(&self, _: &str) -> bool {
            self.0
        }
    }

    struct Manager;
    impl ActiveClusterManager for Manager {
        fn active_cluster_for_workflow(&self, _: &str, _: &str, _: &str) -> Result<String, String> {
            Ok("cluster-b".into())
        }
        fn active_cluster_for_cluster_attribute(
            &self,
            _: &str,
            attr: &str,
        ) -> Result<String, String> {
            if attr.is_empty() {
                Ok("cluster-a".into())
            } else {
                Ok("cluster-b".into())
            }
        }
        fn current_workflow_selection(
            &self,
            _: &str,
            _: &str,
        ) -> Result<(bool, Option<String>), String> {
            Ok((false, None))
        }
    }

    #[derive(Default)]
    struct RecordingMetrics(Mutex<Vec<ForwardRecord>>);
    impl RedirectMetrics for RecordingMetrics {
        fn record_forward(&self, record: &ForwardRecord) {
            self.0.lock().unwrap().push(record.clone());
        }
    }

    #[test]
    fn allowlists_match_go_policy_versions() {
        assert_eq!(api_allowlist(ForwardingPolicy::SelectedV1).len(), 12);
        assert!(!ForwardingPolicy::SelectedV1.allows_api("RespondActivityTaskCompleted"));
        assert!(ForwardingPolicy::SelectedV2.allows_api("RespondActivityTaskCompletedByID"));
        assert!(!ForwardingPolicy::SelectedV2.allows_api("StartWorkflowExecutionAsync"));
        assert!(ForwardingPolicy::SelectedV3.allows_api("StartWorkflowExecutionAsync"));
        assert!(ForwardingPolicy::SelectedV3.allows_api("SignalWithStartWorkflowExecutionAsync"));
    }

    #[test]
    fn forwarding_falls_back_to_selected_allowlist() {
        assert!(!ForwardingPolicy::AllDomainV2.routes_to_active_cluster(
            "DescribeWorkflowExecution",
            false,
            true,
            "cluster-b",
            "cluster-a"
        ));
        assert!(!ForwardingPolicy::AllDomainV2.routes_to_active_cluster(
            "DescribeWorkflowExecution",
            false,
            false,
            "cluster-b",
            "cluster-a"
        ));
        assert!(ForwardingPolicy::AllDomainV2.routes_to_active_cluster(
            "StartWorkflowExecution",
            false,
            false,
            "cluster-b",
            "cluster-a"
        ));
    }

    #[test]
    fn deprecated_domain_allowlist_is_exact() {
        assert!(allowed_on_deprecated_domain("ListWorkflowExecutions"));
        assert!(allowed_on_deprecated_domain("TerminateWorkflowExecution"));
        assert!(!allowed_on_deprecated_domain("DescribeWorkflowExecution"));
    }

    #[test]
    fn stateful_policy_forwards_selected_global_domain_api() {
        let metrics = RecordingMetrics::default();
        let domain = DomainState {
            id: "id".into(),
            name: "global".into(),
            global: true,
            active_cluster: "cluster-b".into(),
            ..Default::default()
        };
        let mut targets = Vec::new();
        redirect(
            ForwardingPolicy::SelectedV3,
            "cluster-a",
            "",
            &domain,
            None,
            None,
            "StartWorkflowExecution",
            Consistency::Eventual,
            &Config(true),
            &Manager,
            &metrics,
            |target| {
                targets.push(target.to_owned());
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(targets, vec![String::from("cluster-b")]);
        assert_eq!(metrics.0.lock().unwrap().len(), 1);
        assert!(metrics.0.lock().unwrap()[0].succeeded);
    }

    #[test]
    fn domain_not_active_fallback_calls_error_active_cluster() {
        let metrics = RecordingMetrics::default();
        let domain = DomainState {
            id: "id".into(),
            name: "global".into(),
            global: true,
            active_cluster: "cluster-b".into(),
            ..Default::default()
        };
        let mut targets = Vec::new();
        let result = redirect(
            ForwardingPolicy::SelectedV3,
            "cluster-a",
            "",
            &domain,
            None,
            None,
            "StartWorkflowExecution",
            Consistency::Eventual,
            &Config(true),
            &Manager,
            &metrics,
            |target| {
                targets.push(target.to_owned());
                if target == "cluster-b" {
                    Err(CadenceError::DomainNotActive(DomainNotActiveError {
                        message: "inactive".into(),
                        domain_name: "global".into(),
                        current_cluster: "cluster-b".into(),
                        active_cluster: "cluster-c".into(),
                        active_clusters: Vec::new(),
                    }))
                } else {
                    Ok(())
                }
            },
        );
        assert!(result.is_ok());
        assert_eq!(
            targets,
            vec![String::from("cluster-b"), String::from("cluster-c")]
        );
        let records = metrics.0.lock().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].target_cluster, "cluster-b");
        assert_eq!(records[0].active_cluster.as_deref(), Some("cluster-c"));
        assert!(records[0].succeeded);
    }

    #[tokio::test]
    async fn async_forwarding_retries_at_domain_not_active_cluster() {
        let metrics = RecordingMetrics::default();
        let targets = std::sync::Arc::new(Mutex::new(Vec::new()));
        let domain = DomainState {
            id: "id".into(),
            name: "global".into(),
            global: true,
            active_cluster: "cluster-b".into(),
            ..Default::default()
        };
        let observed = targets.clone();
        redirect_async(
            ForwardingPolicy::SelectedV3,
            "cluster-a",
            "",
            &domain,
            None,
            None,
            "StartWorkflowExecution",
            Consistency::Eventual,
            &Config(true),
            &Manager,
            &metrics,
            move |target| {
                let observed = observed.clone();
                async move {
                    observed.lock().unwrap().push(target.clone());
                    if target == "cluster-b" {
                        Err(CadenceError::DomainNotActive(DomainNotActiveError {
                            message: "inactive".into(),
                            domain_name: "global".into(),
                            current_cluster: "cluster-b".into(),
                            active_cluster: "cluster-c".into(),
                            active_clusters: Vec::new(),
                        }))
                    } else {
                        Ok("forwarded")
                    }
                }
            },
        )
        .await
        .unwrap();

        assert_eq!(
            *targets.lock().unwrap(),
            ["cluster-b".to_owned(), "cluster-c".to_owned()]
        );
        let records = metrics.0.lock().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].target_cluster, "cluster-b");
        assert_eq!(records[0].active_cluster.as_deref(), Some("cluster-c"));
        assert!(records[0].succeeded);
    }

    #[tokio::test]
    async fn workflow_redirector_routes_handler_calls_through_selected_cluster() {
        #[derive(Default)]
        struct Backend(Mutex<Vec<Option<String>>>);
        impl crate::handler_workflow::WorkflowBackend for Backend {
            fn invoke<'a>(
                &'a self,
                _: crate::handler_workflow::WorkflowOperation,
                _: serde_json::Value,
            ) -> crate::handler_workflow::WorkflowFuture<'a> {
                Box::pin(async { Ok(serde_json::Value::Null) })
            }

            fn invoke_on_cluster<'a>(
                &'a self,
                _: crate::handler_workflow::WorkflowOperation,
                _: serde_json::Value,
                target: Option<String>,
            ) -> crate::handler_workflow::WorkflowFuture<'a> {
                Box::pin(async move {
                    self.0.lock().unwrap().push(target);
                    Ok(serde_json::Value::Null)
                })
            }
        }

        struct Resolver;
        impl RedirectRequestResolver for Resolver {
            fn resolve(
                &self,
                _: &str,
                _: &serde_json::Value,
            ) -> Result<Option<RedirectRequest>, CadenceError> {
                Ok(Some(RedirectRequest {
                    domain: DomainState {
                        id: "domain-id".into(),
                        name: "global-domain".into(),
                        global: true,
                        active_cluster: "cluster-b".into(),
                        ..Default::default()
                    },
                    ..Default::default()
                }))
            }
        }

        let calls = std::sync::Arc::new(Mutex::new(Vec::new()));
        let backend = Backend(calls.clone());
        let redirector = WorkflowRedirector {
            policy: ForwardingPolicy::SelectedV3,
            current_cluster: "cluster-a".into(),
            configured_target: String::new(),
            config: Config(true),
            manager: Manager,
            metrics: std::sync::Arc::new(RecordingMetrics::default()),
            requests: Resolver,
        };
        let handler = crate::handler_workflow::Handler::new(backend)
            .with_cluster_redirection(std::sync::Arc::new(redirector));

        handler
            .dispatch(
                "TerminateWorkflowExecution",
                serde_json::json!({
                    "domain": "global-domain",
                    "workflowExecution": {"workflowId": "wf", "runId": "run"}
                }),
            )
            .await
            .unwrap();

        assert_eq!(
            *calls.lock().unwrap(),
            [Some("cluster-b".to_owned())]
        );
    }

    #[test]
    fn deprecated_domain_rejects_unlisted_operation_before_call() {
        let metrics = RecordingMetrics::default();
        let domain = DomainState {
            name: "old".into(),
            deprecated_or_deleted: true,
            ..Default::default()
        };
        let mut called = false;
        let err = redirect(
            ForwardingPolicy::SelectedV3,
            "cluster-a",
            "",
            &domain,
            None,
            None,
            "DescribeWorkflowExecution",
            Consistency::Eventual,
            &Config(true),
            &Manager,
            &metrics,
            |_| {
                called = true;
                Ok(())
            },
        )
        .unwrap_err();
        assert_eq!(err.to_string(), "domain is deprecated or deleted.");
        assert!(!called);
    }
}
