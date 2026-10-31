//! Worker-specific configuration facade.
//!
//! These are the Go `worker.Config` dynamic-property defaults. Runtime wiring
//! resolves them from Cadence dynamic config; keeping the facade explicit
//! prevents the shared service config from silently dropping worker settings.

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Config {
    pub enable_batcher: bool,
    pub enable_scheduler: bool,
    pub enable_parent_close_policy_worker: bool,
    pub parent_close_policy_system_workflows: i32,
    pub enable_es_analyzer: bool,
    pub enable_failover_manager: bool,
    pub enable_async_workflow_consumption: bool,
    pub enable_domain_audit_logging: bool,
    pub archiver_concurrency: i32,
    pub archivals_per_iteration: i32,
    pub scanner_persistence_max_qps: i32,
    pub scanner_batch_size: i32,
    pub scanner_task_list_enabled: bool,
    pub scanner_history_enabled: bool,
    pub max_workflow_retention_days: i32,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            enable_batcher: false,
            enable_scheduler: false,
            enable_parent_close_policy_worker: false,
            parent_close_policy_system_workflows: 1,
            enable_es_analyzer: false,
            enable_failover_manager: false,
            enable_async_workflow_consumption: false,
            enable_domain_audit_logging: false,
            archiver_concurrency: 10,
            archivals_per_iteration: 10,
            scanner_persistence_max_qps: 0,
            scanner_batch_size: 100,
            scanner_task_list_enabled: false,
            scanner_history_enabled: false,
            max_workflow_retention_days: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Config;

    #[test]
    fn worker_options_accept_go_camel_case_yaml_keys() {
        let config: Config = serde_yaml::from_str(
            "enableBatcher: true\nenableScheduler: true\nparentClosePolicySystemWorkflows: 4\n",
        )
        .unwrap();
        assert!(config.enable_batcher);
        assert!(config.enable_scheduler);
        assert_eq!(config.parent_close_policy_system_workflows, 4);
        assert!(!config.enable_failover_manager);
    }
}
