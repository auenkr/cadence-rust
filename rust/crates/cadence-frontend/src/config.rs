//! Frontend configuration facade over Cadence's shared Go-compatible YAML
//! loader. Dynamic settings remain named by their Go dynamic-config keys.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Config {
    pub num_history_shards: i32,
    pub advanced_visibility_store: String,
    pub host_name: String,
    /// Dynamic values use Go's canonical key (for example
    /// `frontend.visibilityMaxPageSize`) and may be replaced at runtime.
    pub dynamic: BTreeMap<String, serde_yaml::Value>,
}

impl Config {
    /// Load a Go config file or layered config directory using the common
    /// loader, including the same environment interpolation and YAML casing.
    pub fn load(path: &str, environment: &str, zone: &str) -> anyhow::Result<Self> {
        let shared = if std::path::Path::new(path).is_dir() {
            cadence_service_core::config::Config::from_layers(path, environment, zone)?
        } else {
            cadence_service_core::config::Config::from_file(path)?
        };
        let advanced_visibility_store = shared.persistence.advanced_visibility_store.clone();
        Ok(Self {
            num_history_shards: shared.persistence.num_history_shards,
            advanced_visibility_store,
            host_name: std::env::var("HOSTNAME").unwrap_or_default(),
            dynamic: BTreeMap::new(),
        })
    }

    /// Go Frontend dynamic-config keys configured by `service/frontend/config`.
    /// The runtime's dynamic-config provider evaluates values and filters;
    /// this registry prevents service code from inventing Rust-only names.
    pub const DYNAMIC_PROPERTY_KEYS: &'static [&'static str] = &[
        "history.adminOperationToken",
        "limit.blobSize.error",
        "limit.blobSize.warn",
        "frontend.disableListVisibilityByFilter",
        "system.disallowQuery",
        "frontend.domainFailoverRefreshInterval",
        "frontend.domainFailoverRefreshTimerJitterCoefficient",
        "limit.domainNameLength",
        "frontend.enableActiveClusterSelectionPolicyInStartWorkflow",
        "history.enableAdminProtection",
        "frontend.enableClientVersionCheck",
        "system.enableDistributedSemaphore",
        "system.enableDomainAuditLogging",
        "system.enableDomainNotActiveAutoForwarding",
        "system.enableGracefulFailover",
        "system.enableLogCustomerQueryParameter",
        "frontend.enableQueryAttributeValidation",
        "system.enableReadFromClosedExecutionV2",
        "worker.enableScheduler",
        "system.enableTasklistIsolation",
        "system.enableVisibilitySampling",
        "frontend.asyncrps",
        "frontend.decisionResultCountLimit",
        "frontend.esIndexMaxResultWindow",
        "frontend.esVisibilityListMaxQPS",
        "frontend.emitSignalNameMetricsTag",
        "frontend.failoverCoolDown",
        "frontend.failoverHistoryMaxSize",
        "frontend.globalDomainAsyncrps",
        "frontend.globalDomainrps",
        "frontend.globalDomainVisibilityrps",
        "frontend.globalDomainWorkerrps",
        "frontend.globalRatelimiterMode",
        "frontend.globalTaskListAsyncrps",
        "frontend.globalTaskListrps",
        "frontend.globalTaskListWorkerrps",
        "frontend.historyMaxPageSize",
        "frontend.maxBadBinaries",
        "frontend.domainasyncrps",
        "frontend.domainrps",
        "frontend.domainvisibilityrps",
        "frontend.domainworkerrps",
        "frontend.maxFailoverTimeoutInSeconds",
        "frontend.tasklistasyncrps",
        "frontend.tasklistrps",
        "frontend.tasklistworkerrps",
        "frontend.maxWorkerPollDelay",
        "frontend.persistenceGlobalMaxQPS",
        "frontend.persistenceMaxQPS",
        "frontend.shutdownDrainDuration",
        "frontend.throttledLogRPS",
        "frontend.rps",
        "frontend.visibilityListMaxQPS",
        "frontend.visibilityMaxPageSize",
        "frontend.visibilityrps",
        "frontend.warmupDuration",
        "frontend.workerrps",
        "frontend.globalRatelimiterUpdateInterval",
        "limit.identityLength",
        "system.Lockdown",
        "limit.maxIDWarnLength",
        "system.maxRetentionDays",
        "system.minRetentionDays",
        "frontend.pinotOptimizedQueryColumns",
        "system.rateLimiterBypassCallerTypes",
        "system.readVisibilityStoreName",
        "limit.requestIDLength",
        "system.requiredDomainDataKeys",
        "frontend.searchAttributesNumberOfKeysLimit",
        "frontend.searchAttributesSizeOfValueLimit",
        "frontend.searchAttributesTotalSizeLimit",
        "limit.semaphoreNameLength",
        "frontend.sendRawWorkflowHistory",
        "limit.signalNameLength",
        "limit.taskListNameLength",
        "frontend.validSearchAttributes",
        "frontend.visibilityArchivalQueryMaxPageSize",
        "limit.workflowIDLength",
        "limit.workflowTypeLength",
    ];
}
