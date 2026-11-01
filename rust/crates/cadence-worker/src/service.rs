//! Worker process lifecycle boundary.

use crate::runtime::{Lifecycle, WorkflowRegistry};

#[derive(Debug)]
pub struct WorkerService {
    lifecycle: Lifecycle,
    registry: WorkflowRegistry,
}

impl Default for WorkerService {
    fn default() -> Self {
        let mut registry = WorkflowRegistry::default();
        let _ = registry.register(crate::parentclosepolicy::registration());
        let _ = registry.register(crate::archiver::registrations());
        let _ = registry.register(crate::domaindeprecation::registration());
        for registration in crate::failovermanager::registrations() {
            let _ = registry.register(registration);
        }
        let _ = registry.register(crate::scheduler::registration());
        for registration in crate::batcher::registrations() {
            let _ = registry.register(registration);
        }
        for registration in crate::scanner::registrations() {
            let _ = registry.register(registration);
        }
        Self {
            lifecycle: Lifecycle::default(),
            registry,
        }
    }
}

impl WorkerService {
    pub fn registrations(&self) -> impl Iterator<Item = &crate::runtime::WorkflowRegistration> {
        self.registry.iter()
    }

    /// Starts the currently assembled workers. Poller startup stays disabled
    /// until history replay and all registered workflow implementations exist.
    pub fn start(&self) -> anyhow::Result<()> {
        if self.lifecycle.start() {
            anyhow::bail!("worker runtime cannot start: deterministic workflow executor is not ported")
        }
        anyhow::bail!("worker service is already started or stopped")
    }

    /// Starts the SDK decision poller for the parent-close-policy system
    /// workflow when enabled by config. Returns the poller task handle.
    pub fn start_parent_close_policy_poller(
        &self,
        client: crate::client::CadenceClient,
        config: &crate::config::Config,
        identity: String,
        stop: tokio::sync::watch::Receiver<bool>,
    ) -> Option<tokio::task::JoinHandle<()>> {
        if !config.enable_parent_close_policy_worker {
            return None;
        }
        let mut registry = crate::sdk::registry::Registry::default();
        crate::parentclosepolicy::register_sdk(&mut registry);
        let worker = crate::sdk::decision_poller::DecisionWorker::new(
            client,
            std::sync::Arc::new(registry),
            crate::sdk::decision_poller::PollerOptions {
                domain: crate::workercommon::SYSTEM_DOMAIN.to_owned(),
                task_list: crate::parentclosepolicy::TASK_LIST.to_owned(),
                identity,
                binary_checksum: String::new(),
            },
        );
        Some(tokio::spawn(worker.run(stop)))
    }

    pub fn stop(&self) -> bool {
        self.lifecycle.stop()
    }
}
