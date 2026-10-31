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

    pub fn stop(&self) -> bool {
        self.lifecycle.stop()
    }
}
