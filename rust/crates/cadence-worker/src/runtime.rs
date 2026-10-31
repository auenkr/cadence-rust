//! Lifecycle and registration primitives shared by system workflow workers.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkflowRegistration {
    pub workflow_type: &'static str,
    pub task_list: &'static str,
    pub activities: &'static [&'static str],
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct WorkflowRegistry {
    workflows: BTreeMap<&'static str, WorkflowRegistration>,
}

impl WorkflowRegistry {
    pub fn register(&mut self, registration: WorkflowRegistration) -> Result<(), &'static str> {
        if self
            .workflows
            .insert(registration.workflow_type, registration)
            .is_some()
        {
            return Err("workflow type registered more than once");
        }
        Ok(())
    }

    pub fn get(&self, workflow_type: &str) -> Option<&WorkflowRegistration> {
        self.workflows.get(workflow_type)
    }

    pub fn iter(&self) -> impl Iterator<Item = &WorkflowRegistration> {
        self.workflows.values()
    }
}

/// Idempotent lifecycle latch for a worker group. Pollers are added as the
/// deterministic workflow executor is ported; shutdown always stops new work
/// before joining its tasks.
#[derive(Clone, Debug)]
pub struct Lifecycle {
    state: Arc<AtomicU8>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum State {
    Created = 0,
    Running = 1,
    Stopping = 2,
    Stopped = 3,
}

impl Default for Lifecycle {
    fn default() -> Self {
        Self {
            state: Arc::new(AtomicU8::new(State::Created as u8)),
        }
    }
}

impl Lifecycle {
    pub fn state(&self) -> State {
        match self.state.load(Ordering::Acquire) {
            1 => State::Running,
            2 => State::Stopping,
            3 => State::Stopped,
            _ => State::Created,
        }
    }

    pub fn start(&self) -> bool {
        self.state
            .compare_exchange(
                State::Created as u8,
                State::Running as u8,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    pub fn stop(&self) -> bool {
        let result = self.state.compare_exchange(
            State::Running as u8,
            State::Stopping as u8,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
        if result.is_ok() {
            self.state.store(State::Stopped as u8, Ordering::Release);
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Lifecycle, State, WorkflowRegistration, WorkflowRegistry};

    #[test]
    fn lifecycle_transitions_once() {
        let lifecycle = Lifecycle::default();
        assert!(lifecycle.start());
        assert!(!lifecycle.start());
        assert_eq!(lifecycle.state(), State::Running);
        assert!(lifecycle.stop());
        assert!(!lifecycle.stop());
        assert_eq!(lifecycle.state(), State::Stopped);
    }

    #[test]
    fn registry_rejects_duplicate_workflow_names() {
        let registration = WorkflowRegistration {
            workflow_type: "example",
            task_list: "example-list",
            activities: &[],
        };
        let mut registry = WorkflowRegistry::default();
        registry.register(registration.clone()).unwrap();
        assert!(registry.register(registration).is_err());
    }
}
