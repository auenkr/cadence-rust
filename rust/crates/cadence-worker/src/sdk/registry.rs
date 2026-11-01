//! Workflow and activity registration.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use super::core::{WorkflowContext, WorkflowResult};

pub type WorkflowFuture = Pin<Box<dyn Future<Output = WorkflowResult>>>;
/// Workflow entry point: context plus the encoded input (empty if none).
/// The returned future is deliberately not `Send`: it only ever runs on the
/// deterministic single-threaded executor inside one decision task.
pub type WorkflowFn = Arc<dyn Fn(WorkflowContext, Vec<u8>) -> WorkflowFuture + Send + Sync>;

#[derive(Clone, Default)]
pub struct Registry {
    workflows: HashMap<String, WorkflowFn>,
}

impl Registry {
    pub fn register_workflow<F, Fut>(&mut self, name: &str, f: F)
    where
        F: Fn(WorkflowContext, Vec<u8>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = WorkflowResult> + 'static,
    {
        self.workflows
            .insert(name.to_owned(), Arc::new(move |c, i| Box::pin(f(c, i))));
    }

    pub fn workflow(&self, name: &str) -> Option<WorkflowFn> {
        self.workflows.get(name).cloned()
    }

    pub fn workflow_names(&self) -> Vec<String> {
        let mut v: Vec<_> = self.workflows.keys().cloned().collect();
        v.sort();
        v
    }
}
