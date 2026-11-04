//! Port of `go/service/history/task/executor_wrapper.go`.

use std::sync::Arc;

use async_trait::async_trait;

use super::deps::*;
use super::interface::*;

pub struct ExecutorWrapper {
    current_cluster_name: String,
    active_cluster_mgr: Arc<dyn ActiveClusterManager>,
    active_executor: Arc<dyn Executor>,
    standby_executor: Arc<dyn Executor>,
    logger: Logger,
}

/// `NewExecutorWrapper`
pub fn new_executor_wrapper(
    current_cluster_name: String,
    active_cluster_mgr: Arc<dyn ActiveClusterManager>,
    active_executor: Arc<dyn Executor>,
    standby_executor: Arc<dyn Executor>,
    logger: Logger,
) -> Arc<dyn Executor> {
    Arc::new(ExecutorWrapper { current_cluster_name, active_cluster_mgr, active_executor, standby_executor, logger })
}

#[async_trait]
impl Executor for ExecutorWrapper {
    fn stop(&self) {
        self.active_executor.stop();
        self.standby_executor.stop();
    }

    async fn execute(&self, task: Arc<dyn Task>) -> (ExecuteResponse, Result<()>) {
        if self.is_active_task(task.as_ref()).await {
            return self.active_executor.execute(task).await;
        }
        self.standby_executor.execute(task).await
    }
}

impl ExecutorWrapper {
    async fn is_active_task(&self, task: &dyn Task) -> bool {
        let domain_id = task.get_domain_id();
        let wf_id = task.get_workflow_id();
        let r_id = task.get_run_id();
        let info = match self.active_cluster_mgr.get_active_cluster_info_by_workflow(domain_id, wf_id, r_id).await {
            Ok(i) => i,
            Err(e) => {
                self.logger.warn_tags(
                    "Failed to get active cluster info, process task as active.",
                    &[
                        tag::workflow_domain_id(domain_id),
                        tag::workflow_id(wf_id),
                        tag::workflow_run_id(r_id),
                        tag::error(&e),
                    ],
                );
                return true;
            }
        };
        let standby = info.active_cluster_name != self.current_cluster_name;
        if self.logger.debug_on() {
            self.logger.debug_tags(
                if standby { "Process task as standby." } else { "Process task as active." },
                &[
                    tag::workflow_domain_id(domain_id),
                    tag::dynamic("task", format!("{:?}", task)),
                    tag::dynamic("taskType", task.get_task_type().to_string()),
                    tag::cluster_name(&info.active_cluster_name),
                    tag::dynamic("stack", std::backtrace::Backtrace::force_capture().to_string()),
                ],
            );
        }
        !standby
    }
}
