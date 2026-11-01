//! Decision task poller (port of `workflowTaskPoller` without sticky
//! execution): poll, fetch the remaining history pages, replay, respond.

use std::sync::Arc;
use std::time::Duration;

use cadence_proto::api as pb;
use tokio::sync::watch;

use super::registry::Registry;
use super::replay::{process_decision_task, Query, ReplayError, TaskInput};
use crate::client::CadenceClient;

/// `DECISION_TASK_FAILED_CAUSE_WORKFLOW_WORKER_UNHANDLED_FAILURE`.
const CAUSE_UNHANDLED_FAILURE: i32 = 14;
/// `QUERY_RESULT_TYPE_ANSWERED` / `QUERY_RESULT_TYPE_FAILED`.
const QUERY_ANSWERED: i32 = 1;
const QUERY_FAILED: i32 = 2;

#[derive(Clone, Debug)]
pub struct PollerOptions {
    pub domain: String,
    pub task_list: String,
    pub identity: String,
    pub binary_checksum: String,
}

#[derive(Clone)]
pub struct DecisionWorker {
    client: CadenceClient,
    registry: Arc<Registry>,
    options: PollerOptions,
}

fn query_result(r: Result<Vec<u8>, String>) -> pb::WorkflowQueryResult {
    match r {
        Ok(data) => pb::WorkflowQueryResult {
            result_type: QUERY_ANSWERED,
            answer: Some(pb::Payload { data }),
            error_message: String::new(),
        },
        Err(message) => pb::WorkflowQueryResult {
            result_type: QUERY_FAILED,
            answer: None,
            error_message: message,
        },
    }
}

impl DecisionWorker {
    pub fn new(client: CadenceClient, registry: Arc<Registry>, options: PollerOptions) -> Self {
        Self {
            client,
            registry,
            options,
        }
    }

    /// Poll once. Returns `true` when a task was handled.
    pub async fn poll_and_process(&mut self) -> anyhow::Result<bool> {
        let task = self
            .client
            .poll_decision_task(
                self.options.domain.clone(),
                self.options.task_list.clone(),
                self.options.identity.clone(),
                self.options.binary_checksum.clone(),
            )
            .await?;
        if task.task_token.is_empty() {
            return Ok(false);
        }
        self.handle_task(task).await?;
        Ok(true)
    }

    async fn handle_task(&mut self, task: pb::PollForDecisionTaskResponse) -> anyhow::Result<()> {
        let execution = task.workflow_execution.clone().unwrap_or_default();
        let mut events = task.history.clone().unwrap_or_default().events;
        let mut token = task.next_page_token.clone();
        while !token.is_empty() {
            let page = self
                .client
                .get_history_page(self.options.domain.clone(), execution.clone(), token)
                .await?;
            events.extend(page.history.unwrap_or_default().events);
            token = page.next_page_token;
        }
        let mut queries = Vec::new();
        if let Some(q) = &task.query {
            queries.push(Query {
                id: String::new(),
                query_type: q.query_type.clone(),
                args: q.query_args.clone().map(|p| p.data).unwrap_or_default(),
            });
        }
        let mut ids: Vec<_> = task.queries.keys().cloned().collect();
        ids.sort();
        for id in ids {
            let q = &task.queries[&id];
            queries.push(Query {
                id,
                query_type: q.query_type.clone(),
                args: q.query_args.clone().map(|p| p.data).unwrap_or_default(),
            });
        }
        let outcome = process_decision_task(
            &self.registry,
            TaskInput {
                workflow_id: execution.workflow_id.clone(),
                run_id: execution.run_id.clone(),
                workflow_type: task.workflow_type.clone().unwrap_or_default().name,
                domain: self.options.domain.clone(),
                events,
                queries,
            },
        );
        match outcome {
            Err(err) => self.fail_task(&task, err).await,
            Ok(out) if task.query.is_some() => {
                // Legacy query task: answer via RespondQueryTaskCompleted.
                let result = out
                    .query_results
                    .into_iter()
                    .next()
                    .map(|(_, r)| r)
                    .unwrap_or_else(|| Err("query not processed".to_owned()));
                self.client
                    .complete_query_task(task.task_token.clone(), query_result(result))
                    .await?;
                Ok(())
            }
            Ok(out) => {
                let query_results = out
                    .query_results
                    .into_iter()
                    .map(|(id, r)| (id, query_result(r)))
                    .collect();
                self.client
                    .respond_decision_task_completed(pb::RespondDecisionTaskCompletedRequest {
                        task_token: task.task_token.clone(),
                        decisions: out.decisions,
                        execution_context: Vec::new(),
                        identity: self.options.identity.clone(),
                        sticky_attributes: None,
                        return_new_decision_task: false,
                        force_create_new_decision_task: false,
                        binary_checksum: self.options.binary_checksum.clone(),
                        query_results,
                    })
                    .await?;
                Ok(())
            }
        }
    }

    async fn fail_task(
        &mut self,
        task: &pb::PollForDecisionTaskResponse,
        err: ReplayError,
    ) -> anyhow::Result<()> {
        if task.query.is_some() {
            self.client
                .complete_query_task(task.task_token.clone(), query_result(Err(err.to_string())))
                .await?;
            return Ok(());
        }
        self.client
            .fail_decision_task(
                task.task_token.clone(),
                CAUSE_UNHANDLED_FAILURE,
                err.to_string().into_bytes(),
                self.options.identity.clone(),
                self.options.binary_checksum.clone(),
            )
            .await?;
        Ok(())
    }

    /// Poll until `stop` flips to `true`; transient errors back off briefly.
    pub async fn run(mut self, mut stop: watch::Receiver<bool>) {
        loop {
            if *stop.borrow() {
                return;
            }
            tokio::select! {
                _ = stop.changed() => {}
                r = self.poll_and_process() => {
                    if r.is_err() {
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        }
    }
}
