//! Activity task poller (port of `activityTaskPoller`/`activityTaskHandler`):
//! polls, runs the registered activity, heartbeats and reports the outcome.
//! Retries are driven by the server from the activity's retry policy; this
//! worker only reports each attempt's result.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use cadence_proto::api as pb;
use tokio::sync::watch;

use super::core::REASON_GENERIC;
use crate::client::CadenceClient;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ActivityFailure {
    /// Failure with an application reason (retried per the retry policy).
    Failed { reason: String, details: Vec<u8> },
    /// The activity observed its cancellation request.
    Canceled,
    /// Cancelled with details (Go `NewCanceledError(details)`).
    CanceledWith(Vec<u8>),
}

impl ActivityFailure {
    pub fn generic(message: impl AsRef<str>) -> Self {
        Self::Failed {
            reason: REASON_GENERIC.to_owned(),
            details: super::converter::encode(message.as_ref()).unwrap_or_default(),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct ActivityInfo {
    pub activity_id: String,
    pub activity_type: String,
    pub workflow_id: String,
    pub run_id: String,
    pub workflow_domain: String,
    pub attempt: i32,
    pub heartbeat_details: Option<Vec<u8>>,
}

#[derive(Clone)]
pub struct ActivityContext {
    pub info: ActivityInfo,
    client: CadenceClient,
    task_token: Vec<u8>,
    identity: String,
    cancel: Arc<AtomicBool>,
}

impl ActivityContext {
    #[allow(clippy::result_large_err)]
    /// Go `activity.RecordHeartbeat`; also learns about server-side cancellation.
    pub async fn record_heartbeat(&self, details: Option<Vec<u8>>) -> Result<(), tonic::Status> {
        let mut client = self.client.clone();
        let resp = client
            .heartbeat_activity_task(
                self.task_token.clone(),
                details.map(|data| pb::Payload { data }),
                self.identity.clone(),
            )
            .await?;
        if resp.cancel_requested {
            self.cancel.store(true, Ordering::SeqCst);
        }
        Ok(())
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancel.load(Ordering::SeqCst)
    }
}

pub type ActivityOutput = Result<Option<Vec<u8>>, ActivityFailure>;
pub type ActivityFn = Arc<
    dyn Fn(ActivityContext, Vec<u8>) -> Pin<Box<dyn Future<Output = ActivityOutput> + Send>>
        + Send
        + Sync,
>;

#[derive(Clone, Default)]
pub struct ActivityRegistry {
    activities: HashMap<String, ActivityFn>,
}

impl ActivityRegistry {
    pub fn register<F, Fut>(&mut self, name: &str, f: F)
    where
        F: Fn(ActivityContext, Vec<u8>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ActivityOutput> + Send + 'static,
    {
        self.activities
            .insert(name.to_owned(), Arc::new(move |c, i| Box::pin(f(c, i))));
    }

    pub fn get(&self, name: &str) -> Option<ActivityFn> {
        self.activities.get(name).cloned()
    }
}

#[derive(Clone, Debug)]
pub struct ActivityPollerOptions {
    pub domain: String,
    pub task_list: String,
    pub identity: String,
}

#[derive(Clone)]
pub struct ActivityWorker {
    client: CadenceClient,
    registry: Arc<ActivityRegistry>,
    options: ActivityPollerOptions,
}

impl ActivityWorker {
    pub fn new(
        client: CadenceClient,
        registry: Arc<ActivityRegistry>,
        options: ActivityPollerOptions,
    ) -> Self {
        Self {
            client,
            registry,
            options,
        }
    }

    /// Poll and run one activity task. Returns `true` when a task was handled.
    pub async fn poll_and_process(&mut self) -> anyhow::Result<bool> {
        let task = self
            .client
            .poll_activity_task(
                self.options.domain.clone(),
                self.options.task_list.clone(),
                self.options.identity.clone(),
            )
            .await?;
        if task.task_token.is_empty() {
            return Ok(false);
        }
        let name = task.activity_type.clone().unwrap_or_default().name;
        let execution = task.workflow_execution.clone().unwrap_or_default();
        let ctx = ActivityContext {
            info: ActivityInfo {
                activity_id: task.activity_id.clone(),
                activity_type: name.clone(),
                workflow_id: execution.workflow_id,
                run_id: execution.run_id,
                workflow_domain: task.workflow_domain.clone(),
                attempt: task.attempt,
                heartbeat_details: task.heartbeat_details.clone().map(|p| p.data),
            },
            client: self.client.clone(),
            task_token: task.task_token.clone(),
            identity: self.options.identity.clone(),
            cancel: Arc::new(AtomicBool::new(false)),
        };
        let input = task.input.clone().map(|p| p.data).unwrap_or_default();
        let output = match self.registry.get(&name) {
            Some(f) => {
                // A panicking activity fails the attempt like Go's recover.
                match tokio::spawn(f(ctx, input)).await {
                    Ok(o) => o,
                    Err(e) => Err(ActivityFailure::Failed {
                        reason: super::core::REASON_PANIC.to_owned(),
                        details: super::converter::encode(&e.to_string()).unwrap_or_default(),
                    }),
                }
            }
            None => Err(ActivityFailure::generic(format!(
                "unable to find activityType={name}"
            ))),
        };
        self.report(task.task_token, output).await;
        Ok(true)
    }

    /// Report the outcome, retrying transient transport errors a few times.
    async fn report(&mut self, token: Vec<u8>, output: ActivityOutput) {
        for attempt in 0..5u64 {
            let identity = self.options.identity.clone();
            let r = match &output {
                Ok(result) => self
                    .client
                    .complete_activity_task(
                        token.clone(),
                        result.clone().map(|data| pb::Payload { data }),
                        identity,
                    )
                    .await
                    .map(|_| ()),
                Err(ActivityFailure::Failed { reason, details }) => self
                    .client
                    .fail_activity_task(
                        token.clone(),
                        Some(pb::Failure {
                            reason: reason.clone(),
                            details: details.clone(),
                            options: None,
                        }),
                        None,
                        identity,
                    )
                    .await
                    .map(|_| ()),
                Err(ActivityFailure::Canceled) => {
                    self.client
                        .cancel_activity_task(token.clone(), None, identity)
                        .await
                }
                Err(ActivityFailure::CanceledWith(details)) => {
                    self.client
                        .cancel_activity_task(
                            token.clone(),
                            Some(pb::Payload {
                                data: details.clone(),
                            }),
                            identity,
                        )
                        .await
                }
            };
            match r {
                Ok(()) => return,
                // The server no longer knows the task (timed out, completed).
                Err(s) if s.code() == tonic::Code::NotFound => return,
                Err(_) => tokio::time::sleep(Duration::from_millis(100 * (attempt + 1))).await,
            }
        }
    }

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
