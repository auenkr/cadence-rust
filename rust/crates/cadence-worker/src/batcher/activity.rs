//! Port of Go `BatchActivity` (v1) / `batchActivityV2` and `startTaskProcessor`.

use std::collections::VecDeque;
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use super::*;
use crate::sdk::activity::{ActivityContext, ActivityFailure, ActivityRegistry};
use crate::sdk::converter;

/// External dependencies of the activity (frontend and admin clients in Go).
pub trait Backend: Send + Sync + 'static {
    fn current_cluster(&self) -> String;
    /// Go `GetRemoteAdminClient`.
    fn check_remote_admin(&self, cluster: &str) -> Result<(), String>;
    fn describe_domain_id(&self, name: &str) -> impl Future<Output = Result<String, String>> + Send;
    fn count(&self, domain: &str, query: &str) -> impl Future<Output = Result<i64, String>> + Send;
    fn scan(
        &self,
        domain: &str,
        page_size: i32,
        token: &[u8],
        query: &str,
    ) -> impl Future<Output = Result<ScanPage, String>> + Send;
    fn terminate(
        &self,
        domain: &str,
        execution: &WorkflowExecution,
        reason: &str,
        identity: &str,
    ) -> impl Future<Output = Result<(), BatchOperationError<String>>> + Send;
    fn request_cancel(
        &self,
        domain: &str,
        execution: &WorkflowExecution,
        identity: &str,
        request_id: &str,
    ) -> impl Future<Output = Result<(), BatchOperationError<String>>> + Send;
    fn signal(
        &self,
        domain: &str,
        execution: &WorkflowExecution,
        identity: &str,
        request_id: &str,
        name: &str,
        input: &[u8],
    ) -> impl Future<Output = Result<(), BatchOperationError<String>>> + Send;
    fn resend_replication(
        &self,
        target_cluster: &str,
        domain_id: &str,
        execution: &WorkflowExecution,
        source_cluster: &str,
    ) -> impl Future<Output = Result<(), BatchOperationError<String>>> + Send;
    fn refresh(
        &self,
        domain: &str,
        execution: &WorkflowExecution,
    ) -> impl Future<Output = Result<(), BatchOperationError<String>>> + Send;
    fn describe_pending_children(
        &self,
        domain: &str,
        execution: &WorkflowExecution,
    ) -> impl Future<Output = Result<Vec<PendingChild>, BatchOperationError<String>>> + Send;
}

#[derive(Clone, Debug, Default)]
pub struct ScanPage {
    pub executions: Vec<WorkflowExecution>,
    pub next_page_token: Vec<u8>,
}

/// The slice of the Go activity context the batch logic needs.
pub trait Progress: Clone + Send + Sync + 'static {
    /// `activity.RecordHeartbeat` (errors are ignored like Go).
    fn heartbeat(&self, details: &HeartBeatDetails) -> impl Future<Output = ()> + Send;
    /// `ctx.Done()`.
    fn is_cancelled(&self) -> bool;
    /// `activity.GetHeartbeatDetails` raw payload, when present.
    fn prior_details(&self) -> Option<Vec<u8>>;
}

impl Progress for ActivityContext {
    async fn heartbeat(&self, details: &HeartBeatDetails) {
        if let Ok(data) = converter::encode(details) {
            let _ = self.record_heartbeat(Some(data)).await;
        }
    }
    fn is_cancelled(&self) -> bool {
        ActivityContext::is_cancelled(self)
    }
    fn prior_details(&self) -> Option<Vec<u8>> {
        self.info.heartbeat_details.clone()
    }
}

/// `golang.org/x/time/rate.Limiter` with burst equal to the rate.
pub struct Limiter {
    rate: f64,
    burst: f64,
    state: Mutex<(f64, Instant)>,
}

impl Limiter {
    pub fn new(rps: i32) -> Self {
        let rate = f64::from(rps.max(1));
        Self {
            rate,
            burst: rate,
            state: Mutex::new((rate, Instant::now())),
        }
    }

    fn reserve(&self) -> Option<Duration> {
        let mut s = self.state.lock().unwrap();
        let now = Instant::now();
        let elapsed = now.duration_since(s.1).as_secs_f64();
        s.0 = (s.0 + elapsed * self.rate).min(self.burst);
        s.1 = now;
        if s.0 >= 1.0 {
            s.0 -= 1.0;
            None
        } else {
            Some(Duration::from_secs_f64((1.0 - s.0) / self.rate))
        }
    }

    /// Wait for a token; `false` when cancelled while waiting.
    pub async fn wait<P: Progress>(&self, progress: &P) -> bool {
        loop {
            if progress.is_cancelled() {
                return false;
            }
            match self.reserve() {
                None => return true,
                Some(d) => tokio::time::sleep(d.min(Duration::from_millis(250))).await,
            }
        }
    }
}

#[derive(Clone, Debug)]
struct TaskDetail {
    execution: WorkflowExecution,
    attempts: i32,
    hbd: HeartBeatDetails,
}

struct Shared<B, P> {
    backend: Arc<B>,
    progress: P,
    params: BatchParams,
    domain_id: String,
    identity: &'static str,
    limiter: Limiter,
    non_retryable: Vec<String>,
    queue: Mutex<VecDeque<TaskDetail>>,
    remaining: AtomicUsize,
    succeeded: AtomicUsize,
    failed: AtomicUsize,
}

fn new_request_id() -> String {
    crate::parentclosepolicy_processor::random_request_id()
}

impl<B: Backend, P: Progress> Shared<B, P> {
    /// Go `processTask`.
    async fn process_task(&self, task: &TaskDetail) -> Result<(), String> {
        let p = &self.params;
        let (apply_on_child, request_id) = (
            match p.batch_type.as_str() {
                "terminate" => p.terminate_params.terminate_children.unwrap_or(false),
                "cancel" => p.cancel_params.cancel_children.unwrap_or(false),
                _ => false,
            },
            new_request_id(),
        );
        let mut pending = VecDeque::from([task.execution.clone()]);
        while let Some(wf) = pending.pop_front() {
            if !self.limiter.wait(&self.progress).await {
                return Err("context canceled".into());
            }
            self.progress.heartbeat(&task.hbd).await;
            match self.apply(&wf, &request_id).await {
                Ok(()) => {}
                Err(BatchOperationError::EntityNotExists) => continue,
                Err(BatchOperationError::Other(e)) => return Err(e),
            }
            let children = match self
                .backend
                .describe_pending_children(&p.domain_name, &wf)
                .await
            {
                Ok(c) => c,
                Err(BatchOperationError::EntityNotExists) => continue,
                Err(BatchOperationError::Other(e)) => return Err(e),
            };
            if apply_on_child {
                pending.extend(children.into_iter().map(|c| WorkflowExecution {
                    workflow_id: c.workflow_id,
                    run_id: c.run_id,
                }));
            }
        }
        Ok(())
    }

    async fn apply(
        &self,
        wf: &WorkflowExecution,
        request_id: &str,
    ) -> Result<(), BatchOperationError<String>> {
        let p = &self.params;
        let domain = p.domain_name.as_str();
        match p.batch_type.as_str() {
            "terminate" => {
                self.backend
                    .terminate(domain, wf, &p.reason, self.identity)
                    .await
            }
            "cancel" => {
                self.backend
                    .request_cancel(domain, wf, V1_WORKFLOW_TYPE, request_id)
                    .await
            }
            "signal" => {
                self.backend
                    .signal(
                        domain,
                        wf,
                        V1_WORKFLOW_TYPE,
                        request_id,
                        &p.signal_params.signal_name,
                        p.signal_params.input.as_bytes(),
                    )
                    .await
            }
            "replicate" => {
                self.backend
                    .resend_replication(
                        &p.replicate_params.target_cluster,
                        &self.domain_id,
                        wf,
                        &p.replicate_params.source_cluster,
                    )
                    .await
            }
            "refresh" => self.backend.refresh(domain, wf).await,
            other => Err(BatchOperationError::Other(format!(
                "not supported batch type: {other}"
            ))),
        }
    }

    /// Go `startTaskProcessor` loop body for one processor goroutine.
    async fn processor(self: Arc<Self>) {
        loop {
            if self.progress.is_cancelled() {
                return;
            }
            let task = self.queue.lock().unwrap().pop_front();
            let Some(mut task) = task else {
                if self.remaining.load(Ordering::SeqCst) == 0 {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
                continue;
            };
            let result = self.process_task(&task).await;
            match task_retry_decision(
                task.attempts,
                self.params.attempts_on_retryable_error,
                &self.non_retryable,
                result,
            ) {
                TaskRetryDecision::CompleteSuccess => {
                    self.succeeded.fetch_add(1, Ordering::SeqCst);
                    self.remaining.fetch_sub(1, Ordering::SeqCst);
                }
                TaskRetryDecision::Requeue { next_attempt } => {
                    task.attempts = next_attempt;
                    self.queue.lock().unwrap().push_back(task);
                }
                TaskRetryDecision::ReportFailure { .. } => {
                    self.failed.fetch_add(1, Ordering::SeqCst);
                    self.remaining.fetch_sub(1, Ordering::SeqCst);
                }
            }
        }
    }
}

fn non_retriable(message: impl AsRef<str>) -> ActivityFailure {
    ActivityFailure::Failed {
        reason: NON_RETRIABLE_ERROR_REASON.to_owned(),
        details: converter::encode(message.as_ref()).unwrap_or_default(),
    }
}

/// Go `getHeartBeatDetails`: undecodable details mean start over.
fn heartbeat_details<P: Progress>(progress: &P) -> Option<HeartBeatDetails> {
    converter::decode::<HeartBeatDetails>(&progress.prior_details()?).ok()
}

/// Shared body of `BatchActivity` (`v2 = false`) and `batchActivityV2`.
pub async fn run_batch<B: Backend, P: Progress>(
    backend: Arc<B>,
    progress: P,
    params: BatchParams,
    v2: bool,
) -> Result<HeartBeatDetails, ActivityFailure> {
    if params.batch_type == "replicate" {
        let current = backend.current_cluster();
        if current != params.replicate_params.source_cluster {
            return Err(non_retriable(format!(
                "the activity must run in the source cluster, current cluster is {current}"
            )));
        }
        backend
            .check_remote_admin(&params.replicate_params.target_cluster)
            .map_err(non_retriable)?;
    }
    let domain_id = backend
        .describe_domain_id(&params.domain_name)
        .await
        .map_err(ActivityFailure::generic)?;

    let prior = heartbeat_details(&progress);
    let mut hbd = if v2 {
        match (&prior, &params.progress) {
            (None, Some(p)) => p.clone(),
            _ => prior.clone().unwrap_or_default(),
        }
    } else {
        prior.clone().unwrap_or_default()
    };
    if (v2 && hbd.total_estimate == 0) || (!v2 && prior.is_none()) {
        hbd.total_estimate = backend
            .count(&params.domain_name, &params.query)
            .await
            .map_err(ActivityFailure::generic)?;
    }
    if v2 {
        hbd.rps = params.rps;
        hbd.concurrency = params.concurrency;
    }
    let cancelled = |hbd: &HeartBeatDetails| {
        if v2 {
            ActivityFailure::CanceledWith(converter::encode(hbd).unwrap_or_default())
        } else {
            ActivityFailure::generic("context canceled")
        }
    };

    let shared = Arc::new(Shared {
        backend: backend.clone(),
        progress: progress.clone(),
        limiter: Limiter::new(params.rps),
        non_retryable: params.non_retryable_errors.clone().unwrap_or_default(),
        identity: if v2 { V2_WORKFLOW_TYPE } else { V1_WORKFLOW_TYPE },
        domain_id,
        params: params.clone(),
        queue: Mutex::new(VecDeque::new()),
        remaining: AtomicUsize::new(0),
        succeeded: AtomicUsize::new(0),
        failed: AtomicUsize::new(0),
    });

    loop {
        let page = backend
            .scan(&params.domain_name, params.page_size, &hbd.page_token, &params.query)
            .await
            .map_err(ActivityFailure::generic)?;
        let batch_count = page.executions.len();
        if batch_count == 0 {
            break;
        }
        shared.succeeded.store(0, Ordering::SeqCst);
        shared.failed.store(0, Ordering::SeqCst);
        shared.remaining.store(batch_count, Ordering::SeqCst);
        shared.queue.lock().unwrap().extend(page.executions.into_iter().map(|execution| {
            TaskDetail {
                execution,
                attempts: 0,
                hbd: hbd.clone(),
            }
        }));
        let mut workers = tokio::task::JoinSet::new();
        for _ in 0..params.concurrency.max(1) {
            workers.spawn(shared.clone().processor());
        }
        loop {
            if progress.is_cancelled() {
                workers.abort_all();
                return Err(cancelled(&hbd));
            }
            if shared.remaining.load(Ordering::SeqCst) == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        while workers.join_next().await.is_some() {}

        hbd.current_page += 1;
        hbd.page_token = page.next_page_token;
        hbd.success_count += shared.succeeded.load(Ordering::SeqCst) as i32;
        hbd.error_count += shared.failed.load(Ordering::SeqCst) as i32;
        progress.heartbeat(&hbd).await;
        if v2 && progress.is_cancelled() {
            return Err(cancelled(&hbd));
        }
        if hbd.page_token.is_empty() {
            break;
        }
    }
    Ok(hbd)
}

/// Registers both activities.
pub fn register_activities<B: Backend>(registry: &mut ActivityRegistry, backend: Arc<B>) {
    for (name, v2) in [(ACTIVITY_TYPE, false), (V2_ACTIVITY_TYPE, true)] {
        let backend = backend.clone();
        registry.register(name, move |ctx, input| {
            let backend = backend.clone();
            async move {
                let mut params: BatchParams = converter::decode(&input)
                    .map_err(|e| ActivityFailure::generic(e.to_string()))?;
                if params.non_retryable_errors.as_ref().is_some_and(|v| v.is_empty()) {
                    params.non_retryable_errors = None;
                }
                let hbd = run_batch(backend, ctx, params, v2).await?;
                converter::encode(&hbd)
                    .map(Some)
                    .map_err(|e| ActivityFailure::generic(e.to_string()))
            }
        });
    }
}


#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;

    use super::*;

    #[derive(Default)]
    struct Fake {
        pages: Vec<Vec<&'static str>>,
        terminated: Mutex<Vec<String>>,
        missing: Vec<&'static str>,
        failing: Vec<&'static str>,
        children: Vec<(&'static str, Vec<&'static str>)>,
    }

    fn exec(id: &str) -> WorkflowExecution {
        WorkflowExecution {
            workflow_id: id.into(),
            run_id: "r".into(),
        }
    }

    impl Backend for Fake {
        fn current_cluster(&self) -> String {
            "active".into()
        }
        fn check_remote_admin(&self, _: &str) -> Result<(), String> {
            Ok(())
        }
        async fn describe_domain_id(&self, _: &str) -> Result<String, String> {
            Ok("did".into())
        }
        async fn count(&self, _: &str, _: &str) -> Result<i64, String> {
            Ok(42)
        }
        async fn scan(&self, _: &str, _: i32, token: &[u8], _: &str) -> Result<ScanPage, String> {
            let index = token.first().map_or(0, |b| *b as usize);
            let executions = self
                .pages
                .get(index)
                .map(|p| p.iter().map(|id| exec(id)).collect())
                .unwrap_or_default();
            let next = if index + 1 < self.pages.len() { vec![(index + 1) as u8] } else { vec![] };
            Ok(ScanPage { executions, next_page_token: next })
        }
        async fn terminate(
            &self,
            _: &str,
            wf: &WorkflowExecution,
            _: &str,
            _: &str,
        ) -> Result<(), BatchOperationError<String>> {
            if self.missing.contains(&wf.workflow_id.as_str()) {
                return Err(BatchOperationError::EntityNotExists);
            }
            if self.failing.contains(&wf.workflow_id.as_str()) {
                return Err(BatchOperationError::Other("boom".into()));
            }
            self.terminated.lock().unwrap().push(wf.workflow_id.clone());
            Ok(())
        }
        async fn request_cancel(&self, _: &str, _: &WorkflowExecution, _: &str, _: &str)
            -> Result<(), BatchOperationError<String>> { Ok(()) }
        async fn signal(&self, _: &str, _: &WorkflowExecution, _: &str, _: &str, _: &str, _: &[u8])
            -> Result<(), BatchOperationError<String>> { Ok(()) }
        async fn resend_replication(&self, _: &str, _: &str, _: &WorkflowExecution, _: &str)
            -> Result<(), BatchOperationError<String>> { Ok(()) }
        async fn refresh(&self, _: &str, _: &WorkflowExecution)
            -> Result<(), BatchOperationError<String>> { Ok(()) }
        async fn describe_pending_children(&self, _: &str, wf: &WorkflowExecution)
            -> Result<Vec<PendingChild>, BatchOperationError<String>> {
            Ok(self
                .children
                .iter()
                .filter(|(p, _)| *p == wf.workflow_id)
                .flat_map(|(_, c)| c.iter())
                .map(|c| PendingChild { workflow_id: (*c).into(), run_id: "r".into() })
                .collect())
        }
    }

    #[derive(Clone, Default)]
    struct FakeProgress {
        beats: Arc<Mutex<Vec<HeartBeatDetails>>>,
        cancelled: Arc<AtomicBool>,
        prior: Option<Vec<u8>>,
    }

    impl Progress for FakeProgress {
        async fn heartbeat(&self, d: &HeartBeatDetails) {
            self.beats.lock().unwrap().push(d.clone());
            // cancel after the first page like a tune signal would
        }
        fn is_cancelled(&self) -> bool {
            self.cancelled.load(Ordering::SeqCst)
        }
        fn prior_details(&self) -> Option<Vec<u8>> {
            self.prior.clone()
        }
    }

    fn params() -> BatchParams {
        validate_and_default(BatchParams {
            domain_name: "d".into(),
            query: "q".into(),
            reason: "r".into(),
            batch_type: "terminate".into(),
            rps: 1000,
            concurrency: 3,
            attempts_on_retryable_error: 2,
            ..Default::default()
        })
        .unwrap()
        .0
    }

    #[tokio::test]
    async fn processes_pages_counts_failures_and_skips_missing() {
        let backend = Arc::new(Fake {
            pages: vec![vec!["a", "b", "gone"], vec!["c", "bad"]],
            missing: vec!["gone"],
            failing: vec!["bad"],
            children: vec![("a", vec!["a-child"])],
            ..Default::default()
        });
        let progress = FakeProgress::default();
        let hbd = run_batch(backend.clone(), progress.clone(), params(), false).await.unwrap();
        assert_eq!(hbd.total_estimate, 42);
        assert_eq!(hbd.success_count, 4);
        assert_eq!(hbd.error_count, 1);
        assert_eq!(hbd.current_page, 2);
        assert!(hbd.page_token.is_empty());
        let mut done = backend.terminated.lock().unwrap().clone();
        done.sort();
        assert_eq!(done, vec!["a", "a-child", "b", "c"]);
        assert!(progress.beats.lock().unwrap().len() >= 2);
    }

    #[tokio::test]
    async fn v2_resumes_from_progress_and_reports_tuned_values() {
        let backend = Arc::new(Fake {
            pages: vec![vec!["a"], vec!["b"]],
            ..Default::default()
        });
        let mut p = params();
        p.progress = Some(HeartBeatDetails {
            page_token: vec![1],
            current_page: 1,
            total_estimate: 9,
            success_count: 1,
            ..Default::default()
        });
        let hbd = run_batch(backend.clone(), FakeProgress::default(), p, true).await.unwrap();
        assert_eq!(*backend.terminated.lock().unwrap(), vec!["b"]);
        assert_eq!(hbd.total_estimate, 9);
        assert_eq!(hbd.success_count, 2);
        assert_eq!((hbd.rps, hbd.concurrency), (1000, 3));
    }

    #[tokio::test]
    async fn v2_cancel_returns_progress_as_cancel_details() {
        let backend = Arc::new(Fake {
            pages: vec![vec!["a"], vec!["b"]],
            ..Default::default()
        });
        let progress = FakeProgress::default();
        progress.cancelled.store(true, Ordering::SeqCst);
        let err = run_batch(backend, progress, params(), true).await.unwrap_err();
        let ActivityFailure::CanceledWith(d) = err else {
            panic!("expected cancel with details");
        };
        let hbd: HeartBeatDetails = converter::decode(&d).unwrap();
        assert_eq!(hbd.total_estimate, 42);
    }

    #[tokio::test]
    async fn replicate_from_wrong_cluster_is_non_retriable() {
        let mut p = params();
        p.batch_type = "replicate".into();
        p.replicate_params = ReplicateParams {
            source_cluster: "other".into(),
            target_cluster: "t".into(),
        };
        let err = run_batch(Arc::new(Fake::default()), FakeProgress::default(), p, false)
            .await
            .unwrap_err();
        let ActivityFailure::Failed { reason, .. } = err else { panic!("expected failure") };
        assert_eq!(reason, NON_RETRIABLE_ERROR_REASON);
    }
}
