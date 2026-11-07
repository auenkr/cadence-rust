//! Port of Go `service/worker/archiver/client.go` (the archiver `Client` used by the history
//! transfer / timer executors): `Archive` first tries inline archival for each requested target
//! (rate limited), and whatever is left is sent to the archival system workflow with a
//! signal-with-start (`cadence-archival-<n>`).
//!
//! The write-side archivers (Go `historyArchiver.Archive` / `visibilityArchiver.Archive` of the
//! filestore / s3store / gstorage plugins; cadence-common only has the read side) and the
//! signal-with-start client (needs the Rust worker's archival workflow, which does not exist
//! yet) are seams: [`InlineArchivers`] and [`ArchivalSignaler`]. Until real implementations exist
//! the default ones fail with an explicit error, so the callers keep the task (and the data).
//! TODO(link): real filestore/s3/gcs write archivers and frontend signal-with-start client.

use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use async_trait::async_trait;

/// Go `archiver.ArchivalTarget`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ArchivalTarget {
    History,
    Visibility,
}

/// Go `archiver.ArchiveRequest` (the fields shared by both targets).
#[derive(Clone, Debug, Default)]
pub struct ArchiveRequest {
    pub domain_id: String,
    pub domain_name: String,
    pub workflow_id: String,
    pub run_id: String,
    pub shard_id: i32,
    pub branch_token: Vec<u8>,
    pub next_event_id: i64,
    pub close_failover_version: i64,
    /// History archival URI (Go field name `URI`).
    pub uri: String,
    pub workflow_type_name: String,
    pub start_timestamp: i64,
    pub execution_timestamp: i64,
    pub close_timestamp: i64,
    pub close_status: i32,
    pub history_length: i64,
    pub memo: Option<std::collections::BTreeMap<String, Vec<u8>>>,
    pub search_attributes: std::collections::BTreeMap<String, Vec<u8>>,
    pub visibility_uri: String,
    pub targets: Vec<ArchivalTarget>,
}

/// Go `archiver.ClientRequest`.
#[derive(Clone, Debug)]
pub struct ClientRequest {
    pub archive_request: ArchiveRequest,
    pub caller_service: String,
    pub attempt_archive_inline: bool,
}

/// Go `archiver.ClientResponse`.
#[derive(Clone, Copy, Debug, Default)]
pub struct ClientResponse {
    pub history_archived_inline: bool,
}

/// Inline (in-process) write archivers: Go `provider.GetHistoryArchiver(..).Archive` /
/// `GetVisibilityArchiver(..).Archive` selected by the URI scheme.
#[async_trait]
pub trait InlineArchivers: Send + Sync {
    async fn archive_history(&self, request: &ClientRequest) -> Result<(), String>;
    async fn archive_visibility(&self, request: &ClientRequest) -> Result<(), String>;
}

/// Go `cadenceClient.SignalWithStartWorkflow(archivalWorkflowFnName, signalName, request)`.
#[async_trait]
pub trait ArchivalSignaler: Send + Sync {
    /// `workflow_id` is `cadence-archival-<rand(numWorkflows)>`.
    async fn signal_with_start(&self, workflow_id: &str, request: &ArchiveRequest, timeout: Duration) -> Result<(), String>;
}

/// Go `workflowIDPrefix`, `signalTimeout`, `tooManyRequestsErrMsg`.
pub const WORKFLOW_ID_PREFIX: &str = "cadence-archival";
pub const SIGNAL_TIMEOUT: Duration = Duration::from_millis(300);
pub const TOO_MANY_REQUESTS_ERR_MSG: &str = "too many requests to archival workflow";
const ERR_INLINE_ARCHIVAL_THROTTLED: &str = "inline archival throttled";

/// Go `quotas.Limiter.Allow()` (non-blocking, rate re-read on every call).
pub struct AllowLimiter {
    rps: Box<dyn Fn() -> i64 + Send + Sync>,
    next_free: Mutex<Option<Instant>>,
}

impl AllowLimiter {
    pub fn new(rps: impl Fn() -> i64 + Send + Sync + 'static) -> Self {
        Self { rps: Box::new(rps), next_free: Mutex::new(None) }
    }
    pub fn allow(&self) -> bool {
        let rps = (self.rps)();
        if rps <= 0 {
            return false;
        }
        let interval = Duration::from_secs_f64(1.0 / rps as f64);
        let now = Instant::now();
        let mut next = self.next_free.lock().unwrap();
        // burst of one token (Go rate.Limiter with burst = max(rps,1) is approximated by 1 token/interval).
        if next.is_some_and(|n| n > now) {
            return false;
        }
        *next = Some(now + interval);
        true
    }
}

pub struct ArchiverClient {
    num_workflows: Box<dyn Fn() -> i64 + Send + Sync>,
    rate_limiter: AllowLimiter,
    inline_history_rate_limiter: AllowLimiter,
    inline_visibility_rate_limiter: AllowLimiter,
    inline: Arc<dyn InlineArchivers>,
    signaler: Arc<dyn ArchivalSignaler>,
}

impl ArchiverClient {
    pub fn new(
        num_workflows: Box<dyn Fn() -> i64 + Send + Sync>,
        rate_limiter: AllowLimiter,
        inline_history_rate_limiter: AllowLimiter,
        inline_visibility_rate_limiter: AllowLimiter,
        inline: Arc<dyn InlineArchivers>,
        signaler: Arc<dyn ArchivalSignaler>,
    ) -> Self {
        Self { num_workflows, rate_limiter, inline_history_rate_limiter, inline_visibility_rate_limiter, inline, signaler }
    }

    /// Go `client.Archive`.
    pub async fn archive(&self, mut request: ClientRequest) -> Result<ClientResponse, String> {
        let mut resp = ClientResponse::default();
        if request.attempt_archive_inline {
            // Go runs one goroutine per target and then collects the results in target order.
            let targets = request.archive_request.targets.clone();
            let futs = targets.iter().map(|target| async {
                match target {
                    ArchivalTarget::History => self.archive_history_inline(&request).await,
                    ArchivalTarget::Visibility => self.archive_visibility_inline(&request).await,
                }
            });
            let results = futures_util::future::join_all(futs).await;
            let mut remaining = Vec::new();
            for (target, result) in targets.into_iter().zip(results) {
                if result.is_err() {
                    remaining.push(target);
                } else if target == ArchivalTarget::History {
                    resp.history_archived_inline = true;
                }
            }
            request.archive_request.targets = remaining;
        }
        if !request.archive_request.targets.is_empty() {
            self.send_archive_signal(&request.archive_request).await?;
        }
        Ok(resp)
    }

    async fn archive_history_inline(&self, request: &ClientRequest) -> Result<(), String> {
        if !self.inline_history_rate_limiter.allow() {
            tracing::debug!("inline history archival throttled");
            return Err(ERR_INLINE_ARCHIVAL_THROTTLED.into());
        }
        let result = self.inline.archive_history(request).await;
        if let Err(e) = &result {
            tracing::info!(error = %e, "failed to perform workflow history archival inline");
        }
        result
    }

    async fn archive_visibility_inline(&self, request: &ClientRequest) -> Result<(), String> {
        if !self.inline_visibility_rate_limiter.allow() {
            tracing::debug!("inline visibility archival throttled");
            return Err(ERR_INLINE_ARCHIVAL_THROTTLED.into());
        }
        let result = self.inline.archive_visibility(request).await;
        if let Err(e) = &result {
            tracing::info!(error = %e, "failed to perform visibility archival inline");
        }
        result
    }

    async fn send_archive_signal(&self, request: &ArchiveRequest) -> Result<(), String> {
        if !self.rate_limiter.allow() {
            tracing::error!("{TOO_MANY_REQUESTS_ERR_MSG}");
            return Err(TOO_MANY_REQUESTS_ERR_MSG.into());
        }
        let n = (self.num_workflows)().max(1);
        let workflow_id = format!("{WORKFLOW_ID_PREFIX}-{}", rand::Rng::gen_range(&mut rand::thread_rng(), 0..n));
        self.signaler
            .signal_with_start(&workflow_id, request, SIGNAL_TIMEOUT)
            .await
            .inspect_err(|e| {
                tracing::error!(error = %e, domain_id = %request.domain_id, workflow_id = %request.workflow_id,
                    run_id = %request.run_id, archival_workflow_id = %workflow_id, "failed to send signal to archival system workflow");
            })
    }
}

const NOT_PORTED: &str = "archiver is not ported yet (no write-side archiver / archival workflow signal client in Rust)";

/// Inline archivers: the `file://` (filestore) scheme is written like Go (same on-disk format);
/// other schemes (s3 / gs) and history without a [`HistoryBatchSource`] keep failing explicitly so
/// the task is retried and the data is kept. Go default config: fileMode 0666, dirMode 0766.
struct UnconfiguredInline {
    history_source: Option<Arc<dyn cadence_common::archiver::HistoryBatchSource + Send + Sync>>,
}
#[async_trait]
impl InlineArchivers for UnconfiguredInline {
    async fn archive_history(&self, r: &ClientRequest) -> Result<(), String> {
        let a = &r.archive_request;
        let uri = cadence_common::archiver::parse_uri(&a.uri).map_err(|e| e.to_string())?;
        if uri.scheme != cadence_common::archiver::FILESTORE_SCHEME {
            return Err(format!("inline history archival for scheme {:?}: {NOT_PORTED}", uri.scheme));
        }
        let Some(source) = self.history_source.clone() else {
            return Err(format!("inline history archival: no history batch source ({NOT_PORTED})"));
        };
        let cfg = cadence_common::archiver::FilestoreWriterConfig::new("0666", "0766").map_err(|e| e.to_string())?;
        let req = cadence_common::archiver::ArchiveHistoryRequest {
            domain_id: a.domain_id.clone(),
            domain_name: a.domain_name.clone(),
            workflow_id: a.workflow_id.clone(),
            run_id: a.run_id.clone(),
            branch_token: a.branch_token.clone(),
            next_event_id: a.next_event_id,
            close_failover_version: a.close_failover_version,
        };
        tokio::task::spawn_blocking(move || {
            cadence_common::archiver::filestore_archive_history(&cfg, &uri, &req, source.as_ref())
        })
        .await
        .map_err(|e| e.to_string())?
        .map_err(|e| e.to_string())
    }
    async fn archive_visibility(&self, r: &ClientRequest) -> Result<(), String> {
        let a = &r.archive_request;
        let uri = cadence_common::archiver::parse_uri(&a.visibility_uri).map_err(|e| e.to_string())?;
        if uri.scheme != cadence_common::archiver::FILESTORE_SCHEME {
            return Err(format!("inline visibility archival for scheme {:?}: {NOT_PORTED}", uri.scheme));
        }
        let cfg = cadence_common::archiver::FilestoreWriterConfig::new("0666", "0766").map_err(|e| e.to_string())?;
        let req = cadence_common::archiver::ArchiveVisibilityRequest {
            domain_id: a.domain_id.clone(),
            domain_name: a.domain_name.clone(),
            workflow_id: a.workflow_id.clone(),
            run_id: a.run_id.clone(),
            workflow_type_name: a.workflow_type_name.clone(),
            start_timestamp: a.start_timestamp,
            execution_timestamp: a.execution_timestamp,
            close_timestamp: a.close_timestamp,
            close_status: a.close_status,
            history_length: a.history_length,
            memo: a.memo.clone(),
            search_attributes: Some(
                a.search_attributes
                    .iter()
                    .map(|(k, v)| (k.clone(), String::from_utf8_lossy(v).into_owned()))
                    .collect(),
            ),
            history_archival_uri: a.uri.clone(),
        };
        tokio::task::spawn_blocking(move || cadence_common::archiver::filestore_archive_visibility(&cfg, &uri, &req))
            .await
            .map_err(|e| e.to_string())?
            .map_err(|e| e.to_string())
    }
}
struct UnconfiguredSignaler;
#[async_trait]
impl ArchivalSignaler for UnconfiguredSignaler {
    async fn signal_with_start(&self, _id: &str, _r: &ArchiveRequest, _t: Duration) -> Result<(), String> {
        Err(format!("archival signal: {NOT_PORTED}"))
    }
}

/// Process-wide client with the default seams (Go: `resource.GetArchiverClient`). The rates are
/// the Go defaults of `ArchiveRequestRPS` (300), `ArchiveInlineHistoryRPS` (1000) and
/// `ArchiveInlineVisibilityRPS` (1000); `NumArchiveSystemWorkflows` defaults to 1000.
pub fn default_client() -> &'static ArchiverClient {
    static CLIENT: OnceLock<ArchiverClient> = OnceLock::new();
    CLIENT.get_or_init(|| {
        ArchiverClient::new(
            Box::new(|| 1000),
            AllowLimiter::new(|| 300),
            AllowLimiter::new(|| 1000),
            AllowLimiter::new(|| 1000),
            Arc::new(UnconfiguredInline { history_source: None }),
            Arc::new(UnconfiguredSignaler),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Fake {
        history_ok: bool,
    }
    #[async_trait]
    impl InlineArchivers for Fake {
        async fn archive_history(&self, _r: &ClientRequest) -> Result<(), String> {
            if self.history_ok { Ok(()) } else { Err("boom".into()) }
        }
        async fn archive_visibility(&self, _r: &ClientRequest) -> Result<(), String> {
            Err("vis".into())
        }
    }
    struct Sig(Mutex<Vec<(String, usize)>>);
    #[async_trait]
    impl ArchivalSignaler for Sig {
        async fn signal_with_start(&self, id: &str, r: &ArchiveRequest, _t: Duration) -> Result<(), String> {
            self.0.lock().unwrap().push((id.to_string(), r.targets.len()));
            Ok(())
        }
    }

    fn client(history_ok: bool, sig: Arc<Sig>) -> ArchiverClient {
        ArchiverClient::new(
            Box::new(|| 1),
            AllowLimiter::new(|| 100),
            AllowLimiter::new(|| 100),
            AllowLimiter::new(|| 100),
            Arc::new(Fake { history_ok }),
            sig,
        )
    }

    fn req(inline: bool) -> ClientRequest {
        ClientRequest {
            archive_request: ArchiveRequest { targets: vec![ArchivalTarget::History, ArchivalTarget::Visibility], ..Default::default() },
            caller_service: "history".into(),
            attempt_archive_inline: inline,
        }
    }

    #[tokio::test]
    async fn inline_history_success_signals_only_remaining_targets() {
        let sig = Arc::new(Sig(Mutex::new(vec![])));
        let resp = client(true, sig.clone()).archive(req(true)).await.unwrap();
        assert!(resp.history_archived_inline);
        assert_eq!(*sig.0.lock().unwrap(), vec![("cadence-archival-0".to_string(), 1)]);
    }

    #[tokio::test]
    async fn not_inline_signals_all_targets() {
        let sig = Arc::new(Sig(Mutex::new(vec![])));
        let resp = client(true, sig.clone()).archive(req(false)).await.unwrap();
        assert!(!resp.history_archived_inline);
        assert_eq!(sig.0.lock().unwrap()[0].1, 2);
    }

    #[tokio::test]
    async fn default_client_fails_explicitly() {
        // empty URIs are invalid -> explicit error, task is kept
        assert!(default_client().archive(req(true)).await.is_err());
    }
}
