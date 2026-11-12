//! Archive (write) path of the Go `s3store` and `gcloud` archivers.
//!
//! Mirrors `go/common/archiver/s3store/{historyArchiver,visibilityArchiver,util}.go` `Archive`
//! and `go/common/archiver/gcloud/{historyArchiver,visibilityArchiver,util}.go` `Archive`
//! (uploads through `connector.Client.Upload`/`Exist`). History batches come from the same
//! [`HistoryBatchSource`] the filestore archiver uses; [`HistoryIterator`] re-splits them into
//! blobs exactly like Go's `archiver.historyIterator` (state `NextEventID`/`FinishedIteration`,
//! JSON size estimator, 2MB target) and the archivers drive it with the Go `ProgressManager`
//! protocol, metrics and `NonRetriableError` substitution.

use std::fmt;
use std::sync::Arc;
use std::time::Instant;

use serde_json::{json, Value};

use super::{
    gcs_hash, gcs_history_filename, gcs_object, gcs_validate_uri, s3_bucket, s3_history_key,
    s3_key_path, s3_soft_validate_uri, s3_visibility_prefix, ObjectStore, StoreError,
    StoreErrorKind, ERR_BUCKET_NOT_EXISTS, GCS_ERR_BUCKET_NOT_EXIST,
};
use crate::archiver::{
    validate_history_request, validate_visibility_request, ArchiveHistoryRequest,
    ArchiveVisibilityRequest, ArchiverError, ArchiverUri, HistoryBatchSource, ERR_INVALID_URI,
    ERR_URI_SCHEME_MISMATCH, ERR_HISTORY_NOT_EXIST,
};

/// Go `targetHistoryBlobSize` (s3store / gcloud): 2MB.
pub const TARGET_HISTORY_BLOB_SIZE: usize = 2 * 1024 * 1024;
/// Go gcloud `errUploadNonRetriable`.
pub const GCS_ERR_UPLOAD_NON_RETRIABLE: &str = "upload non-retriable error";
/// Go gcloud `errRetriable`.
pub const GCS_ERR_RETRIABLE: &str = "retriable error";

const S3_INDEX_CLOSE: &str = "closeTimeout";
const S3_INDEX_START: &str = "startTimeout";
const S3_PRIMARY_TYPE: &str = "workflowTypeName";
const S3_PRIMARY_ID: &str = "workflowID";
const GCS_INDEX_CLOSE: &str = "closeTimeout";
const GCS_INDEX_START: &str = "startTimeout";

/// Go metrics scope operation names (`metrics.HistoryArchiverScope` / `VisibilityArchiverScope`).
pub const HISTORY_ARCHIVER_SCOPE: &str = "HistoryArchiver";
pub const VISIBILITY_ARCHIVER_SCOPE: &str = "VisibilityArchiver";

/// Go metric names (`common/metrics/defs.go`) emitted by the s3/gcloud archivers.
pub mod metric {
    pub const CADENCE_LATENCY: &str = "cadence_latency";
    pub const CADENCE_LATENCY_HISTOGRAM: &str = "cadence_latency_ns";
    pub const HISTORY_ARCHIVE_NON_RETRYABLE_ERROR: &str = "history_archiver_archive_non_retryable_error";
    pub const HISTORY_ARCHIVE_TRANSIENT_ERROR: &str = "history_archiver_archive_transient_error";
    pub const HISTORY_ARCHIVE_SUCCESS: &str = "history_archiver_archive_success";
    pub const HISTORY_TOTAL_UPLOAD_SIZE: &str = "history_archiver_total_upload_size";
    pub const HISTORY_TOTAL_UPLOAD_SIZE_HISTOGRAM: &str = "history_archiver_total_upload_size_counts";
    pub const HISTORY_SIZE: &str = "history_archiver_history_size";
    pub const HISTORY_SIZE_HISTOGRAM: &str = "history_archiver_history_size_counts";
    pub const HISTORY_DUPLICATE_ARCHIVALS: &str = "history_archiver_duplicate_archivals";
    pub const HISTORY_BLOB_EXISTS: &str = "history_archiver_blob_exists";
    pub const HISTORY_BLOB_SIZE: &str = "history_archiver_blob_size";
    pub const HISTORY_BLOB_SIZE_HISTOGRAM: &str = "history_archiver_blob_size_counts";
    pub const VISIBILITY_ARCHIVE_NON_RETRYABLE_ERROR: &str = "visibility_archiver_archive_non_retryable_error";
    pub const VISIBILITY_ARCHIVE_TRANSIENT_ERROR: &str = "visibility_archiver_archive_transient_error";
    pub const VISIBILITY_ARCHIVE_SUCCESS: &str = "visibility_archiver_archive_success";
}

/// Go `metrics.Scope` as used by the archivers (`MetricsClient.Scope(scope, DomainTag(domain))`).
pub trait ArchiverMetrics: Send + Sync {
    fn inc_counter(&self, scope: &str, domain: &str, name: &str);
    fn add_counter(&self, scope: &str, domain: &str, name: &str, delta: i64);
    /// Go `RecordTimer(name, time.Duration(v))` (also used for byte sizes).
    fn record_timer(&self, scope: &str, domain: &str, name: &str, value: i64);
    /// Go `IntExponentialHistogram`.
    fn int_histogram(&self, scope: &str, domain: &str, name: &str, value: i64);
}

/// Metrics sink that drops everything (Go `metrics.NoopClient`).
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopArchiverMetrics;

impl ArchiverMetrics for NoopArchiverMetrics {
    fn inc_counter(&self, _: &str, _: &str, _: &str) {}
    fn add_counter(&self, _: &str, _: &str, _: &str, _: i64) {}
    fn record_timer(&self, _: &str, _: &str, _: &str, _: i64) {}
    fn int_histogram(&self, _: &str, _: &str, _: &str, _: i64) {}
}

/// Go `archiver.ProgressManager` payload shared by the s3 `uploadProgress` (`BatchIdx`) and the
/// gcloud `progress` (`CurrentPageNumber`) structs; `IteratorState` is the encoded iterator state.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ArchiveProgress {
    pub batch_idx: usize,
    pub iterator_state: Option<Vec<u8>>,
}

/// Go `archiver.ProgressManager`.
pub trait ProgressManager: Send + Sync {
    fn has_progress(&self) -> bool;
    fn load_progress(&self) -> Result<ArchiveProgress, String>;
    fn record_progress(&self, progress: &ArchiveProgress) -> Result<(), String>;
}

/// Go `archiver.ArchiveFeatureCatalog` + the metrics client of the archiver container.
#[derive(Clone)]
pub struct ArchiveOptions {
    pub metrics: Arc<dyn ArchiverMetrics>,
    /// Go `NonRetriableError`: replaces any non-retryable error returned by `Archive`.
    pub non_retriable_error: Option<Arc<dyn Fn() -> ArchiverError + Send + Sync>>,
    pub progress_manager: Option<Arc<dyn ProgressManager>>,
}

impl Default for ArchiveOptions {
    fn default() -> Self {
        Self { metrics: Arc::new(NoopArchiverMetrics), non_retriable_error: None, progress_manager: None }
    }
}

impl fmt::Debug for ArchiveOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ArchiveOptions")
            .field("non_retriable_error", &self.non_retriable_error.is_some())
            .field("progress_manager", &self.progress_manager.is_some())
            .finish()
    }
}

/// Go `time.Unix(0, ts).UTC().Format(time.RFC3339)`.
fn rfc3339_utc(ts: i64) -> String {
    chrono::DateTime::from_timestamp_nanos(ts)
        .format("%Y-%m-%dT%H:%M:%SZ")
        .to_string()
}

fn event_i64(batch: &Value, last: bool, field: &str) -> i64 {
    let events = batch.get("events").and_then(Value::as_array);
    let ev = events.and_then(|e| if last { e.last() } else { e.first() });
    ev.and_then(|e| e.get(field)).and_then(Value::as_i64).unwrap_or(0)
}

/// Go `historyIteratorState` (JSON without tags: Go field names).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IteratorState {
    pub next_event_id: i64,
    pub finished: bool,
}

impl IteratorState {
    /// Go `GetState` (`json.Marshal(historyIteratorState)`).
    pub fn encode(&self) -> Vec<u8> {
        format!(
            "{{\"NextEventID\":{},\"FinishedIteration\":{}}}",
            self.next_event_id, self.finished
        )
        .into_bytes()
    }

    pub fn decode(bytes: &[u8]) -> Option<Self> {
        let v: Value = serde_json::from_slice(bytes).ok()?;
        Some(Self {
            next_event_id: v.get("NextEventID")?.as_i64()?,
            finished: v.get("FinishedIteration")?.as_bool()?,
        })
    }
}

/// Go `json.Marshal` bytes of a JSON value: compact, with Go's HTML-safe string escaping
/// (`<`, `>`, `&`, U+2028, U+2029 as `\u00xx`/`\u202x`).
pub fn go_json_bytes(v: &Value) -> Vec<u8> {
    fn string(out: &mut Vec<u8>, s: &str) {
        let enc = serde_json::to_string(s).unwrap_or_default();
        let enc = enc
            .replace('<', "\\u003c")
            .replace('>', "\\u003e")
            .replace('&', "\\u0026")
            .replace('\u{2028}', "\\u2028")
            .replace('\u{2029}', "\\u2029");
        out.extend_from_slice(enc.as_bytes());
    }
    fn walk(out: &mut Vec<u8>, v: &Value) {
        match v {
            Value::String(s) => string(out, s),
            Value::Array(a) => {
                out.push(b'[');
                for (i, x) in a.iter().enumerate() {
                    if i > 0 {
                        out.push(b',');
                    }
                    walk(out, x);
                }
                out.push(b']');
            }
            Value::Object(m) => {
                out.push(b'{');
                for (i, (k, x)) in m.iter().enumerate() {
                    if i > 0 {
                        out.push(b',');
                    }
                    string(out, k);
                    out.push(b':');
                    walk(out, x);
                }
                out.push(b'}');
            }
            other => out.extend_from_slice(other.to_string().as_bytes()),
        }
    }
    let mut out = Vec::new();
    walk(&mut out, v);
    out
}

/// Go `types.HistoryEvent` tags `eventId,omitempty`, `version,omitempty`, `taskId,omitempty`:
/// zero values are not part of Go's encoding (Rust always serialises them).
fn go_history_batch(mut batch: Value) -> Value {
    if let Some(events) = batch.get_mut("events").and_then(Value::as_array_mut) {
        for e in events {
            if let Some(m) = e.as_object_mut() {
                for f in ["eventId", "version", "taskId"] {
                    if m.get(f).and_then(Value::as_i64) == Some(0) {
                        m.remove(f);
                    }
                }
                // `WorkflowExecutionStartedEventAttributes.Attempt` is `json:"attempt,omitempty"`.
                if let Some(a) = m
                    .get_mut("workflowExecutionStartedEventAttributes")
                    .and_then(Value::as_object_mut)
                {
                    if a.get("attempt").and_then(Value::as_i64) == Some(0) {
                        a.remove("attempt");
                    }
                }
            }
        }
    }
    batch
}

/// Go `jsonSizeEstimator.EstimateSize`: length of the Go-compatible JSON encoding.
pub fn estimate_size(batch: &Value) -> usize {
    go_json_bytes(batch).len()
}

/// Go `archiver.IsHistoryMutated` over JSON batches.
pub fn is_history_mutated(request: &ArchiveHistoryRequest, batches: &[Value], is_last: bool) -> bool {
    let Some(last) = batches.last() else {
        return false;
    };
    let version = event_i64(last, true, "version");
    if version > request.close_failover_version {
        return true;
    }
    if !is_last {
        return false;
    }
    version != request.close_failover_version || event_i64(last, true, "eventId") + 1 != request.next_event_id
}

/// Go `archiver.historyIterator`: pages through the history through the source
/// (`ReadHistoryBranch` pages, like `readHistory`) and closes a blob once the Go-encoded size
/// reaches the target; the iteration is finished when no batch follows the blob.
pub struct HistoryIterator<'a> {
    request: &'a ArchiveHistoryRequest,
    source: &'a dyn HistoryBatchSource,
    target: usize,
    state: IteratorState,
}

impl<'a> HistoryIterator<'a> {
    /// Go `NewHistoryIterator`.
    pub fn new(request: &'a ArchiveHistoryRequest, source: &'a dyn HistoryBatchSource, target: usize) -> Self {
        Self { request, source, target, state: IteratorState { next_event_id: 1, finished: false } }
    }

    /// Go `NewHistoryIteratorFromState`; undecodable state is an error (caller falls back to a
    /// fresh iterator like Go's `loadHistoryIterator`).
    pub fn from_state(
        request: &'a ArchiveHistoryRequest,
        source: &'a dyn HistoryBatchSource,
        target: usize,
        state: &[u8],
    ) -> Result<Self, String> {
        let state = IteratorState::decode(state).ok_or("invalid iterator state")?;
        Ok(Self { request, source, target, state })
    }

    /// Go `HasNext`.
    pub fn has_next(&self) -> bool {
        !self.state.finished
    }

    /// Go `GetState`.
    pub fn state(&self) -> Vec<u8> {
        self.state.encode()
    }

    /// Go `readHistory`.
    fn read_history(&self, first_event_id: i64) -> Result<Vec<Value>, ArchiverError> {
        Ok(self
            .source
            .read_page(self.request, first_event_id)?
            .into_iter()
            .map(go_history_batch)
            .collect())
    }

    /// Go `readHistoryBatches`.
    fn read_history_batches(&self, first_event_id: i64) -> Result<(Vec<Value>, IteratorState), ArchiverError> {
        let mut first = first_event_id;
        let mut size = 0_usize;
        let mut out: Vec<Value> = Vec::new();
        // Go: `newIterState := historyIteratorState{}` (NextEventID 0 once finished).
        let finished = IteratorState { next_event_id: 0, finished: true };
        while size < self.target {
            let curr = match self.read_history(first) {
                Err(ArchiverError::EntityNotExists(_)) if first != 1 => return Ok((out, finished)),
                Err(e) => return Err(e),
                Ok(c) => c,
            };
            if curr.is_empty() {
                return Err(ArchiverError::EntityNotExists(ERR_HISTORY_NOT_EXIST.into()));
            }
            let n = curr.len();
            for (idx, batch) in curr.into_iter().enumerate() {
                size += estimate_size(&batch);
                first = event_i64(&batch, true, "eventId") + 1;
                out.push(batch);
                // Target reached before the end of this page: return immediately.
                if size >= self.target && idx != n - 1 {
                    return Ok((out, IteratorState { next_event_id: first, finished: false }));
                }
            }
        }
        // Target met after the last batch read: look ahead for more history.
        match self.read_history(first) {
            Err(ArchiverError::EntityNotExists(_)) if first != 1 => Ok((out, finished)),
            Err(e) => Err(e),
            Ok(_) => Ok((out, IteratorState { next_event_id: first, finished: false })),
        }
    }

    /// Go `Next`: the next `HistoryBlob` (`header` + `body`) as JSON.
    pub fn next_blob(&mut self) -> Result<Value, ArchiverError> {
        if self.state.finished {
            return Err(ArchiverError::Other("iterator depleted".to_owned()));
        }
        let (taken, state) = self.read_history_batches(self.state.next_event_id)?;
        if taken.is_empty() {
            // Go would index an empty slice here; surface it as a vanished history.
            return Err(ArchiverError::EntityNotExists(ERR_HISTORY_NOT_EXIST.into()));
        }
        self.state = state;
        Ok(history_blob(self.request, &taken, state.finished))
    }
}

/// `ErrHistoryMutated` text.
pub const ERR_HISTORY_MUTATED: &str = "history was mutated";

/// `IsHistoryMutated(request, blob.Body, *blob.Header.IsLast)`.
fn blob_mutated(request: &ArchiveHistoryRequest, blob: &Value) -> bool {
    let body: Vec<Value> = blob["body"].as_array().cloned().unwrap_or_default();
    is_history_mutated(request, &body, blob["header"]["is_last"].as_bool().unwrap_or(false))
}

/// Go `archiver.HistoryBlob` (`header` + `body`) as JSON, matching the Go struct tags.
fn history_blob(request: &ArchiveHistoryRequest, batches: &[Value], is_last: bool) -> Value {
    let (first, last) = (&batches[0], &batches[batches.len() - 1]);
    let event_count: usize = batches
        .iter()
        .map(|b| b.get("events").and_then(Value::as_array).map_or(0, Vec::len))
        .sum();
    json!({
        "header": {
            "domain_name": request.domain_name,
            "domain_id": request.domain_id,
            "workflow_id": request.workflow_id,
            "run_id": request.run_id,
            "is_last": is_last,
            "first_failover_version": event_i64(first, false, "version"),
            "last_failover_version": event_i64(last, true, "version"),
            "first_event_id": event_i64(first, false, "eventId"),
            "last_event_id": event_i64(last, true, "eventId"),
            "event_count": event_count,
        },
        "body": batches,
    })
}

fn encode(v: &impl serde::Serialize) -> Result<Vec<u8>, ArchiverError> {
    let v = serde_json::to_value(v).map_err(|e| ArchiverError::Other(e.to_string()))?;
    Ok(go_json_bytes(&v))
}

/// Failure of an inner archive step with the Go retryability classification.
struct Failure {
    err: ArchiverError,
    retryable: bool,
}

impl Failure {
    fn new(err: ArchiverError, retryable: bool) -> Self {
        Self { err, retryable }
    }
}

/// Go `startTimer`/deferred block shared by all four `Archive` methods: records latency, counts
/// transient vs non-retryable errors and substitutes `NonRetriableError`.
struct Scoped<'a> {
    opts: &'a ArchiveOptions,
    scope: &'static str,
    domain: &'a str,
    start: Instant,
}

impl<'a> Scoped<'a> {
    fn new(opts: &'a ArchiveOptions, scope: &'static str, domain: &'a str) -> Self {
        Self { opts, scope, domain, start: Instant::now() }
    }
    fn inc(&self, name: &str) {
        self.opts.metrics.inc_counter(self.scope, self.domain, name);
    }
    fn finish(
        self,
        result: Result<(), Failure>,
        transient_metric: &str,
        non_retryable_metric: &str,
    ) -> Result<(), ArchiverError> {
        let ns = i64::try_from(self.start.elapsed().as_nanos()).unwrap_or(i64::MAX);
        self.opts.metrics.record_timer(self.scope, self.domain, metric::CADENCE_LATENCY, ns);
        self.opts.metrics.int_histogram(self.scope, self.domain, metric::CADENCE_LATENCY_HISTOGRAM, ns);
        match result {
            Ok(()) => Ok(()),
            Err(f) if f.retryable => {
                self.inc(transient_metric);
                Err(f.err)
            }
            Err(f) => {
                self.inc(non_retryable_metric);
                Err(match &self.opts.non_retriable_error {
                    Some(sub) => sub(),
                    None => f.err,
                })
            }
        }
    }
}

// ---------------------------------------------------------------------- S3

/// Go s3store `upload`: `NoSuchBucket` becomes a `BadRequestError`.
fn s3_upload(
    store: &dyn ObjectStore,
    bucket: &str,
    key: &str,
    data: &[u8],
) -> Result<(), Failure> {
    store.put_object(bucket, key, data).map_err(|e| match e.kind {
        StoreErrorKind::NoSuchBucket => {
            Failure::new(ArchiverError::BadRequest(ERR_BUCKET_NOT_EXISTS.to_owned()), false)
        }
        kind => Failure::new(ArchiverError::Other(e.message), kind == StoreErrorKind::Retryable),
    })
}

/// Go `loadHistoryIterator` + `uploadProgress` handling shared by s3 (and gcs).
fn load_iterator<'a>(
    request: &'a ArchiveHistoryRequest,
    source: &'a dyn HistoryBatchSource,
    opts: &ArchiveOptions,
    progress: &mut ArchiveProgress,
) -> HistoryIterator<'a> {
    if let Some(pm) = &opts.progress_manager {
        if pm.has_progress() {
            if let Ok(loaded) = pm.load_progress() {
                if let Some(state) = &loaded.iterator_state {
                    if let Ok(it) = HistoryIterator::from_state(
                        request,
                        source,
                        TARGET_HISTORY_BLOB_SIZE,
                        state,
                    ) {
                        *progress = loaded;
                        return it;
                    }
                }
            }
            *progress = ArchiveProgress::default();
        }
    }
    HistoryIterator::new(request, source, TARGET_HISTORY_BLOB_SIZE)
}

/// Go `saveHistoryIteratorState` (best effort).
fn save_state(opts: &ArchiveOptions, it: &HistoryIterator<'_>, progress: &mut ArchiveProgress) {
    if let Some(pm) = &opts.progress_manager {
        progress.iterator_state = Some(it.state());
        let _ = pm.record_progress(progress);
    }
}

/// Go s3store `historyArchiver.Archive` (one `HistoryBlob` object per key `.../<version>/<batchIdx>`).
pub fn s3_archive_history(
    store: &dyn ObjectStore,
    region: &str,
    uri: &ArchiverUri,
    request: &ArchiveHistoryRequest,
    source: &dyn HistoryBatchSource,
    opts: &ArchiveOptions,
) -> Result<(), ArchiverError> {
    let scoped = Scoped::new(opts, HISTORY_ARCHIVER_SCOPE, &request.domain_name);
    let r = s3_archive_history_inner(&scoped, store, region, uri, request, source);
    scoped.finish(
        r,
        metric::HISTORY_ARCHIVE_TRANSIENT_ERROR,
        metric::HISTORY_ARCHIVE_NON_RETRYABLE_ERROR,
    )
}

fn s3_archive_history_inner(
    scoped: &Scoped<'_>,
    store: &dyn ObjectStore,
    region: &str,
    uri: &ArchiverUri,
    request: &ArchiveHistoryRequest,
    source: &dyn HistoryBatchSource,
) -> Result<(), Failure> {
    let (opts, scope, domain) = (scoped.opts, scoped.scope, scoped.domain);
    let fail = |e: ArchiverError| {
        // Go: persistence.IsTransientError (internal service error) || isRetryableError.
        let retryable = matches!(e, ArchiverError::Internal(_));
        Failure::new(e, retryable)
    };
    s3_soft_validate_uri(uri).map_err(|m| fail(ArchiverError::Other(m)))?;
    validate_history_request(request).map_err(fail)?;
    let mut progress = ArchiveProgress::default();
    let bucket = s3_bucket(uri, region).map_err(|m| fail(ArchiverError::Other(m)))?;
    let path = s3_key_path(uri);
    let mut it = load_iterator(request, source, opts, &mut progress);
    let (mut uploaded_size, mut history_size) = (0_i64, 0_i64);
    while it.has_next() {
        let blob = match it.next_blob() {
            Ok(b) => b,
            Err(ArchiverError::EntityNotExists(_)) => {
                scoped.inc(metric::HISTORY_DUPLICATE_ARCHIVALS);
                return Ok(());
            }
            Err(e) => return Err(fail(e)),
        };
        if blob_mutated(request, &blob) && !source.archive_incomplete_history() {
            // Go returns the plain `ErrHistoryMutated` (neither transient nor retryable).
            return Err(Failure::new(ArchiverError::Other(ERR_HISTORY_MUTATED.into()), false));
        }
        let encoded = encode(&blob).map_err(fail)?;
        let key = s3_history_key(
            &path,
            &request.domain_id,
            &request.workflow_id,
            &request.run_id,
            request.close_failover_version,
            progress.batch_idx as i64,
        );
        // Go keyExists: HeadObject; NotFound = false.
        let exists = store
            .head_object(&bucket, &key)
            .map_err(|e| Failure::new(ArchiverError::Other(e.message), e.kind == StoreErrorKind::Retryable))?;
        let blob_size = encoded.len() as i64;
        if exists {
            scoped.inc(metric::HISTORY_BLOB_EXISTS);
        } else {
            s3_upload(store, &bucket, &key, &encoded)?;
            uploaded_size += blob_size;
            opts.metrics.record_timer(scope, domain, metric::HISTORY_BLOB_SIZE, blob_size);
            opts.metrics.int_histogram(scope, domain, metric::HISTORY_BLOB_SIZE_HISTOGRAM, blob_size);
        }
        history_size += blob_size;
        progress.batch_idx += 1;
        save_state(opts, &it, &mut progress);
    }
    let m = &opts.metrics;
    m.record_timer(scope, domain, metric::HISTORY_TOTAL_UPLOAD_SIZE, uploaded_size);
    m.int_histogram(scope, domain, metric::HISTORY_TOTAL_UPLOAD_SIZE_HISTOGRAM, uploaded_size);
    m.record_timer(scope, domain, metric::HISTORY_SIZE, history_size);
    m.int_histogram(scope, domain, metric::HISTORY_SIZE_HISTOGRAM, history_size);
    scoped.inc(metric::HISTORY_ARCHIVE_SUCCESS);
    Ok(())
}

/// Go s3store `visibilityArchiver.Archive`: the record is uploaded under four index keys.
pub fn s3_archive_visibility(
    store: &dyn ObjectStore,
    region: &str,
    uri: &ArchiverUri,
    request: &ArchiveVisibilityRequest,
    opts: &ArchiveOptions,
) -> Result<(), ArchiverError> {
    let scoped = Scoped::new(opts, VISIBILITY_ARCHIVER_SCOPE, &request.domain_name);
    let r = (|| {
        let fail = |e: ArchiverError| Failure::new(e, false);
        s3_soft_validate_uri(uri).map_err(|m| fail(ArchiverError::Other(m)))?;
        validate_visibility_request(request).map_err(fail)?;
        let encoded = encode(request).map_err(fail)?;
        let path = s3_key_path(uri);
        let indexes = [
            (S3_PRIMARY_TYPE, &request.workflow_type_name, S3_INDEX_CLOSE, request.close_timestamp),
            (S3_PRIMARY_TYPE, &request.workflow_type_name, S3_INDEX_START, request.start_timestamp),
            (S3_PRIMARY_ID, &request.workflow_id, S3_INDEX_CLOSE, request.close_timestamp),
            (S3_PRIMARY_ID, &request.workflow_id, S3_INDEX_START, request.start_timestamp),
        ];
        for (primary, value, secondary, ts) in indexes {
            // Go constructTimestampIndex.
            let key = format!(
                "{}/{}/{}",
                s3_visibility_prefix(&path, &request.domain_id, primary, value, secondary),
                rfc3339_utc(ts),
                request.run_id
            );
            let bucket = s3_bucket(uri, region).map_err(|m| fail(ArchiverError::Other(m)))?;
            s3_upload(store, &bucket, &key, &encoded)?;
        }
        scoped.inc(metric::VISIBILITY_ARCHIVE_SUCCESS);
        Ok(())
    })();
    scoped.finish(
        r,
        metric::VISIBILITY_ARCHIVE_TRANSIENT_ERROR,
        metric::VISIBILITY_ARCHIVE_NON_RETRYABLE_ERROR,
    )
}

// --------------------------------------------------------------------- GCS

/// Go gcloud `constructVisibilityFilename`.
fn gcs_visibility_filename(req: &ArchiveVisibilityRequest, tag: &str, ts: i64) -> String {
    format!(
        "{}/{tag}_{}_{}_{}_{}.visibility",
        req.domain_id,
        rfc3339_utc(ts),
        gcs_hash(&req.workflow_type_name),
        gcs_hash(&req.workflow_id),
        gcs_hash(&req.run_id)
    )
}

fn gcs_upload(
    store: &dyn ObjectStore,
    uri: &ArchiverUri,
    file: &str,
    data: &[u8],
) -> Result<(), StoreError> {
    store.put_object(&uri.host, &gcs_object(uri, file), data)
}

/// Go gcloud `isRetryableError`: everything but bucket-not-found / scheme mismatch / invalid URI.
fn gcs_is_retryable(e: &ArchiverError) -> bool {
    !matches!(
        e.message(),
        GCS_ERR_BUCKET_NOT_EXIST | ERR_URI_SCHEME_MISMATCH | ERR_INVALID_URI
    )
}

/// Go gcloud `historyArchiver.Archive`. Like Go, `Exist` checks the bare file name (no sink
/// path), so it effectively never skips; the part number is only advanced by the progress
/// manager (`CurrentPageNumber`), so without one every blob is written to part 0.
pub fn gcs_archive_history(
    store: &dyn ObjectStore,
    uri: &ArchiverUri,
    request: &ArchiveHistoryRequest,
    source: &dyn HistoryBatchSource,
    opts: &ArchiveOptions,
) -> Result<(), ArchiverError> {
    let scoped = Scoped::new(opts, HISTORY_ARCHIVER_SCOPE, &request.domain_name);
    let r = gcs_archive_history_inner(&scoped, store, uri, request, source);
    scoped.finish(
        r,
        metric::HISTORY_ARCHIVE_TRANSIENT_ERROR,
        metric::HISTORY_ARCHIVE_NON_RETRYABLE_ERROR,
    )
}

fn gcs_archive_history_inner(
    scoped: &Scoped<'_>,
    store: &dyn ObjectStore,
    uri: &ArchiverUri,
    request: &ArchiveHistoryRequest,
    source: &dyn HistoryBatchSource,
) -> Result<(), Failure> {
    let (opts, scope, domain) = (scoped.opts, scoped.scope, scoped.domain);
    let non_retriable =
        || Failure::new(ArchiverError::Other(GCS_ERR_UPLOAD_NON_RETRIABLE.to_owned()), false);
    gcs_validate_uri(store, uri).map_err(|_| non_retriable())?;
    validate_history_request(request).map_err(|_| non_retriable())?;
    let mut progress = ArchiveProgress::default();
    let mut it = load_iterator(request, source, opts, &mut progress);
    let mut total_upload_size = 0_i64;
    while it.has_next() {
        let part = progress.batch_idx;
        let blob = match it.next_blob() {
            Ok(b) => b,
            Err(ArchiverError::EntityNotExists(_)) => {
                scoped.inc(metric::HISTORY_DUPLICATE_ARCHIVALS);
                return Ok(());
            }
            // Go: transient (internal) read errors are returned, others are errUploadNonRetriable.
            Err(e @ ArchiverError::Internal(_)) => return Err(Failure::new(e, true)),
            Err(_) => return Err(non_retriable()),
        };
        if blob_mutated(request, &blob) && !source.archive_incomplete_history() {
            // Go returns ErrHistoryMutated as is (not errUploadNonRetriable): counted as transient.
            return Err(Failure::new(ArchiverError::Other(ERR_HISTORY_MUTATED.into()), true));
        }
        let encoded = encode(&blob["body"]).map_err(|_| non_retriable())?; // Go encodes Body only
        let filename = gcs_history_filename(
            &request.domain_id,
            &request.workflow_id,
            &request.run_id,
            request.close_failover_version,
            part as i64,
        );
        // Go: `exist, _ := Exist(ctx, URI, filename)`; errors count as not existing.
        let exist = store.head_object(&uri.host, &filename).unwrap_or(false);
        if !exist {
            if let Err(e) = gcs_upload(store, uri, &filename, &encoded) {
                // Go counts the transient error here and again in the deferred block.
                scoped.inc(metric::HISTORY_ARCHIVE_TRANSIENT_ERROR);
                return Err(Failure::new(ArchiverError::Other(e.message), true));
            }
            total_upload_size += encoded.len() as i64;
        }
        // Go saveHistoryIteratorState: CurrentPageNumber = part + 1 (only with a ProgressManager).
        if opts.progress_manager.is_some() {
            progress.batch_idx = part + 1;
        }
        save_state(opts, &it, &mut progress);
    }
    let m = &opts.metrics;
    m.add_counter(scope, domain, metric::HISTORY_TOTAL_UPLOAD_SIZE, total_upload_size);
    m.add_counter(scope, domain, metric::HISTORY_SIZE, total_upload_size);
    m.int_histogram(scope, domain, metric::HISTORY_TOTAL_UPLOAD_SIZE_HISTOGRAM, total_upload_size);
    m.int_histogram(scope, domain, metric::HISTORY_SIZE_HISTOGRAM, total_upload_size);
    scoped.inc(metric::HISTORY_ARCHIVE_SUCCESS);
    Ok(())
}

/// Go gcloud `visibilityArchiver.Archive`: close-time record then start-time record. Go emits
/// these metrics on `HistoryArchiverScope` (kept as is).
pub fn gcs_archive_visibility(
    store: &dyn ObjectStore,
    uri: &ArchiverUri,
    request: &ArchiveVisibilityRequest,
    opts: &ArchiveOptions,
) -> Result<(), ArchiverError> {
    let scoped = Scoped::new(opts, HISTORY_ARCHIVER_SCOPE, &request.domain_name);
    let r = (|| {
        let fail = |e: ArchiverError| {
            let retryable = gcs_is_retryable(&e);
            Failure::new(e, retryable)
        };
        gcs_validate_uri(store, uri).map_err(fail)?;
        validate_visibility_request(request).map_err(fail)?;
        let encoded = encode(request).map_err(fail)?;
        for (tag, ts) in [
            (GCS_INDEX_CLOSE, request.close_timestamp),
            (GCS_INDEX_START, request.start_timestamp),
        ] {
            let filename = gcs_visibility_filename(request, tag, ts);
            gcs_upload(store, uri, &filename, &encoded)
                .map_err(|_| fail(ArchiverError::Other(GCS_ERR_RETRIABLE.to_owned())))?;
        }
        scoped.inc(metric::VISIBILITY_ARCHIVE_SUCCESS);
        Ok(())
    })();
    scoped.finish(
        r,
        metric::VISIBILITY_ARCHIVE_TRANSIENT_ERROR,
        metric::VISIBILITY_ARCHIVE_NON_RETRYABLE_ERROR,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::archiver::parse_uri;
    use crate::cloud_archiver::{ListPage, GCS_ERR_BUCKET_NOT_EXIST};
    use std::collections::BTreeMap;
    use std::sync::Mutex;

    #[derive(Default)]
    struct Mock {
        bucket: String,
        objects: Mutex<BTreeMap<String, Vec<u8>>>,
        fail_put: bool,
    }

    impl ObjectStore for Mock {
        fn head_bucket(&self, bucket: &str) -> Result<(), StoreError> {
            if bucket == self.bucket {
                Ok(())
            } else {
                Err(StoreError::new(StoreErrorKind::NotFound, "NotFound"))
            }
        }
        fn get_object(&self, _b: &str, key: &str) -> Result<Vec<u8>, StoreError> {
            self.objects
                .lock()
                .unwrap()
                .get(key)
                .cloned()
                .ok_or_else(|| StoreError::new(StoreErrorKind::NoSuchKey, "no key"))
        }
        fn put_object(&self, bucket: &str, key: &str, data: &[u8]) -> Result<(), StoreError> {
            if self.fail_put {
                return Err(StoreError::new(StoreErrorKind::Other, "put failed"));
            }
            if bucket != self.bucket {
                return Err(StoreError::new(StoreErrorKind::NoSuchBucket, "no bucket"));
            }
            self.objects.lock().unwrap().insert(key.to_owned(), data.to_vec());
            Ok(())
        }
        fn head_object(&self, _b: &str, key: &str) -> Result<bool, StoreError> {
            Ok(self.objects.lock().unwrap().contains_key(key))
        }
        fn list(
            &self,
            _b: &str,
            prefix: &str,
            _d: Option<&str>,
            _m: Option<usize>,
            _t: Option<&str>,
        ) -> Result<ListPage, StoreError> {
            Ok(ListPage {
                keys: self
                    .objects
                    .lock()
                    .unwrap()
                    .keys()
                    .filter(|k| k.starts_with(prefix))
                    .cloned()
                    .collect(),
                ..Default::default()
            })
        }
    }

    struct Src(Option<Vec<Value>>);
    impl HistoryBatchSource for Src {
        fn read_all(&self, _r: &ArchiveHistoryRequest) -> Result<Option<Vec<Value>>, ArchiverError> {
            Ok(self.0.clone())
        }
    }

    fn hist_req() -> ArchiveHistoryRequest {
        ArchiveHistoryRequest {
            domain_id: "d".into(),
            domain_name: "dn".into(),
            workflow_id: "w".into(),
            run_id: "r".into(),
            branch_token: vec![1],
            next_event_id: 4,
            close_failover_version: 7,
        }
    }

    fn vis_req() -> ArchiveVisibilityRequest {
        ArchiveVisibilityRequest {
            domain_id: "d".into(),
            domain_name: "dn".into(),
            workflow_id: "w".into(),
            run_id: "r".into(),
            workflow_type_name: "t".into(),
            start_timestamp: 1_580_819_141_000_000_000,
            close_timestamp: 1_580_819_200_000_000_000,
            ..Default::default()
        }
    }

    fn batches() -> Vec<Value> {
        vec![
            json!({"events":[{"eventId":1,"version":7},{"eventId":2,"version":7}]}),
            json!({"events":[{"eventId":3,"version":7}]}),
        ]
    }

    #[derive(Default)]
    struct Rec(Mutex<Vec<String>>);
    impl ArchiverMetrics for Rec {
        fn inc_counter(&self, scope: &str, domain: &str, name: &str) {
            self.0.lock().unwrap().push(format!("inc {scope}/{domain}/{name}"));
        }
        fn add_counter(&self, _: &str, _: &str, name: &str, d: i64) {
            self.0.lock().unwrap().push(format!("add {name}={d}"));
        }
        fn record_timer(&self, _: &str, _: &str, name: &str, v: i64) {
            self.0.lock().unwrap().push(format!("timer {name}={v}"));
        }
        fn int_histogram(&self, _: &str, _: &str, name: &str, v: i64) {
            self.0.lock().unwrap().push(format!("hist {name}={v}"));
        }
    }

    fn opts_with(rec: &Arc<Rec>) -> ArchiveOptions {
        ArchiveOptions { metrics: rec.clone(), ..Default::default() }
    }

    #[test]
    fn iterator_closes_blob_at_target_and_tracks_state() {
        let req = hist_req();
        let one = estimate_size(&batches()[0]);
        let src = Src(Some(batches()));
        let mut it = HistoryIterator::new(&req, &src, usize::MAX);
        let blob = it.next_blob().unwrap();
        assert_eq!(blob["header"]["is_last"], json!(true));
        assert!(!it.has_next());
        let mut it = HistoryIterator::new(&req, &src, one);
        let b1 = it.next_blob().unwrap();
        assert_eq!(b1["header"]["is_last"], json!(false));
        assert_eq!(b1["header"]["last_event_id"], json!(2));
        assert_eq!(it.state(), br#"{"NextEventID":3,"FinishedIteration":false}"#.to_vec());
        // resume from state
        let mut it2 = HistoryIterator::from_state(&req, &src, one, &it.state()).unwrap();
        let b2 = it2.next_blob().unwrap();
        assert_eq!(b2["header"]["first_event_id"], json!(3));
        assert_eq!(b2["header"]["is_last"], json!(true));
        assert!(!it2.has_next());
        assert!(HistoryIterator::from_state(&req, &src, one, b"x").is_err());
    }

    #[test]
    fn s3_multi_blob_uses_batch_idx_and_emits_metrics() {
        let store = Mock { bucket: "b".into(), ..Default::default() };
        let uri = parse_uri("s3://b/p").unwrap();
        let rec = Arc::new(Rec::default());
        // events of two batches never fit one blob when the target is tiny: use big batches.
        let big: Vec<Value> = (0..3)
            .map(|i| json!({"events":[{"eventId": i + 1, "version": 7, "pad": "x".repeat(TARGET_HISTORY_BLOB_SIZE)}]}))
            .collect();
        let mut r = hist_req();
        r.next_event_id = 4;
        s3_archive_history(&store, "r", &uri, &r, &Src(Some(big)), &opts_with(&rec)).unwrap();
        assert_eq!(store.objects.lock().unwrap().len(), 3);
        for idx in 0..3 {
            assert!(store.objects.lock().unwrap().contains_key(&s3_history_key("/p", "d", "w", "r", 7, idx)));
        }
        let log = rec.0.lock().unwrap().clone();
        assert!(log.contains(&"inc HistoryArchiver/dn/history_archiver_archive_success".to_owned()), "{log:?}");
        assert_eq!(log.iter().filter(|l| l.starts_with("timer history_archiver_blob_size=")).count(), 3);
        assert!(log.iter().any(|l| l.starts_with("timer history_archiver_total_upload_size=")));
        assert!(log.iter().any(|l| l.starts_with("timer cadence_latency=")));
    }

    #[test]
    fn s3_blob_exists_counter_and_nonretriable_substitution() {
        let store = Mock { bucket: "b".into(), ..Default::default() };
        let uri = parse_uri("s3://b/p").unwrap();
        store.objects.lock().unwrap().insert(s3_history_key("/p", "d", "w", "r", 7, 0), b"old".to_vec());
        let rec = Arc::new(Rec::default());
        s3_archive_history(&store, "r", &uri, &hist_req(), &Src(Some(batches())), &opts_with(&rec)).unwrap();
        assert!(rec.0.lock().unwrap().contains(&"inc HistoryArchiver/dn/history_archiver_blob_exists".to_owned()));
        // duplicate archival: history gone
        let rec = Arc::new(Rec::default());
        s3_archive_history(&store, "r", &uri, &hist_req(), &Src(None), &opts_with(&rec)).unwrap();
        assert!(rec.0.lock().unwrap().contains(&"inc HistoryArchiver/dn/history_archiver_duplicate_archivals".to_owned()));
        // non-retryable error is substituted and counted
        let rec = Arc::new(Rec::default());
        let mut opts = opts_with(&rec);
        opts.non_retriable_error = Some(Arc::new(|| ArchiverError::Other("sub".into())));
        let mut bad = hist_req();
        bad.run_id.clear();
        let e = s3_archive_history(&store, "r", &uri, &bad, &Src(None), &opts).unwrap_err();
        assert_eq!(e.message(), "sub");
        assert!(rec.0.lock().unwrap().contains(&"inc HistoryArchiver/dn/history_archiver_archive_non_retryable_error".to_owned()));
        // retryable (internal) error: transient counter, not substituted
        struct Failing;
        impl HistoryBatchSource for Failing {
            fn read_all(&self, _: &ArchiveHistoryRequest) -> Result<Option<Vec<Value>>, ArchiverError> {
                Err(ArchiverError::Internal("db".into()))
            }
        }
        let rec = Arc::new(Rec::default());
        let mut opts = opts_with(&rec);
        opts.non_retriable_error = Some(Arc::new(|| ArchiverError::Other("sub".into())));
        let e = s3_archive_history(&store, "r", &uri, &hist_req(), &Failing, &opts).unwrap_err();
        assert_eq!(e, ArchiverError::Internal("db".into()));
        assert!(rec.0.lock().unwrap().contains(&"inc HistoryArchiver/dn/history_archiver_archive_transient_error".to_owned()));
        // visibility metrics
        let rec = Arc::new(Rec::default());
        s3_archive_visibility(&store, "r", &uri, &vis_req(), &opts_with(&rec)).unwrap();
        assert!(rec.0.lock().unwrap().contains(&"inc VisibilityArchiver/dn/visibility_archiver_archive_success".to_owned()));
    }

    #[test]
    fn gcs_metrics_substitution_and_progress_parts() {
        struct Pm(Mutex<Vec<ArchiveProgress>>);
        impl ProgressManager for Pm {
            fn has_progress(&self) -> bool {
                false
            }
            fn load_progress(&self) -> Result<ArchiveProgress, String> {
                Err("none".into())
            }
            fn record_progress(&self, p: &ArchiveProgress) -> Result<(), String> {
                self.0.lock().unwrap().push(p.clone());
                Ok(())
            }
        }
        let store = Mock { bucket: "bkt".into(), ..Default::default() };
        let uri = parse_uri("gs://bkt/p").unwrap();
        let big: Vec<Value> = (0..2)
            .map(|i| json!({"events":[{"eventId": i + 1, "version": 7, "pad": "x".repeat(TARGET_HISTORY_BLOB_SIZE)}]}))
            .collect();
        let mut r = hist_req();
        r.next_event_id = 3;
        // Without a progress manager every blob goes to part 0 (Go quirk): one object.
        let rec = Arc::new(Rec::default());
        gcs_archive_history(&store, &uri, &r, &Src(Some(big.clone())), &opts_with(&rec)).unwrap();
        assert_eq!(store.objects.lock().unwrap().len(), 1);
        let log = rec.0.lock().unwrap().clone();
        assert!(log.iter().any(|l| l.starts_with("add history_archiver_total_upload_size=")), "{log:?}");
        assert!(log.contains(&"inc HistoryArchiver/dn/history_archiver_archive_success".to_owned()));
        // With a progress manager the part number advances.
        let store2 = Mock { bucket: "bkt".into(), ..Default::default() };
        let pm = Arc::new(Pm(Mutex::default()));
        let opts = ArchiveOptions { progress_manager: Some(pm.clone()), ..Default::default() };
        gcs_archive_history(&store2, &uri, &r, &Src(Some(big)), &opts).unwrap();
        assert_eq!(store2.objects.lock().unwrap().len(), 2);
        let saved = pm.0.lock().unwrap().clone();
        assert_eq!(saved.iter().map(|p| p.batch_idx).collect::<Vec<_>>(), [1, 2]);
        // errUploadNonRetriable is non-retryable (substituted); other errors are transient.
        let rec = Arc::new(Rec::default());
        let mut opts = opts_with(&rec);
        opts.non_retriable_error = Some(Arc::new(|| ArchiverError::Other("sub".into())));
        let missing = parse_uri("gs://nope/p").unwrap();
        let e = gcs_archive_history(&store, &missing, &hist_req(), &Src(None), &opts).unwrap_err();
        assert_eq!(e.message(), "sub");
        assert!(rec.0.lock().unwrap().contains(&"inc HistoryArchiver/dn/history_archiver_archive_non_retryable_error".to_owned()));
        let failing = Mock { bucket: "bkt".into(), fail_put: true, ..Default::default() };
        let rec = Arc::new(Rec::default());
        let e = gcs_archive_history(&failing, &uri, &hist_req(), &Src(Some(batches())), &opts_with(&rec)).unwrap_err();
        assert_eq!(e.message(), "put failed");
        let n = rec.0.lock().unwrap().iter().filter(|l| l.ends_with("history_archiver_archive_transient_error")).count();
        assert_eq!(n, 2, "Go counts the transient upload error twice");
        // visibility: bucket-not-found is non-retryable -> substituted; metrics on the history scope
        let rec = Arc::new(Rec::default());
        let mut opts = opts_with(&rec);
        opts.non_retriable_error = Some(Arc::new(|| ArchiverError::Other("sub".into())));
        let e = gcs_archive_visibility(&store, &missing, &vis_req(), &opts).unwrap_err();
        assert_eq!(e.message(), "sub");
        assert!(rec.0.lock().unwrap().contains(&"inc HistoryArchiver/dn/visibility_archiver_archive_non_retryable_error".to_owned()));
        let rec = Arc::new(Rec::default());
        gcs_archive_visibility(&store, &uri, &vis_req(), &opts_with(&rec)).unwrap();
        assert!(rec.0.lock().unwrap().contains(&"inc HistoryArchiver/dn/visibility_archiver_archive_success".to_owned()));
    }

    #[test]
    fn s3_history_archive_writes_blob_and_roundtrips() {
        let store = Mock { bucket: "b".into(), ..Default::default() };
        let uri = parse_uri("s3://b/prefix").unwrap();
        s3_archive_history(&store, "us-east-1", &uri, &hist_req(), &Src(Some(batches())), &ArchiveOptions::default()).unwrap();
        let key = s3_history_key("/prefix", "d", "w", "r", 7, 0);
        let blob: Value = serde_json::from_slice(&store.objects.lock().unwrap()[&key]).unwrap();
        assert_eq!(blob["header"]["is_last"], json!(true));
        assert_eq!(blob["header"]["event_count"], json!(3));
        assert_eq!(blob["header"]["first_event_id"], json!(1));
        assert_eq!(blob["header"]["last_event_id"], json!(3));
        assert_eq!(blob["header"]["first_failover_version"], json!(7));
        assert_eq!(blob["body"], json!(batches()));
        let got = crate::cloud_archiver::s3_get_history(
            &store,
            "us-east-1",
            &uri,
            &crate::archiver::GetHistoryRequest {
                domain_id: "d".into(),
                workflow_id: "w".into(),
                run_id: "r".into(),
                close_failover_version: Some(7),
                next_page_token: vec![],
                page_size: 10,
            },
        )
        .unwrap();
        assert_eq!(got.history_batches, batches());
    }

    #[test]
    fn s3_history_existing_key_is_not_overwritten_and_gone_history_skips() {
        let store = Mock { bucket: "b".into(), ..Default::default() };
        let uri = parse_uri("s3://b/prefix").unwrap();
        let key = s3_history_key("/prefix", "d", "w", "r", 7, 0);
        store.objects.lock().unwrap().insert(key.clone(), b"old".to_vec());
        s3_archive_history(&store, "r", &uri, &hist_req(), &Src(Some(batches())), &ArchiveOptions::default()).unwrap();
        assert_eq!(store.objects.lock().unwrap()[&key], b"old".to_vec());
        let empty = Mock { bucket: "b".into(), ..Default::default() };
        s3_archive_history(&empty, "r", &uri, &hist_req(), &Src(None), &ArchiveOptions::default()).unwrap();
        assert!(empty.objects.lock().unwrap().is_empty());
    }

    #[test]
    fn s3_history_errors_like_go() {
        let store = Mock { bucket: "b".into(), ..Default::default() };
        let bad = parse_uri("file:///tmp/x").unwrap();
        assert!(s3_archive_history(&store, "r", &bad, &hist_req(), &Src(None), &ArchiveOptions::default()).is_err());
        let uri = parse_uri("s3://b/p").unwrap();
        let mut r = hist_req();
        r.domain_id.clear();
        assert!(s3_archive_history(&store, "r", &uri, &r, &Src(None), &ArchiveOptions::default()).is_err());
        let missing = parse_uri("s3://other/p").unwrap();
        let e = s3_archive_history(&store, "r", &missing, &hist_req(), &Src(Some(batches())), &ArchiveOptions::default())
            .unwrap_err();
        assert_eq!(e, ArchiverError::BadRequest(ERR_BUCKET_NOT_EXISTS.into()));
        let failing = Mock { bucket: "b".into(), fail_put: true, ..Default::default() };
        let e = s3_archive_history(&failing, "r", &uri, &hist_req(), &Src(Some(batches())), &ArchiveOptions::default())
            .unwrap_err();
        assert_eq!(e.message(), "put failed");
    }

    #[test]
    fn s3_visibility_archive_writes_four_indexes() {
        let store = Mock { bucket: "b".into(), ..Default::default() };
        let uri = parse_uri("s3://b/prefix").unwrap();
        s3_archive_visibility(&store, "r", &uri, &vis_req(), &ArchiveOptions::default()).unwrap();
        let keys: Vec<String> = store.objects.lock().unwrap().keys().cloned().collect();
        assert_eq!(
            keys,
            vec![
                "prefix/d/visibility/workflowID/w/closeTimeout/2020-02-04T12:26:40Z/r",
                "prefix/d/visibility/workflowID/w/startTimeout/2020-02-04T12:25:41Z/r",
                "prefix/d/visibility/workflowTypeName/t/closeTimeout/2020-02-04T12:26:40Z/r",
                "prefix/d/visibility/workflowTypeName/t/startTimeout/2020-02-04T12:25:41Z/r",
            ]
        );
        let mut bad = vis_req();
        bad.close_timestamp = 0;
        assert!(s3_archive_visibility(&store, "r", &uri, &bad, &ArchiveOptions::default()).is_err());
    }

    #[test]
    fn gcs_filename_matches_go_util_test() {
        let req = ArchiveVisibilityRequest {
            domain_id: "domainID".into(),
            workflow_type_name: "workflowTypeName".into(),
            workflow_id: "workflowID".into(),
            run_id: "runID".into(),
            ..Default::default()
        };
        assert_eq!(
            gcs_visibility_filename(&req, "startTimeout", 1_472_313_624_305),
            "domainID/startTimeout_1970-01-01T00:24:32Z_4346151385925082125_8344541402884576509_131521284625246243.visibility"
        );
    }

    #[test]
    fn gcs_history_and_visibility_archive() {
        let store = Mock { bucket: "bkt".into(), ..Default::default() };
        let uri = parse_uri("gs://bkt/cadence_archival/dev").unwrap();
        gcs_archive_history(&store, &uri, &hist_req(), &Src(Some(batches())), &ArchiveOptions::default()).unwrap();
        gcs_archive_visibility(&store, &uri, &vis_req(), &ArchiveOptions::default()).unwrap();
        let keys: Vec<String> = store.objects.lock().unwrap().keys().cloned().collect();
        assert_eq!(keys.len(), 3);
        assert!(keys.iter().all(|k| k.starts_with("cadence_archival/dev/")));
        let got = crate::cloud_archiver::gcs_get_history(
            &store,
            &uri,
            &crate::archiver::GetHistoryRequest {
                domain_id: "d".into(),
                workflow_id: "w".into(),
                run_id: "r".into(),
                close_failover_version: Some(7),
                next_page_token: vec![],
                page_size: 10,
            },
        )
        .unwrap();
        assert_eq!(got.history_batches, batches());
    }

    #[test]
    fn gcs_errors_like_go() {
        let store = Mock { bucket: "bkt".into(), ..Default::default() };
        let missing = parse_uri("gs://nope/p").unwrap();
        let e = gcs_archive_history(&store, &missing, &hist_req(), &Src(None), &ArchiveOptions::default()).unwrap_err();
        assert_eq!(e.message(), GCS_ERR_UPLOAD_NON_RETRIABLE);
        let e = gcs_archive_visibility(&store, &missing, &vis_req(), &ArchiveOptions::default()).unwrap_err();
        assert_eq!(e.message(), GCS_ERR_BUCKET_NOT_EXIST);
        let uri = parse_uri("gs://bkt/p").unwrap();
        let failing = Mock { bucket: "bkt".into(), fail_put: true, ..Default::default() };
        let e = gcs_archive_visibility(&failing, &uri, &vis_req(), &ArchiveOptions::default()).unwrap_err();
        assert_eq!(e.message(), GCS_ERR_RETRIABLE);
        let e = gcs_archive_history(&failing, &uri, &hist_req(), &Src(Some(batches())), &ArchiveOptions::default()).unwrap_err();
        assert_eq!(e.message(), "put failed");
        let mut r = hist_req();
        r.run_id.clear();
        let e = gcs_archive_history(&store, &uri, &r, &Src(None), &ArchiveOptions::default()).unwrap_err();
        assert_eq!(e.message(), GCS_ERR_UPLOAD_NON_RETRIABLE);
    }

    /// Source that serves pages like persistence and records each requested first event id.
    struct Paged {
        all: Vec<Value>,
        calls: Mutex<Vec<i64>>,
    }
    impl HistoryBatchSource for Paged {
        fn read_all(&self, _r: &ArchiveHistoryRequest) -> Result<Option<Vec<Value>>, ArchiverError> {
            panic!("the iterator must page, not read everything up front");
        }
        fn read_page(&self, _r: &ArchiveHistoryRequest, first: i64) -> Result<Vec<Value>, ArchiverError> {
            self.calls.lock().unwrap().push(first);
            let page = crate::archiver::page_from(&self.all, first);
            if page.is_empty() {
                return Err(ArchiverError::EntityNotExists("gone".into()));
            }
            Ok(page)
        }
    }

    #[test]
    fn iterator_pages_through_source_like_go() {
        let req = hist_req();
        let big: Vec<Value> = (0..3)
            .map(|i| json!({"events":[{"eventId": i + 1, "version": 7, "pad": "x".repeat(1000)}]}))
            .collect();
        let src = Paged { all: big, calls: Mutex::default() };
        // target below one batch: each blob has one batch, lookahead read decides `finished`.
        let mut it = HistoryIterator::new(&req, &src, 10);
        let b1 = it.next_blob().unwrap();
        assert_eq!(b1["header"]["is_last"], json!(false));
        assert_eq!(it.state(), br#"{"NextEventID":2,"FinishedIteration":false}"#.to_vec());
        it.next_blob().unwrap();
        let b3 = it.next_blob().unwrap();
        assert_eq!(b3["header"]["is_last"], json!(true));
        assert!(!it.has_next());
        // Go: a finished iteration encodes NextEventID 0.
        assert_eq!(it.state(), br#"{"NextEventID":0,"FinishedIteration":true}"#.to_vec());
        // read(1) -> blob1 returns mid-page; read(2); read(3) + lookahead read(4).
        assert_eq!(*src.calls.lock().unwrap(), vec![1, 2, 3, 4]);
    }

    #[test]
    fn iterator_first_page_not_found_is_entity_not_exists() {
        let req = hist_req();
        let src = Paged { all: vec![], calls: Mutex::default() };
        let mut it = HistoryIterator::new(&req, &src, 10);
        assert!(matches!(it.next_blob(), Err(ArchiverError::EntityNotExists(_))));
    }

    #[test]
    fn size_estimate_is_go_json_encoding() {
        // Go omits zero eventId/version/taskId and HTML-escapes strings.
        let b = json!({"events":[{"eventId":1,"version":0,"taskId":0,"note":"a<b>&c"}]});
        let req = hist_req();
        let src = Src(Some(vec![b.clone()]));
        let mut it = HistoryIterator::new(&req, &src, usize::MAX);
        let blob = it.next_blob().unwrap();
        let body = &blob["body"][0]["events"][0];
        assert!(body.get("eventId").is_some() && body.get("version").is_none() && body.get("taskId").is_none());
        let go = br#"{"events":[{"note":"a\u003cb\u003e\u0026c"}]}"#;
        assert_eq!(go_json_bytes(&json!({"events":[{"note":"a<b>&c"}]})), go.to_vec());
        assert_eq!(estimate_size(&json!({"events":[{"note":"a<b>&c"}]})), go.len());
        // blob bytes are the Go-compatible encoding of the blob
        let enc = encode(&blob).unwrap();
        assert!(String::from_utf8(enc).unwrap().contains("a\\u003cb\\u003e\\u0026c"));
    }

    #[test]
    fn mutated_history_is_checked_per_blob() {
        let store = Mock { bucket: "b".into(), ..Default::default() };
        let uri = parse_uri("s3://b/p").unwrap();
        let mut r = hist_req();
        r.next_event_id = 99; // last event 3 + 1 != 99
        let e = s3_archive_history(&store, "r", &uri, &r, &Src(Some(batches())), &ArchiveOptions::default()).unwrap_err();
        assert_eq!(e.message(), ERR_HISTORY_MUTATED);
        assert!(store.objects.lock().unwrap().is_empty());
        // Source allowing incomplete history archives anyway.
        struct Lax(Vec<Value>);
        impl HistoryBatchSource for Lax {
            fn read_all(&self, _: &ArchiveHistoryRequest) -> Result<Option<Vec<Value>>, ArchiverError> {
                Ok(Some(self.0.clone()))
            }
            fn archive_incomplete_history(&self) -> bool {
                true
            }
        }
        s3_archive_history(&store, "r", &uri, &r, &Lax(batches()), &ArchiveOptions::default()).unwrap();
        assert_eq!(store.objects.lock().unwrap().len(), 1);
    }

    #[test]
    fn inline_style_options_pass_errors_through_without_progress() {
        // Go inline archival: no NonRetriableError (error unchanged) and no ProgressManager.
        let store = Mock { bucket: "b".into(), ..Default::default() };
        let uri = parse_uri("s3://b/p").unwrap();
        let mut bad = hist_req();
        bad.run_id.clear();
        let rec = Arc::new(Rec::default());
        let e = s3_archive_history(&store, "r", &uri, &bad, &Src(None), &opts_with(&rec)).unwrap_err();
        assert_eq!(e.message(), crate::archiver::ERR_INVALID_ARCHIVE_REQUEST);
    }
}
