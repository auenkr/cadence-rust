//! Read path of the Go `s3store` and `gcloud` (gstorage) archivers.
//!
//! Mirrors `go/common/archiver/s3store` (`util.go`, `historyArchiver.go` Get /
//! ValidateURI, `visibilityArchiver.go` Query, `queryParser.go`) and
//! `go/common/archiver/gcloud` (`util.go`, `historyArchiver.go`,
//! `visibilityArchiver.go`, `queryParser.go`, `connector/client.go`).
//! Object-store I/O is behind [`ObjectStore`]; the HTTP implementations
//! (S3 SigV4, GCS JSON API) live in `cadence-frontend`. Archive (write) paths
//! belong to the History service and are not ported here.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::archiver::{
    walk_where, ArchiverError, ArchiverUri, GetHistoryRequest, GetHistoryResponse,
    QueryVisibilityRequest, QueryVisibilityResponse, VisibilityRecord, ERR_HISTORY_NOT_EXIST,
    ERR_INVALID_GET_HISTORY_REQUEST, ERR_INVALID_QUERY_VISIBILITY_REQUEST, ERR_INVALID_URI,
    ERR_NEXT_PAGE_TOKEN_CORRUPTED, ERR_URI_SCHEME_MISMATCH,
};

pub const S3_ACCESS_POINT_SCHEME: &str = "s3-ap";

const ERR_NO_BUCKET_SPECIFIED: &str = "no bucket specified";
const ERR_BUCKET_NOT_EXISTS: &str = "requested bucket does not exist";
const ERR_EMPTY_AWS_REGION: &str = "empty aws region";
const ERR_INVALID_ACCESS_POINT_URI: &str =
    "invalid s3-ap URI: expected s3-ap://<account-id>/<access-point-name>[/<path>]";
const ERR_INVALID_ACCESS_POINT_ACCT: &str = "invalid s3-ap URI: account id must be 12 digits";
/// Go `storage.ErrBucketNotExist`.
pub const GCS_ERR_BUCKET_NOT_EXIST: &str = "storage: bucket doesn't exist";
const GCS_ERR_OBJECT_NOT_FOUND: &str = "object not found";

// ------------------------------------------------------------ object store

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreErrorKind {
    NoSuchBucket,
    NoSuchKey,
    /// Plain HTTP 404 without an S3 error code (HEAD requests).
    NotFound,
    /// Throttling / 5xx (Go `isRetryableError`).
    Retryable,
    Other,
}

#[derive(Debug, Clone)]
pub struct StoreError {
    pub kind: StoreErrorKind,
    pub message: String,
}

impl StoreError {
    pub fn new(kind: StoreErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }
}

impl std::fmt::Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

#[derive(Debug, Clone, Default)]
pub struct ListPage {
    pub keys: Vec<String>,
    pub common_prefixes: Vec<String>,
    pub next_token: Option<String>,
}

/// Minimal blob-store surface used by the archive readers.
pub trait ObjectStore: Send + Sync {
    fn head_bucket(&self, bucket: &str) -> Result<(), StoreError>;
    fn get_object(&self, bucket: &str, key: &str) -> Result<Vec<u8>, StoreError>;
    /// One page of a prefix listing (S3 ListObjectsV2 / GCS objects.list).
    fn list(
        &self,
        bucket: &str,
        prefix: &str,
        delimiter: Option<&str>,
        max_keys: Option<usize>,
        token: Option<&str>,
    ) -> Result<ListPage, StoreError>;

    /// Every key under the prefix, following continuation tokens.
    fn list_all(&self, bucket: &str, prefix: &str) -> Result<Vec<String>, StoreError> {
        let mut out = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let page = self.list(bucket, prefix, None, None, token.as_deref())?;
            out.extend(page.keys);
            match page.next_token {
                Some(t) if !t.is_empty() => token = Some(t),
                _ => return Ok(out),
            }
        }
    }
}

/// `archival.*.provider.s3store` (Go `s3store.Config`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct S3Config {
    pub region: String,
    pub endpoint: Option<String>,
    pub force_path_style: bool,
}

/// `archival.*.provider.gstorage` (Go `connector.Config`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GcsConfig {
    pub credentials_path: String,
}

/// Builds the concrete stores (kept out of this crate to avoid HTTP deps).
pub trait CloudStoreFactory: Send + Sync {
    fn s3(&self, config: &S3Config) -> Result<Arc<dyn ObjectStore>, String>;
    fn gcs(&self, config: &GcsConfig) -> Result<Arc<dyn ObjectStore>, String>;
}

/// Configured cloud archivers; construction errors surface on first use like
/// Go's lazily built archivers.
#[derive(Clone, Default)]
pub struct CloudBackends {
    pub s3_region: String,
    s3: Option<Result<Arc<dyn ObjectStore>, String>>,
    gcs: Option<Result<Arc<dyn ObjectStore>, String>>,
}

impl std::fmt::Debug for CloudBackends {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CloudBackends")
            .field("s3", &self.s3.is_some())
            .field("gcs", &self.gcs.is_some())
            .finish()
    }
}

impl CloudBackends {
    pub fn new(
        s3: Option<S3Config>,
        gcs: Option<GcsConfig>,
        factory: &dyn CloudStoreFactory,
    ) -> Self {
        Self {
            s3_region: s3.as_ref().map(|c| c.region.clone()).unwrap_or_default(),
            s3: s3.map(|c| factory.s3(&c)),
            gcs: gcs.map(|c| factory.gcs(&c)),
        }
    }

    pub fn s3_store(&self) -> Option<Result<Arc<dyn ObjectStore>, ArchiverError>> {
        self.s3
            .clone()
            .map(|r| r.map_err(ArchiverError::Other))
    }

    pub fn gcs_store(&self) -> Option<Result<Arc<dyn ObjectStore>, ArchiverError>> {
        self.gcs
            .clone()
            .map(|r| r.map_err(ArchiverError::Other))
    }
}

// ------------------------------------------------------------------ shared

fn nanos_to_string(ts: i64, format: &str) -> String {
    chrono::DateTime::from_timestamp_nanos(ts)
        .format(format)
        .to_string()
}

/// Go `constructTimeBasedSearchKey` time layout for the search precision.
fn precision_time(ts: i64, precision: &str) -> String {
    let format = match precision {
        "Second" => "%Y-%m-%dT%H:%M:%S",
        "Minute" => "%Y-%m-%dT%H:%M",
        "Hour" => "%Y-%m-%dT%H",
        "Day" => "%Y-%m-%dT",
        _ => "",
    };
    nanos_to_string(ts, format)
}

fn extract_string(val: &str, quoted: bool) -> Result<String, String> {
    if quoted {
        Ok(val.to_owned())
    } else {
        Err(format!("value {val} is not a string value"))
    }
}

/// Go `convertToTimestamp` of the s3store / gcloud query parsers.
fn convert_timestamp(val: &str, quoted: bool) -> Result<i64, String> {
    if !quoted {
        return val
            .parse::<i64>()
            .map_err(|_| format!("value {val} is not a string value"));
    }
    chrono::DateTime::parse_from_rfc3339(val)
        .ok()
        .and_then(|t| t.timestamp_nanos_opt())
        .ok_or_else(|| {
            format!("parsing time \"{val}\" as \"2006-01-02T15:04:05Z07:00\": cannot parse \"{val}\" as \"2006\"")
        })
}

fn only_eq(op: &str, name: &str, backend: &str) -> Result<(), String> {
    if op == "=" {
        Ok(())
    } else {
        Err(format!(
            "only operator = is supported for {name} with {backend}"
        ))
    }
}

fn set_precision(
    slot: &mut Option<String>,
    val: String,
) -> Result<(), String> {
    if slot.as_ref().is_some_and(|c| *c != val) {
        return Err("only one expression is allowed for SearchPrecision".to_owned());
    }
    match val.as_str() {
        "Day" | "Hour" | "Minute" | "Second" => {}
        _ => return Err(format!("invalid value for SearchPrecision: {val}")),
    }
    *slot = Some(val);
    Ok(())
}

fn validate_get_request(r: &GetHistoryRequest) -> bool {
    !(r.domain_id.is_empty()
        || r.workflow_id.is_empty()
        || r.run_id.is_empty()
        || r.page_size == 0)
}

fn validate_query_request(r: &QueryVisibilityRequest) -> bool {
    !(r.domain_id.is_empty() || r.page_size == 0 || r.query.is_empty())
}

fn decode_record(data: &[u8]) -> Result<VisibilityRecord, ArchiverError> {
    serde_json::from_slice(data).map_err(|e| ArchiverError::Internal(e.to_string()))
}

fn serialize_token<T: Serialize>(t: &T) -> Result<Vec<u8>, ArchiverError> {
    serde_json::to_vec(t).map_err(|e| ArchiverError::Internal(e.to_string()))
}

// ---------------------------------------------------------------------- S3

fn split_access_point_path(p: &str) -> Option<(String, String)> {
    let p = p.strip_prefix('/').unwrap_or(p);
    if p.is_empty() {
        return None;
    }
    match p.find('/') {
        Some(idx) => {
            let name = &p[..idx];
            if name.is_empty() {
                None
            } else {
                Some((name.to_owned(), p[idx..].to_owned()))
            }
        }
        None => Some((p.to_owned(), String::new())),
    }
}

/// Go s3store `softValidateURI`.
pub fn s3_soft_validate_uri(uri: &ArchiverUri) -> Result<(), String> {
    match uri.scheme.as_str() {
        "s3" => {
            if uri.host.is_empty() {
                Err(ERR_NO_BUCKET_SPECIFIED.to_owned())
            } else {
                Ok(())
            }
        }
        S3_ACCESS_POINT_SCHEME => {
            if uri.host.len() != 12 || !uri.host.chars().all(|c| c.is_ascii_digit()) {
                return Err(ERR_INVALID_ACCESS_POINT_ACCT.to_owned());
            }
            if split_access_point_path(&uri.path).is_none() {
                return Err(ERR_INVALID_ACCESS_POINT_URI.to_owned());
            }
            Ok(())
        }
        _ => Err(ERR_URI_SCHEME_MISMATCH.to_owned()),
    }
}

fn partition_for_region(region: &str) -> &'static str {
    if region.starts_with("cn-") {
        "aws-cn"
    } else if region.starts_with("us-gov-") {
        "aws-us-gov"
    } else if region.starts_with("us-iso-") {
        "aws-iso"
    } else if region.starts_with("us-isob-") {
        "aws-iso-b"
    } else {
        "aws"
    }
}

/// Go `s3Bucket`: bucket name, or the access point ARN for `s3-ap`.
fn s3_bucket(uri: &ArchiverUri, region: &str) -> Result<String, String> {
    if uri.scheme == S3_ACCESS_POINT_SCHEME {
        let (name, _) = split_access_point_path(&uri.path)
            .ok_or_else(|| ERR_INVALID_ACCESS_POINT_URI.to_owned())?;
        if region.is_empty() {
            return Err(ERR_EMPTY_AWS_REGION.to_owned());
        }
        return Ok(format!(
            "arn:{}:s3:{region}:{}:accesspoint/{name}",
            partition_for_region(region),
            uri.host
        ));
    }
    Ok(uri.host.clone())
}

/// Go `s3KeyPath`.
fn s3_key_path(uri: &ArchiverUri) -> String {
    if uri.scheme == S3_ACCESS_POINT_SCHEME {
        return split_access_point_path(&uri.path)
            .map(|(_, p)| p)
            .unwrap_or_default();
    }
    uri.path.clone()
}

fn trim_left_slash(s: String) -> String {
    s.trim_start_matches('/').to_owned()
}

/// Go `constructHistoryKeyPrefix`.
pub fn s3_history_key_prefix(path: &str, domain: &str, workflow: &str, run: &str) -> String {
    trim_left_slash([path, domain, "history", workflow, run].join("/"))
}

/// Go `constructHistoryKey`.
pub fn s3_history_key(
    path: &str,
    domain: &str,
    workflow: &str,
    run: &str,
    version: i64,
    batch_idx: i64,
) -> String {
    format!(
        "{}/{version}/{batch_idx}",
        s3_history_key_prefix(path, domain, workflow, run)
    )
}

/// Go `constructVisibilitySearchPrefix`.
pub fn s3_visibility_prefix(
    path: &str,
    domain: &str,
    primary_key: &str,
    primary_value: &str,
    secondary: &str,
) -> String {
    trim_left_slash(
        [path, domain, "visibility", primary_key, primary_value, secondary].join("/"),
    )
}

/// Go `visibilityArchiver.ValidateURI` / `historyArchiver.ValidateURI`.
pub fn s3_validate_uri(
    store: &dyn ObjectStore,
    region: &str,
    uri: &ArchiverUri,
) -> Result<(), ArchiverError> {
    s3_soft_validate_uri(uri).map_err(ArchiverError::Other)?;
    let bucket = s3_bucket(uri, region).map_err(ArchiverError::Other)?;
    match store.head_bucket(&bucket) {
        Ok(()) => Ok(()),
        Err(e) if matches!(e.kind, StoreErrorKind::NotFound | StoreErrorKind::NoSuchBucket) => {
            Err(ArchiverError::Other(ERR_BUCKET_NOT_EXISTS.to_owned()))
        }
        Err(e) => Err(ArchiverError::Other(e.message)),
    }
}

fn s3_download(store: &dyn ObjectStore, bucket: &str, key: &str) -> Result<Vec<u8>, ArchiverError> {
    store.get_object(bucket, key).map_err(|e| match e.kind {
        StoreErrorKind::NoSuchBucket => ArchiverError::BadRequest(ERR_BUCKET_NOT_EXISTS.to_owned()),
        StoreErrorKind::NoSuchKey => ArchiverError::EntityNotExists(ERR_HISTORY_NOT_EXIST.to_owned()),
        _ => ArchiverError::Internal(e.message),
    })
}

#[derive(Debug, Serialize, Deserialize)]
struct S3HistoryToken {
    #[serde(rename = "CloseFailoverVersion")]
    close_failover_version: i64,
    #[serde(rename = "BatchIdx")]
    batch_idx: i64,
}

/// Go s3store `getHighestVersion`.
fn s3_highest_version(
    store: &dyn ObjectStore,
    bucket: &str,
    path: &str,
    request: &GetHistoryRequest,
) -> Result<i64, ArchiverError> {
    let prefix = format!(
        "{}/",
        s3_history_key_prefix(path, &request.domain_id, &request.workflow_id, &request.run_id)
    );
    let page = store
        .list(bucket, &prefix, Some("/"), None, None)
        .map_err(|e| match e.kind {
            StoreErrorKind::NoSuchBucket => ArchiverError::BadRequest(ERR_BUCKET_NOT_EXISTS.to_owned()),
            _ => ArchiverError::BadRequest(e.message),
        })?;
    let mut highest: Option<i64> = None;
    for p in &page.common_prefixes {
        let stripped = p.replacen(&prefix, "", 1).replacen('/', "", 1);
        if let Ok(version) = stripped.parse::<i64>() {
            if highest.is_none_or(|h| version > h) {
                highest = Some(version);
            }
        }
    }
    highest.ok_or_else(|| ArchiverError::BadRequest(ERR_HISTORY_NOT_EXIST.to_owned()))
}

/// Go s3store `historyArchiver.Get`.
pub fn s3_get_history(
    store: &dyn ObjectStore,
    region: &str,
    uri: &ArchiverUri,
    request: &GetHistoryRequest,
) -> Result<GetHistoryResponse, ArchiverError> {
    if s3_soft_validate_uri(uri).is_err() {
        return Err(ArchiverError::BadRequest(ERR_INVALID_URI.to_owned()));
    }
    if !validate_get_request(request) {
        return Err(ArchiverError::BadRequest(
            ERR_INVALID_GET_HISTORY_REQUEST.to_owned(),
        ));
    }
    let path = s3_key_path(uri);
    let bucket = s3_bucket(uri, region);
    let mut token = if !request.next_page_token.is_empty() {
        serde_json::from_slice::<S3HistoryToken>(&request.next_page_token)
            .map_err(|_| ArchiverError::BadRequest(ERR_NEXT_PAGE_TOKEN_CORRUPTED.to_owned()))?
    } else if let Some(version) = request.close_failover_version {
        S3HistoryToken {
            close_failover_version: version,
            batch_idx: 0,
        }
    } else {
        let bucket = bucket.clone().map_err(ArchiverError::BadRequest)?;
        S3HistoryToken {
            close_failover_version: s3_highest_version(store, &bucket, &path, request)?,
            batch_idx: 0,
        }
    };
    let bucket = bucket.map_err(ArchiverError::Internal)?;
    let mut response = GetHistoryResponse::default();
    let mut num_events = 0_i64;
    let mut truncated = false;
    loop {
        if num_events >= i64::from(request.page_size) {
            truncated = true;
            break;
        }
        let key = s3_history_key(
            &path,
            &request.domain_id,
            &request.workflow_id,
            &request.run_id,
            token.close_failover_version,
            token.batch_idx,
        );
        let data = s3_download(store, &bucket, &key)?;
        let blob: Value =
            serde_json::from_slice(&data).map_err(|e| ArchiverError::Internal(e.to_string()))?;
        if let Some(body) = blob.get("body").and_then(Value::as_array) {
            for batch in body {
                num_events += batch
                    .get("events")
                    .and_then(Value::as_array)
                    .map_or(0, |e| e.len() as i64);
                response.history_batches.push(batch.clone());
            }
        }
        let is_last = blob
            .get("header")
            .and_then(|h| h.get("is_last"))
            .and_then(Value::as_bool)
            .unwrap_or(false);
        if is_last {
            break;
        }
        token.batch_idx += 1;
    }
    if truncated {
        response.next_page_token = serialize_token(&token)?;
    }
    Ok(response)
}

#[derive(Debug, Default)]
struct S3Query {
    workflow_type_name: Option<String>,
    workflow_id: Option<String>,
    start_time: Option<i64>,
    close_time: Option<i64>,
    precision: Option<String>,
}

fn s3_parse_query(query: &str) -> Result<S3Query, String> {
    const BACKEND: &str = "Amazon S3";
    let mut q = S3Query::default();
    walk_where(query, |col, op, val, quoted| {
        match col {
            "WorkflowTypeName" | "WorkflowID" => {
                let v = extract_string(val, quoted)?;
                only_eq(op, col, BACKEND)?;
                let slot = if col == "WorkflowID" {
                    &mut q.workflow_id
                } else {
                    &mut q.workflow_type_name
                };
                if slot.is_some() {
                    return Err(format!("can not query {col} multiple times"));
                }
                *slot = Some(v);
            }
            "CloseTime" | "StartTime" => {
                let ts = convert_timestamp(val, quoted)?;
                only_eq(op, col, BACKEND)?;
                if col == "CloseTime" {
                    q.close_time = Some(ts);
                } else {
                    q.start_time = Some(ts);
                }
            }
            "SearchPrecision" => {
                let v = extract_string(val, quoted)?;
                only_eq(op, col, BACKEND)?;
                set_precision(&mut q.precision, v)?;
            }
            _ => return Err(format!("unknown filter name: {col}")),
        }
        Ok(())
    })?;
    if q.workflow_id.is_none() && q.workflow_type_name.is_none() {
        return Err("WorkflowID or WorkflowTypeName is required in query".to_owned());
    }
    if q.workflow_id.is_some() && q.workflow_type_name.is_some() {
        return Err("only one of WorkflowID or WorkflowTypeName can be specified in a query".to_owned());
    }
    if q.close_time.is_some() && q.start_time.is_some() {
        return Err("only one of StartTime or CloseTime can be specified in a query".to_owned());
    }
    if (q.close_time.is_some() || q.start_time.is_some()) && q.precision.is_none() {
        return Err("SearchPrecision is required when searching for a StartTime or CloseTime".to_owned());
    }
    if q.close_time.is_none() && q.start_time.is_none() && q.precision.is_some() {
        return Err("SearchPrecision requires a StartTime or CloseTime".to_owned());
    }
    Ok(q)
}

/// Go s3store `visibilityArchiver.Query`.
pub fn s3_query_visibility(
    store: &dyn ObjectStore,
    region: &str,
    uri: &ArchiverUri,
    request: &QueryVisibilityRequest,
) -> Result<QueryVisibilityResponse, ArchiverError> {
    if s3_soft_validate_uri(uri).is_err() {
        return Err(ArchiverError::BadRequest(ERR_INVALID_URI.to_owned()));
    }
    if !validate_query_request(request) {
        return Err(ArchiverError::BadRequest(
            ERR_INVALID_QUERY_VISIBILITY_REQUEST.to_owned(),
        ));
    }
    let q = s3_parse_query(&request.query).map_err(ArchiverError::BadRequest)?;
    let (primary_key, primary_value) = match &q.workflow_id {
        Some(id) => ("workflowID", id.clone()),
        None => (
            "workflowTypeName",
            q.workflow_type_name.clone().unwrap_or_default(),
        ),
    };
    let path = s3_key_path(uri);
    let domain = &request.domain_id;
    let mut prefix = format!(
        "{}/",
        s3_visibility_prefix(&path, domain, primary_key, &primary_value, "closeTimeout")
    );
    let precision = q.precision.clone().unwrap_or_default();
    if let Some(ts) = q.close_time {
        prefix = format!(
            "{}/{}",
            s3_visibility_prefix(&path, domain, primary_key, &primary_value, "closeTimeout"),
            precision_time(ts, &precision)
        );
    }
    if let Some(ts) = q.start_time {
        prefix = format!(
            "{}/{}",
            s3_visibility_prefix(&path, domain, primary_key, &primary_value, "startTimeout"),
            precision_time(ts, &precision)
        );
    }
    let bucket = s3_bucket(uri, region).map_err(ArchiverError::BadRequest)?;
    let token = if request.next_page_token.is_empty() {
        None
    } else {
        Some(String::from_utf8_lossy(&request.next_page_token).into_owned())
    };
    let page_size = usize::try_from(request.page_size).unwrap_or(0);
    let results = store
        .list(&bucket, &prefix, None, Some(page_size), token.as_deref())
        .map_err(|e| match e.kind {
            StoreErrorKind::Retryable => ArchiverError::Internal(e.message),
            _ => ArchiverError::BadRequest(e.message),
        })?;
    let mut response = QueryVisibilityResponse::default();
    if results.keys.is_empty() {
        return Ok(response);
    }
    if let Some(next) = results.next_token.filter(|t| !t.is_empty()) {
        response.next_page_token = next.into_bytes();
    }
    for key in &results.keys {
        let data = store
            .get_object(&bucket, key)
            .map_err(|e| ArchiverError::Internal(e.message))?;
        response.records.push(decode_record(&data)?);
    }
    Ok(response)
}

// --------------------------------------------------------------------- GCS

/// Go gcloud `validateURI` (scheme / host / path only).
pub fn gcs_soft_validate_uri(uri: &ArchiverUri) -> Result<(), ArchiverError> {
    if uri.scheme != "gs" {
        return Err(ArchiverError::Other(ERR_URI_SCHEME_MISMATCH.to_owned()));
    }
    if uri.path.is_empty() || uri.host.is_empty() {
        return Err(ArchiverError::Other(ERR_INVALID_URI.to_owned()));
    }
    Ok(())
}

/// Go gcloud `ValidateURI`: scheme check then bucket existence.
pub fn gcs_validate_uri(store: &dyn ObjectStore, uri: &ArchiverUri) -> Result<(), ArchiverError> {
    gcs_soft_validate_uri(uri)?;
    store.head_bucket(&uri.host).map_err(|e| {
        if matches!(e.kind, StoreErrorKind::NoSuchBucket | StoreErrorKind::NotFound) {
            ArchiverError::Other(GCS_ERR_BUCKET_NOT_EXIST.to_owned())
        } else {
            ArchiverError::Other(e.message)
        }
    })
}

/// Go `formatSinkPath`: drop the leading character of the URI path.
fn gcs_sink(uri: &ArchiverUri) -> String {
    uri.path.chars().skip(1).collect()
}

fn gcs_object(uri: &ArchiverUri, file: &str) -> String {
    format!("{}/{file}", gcs_sink(uri))
}

fn gcs_hash(s: &str) -> String {
    if s.is_empty() {
        String::new()
    } else {
        farmhash::fingerprint64(s.as_bytes()).to_string()
    }
}

fn gcs_history_prefix(domain: &str, workflow: &str, run: &str) -> String {
    format!(
        "{}{}{}",
        gcs_hash(domain),
        gcs_hash(workflow),
        gcs_hash(run)
    )
}

fn gcs_history_filename(domain: &str, workflow: &str, run: &str, version: i64, part: i64) -> String {
    format!(
        "{}_{version}_{part}.history",
        gcs_history_prefix(domain, workflow, run)
    )
}

fn base_name(name: &str) -> &str {
    name.rsplit('/').next().unwrap_or(name)
}

/// Go gcloud `extractCloseFailoverVersion`.
fn gcs_extract_version(filename: &str) -> Option<(i64, i64)> {
    let parts: Vec<&str> = filename
        .split(['_', '.'])
        .filter(|p| !p.is_empty())
        .collect();
    if parts.len() != 4 {
        return None;
    }
    Some((parts[1].parse().ok()?, parts[2].parse().ok()?))
}

#[derive(Debug, Serialize, Deserialize)]
struct GcsHistoryToken {
    #[serde(rename = "CloseFailoverVersion")]
    close_failover_version: i64,
    #[serde(rename = "HighestPart")]
    highest_part: i64,
    #[serde(rename = "CurrentPart")]
    current_part: i64,
    #[serde(rename = "BatchIdxOffset")]
    batch_idx_offset: i64,
}

/// Go gcloud `getHighestVersion`: (version, highest part, lowest part).
fn gcs_highest_version(
    store: &dyn ObjectStore,
    uri: &ArchiverUri,
    request: &GetHistoryRequest,
) -> Result<(i64, i64, i64), String> {
    let prefix = gcs_object(
        uri,
        &gcs_history_prefix(&request.domain_id, &request.workflow_id, &request.run_id),
    );
    let names = store
        .list_all(&uri.host, &prefix)
        .map_err(|e| e.message)?;
    let mut best: Option<(i64, i64, i64)> = None;
    for name in names {
        let Some((version, part)) = gcs_extract_version(base_name(&name)) else {
            continue;
        };
        if request
            .close_failover_version
            .is_some_and(|want| want != version)
        {
            continue;
        }
        match best.as_mut() {
            Some(b) if version < b.0 => {}
            Some(b) if version == b.0 => {
                b.1 = b.1.max(part);
                b.2 = b.2.min(part);
            }
            _ => best = Some((version, part, part)),
        }
    }
    best.ok_or_else(|| ERR_HISTORY_NOT_EXIST.to_owned())
}

/// Go gcloud `historyArchiver.Get`.
pub fn gcs_get_history(
    store: &dyn ObjectStore,
    uri: &ArchiverUri,
    request: &GetHistoryRequest,
) -> Result<GetHistoryResponse, ArchiverError> {
    if gcs_validate_uri(store, uri).is_err() {
        return Err(ArchiverError::BadRequest(ERR_INVALID_URI.to_owned()));
    }
    if !validate_get_request(request) {
        return Err(ArchiverError::BadRequest(
            ERR_INVALID_GET_HISTORY_REQUEST.to_owned(),
        ));
    }
    let mut token = if !request.next_page_token.is_empty() {
        serde_json::from_slice::<GcsHistoryToken>(&request.next_page_token)
            .map_err(|_| ArchiverError::BadRequest(ERR_NEXT_PAGE_TOKEN_CORRUPTED.to_owned()))?
    } else {
        let (version, highest, current) =
            gcs_highest_version(store, uri, request).map_err(ArchiverError::Internal)?;
        GcsHistoryToken {
            close_failover_version: version,
            highest_part: highest,
            current_part: current,
            batch_idx_offset: 0,
        }
    };
    let mut response = GetHistoryResponse::default();
    let mut num_events = 0_i64;
    'outer: while token.current_part <= token.highest_part {
        let filename = gcs_history_filename(
            &request.domain_id,
            &request.workflow_id,
            &request.run_id,
            token.close_failover_version,
            token.current_part,
        );
        let data = store
            .get_object(&uri.host, &gcs_object(uri, &filename))
            .map_err(|e| ArchiverError::Internal(e.message))?;
        let batches: Vec<Value> =
            serde_json::from_slice(&data).map_err(|e| ArchiverError::Internal(e.to_string()))?;
        let offset = usize::try_from(token.batch_idx_offset).unwrap_or(0);
        let batches = batches
            .get(offset..)
            .ok_or_else(|| ArchiverError::Internal("slice bounds out of range".to_owned()))?;
        for (idx, batch) in batches.iter().enumerate() {
            response.history_batches.push(batch.clone());
            token.batch_idx_offset += 1;
            num_events += batch
                .get("events")
                .and_then(Value::as_array)
                .map_or(0, |e| e.len() as i64);
            if num_events >= i64::from(request.page_size) {
                if idx == batches.len() - 1 {
                    token.batch_idx_offset = 0;
                    token.current_part += 1;
                }
                break 'outer;
            }
        }
        token.batch_idx_offset = 0;
        token.current_part += 1;
    }
    if token.current_part <= token.highest_part {
        response.next_page_token = serialize_token(&token)?;
    }
    Ok(response)
}

#[derive(Debug, Default)]
struct GcsQuery {
    workflow_id: Option<String>,
    workflow_type: Option<String>,
    run_id: Option<String>,
    start_time: i64,
    close_time: i64,
    precision: Option<String>,
    empty_result: bool,
}

fn gcs_parse_query(query: &str) -> Result<GcsQuery, String> {
    const BACKEND: &str = "Google Cloud Storage";
    let mut q = GcsQuery::default();
    walk_where(query, |col, op, val, quoted| {
        match col {
            "WorkflowID" | "RunID" | "WorkflowType" => {
                let v = extract_string(val, quoted)?;
                only_eq(op, col, BACKEND)?;
                let slot = match col {
                    "WorkflowID" => &mut q.workflow_id,
                    "RunID" => &mut q.run_id,
                    _ => &mut q.workflow_type,
                };
                if slot.as_ref().is_some_and(|c| *c != v) {
                    q.empty_result = true;
                    return Ok(());
                }
                *slot = Some(v);
            }
            "CloseTime" | "StartTime" => {
                let ts = convert_timestamp(val, quoted)?;
                only_eq(op, col, BACKEND)?;
                if col == "CloseTime" {
                    q.close_time = ts;
                } else {
                    q.start_time = ts;
                }
            }
            "SearchPrecision" => {
                let v = extract_string(val, quoted)?;
                only_eq(op, col, BACKEND)?;
                set_precision(&mut q.precision, v)?;
            }
            _ => return Err(format!("unknown filter name: {col}")),
        }
        Ok(())
    })?;
    if (q.close_time == 0 && q.start_time == 0) || (q.close_time != 0 && q.start_time != 0) {
        return Err("requires a StartTime or CloseTime".to_owned());
    }
    if q.precision.is_none() {
        return Err(
            "SearchPrecision is required when searching for a StartTime or CloseTime".to_owned(),
        );
    }
    Ok(q)
}

#[derive(Debug, Serialize, Deserialize)]
struct GcsQueryToken {
    #[serde(rename = "Offset")]
    offset: i64,
}

/// Go gcloud `newWorkflowIDPrecondition` (used for workflow id, run id and type).
fn gcs_precondition(hash: &str, file_name: &str) -> bool {
    if hash.is_empty() {
        return true;
    }
    if !file_name.contains(hash) {
        return false;
    }
    let parts: Vec<&str> = file_name.split('_').collect();
    if parts.len() != 5 {
        return true;
    }
    file_name.contains(parts[3])
}

/// Go gcloud `visibilityArchiver.Query`.
pub fn gcs_query_visibility(
    store: &dyn ObjectStore,
    uri: &ArchiverUri,
    request: &QueryVisibilityRequest,
) -> Result<QueryVisibilityResponse, ArchiverError> {
    if gcs_validate_uri(store, uri).is_err() {
        return Err(ArchiverError::BadRequest(ERR_INVALID_URI.to_owned()));
    }
    if !validate_query_request(request) {
        return Err(ArchiverError::BadRequest(
            ERR_INVALID_QUERY_VISIBILITY_REQUEST.to_owned(),
        ));
    }
    let q = gcs_parse_query(&request.query).map_err(ArchiverError::BadRequest)?;
    if q.empty_result {
        return Ok(QueryVisibilityResponse::default());
    }
    let mut offset = if request.next_page_token.is_empty() {
        0
    } else {
        serde_json::from_slice::<GcsQueryToken>(&request.next_page_token)
            .map_err(|_| ArchiverError::BadRequest(ERR_NEXT_PAGE_TOKEN_CORRUPTED.to_owned()))?
            .offset
    };
    let domain = &request.domain_id;
    let precision = q.precision.clone().unwrap_or_default();
    let mut prefix = format!("{domain}/closeTimeout");
    if q.close_time != 0 {
        prefix = format!(
            "{domain}/closeTimeout_{}",
            precision_time(q.close_time, &precision)
        );
    }
    if q.start_time != 0 {
        prefix = format!(
            "{domain}/startTimeout_{}",
            precision_time(q.start_time, &precision)
        );
    }
    let names = store
        .list_all(&uri.host, &gcs_object(uri, &prefix))
        .map_err(|e| ArchiverError::Internal(e.message))?;
    let hashes: Vec<String> = [&q.workflow_id, &q.run_id, &q.workflow_type]
        .iter()
        .filter_map(|v| v.as_deref().map(gcs_hash))
        .collect();
    let page_size = usize::try_from(request.page_size).unwrap_or(0);
    // Go `QueryWithFilters`: the page-full check yields `completed = true`,
    // so a continuation token is never produced.
    let mut files = Vec::new();
    for name in &names {
        if page_size != 0 && !files.is_empty() && page_size <= files.len() {
            break;
        }
        if !hashes.iter().all(|h| gcs_precondition(h, name)) {
            continue;
        }
        if offset > 0 {
            offset -= 1;
            continue;
        }
        files.push(name.clone());
    }
    let mut response = QueryVisibilityResponse::default();
    for file in files {
        let data = store
            .get_object(&uri.host, &gcs_object(uri, &format!("{domain}/{}", base_name(&file))))
            .map_err(|e| ArchiverError::Internal(e.message))?;
        response.records.push(decode_record(&data)?);
    }
    Ok(response)
}

/// Message of Go's `errObjectNotFound` (kept for HTTP store mapping).
pub fn gcs_object_not_found_message() -> &'static str {
    GCS_ERR_OBJECT_NOT_FOUND
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::archiver::parse_uri;
    use std::collections::BTreeMap;

    #[derive(Default)]
    struct Fake {
        bucket: String,
        objects: BTreeMap<String, Vec<u8>>,
    }

    impl ObjectStore for Fake {
        fn head_bucket(&self, bucket: &str) -> Result<(), StoreError> {
            if bucket == self.bucket {
                Ok(())
            } else {
                Err(StoreError::new(StoreErrorKind::NotFound, "NotFound"))
            }
        }
        fn get_object(&self, bucket: &str, key: &str) -> Result<Vec<u8>, StoreError> {
            if bucket != self.bucket {
                return Err(StoreError::new(StoreErrorKind::NoSuchBucket, "no bucket"));
            }
            self.objects
                .get(key)
                .cloned()
                .ok_or_else(|| StoreError::new(StoreErrorKind::NoSuchKey, "no key"))
        }
        fn list(
            &self,
            _bucket: &str,
            prefix: &str,
            delimiter: Option<&str>,
            max_keys: Option<usize>,
            token: Option<&str>,
        ) -> Result<ListPage, StoreError> {
            let mut page = ListPage::default();
            let mut count = 0;
            for key in self.objects.keys().filter(|k| k.starts_with(prefix)) {
                if token.is_some_and(|t| key.as_str() <= t) {
                    continue;
                }
                if let Some(d) = delimiter {
                    let rest = &key[prefix.len()..];
                    if let Some(i) = rest.find(d) {
                        let cp = format!("{prefix}{}", &rest[..=i]);
                        if !page.common_prefixes.contains(&cp) {
                            page.common_prefixes.push(cp);
                        }
                        continue;
                    }
                }
                if max_keys.is_some_and(|m| count >= m) {
                    page.next_token = page.keys.last().cloned();
                    break;
                }
                page.keys.push(key.clone());
                count += 1;
            }
            Ok(page)
        }
    }

    fn blob(events: usize, last: bool) -> Vec<u8> {
        let evs: Vec<Value> = (0..events).map(|i| serde_json::json!({"eventId": i})).collect();
        serde_json::to_vec(&serde_json::json!({
            "header": {"is_last": last},
            "body": [{"events": evs}],
        }))
        .unwrap()
    }

    fn req(page_size: i32) -> GetHistoryRequest {
        GetHistoryRequest {
            domain_id: "dom".into(),
            workflow_id: "wf".into(),
            run_id: "run".into(),
            page_size,
            ..Default::default()
        }
    }

    #[test]
    fn s3_uri_validation_matches_go() {
        let u = |s: &str| parse_uri(s).unwrap();
        assert_eq!(s3_soft_validate_uri(&u("s3:///p")).unwrap_err(), "no bucket specified");
        assert!(s3_soft_validate_uri(&u("s3://bkt/p")).is_ok());
        assert_eq!(
            s3_soft_validate_uri(&u("gs://bkt/p")).unwrap_err(),
            ERR_URI_SCHEME_MISMATCH
        );
        assert!(s3_soft_validate_uri(&u("s3-ap://123456789012/ap/x")).is_ok());
        assert_eq!(
            s3_soft_validate_uri(&u("s3-ap://12345/ap")).unwrap_err(),
            ERR_INVALID_ACCESS_POINT_ACCT
        );
        assert_eq!(
            s3_soft_validate_uri(&u("s3-ap://123456789012/")).unwrap_err(),
            ERR_INVALID_ACCESS_POINT_URI
        );
        assert_eq!(
            s3_bucket(&u("s3-ap://123456789012/ap/x"), "cn-north-1").unwrap(),
            "arn:aws-cn:s3:cn-north-1:123456789012:accesspoint/ap"
        );
        assert_eq!(s3_key_path(&u("s3-ap://123456789012/ap/x")), "/x");
        let fake = Fake {
            bucket: "bkt".into(),
            ..Default::default()
        };
        assert!(s3_validate_uri(&fake, "us-east-1", &u("s3://bkt/p")).is_ok());
        assert_eq!(
            s3_validate_uri(&fake, "us-east-1", &u("s3://other/p")).unwrap_err().message(),
            ERR_BUCKET_NOT_EXISTS
        );
    }

    #[test]
    fn s3_history_pages_and_highest_version() {
        let mut fake = Fake {
            bucket: "bkt".into(),
            ..Default::default()
        };
        for (v, n) in [(1_i64, 0_i64), (5, 0)] {
            fake.objects.insert(
                s3_history_key("/arch", "dom", "wf", "run", v, n),
                blob(2, false),
            );
        }
        fake.objects
            .insert(s3_history_key("/arch", "dom", "wf", "run", 5, 1), blob(3, true));
        let uri = parse_uri("s3://bkt/arch").unwrap();
        let first = s3_get_history(&fake, "us-east-1", &uri, &req(2)).unwrap();
        assert_eq!(first.history_batches.len(), 1);
        assert!(!first.next_page_token.is_empty());
        let mut r = req(2);
        r.next_page_token = first.next_page_token;
        let second = s3_get_history(&fake, "us-east-1", &uri, &r).unwrap();
        assert_eq!(second.history_batches[0]["events"].as_array().unwrap().len(), 3);
        assert!(second.next_page_token.is_empty());
        // pinned older version
        let mut r = req(100);
        r.close_failover_version = Some(1);
        let err = s3_get_history(&fake, "us-east-1", &uri, &r).unwrap_err();
        assert_eq!(err, ArchiverError::EntityNotExists(ERR_HISTORY_NOT_EXIST.into()));
        // missing history
        let mut r = req(100);
        r.run_id = "nope".into();
        let err = s3_get_history(&fake, "us-east-1", &uri, &r).unwrap_err();
        assert_eq!(err, ArchiverError::BadRequest(ERR_HISTORY_NOT_EXIST.into()));
        // corrupted token, invalid request, invalid uri
        let mut r = req(1);
        r.next_page_token = b"zz".to_vec();
        assert_eq!(
            s3_get_history(&fake, "r", &uri, &r).unwrap_err(),
            ArchiverError::BadRequest(ERR_NEXT_PAGE_TOKEN_CORRUPTED.into())
        );
        assert_eq!(
            s3_get_history(&fake, "r", &uri, &req(0)).unwrap_err(),
            ArchiverError::BadRequest(ERR_INVALID_GET_HISTORY_REQUEST.into())
        );
        assert_eq!(
            s3_get_history(&fake, "r", &parse_uri("s3:///x").unwrap(), &req(1)).unwrap_err(),
            ArchiverError::BadRequest(ERR_INVALID_URI.into())
        );
    }

    #[test]
    fn s3_query_parser_rules() {
        let ok = |q: &str| s3_parse_query(q).unwrap();
        let q = ok("WorkflowID = 'a' and CloseTime = '2020-01-02T03:04:05Z' and SearchPrecision = 'Hour'");
        assert_eq!(q.workflow_id.as_deref(), Some("a"));
        assert_eq!(q.close_time, Some(1_577_934_245_000_000_000));
        let err = |q: &str| s3_parse_query(q).unwrap_err();
        assert_eq!(err("CloseTime = 5"), "WorkflowID or WorkflowTypeName is required in query");
        assert_eq!(
            err("WorkflowID = 'a' and WorkflowTypeName = 'b'"),
            "only one of WorkflowID or WorkflowTypeName can be specified in a query"
        );
        assert_eq!(
            err("WorkflowID = 'a' and CloseTime = 5"),
            "SearchPrecision is required when searching for a StartTime or CloseTime"
        );
        assert_eq!(
            err("WorkflowID = 'a' and SearchPrecision = 'Day'"),
            "SearchPrecision requires a StartTime or CloseTime"
        );
        assert_eq!(
            err("WorkflowID > 'a'"),
            "only operator = is supported for WorkflowID with Amazon S3"
        );
        assert_eq!(err("WorkflowID = 'a' and WorkflowID = 'b'"), "can not query WorkflowID multiple times");
        assert_eq!(err("Foo = 'a'"), "unknown filter name: Foo");
        assert_eq!(
            err("WorkflowID = 'a' and SearchPrecision = 'Week'"),
            "invalid value for SearchPrecision: Week"
        );
        assert_eq!(precision_time(1_577_934_245_000_000_000, "Day"), "2020-01-02T");
        assert_eq!(precision_time(1_577_934_245_000_000_000, "Minute"), "2020-01-02T03:04");
    }

    fn vis_record(wf: &str, run: &str, close: i64) -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({"WorkflowID": wf, "RunID": run,
            "WorkflowTypeName": "t", "StartTimestamp": 1, "ExecutionTimestamp": 0,
            "CloseTimestamp": close, "CloseStatus": 0, "HistoryLength": 3}))
        .unwrap()
    }

    #[test]
    fn s3_visibility_query_lists_by_index_prefix() {
        let mut fake = Fake {
            bucket: "bkt".into(),
            ..Default::default()
        };
        for run in ["r1", "r2", "r3"] {
            let prefix = s3_visibility_prefix("/arch", "dom", "workflowID", "wf", "closeTimeout");
            fake.objects.insert(
                format!("{prefix}/2020-01-02T03:04:05Z/{run}"),
                vis_record("wf", run, 10),
            );
        }
        let uri = parse_uri("s3://bkt/arch").unwrap();
        let mut r = QueryVisibilityRequest {
            domain_id: "dom".into(),
            page_size: 2,
            query: "WorkflowID = 'wf'".into(),
            ..Default::default()
        };
        let first = s3_query_visibility(&fake, "r", &uri, &r).unwrap();
        assert_eq!(first.records.len(), 2);
        assert!(!first.next_page_token.is_empty());
        r.next_page_token = first.next_page_token;
        let second = s3_query_visibility(&fake, "r", &uri, &r).unwrap();
        assert_eq!(second.records.len(), 1);
        assert_eq!(second.records[0].run_id, "r3");
        assert!(second.next_page_token.is_empty());
        r.next_page_token.clear();
        r.query = "WorkflowID = 'wf' and CloseTime = '2020-01-02T03:04:05Z' and SearchPrecision = 'Day'".into();
        assert_eq!(s3_query_visibility(&fake, "r", &uri, &r).unwrap().records.len(), 2);
        r.query = "WorkflowID = 'other'".into();
        assert!(s3_query_visibility(&fake, "r", &uri, &r).unwrap().records.is_empty());
        r.query = "WorkflowID > 'x'".into();
        assert_eq!(
            s3_query_visibility(&fake, "r", &uri, &r).unwrap_err(),
            ArchiverError::BadRequest("only operator = is supported for WorkflowID with Amazon S3".into())
        );
    }

    #[test]
    fn gcs_history_multipart_paging() {
        let mut fake = Fake {
            bucket: "bkt".into(),
            ..Default::default()
        };
        let batch = |n: usize| {
            let evs: Vec<Value> = (0..n).map(|i| serde_json::json!({"eventId": i})).collect();
            serde_json::to_vec(&serde_json::json!([{"events": evs}, {"events": evs}])).unwrap()
        };
        let uri = parse_uri("gs://bkt/arch").unwrap();
        for (v, p) in [(1_i64, 0_i64), (3, 0), (3, 1)] {
            fake.objects.insert(
                gcs_object(&uri, &gcs_history_filename("dom", "wf", "run", v, p)),
                batch(2),
            );
        }
        // part 0 (2 batches x 2 events) then part 1; page_size 4 stops at the end of part 0
        let first = gcs_get_history(&fake, &uri, &req(4)).unwrap();
        assert_eq!(first.history_batches.len(), 2);
        let token: GcsHistoryToken = serde_json::from_slice(&first.next_page_token).unwrap();
        assert_eq!((token.close_failover_version, token.current_part, token.highest_part), (3, 1, 1));
        let mut r = req(4);
        r.next_page_token = first.next_page_token;
        let second = gcs_get_history(&fake, &uri, &r).unwrap();
        assert_eq!(second.history_batches.len(), 2);
        assert!(second.next_page_token.is_empty());
        // mid-part truncation keeps the offset
        let small = gcs_get_history(&fake, &uri, &req(1)).unwrap();
        let t: GcsHistoryToken = serde_json::from_slice(&small.next_page_token).unwrap();
        assert_eq!((t.current_part, t.batch_idx_offset), (0, 1));
        // errors
        let mut r = req(1);
        r.run_id = "none".into();
        assert_eq!(
            gcs_get_history(&fake, &uri, &r).unwrap_err(),
            ArchiverError::Internal(ERR_HISTORY_NOT_EXIST.into())
        );
        assert_eq!(
            gcs_get_history(&fake, &parse_uri("gs://missing/arch").unwrap(), &req(1)).unwrap_err(),
            ArchiverError::BadRequest(ERR_INVALID_URI.into())
        );
    }

    #[test]
    fn gcs_visibility_filters_and_parser() {
        let mut fake = Fake {
            bucket: "bkt".into(),
            ..Default::default()
        };
        let uri = parse_uri("gs://bkt/arch").unwrap();
        for (wf, run) in [("wf1", "r1"), ("wf2", "r2"), ("wf1", "r3")] {
            let file = format!(
                "dom/closeTimeout_2020-01-02T03:04:05Z_{}_{}_{}.visibility",
                gcs_hash("t"),
                gcs_hash(wf),
                gcs_hash(run)
            );
            fake.objects.insert(gcs_object(&uri, &file), vis_record(wf, run, 10));
        }
        let mut r = QueryVisibilityRequest {
            domain_id: "dom".into(),
            page_size: 10,
            query: "CloseTime = '2020-01-02T03:04:05Z' and SearchPrecision = 'Day' and WorkflowID = 'wf1'".into(),
            ..Default::default()
        };
        let resp = gcs_query_visibility(&fake, &uri, &r).unwrap();
        assert_eq!(resp.records.len(), 2);
        r.page_size = 1;
        let resp = gcs_query_visibility(&fake, &uri, &r).unwrap();
        assert_eq!(resp.records.len(), 1);
        assert!(resp.next_page_token.is_empty());
        r.query = "CloseTime = 5 and SearchPrecision = 'Day' and WorkflowID = 'a' and WorkflowID = 'b'".into();
        assert!(gcs_query_visibility(&fake, &uri, &r).unwrap().records.is_empty());
        r.query = "WorkflowID = 'wf1'".into();
        assert_eq!(
            gcs_query_visibility(&fake, &uri, &r).unwrap_err(),
            ArchiverError::BadRequest("requires a StartTime or CloseTime".into())
        );
        r.query = "CloseTime = 5".into();
        assert_eq!(
            gcs_query_visibility(&fake, &uri, &r).unwrap_err(),
            ArchiverError::BadRequest(
                "SearchPrecision is required when searching for a StartTime or CloseTime".into()
            )
        );
        r.query = "WorkflowID > 'x'".into();
        assert_eq!(
            gcs_query_visibility(&fake, &uri, &r).unwrap_err(),
            ArchiverError::BadRequest(
                "only operator = is supported for WorkflowID with Google Cloud Storage".into()
            )
        );
    }

    #[test]
    fn gcs_uri_validation() {
        let fake = Fake {
            bucket: "bkt".into(),
            ..Default::default()
        };
        assert!(gcs_validate_uri(&fake, &parse_uri("gs://bkt/a").unwrap()).is_ok());
        assert_eq!(
            gcs_validate_uri(&fake, &parse_uri("s3://bkt/a").unwrap()).unwrap_err().message(),
            ERR_URI_SCHEME_MISMATCH
        );
        assert_eq!(
            gcs_validate_uri(&fake, &parse_uri("gs://bkt").unwrap()).unwrap_err().message(),
            ERR_INVALID_URI
        );
        assert_eq!(
            gcs_validate_uri(&fake, &parse_uri("gs://zzz/a").unwrap()).unwrap_err().message(),
            GCS_ERR_BUCKET_NOT_EXIST
        );
    }
}
