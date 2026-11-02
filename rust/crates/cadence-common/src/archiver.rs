//! Archiver URI parsing, provider seams and the filestore archiver read path.
//!
//! Mirrors `go/common/archiver` (`URI.go`, `util.go`, `provider`) and
//! `go/common/archiver/filestore` (`util.go`, `historyArchiver.go` Get/ValidateURI,
//! `visibilityArchiver.go` Query/ValidateURI, `queryParser.go`).
//! Archive (write) paths belong to the History service and are not ported here.

use std::collections::BTreeSet;
use std::path::Path;

use serde::Deserialize;
use serde_json::Value;

pub const FILESTORE_SCHEME: &str = "file";
pub const FILESTORE_CONFIG_KEY: &str = "filestore";
pub const S3_SCHEME: &str = "s3";
pub const S3_CONFIG_KEY: &str = "s3store";
pub const GCLOUD_SCHEME: &str = "gs";
pub const GCLOUD_CONFIG_KEY: &str = "gstorage";

pub const ERR_INVALID_URI: &str = "URI is invalid";
pub const ERR_URI_SCHEME_MISMATCH: &str = "URI scheme does not match the archiver";
pub const ERR_INVALID_GET_HISTORY_REQUEST: &str = "get archived history request is invalid";
pub const ERR_INVALID_QUERY_VISIBILITY_REQUEST: &str = "query visiblity request is invalid";
pub const ERR_NEXT_PAGE_TOKEN_CORRUPTED: &str = "next page token is corrupted";
pub const ERR_HISTORY_NOT_EXIST: &str = "requested workflow history does not exist";
pub const ERR_NOT_SUPPORTED: &str = "archiver provider not supported";
const ERR_EMPTY_DIRECTORY_PATH: &str = "directory path is empty";
const ERR_DIRECTORY_EXPECTED: &str = "a path to a directory was expected";

/// Errors with the Cadence error type the Go archiver returns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArchiverError {
    BadRequest(String),
    EntityNotExists(String),
    Internal(String),
    /// Plain Go `error` (provider/URI failures).
    Other(String),
}

impl ArchiverError {
    pub fn message(&self) -> &str {
        match self {
            Self::BadRequest(m) | Self::EntityNotExists(m) | Self::Internal(m) | Self::Other(m) => {
                m
            }
        }
    }
}

impl std::fmt::Display for ArchiverError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.message())
    }
}

impl std::error::Error for ArchiverError {}

/// Go `archiver.URI` over `net/url.ParseRequestURI`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiverUri {
    pub raw: String,
    pub scheme: String,
    pub path: String,
    pub host: String,
    pub opaque: String,
}

fn go_quote(s: &str) -> String {
    format!("{s:?}")
}

fn percent_decode(path: &str) -> Result<String, String> {
    let bytes = path.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let hex = bytes
                .get(i + 1..i + 3)
                .and_then(|h| std::str::from_utf8(h).ok())
                .and_then(|h| u8::from_str_radix(h, 16).ok());
            match hex {
                Some(b) => {
                    out.push(b);
                    i += 3;
                }
                None => {
                    let end = (i + 3).min(bytes.len());
                    return Err(format!("invalid URL escape {}", go_quote(&path[i..end])));
                }
            }
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    Ok(String::from_utf8_lossy(&out).into_owned())
}

/// Go `archiver.NewURI` (`url.ParseRequestURI`).
pub fn parse_uri(s: &str) -> Result<ArchiverUri, String> {
    let wrap = |e: &str| format!("parse {}: {e}", go_quote(s));
    if s.is_empty() {
        return Err(wrap("empty url"));
    }
    // getScheme
    let mut scheme = String::new();
    let mut rest = s;
    for (i, c) in s.char_indices() {
        match c {
            'a'..='z' | 'A'..='Z' => {}
            '0'..='9' | '+' | '-' | '.' => {
                if i == 0 {
                    break;
                }
            }
            ':' => {
                if i == 0 {
                    return Err(wrap("missing protocol scheme"));
                }
                scheme = s[..i].to_ascii_lowercase();
                rest = &s[i + 1..];
                break;
            }
            _ => break,
        }
    }
    let rest = rest.split('?').next().unwrap_or("");
    if !rest.starts_with('/') {
        if !scheme.is_empty() {
            return Ok(ArchiverUri {
                raw: s.to_owned(),
                scheme,
                path: String::new(),
                host: String::new(),
                opaque: rest.to_owned(),
            });
        }
        return Err(wrap("invalid URI for request"));
    }
    let mut host = String::new();
    let mut path = rest;
    if !scheme.is_empty() && rest.starts_with("//") {
        let after = &rest[2..];
        let end = after.find('/').unwrap_or(after.len());
        let authority = &after[..end];
        let hostport = authority.rsplit('@').next().unwrap_or("");
        host = match hostport.rsplit_once(':') {
            Some((h, p)) if p.chars().all(|c| c.is_ascii_digit()) => h.to_owned(),
            _ => hostport.to_owned(),
        };
        path = &after[end..];
    }
    let path = percent_decode(path).map_err(|e| wrap(&e))?;
    Ok(ArchiverUri {
        raw: s.to_owned(),
        scheme,
        path,
        host,
        opaque: String::new(),
    })
}

/// Which archiver plugins the operator configured (`archival.<kind>.provider`).
#[derive(Debug, Clone, Default)]
pub struct ArchiverProvider {
    configured_keys: BTreeSet<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    History,
    Visibility,
}

impl Kind {
    fn name(self) -> &'static str {
        match self {
            Self::History => "history",
            Self::Visibility => "visibility",
        }
    }
}

impl ArchiverProvider {
    pub fn new(configured_keys: impl IntoIterator<Item = String>) -> Self {
        Self {
            configured_keys: configured_keys.into_iter().collect(),
        }
    }

    /// Go `provider.GetHistoryArchiver/GetVisibilityArchiver` resolution.
    /// Only the filestore plugin is implemented; s3store and gstorage are
    /// seams that fail with Go's not-supported error once configured.
    fn resolve(&self, kind: Kind, scheme: &str) -> Result<(), ArchiverError> {
        let key = match scheme {
            FILESTORE_SCHEME => FILESTORE_CONFIG_KEY,
            S3_SCHEME => S3_CONFIG_KEY,
            GCLOUD_SCHEME => GCLOUD_CONFIG_KEY,
            _ => {
                return Err(ArchiverError::Other(format!(
                    "no {} archiver constructor for scheme {}",
                    kind.name(),
                    go_quote(scheme)
                )))
            }
        };
        if !self.configured_keys.contains(key) {
            return Err(ArchiverError::Other(format!(
                "no {} archiver config for scheme {}, config key {}",
                kind.name(),
                go_quote(scheme),
                go_quote(key)
            )));
        }
        if scheme != FILESTORE_SCHEME {
            return Err(ArchiverError::Other(format!(
                "{}: {} archiver for scheme {} is not implemented in the Rust port",
                ERR_NOT_SUPPORTED,
                kind.name(),
                go_quote(scheme)
            )));
        }
        Ok(())
    }

    /// Go `validateHistoryArchivalURI` in `common/domain/handler.go`.
    pub fn validate_history_uri(&self, uri: &str) -> Result<(), ArchiverError> {
        self.validate_uri(Kind::History, uri)
    }

    /// Go `validateVisibilityArchivalURI` in `common/domain/handler.go`.
    pub fn validate_visibility_uri(&self, uri: &str) -> Result<(), ArchiverError> {
        self.validate_uri(Kind::Visibility, uri)
    }

    fn validate_uri(&self, kind: Kind, uri: &str) -> Result<(), ArchiverError> {
        let parsed = parse_uri(uri).map_err(ArchiverError::Other)?;
        self.resolve(kind, &parsed.scheme)?;
        filestore_validate_uri(&parsed)
    }

    pub fn get_history(
        &self,
        uri: &str,
        request: &GetHistoryRequest,
    ) -> Result<GetHistoryResponse, ArchiverError> {
        let parsed = parse_uri(uri).map_err(ArchiverError::Other)?;
        self.resolve(Kind::History, &parsed.scheme)?;
        filestore_get_history(&parsed, request)
    }

    pub fn query_visibility(
        &self,
        uri: &str,
        request: &QueryVisibilityRequest,
    ) -> Result<QueryVisibilityResponse, ArchiverError> {
        let parsed = parse_uri(uri).map_err(ArchiverError::Other)?;
        self.resolve(Kind::Visibility, &parsed.scheme)?;
        filestore_query_visibility(&parsed, request)
    }
}

// ---------------------------------------------------------------- filestore

fn fs_hash(s: &str) -> String {
    farmhash::fingerprint64(s.as_bytes()).to_string()
}

fn history_filename_prefix(domain_id: &str, workflow_id: &str, run_id: &str) -> String {
    format!(
        "{}{}{}",
        fs_hash(domain_id),
        fs_hash(workflow_id),
        fs_hash(run_id)
    )
}

pub fn history_filename(domain_id: &str, workflow_id: &str, run_id: &str, version: i64) -> String {
    format!(
        "{}_{version}.history",
        history_filename_prefix(domain_id, workflow_id, run_id)
    )
}

pub fn visibility_filename(close_timestamp: i64, run_id: &str) -> String {
    format!("{close_timestamp}_{}.visibility", fs_hash(run_id))
}

fn validate_dir_path(dir: &str) -> Result<(), ArchiverError> {
    if dir.is_empty() {
        return Err(ArchiverError::Other(ERR_EMPTY_DIRECTORY_PATH.into()));
    }
    match std::fs::metadata(dir) {
        Ok(m) if m.is_dir() => Ok(()),
        Ok(_) => Err(ArchiverError::Other(ERR_DIRECTORY_EXPECTED.into())),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(ArchiverError::Other(e.to_string())),
    }
}

pub fn filestore_validate_uri(uri: &ArchiverUri) -> Result<(), ArchiverError> {
    if uri.scheme != FILESTORE_SCHEME {
        return Err(ArchiverError::Other(ERR_URI_SCHEME_MISMATCH.into()));
    }
    validate_dir_path(&uri.path)
}

fn directory_exists(path: &str) -> Result<bool, ArchiverError> {
    match std::fs::metadata(path) {
        Ok(m) => Ok(m.is_dir()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(ArchiverError::Internal(e.to_string())),
    }
}

fn list_files(dir: &str) -> Result<Vec<String>, ArchiverError> {
    let internal = |e: std::io::Error| ArchiverError::Internal(e.to_string());
    let mut names = Vec::new();
    for entry in std::fs::read_dir(dir).map_err(internal)? {
        let entry = entry.map_err(internal)?;
        if entry.file_type().map_err(internal)?.is_file() {
            names.push(entry.file_name().to_string_lossy().into_owned());
        }
    }
    names.sort();
    Ok(names)
}

#[derive(Debug, Clone, Default)]
pub struct GetHistoryRequest {
    pub domain_id: String,
    pub workflow_id: String,
    pub run_id: String,
    pub close_failover_version: Option<i64>,
    pub next_page_token: Vec<u8>,
    pub page_size: i32,
}

#[derive(Debug, Clone, Default)]
pub struct GetHistoryResponse {
    /// Each batch is a Go `types.History` JSON object (`{"events":[...]}`).
    pub history_batches: Vec<Value>,
    pub next_page_token: Vec<u8>,
}

#[derive(Debug, Deserialize, serde::Serialize)]
struct GetHistoryToken {
    #[serde(rename = "CloseFailoverVersion")]
    close_failover_version: i64,
    #[serde(rename = "NextBatchIdx")]
    next_batch_idx: i64,
}

fn extract_close_failover_version(filename: &str) -> Option<i64> {
    let parts: Vec<&str> = filename
        .split(['_', '.'])
        .filter(|p| !p.is_empty())
        .collect();
    if parts.len() != 3 {
        return None;
    }
    parts[1].parse().ok()
}

fn get_highest_version(dir: &str, request: &GetHistoryRequest) -> Result<i64, ArchiverError> {
    let prefix = history_filename_prefix(&request.domain_id, &request.workflow_id, &request.run_id);
    let mut highest: Option<i64> = None;
    for name in list_files(dir)? {
        if !name.starts_with(&prefix) {
            continue;
        }
        if let Some(v) = extract_close_failover_version(&name) {
            if highest.is_none_or(|h| v > h) {
                highest = Some(v);
            }
        }
    }
    highest.ok_or_else(|| ArchiverError::Internal(ERR_HISTORY_NOT_EXIST.into()))
}

/// Go filestore `historyArchiver.Get`.
pub fn filestore_get_history(
    uri: &ArchiverUri,
    request: &GetHistoryRequest,
) -> Result<GetHistoryResponse, ArchiverError> {
    if filestore_validate_uri(uri).is_err() {
        return Err(ArchiverError::BadRequest(ERR_INVALID_URI.into()));
    }
    if request.domain_id.is_empty()
        || request.workflow_id.is_empty()
        || request.run_id.is_empty()
        || request.page_size == 0
    {
        return Err(ArchiverError::BadRequest(
            ERR_INVALID_GET_HISTORY_REQUEST.into(),
        ));
    }
    let dir = uri.path.as_str();
    if !directory_exists(dir)? {
        return Err(ArchiverError::BadRequest(ERR_HISTORY_NOT_EXIST.into()));
    }
    let mut token = if !request.next_page_token.is_empty() {
        serde_json::from_slice::<GetHistoryToken>(&request.next_page_token)
            .map_err(|_| ArchiverError::BadRequest(ERR_NEXT_PAGE_TOKEN_CORRUPTED.into()))?
    } else if let Some(version) = request.close_failover_version {
        GetHistoryToken {
            close_failover_version: version,
            next_batch_idx: 0,
        }
    } else {
        GetHistoryToken {
            close_failover_version: get_highest_version(dir, request)?,
            next_batch_idx: 0,
        }
    };
    let filename = history_filename(
        &request.domain_id,
        &request.workflow_id,
        &request.run_id,
        token.close_failover_version,
    );
    let file = Path::new(dir).join(filename);
    match std::fs::metadata(&file) {
        Ok(m) if m.is_file() => {}
        Ok(_) => {
            return Err(ArchiverError::EntityNotExists(ERR_HISTORY_NOT_EXIST.into()));
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Err(ArchiverError::EntityNotExists(ERR_HISTORY_NOT_EXIST.into()));
        }
        Err(e) => return Err(ArchiverError::Internal(e.to_string())),
    }
    let data = std::fs::read(&file).map_err(|e| ArchiverError::Internal(e.to_string()))?;
    let batches: Vec<Value> =
        serde_json::from_slice(&data).map_err(|e| ArchiverError::Internal(e.to_string()))?;
    let start = usize::try_from(token.next_batch_idx).unwrap_or(0);
    // Go slices `historyBatches[token.NextBatchIdx:]`, which panics out of range.
    let remaining = batches
        .get(start..)
        .ok_or_else(|| ArchiverError::Internal("slice bounds out of range".to_owned()))?;
    let mut response = GetHistoryResponse::default();
    let mut num_events = 0_i64;
    let mut num_batches = 0_usize;
    for batch in remaining {
        response.history_batches.push(batch.clone());
        num_batches += 1;
        num_events += batch
            .get("events")
            .and_then(Value::as_array)
            .map_or(0, |e| e.len() as i64);
        if num_events >= i64::from(request.page_size) {
            break;
        }
    }
    if num_batches < remaining.len() {
        token.next_batch_idx += num_batches as i64;
        response.next_page_token =
            serde_json::to_vec(&token).map_err(|e| ArchiverError::Internal(e.to_string()))?;
    }
    Ok(response)
}

// -------------------------------------------------------- visibility query

#[derive(Debug, Clone, Default)]
pub struct QueryVisibilityRequest {
    pub domain_id: String,
    pub page_size: i32,
    pub next_page_token: Vec<u8>,
    pub query: String,
}

/// Visibility record as written by Go's filestore archiver (no JSON tags, so
/// Go field names). `close_status`, `memo` stay raw `Value`s for the caller.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct VisibilityRecord {
    #[serde(rename = "WorkflowID")]
    pub workflow_id: String,
    #[serde(rename = "RunID")]
    pub run_id: String,
    #[serde(rename = "WorkflowTypeName")]
    pub workflow_type_name: String,
    #[serde(rename = "StartTimestamp")]
    pub start_timestamp: i64,
    #[serde(rename = "ExecutionTimestamp")]
    pub execution_timestamp: i64,
    #[serde(rename = "CloseTimestamp")]
    pub close_timestamp: i64,
    #[serde(rename = "CloseStatus")]
    pub close_status: Value,
    #[serde(rename = "HistoryLength")]
    pub history_length: i64,
    #[serde(rename = "Memo")]
    pub memo: Value,
    #[serde(rename = "SearchAttributes")]
    pub search_attributes: Option<std::collections::BTreeMap<String, String>>,
}

#[derive(Debug, Clone, Default)]
pub struct QueryVisibilityResponse {
    pub records: Vec<VisibilityRecord>,
    pub next_page_token: Vec<u8>,
}

#[derive(Debug, Deserialize, serde::Serialize)]
struct QueryVisibilityToken {
    #[serde(rename = "LastCloseTime")]
    last_close_time: i64,
    #[serde(rename = "LastRunID")]
    last_run_id: String,
}

/// Go `WorkflowExecutionCloseStatus` ordinals.
pub fn close_status_ordinal(status: &Value) -> Option<i64> {
    match status {
        Value::Number(n) => n.as_i64(),
        Value::String(s) => match s.as_str() {
            "COMPLETED" => Some(0),
            "FAILED" => Some(1),
            "CANCELED" => Some(2),
            "TERMINATED" => Some(3),
            "CONTINUED_AS_NEW" => Some(4),
            "TIMED_OUT" => Some(5),
            _ => None,
        },
        _ => None,
    }
}

#[derive(Debug, Default)]
struct ParsedQuery {
    earliest_close_time: i64,
    latest_close_time: i64,
    workflow_id: Option<String>,
    run_id: Option<String>,
    workflow_type_name: Option<String>,
    close_status: Option<i64>,
    empty_result: bool,
}

#[derive(Debug, Clone, PartialEq)]
enum Tok {
    Ident(String),
    Str(String),
    Num(String),
    Op(String),
    LParen,
    RParen,
}

fn tokenize(q: &str) -> Result<Vec<Tok>, String> {
    let chars: Vec<char> = q.chars().collect();
    let mut i = 0;
    let mut out = Vec::new();
    while i < chars.len() {
        let c = chars[i];
        if c.is_whitespace() {
            i += 1;
        } else if c == '(' {
            out.push(Tok::LParen);
            i += 1;
        } else if c == ')' {
            out.push(Tok::RParen);
            i += 1;
        } else if c == '\'' {
            let mut s = String::new();
            i += 1;
            loop {
                match chars.get(i) {
                    None => return Err(format!("syntax error at position {}", i + 1)),
                    Some('\'') if chars.get(i + 1) == Some(&'\'') => {
                        s.push('\'');
                        i += 2;
                    }
                    Some('\'') => {
                        i += 1;
                        break;
                    }
                    Some(ch) => {
                        s.push(*ch);
                        i += 1;
                    }
                }
            }
            out.push(Tok::Str(s));
        } else if matches!(c, '=' | '<' | '>' | '!') {
            let mut op = c.to_string();
            if let Some(n) = chars.get(i + 1) {
                if matches!((c, *n), ('<', '=') | ('>', '=') | ('!', '=') | ('<', '>')) {
                    op.push(*n);
                    i += 1;
                }
            }
            i += 1;
            out.push(Tok::Op(op));
        } else if c.is_ascii_digit() || c == '-' {
            let start = i;
            i += 1;
            while i < chars.len() && chars[i].is_ascii_digit() {
                i += 1;
            }
            out.push(Tok::Num(chars[start..i].iter().collect()));
        } else if c.is_alphabetic() || c == '_' {
            let start = i;
            while i < chars.len() && (chars[i].is_alphanumeric() || chars[i] == '_') {
                i += 1;
            }
            out.push(Tok::Ident(chars[start..i].iter().collect()));
        } else {
            return Err(format!("syntax error at position {}", i + 1));
        }
    }
    Ok(out)
}

struct Parser {
    toks: Vec<Tok>,
    pos: usize,
}

impl Parser {
    fn expr(&mut self, q: &mut ParsedQuery) -> Result<(), String> {
        self.term(q)?;
        while let Some(Tok::Ident(w)) = self.toks.get(self.pos) {
            if w.eq_ignore_ascii_case("and") {
                self.pos += 1;
                self.term(q)?;
            } else if w.eq_ignore_ascii_case("or") {
                return Err("only comparison and \"and\" expression is supported".into());
            } else {
                return Err(format!("syntax error near '{w}'"));
            }
        }
        Ok(())
    }

    fn term(&mut self, q: &mut ParsedQuery) -> Result<(), String> {
        match self.toks.get(self.pos).cloned() {
            Some(Tok::LParen) => {
                self.pos += 1;
                self.expr(q)?;
                if self.toks.get(self.pos) != Some(&Tok::RParen) {
                    return Err("syntax error".into());
                }
                self.pos += 1;
                Ok(())
            }
            Some(Tok::Ident(col)) => {
                self.pos += 1;
                let Some(Tok::Op(op)) = self.toks.get(self.pos).cloned() else {
                    return Err("only comparison and \"and\" expression is supported".to_string());
                };
                self.pos += 1;
                let (val, quoted) = match self.toks.get(self.pos).cloned() {
                    Some(Tok::Str(s)) => (s, true),
                    Some(Tok::Num(n)) => (n, false),
                    Some(Tok::Ident(i)) => return Err(format!("invalid value: {i}")),
                    _ => return Err("syntax error".into()),
                };
                self.pos += 1;
                apply_comparison(&col, &op, &val, quoted, q)
            }
            _ => Err("only comparison and \"and\" expression is supported".into()),
        }
    }
}

/// Go renders a string SQLVal as `'v'` (quotes re-escaped) and a number as-is.
fn render_val(val: &str, quoted: bool) -> String {
    if quoted {
        format!("'{}'", val.replace('\'', "''"))
    } else {
        val.to_owned()
    }
}

fn extract_string(val: &str, quoted: bool) -> Result<String, String> {
    if quoted {
        Ok(val.to_owned())
    } else {
        Err(format!("value {val} is not a string value"))
    }
}

fn convert_status(s: &str) -> Result<i64, String> {
    match s.trim().to_lowercase().as_str() {
        "completed" | "0" => Ok(0),
        "failed" | "1" => Ok(1),
        "canceled" | "2" => Ok(2),
        "terminated" | "3" => Ok(3),
        "continuedasnew" | "continued_as_new" | "4" => Ok(4),
        "timedout" | "timed_out" | "5" => Ok(5),
        other => Err(format!("unknown workflow close status: {other}")),
    }
}

fn convert_to_timestamp(val: &str, quoted: bool) -> Result<i64, String> {
    if !quoted {
        if let Ok(n) = val.parse::<i64>() {
            return Ok(n);
        }
    }
    let s = extract_string(val, quoted)?;
    if let Ok(n) = s.parse::<i64>() {
        return Ok(n);
    }
    let t = chrono::DateTime::parse_from_rfc3339(&s).map_err(|e| e.to_string())?;
    t.timestamp_nanos_opt()
        .ok_or_else(|| "timestamp out of range".to_string())
}

fn convert_close_time(ts: i64, op: &str, q: &mut ParsedQuery) -> Result<(), String> {
    match op {
        "=" => {
            convert_close_time(ts, ">=", q)?;
            convert_close_time(ts, "<=", q)?;
        }
        "<" => q.latest_close_time = q.latest_close_time.min(ts - 1),
        "<=" => q.latest_close_time = q.latest_close_time.min(ts),
        ">" => q.earliest_close_time = q.earliest_close_time.max(ts + 1),
        ">=" => q.earliest_close_time = q.earliest_close_time.max(ts),
        _ => return Err(format!("operator {op} is not supported for close time")),
    }
    Ok(())
}

fn apply_comparison(
    col: &str,
    op: &str,
    val: &str,
    quoted: bool,
    q: &mut ParsedQuery,
) -> Result<(), String> {
    let only_eq = |name: &str| {
        if op == "=" {
            Ok(())
        } else {
            Err(format!(
                "only operator = is supported for {name} with file system"
            ))
        }
    };
    fn set(slot: &mut Option<String>, empty: &mut bool, v: String) {
        if slot.as_ref().is_some_and(|c| *c != v) {
            *empty = true;
        } else {
            *slot = Some(v);
        }
    }
    match col {
        "WorkflowID" => {
            let v = extract_string(val, quoted)?;
            only_eq(col)?;
            set(&mut q.workflow_id, &mut q.empty_result, v);
        }
        "RunID" => {
            let v = extract_string(val, quoted)?;
            only_eq(col)?;
            set(&mut q.run_id, &mut q.empty_result, v);
        }
        "WorkflowType" => {
            let v = extract_string(val, quoted)?;
            only_eq(col)?;
            set(&mut q.workflow_type_name, &mut q.empty_result, v);
        }
        "CloseStatus" => {
            let v = extract_string(val, quoted).unwrap_or_else(|_| render_val(val, quoted));
            only_eq(col)?;
            let status = convert_status(&v)?;
            if q.close_status.is_some_and(|c| c != status) {
                q.empty_result = true;
            } else {
                q.close_status = Some(status);
            }
        }
        "CloseTime" => {
            let ts = convert_to_timestamp(&render_val(val, quoted), quoted)
                .or_else(|_| convert_to_timestamp(val, quoted))?;
            convert_close_time(ts, op, q)?;
        }
        _ => return Err(format!("unknown filter name: {col}")),
    }
    Ok(())
}

fn parse_query(query: &str) -> Result<ParsedQuery, String> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_nanos()).unwrap_or(i64::MAX));
    let mut parsed = ParsedQuery {
        earliest_close_time: 0,
        latest_close_time: now,
        ..Default::default()
    };
    let toks = tokenize(query)?;
    if toks.is_empty() {
        return Err("syntax error at position 1".into());
    }
    let mut parser = Parser { toks, pos: 0 };
    parser.expr(&mut parsed)?;
    if parser.pos != parser.toks.len() {
        return Err("syntax error".into());
    }
    Ok(parsed)
}

fn match_query(r: &VisibilityRecord, q: &ParsedQuery) -> bool {
    if r.close_timestamp < q.earliest_close_time || r.close_timestamp > q.latest_close_time {
        return false;
    }
    if q.workflow_id.as_ref().is_some_and(|v| *v != r.workflow_id) {
        return false;
    }
    if q.run_id.as_ref().is_some_and(|v| *v != r.run_id) {
        return false;
    }
    if q.workflow_type_name
        .as_ref()
        .is_some_and(|v| *v != r.workflow_type_name)
    {
        return false;
    }
    if let Some(status) = q.close_status {
        if close_status_ordinal(&r.close_status) != Some(status) {
            return false;
        }
    }
    true
}

/// Go `sortAndFilterFiles`: close time desc, hashed run id desc as tie-break,
/// then only names strictly after the token position.
fn sort_and_filter_files(
    names: Vec<String>,
    token: Option<&QueryVisibilityToken>,
) -> Result<Vec<String>, String> {
    let mut parsed = Vec::new();
    for name in names {
        let pieces: Vec<&str> = name.split(['_', '.']).filter(|p| !p.is_empty()).collect();
        let fail = || format!("failed to parse visibility filename {name}");
        if pieces.len() != 3 {
            return Err(fail());
        }
        let close_time: i64 = pieces[0].parse().map_err(|_| fail())?;
        parsed.push((close_time, pieces[1].to_owned(), name.clone()));
    }
    parsed.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| b.1.cmp(&a.1)));
    let start = match token {
        None => 0,
        Some(t) => {
            let last_hash = fs_hash(&t.last_run_id);
            parsed
                .iter()
                .position(|p| {
                    if p.0 == t.last_close_time {
                        p.1.as_str() < last_hash.as_str()
                    } else {
                        p.0 < t.last_close_time
                    }
                })
                .unwrap_or(parsed.len())
        }
    };
    Ok(parsed.into_iter().skip(start).map(|p| p.2).collect())
}

/// Go filestore `visibilityArchiver.Query`.
pub fn filestore_query_visibility(
    uri: &ArchiverUri,
    request: &QueryVisibilityRequest,
) -> Result<QueryVisibilityResponse, ArchiverError> {
    if filestore_validate_uri(uri).is_err() {
        return Err(ArchiverError::BadRequest(ERR_INVALID_URI.into()));
    }
    if request.domain_id.is_empty() || request.page_size == 0 || request.query.is_empty() {
        return Err(ArchiverError::BadRequest(
            ERR_INVALID_QUERY_VISIBILITY_REQUEST.into(),
        ));
    }
    let parsed = parse_query(&request.query).map_err(ArchiverError::BadRequest)?;
    if parsed.empty_result {
        return Ok(QueryVisibilityResponse::default());
    }
    let token = if request.next_page_token.is_empty() {
        None
    } else {
        Some(
            serde_json::from_slice::<QueryVisibilityToken>(&request.next_page_token)
                .map_err(|_| ArchiverError::BadRequest(ERR_NEXT_PAGE_TOKEN_CORRUPTED.into()))?,
        )
    };
    let dir = Path::new(&uri.path).join(&request.domain_id);
    let dir_str = dir.to_string_lossy().into_owned();
    if !directory_exists(&dir_str)? {
        return Ok(QueryVisibilityResponse::default());
    }
    let files = sort_and_filter_files(list_files(&dir_str)?, token.as_ref())
        .map_err(ArchiverError::Internal)?;
    let mut response = QueryVisibilityResponse::default();
    for file in files {
        let data =
            std::fs::read(dir.join(&file)).map_err(|e| ArchiverError::Internal(e.to_string()))?;
        let record: VisibilityRecord =
            serde_json::from_slice(&data).map_err(|e| ArchiverError::Internal(e.to_string()))?;
        if record.close_timestamp < parsed.earliest_close_time {
            break;
        }
        if match_query(&record, &parsed) {
            let last = (record.close_timestamp, record.run_id.clone());
            response.records.push(record);
            if response.records.len() as i64 == i64::from(request.page_size) {
                // Go's `idx != len(files)` is always true: a full page always
                // carries a token.
                response.next_page_token = serde_json::to_vec(&QueryVisibilityToken {
                    last_close_time: last.0,
                    last_run_id: last.1,
                })
                .map_err(|e| ArchiverError::Internal(e.to_string()))?;
                break;
            }
        }
    }
    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(name: &str) -> std::path::PathBuf {
        let d =
            std::env::temp_dir().join(format!("cadence-archiver-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    fn provider() -> ArchiverProvider {
        ArchiverProvider::new(["filestore".to_owned(), "s3store".to_owned()])
    }

    #[test]
    fn uri_parse_matches_go_request_uri() {
        let u = parse_uri("file:///tmp/cadence_archival/development").unwrap();
        assert_eq!(u.scheme, "file");
        assert_eq!(u.path, "/tmp/cadence_archival/development");
        let u = parse_uri("S3://bucket:80/a%20b?x=1").unwrap();
        assert_eq!(
            (u.scheme.as_str(), u.host.as_str(), u.path.as_str()),
            ("s3", "bucket", "/a b")
        );
        assert_eq!(parse_uri("").unwrap_err(), "parse \"\": empty url");
        assert_eq!(
            parse_uri("nope").unwrap_err(),
            "parse \"nope\": invalid URI for request"
        );
        assert_eq!(
            parse_uri(":x").unwrap_err(),
            "parse \":x\": missing protocol scheme"
        );
        assert_eq!(parse_uri("mailto:a@b").unwrap().opaque, "a@b");
        assert!(parse_uri("/rooted").unwrap().scheme.is_empty());
    }

    #[test]
    fn provider_errors_follow_go_wording() {
        let p = provider();
        assert_eq!(
            p.validate_history_uri("http://x/y").unwrap_err().message(),
            "no history archiver constructor for scheme \"http\""
        );
        assert_eq!(
            p.validate_visibility_uri("gs://b/p").unwrap_err().message(),
            "no visibility archiver config for scheme \"gs\", config key \"gstorage\""
        );
        assert!(p
            .validate_history_uri("s3://b/p")
            .unwrap_err()
            .message()
            .starts_with(ERR_NOT_SUPPORTED));
        assert!(p
            .validate_history_uri("file:///tmp/does-not-exist-xyz")
            .is_ok());
        let d = tmp("valid");
        let f = d.join("f");
        std::fs::write(&f, b"x").unwrap();
        assert_eq!(
            p.validate_history_uri(&format!("file://{}", f.display()))
                .unwrap_err()
                .message(),
            ERR_DIRECTORY_EXPECTED
        );
    }

    #[test]
    fn hash_and_filenames_use_farm_fingerprint64() {
        let h = fs_hash("");
        assert_eq!(h, farmhash::fingerprint64(b"").to_string());
        assert!(history_filename("d", "w", "r", 3).ends_with("_3.history"));
        assert!(visibility_filename(7, "r").starts_with("7_"));
    }

    #[test]
    fn history_get_paginates_and_picks_highest_version() {
        let d = tmp("history");
        let uri = format!("file://{}", d.display());
        let batches = serde_json::json!([
            {"events": [{"eventId": 1}, {"eventId": 2}]},
            {"events": [{"eventId": 3}]},
            {"events": [{"eventId": 4}]}
        ]);
        std::fs::write(d.join(history_filename("dom", "wf", "run", 1)), b"[]").unwrap();
        std::fs::write(
            d.join(history_filename("dom", "wf", "run", 5)),
            serde_json::to_vec(&batches).unwrap(),
        )
        .unwrap();
        let p = provider();
        let mut req = GetHistoryRequest {
            domain_id: "dom".into(),
            workflow_id: "wf".into(),
            run_id: "run".into(),
            page_size: 2,
            ..Default::default()
        };
        let first = p.get_history(&uri, &req).unwrap();
        assert_eq!(first.history_batches.len(), 1);
        assert!(!first.next_page_token.is_empty());
        req.next_page_token = first.next_page_token;
        let second = p.get_history(&uri, &req).unwrap();
        assert_eq!(second.history_batches.len(), 2);
        assert!(second.next_page_token.is_empty());

        req.next_page_token = b"garbage".to_vec();
        assert_eq!(
            p.get_history(&uri, &req).unwrap_err(),
            ArchiverError::BadRequest(ERR_NEXT_PAGE_TOKEN_CORRUPTED.into())
        );
        req.next_page_token.clear();
        req.run_id = "other".into();
        assert_eq!(
            p.get_history(&uri, &req).unwrap_err(),
            ArchiverError::Internal(ERR_HISTORY_NOT_EXIST.into())
        );
        req.close_failover_version = Some(9);
        assert_eq!(
            p.get_history(&uri, &req).unwrap_err(),
            ArchiverError::EntityNotExists(ERR_HISTORY_NOT_EXIST.into())
        );
        req.page_size = 0;
        assert_eq!(
            p.get_history(&uri, &req).unwrap_err(),
            ArchiverError::BadRequest(ERR_INVALID_GET_HISTORY_REQUEST.into())
        );
        assert_eq!(
            p.get_history(
                "file:///nonexistent-archive-dir",
                &GetHistoryRequest {
                    domain_id: "d".into(),
                    workflow_id: "w".into(),
                    run_id: "r".into(),
                    page_size: 1,
                    ..Default::default()
                }
            )
            .unwrap_err(),
            ArchiverError::BadRequest(ERR_HISTORY_NOT_EXIST.into())
        );
    }

    #[test]
    fn query_parser_follows_go_rules() {
        let q = parse_query("WorkflowType = 'a' and (CloseStatus = 'completed' and CloseTime > 5)")
            .unwrap();
        assert_eq!(q.workflow_type_name.as_deref(), Some("a"));
        assert_eq!(q.close_status, Some(0));
        assert_eq!(q.earliest_close_time, 6);
        assert!(
            parse_query("WorkflowID = 'a' and WorkflowID = 'b'")
                .unwrap()
                .empty_result
        );
        assert_eq!(
            parse_query("WorkflowID = 'a' or RunID = 'b'").unwrap_err(),
            "only comparison and \"and\" expression is supported"
        );
        assert_eq!(
            parse_query("WorkflowID > 'a'").unwrap_err(),
            "only operator = is supported for WorkflowID with file system"
        );
        assert_eq!(
            parse_query("Bogus = 'a'").unwrap_err(),
            "unknown filter name: Bogus"
        );
        assert_eq!(
            parse_query("WorkflowID = 5").unwrap_err(),
            "value 5 is not a string value"
        );
        assert_eq!(
            parse_query("CloseStatus = 'weird'").unwrap_err(),
            "unknown workflow close status: weird"
        );
        assert!(!parse_query("CloseTime < 'x'").unwrap_err().is_empty());
        let t = parse_query("CloseTime = '2020-01-01T00:00:00Z'").unwrap();
        assert_eq!(t.earliest_close_time, 1_577_836_800_000_000_000);
        assert_eq!(t.latest_close_time, 1_577_836_800_000_000_000);
    }

    fn write_record(dir: &Path, close: i64, run: &str, wf_type: &str, status: &str) {
        let rec = serde_json::json!({
            "DomainID": "dom", "WorkflowID": "wf", "RunID": run,
            "WorkflowTypeName": wf_type, "StartTimestamp": close - 1,
            "ExecutionTimestamp": 0, "CloseTimestamp": close,
            "CloseStatus": status, "HistoryLength": 3, "Memo": null,
            "SearchAttributes": {"k": "v"}
        });
        std::fs::write(
            dir.join(visibility_filename(close, run)),
            serde_json::to_vec(&rec).unwrap(),
        )
        .unwrap();
    }

    #[test]
    fn visibility_query_orders_filters_and_pages() {
        let d = tmp("vis");
        let dom = d.join("dom");
        std::fs::create_dir_all(&dom).unwrap();
        write_record(&dom, 100, "r1", "a", "COMPLETED");
        write_record(&dom, 200, "r2", "b", "FAILED");
        write_record(&dom, 300, "r3", "a", "COMPLETED");
        let uri = format!("file://{}", d.display());
        let p = provider();
        let mut req = QueryVisibilityRequest {
            domain_id: "dom".into(),
            page_size: 2,
            query: "CloseTime >= 1".into(),
            ..Default::default()
        };
        let first = p.query_visibility(&uri, &req).unwrap();
        let runs: Vec<_> = first.records.iter().map(|r| r.run_id.as_str()).collect();
        assert_eq!(runs, ["r3", "r2"]);
        assert!(!first.next_page_token.is_empty());
        req.next_page_token = first.next_page_token;
        let second = p.query_visibility(&uri, &req).unwrap();
        assert_eq!(second.records.len(), 1);
        assert_eq!(second.records[0].run_id, "r1");
        assert!(second.next_page_token.is_empty());

        req.next_page_token.clear();
        req.query = "WorkflowType = 'a' and CloseStatus = 'completed'".into();
        req.page_size = 10;
        assert_eq!(p.query_visibility(&uri, &req).unwrap().records.len(), 2);
        req.query = "CloseStatus = 'failed'".into();
        assert_eq!(
            p.query_visibility(&uri, &req).unwrap().records[0].run_id,
            "r2"
        );
        req.domain_id = "missing".into();
        assert!(p.query_visibility(&uri, &req).unwrap().records.is_empty());
        req.query = String::new();
        assert_eq!(
            p.query_visibility(&uri, &req).unwrap_err(),
            ArchiverError::BadRequest(ERR_INVALID_QUERY_VISIBILITY_REQUEST.into())
        );
    }
}
