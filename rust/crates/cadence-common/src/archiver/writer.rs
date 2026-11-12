//! Write side of Go `common/archiver/filestore` (`historyArchiver.Archive`,
//! `visibilityArchiver.Archive`). Same on-disk format as Go so Go readers can read the files:
//! history = JSON array of `types.History` batches in `<hash(domain)><hash(wf)><hash(run)>_<version>.history`,
//! visibility = JSON of `ArchiveVisibilityRequest` (Go field names, no tags) in
//! `<URI path>/<domainID>/<closeTimestamp>_<hash(runID)>.visibility`.

use std::collections::BTreeMap;
use std::io::Write;
use std::path::Path;

use serde::Serialize;
use serde_json::Value;

use super::{
    filestore_validate_uri, history_filename, visibility_filename, ArchiverError, ArchiverUri,
};

pub const ERR_INVALID_FILE_MODE: &str = "invalid file mode";
pub const ERR_INVALID_DIR_MODE: &str = "invalid dir mode";
pub const ERR_INVALID_ARCHIVE_REQUEST: &str = "archive request is invalid";

/// Go `strconv.ParseUint(s, 0, 32)` (base prefix 0x / 0o / 0b / leading 0 octal).
fn parse_mode(s: &str, err: &str) -> Result<u32, ArchiverError> {
    let bad = || ArchiverError::Other(err.to_owned());
    let t = s.replace('_', "");
    let (digits, radix) = if let Some(r) = t.strip_prefix("0x").or_else(|| t.strip_prefix("0X")) {
        (r.to_owned(), 16)
    } else if let Some(r) = t.strip_prefix("0b").or_else(|| t.strip_prefix("0B")) {
        (r.to_owned(), 2)
    } else if let Some(r) = t.strip_prefix("0o").or_else(|| t.strip_prefix("0O")) {
        (r.to_owned(), 8)
    } else if t.len() > 1 && t.starts_with('0') {
        (t[1..].to_owned(), 8)
    } else {
        (t.clone(), 10)
    };
    u32::from_str_radix(&digits, radix).map_err(|_| bad())
}

/// Go filestore `Config` (`fileMode` / `dirMode`).
#[derive(Debug, Clone)]
pub struct FilestoreWriterConfig {
    pub file_mode: u32,
    pub dir_mode: u32,
}

impl FilestoreWriterConfig {
    pub fn new(file_mode: &str, dir_mode: &str) -> Result<Self, ArchiverError> {
        Ok(Self {
            file_mode: parse_mode(file_mode, ERR_INVALID_FILE_MODE)?,
            dir_mode: parse_mode(dir_mode, ERR_INVALID_DIR_MODE)?,
        })
    }
}

fn mkdir_all(path: &str, mode: u32) -> Result<(), ArchiverError> {
    let mut b = std::fs::DirBuilder::new();
    b.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        b.mode(mode);
    }
    #[cfg(not(unix))]
    let _ = mode;
    b.create(path).map_err(|e| ArchiverError::Other(e.to_string()))
}

/// Go `util.WriteFile`: remove the old file, create anew, write.
fn write_file(path: &Path, data: &[u8], mode: u32) -> Result<(), ArchiverError> {
    let io = |e: std::io::Error| ArchiverError::Other(e.to_string());
    match std::fs::remove_file(path) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(io(e)),
    }
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(mode);
    }
    #[cfg(not(unix))]
    let _ = mode;
    let mut f = opts.open(path).map_err(io)?;
    f.write_all(data).map_err(io)?;
    f.flush().map_err(io)
}

/// Go `archiver.ArchiveHistoryRequest` validation (`ValidateHistoryArchiveRequest`).
#[derive(Debug, Clone, Default)]
pub struct ArchiveHistoryRequest {
    pub domain_id: String,
    pub domain_name: String,
    pub workflow_id: String,
    pub run_id: String,
    pub branch_token: Vec<u8>,
    pub next_event_id: i64,
    pub close_failover_version: i64,
}

/// Source of history batches (Go `archiver.HistoryIterator` drained into one list). Each batch is a
/// Go `types.History` JSON object (`{"events":[...]}`). `Ok(None)` = history no longer exists
/// (Go `EntityNotExistsError`: the archive is skipped, not failed).
pub trait HistoryBatchSource {
    fn read_all(&self, request: &ArchiveHistoryRequest) -> Result<Option<Vec<Value>>, ArchiverError>;
}

pub(crate) fn validate_history_request(r: &ArchiveHistoryRequest) -> Result<(), ArchiverError> {
    let missing = |s: &str| s.is_empty();
    if missing(&r.domain_id)
        || missing(&r.domain_name)
        || missing(&r.workflow_id)
        || missing(&r.run_id)
    {
        return Err(ArchiverError::Other(ERR_INVALID_ARCHIVE_REQUEST.into()));
    }
    Ok(())
}

/// Go filestore `historyArchiver.Archive` with the batches already fetched via `source`.
pub fn filestore_archive_history(
    cfg: &FilestoreWriterConfig,
    uri: &ArchiverUri,
    request: &ArchiveHistoryRequest,
    source: &dyn HistoryBatchSource,
) -> Result<(), ArchiverError> {
    filestore_validate_uri(uri)?;
    validate_history_request(request)?;
    let Some(batches) = source.read_all(request)? else {
        return Ok(()); // history gone (duplicate archival signal): skipped like Go
    };
    let encoded = serde_json::to_vec(&batches).map_err(|e| ArchiverError::Other(e.to_string()))?;
    mkdir_all(&uri.path, cfg.dir_mode)?;
    let filename = history_filename(
        &request.domain_id,
        &request.workflow_id,
        &request.run_id,
        request.close_failover_version,
    );
    write_file(&Path::new(&uri.path).join(filename), &encoded, cfg.file_mode)
}

/// Go `archiver.ArchiveVisibilityRequest` (serialized with Go field names, like Go's tagless JSON).
#[derive(Debug, Clone, Default, Serialize)]
pub struct ArchiveVisibilityRequest {
    #[serde(rename = "DomainID")]
    pub domain_id: String,
    #[serde(rename = "DomainName")]
    pub domain_name: String,
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
    /// Go `CloseStatus` ordinal (marshalled as its text name).
    #[serde(rename = "CloseStatus", serialize_with = "ser_close_status")]
    pub close_status: i32,
    #[serde(rename = "HistoryLength")]
    pub history_length: i64,
    #[serde(rename = "Memo", serialize_with = "ser_memo")]
    pub memo: Option<BTreeMap<String, Vec<u8>>>,
    #[serde(rename = "SearchAttributes")]
    pub search_attributes: Option<BTreeMap<String, String>>,
    #[serde(rename = "HistoryArchivalURI")]
    pub history_archival_uri: String,
}

fn close_status_name(v: i32) -> String {
    match v {
        0 => "COMPLETED".into(),
        1 => "FAILED".into(),
        2 => "CANCELED".into(),
        3 => "TERMINATED".into(),
        4 => "CONTINUED_AS_NEW".into(),
        5 => "TIMED_OUT".into(),
        n => format!("WorkflowExecutionCloseStatus({n})"),
    }
}

fn ser_close_status<S: serde::Serializer>(v: &i32, s: S) -> Result<S::Ok, S::Error> {
    s.serialize_str(&close_status_name(*v))
}

fn base64_std(data: &[u8]) -> String {
    const T: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for c in data.chunks(3) {
        let n = (u32::from(c[0]) << 16)
            | (u32::from(*c.get(1).unwrap_or(&0)) << 8)
            | u32::from(*c.get(2).unwrap_or(&0));
        out.push(T[(n >> 18) as usize & 63] as char);
        out.push(T[(n >> 12) as usize & 63] as char);
        out.push(if c.len() > 1 { T[(n >> 6) as usize & 63] as char } else { '=' });
        out.push(if c.len() > 2 { T[n as usize & 63] as char } else { '=' });
    }
    out
}

/// Go `*types.Memo` (`{"fields":{k: base64}}`, `omitempty`; nil pointer -> `null`).
fn ser_memo<S: serde::Serializer>(
    v: &Option<BTreeMap<String, Vec<u8>>>,
    s: S,
) -> Result<S::Ok, S::Error> {
    use serde::ser::SerializeMap;
    match v {
        None => s.serialize_none(),
        Some(fields) => {
            let mut m = s.serialize_map(Some(usize::from(!fields.is_empty())))?;
            if !fields.is_empty() {
                let enc: BTreeMap<&String, String> =
                    fields.iter().map(|(k, b)| (k, base64_std(b))).collect();
                m.serialize_entry("fields", &enc)?;
            }
            m.end()
        }
    }
}

/// Go `ValidateVisibilityArchivalRequest`: DomainID, DomainName, WorkflowID, RunID, WorkflowTypeName,
/// StartTimestamp and CloseTimestamp are required.
pub(crate) fn validate_visibility_request(
    request: &ArchiveVisibilityRequest,
) -> Result<(), ArchiverError> {
    if request.domain_id.is_empty()
        || request.domain_name.is_empty()
        || request.workflow_id.is_empty()
        || request.run_id.is_empty()
        || request.workflow_type_name.is_empty()
        || request.start_timestamp == 0
        || request.close_timestamp == 0
    {
        return Err(ArchiverError::Other(ERR_INVALID_ARCHIVE_REQUEST.into()));
    }
    Ok(())
}

/// Go filestore `visibilityArchiver.Archive`.
pub fn filestore_archive_visibility(
    cfg: &FilestoreWriterConfig,
    uri: &ArchiverUri,
    request: &ArchiveVisibilityRequest,
) -> Result<(), ArchiverError> {
    filestore_validate_uri(uri)?;
    validate_visibility_request(request)?;
    let dir = Path::new(&uri.path).join(&request.domain_id);
    let dir_s = dir.to_string_lossy().into_owned();
    mkdir_all(&dir_s, cfg.dir_mode)?;
    let encoded = serde_json::to_vec(request).map_err(|e| ArchiverError::Other(e.to_string()))?;
    let filename = visibility_filename(request.close_timestamp, &request.run_id);
    write_file(&dir.join(filename), &encoded, cfg.file_mode)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::archiver::{
        filestore_get_history, filestore_query_visibility, parse_uri, GetHistoryRequest,
        QueryVisibilityRequest,
    };

    struct Src(Vec<Value>);
    impl HistoryBatchSource for Src {
        fn read_all(&self, _r: &ArchiveHistoryRequest) -> Result<Option<Vec<Value>>, ArchiverError> {
            Ok(Some(self.0.clone()))
        }
    }

    fn tmp(name: &str) -> String {
        let d = std::env::temp_dir().join(format!("archwriter-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        d.to_string_lossy().into_owned()
    }

    #[test]
    fn mode_parsing_like_go() {
        let c = FilestoreWriterConfig::new("0666", "0700").unwrap();
        assert_eq!((c.file_mode, c.dir_mode), (0o666, 0o700));
        assert!(FilestoreWriterConfig::new("zz", "0700").is_err());
        assert!(FilestoreWriterConfig::new("0666", "08").is_err());
    }

    #[test]
    fn history_roundtrip_with_reader() {
        let dir = tmp("hist");
        let uri = parse_uri(&format!("file://{dir}")).unwrap();
        let cfg = FilestoreWriterConfig::new("0666", "0700").unwrap();
        let req = ArchiveHistoryRequest {
            domain_id: "d".into(),
            domain_name: "dn".into(),
            workflow_id: "w".into(),
            run_id: "r".into(),
            branch_token: vec![1],
            next_event_id: 3,
            close_failover_version: 7,
        };
        let batches = vec![serde_json::json!({"events":[{"eventId":1},{"eventId":2}]})];
        filestore_archive_history(&cfg, &uri, &req, &Src(batches.clone())).unwrap();
        let got = filestore_get_history(
            &uri,
            &GetHistoryRequest {
                domain_id: "d".into(),
                workflow_id: "w".into(),
                run_id: "r".into(),
                close_failover_version: None,
                next_page_token: vec![],
                page_size: 10,
            },
        )
        .unwrap();
        assert_eq!(got.history_batches, batches);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn visibility_roundtrip_with_reader() {
        let dir = tmp("vis");
        let uri = parse_uri(&format!("file://{dir}")).unwrap();
        let cfg = FilestoreWriterConfig::new("0666", "0700").unwrap();
        let req = ArchiveVisibilityRequest {
            domain_id: "d".into(),
            domain_name: "dn".into(),
            workflow_id: "w".into(),
            run_id: "r".into(),
            workflow_type_name: "t".into(),
            start_timestamp: 50,
            close_timestamp: 100,
            close_status: 1,
            memo: Some(BTreeMap::from([("k".to_owned(), b"hi".to_vec())])),
            ..Default::default()
        };
        filestore_archive_visibility(&cfg, &uri, &req).unwrap();
        let got = filestore_query_visibility(
            &uri,
            &QueryVisibilityRequest {
                domain_id: "d".into(),
                page_size: 10,
                next_page_token: vec![],
                query: "WorkflowID = 'w'".into(),
            },
        )
        .unwrap();
        assert_eq!(got.records.len(), 1);
        assert_eq!(got.records[0].workflow_id, "w");
        assert_eq!(got.records[0].close_status, Value::String("FAILED".into()));
        assert_eq!(base64_std(b"hi"), "aGk=");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
