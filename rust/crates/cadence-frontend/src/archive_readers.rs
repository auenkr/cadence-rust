#![allow(clippy::result_large_err)]
//! Frontend archival readers: Go `ArchivalMetadata`, `historyArchived` /
//! `getArchivedHistory` (`service/frontend/api/handler.go`) and
//! `ListArchivedWorkflowExecutions` (`api/list_workflow_handlers.go`).

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use cadence_api::enums::ArchivalStatus;
use cadence_api::errors::{BadRequestError, CadenceError, InternalServiceError};
use cadence_api::service_types as st;
use cadence_common::archiver::{
    close_status_ordinal, ArchiverError, ArchiverProvider, GetHistoryRequest,
    QueryVisibilityRequest,
};
use serde_json::{json, Value};

use crate::handler_workflow::{ArchivedHistoryReader, ArchivedVisibilityReader, WorkflowFuture};

/// Go `validate.ErrHistoryNotFound`.
pub const HISTORY_NOT_FOUND: &str =
    "Requested workflow history not found, may have passed retention period.";

/// Go `archiver.ArchivalStatus` for the cluster-level setting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterArchivalStatus {
    Disabled,
    Paused,
    Enabled,
}

/// Go `getClusterArchivalStatus`.
pub fn parse_cluster_status(s: &str) -> Result<ClusterArchivalStatus, String> {
    match s.trim().to_lowercase().as_str() {
        "" | "disabled" => Ok(ClusterArchivalStatus::Disabled),
        "paused" => Ok(ClusterArchivalStatus::Paused),
        "enabled" => Ok(ClusterArchivalStatus::Enabled),
        other => Err(format!(
            "invalid archival status of {other} for cluster, valid status are: {{\"\", \"disabled\", \"paused\", \"enabled\"}}"
        )),
    }
}

/// Go `archiver.ArchivalConfig`.
#[derive(Clone)]
pub struct ArchivalConfig {
    static_status: ClusterArchivalStatus,
    dynamic_status: Arc<dyn Fn() -> String + Send + Sync>,
    static_enable_read: bool,
    dynamic_enable_read: Arc<dyn Fn() -> bool + Send + Sync>,
}

impl ArchivalConfig {
    pub fn new(
        static_status: &str,
        dynamic_status: Arc<dyn Fn() -> String + Send + Sync>,
        static_enable_read: bool,
        dynamic_enable_read: Arc<dyn Fn() -> bool + Send + Sync>,
    ) -> Result<Self, String> {
        Ok(Self {
            static_status: parse_cluster_status(static_status)?,
            dynamic_status,
            static_enable_read,
            dynamic_enable_read,
        })
    }

    pub fn cluster_status(&self) -> ClusterArchivalStatus {
        if self.static_status != ClusterArchivalStatus::Enabled {
            return self.static_status;
        }
        parse_cluster_status(&(self.dynamic_status)()).unwrap_or(ClusterArchivalStatus::Disabled)
    }

    pub fn cluster_configured_for_archival(&self) -> bool {
        self.cluster_status() == ClusterArchivalStatus::Enabled
    }

    pub fn read_enabled(&self) -> bool {
        self.cluster_configured_for_archival()
            && self.static_enable_read
            && (self.dynamic_enable_read)()
    }
}

/// Static `archival:` section of the service config
/// (`archiverfx.Archival`): status/enableRead and the provider plugin keys.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ArchivalSection {
    pub history_status: String,
    pub history_enable_read: bool,
    pub visibility_status: String,
    pub visibility_enable_read: bool,
    pub provider_keys: Vec<String>,
}

impl ArchivalSection {
    pub fn from_yaml(value: Option<&serde_yaml::Value>) -> Self {
        let mut out = Self::default();
        let Some(root) = value else { return out };
        let text = |v: &serde_yaml::Value, k: &str| {
            v.get(k)
                .and_then(serde_yaml::Value::as_str)
                .unwrap_or_default()
                .to_owned()
        };
        let flag = |v: &serde_yaml::Value| {
            v.get("enableRead")
                .and_then(serde_yaml::Value::as_bool)
                .unwrap_or(false)
        };
        if let Some(h) = root.get("history") {
            out.history_status = text(h, "status");
            out.history_enable_read = flag(h);
        }
        if let Some(v) = root.get("visibility") {
            out.visibility_status = text(v, "status");
            out.visibility_enable_read = flag(v);
        }
        // Go decodes history and visibility providers separately, each only
        // enabling its own plugins; the Frontend provider is the union here.
        for kind in ["history", "visibility"] {
            if let Some(map) = root
                .get(kind)
                .and_then(|k| k.get("provider"))
                .and_then(serde_yaml::Value::as_mapping)
            {
                for key in map.keys().filter_map(serde_yaml::Value::as_str) {
                    if !out.provider_keys.iter().any(|k| k == key) {
                        out.provider_keys.push(key.to_owned());
                    }
                }
            }
        }
        out
    }
}

/// Domain archival configuration access (Go domain cache entry config).
pub trait ArchivalDomainConfig: Send + Sync {
    /// Returns (history status, history URI, visibility status, visibility URI).
    fn archival(&self, domain_id: &str) -> Result<DomainArchival, CadenceError>;
}

#[derive(Debug, Clone, Default)]
pub struct DomainArchival {
    pub history_status: Option<ArchivalStatus>,
    pub history_uri: String,
    pub visibility_status: Option<ArchivalStatus>,
    pub visibility_uri: String,
}

pub struct PersistenceArchivalDomainConfig {
    manager: Arc<dyn cadence_persistence::FrontendDomainManager>,
}

impl PersistenceArchivalDomainConfig {
    pub fn new(manager: Arc<dyn cadence_persistence::FrontendDomainManager>) -> Self {
        Self { manager }
    }
}

impl ArchivalDomainConfig for PersistenceArchivalDomainConfig {
    fn archival(&self, domain_id: &str) -> Result<DomainArchival, CadenceError> {
        let described = self
            .manager
            .describe(&st::DescribeDomainRequest {
                uuid: Some(domain_id.to_owned()),
                ..Default::default()
            })
            .map_err(CadenceError::from)?;
        let c = described.configuration.unwrap_or_default();
        Ok(DomainArchival {
            history_status: c.history_archival_status,
            history_uri: c.history_archival_uri,
            visibility_status: c.visibility_archival_status,
            visibility_uri: c.visibility_archival_uri,
        })
    }
}

/// Go `historyArchived` probe: does History know the execution?
pub trait ExecutionProbe: Send + Sync {
    fn exists<'a>(
        &'a self,
        domain_id: &'a str,
        workflow_id: &'a str,
        run_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<bool, CadenceError>> + Send + 'a>>;
}

fn map_archiver_error(e: ArchiverError) -> CadenceError {
    match e {
        ArchiverError::BadRequest(m) => BadRequestError::new(m).into(),
        ArchiverError::EntityNotExists(m) => {
            cadence_api::errors::EntityNotExistsError::new(m).into()
        }
        ArchiverError::Internal(m) | ArchiverError::Other(m) => InternalServiceError::new(m).into(),
    }
}

fn decode_token(v: Option<&Value>) -> Result<Vec<u8>, CadenceError> {
    use base64::Engine;
    match v.and_then(Value::as_str) {
        None | Some("") => Ok(Vec::new()),
        Some(s) => base64::engine::general_purpose::STANDARD
            .decode(s)
            .map_err(|e| BadRequestError::new(e.to_string()).into()),
    }
}

fn b64_token(token: &[u8]) -> Value {
    if token.is_empty() {
        Value::Null
    } else {
        Value::String(cadence_api::b64::encode_bytes(token))
    }
}

pub struct ArchivedHistory {
    config: ArchivalConfig,
    provider: ArchiverProvider,
    domains: Arc<dyn ArchivalDomainConfig>,
    probe: Arc<dyn ExecutionProbe>,
}

impl ArchivedHistory {
    pub fn new(
        config: ArchivalConfig,
        provider: ArchiverProvider,
        domains: Arc<dyn ArchivalDomainConfig>,
        probe: Arc<dyn ExecutionProbe>,
    ) -> Self {
        Self {
            config,
            provider,
            domains,
            probe,
        }
    }

    async fn get(&self, domain_id: String, request: Value) -> Result<Option<Value>, CadenceError> {
        if !self.config.read_enabled() {
            return Ok(None);
        }
        let field = |v: &Value, k: &str| {
            v.get(k)
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned()
        };
        let exec = request.get("execution").cloned().unwrap_or(Value::Null);
        let workflow_id = field(&exec, "workflowId");
        let run_id = field(&exec, "runId");
        if run_id.is_empty() {
            return Ok(None);
        }
        let token = decode_token(request.get("nextPageToken"))?;
        let page_size = request
            .get("maximumPageSize")
            .and_then(Value::as_i64)
            .and_then(|n| i32::try_from(n).ok())
            .unwrap_or_default();
        // Only an EntityNotExists from History means "archived".
        match self.probe.exists(&domain_id, &workflow_id, &run_id).await {
            Ok(false) => {}
            _ => return Ok(None),
        }
        let archival = self.domains.archival(&domain_id)?;
        if archival.history_uri.is_empty() {
            return Err(BadRequestError::new(HISTORY_NOT_FOUND).into());
        }
        let response = self
            .provider
            .get_history(
                &archival.history_uri,
                &GetHistoryRequest {
                    domain_id,
                    workflow_id,
                    run_id,
                    close_failover_version: None,
                    next_page_token: token,
                    page_size,
                },
            )
            .map_err(map_archiver_error)?;
        let mut events = Vec::new();
        for batch in response.history_batches {
            if let Some(list) = batch.get("events").and_then(Value::as_array) {
                events.extend(list.iter().cloned());
            }
        }
        Ok(Some(json!({
            "history": {"events": events},
            "nextPageToken": b64_token(&response.next_page_token),
            "archived": true,
        })))
    }
}

impl ArchivedHistoryReader for ArchivedHistory {
    fn get_if_archived<'a>(
        &'a self,
        domain_id: String,
        request: Value,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Value>, CadenceError>> + Send + 'a>> {
        Box::pin(self.get(domain_id, request))
    }
}

pub struct ArchivedVisibility {
    config: ArchivalConfig,
    provider: ArchiverProvider,
    domains: Arc<dyn ArchivalDomainConfig>,
}

impl ArchivedVisibility {
    pub fn new(
        config: ArchivalConfig,
        provider: ArchiverProvider,
        domains: Arc<dyn ArchivalDomainConfig>,
    ) -> Self {
        Self {
            config,
            provider,
            domains,
        }
    }

    async fn list(&self, request: Value) -> Result<Value, CadenceError> {
        if !self.config.cluster_configured_for_archival() {
            return Err(
                BadRequestError::new("Cluster is not configured for visibility archival").into(),
            );
        }
        if !self.config.read_enabled() {
            return Err(BadRequestError::new(
                "Cluster is not configured for reading archived visibility records",
            )
            .into());
        }
        let domain_id = request
            .get("domainUUID")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        let archival = self.domains.archival(&domain_id)?;
        if archival.visibility_status != Some(ArchivalStatus::Enabled) {
            return Err(
                BadRequestError::new("Domain is not configured for visibility archival").into(),
            );
        }
        let token = decode_token(request.get("nextPageToken"))?;
        let response = self
            .provider
            .query_visibility(
                &archival.visibility_uri,
                &QueryVisibilityRequest {
                    domain_id,
                    page_size: request
                        .get("pageSize")
                        .and_then(Value::as_i64)
                        .and_then(|n| i32::try_from(n).ok())
                        .unwrap_or_default(),
                    next_page_token: token,
                    query: request
                        .get("query")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_owned(),
                },
            )
            .map_err(map_archiver_error)?;
        let executions: Vec<Value> = response
            .records
            .into_iter()
            .map(|r| {
                let status = match close_status_ordinal(&r.close_status) {
                    Some(0) => json!(0),
                    Some(1) => json!(1),
                    Some(2) => json!(2),
                    Some(3) => json!(3),
                    Some(4) => json!(4),
                    Some(5) => json!(5),
                    _ => Value::Null,
                };
                let indexed: serde_json::Map<String, Value> = r
                    .search_attributes
                    .unwrap_or_default()
                    .into_iter()
                    .map(|(k, v)| {
                        (
                            k,
                            Value::String(cadence_api::b64::encode_bytes(v.as_bytes())),
                        )
                    })
                    .collect();
                let mut info = json!({
                    "execution": {"workflowId": r.workflow_id, "runId": r.run_id},
                    "type": {"name": r.workflow_type_name},
                    "startTime": r.start_timestamp,
                    "executionTime": r.execution_timestamp,
                    "closeTime": r.close_timestamp,
                    "closeStatus": status,
                    "historyLength": r.history_length,
                    "searchAttributes": {"indexedFields": indexed},
                });
                if !r.memo.is_null() {
                    info["memo"] = r.memo;
                }
                info
            })
            .collect();
        Ok(json!({
            "executions": executions,
            "nextPageToken": b64_token(&response.next_page_token),
        }))
    }
}

impl ArchivedVisibilityReader for ArchivedVisibility {
    fn list_archived<'a>(&'a self, request: Value) -> WorkflowFuture<'a> {
        Box::pin(self.list(request))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Domains(DomainArchival);
    impl ArchivalDomainConfig for Domains {
        fn archival(&self, _: &str) -> Result<DomainArchival, CadenceError> {
            Ok(self.0.clone())
        }
    }

    struct Probe(bool);
    impl ExecutionProbe for Probe {
        fn exists<'a>(
            &'a self,
            _: &'a str,
            _: &'a str,
            _: &'a str,
        ) -> Pin<Box<dyn Future<Output = Result<bool, CadenceError>> + Send + 'a>> {
            let v = self.0;
            Box::pin(async move { Ok(v) })
        }
    }

    fn cfg(status: &str, read: bool) -> ArchivalConfig {
        ArchivalConfig::new(
            status,
            Arc::new(|| "enabled".to_owned()),
            read,
            Arc::new(|| true),
        )
        .unwrap()
    }

    fn provider() -> ArchiverProvider {
        ArchiverProvider::new(["filestore".to_owned()])
    }

    fn tmp(name: &str) -> std::path::PathBuf {
        let d = std::env::temp_dir().join(format!("cadence-readers-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    #[test]
    fn cluster_status_parsing_and_gating() {
        assert!(parse_cluster_status("bogus")
            .unwrap_err()
            .starts_with("invalid archival status of bogus for cluster"));
        assert!(cfg("enabled", true).read_enabled());
        assert!(!cfg("enabled", false).read_enabled());
        assert!(!cfg("paused", true).cluster_configured_for_archival());
        let dyn_off = ArchivalConfig::new(
            "enabled",
            Arc::new(|| "disabled".into()),
            true,
            Arc::new(|| true),
        )
        .unwrap();
        assert!(!dyn_off.cluster_configured_for_archival());
    }

    #[test]
    fn section_parses_go_archival_yaml() {
        let y: serde_yaml::Value = serde_yaml::from_str(
            "history: {status: enabled, enableRead: true, provider: {filestore: {fileMode: '0666'}}}\nvisibility: {status: enabled, enableRead: true, provider: {gstorage: {}}}",
        )
        .unwrap();
        let s = ArchivalSection::from_yaml(Some(&y));
        assert_eq!(s.history_status, "enabled");
        assert!(s.visibility_enable_read);
        assert_eq!(s.provider_keys, ["filestore", "gstorage"]);
        assert_eq!(ArchivalSection::from_yaml(None), ArchivalSection::default());
    }

    #[tokio::test]
    async fn history_falls_back_to_archive_only_when_not_found() {
        let d = tmp("hist");
        let name = cadence_common::archiver::history_filename("dom", "wf", "run", 2);
        std::fs::write(
            d.join(name),
            br#"[{"events":[{"eventId":1},{"eventId":2}]},{"events":[{"eventId":3}]}]"#,
        )
        .unwrap();
        let archival = DomainArchival {
            history_status: Some(ArchivalStatus::Enabled),
            history_uri: format!("file://{}", d.display()),
            ..Default::default()
        };
        let reader = |exists: bool, uri: &str| {
            ArchivedHistory::new(
                cfg("enabled", true),
                provider(),
                Arc::new(Domains(DomainArchival {
                    history_uri: uri.to_owned(),
                    ..archival.clone()
                })),
                Arc::new(Probe(exists)),
            )
        };
        let req = json!({"domain":"d","execution":{"workflowId":"wf","runId":"run"},"maximumPageSize":10});
        let r = reader(false, &archival.history_uri);
        let resp = r.get("dom".into(), req.clone()).await.unwrap().unwrap();
        assert_eq!(resp["archived"], true);
        assert_eq!(resp["history"]["events"].as_array().unwrap().len(), 3);
        assert!(resp["nextPageToken"].is_null());
        // live workflow: no archive read
        assert!(reader(true, &archival.history_uri)
            .get("dom".into(), req.clone())
            .await
            .unwrap()
            .is_none());
        // no run id: no archive read
        let no_run = json!({"domain":"d","execution":{"workflowId":"wf"},"maximumPageSize":10});
        assert!(r.get("dom".into(), no_run).await.unwrap().is_none());
        // never archived domain
        let err = reader(false, "")
            .get("dom".into(), req.clone())
            .await
            .unwrap_err();
        assert_eq!(err.to_string(), HISTORY_NOT_FOUND);
        // read disabled
        let off = ArchivedHistory::new(
            cfg("enabled", false),
            provider(),
            Arc::new(Domains(archival.clone())),
            Arc::new(Probe(false)),
        );
        assert!(off.get("dom".into(), req).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn visibility_listing_gates_and_reads_filestore() {
        let d = tmp("vis");
        let dom = d.join("dom");
        std::fs::create_dir_all(&dom).unwrap();
        let rec = json!({"WorkflowID":"wf","RunID":"r1","WorkflowTypeName":"t","StartTimestamp":5,
            "ExecutionTimestamp":0,"CloseTimestamp":10,"CloseStatus":"COMPLETED","HistoryLength":4});
        std::fs::write(
            dom.join(cadence_common::archiver::visibility_filename(10, "r1")),
            serde_json::to_vec(&rec).unwrap(),
        )
        .unwrap();
        let archival = DomainArchival {
            visibility_status: Some(ArchivalStatus::Enabled),
            visibility_uri: format!("file://{}", d.display()),
            ..Default::default()
        };
        let mk = |c: ArchivalConfig, a: DomainArchival| {
            ArchivedVisibility::new(c, provider(), Arc::new(Domains(a)))
        };
        let req = json!({"domainUUID":"dom","pageSize":5,"nextPageToken":null,"query":"WorkflowType = 't'"});
        let resp = mk(cfg("enabled", true), archival.clone())
            .list(req.clone())
            .await
            .unwrap();
        assert_eq!(resp["executions"][0]["execution"]["runId"], "r1");
        assert_eq!(resp["executions"][0]["closeStatus"], 0);
        assert!(resp["nextPageToken"].is_null());
        let e = mk(cfg("disabled", true), archival.clone())
            .list(req.clone())
            .await
            .unwrap_err();
        assert_eq!(
            e.to_string(),
            "Cluster is not configured for visibility archival"
        );
        let e = mk(cfg("enabled", false), archival.clone())
            .list(req.clone())
            .await
            .unwrap_err();
        assert_eq!(
            e.to_string(),
            "Cluster is not configured for reading archived visibility records"
        );
        let e = mk(cfg("enabled", true), DomainArchival::default())
            .list(req)
            .await
            .unwrap_err();
        assert_eq!(
            e.to_string(),
            "Domain is not configured for visibility archival"
        );
    }
}
