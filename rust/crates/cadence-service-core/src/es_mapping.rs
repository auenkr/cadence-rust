//! Elasticsearch/OpenSearch index-mapping update used by admin `AddSearchAttribute`.
//!
//! Mirrors Go `adminHandlerImpl.AddSearchAttribute` (service/frontend/admin/handler.go) and the
//! `PutMapping`/`CreateIndex` calls of `common/elasticsearch` (v6, v7, os2 clients):
//! - v6: `PUT /{index}/_mapping/_doc`; v7 and os2: `PUT /{index}/_mapping`
//! - body `{"properties":{"Attr":{"properties":{<key>:{"type":<es type>}}}}}`
//! - HTTP 404 on put-mapping => `PUT /{index}` (create) then put-mapping again
//! - the response must be `acknowledged: true`.

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;

use cadence_api::enums::IndexedValueType;
use serde_json::{json, Value};

use crate::config::ElasticSearchConfig;

/// Go `definition.Attr`.
pub const ATTR_ROOT: &str = "Attr";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EsError {
    /// HTTP 404 (Go `IsNotFoundError`).
    NotFound(String),
    Other(String),
}

impl std::fmt::Display for EsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EsError::NotFound(m) | EsError::Other(m) => f.write_str(m),
        }
    }
}

/// Subset of Go `GenericClient` used by `AddSearchAttribute`.
pub trait EsMappingClient: Send + Sync + Debug {
    fn put_mapping(&self, index: &str, key: &str, es_type: &str) -> Result<(), EsError>;
    fn create_index(&self, index: &str) -> Result<(), EsError>;
}

/// Client + visibility index (Go `ESClient` + `ESConfig.GetVisibilityIndex()`).
#[derive(Debug, Clone)]
pub struct EsVisibility {
    pub client: Arc<dyn EsMappingClient>,
    pub index: String,
}

/// Go `convertIndexedValueTypeToESDataType`.
#[must_use]
pub fn es_data_type(value_type: IndexedValueType) -> &'static str {
    match value_type {
        IndexedValueType::String => "text",
        IndexedValueType::Keyword => "keyword",
        IndexedValueType::Int => "long",
        IndexedValueType::Double => "double",
        IndexedValueType::Bool => "boolean",
        IndexedValueType::Datetime => "date",
        // Go's switch has no default arm and returns the empty string.
        _ => "",
    }
}

/// Go `buildPutMappingBody`.
#[must_use]
pub fn put_mapping_body(root: &str, key: &str, es_type: &str) -> Value {
    if root.is_empty() {
        json!({"properties": {key: {"type": es_type}}})
    } else {
        json!({"properties": {root: {"properties": {key: {"type": es_type}}}}})
    }
}

/// Apply Go's per-key loop: put mapping, on not-found create the index and put again.
pub fn apply_search_attributes(
    es: &EsVisibility,
    attrs: &HashMap<String, IndexedValueType>,
) -> Result<(), ApplyError> {
    for (key, value_type) in attrs {
        let es_type = es_data_type(*value_type);
        let mut result = es.client.put_mapping(&es.index, key, es_type);
        if matches!(result, Err(EsError::NotFound(_))) {
            es.client
                .create_index(&es.index)
                .map_err(|e| ApplyError::Internal(format!("Failed to create ES index, err: {e}")))?;
            result = es.client.put_mapping(&es.index, key, es_type);
        }
        result.map_err(|e| ApplyError::Internal(format!("Failed to update ES mapping, err: {e}")))?;
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApplyError {
    Internal(String),
}

/// Plain HTTP(S) client for v6/v7/os2 compatible endpoints (basic auth + custom headers).
/// Requests run on a short-lived thread so the call is safe from async contexts.
#[derive(Debug, Clone)]
pub struct HttpEsClient {
    base: String,
    version: String,
    username: String,
    password: String,
    headers: HashMap<String, String>,
}

impl HttpEsClient {
    /// Go `NewGenericClient`: empty version defaults to v6; unsupported versions are an error.
    pub fn from_config(cfg: &ElasticSearchConfig) -> Result<Self, String> {
        let version = if cfg.version.is_empty() { "v6" } else { cfg.version.as_str() };
        if !matches!(version, "v6" | "v7" | "os2") {
            return Err(format!("not supported ElasticSearch version: {version}"));
        }
        let scheme = if cfg.url.scheme.is_empty() { "http" } else { cfg.url.scheme.as_str() };
        Ok(Self {
            base: format!("{scheme}://{}{}", cfg.url.host, cfg.url.path.trim_end_matches('/')),
            version: version.to_owned(),
            username: cfg.username.clone(),
            password: cfg.password.clone(),
            headers: cfg.custom_headers.clone(),
        })
    }

    /// Generic JSON call used by the Frontend's search/scroll/count read path.
    /// Errors read like olivere's `elastic: Error <status> (<reason>): <body>`.
    pub fn raw(&self, method: &str, path: &str, body: Option<String>) -> Result<Value, String> {
        let this = self.clone();
        let (method, path) = (method.to_string(), path.to_string());
        std::thread::spawn(move || {
            let client = reqwest::blocking::Client::builder()
                .timeout(Duration::from_secs(60))
                .build()
                .map_err(|e| e.to_string())?;
            let m = reqwest::Method::from_bytes(method.as_bytes()).map_err(|e| e.to_string())?;
            let mut req = client.request(m, format!("{}{}", this.base, path));
            if !this.username.is_empty() {
                req = req.basic_auth(&this.username, Some(&this.password));
            }
            for (k, v) in &this.headers {
                req = req.header(k, v);
            }
            if let Some(b) = body {
                req = req.header("Content-Type", "application/json").body(b);
            }
            let resp = req.send().map_err(|e| e.to_string())?;
            let status = resp.status();
            let text = resp.text().unwrap_or_default();
            if !status.is_success() {
                return Err(format!(
                    "elastic: Error {} ({}): {text}",
                    status.as_u16(),
                    status.canonical_reason().unwrap_or("")
                ));
            }
            if text.trim().is_empty() {
                return Ok(Value::Null);
            }
            serde_json::from_str(&text).map_err(|e| e.to_string())
        })
        .join()
        .unwrap_or_else(|_| Err("elasticsearch request thread panicked".into()))
    }

    fn request(&self, path: String, body: Option<Value>) -> Result<(), EsError> {
        let this = self.clone();
        std::thread::spawn(move || {
            let client = reqwest::blocking::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .map_err(|e| EsError::Other(e.to_string()))?;
            let mut req = client.put(format!("{}{}", this.base, path));
            if !this.username.is_empty() {
                req = req.basic_auth(&this.username, Some(&this.password));
            }
            for (k, v) in &this.headers {
                req = req.header(k, v);
            }
            if let Some(body) = body {
                req = req.json(&body);
            }
            let resp = req.send().map_err(|e| EsError::Other(e.to_string()))?;
            let status = resp.status();
            let text = resp.text().unwrap_or_default();
            if status.as_u16() == 404 {
                return Err(EsError::NotFound(format!("elastic: Error 404 (Not Found): {text}")));
            }
            if !status.is_success() {
                return Err(EsError::Other(format!("elastic: Error {status}: {text}")));
            }
            let acknowledged = serde_json::from_str::<Value>(&text)
                .ok()
                .and_then(|v| v.get("acknowledged").and_then(Value::as_bool))
                .unwrap_or(false);
            if !acknowledged {
                return Err(EsError::Other(format!("request not acknowledged: {text}")));
            }
            Ok(())
        })
        .join()
        .unwrap_or_else(|_| Err(EsError::Other("elasticsearch request thread panicked".into())))
    }
}

impl EsMappingClient for HttpEsClient {
    fn put_mapping(&self, index: &str, key: &str, es_type: &str) -> Result<(), EsError> {
        let path = if self.version == "v6" {
            format!("/{index}/_mapping/_doc")
        } else {
            format!("/{index}/_mapping")
        };
        self.request(path, Some(put_mapping_body(ATTR_ROOT, key, es_type)))
    }

    fn create_index(&self, index: &str) -> Result<(), EsError> {
        self.request(format!("/{index}"), None)
    }
}

/// Go `setupESClient`/`setupOSClient` selection: `os-visibility` uses its own store, anything
/// else (including empty) reads `es-visibility`. Returns None when no advanced visibility store
/// is configured (Go: "Skip updating OpenSearch/ElasticSearch mapping").
pub fn from_persistence(
    persistence: &crate::config::PersistenceConfig,
) -> Result<Option<EsVisibility>, String> {
    if persistence.advanced_visibility_store.is_empty() {
        return Ok(None);
    }
    let key = if persistence.advanced_visibility_store == "os-visibility" {
        "os-visibility"
    } else {
        "es-visibility"
    };
    let cfg = persistence
        .datastores
        .get(key)
        .and_then(|d| d.elasticsearch.as_ref())
        .ok_or_else(|| format!("Cannot find {key} store with elasticsearch config"))?;
    let client = HttpEsClient::from_config(cfg)?;
    Ok(Some(EsVisibility {
        client: Arc::new(client),
        index: cfg.indices.get("visibility").cloned().unwrap_or_default(),
    }))
}

/// Read-path client + visibility index for the Frontend (same store selection
/// as `from_persistence`). Returns None when advanced visibility is not configured.
pub fn search_client_from_persistence(
    persistence: &crate::config::PersistenceConfig,
) -> Result<Option<(HttpEsClient, String)>, String> {
    if persistence.advanced_visibility_store.is_empty() {
        return Ok(None);
    }
    let key = if persistence.advanced_visibility_store == "os-visibility" {
        "os-visibility"
    } else {
        "es-visibility"
    };
    let cfg = persistence
        .datastores
        .get(key)
        .and_then(|d| d.elasticsearch.as_ref())
        .ok_or_else(|| format!("Cannot find {key} store with elasticsearch config"))?;
    Ok(Some((
        HttpEsClient::from_config(cfg)?,
        cfg.indices.get("visibility").cloned().unwrap_or_default(),
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::Mutex;

    /// Minimal fake ES: records "METHOD path body" and answers from a script keyed by request order.
    fn fake_server(responses: Vec<(u16, &'static str)>) -> (String, Arc<Mutex<Vec<String>>>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let seen2 = seen.clone();
        std::thread::spawn(move || {
            for (status, body) in responses {
                let Ok((mut s, _)) = listener.accept() else { return };
                let mut buf = vec![0u8; 16384];
                let mut n = 0;
                loop {
                    let r = s.read(&mut buf[n..]).unwrap_or(0);
                    if r == 0 {
                        break;
                    }
                    n += r;
                    let text = String::from_utf8_lossy(&buf[..n]).to_string();
                    if let Some(h) = text.find("\r\n\r\n") {
                        let len = text[..h]
                            .lines()
                            .find_map(|l| l.to_ascii_lowercase().strip_prefix("content-length:").map(|v| v.trim().parse::<usize>().unwrap_or(0)))
                            .unwrap_or(0);
                        if n >= h + 4 + len {
                            break;
                        }
                    }
                }
                let text = String::from_utf8_lossy(&buf[..n]).to_string();
                let line = text.lines().next().unwrap_or("").to_string();
                let body_in = text.split("\r\n\r\n").nth(1).unwrap_or("").to_string();
                seen2.lock().unwrap().push(format!("{line} | {body_in}"));
                let resp = format!(
                    "HTTP/1.1 {status} X\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = s.write_all(resp.as_bytes());
            }
        });
        (addr, seen)
    }

    fn cfg(addr: &str, version: &str) -> ElasticSearchConfig {
        ElasticSearchConfig {
            url: crate::config::EsUrl { scheme: "http".into(), host: addr.into(), path: String::new() },
            indices: HashMap::from([("visibility".to_string(), "cadence-visibility".to_string())]),
            version: version.into(),
            ..Default::default()
        }
    }

    fn attrs() -> HashMap<String, IndexedValueType> {
        HashMap::from([("CustomKw".to_string(), IndexedValueType::Keyword)])
    }

    #[test]
    fn type_mapping_matches_go() {
        assert_eq!(es_data_type(IndexedValueType::String), "text");
        assert_eq!(es_data_type(IndexedValueType::Int), "long");
        assert_eq!(es_data_type(IndexedValueType::Bool), "boolean");
        assert_eq!(es_data_type(IndexedValueType::Datetime), "date");
        assert_eq!(es_data_type(IndexedValueType::Double), "double");
    }

    #[test]
    fn v7_put_mapping_request_shape() {
        let (addr, seen) = fake_server(vec![(200, r#"{"acknowledged":true}"#)]);
        let es = EsVisibility {
            client: Arc::new(HttpEsClient::from_config(&cfg(&addr, "v7")).unwrap()),
            index: "cadence-visibility".into(),
        };
        apply_search_attributes(&es, &attrs()).unwrap();
        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert!(seen[0].starts_with("PUT /cadence-visibility/_mapping HTTP/1.1"), "{}", seen[0]);
        let body: Value = serde_json::from_str(seen[0].split(" | ").nth(1).unwrap()).unwrap();
        assert_eq!(body, json!({"properties":{"Attr":{"properties":{"CustomKw":{"type":"keyword"}}}}}));
    }

    #[test]
    fn v6_default_version_uses_doc_type_path() {
        let (addr, seen) = fake_server(vec![(200, r#"{"acknowledged":true}"#)]);
        let es = EsVisibility {
            client: Arc::new(HttpEsClient::from_config(&cfg(&addr, "")).unwrap()),
            index: "idx".into(),
        };
        apply_search_attributes(&es, &attrs()).unwrap();
        assert!(seen.lock().unwrap()[0].starts_with("PUT /idx/_mapping/_doc HTTP/1.1"));
    }

    #[test]
    fn missing_index_is_created_then_mapping_retried() {
        let (addr, seen) = fake_server(vec![
            (404, r#"{"error":"index_not_found_exception"}"#),
            (200, r#"{"acknowledged":true}"#),
            (200, r#"{"acknowledged":true}"#),
        ]);
        let es = EsVisibility {
            client: Arc::new(HttpEsClient::from_config(&cfg(&addr, "os2")).unwrap()),
            index: "idx".into(),
        };
        apply_search_attributes(&es, &attrs()).unwrap();
        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 3);
        assert!(seen[0].starts_with("PUT /idx/_mapping "));
        assert!(seen[1].starts_with("PUT /idx "));
        assert!(seen[2].starts_with("PUT /idx/_mapping "));
    }

    #[test]
    fn server_error_and_unacknowledged_are_internal_errors() {
        let (addr, _) = fake_server(vec![(500, "boom")]);
        let es = EsVisibility {
            client: Arc::new(HttpEsClient::from_config(&cfg(&addr, "v7")).unwrap()),
            index: "idx".into(),
        };
        let ApplyError::Internal(m) = apply_search_attributes(&es, &attrs()).unwrap_err();
        assert!(m.starts_with("Failed to update ES mapping, err:"), "{m}");

        let (addr, _) = fake_server(vec![(200, r#"{"acknowledged":false}"#)]);
        let es = EsVisibility {
            client: Arc::new(HttpEsClient::from_config(&cfg(&addr, "v7")).unwrap()),
            index: "idx".into(),
        };
        assert!(apply_search_attributes(&es, &attrs()).is_err());
    }

    #[test]
    fn create_index_failure_message() {
        let (addr, _) = fake_server(vec![(404, "{}"), (500, "nope")]);
        let es = EsVisibility {
            client: Arc::new(HttpEsClient::from_config(&cfg(&addr, "v7")).unwrap()),
            index: "idx".into(),
        };
        let ApplyError::Internal(m) = apply_search_attributes(&es, &attrs()).unwrap_err();
        assert!(m.starts_with("Failed to create ES index, err:"), "{m}");
    }

    #[test]
    fn unsupported_version_rejected() {
        assert!(HttpEsClient::from_config(&cfg("h:1", "v5")).is_err());
    }

    #[test]
    fn no_advanced_visibility_store_means_no_es() {
        assert!(from_persistence(&crate::config::PersistenceConfig::default()).unwrap().is_none());
    }
}
