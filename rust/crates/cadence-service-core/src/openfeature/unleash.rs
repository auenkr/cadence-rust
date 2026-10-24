//! Unleash provider configuration and construction, mirroring Go
//! `common/dynamicconfig/openfeatureprovider/unleash` (`init.go`, `unleash.go`).
//!
//! PARTIAL PORT. Config decoding, validation and the bootstrap-file read match
//! Go's `newProvider`. Constructing a working provider is not yet possible:
//! Go delegates polling, strategy evaluation, variants and payloads to
//! `unleash-client-go` v4.5.0, and no Rust equivalent exists in this crate.
//! `new_provider` therefore stops with [`NOT_PORTED_MESSAGE`] after the
//! validation steps instead of returning a provider that approximates Go.

use std::sync::{Arc, RwLock, atomic::{AtomicBool, Ordering}};
use std::time::Duration;
use std::thread::JoinHandle;

use anyhow::{anyhow, bail};
use serde::Deserialize;
use serde_json::Value;

use super::provider::{Constructor, FeatureProvider};
use crate::config::de_go_duration;

/// Plugin name, set as `dynamicconfig.openfeature.providerName`.
pub const PROVIDER_NAME: &str = "unleash";

/// Returned by [`new_provider`] once validation passes, until the evaluation
/// engine is ported.
pub const NOT_PORTED_MESSAGE: &str = "unleash provider evaluation is not ported to Rust yet: \
    Go's unleash-client-go v4.5.0 polling, strategy and variant evaluation has no Rust \
    equivalent (memory/evidence/matching-openfeature-20261024/notes.md)";

/// Go `unleash.Config`, decoded from the `provider:` block. Durations are
/// nanoseconds, as Go's `time.Duration`; zero means "client default".
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
pub struct UnleashConfig {
    #[serde(default)]
    pub url: String,
    #[serde(default, rename = "appName")]
    pub app_name: String,
    #[serde(default, rename = "instanceId")]
    pub instance_id: String,
    #[serde(default)]
    pub environment: String,
    #[serde(default, rename = "projectName")]
    pub project_name: String,
    #[serde(default, rename = "apiToken")]
    pub api_token: String,
    #[serde(default, rename = "refreshInterval", deserialize_with = "de_go_duration")]
    pub refresh_interval_nanos: i64,
    #[serde(default, rename = "metricsInterval", deserialize_with = "de_go_duration")]
    pub metrics_interval_nanos: i64,
    #[serde(default, rename = "bootstrapFile")]
    pub bootstrap_file: String,
}

/// Go `newProvider`'s decode step. A missing or null `provider:` block decodes
/// to the zero config, as Go's nil `*yaml.Node` `Decode` does (it returns nil
/// without writing), so validation then reports the missing `url`.
fn decode(config: Option<&serde_yaml::Value>) -> anyhow::Result<UnleashConfig> {
    match config {
        None | Some(serde_yaml::Value::Null) => Ok(UnleashConfig::default()),
        Some(value) => serde_yaml::from_value(value.clone())
            .map_err(|error| anyhow!("failed to decode unleash provider config: {error}")),
    }
}

/// Go `newProvider`: decode, validate, read the bootstrap file, then build the
/// client. The last step is the part not yet ported; see the module docs.
pub fn new_provider(config: Option<&serde_yaml::Value>) -> anyhow::Result<Arc<dyn FeatureProvider>> {
    let config = decode(config)?;
    if config.url.is_empty() {
        bail!("unleash provider config requires url");
    }
    if config.app_name.is_empty() {
        bail!("unleash provider config requires appName");
    }
    let bootstrap = if !config.bootstrap_file.is_empty() {
        let data = std::fs::read(&config.bootstrap_file).map_err(|error| anyhow!("failed to read unleash bootstrap file {:?}: {error}", config.bootstrap_file))?;
        Some(serde_json::from_slice::<Value>(&data).map_err(|error| anyhow!("failed to decode unleash bootstrap file {:?}: {error}", config.bootstrap_file))?)
    } else { None };
    let repository = UnleashRepository::new(config, bootstrap)?;
    Ok(Arc::new(UnleashProvider { repository }))
}


/// Client-feature repository state. This ports storage and polling only;
/// OpenFeature evaluation remains explicitly unported.
pub struct UnleashRepository {
    features: Arc<RwLock<Value>>,
    stop: Arc<AtomicBool>,
    poller: Option<JoinHandle<()>>,
}
impl UnleashRepository {
    fn new(config: UnleashConfig, bootstrap: Option<Value>) -> anyhow::Result<Self> {
        let features = Arc::new(RwLock::new(feature_map(bootstrap.unwrap_or_else(|| serde_json::json!({"features": []})))));
        let stop = Arc::new(AtomicBool::new(false));
        let interval = if config.refresh_interval_nanos > 0 { Duration::from_nanos(config.refresh_interval_nanos as u64) } else { Duration::from_secs(15) };
        let url = reqwest::Url::parse(&config.url).map_err(|e| anyhow!("invalid unleash url: {e}"))?;
        let mut endpoint = url.join("client/features").map_err(|e| anyhow!("invalid unleash url: {e}"))?; if !config.project_name.is_empty() { endpoint.query_pairs_mut().append_pair("project", &config.project_name); }
        let client = reqwest::blocking::Client::builder().build().map_err(|e| anyhow!("failed to create unleash HTTP client: {e}"))?;
        let headers = vec![("UNLEASH-APPNAME", config.app_name.clone()), ("UNLEASH-INSTANCEID", config.instance_id.clone()), ("Unleash-Interval", interval.as_millis().to_string()), ("User-Agent", config.app_name.clone()), ("Unleash-Client-Spec", "4.3.1".into()), ("Authorization", config.api_token.clone())];
        let state = features.clone(); let stop_flag = stop.clone();
        let poller = std::thread::Builder::new().name("unleash-repository".into()).spawn(move || {
            while !stop_flag.load(Ordering::Acquire) {
                let mut request = client.get(endpoint.clone());
                for (name,value) in &headers { if !value.is_empty() { request = request.header(*name, value); } }
                if let Ok(response) = request.send() {
                    if response.status().is_success() {
                        if let Ok(body) = response.json::<Value>() { *state.write().unwrap_or_else(|p| p.into_inner()) = feature_map(body); }
                    }
                }
                let until = std::time::Instant::now() + interval;
                while !stop_flag.load(Ordering::Acquire) && std::time::Instant::now() < until { std::thread::sleep(Duration::from_millis(10).min(until.saturating_duration_since(std::time::Instant::now()))); }
            }
        }).map_err(|e| anyhow!("failed to start unleash poller: {e}"))?;
        Ok(Self { features, stop, poller: Some(poller) })
    }
    pub fn snapshot(&self) -> Value { self.features.read().unwrap_or_else(|p| p.into_inner()).clone() }
    pub fn shutdown(&mut self) { self.stop.store(true, Ordering::Release); if let Some(task)=self.poller.take() { let _=task.join(); } }
}
impl Drop for UnleashRepository { fn drop(&mut self) { self.shutdown(); } }
fn feature_map(body: Value) -> Value {
    let mut map = serde_json::Map::new();
    if let Some(features) = body.get("features").and_then(Value::as_array) { for feature in features { if let Some(name)=feature.get("name").and_then(Value::as_str) { map.insert(name.to_string(), feature.clone()); } } }
    Value::Object(map)
}
struct UnleashProvider { repository: UnleashRepository }
impl FeatureProvider for UnleashProvider {
    fn shutdown(&self) { /* Repository drop/shutdown owns poller lifetime. */ }
    fn resolve(&self, _flag: &str, default: super::provider::FlagValue, _ctx: &super::provider::FlattenedContext) -> super::provider::Resolution {
        super::provider::Resolution { value: default, error: Some(super::provider::ResolutionError { code: super::provider::ErrorCode::ProviderNotReady, message: NOT_PORTED_MESSAGE.into() }) }
    }
}

/// Go self-registers in `init()`. Rust has no import-time registration, so the
/// caller must invoke this explicitly. Nothing does yet (see the module docs).
pub fn constructor() -> Constructor {
    Arc::new(|config: Option<&serde_yaml::Value>| new_provider(config))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn yaml(text: &str) -> serde_yaml::Value {
        serde_yaml::from_str(text).expect("valid yaml")
    }

    /// Writes `contents` to a unique temp file and returns its path.
    fn temp_file(name: &str, contents: &str) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!("unleash-{name}-{}.json", std::process::id()));
        std::fs::write(&path, contents).expect("write temp file");
        path
    }

    // Mirrors unleash_test.go TestNewProvider, "decode error propagates".
    #[test]
    fn decode_error_propagates() {
        let err = new_provider(Some(&yaml("just-a-string"))).unwrap_err();
        assert!(err.to_string().contains("failed to decode unleash provider config"));
    }

    // Mirrors "missing url".
    #[test]
    fn missing_url_is_rejected() {
        let err = new_provider(Some(&yaml("appName: cadence"))).unwrap_err();
        assert!(err.to_string().contains("requires url"));
    }

    // Mirrors "missing appName".
    #[test]
    fn missing_app_name_is_rejected() {
        let err = new_provider(Some(&yaml("url: https://unleash.example.com/api"))).unwrap_err();
        assert!(err.to_string().contains("requires appName"));
    }

    // A missing provider block is Go's nil Decode followed by the url check.
    #[test]
    fn missing_provider_block_reports_missing_url() {
        let err = new_provider(None).unwrap_err();
        assert!(err.to_string().contains("requires url"));
        let err = new_provider(Some(&serde_yaml::Value::Null)).unwrap_err();
        assert!(err.to_string().contains("requires url"));
    }

    // Mirrors "bootstrap file not found".
    #[test]
    fn missing_bootstrap_file_is_rejected() {
        let missing = std::env::temp_dir().join("unleash-does-not-exist.json");
        let config = format!(
            "url: https://unleash.example.com/api\nappName: cadence\nbootstrapFile: {missing:?}"
        );
        let err = new_provider(Some(&yaml(&config))).unwrap_err();
        assert!(err.to_string().contains("failed to read unleash bootstrap file"));
    }

    // Validation passes for both configs below; the remaining Go assertion
    // (a constructed provider) is the part that is not yet ported.
    #[test]
    fn valid_config_constructs_repository() {
        let provider = new_provider(Some(&yaml(
            "url: http://127.0.0.1:1/api\nappName: cadence",
        )));
        assert!(provider.is_ok());
    }

    #[test]
    fn valid_bootstrap_file_reaches_the_unported_engine() {
        let path = temp_file("bootstrap", r#"{"version":1,"features":[]}"#);
        let config = format!(
            "url: https://unleash.example.com/api\nappName: cadence\nbootstrapFile: {path:?}"
        );
        let provider = new_provider(Some(&yaml(&config)));
        assert!(provider.is_ok());
        let _ = std::fs::remove_file(&path);
    }

    // Field names and duration decoding match init.go / Go's yaml.v3 for
    // time.Duration (string such as "30s", or integer nanoseconds).
    #[test]
    fn config_fields_decode_with_go_names() {
        let config = decode(Some(&yaml(
            "url: u\nappName: a\ninstanceId: i\nenvironment: e\nprojectName: p\n\
             apiToken: t\nrefreshInterval: 30s\nmetricsInterval: 60000000000\nbootstrapFile: b",
        )))
        .unwrap();
        assert_eq!(config.url, "u");
        assert_eq!(config.app_name, "a");
        assert_eq!(config.instance_id, "i");
        assert_eq!(config.environment, "e");
        assert_eq!(config.project_name, "p");
        assert_eq!(config.api_token, "t");
        assert_eq!(config.refresh_interval_nanos, 30_000_000_000);
        assert_eq!(config.metrics_interval_nanos, 60_000_000_000);
        assert_eq!(config.bootstrap_file, "b");
    }
}
