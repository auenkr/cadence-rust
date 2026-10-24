//! Unleash provider configuration and construction, mirroring Go
//! `common/dynamicconfig/openfeatureprovider/unleash` (`init.go`, `unleash.go`).
//!
//! Config decoding, bootstrap, repository polling and local evaluation are
//! implemented against unleash-client-go v4.5.0 semantics.

use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex, RwLock,
};
use std::thread::JoinHandle;
use std::time::Duration;

use anyhow::{anyhow, bail};
use serde::Deserialize;
use serde_json::Value;

use super::provider::{Constructor, FeatureProvider};
use super::provider::{ErrorCode, FlagValue, FlattenedContext, Resolution, ResolutionError};
use super::unleash_engine::{self, Context as UnleashContext, FeatureResponse};
use crate::config::de_go_duration;

/// Plugin name, set as `dynamicconfig.openfeature.providerName`.
pub const PROVIDER_NAME: &str = "unleash";

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
    #[serde(
        default,
        rename = "refreshInterval",
        deserialize_with = "de_go_duration"
    )]
    pub refresh_interval_nanos: i64,
    #[serde(
        default,
        rename = "metricsInterval",
        deserialize_with = "de_go_duration"
    )]
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
pub fn new_provider(
    config: Option<&serde_yaml::Value>,
) -> anyhow::Result<Arc<dyn FeatureProvider>> {
    let config = decode(config)?;
    if config.url.is_empty() {
        bail!("unleash provider config requires url");
    }
    if config.app_name.is_empty() {
        bail!("unleash provider config requires appName");
    }
    let bootstrap = if !config.bootstrap_file.is_empty() {
        let data = std::fs::read(&config.bootstrap_file).map_err(|error| {
            anyhow!(
                "failed to read unleash bootstrap file {:?}: {error}",
                config.bootstrap_file
            )
        })?;
        Some(serde_json::from_slice::<Value>(&data).map_err(|error| {
            anyhow!(
                "failed to decode unleash bootstrap file {:?}: {error}",
                config.bootstrap_file
            )
        })?)
    } else {
        None
    };
    let repository = UnleashRepository::new(config, bootstrap)?;
    Ok(Arc::new(UnleashProvider {
        repository,
        ready: AtomicBool::new(false),
    }))
}

/// Client-feature repository state. This ports storage and polling only;
/// OpenFeature evaluation remains explicitly unported.
pub struct UnleashRepository {
    features: Arc<RwLock<Value>>,
    static_context: UnleashContext,
    metrics: Arc<Mutex<MetricsBucket>>,
    stop: Arc<AtomicBool>,
    poller: Mutex<Option<JoinHandle<()>>>,
    reporter: Mutex<Option<JoinHandle<()>>>,
}

#[derive(Default)]
struct MetricsBucket {
    toggles: serde_json::Map<String, Value>,
    start: Option<String>,
}
impl MetricsBucket {
    fn count(&mut self, name: &str, enabled: bool, variant: Option<&str>) {
        let toggle = self
            .toggles
            .entry(name.to_owned())
            .or_insert_with(|| serde_json::json!({"yes":0,"no":0,"variants":{}}));
        let key = if enabled { "yes" } else { "no" };
        toggle[key] = Value::from(toggle[key].as_i64().unwrap_or(0) + 1);
        if let Some(variant) = variant {
            let variants = toggle["variants"].as_object_mut().expect("variants map");
            variants.insert(
                variant.to_owned(),
                Value::from(variants.get(variant).and_then(Value::as_i64).unwrap_or(0) + 1),
            );
        }
    }

    fn reset(&mut self, next_start: String) -> (String, serde_json::Map<String, Value>) {
        let start = self.start.take().unwrap_or_else(|| next_start.clone());
        let toggles = std::mem::take(&mut self.toggles);
        self.start = Some(next_start);
        (start, toggles)
    }

    fn restore_failed(&mut self, start: String, toggles: serde_json::Map<String, Value>) {
        for (name, counts) in toggles {
            let current = self
                .toggles
                .entry(name)
                .or_insert_with(|| serde_json::json!({"yes":0,"no":0,"variants":{}}));
            for field in ["yes", "no"] {
                current[field] = Value::from(
                    current[field].as_i64().unwrap_or(0) + counts[field].as_i64().unwrap_or(0),
                );
            }
            for (variant, count) in counts["variants"].as_object().into_iter().flatten() {
                let variants = current["variants"].as_object_mut().expect("variants map");
                variants.insert(
                    variant.clone(),
                    Value::from(
                        variants.get(variant).and_then(Value::as_i64).unwrap_or(0)
                            + count.as_i64().unwrap_or(0),
                    ),
                );
            }
        }
        // Go's failed-post path moves the new bucket start back to the failed
        // bucket boundary, so the retry covers the full retained interval.
        self.start = Some(start);
    }
}
fn default_instance_id() -> String {
    let user = std::env::var("USER")
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| format!("generated-{}", std::process::id()));
    match std::fs::read_to_string("/etc/hostname")
        .ok()
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty())
    {
        Some(host) => format!("{user}-{host}"),
        None => user,
    }
}
impl UnleashRepository {
    fn new(config: UnleashConfig, bootstrap: Option<Value>) -> anyhow::Result<Self> {
        let features = Arc::new(RwLock::new(feature_map(
            bootstrap.unwrap_or_else(|| serde_json::json!({"features": []})),
        )));
        let static_context = UnleashContext {
            app_name: config.app_name.clone(),
            environment: if config.environment.is_empty() {
                "default".into()
            } else {
                config.environment.clone()
            },
            ..Default::default()
        };
        let stop = Arc::new(AtomicBool::new(false));
        let interval = if config.refresh_interval_nanos > 0 {
            Duration::from_nanos(config.refresh_interval_nanos as u64)
        } else {
            Duration::from_secs(15)
        };
        // unleash-client-go appends a slash before resolving ./client/features.
        let mut base = config.url.clone();
        if !base.ends_with('/') {
            base.push('/');
        }
        let url = reqwest::Url::parse(&base).map_err(|e| anyhow!("invalid unleash url: {e}"))?;
        let mut endpoint = url
            .join("./client/features")
            .map_err(|e| anyhow!("invalid unleash url: {e}"))?;
        // Go builds ./client/features?project=<name> as a reference string;
        // preserve that URL escaping behavior instead of form-encoding spaces as '+'.
        if !config.project_name.is_empty() {
            endpoint.set_query(Some(&format!("project={}", config.project_name)));
        }
        let instance_id = if config.instance_id.is_empty() {
            default_instance_id()
        } else {
            config.instance_id.clone()
        };
        let metrics_interval = if config.metrics_interval_nanos > 0 {
            Duration::from_nanos(config.metrics_interval_nanos as u64)
        } else {
            Duration::from_secs(60)
        };
        let connection_id = uuid::Uuid::new_v4().to_string();
        let headers = vec![
            ("UNLEASH-APPNAME", config.app_name.clone()),
            ("UNLEASH-INSTANCEID", instance_id.clone()),
            ("Unleash-Interval", interval.as_millis().to_string()),
            ("User-Agent", config.app_name.clone()),
            ("Unleash-Client-Spec", "4.3.1".into()),
            ("Authorization", config.api_token.clone()),
            ("Unleash-SDK", "unleash-client-go:4.5.0".into()),
            ("Unleash-Connection-Id", connection_id.clone()),
        ];
        let mut shared_headers = headers.clone();
        if let Some((_, value)) = shared_headers
            .iter_mut()
            .find(|(name, _)| *name == "Unleash-Interval")
        {
            *value = metrics_interval.as_millis().to_string();
        }
        let etag = Arc::new(Mutex::new(String::new()));
        let poll_etag = etag.clone();
        let state = features.clone();
        let stop_flag = stop.clone();
        let poller = std::thread::Builder::new()
            .name("unleash-repository".into())
            .spawn(move || {
                // reqwest's blocking client must be created off any Tokio runtime
                // thread. Its finite timeout also bounds shutdown during a fetch.
                let Ok(client) = reqwest::blocking::Client::builder()
                    .timeout(Duration::from_millis(500))
                    .build()
                else {
                    return;
                };
                while !stop_flag.load(Ordering::Acquire) {
                    let mut request = client.get(endpoint.clone());
                    for (name, value) in &headers {
                        if !value.is_empty() {
                            request = request.header(*name, value);
                        }
                    }
                    let tag = poll_etag.lock().unwrap_or_else(|p| p.into_inner()).clone();
                    if !tag.is_empty() {
                        request = request.header("If-None-Match", tag);
                    }
                    if let Ok(response) = request.send() {
                        if response.status() == reqwest::StatusCode::NOT_MODIFIED {
                        } else if response.status().is_success() {
                            let new_etag = response
                                .headers()
                                .get(reqwest::header::ETAG)
                                .and_then(|v| v.to_str().ok())
                                .map(str::to_owned);
                            if let Ok(body) = response.json::<Value>() {
                                *state.write().unwrap_or_else(|p| p.into_inner()) =
                                    feature_map(body);
                                *poll_etag.lock().unwrap_or_else(|p| p.into_inner()) =
                                    new_etag.unwrap_or_default();
                            }
                        }
                    }
                    let until = std::time::Instant::now() + interval;
                    while !stop_flag.load(Ordering::Acquire) && std::time::Instant::now() < until {
                        std::thread::sleep(
                            Duration::from_millis(10)
                                .min(until.saturating_duration_since(std::time::Instant::now())),
                        );
                    }
                }
            })
            .map_err(|e| anyhow!("failed to start unleash poller: {e}"))?;
        let metrics = Arc::new(Mutex::new(MetricsBucket {
            toggles: serde_json::Map::new(),
            start: Some(chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true)),
        }));
        let report_metrics = metrics.clone();
        let report_stop = stop.clone();
        let report_url = url
            .join("./client/metrics")
            .map_err(|e| anyhow!("invalid unleash url: {e}"))?;
        let register_url = url
            .join("./client/register")
            .map_err(|e| anyhow!("invalid unleash url: {e}"))?;
        let app_name = config.app_name.clone();
        let strategies = vec![
            "default",
            "applicationHostname",
            "gradualRollout/random",
            "gradualRollout/sessionId",
            "gradualRollout/userId",
            "remoteAddress",
            "userWithId",
            "flexibleRollout",
        ];
        let reporter_result = std::thread::Builder::new().name("unleash-metrics".into()).spawn(move || {
            let Ok(client) = reqwest::blocking::Client::builder().timeout(Duration::from_millis(500)).build() else { return; };
            let started = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true);
            let register = serde_json::json!({"appName":app_name,"instanceId":instance_id,"connectionId":connection_id,"sdkVersion":"unleash-client-go:4.5.0","strategies":strategies,"started":started,"interval":metrics_interval.as_secs(),"platformVersion":format!("{} {}", std::env::consts::OS, std::env::consts::ARCH),"platformName":"rust","yggdrasilVersion":null,"specVersion":"4.3.1"});
            let mut req = client.post(register_url).json(&register);
            for (name,value) in &shared_headers { if !value.is_empty() { req = req.header(*name,value); } }
            let _ = req.send();
            let mut next = std::time::Instant::now() + metrics_interval;
            let mut errors = 0u8;
            let mut skips = 0u8;
            while !report_stop.load(Ordering::Acquire) {
                while !report_stop.load(Ordering::Acquire) && std::time::Instant::now() < next { std::thread::sleep(Duration::from_millis(10).min(next.saturating_duration_since(std::time::Instant::now()))); }
                if report_stop.load(Ordering::Acquire) { break; }
                next = std::time::Instant::now() + metrics_interval;
                if skips > 0 {
                    skips -= 1;
                    continue;
                }
                let mut bucket = report_metrics.lock().unwrap_or_else(|p| p.into_inner());
                let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true);
                let (start, toggles) = bucket.reset(now.clone());
                if toggles.is_empty() { continue; }
                drop(bucket);
                let payload = serde_json::json!({"appName":app_name,"instanceId":instance_id,"connectionId":connection_id,"bucket":{"start":start,"stop":now,"toggles":toggles},"platformVersion":format!("{} {}",std::env::consts::OS,std::env::consts::ARCH),"platformName":"rust","yggdrasilVersion":null,"sdkVersion":"unleash-client-go:4.5.0","specVersion":"4.3.1"});
                let mut req = client.post(report_url.clone()).json(&payload);
                for (name,value) in &shared_headers { if !value.is_empty() { req = req.header(*name,value); } }
                let post_result = req.send();
                let failed_status = post_result
                    .as_ref()
                    .ok()
                    .map(reqwest::blocking::Response::status)
                    .filter(|status| !status.is_success());
                if post_result.is_err() || failed_status.is_some() {
                    if let Some(status) = failed_status {
                        if matches!(status, reqwest::StatusCode::UNAUTHORIZED | reqwest::StatusCode::FORBIDDEN | reqwest::StatusCode::NOT_FOUND) {
                            errors = 10;
                        } else if status == reqwest::StatusCode::TOO_MANY_REQUESTS || status.is_server_error() {
                            errors = (errors + 1).min(10);
                        }
                        skips = errors;
                    }
                    let mut bucket = report_metrics.lock().unwrap_or_else(|p| p.into_inner());
                    bucket.restore_failed(start, toggles);
                } else {
                    errors = errors.saturating_sub(1);
                    skips = errors;
                }
            }
        });
        let reporter = match reporter_result {
            Ok(reporter) => reporter,
            Err(error) => {
                stop.store(true, Ordering::Release);
                let _ = poller.join();
                return Err(anyhow!("failed to start unleash metrics sender: {error}"));
            }
        };
        Ok(Self {
            features,
            static_context,
            metrics,
            stop,
            poller: Mutex::new(Some(poller)),
            reporter: Mutex::new(Some(reporter)),
        })
    }
    pub fn snapshot(&self) -> Value {
        self.features
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }
    pub fn shutdown(&self) {
        self.stop.store(true, Ordering::Release);
        if let Some(task) = self.poller.lock().unwrap_or_else(|p| p.into_inner()).take() {
            let _ = task.join();
        }
        if let Some(task) = self
            .reporter
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take()
        {
            let _ = task.join();
        }
    }
}
impl Drop for UnleashRepository {
    fn drop(&mut self) {
        self.shutdown();
    }
}
fn feature_map(body: Value) -> Value {
    let mut map = serde_json::Map::new();
    if let Some(features) = body.get("features").and_then(Value::as_array) {
        for feature in features {
            if let Some(name) = feature.get("name").and_then(Value::as_str) {
                map.insert(name.to_string(), feature.clone());
            }
        }
    }
    map.insert(
        "__segments".into(),
        body.get("segments")
            .cloned()
            .unwrap_or_else(|| Value::Array(Vec::new())),
    );
    Value::Object(map)
}
struct UnleashProvider {
    repository: UnleashRepository,
    ready: AtomicBool,
}
impl FeatureProvider for UnleashProvider {
    fn init(&self) -> anyhow::Result<()> {
        self.ready.store(true, Ordering::Release);
        Ok(())
    }

    fn shutdown(&self) {
        self.ready.store(false, Ordering::Release);
        self.repository.shutdown();
    }
    fn resolve(&self, flag: &str, default: FlagValue, ctx: &FlattenedContext) -> Resolution {
        if !self.ready.load(Ordering::Acquire) {
            return failure(default, ErrorCode::ProviderNotReady, "Provider not ready");
        }
        let context = match unleash_context(ctx) {
            Ok(context) => context,
            Err(error) => {
                let code = if matches!(default, FlagValue::Bool(_)) {
                    ErrorCode::InvalidContext
                } else {
                    ErrorCode::General
                };
                return failure(default, code, error);
            }
        };
        let context = merge_context(self.repository.static_context.clone(), context);
        let snapshot = match evaluation_snapshot(self.repository.snapshot()) {
            Ok(snapshot) => snapshot,
            Err(error) => return failure(default, ErrorCode::General, error.to_string()),
        };
        if matches!(default, FlagValue::Bool(_)) {
            let fallback = match &default {
                FlagValue::Bool(value) => *value,
                _ => unreachable!(),
            };
            let enabled = if snapshot.features.iter().any(|feature| feature.name == flag) {
                unleash_engine::is_enabled(&snapshot, flag, &context)
            } else {
                fallback
            };
            self.repository
                .metrics
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .count(flag, enabled, None);
            return Resolution {
                value: FlagValue::Bool(enabled),
                error: None,
            };
        }
        let variant = unleash_engine::get_variant(&snapshot, flag, &context, 0);
        self.repository
            .metrics
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .count(flag, variant.feature_enabled, Some(&variant.name));
        if !variant.feature_enabled || variant.name == "disabled" {
            return Resolution {
                value: default,
                error: None,
            };
        }
        let payload = variant.payload.value;
        let fallback = default.clone();
        match default {
            FlagValue::String(_) => Resolution {
                value: FlagValue::String(payload),
                error: None,
            },
            FlagValue::Int(_) => match payload.parse::<i64>() {
                Ok(value) => Resolution {
                    value: FlagValue::Int(value),
                    error: None,
                },
                Err(_) => failure(
                    fallback,
                    ErrorCode::TypeMismatch,
                    format!("IntEvaluation type error for {flag}"),
                ),
            },
            FlagValue::Float(_) => match payload.parse::<f64>() {
                Ok(value) => Resolution {
                    value: FlagValue::Float(value),
                    error: None,
                },
                Err(_) => failure(
                    fallback,
                    ErrorCode::TypeMismatch,
                    format!("FloatEvaluation type error for {flag}"),
                ),
            },
            FlagValue::Object(_) => Resolution {
                value: FlagValue::Object(Value::String(payload)),
                error: None,
            },
            FlagValue::Bool(value) => Resolution {
                value: FlagValue::Bool(value),
                error: None,
            },
        }
    }
}

fn failure(default: FlagValue, code: ErrorCode, message: impl Into<String>) -> Resolution {
    Resolution {
        value: default,
        error: Some(ResolutionError {
            code,
            message: message.into(),
        }),
    }
}

fn evaluation_snapshot(snapshot: Value) -> anyhow::Result<FeatureResponse> {
    let entries = snapshot
        .as_object()
        .ok_or_else(|| anyhow!("invalid unleash feature cache"))?;
    let features = entries
        .iter()
        .filter(|(name, _)| name.as_str() != "__segments")
        .map(|(_, feature)| serde_json::from_value(feature.clone()))
        .collect::<Result<Vec<_>, _>>()?;
    let segments = entries
        .get("__segments")
        .cloned()
        .unwrap_or_else(|| Value::Array(Vec::new()));
    Ok(FeatureResponse {
        features,
        segments: serde_json::from_value(segments)?,
    })
}

fn unleash_context(context: &FlattenedContext) -> Result<UnleashContext, String> {
    let mut output = UnleashContext::default();
    for (key, value) in context {
        let text = match value {
            Value::String(value) => value.clone(),
            Value::Bool(value) => value.to_string(),
            Value::Number(value) if value.is_i64() || value.is_u64() => value.to_string(),
            Value::Number(value) => format!("{:.6}", value.as_f64().unwrap_or_default()),
            _ => return Err(format!("key `{key}` can not be converted to string")),
        };
        match key.as_str() {
            "AppName" => output.app_name = text,
            "CurrentTime" => output.current_time = text,
            "Environment" => output.environment = text,
            "RemoteAddress" => output.remote_address = text,
            "SessionId" => output.session_id = text,
            "UserId" => output.user_id = text,
            _ => {
                output.properties.insert(key.clone(), text);
            }
        }
    }
    Ok(output)
}

fn merge_context(mut base: UnleashContext, override_context: UnleashContext) -> UnleashContext {
    if !override_context.user_id.is_empty() {
        base.user_id = override_context.user_id;
    }
    if !override_context.session_id.is_empty() {
        base.session_id = override_context.session_id;
    }
    if !override_context.remote_address.is_empty() {
        base.remote_address = override_context.remote_address;
    }
    if !override_context.environment.is_empty() {
        base.environment = override_context.environment;
    }
    if !override_context.app_name.is_empty() {
        base.app_name = override_context.app_name;
    }
    if !override_context.current_time.is_empty() {
        base.current_time = override_context.current_time;
    }
    if !override_context.properties.is_empty() {
        base.properties = override_context.properties;
    }
    base
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
        assert!(err
            .to_string()
            .contains("failed to decode unleash provider config"));
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
        assert!(err
            .to_string()
            .contains("failed to read unleash bootstrap file"));
    }

    // A constructed provider starts polling asynchronously.
    #[test]
    fn valid_config_constructs_repository() {
        let provider = new_provider(Some(&yaml("url: http://127.0.0.1:1/api\nappName: cadence")));
        assert!(provider.is_ok());
    }

    #[test]
    fn provider_construction_and_shutdown_are_safe_inside_tokio_runtime() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let provider =
                new_provider(Some(&yaml("url: http://127.0.0.1:1/api\nappName: cadence"))).unwrap();
            provider.shutdown();
        });
    }

    #[test]
    fn valid_bootstrap_file_constructs_provider() {
        let path = temp_file("bootstrap", r#"{"version":1,"features":[]}"#);
        let config = format!(
            "url: https://unleash.example.com/api\nappName: cadence\nbootstrapFile: {path:?}"
        );
        let provider = new_provider(Some(&yaml(&config)));
        assert!(provider.is_ok());
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn typed_resolutions_use_cached_variant_payloads_and_default_fallbacks() {
        use super::super::provider::{ErrorCode, FlagValue};
        use std::collections::BTreeMap;

        let repository = UnleashRepository::new(
            UnleashConfig {
                url: "http://127.0.0.1:1/api".into(),
                app_name: "cadence-test".into(),
                ..Default::default()
            },
            Some(serde_json::json!({"features":[
                {"name":"number","enabled":true,"strategies":[{"name":"default"}],
                 "variants":[{"name":"v1","weight":100,"payload":{"type":"string","value":"42"}}]},
                {"name":"bad-number","enabled":true,"strategies":[{"name":"default"}],
                 "variants":[{"name":"v1","weight":100,"payload":{"type":"string","value":"nope"}}]},
                {"name":"off","enabled":false}
            ]})),
        )
        .unwrap();
        let provider = UnleashProvider {
            repository,
            ready: AtomicBool::new(false),
        };
        let mut context = BTreeMap::new();
        context.insert("UserId".into(), Value::String("alice".into()));
        let not_ready = provider.resolve("number", FlagValue::Int(7), &context);
        assert_eq!(not_ready.value, FlagValue::Int(7));
        assert_eq!(not_ready.error.unwrap().code, ErrorCode::ProviderNotReady);

        provider.init().unwrap();
        assert_eq!(
            provider
                .resolve("number", FlagValue::Bool(false), &context)
                .value,
            FlagValue::Bool(true)
        );
        assert_eq!(
            provider
                .resolve("number", FlagValue::String("fallback".into()), &context)
                .value,
            FlagValue::String("42".into())
        );
        assert_eq!(
            provider
                .resolve("number", FlagValue::Int(7), &context)
                .value,
            FlagValue::Int(42)
        );
        assert_eq!(
            provider
                .resolve("number", FlagValue::Float(7.0), &context)
                .value,
            FlagValue::Float(42.0)
        );
        assert_eq!(
            provider
                .resolve("number", FlagValue::Object(Value::Null), &context)
                .value,
            FlagValue::Object(Value::String("42".into()))
        );
        let mismatch = provider.resolve("bad-number", FlagValue::Int(7), &context);
        assert_eq!(mismatch.value, FlagValue::Int(7));
        assert_eq!(mismatch.error.unwrap().code, ErrorCode::TypeMismatch);
        assert_eq!(
            provider.resolve("off", FlagValue::Int(7), &context).value,
            FlagValue::Int(7)
        );
        provider.shutdown();
    }

    #[test]
    fn bool_context_conversion_failure_matches_openfeature_invalid_context() {
        use super::super::provider::{ErrorCode, FlagValue};
        use std::collections::BTreeMap;
        let provider = UnleashProvider {
            repository: UnleashRepository::new(
                UnleashConfig {
                    url: "http://127.0.0.1:1/api".into(),
                    app_name: "test".into(),
                    ..Default::default()
                },
                None,
            )
            .unwrap(),
            ready: AtomicBool::new(false),
        };
        provider.init().unwrap();
        let mut context = BTreeMap::new();
        context.insert("bad".into(), Value::Array(vec![]));
        let result = provider.resolve("flag", FlagValue::Bool(true), &context);
        assert_eq!(result.value, FlagValue::Bool(true));
        assert_eq!(result.error.unwrap().code, ErrorCode::InvalidContext);
        provider.shutdown();
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

    fn local_server<F>(
        mut respond: F,
    ) -> (
        String,
        std::sync::mpsc::Receiver<String>,
        std::thread::JoinHandle<()>,
    )
    where
        F: FnMut(usize, &str) -> (&'static str, &'static str) + Send + 'static,
    {
        use std::io::{Read, Write};
        use std::net::TcpListener;
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let (tx, rx) = std::sync::mpsc::channel();
        let handle = std::thread::spawn(move || {
            for index in 0..5 {
                let Ok((mut stream, _)) = listener.accept() else {
                    break;
                };
                let mut request = [0; 4096];
                let size = stream.read(&mut request).unwrap_or(0);
                tx.send(String::from_utf8_lossy(&request[..size]).into_owned())
                    .unwrap();
                let (status, body) = respond(index, &String::from_utf8_lossy(&request));
                let _ = write!(stream, "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
            }
        });
        (format!("http://{address}/api"), rx, handle)
    }

    fn wait_for_feature(repository: &UnleashRepository, name: &str) {
        let until = std::time::Instant::now() + Duration::from_secs(2);
        while std::time::Instant::now() < until {
            if repository.snapshot().get(name).is_some() {
                return;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        panic!("feature {name} did not arrive");
    }

    #[test]
    fn polling_bootstraps_refreshes_uses_go_url_and_headers_and_keeps_cache_on_errors() {
        let (release_first_response, wait_for_release) = std::sync::mpsc::channel();
        let (release_final_response, wait_for_final_release) = std::sync::mpsc::channel();
        let mut get_count = 0usize;
        let (base, requests, server) = local_server(move |_index, request| {
            if request.starts_with("GET ") {
                get_count += 1;
            }
            if request.starts_with("GET ") && get_count == 1 {
                wait_for_release.recv().unwrap();
            }
            if request.starts_with("GET ") && get_count == 4 {
                wait_for_final_release.recv().unwrap();
            }
            if request.starts_with("POST ") {
                return ("200 OK", "{}");
            }
            match get_count {
                1 => (
                    "200 OK",
                    r#"{"features":[{"name":"refreshed","enabled":true}]}"#,
                ),
                2 => ("503 Service Unavailable", "{}"),
                _ => ("200 OK", "not-json"),
            }
        });
        let bootstrap = serde_json::json!({"features":[{"name":"bootstrapped","enabled":true}]});
        let repository = UnleashRepository::new(
            UnleashConfig {
                url: base,
                app_name: "cadence-test".into(),
                instance_id: "instance-7".into(),
                project_name: "project blue".into(),
                api_token: "token-123".into(),
                refresh_interval_nanos: 20_000_000,
                ..Default::default()
            },
            Some(bootstrap),
        )
        .unwrap();
        assert!(repository.snapshot().get("bootstrapped").is_some());
        let mut observed = Vec::new();
        let first = loop {
            let request = requests.recv_timeout(Duration::from_secs(1)).unwrap();
            if request.starts_with("GET ") {
                break request;
            }
            observed.push(request);
        };
        assert!(
            first.starts_with("GET /api/client/features?project=project%20blue HTTP/1.1"),
            "{first}"
        );
        release_first_response.send(()).unwrap();
        wait_for_feature(&repository, "refreshed");
        for expected in [
            "UNLEASH-APPNAME: cadence-test",
            "UNLEASH-INSTANCEID: instance-7",
            "Unleash-Interval: 20",
            "Unleash-Client-Spec: 4.3.1",
            "Authorization: token-123",
            "User-Agent: cadence-test",
        ] {
            assert!(
                first
                    .to_ascii_lowercase()
                    .contains(&expected.to_ascii_lowercase()),
                "missing {expected} in {first}"
            );
        }
        while observed.len() < 4 {
            observed.push(requests.recv_timeout(Duration::from_secs(1)).unwrap());
        }
        assert!(repository.snapshot().get("refreshed").is_some());
        release_final_response.send(()).unwrap();
        let provider = UnleashProvider {
            repository,
            ready: AtomicBool::new(false),
        };
        provider.shutdown();
        server.join().unwrap();
    }

    #[test]
    fn shutdown_joins_an_in_flight_request_within_client_timeout() {
        use std::io::Read;
        use std::net::TcpListener;
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let (tx, rx) = std::sync::mpsc::channel();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut buffer = [0; 1024];
            let _ = stream.read(&mut buffer);
            tx.send(()).unwrap();
            // Keep the socket open; reqwest's configured deadline releases the poller.
            std::thread::sleep(Duration::from_secs(1));
        });
        let repository = UnleashRepository::new(
            UnleashConfig {
                url: format!("http://{address}/api"),
                app_name: "cadence".into(),
                refresh_interval_nanos: 1_000_000,
                ..Default::default()
            },
            None,
        )
        .unwrap();
        rx.recv_timeout(Duration::from_secs(1)).unwrap();
        let started = std::time::Instant::now();
        let provider = UnleashProvider {
            repository,
            ready: AtomicBool::new(false),
        };
        provider.shutdown();
        assert!(started.elapsed() < Duration::from_secs(2));
        server.join().unwrap();
    }

    #[test]
    fn metrics_registration_flush_and_shutdown_use_fake_http_server() {
        use std::io::{Read, Write};
        use std::net::TcpListener;
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let (tx, rx) = std::sync::mpsc::channel();
        let server = std::thread::spawn(move || {
            for index in 0..4 {
                let (mut stream, _) = listener.accept().unwrap();
                let mut request = Vec::new();
                let mut chunk = [0u8; 1024];
                loop {
                    let n = stream.read(&mut chunk).unwrap_or(0);
                    if n == 0 {
                        break;
                    }
                    request.extend_from_slice(&chunk[..n]);
                    let text = String::from_utf8_lossy(&request);
                    if let Some(length) = text
                        .to_ascii_lowercase()
                        .split("content-length: ")
                        .nth(1)
                        .and_then(|s| s.split("\r\n").next())
                        .and_then(|s| s.parse::<usize>().ok())
                    {
                        if let Some(body) = text.split("\r\n\r\n").nth(1) {
                            if body.len() >= length {
                                break;
                            }
                        }
                    } else if text.contains("\r\n\r\n") {
                        break;
                    }
                }
                let raw = String::from_utf8_lossy(&request).into_owned();
                tx.send(raw).unwrap();
                let body = if request.starts_with(b"GET ") {
                    r#"{"features":[{"name":"flag","enabled":true,"strategies":[{"name":"default"}],"variants":[{"name":"blue","weight":100}]},{"name":"enabled-no-variants","enabled":true,"strategies":[{"name":"default"}]}]}"#
                } else {
                    "{}"
                };
                let status = if index == 2 {
                    "500 Internal Server Error"
                } else {
                    "200 OK"
                };
                write!(stream, "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
            }
        });
        let repository = UnleashRepository::new(
            UnleashConfig {
                url: format!("http://{address}/api"),
                app_name: "cadence-test".into(),
                instance_id: "instance-test".into(),
                refresh_interval_nanos: 60_000_000_000,
                metrics_interval_nanos: 40_000_000,
                ..Default::default()
            },
            None,
        )
        .unwrap();
        let mut first = None;
        let mut register = None;
        while first.is_none() || register.is_none() {
            let request = rx.recv_timeout(Duration::from_secs(2)).unwrap();
            if request.starts_with("GET /api/client/features") {
                first = Some(request);
            } else if request.starts_with("POST /api/client/register") {
                register = Some(request);
            }
        }
        let first = first.unwrap();
        let register = register.unwrap();
        assert!(first.starts_with("GET /api/client/features"), "{first}");
        assert!(first
            .to_ascii_lowercase()
            .contains("unleash-instanceid: instance-test"));
        assert!(
            register.starts_with("POST /api/client/register"),
            "{register}"
        );
        assert!(register
            .to_ascii_lowercase()
            .contains("content-type: application/json"));
        assert!(
            register.contains("\"instanceId\":\"instance-test\""),
            "{register}"
        );
        assert!(
            register.contains("\"sdkVersion\":\"unleash-client-go:4.5.0\""),
            "{register}"
        );
        let provider = UnleashProvider {
            repository,
            ready: AtomicBool::new(true),
        };
        wait_for_feature(&provider.repository, "flag");
        let result = provider.resolve("flag", FlagValue::Bool(false), &Default::default());
        assert_eq!(result.value, FlagValue::Bool(true));
        let _ = provider.resolve(
            "flag",
            FlagValue::String("fallback".into()),
            &Default::default(),
        );
        let _ = provider.resolve(
            "enabled-no-variants",
            FlagValue::String("fallback".into()),
            &Default::default(),
        );
        let metrics = rx.recv_timeout(Duration::from_secs(2)).unwrap();
        assert!(metrics.starts_with("POST /api/client/metrics"), "{metrics}");
        assert!(metrics.contains("\"flag\":{"), "{metrics}");
        assert!(metrics.contains("\"yes\":3"), "{metrics}");
        assert!(metrics.contains("\"blue\":1"), "{metrics}");
        assert!(metrics.contains("\"disabled\":1"), "{metrics}");
        let retry = rx.recv_timeout(Duration::from_secs(2)).unwrap();
        assert!(retry.starts_with("POST /api/client/metrics"), "{retry}");
        assert!(retry.contains("\"yes\":3"), "{retry}");
        assert!(retry.contains("\"blue\":1"), "{retry}");
        assert!(retry.contains("\"disabled\":1"), "{retry}");
        provider.shutdown();
        server.join().unwrap();
    }

    #[test]
    fn metrics_bucket_reset_advances_boundary_and_failed_post_restores_it() {
        let mut bucket = MetricsBucket {
            toggles: serde_json::Map::new(),
            start: Some("start-0".into()),
        };
        bucket.count("flag", true, Some("blue"));
        let (first_start, first_toggles) = bucket.reset("boundary-1".into());
        assert_eq!(first_start, "start-0");
        assert_eq!(first_toggles["flag"]["yes"], 1);
        assert_eq!(bucket.start.as_deref(), Some("boundary-1"));

        bucket.count("flag", false, Some("disabled"));
        let (second_start, second_toggles) = bucket.reset("boundary-2".into());
        assert_eq!(second_start, "boundary-1");
        bucket.restore_failed(second_start, second_toggles);
        assert_eq!(bucket.start.as_deref(), Some("boundary-1"));
        assert_eq!(bucket.toggles["flag"]["no"], 1);
        assert_eq!(bucket.toggles["flag"]["variants"]["disabled"], 1);
    }

    #[test]
    fn etag_304_retains_snapshot_and_sends_if_none_match() {
        use std::io::{Read, Write};
        use std::net::TcpListener;
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let (tx, rx) = std::sync::mpsc::channel();
        let server = std::thread::spawn(move || {
            let mut feature_requests = 0;
            for _ in 0..4 {
                let (mut stream, _) = listener.accept().unwrap();
                let mut request = [0u8; 4096];
                let n = stream.read(&mut request).unwrap_or(0);
                let request = String::from_utf8_lossy(&request[..n]).into_owned();
                let is_features = request.starts_with("GET ");
                if is_features {
                    feature_requests += 1;
                }
                tx.send(request).unwrap();
                if !is_features {
                    write!(
                        stream,
                        "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{{}}"
                    )
                    .unwrap();
                } else if feature_requests == 1 {
                    let body = r#"{"features":[{"name":"retained","enabled":true}]}"#;
                    write!(stream, "HTTP/1.1 200 OK\r\nETag: \"rev-1\"\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
                } else {
                    write!(stream, "HTTP/1.1 304 Not Modified\r\nETag: \"rev-1\"\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").unwrap();
                }
            }
        });
        let repository = UnleashRepository::new(
            UnleashConfig {
                url: format!("http://{address}/api"),
                app_name: "etag-test".into(),
                refresh_interval_nanos: 20_000_000,
                metrics_interval_nanos: 60_000_000_000,
                ..Default::default()
            },
            None,
        )
        .unwrap();
        wait_for_feature(&repository, "retained");
        let mut requests = Vec::new();
        for _ in 0..4 {
            requests.push(rx.recv_timeout(Duration::from_secs(2)).unwrap());
        }
        assert_eq!(requests.iter().filter(|r| r.starts_with("GET ")).count(), 3);
        assert!(
            requests
                .iter()
                .any(|r| r.contains("if-none-match: \"rev-1\"")),
            "{requests:?}"
        );
        assert!(repository.snapshot().get("retained").is_some());
        repository.shutdown();
        server.join().unwrap();
    }
}
