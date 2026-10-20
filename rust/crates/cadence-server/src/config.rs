//! Server configuration, mirroring the subset of `go/common/config/config.go`
//! needed to boot: persistence store, per-service RPC addresses, log level,
//! and cluster metadata.
//!
//! Field names match the YAML keys used by `go/config/*.yaml` so existing
//! config files load without modification. Like the Go loader, `${VAR:default}`
//! and `$VAR` expressions are expanded from the environment before parsing.

use std::collections::HashMap;
use std::path::Path;

use serde::{Deserialize, Deserializer, Serialize};

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Config {
    /// Go's dynamicconfig.client provider stanza (filebased is supported for
    /// Matching; other providers are rejected explicitly by the runtime).
    #[serde(default, rename = "dynamicconfig")]
    pub dynamic_config: DynamicConfigConfig,
    /// Legacy spelling retained by Go config files.
    #[serde(default, rename = "dynamicConfigClient")]
    pub legacy_dynamic_config_client: Option<DynamicConfigClientConfig>,
    /// Mirrors Go dynamic config `worker.enableScheduler` (default false):
    /// schedule APIs are rejected until the operator enables them.
    #[serde(default, rename = "enableScheduler")]
    pub enable_scheduler: bool,
    /// Mirrors Go dynamic config `system.enableDistributedSemaphore`
    /// (default false).
    #[serde(default, rename = "enableDistributedSemaphore")]
    pub enable_distributed_semaphore: bool,
    #[serde(default)]
    pub persistence: PersistenceConfig,
    #[serde(default)]
    pub log: LogConfig,
    #[serde(default)]
    pub services: HashMap<String, ServiceConfig>,
    #[serde(default, rename = "clusterGroupMetadata")]
    pub cluster_metadata: Option<ClusterMetadata>,
    #[serde(default, rename = "clusterMetadata")]
    pub legacy_cluster_metadata: Option<ClusterMetadata>,
    /// Ringpop peer discovery options used by Matching. These are parsed here
    /// for Go-compatible configuration; runtime wiring is intentionally separate.
    #[serde(default)]
    pub ringpop: RingpopConfig,
    /// Outbound shard-manager target. This field is intentionally parsed even
    /// before the Matching runtime wires the client, so Go config cannot be
    /// silently ignored by the Rust server.
    #[serde(default, rename = "shardDistributorClient")]
    pub shard_distributor_client: ShardDistributorClientConfig,
    /// Matching's shard-manager namespace/executor settings.
    #[serde(default, rename = "shard-distributor-matching")]
    pub shard_distributor_matching: ShardDistributorMatchingConfig,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardDistributorClientConfig {
    #[serde(default, rename = "hostPort")]
    pub host_port: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardDistributorMatchingConfig {
    #[serde(default)]
    pub namespaces: Vec<ShardDistributorNamespaceConfig>,
    #[serde(default, deserialize_with = "de_go_duration", rename = "peer_ttl")]
    pub peer_ttl_nanos: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardDistributorNamespaceConfig {
    #[serde(default)]
    pub namespace: String,
    #[serde(
        default,
        deserialize_with = "de_go_duration",
        rename = "heartbeat_interval"
    )]
    pub heartbeat_interval_nanos: i64,
    #[serde(default, deserialize_with = "de_go_duration", rename = "ttl_shard")]
    pub ttl_shard_nanos: i64,
    #[serde(default, deserialize_with = "de_go_duration", rename = "ttl_report")]
    pub ttl_report_nanos: i64,
}

impl ShardDistributorMatchingConfig {
    pub const DEFAULT_PEER_TTL_NANOS: i64 = 120_000_000_000;
    pub const DEFAULT_REPORT_TTL_NANOS: i64 = 60_000_000_000;

    /// Go shard-manager uses a two-minute peer TTL when the configured value
    /// is non-positive.
    #[must_use]
    pub fn effective_peer_ttl_nanos(&self) -> i64 {
        if self.peer_ttl_nanos <= 0 {
            Self::DEFAULT_PEER_TTL_NANOS
        } else {
            self.peer_ttl_nanos
        }
    }

    /// Cadence Matching's processor report TTL defaults to one minute; the
    /// value is consumed by the processor factory rather than sent on wire.
    #[must_use]
    pub fn report_ttl_nanos(&self) -> i64 {
        self.namespaces
            .first()
            .map(|namespace| namespace.ttl_report_nanos)
            .filter(|ttl| *ttl != 0)
            .unwrap_or(Self::DEFAULT_REPORT_TTL_NANOS)
    }

    pub fn validate_for_matching(
        &self,
        client: &ShardDistributorClientConfig,
    ) -> anyhow::Result<()> {
        if self.namespaces.len() > 1 {
            anyhow::bail!(
                "Matching supports at most one shard-distributor namespace (got {})",
                self.namespaces.len()
            );
        }
        for namespace in &self.namespaces {
            if namespace.namespace.is_empty() {
                anyhow::bail!("shard-distributor namespace cannot be empty");
            }
            if namespace.heartbeat_interval_nanos <= 0 {
                anyhow::bail!(
                    "shard-distributor namespace {} heartbeat_interval must be greater than 0",
                    namespace.namespace
                );
            }
        }
        if !self.namespaces.is_empty() && client.host_port.trim().is_empty() {
            anyhow::bail!(
                "shardDistributorClient.hostPort is required when Matching shard-distributor namespaces are configured"
            );
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DynamicConfigConfig {
    #[serde(default)]
    pub client: String,
    #[serde(default, rename = "filebased")]
    pub file_based: DynamicConfigClientConfig,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DynamicConfigClientConfig {
    #[serde(default)]
    pub filepath: String,
    #[serde(default, rename = "pollInterval")]
    pub poll_interval: String,
}

impl Config {
    /// Resolve Go's modern/legacy client selection. Empty modern clients defer
    /// to the legacy stanza; nonempty modern clients take precedence.
    pub fn selected_dynamic_config_client(&self) -> Option<(&str, &DynamicConfigClientConfig)> {
        if !self.dynamic_config.client.is_empty() {
            Some((&self.dynamic_config.client, &self.dynamic_config.file_based))
        } else {
            self.legacy_dynamic_config_client
                .as_ref()
                .map(|config| ("filebased", config))
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RingpopConfig {
    #[serde(default)]
    pub name: String,
    #[serde(default, rename = "broadcastAddress")]
    pub broadcast_address: String,
    #[serde(default, rename = "bootstrapMode")]
    pub bootstrap_mode: BootstrapMode,
    #[serde(default, rename = "bootstrapHosts")]
    pub bootstrap_hosts: Vec<String>,
    #[serde(default, rename = "bootstrapFile")]
    pub bootstrap_file: String,
    /// Go time.Duration in signed nanoseconds; zero is replaced with 10 seconds by Validate.
    #[serde(
        default,
        rename = "maxJoinDuration",
        deserialize_with = "de_go_duration"
    )]
    pub max_join_duration_nanos: i64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub enum BootstrapMode {
    #[default]
    None,
    File,
    Hosts,
    Custom,
    Dns,
    DnsSrv,
}

impl<'de> Deserialize<'de> for BootstrapMode {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        use serde::de::Error;
        let s = String::deserialize(d)?;
        match s.to_lowercase().as_str() {
            "hosts" => Ok(Self::Hosts),
            "file" => Ok(Self::File),
            "custom" => Ok(Self::Custom),
            "dns" => Ok(Self::Dns),
            "dns-srv" => Ok(Self::DnsSrv),
            _ => Err(Error::custom(format!(
                "invalid ringpop bootstrap mode {s:?}"
            ))),
        }
    }
}

impl RingpopConfig {
    pub fn validate(&mut self) -> anyhow::Result<()> {
        if self.name.is_empty() {
            anyhow::bail!("ringpop config missing `name` param");
        }
        if self.max_join_duration_nanos == 0 {
            self.max_join_duration_nanos = 10_000_000_000;
        }
        match self.bootstrap_mode {
            BootstrapMode::File if self.bootstrap_file.is_empty() => {
                anyhow::bail!("ringpop config missing bootstrap file param")
            }
            BootstrapMode::Hosts | BootstrapMode::Dns | BootstrapMode::DnsSrv
                if self.bootstrap_hosts.is_empty() =>
            {
                anyhow::bail!("ringpop config missing boostrap hosts param")
            }
            BootstrapMode::Custom => anyhow::bail!("ringpop bootstrapMode custom is not supported"),
            // Go fmt %q applied to the zero-valued integer enum renders the
            // corresponding NUL rune, including the literal escape.
            BootstrapMode::None => {
                anyhow::bail!("ringpop config with unknown boostrap mode '\\x00'")
            }
            _ => {}
        }
        Ok(())
    }
}

fn de_go_duration<'de, D: Deserializer<'de>>(d: D) -> Result<i64, D::Error> {
    use serde::de::Error;
    let value = serde_yaml::Value::deserialize(d)?;
    match value {
        serde_yaml::Value::Null => Ok(0),
        serde_yaml::Value::Number(n) => n
            .as_i64()
            .ok_or_else(|| Error::custom(format!("invalid duration {n:?}"))),
        serde_yaml::Value::String(s) => parse_go_duration(&s).map_err(Error::custom),
        other => Err(Error::custom(format!("invalid duration {other:?}"))),
    }
}

pub fn parse_go_duration(input: &str) -> Result<i64, String> {
    let (negative, mut rest) = if let Some(r) = input.strip_prefix('-') {
        (true, r)
    } else if let Some(r) = input.strip_prefix('+') {
        (false, r)
    } else {
        (false, input)
    };
    if rest.is_empty() {
        return Err(format!("invalid duration {input:?}"));
    }
    if rest == "0" {
        return Ok(0);
    }
    let mut total = 0u128;
    while !rest.is_empty() {
        let number_end = rest
            .find(|c: char| !c.is_ascii_digit() && c != '.')
            .unwrap_or(rest.len());
        if number_end == 0 {
            return Err(format!("invalid duration {input:?}"));
        }
        let number = &rest[..number_end];
        rest = &rest[number_end..];
        let (unit, scale) = [
            ("ns", 1u128),
            ("us", 1_000),
            ("µs", 1_000),
            ("μs", 1_000),
            ("ms", 1_000_000),
            ("s", 1_000_000_000),
            ("m", 60_000_000_000),
            ("h", 3_600_000_000_000),
        ]
        .into_iter()
        .find(|(u, _)| rest.starts_with(u))
        .ok_or_else(|| format!("invalid duration {input:?}"))?;
        if number.matches('.').count() > 1 {
            return Err(format!("invalid duration {input:?}"));
        }
        let (whole, fraction) = number.split_once('.').unwrap_or((number, ""));
        if whole.is_empty() && fraction.is_empty() {
            return Err(format!("invalid duration {input:?}"));
        }
        let whole: u128 = if whole.is_empty() {
            0
        } else {
            whole
                .parse()
                .map_err(|_| format!("invalid duration {input:?}"))?
        };
        let integral = whole
            .checked_mul(scale)
            .ok_or_else(|| format!("invalid duration {input:?}"))?;
        let fraction = if fraction.is_empty() {
            0
        } else {
            if !fraction.bytes().all(|b| b.is_ascii_digit()) {
                return Err(format!("invalid duration {input:?}"));
            }
            // time.ParseDuration accumulates at most a signed-64-bit fraction,
            // grows a float scale as it reads digits, then evaluates
            // float64(f) * (unit / scale) before truncating to nanoseconds.
            let mut numerator = 0u64;
            let mut fraction_scale = 1.0f64;
            let mut overflow = false;
            for digit in fraction.bytes() {
                if overflow {
                    continue;
                }
                if numerator > (i64::MAX as u64) / 10 {
                    overflow = true;
                    continue;
                }
                let next = numerator * 10 + u64::from(digit - b'0');
                if next > (1u64 << 63) {
                    overflow = true;
                    continue;
                }
                numerator = next;
                fraction_scale *= 10.0;
            }
            ((numerator as f64) * (scale as f64 / fraction_scale)) as u128
        };
        total = total
            .checked_add(integral)
            .and_then(|v| v.checked_add(fraction))
            .ok_or_else(|| format!("invalid duration {input:?}"))?;
        rest = &rest[unit.len()..];
    }
    let limit = if negative {
        1u128 << 63
    } else {
        i64::MAX as u128
    };
    if total > limit {
        return Err(format!("invalid duration {input:?}"));
    }
    if negative {
        Ok((-(total as i128)) as i64)
    } else {
        Ok(total as i64)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistenceConfig {
    #[serde(default, rename = "defaultStore")]
    pub default_store: String,
    #[serde(default, rename = "visibilityStore")]
    pub visibility_store: String,
    #[serde(default, rename = "numHistoryShards")]
    pub num_history_shards: i32,
    #[serde(default, rename = "datastores")]
    pub datastores: HashMap<String, DatastoreConfig>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatastoreConfig {
    #[serde(default)]
    pub nosql: Option<NosqlConfig>,
    #[serde(default)]
    pub sql: Option<SqlConfig>,
}

/// Mirrors Go `config.NoSQL`: `hosts` is a CSV string, not a list.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NosqlConfig {
    #[serde(default, rename = "pluginName", alias = "plugin")]
    pub plugin_name: String,
    #[serde(default)]
    pub hosts: String,
    #[serde(default)]
    pub port: i32,
    #[serde(default)]
    pub user: String,
    #[serde(default)]
    pub password: String,
    #[serde(default)]
    pub keyspace: String,
}

impl NosqlConfig {
    /// Split the CSV `hosts` value the way Go callers do.
    #[must_use]
    #[allow(dead_code)]
    pub fn hosts_list(&self) -> Vec<String> {
        self.hosts
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .collect()
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqlConfig {
    #[serde(default, rename = "pluginName")]
    pub plugin_name: String,
    #[serde(default, rename = "databaseName")]
    pub database_name: String,
    #[serde(default, rename = "connectAddr")]
    pub connect_addr: String,
    #[serde(default, rename = "connectAttributes")]
    pub connect_attributes: HashMap<String, String>,
    /// Go SQL persistence uses this for physical database routing. Matching's
    /// current Rust task adapter supports only the single database case.
    #[serde(default, rename = "nShards")]
    pub n_shards: i32,
    #[serde(default, rename = "useMultipleDatabases")]
    pub use_multiple_databases: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogConfig {
    #[serde(default = "default_log_level")]
    pub level: String,
    #[serde(default)]
    pub stdout: bool,
}

fn default_log_level() -> String {
    "info".to_string()
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            level: default_log_level(),
            stdout: true,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServiceConfig {
    #[serde(default)]
    pub rpc: RpcConfig,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RpcConfig {
    #[serde(default, deserialize_with = "de_flex_u16")]
    pub port: u16,
    #[serde(default, rename = "grpcPort", deserialize_with = "de_flex_u16")]
    pub grpc_port: u16,
    #[serde(default, rename = "grpcMaxMsgSize")]
    pub grpc_max_msg_size: usize,
    #[serde(default)]
    pub host: String,
    #[serde(default, rename = "bindOnLocalHost")]
    pub bind_on_local_host: bool,
    #[serde(default, rename = "bindOnIP")]
    pub bind_on_ip: String,
    #[serde(default)]
    pub http: Option<HttpConfig>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HttpConfig {
    #[serde(default, deserialize_with = "de_flex_u16")]
    pub port: u16,
    #[serde(default)]
    pub procedures: Vec<String>,
    #[serde(default)]
    pub tls: Option<TlsConfig>,
    #[serde(default, rename = "TLSMode")]
    pub tls_mode: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TlsConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default, rename = "certFile")]
    pub cert_file: String,
    #[serde(default, rename = "keyFile")]
    pub key_file: String,
    #[serde(default, rename = "caFile")]
    pub ca_file: String,
    #[serde(default, rename = "caFiles")]
    pub ca_files: Vec<String>,
    #[serde(default, rename = "enableHostVerification")]
    pub enable_host_verification: bool,
    #[serde(default, rename = "requireClientAuth")]
    pub require_client_auth: bool,
    #[serde(default, rename = "serverName")]
    pub server_name: String,
    #[serde(default, rename = "sslmode")]
    pub ssl_mode: String,
}

fn de_flex_u16<'de, D: Deserializer<'de>>(d: D) -> Result<u16, D::Error> {
    use serde::de::Error;
    let v = serde_yaml::Value::deserialize(d)?;
    match v {
        serde_yaml::Value::Number(n) => n
            .as_u64()
            .and_then(|n| u16::try_from(n).ok())
            .ok_or_else(|| Error::custom(format!("invalid port {n:?}"))),
        serde_yaml::Value::String(s) => s
            .trim()
            .parse::<u16>()
            .map_err(|_| Error::custom(format!("invalid port {s:?}"))),
        serde_yaml::Value::Null => Ok(0),
        other => Err(Error::custom(format!("invalid port {other:?}"))),
    }
}

pub type ClusterMetadata = cadence_common::cluster::GroupMetadata;

impl Config {
    /// Embedded development defaults keep the executable independent of the
    /// reference Go checkout. Explicit files still use the Go-shaped parser.
    pub fn development() -> anyhow::Result<Self> {
        Self::from_yaml(include_str!("../../../config/development.yaml"))
    }

    pub fn from_file(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let text = std::fs::read_to_string(path.as_ref())?;
        Self::from_yaml(&text)
    }

    pub fn from_yaml(text: &str) -> anyhow::Result<Self> {
        let mut config: Self = serde_yaml::from_str(&expand_env(text))?;
        if config.cluster_metadata.is_none() {
            config
                .cluster_metadata
                .clone_from(&config.legacy_cluster_metadata);
        }
        if let Some(metadata) = config.cluster_metadata.as_mut() {
            metadata.fill_defaults();
        }
        Ok(config)
    }

    /// Service names this binary knows how to run.
    /// Mirrors the four Cadence services plus the shard distributor.
    #[must_use]
    #[allow(dead_code)]
    pub fn known_services() -> &'static [&'static str] {
        &[
            "frontend",
            "history",
            "matching",
            "worker",
            "sharddistributor",
        ]
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        if self.persistence.num_history_shards < 0 {
            anyhow::bail!("numHistoryShards cannot be negative");
        }
        let metadata = self
            .cluster_metadata
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("ClusterGroupMetadata cannot be empty"))?;
        let errors = metadata.validation_errors();
        if !errors.is_empty() {
            anyhow::bail!(errors.join("; "));
        }
        self.shard_distributor_matching
            .validate_for_matching(&self.shard_distributor_client)?;
        Ok(())
    }

    pub fn validate_ringpop(&mut self) -> anyhow::Result<()> {
        self.ringpop.validate()
    }
}

/// Expand `${VAR:default}`, `${VAR}`, and `$VAR` from the environment,
/// mirroring the Go config loader's envsubst behavior.
fn expand_env(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut chars = text.chars().peekable();
    while let Some(c) = chars.next() {
        if c != '$' {
            out.push(c);
            continue;
        }
        if chars.peek() == Some(&'{') {
            chars.next();
            let mut expr = String::new();
            for ch in chars.by_ref() {
                if ch == '}' {
                    break;
                }
                expr.push(ch);
            }
            let (var, default) = match expr.split_once(":-") {
                Some((v, d)) => (v, Some(d)),
                None => match expr.split_once(':') {
                    Some((v, d)) => (v, Some(d)),
                    None => (expr.as_str(), None),
                },
            };
            let value = std::env::var(var).unwrap_or_else(|_| default.unwrap_or("").to_string());
            out.push_str(&value);
            continue;
        }
        let mut var = String::new();
        while let Some(&ch) = chars.peek() {
            if ch.is_ascii_alphanumeric() || ch == '_' {
                var.push(ch);
                chars.next();
            } else {
                break;
            }
        }
        if var.is_empty() {
            out.push('$');
        } else {
            out.push_str(&std::env::var(&var).unwrap_or_default());
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dynamic_config_modern_client_takes_precedence_and_legacy_is_supported() {
        let modern = Config::from_yaml("dynamicconfig:\n  client: filebased\n  filebased:\n    filepath: modern.yaml\n    pollInterval: 7s\ndynamicConfigClient:\n  filepath: legacy.yaml\n").unwrap();
        let (client, provider) = modern.selected_dynamic_config_client().unwrap();
        assert_eq!(client, "filebased");
        assert_eq!(provider.filepath, "modern.yaml");
        assert_eq!(provider.poll_interval, "7s");
        let legacy = Config::from_yaml("dynamicConfigClient:\n  filepath: legacy.yaml\n").unwrap();
        assert_eq!(legacy.selected_dynamic_config_client().unwrap().1.filepath, "legacy.yaml");
        assert_eq!(parse_go_duration("2s").unwrap(), 2_000_000_000);
    }

    const SAMPLE: &str = r#"
persistence:
  defaultStore: default
  visibilityStore: visibility
  numHistoryShards: 4
  datastores:
    default:
      sql:
        pluginName: sqlite
        databaseName: cadence
        connectAddr: ":memory:"
log:
  stdout: true
  level: info
services:
  frontend:
    rpc:
      port: 1234
clusterMetadata:
  enableGlobalDomain: false
  primaryClusterName: active
  currentClusterName: active
  failoverVersionIncrement: 10
  clusterGroup:
    active:
      enabled: true
      initialFailoverVersion: 0
      rpcAddress: localhost:1234
"#;

    #[test]
    fn canonical_and_legacy_cluster_keys_use_go_precedence() {
        let legacy = Config::from_yaml(SAMPLE).unwrap();
        legacy.validate().unwrap();
        let cluster = &legacy.cluster_metadata.as_ref().unwrap().cluster_group["active"];
        assert_eq!(cluster.rpc_name, "cadence-frontend");
        assert_eq!(cluster.rpc_transport, "tchannel");
        let yaml = format!(
            "{}\nclusterMetadata: {{currentClusterName: shadow}}\n",
            SAMPLE.replace("clusterMetadata:", "clusterGroupMetadata:")
        );
        let both = Config::from_yaml(&yaml).unwrap();
        both.validate().unwrap();
        assert_eq!(
            both.cluster_metadata.as_ref().unwrap().current_cluster_name,
            "active"
        );
        let explicit_empty =
            Config::from_yaml(&format!("{SAMPLE}\nclusterGroupMetadata: {{}}\n")).unwrap();
        assert!(explicit_empty.validate().is_err());
        let missing = Config::from_yaml("log: {level: info}").unwrap();
        assert_eq!(
            missing.validate().unwrap_err().to_string(),
            "ClusterGroupMetadata cannot be empty"
        );
    }

    #[test]
    fn embedded_defaults_match_go_development_config() {
        let embedded = Config::development().unwrap();
        let reference =
            Config::from_yaml(include_str!("../../../../go/config/development.yaml")).unwrap();
        assert_eq!(embedded, reference);
        embedded.validate().unwrap();
    }

    #[test]
    fn parses_go_style_config() {
        let cfg = Config::from_yaml(SAMPLE).unwrap();
        assert_eq!(cfg.persistence.num_history_shards, 4);
        assert_eq!(cfg.services["frontend"].rpc.port, 1234);
        assert_eq!(
            cfg.cluster_metadata.as_ref().unwrap().current_cluster_name,
            "active"
        );
        cfg.validate().unwrap();
    }

    #[test]
    fn parses_go_env_templated_ports() {
        let yaml = "services:\n  frontend:\n    rpc:\n      port: ${FRONTEND_PORT:7933}\n";
        let cfg = Config::from_yaml(yaml).unwrap();
        assert_eq!(cfg.services["frontend"].rpc.port, 7933);
    }

    #[test]
    fn parses_cassandra_csv_hosts() {
        let yaml = "persistence:\n  datastores:\n    cass-default:\n      nosql:\n        pluginName: cassandra\n        hosts: \"127.0.0.1, 127.0.0.2\"\n        keyspace: cadence\n";
        let cfg = Config::from_yaml(yaml).unwrap();
        let ds = &cfg.persistence.datastores["cass-default"];
        assert_eq!(
            ds.nosql.as_ref().unwrap().hosts_list(),
            vec!["127.0.0.1".to_string(), "127.0.0.2".to_string()]
        );
    }

    #[test]
    fn parses_real_go_base_config_subset() {
        // go/config/base.yaml only sets log today; ensure unknown keys are tolerated.
        let text = std::fs::read_to_string(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../../go/config/base.yaml"
        ));
        match text {
            Ok(t) => {
                let cfg = Config::from_yaml(&t).unwrap();
                assert_eq!(cfg.log.level, "info");
            }
            Err(_) => {
                // go/ checkout without config (e.g. sparse CI) — sample still covers parsing.
            }
        }
    }

    #[test]
    fn parses_real_go_development_config() {
        if let Ok(t) = std::fs::read_to_string(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../../go/config/development.yaml"
        )) {
            let cfg = Config::from_yaml(&t).unwrap();
            cfg.validate().unwrap();
            assert_eq!(cfg.persistence.num_history_shards, 4);
            assert_eq!(
                cfg.services["frontend"].rpc.port,
                std::env::var("FRONTEND_PORT")
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(7933)
            );
        }
    }

    #[test]
    fn shard_distributor_config_matches_go_names_and_duration_semantics() {
        let yaml = r#"
clusterMetadata:
  enableGlobalDomain: false
  primaryClusterName: active
  currentClusterName: active
  failoverVersionIncrement: 10
  clusterGroup:
    active: {enabled: true, initialFailoverVersion: 0, rpcAddress: localhost:7933}
shardDistributorClient: {hostPort: "localhost:7943"}
shard-distributor-matching:
  peer_ttl: 3m
  namespaces:
    - namespace: cadence-matching-dev
      heartbeat_interval: 1s
      ttl_shard: 5m
      ttl_report: 30s
"#;
        let cfg = Config::from_yaml(yaml).unwrap();
        cfg.validate().unwrap();
        assert_eq!(cfg.shard_distributor_client.host_port, "localhost:7943");
        assert_eq!(
            cfg.shard_distributor_matching.peer_ttl_nanos,
            180_000_000_000
        );
        assert_eq!(cfg.shard_distributor_matching.namespaces.len(), 1);
        let namespace = &cfg.shard_distributor_matching.namespaces[0];
        assert_eq!(namespace.namespace, "cadence-matching-dev");
        assert_eq!(namespace.heartbeat_interval_nanos, 1_000_000_000);
        assert_eq!(namespace.ttl_shard_nanos, 300_000_000_000);
        assert_eq!(namespace.ttl_report_nanos, 30_000_000_000);
        assert_eq!(
            cfg.shard_distributor_matching.report_ttl_nanos(),
            30_000_000_000
        );
        assert_eq!(
            cfg.shard_distributor_matching.effective_peer_ttl_nanos(),
            180_000_000_000
        );
    }

    #[test]
    fn shard_distributor_matching_validation_handles_zero_one_and_multiple_namespaces() {
        let mut cfg = Config::development().unwrap();
        cfg.shard_distributor_matching.namespaces.clear();
        cfg.shard_distributor_client.host_port.clear();
        cfg.validate().unwrap();
        assert_eq!(
            cfg.shard_distributor_matching.report_ttl_nanos(),
            60_000_000_000
        );
        assert_eq!(
            cfg.shard_distributor_matching.effective_peer_ttl_nanos(),
            120_000_000_000
        );

        cfg.shard_distributor_matching.namespaces = vec![ShardDistributorNamespaceConfig {
            namespace: "matching".into(),
            heartbeat_interval_nanos: 1,
            ttl_shard_nanos: 0,
            ttl_report_nanos: 0,
        }];
        assert!(
            cfg.validate().is_err(),
            "configured namespace requires client address"
        );
        cfg.shard_distributor_client.host_port = "localhost:7943".into();
        cfg.validate().unwrap();
        assert_eq!(
            cfg.shard_distributor_matching.report_ttl_nanos(),
            60_000_000_000
        );

        cfg.shard_distributor_matching
            .namespaces
            .push(ShardDistributorNamespaceConfig {
                namespace: "another".into(),
                heartbeat_interval_nanos: 1,
                ttl_shard_nanos: 0,
                ttl_report_nanos: 0,
            });
        assert!(
            cfg.validate().is_err(),
            "Matching only supports one namespace"
        );
    }

    #[test]
    fn shard_distributor_rejects_invalid_namespace_and_heartbeat_interval() {
        let mut cfg = Config::development().unwrap();
        cfg.shard_distributor_matching.namespaces[0].heartbeat_interval_nanos = 0;
        assert!(
            cfg.validate()
                .unwrap_err()
                .to_string()
                .contains("heartbeat_interval")
        );
        cfg.shard_distributor_matching.namespaces[0].heartbeat_interval_nanos = 1;
        cfg.shard_distributor_matching.namespaces[0]
            .namespace
            .clear();
        assert!(
            cfg.validate()
                .unwrap_err()
                .to_string()
                .contains("cannot be empty")
        );
    }

    #[test]
    fn rejects_negative_shards() {
        let mut cfg = Config::default();
        cfg.persistence.num_history_shards = -1;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn ringpop_configuration_maps_defaults_validation_and_signed_values() {
        let mut cfg = Config::from_yaml("ringpop: {name: matching, bootstrapMode: HoStS, bootstrapHosts: [' x:1 ', 'x:1'], maxJoinDuration: 30s}").unwrap();
        assert_eq!(cfg.ringpop.bootstrap_mode, BootstrapMode::Hosts);
        assert_eq!(cfg.ringpop.bootstrap_hosts, [" x:1 ", "x:1"]);
        assert_eq!(cfg.ringpop.max_join_duration_nanos, 30_000_000_000);
        cfg.validate_ringpop().unwrap();
        let mut zero =
            Config::from_yaml("ringpop: {name: x, bootstrapMode: file, bootstrapFile: seeds.json}")
                .unwrap();
        zero.validate_ringpop().unwrap();
        assert_eq!(zero.ringpop.max_join_duration_nanos, 10_000_000_000);
        let mut negative = Config::from_yaml(
            "ringpop: {name: x, bootstrapMode: hosts, bootstrapHosts: [a], maxJoinDuration: -2ms}",
        )
        .unwrap();
        negative.validate_ringpop().unwrap();
        assert_eq!(negative.ringpop.max_join_duration_nanos, -2_000_000);
        let err = Config::from_yaml("ringpop: {name: x, bootstrapMode: custom}")
            .unwrap()
            .ringpop;
        assert!(
            err.clone()
                .validate()
                .unwrap_err()
                .to_string()
                .contains("custom is not supported")
        );
        let missing = Config::from_yaml("ringpop: {name: x, bootstrapMode: hosts}")
            .unwrap()
            .ringpop;
        assert!(
            missing
                .clone()
                .validate()
                .unwrap_err()
                .to_string()
                .contains("boostrap hosts")
        );
        assert!(Config::from_yaml("ringpop: {name: x, bootstrapMode: weird}").is_err());
    }
}
