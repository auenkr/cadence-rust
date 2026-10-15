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
            config.cluster_metadata.clone_from(&config.legacy_cluster_metadata);
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
        let metadata = self.cluster_metadata.as_ref()
            .ok_or_else(|| anyhow::anyhow!("ClusterGroupMetadata cannot be empty"))?;
        let errors = metadata.validation_errors();
        if !errors.is_empty() { anyhow::bail!(errors.join("; ")); }
        Ok(())
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
        let yaml = format!("{}\nclusterMetadata: {{currentClusterName: shadow}}\n", SAMPLE.replace("clusterMetadata:", "clusterGroupMetadata:"));
        let both = Config::from_yaml(&yaml).unwrap();
        both.validate().unwrap();
        assert_eq!(both.cluster_metadata.as_ref().unwrap().current_cluster_name, "active");
        let explicit_empty = Config::from_yaml(&format!("{SAMPLE}\nclusterGroupMetadata: {{}}\n")).unwrap();
        assert!(explicit_empty.validate().is_err());
        let missing = Config::from_yaml("log: {level: info}").unwrap();
        assert_eq!(missing.validate().unwrap_err().to_string(), "ClusterGroupMetadata cannot be empty");
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
        assert_eq!(cfg.cluster_metadata.as_ref().unwrap().current_cluster_name, "active");
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
    fn rejects_negative_shards() {
        let mut cfg = Config::default();
        cfg.persistence.num_history_shards = -1;
        assert!(cfg.validate().is_err());
    }
}
