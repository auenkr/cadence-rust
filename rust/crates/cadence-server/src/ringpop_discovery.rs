//! Go-compatible static-host and JSON-file Ringpop seed discovery.
//! DNS and DNS-SRV modes remain deliberately unimplemented here.

use crate::config::{BootstrapMode, RingpopConfig};
use std::{fs, path::PathBuf};

pub trait DiscoverProvider {
    fn hosts(&self) -> Result<Vec<String>, DiscoveryError>;
}

#[derive(Debug, thiserror::Error)]
pub enum DiscoveryError {
    #[error(transparent)] Io(#[from] std::io::Error),
    #[error("JSON syntax error: {0}")]
    JsonSyntax(serde_json::Error),
    #[error("JSON value has the wrong type: {0}")]
    JsonType(serde_json::Error),
    #[error("unknown bootstrap mode {0:?}")]
    UnsupportedMode(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StaticHosts { hosts: Vec<String> }

impl StaticHosts {
    pub fn new(hosts: impl IntoIterator<Item = String>) -> Self { Self { hosts: hosts.into_iter().collect() } }
}

impl DiscoverProvider for StaticHosts {
    fn hosts(&self) -> Result<Vec<String>, DiscoveryError> { Ok(self.hosts.clone()) }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JsonFile { path: PathBuf }

impl JsonFile {
    pub fn new(path: impl Into<PathBuf>) -> Self { Self { path: path.into() } }
    pub fn path(&self) -> &std::path::Path { &self.path }
}

impl DiscoverProvider for JsonFile {
    fn hosts(&self) -> Result<Vec<String>, DiscoveryError> {
        let data = fs::read(&self.path)?;
        serde_json::from_slice(&data).map_err(|error| match error.classify() {
            serde_json::error::Category::Syntax | serde_json::error::Category::Eof => DiscoveryError::JsonSyntax(error),
            serde_json::error::Category::Data | serde_json::error::Category::Io => DiscoveryError::JsonType(error),
        })
    }
}

pub enum Provider { Static(StaticHosts), File(JsonFile) }

impl Provider {
    pub fn from_config(config: &RingpopConfig) -> Result<Self, DiscoveryError> {
        match config.bootstrap_mode {
            BootstrapMode::Hosts => Ok(Self::Static(StaticHosts::new(config.bootstrap_hosts.clone()))),
            BootstrapMode::File => Ok(Self::File(JsonFile::new(config.bootstrap_file.clone()))),
            BootstrapMode::Dns => Err(DiscoveryError::UnsupportedMode("dns".into())),
            BootstrapMode::DnsSrv => Err(DiscoveryError::UnsupportedMode("dns-srv".into())),
            BootstrapMode::Custom => Err(DiscoveryError::UnsupportedMode("custom".into())),
            BootstrapMode::None => Err(DiscoveryError::UnsupportedMode("0".into())),
        }
    }
}

impl DiscoverProvider for Provider {
    fn hosts(&self) -> Result<Vec<String>, DiscoveryError> {
        match self { Self::Static(provider) => provider.hosts(), Self::File(provider) => provider.hosts() }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn static_hosts_preserve_order_duplicates_and_strings() {
        let p = StaticHosts::new([" host:1 ".to_string(), "host:1".into(), "raw".into()]);
        assert_eq!(p.hosts().unwrap(), [" host:1 ", "host:1", "raw"]);
        assert_eq!(p.hosts().unwrap(), [" host:1 ", "host:1", "raw"]);
    }

    #[test]
    fn file_reads_each_time_and_preserves_array_contents() {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(br#"["a:1","a:1"," raw "]"#).unwrap();
        let p = JsonFile::new(f.path());
        assert_eq!(p.hosts().unwrap(), ["a:1", "a:1", " raw "]);
        fs::write(f.path(), br#"["updated:2"]"#).unwrap();
        assert_eq!(p.hosts().unwrap(), ["updated:2"]);
        fs::write(f.path(), br#"["a","#).unwrap();
        assert!(matches!(p.hosts(), Err(DiscoveryError::JsonSyntax(_))));
        fs::write(f.path(), b"{}").unwrap();
        assert!(matches!(p.hosts(), Err(DiscoveryError::JsonType(_))));
    }

    #[test]
    fn missing_file_is_io_error_and_config_selects_supported_provider() {
        assert!(matches!(JsonFile::new("/does/not/exist").hosts(), Err(DiscoveryError::Io(_))));
        let cfg = crate::config::Config::from_yaml("ringpop: {name: matching, bootstrapMode: hosts, bootstrapHosts: [' x ', 'x']}").unwrap();
        let provider = Provider::from_config(&cfg.ringpop).unwrap();
        assert_eq!(provider.hosts().unwrap(), [" x ", "x"]);
    }

    #[test]
    fn matches_actual_go_provider_oracle_fixture() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!("../../../../tests/fixtures/ringpop_bootstrap_provider.json")).unwrap();
        let empty = tempfile::NamedTempFile::new().unwrap();
        fs::write(empty.path(), b"[]").unwrap();
        assert!(JsonFile::new(empty.path()).hosts().unwrap().is_empty());
        let static_hosts: Vec<String> = serde_json::from_value(fixture["static"].clone()).unwrap();
        assert_eq!(StaticHosts::new(static_hosts).hosts().unwrap(), serde_json::from_value::<Vec<String>>(fixture["static"].clone()).unwrap());
        let file_hosts: Vec<String> = serde_json::from_value(fixture["file"].clone()).unwrap();
        let f = tempfile::NamedTempFile::new().unwrap();
        fs::write(f.path(), serde_json::to_vec(&file_hosts).unwrap()).unwrap();
        let file = JsonFile::new(f.path());
        assert_eq!(file.hosts().unwrap(), serde_json::from_value::<Vec<String>>(fixture["file"].clone()).unwrap());
        let updated: Vec<String> = serde_json::from_value(fixture["fileAfterUpdate"].clone()).unwrap();
        fs::write(f.path(), serde_json::to_vec(&updated).unwrap()).unwrap();
        assert_eq!(file.hosts().unwrap(), updated);
        fs::write(f.path(), b"{}").unwrap();
        assert!(matches!(file.hosts(), Err(DiscoveryError::JsonType(_))));
        let malformed_error = fixture["malformedErrorType"].as_str().unwrap();
        assert_eq!(malformed_error, "*json.SyntaxError");
        fs::write(f.path(), br#"["a","#).unwrap();
        assert!(matches!(file.hosts(), Err(DiscoveryError::JsonSyntax(_))));
        let file_config = RingpopConfig { bootstrap_mode: BootstrapMode::File, bootstrap_file: f.path().to_string_lossy().into_owned(), ..Default::default() };
        assert!(matches!(Provider::from_config(&file_config).unwrap(), Provider::File(_)));
        let cfg = crate::config::Config::from_yaml("ringpop: {name: matching, bootstrapMode: hosts, bootstrapHosts: [' x ', x, x]}").unwrap();
        assert_eq!(cfg.ringpop.bootstrap_hosts, serde_json::from_value::<Vec<String>>(fixture["configHosts"].clone()).unwrap());
        let mut cfg = cfg;
        cfg.validate_ringpop().unwrap();
        assert_eq!(cfg.ringpop.max_join_duration_nanos, fixture["configMaxJoinDurationNanos"].as_i64().unwrap());
        let config_errors = &fixture["configErrors"];
        let mut missing_name = crate::config::RingpopConfig::default();
        assert_eq!(missing_name.validate().unwrap_err().to_string(), config_errors["missingName"]);
        let mut unknown = crate::config::RingpopConfig { name: "x".into(), ..Default::default() };
        assert_eq!(unknown.validate().unwrap_err().to_string(), config_errors["unknownMode"]);
        let mut missing_file = crate::config::RingpopConfig { name: "x".into(), bootstrap_mode: BootstrapMode::File, ..Default::default() };
        assert_eq!(missing_file.validate().unwrap_err().to_string(), config_errors["missingFile"]);
        let mut missing_hosts = crate::config::RingpopConfig { name: "x".into(), bootstrap_mode: BootstrapMode::Hosts, ..Default::default() };
        assert_eq!(missing_hosts.validate().unwrap_err().to_string(), config_errors["missingHosts"]);
        let mut custom = crate::config::RingpopConfig { name: "x".into(), bootstrap_mode: BootstrapMode::Custom, ..Default::default() };
        assert_eq!(custom.validate().unwrap_err().to_string(), config_errors["custom"]);
        for (key, source) in [("max", "9223372036854775807ns"), ("min", "-9223372036854775808ns"), ("fraction", "1.000000001s")] {
            let config: crate::config::Config = crate::config::Config::from_yaml(&format!("ringpop: {{name: x, bootstrapMode: hosts, bootstrapHosts: [x], maxJoinDuration: {source}}}")).unwrap();
            assert_eq!(config.ringpop.max_join_duration_nanos, fixture["durations"][key].as_i64().unwrap());
        }
    }
}
