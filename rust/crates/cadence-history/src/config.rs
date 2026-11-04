//! History service configuration values used by shard acquisition.
//! Dynamic property names/defaults mirror `go/service/history/config` and
//! `go/common/dynamicconfig/dynamicproperties/constants.go`.

use serde::Deserialize;

#[derive(Clone, Debug, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Config {
    pub num_history_shards: u32,
    pub range_size_bits: u32,
    pub acquire_shard_interval: String,
    pub acquire_shard_concurrency: usize,
    pub shutdown_drain_duration: String,
    pub history_rps: i64,
    pub history_persistence_max_qps: i64,
    pub history_persistence_global_max_qps: i64,
    pub history_cache_initial_size: i64,
    pub history_cache_max_size: i64,
    pub history_cache_ttl: String,
    pub events_cache_initial_count: i64,
    pub events_cache_max_count: i64,
    pub events_cache_ttl: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            num_history_shards: 4_096,
            range_size_bits: 20,
            acquire_shard_interval: "1m".into(),
            acquire_shard_concurrency: 1,
            shutdown_drain_duration: "0s".into(),
            history_rps: 3000,
            history_persistence_max_qps: 9000,
            history_persistence_global_max_qps: 0,
            history_cache_initial_size: 128,
            history_cache_max_size: 512,
            history_cache_ttl: "1h".into(),
            events_cache_initial_count: 128,
            events_cache_max_count: 512,
            events_cache_ttl: "1h".into(),
        }
    }
}

impl Config {
    pub fn from_yaml(yaml: &str) -> anyhow::Result<Self> {
        Ok(serde_yaml::from_str(yaml)?)
    }
}

#[cfg(test)]
mod tests {
    use super::Config;
    #[test]
    fn go_history_defaults_are_present() {
        let c = Config::default();
        assert_eq!(c.num_history_shards, 4096);
        assert_eq!(c.range_size_bits, 20);
        assert_eq!(c.acquire_shard_interval, "1m");
        assert_eq!(c.acquire_shard_concurrency, 1);
        assert_eq!(c.history_rps, 3000);
        assert_eq!(Config::from_yaml("numHistoryShards: 1024").unwrap().num_history_shards, 1024);
    }
}
