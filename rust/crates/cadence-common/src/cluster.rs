//! Version ownership rules from Go common/cluster/metadata.go.
use std::collections::BTreeMap;

#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct ClusterInformation {
    pub enabled: bool,
    pub initial_failover_version: i64,
    pub new_initial_failover_version: Option<i64>,
    pub rpc_name: String,
    pub rpc_address: String,
    pub rpc_transport: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Metadata {
    pub failover_version_increment: i64,
    pub primary_cluster_name: String,
    pub current_cluster_name: String,
    pub clusters: BTreeMap<String, ClusterInformation>,
}

impl Default for Metadata {
    fn default() -> Self {
        Self {
            failover_version_increment: 10,
            primary_cluster_name: "cluster0".into(),
            current_cluster_name: "cluster0".into(),
            clusters: BTreeMap::from([(
                "cluster0".into(),
                ClusterInformation {
                    enabled: true,
                    ..Default::default()
                },
            )]),
        }
    }
}

impl Metadata {
    /// The dynamic migration override is evaluated by the caller for its domain.
    pub fn next_failover_version(
        &self,
        target: &str,
        current: i64,
        use_new: bool,
    ) -> Result<i64, String> {
        let info = self
            .clusters
            .get(target)
            .ok_or_else(|| format!("Invalid cluster name: {target}"))?;
        let initial = if use_new {
            info.new_initial_failover_version
                .unwrap_or(info.initial_failover_version)
        } else {
            info.initial_failover_version
        };
        // Go's signed arithmetic wraps and integer division truncates toward zero.
        let increment = self.failover_version_increment;
        if increment == 0 {
            return Err("version increment is 0".into());
        }
        let version = current
            .wrapping_div(increment)
            .wrapping_mul(increment)
            .wrapping_add(initial);
        Ok(if version < current {
            version.wrapping_add(increment)
        } else {
            version
        })
    }

    fn resolve(&self, version: i64) -> Result<&str, String> {
        if self.failover_version_increment == 0 {
            return Err("version increment is 0".into());
        }
        let remainder = version.wrapping_rem(self.failover_version_increment);
        // Original offsets take precedence over migration offsets.
        self.clusters
            .iter()
            .find(|(_, c)| c.initial_failover_version == remainder)
            .or_else(|| {
                self.clusters
                    .iter()
                    .find(|(_, c)| c.new_initial_failover_version == Some(remainder))
            })
            .map(|(name, _)| name.as_str())
            .ok_or_else(|| format!("could not resolve failover version: {version}"))
    }

    pub fn cluster_name_for_failover_version(&self, version: i64) -> Result<&str, String> {
        if version == -24 {
            return Ok(&self.current_cluster_name);
        }
        self.resolve(version)
            .map_err(|e| format!("failed to resolve failover version to a cluster: {e}"))
    }

    pub fn is_version_from_same_cluster(&self, first: i64, second: i64) -> bool {
        match (self.resolve(first), self.resolve(second)) {
            (Ok(first), Ok(second)) => first == second,
            _ => false,
        }
    }

    pub fn enabled_clusters(&self) -> impl Iterator<Item = (&String, &ClusterInformation)> {
        self.clusters.iter().filter(|(_, c)| c.enabled)
    }

    pub fn remote_clusters(&self) -> impl Iterator<Item = (&String, &ClusterInformation)> {
        self.enabled_clusters()
            .filter(|(name, _)| *name != &self.current_cluster_name)
    }

    pub fn is_primary_cluster(&self) -> bool {
        self.primary_cluster_name == self.current_cluster_name
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};

    #[test]
    fn cluster_metadata_go_fixtures() {
        let cases: Vec<Value> = serde_json::from_str(include_str!(
            "../../../../tests/fixtures/cluster-metadata.json"
        ))
        .unwrap();
        let metadata = Metadata {
            failover_version_increment: 10,
            primary_cluster_name: "a".into(),
            current_cluster_name: "a".into(),
            clusters: BTreeMap::from([
                (
                    "a".into(),
                    ClusterInformation {
                        enabled: true,
                        initial_failover_version: 0,
                        new_initial_failover_version: Some(3),
                        ..Default::default()
                    },
                ),
                (
                    "b".into(),
                    ClusterInformation {
                        enabled: true,
                        initial_failover_version: 2,
                        ..Default::default()
                    },
                ),
                (
                    "disabled".into(),
                    ClusterInformation {
                        enabled: false,
                        initial_failover_version: 4,
                        ..Default::default()
                    },
                ),
            ]),
        };
        for case in cases {
            let current = case["current"].as_i64().unwrap_or(0);
            let actual = match case["op"].as_str().unwrap() {
                "next" => json!(metadata
                    .next_failover_version(
                        case["target"].as_str().unwrap(),
                        current,
                        case["useNew"].as_bool().unwrap_or(false)
                    )
                    .unwrap()),
                "resolve" => match metadata.cluster_name_for_failover_version(current) {
                    Ok(name) => json!(name),
                    Err(e) => json!({"error": e}),
                },
                "same" => json!(metadata
                    .is_version_from_same_cluster(current, case["other"].as_i64().unwrap_or(0))),
                "sets" => {
                    json!({"primary": metadata.is_primary_cluster(), "enabled": metadata.enabled_clusters().count(), "remote": metadata.remote_clusters().count(), "all": metadata.clusters.len()})
                }
                op => panic!("unknown fixture op {op}"),
            };
            assert_eq!(actual, case["expected"], "{}", case["name"]);
        }
    }
}
