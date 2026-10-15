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

/// Configuration/defaulting from Go common/config/cluster.go. Deprecated
/// fields remain distinct because canonical and legacy keys may coexist.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct GroupMetadata {
    pub failover_version_increment: i64,
    pub primary_cluster_name: String,
    pub master_cluster_name: String,
    pub current_cluster_name: String,
    pub cluster_group: BTreeMap<String, ClusterInformation>,
    pub cluster_information: BTreeMap<String, ClusterInformation>,
    pub enable_global_domain: bool,
    pub cluster_name_for_failover_version_increment: String,
}

impl GroupMetadata {
    pub fn fill_defaults(&mut self) {
        if self.primary_cluster_name.is_empty() && !self.master_cluster_name.is_empty() {
            self.primary_cluster_name
                .clone_from(&self.master_cluster_name);
        }
        let legacy_group = self.cluster_group.is_empty() && !self.cluster_information.is_empty();
        if legacy_group {
            self.cluster_group.clone_from(&self.cluster_information);
        }
        for cluster in self.cluster_group.values_mut() {
            if cluster.rpc_name.is_empty() {
                cluster.rpc_name = "cadence-frontend".into();
            }
            if cluster.rpc_transport.is_empty() {
                cluster.rpc_transport = "tchannel".into();
            }
        }
        // Go assigns the legacy map directly, so defaulted entries are visible
        // through both names after fallback.
        if legacy_group {
            self.cluster_information.clone_from(&self.cluster_group);
        }
    }

    /// Go aggregates all errors; map iteration order is unspecified there.
    pub fn validation_errors(&self) -> Vec<String> {
        let mut errors = Vec::new();
        if self.primary_cluster_name.is_empty() {
            errors.push("primary cluster name is empty".into());
        }
        if self.current_cluster_name.is_empty() {
            errors.push("current cluster name is empty".into());
        }
        if self.failover_version_increment == 0 {
            errors.push("version increment is 0".into());
        }
        if self.cluster_group.is_empty() {
            errors.push("empty cluster group".into());
        }
        if !self.primary_cluster_name.is_empty()
            && !self.cluster_group.contains_key(&self.primary_cluster_name)
        {
            errors.push("primary cluster is not specified in the cluster group".into());
        }
        if !self.current_cluster_name.is_empty()
            && !self.cluster_group.contains_key(&self.current_cluster_name)
        {
            errors.push("current cluster is not specified in the cluster group".into());
        }
        let mut versions = std::collections::BTreeSet::new();
        for (name, info) in &self.cluster_group {
            if name.is_empty() {
                errors.push("cluster with empty name defined".into());
            }
            versions.insert(info.initial_failover_version);
            if self.failover_version_increment <= info.initial_failover_version
                || info.initial_failover_version < 0
            {
                errors.push(format!(
                    "cluster {name}: version increment {} is smaller than initial version: {}",
                    self.failover_version_increment, info.initial_failover_version
                ));
            }
            if info.enabled && (info.rpc_name.is_empty() || info.rpc_address.is_empty()) {
                errors.push(format!("cluster {name}: rpc name / address is empty"));
            }
            if info.rpc_transport != "tchannel" && info.rpc_transport != "grpc" {
                errors.push(format!(
                    "cluster {name}: rpc transport must tchannel or grpc"
                ));
            }
        }
        if versions.len() != self.cluster_group.len() {
            errors.push("initial versions of the cluster group have duplicates".into());
        }
        errors
    }

    pub fn metadata(&self) -> Metadata {
        Metadata {
            failover_version_increment: self.failover_version_increment,
            primary_cluster_name: self.primary_cluster_name.clone(),
            current_cluster_name: self.current_cluster_name.clone(),
            clusters: self.cluster_group.clone(),
        }
    }
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
    fn cluster_config_go_fixtures() {
        let cases: Vec<Value> = serde_json::from_str(include_str!(
            "../../../../tests/fixtures/cluster-config.json"
        ))
        .unwrap();
        for case in cases {
            let (config, mut errors) = if case["input"].is_null() {
                (
                    Value::Null,
                    vec!["ClusterGroupMetadata cannot be empty".to_string()],
                )
            } else {
                let mut config: GroupMetadata =
                    serde_json::from_value(case["input"].clone()).unwrap();
                if case["fillDefaults"].as_bool().unwrap() {
                    config.fill_defaults();
                }
                let errors = config.validation_errors();
                let mut normalized = serde_json::to_value(config).unwrap();
                normalized
                    .as_object_mut()
                    .unwrap()
                    .remove("enableGlobalDomain");
                normalized
                    .as_object_mut()
                    .unwrap()
                    .remove("clusterNameForFailoverVersionIncrement");
                (normalized, errors)
            };
            errors.sort();
            assert_eq!(
                json!({"config": config, "errors": errors}),
                case["expected"],
                "{}",
                case["name"]
            );
        }
    }

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
