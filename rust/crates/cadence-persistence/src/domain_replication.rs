//! Active-active domain state rules from Go common/domain/{handler,clusterattributes}.go.
use std::collections::HashMap;

use cadence_api::errors::BadRequestError;
use cadence_api::types::{ActiveClusters, ClusterAttributeScope, DomainReplicationConfiguration};
use cadence_common::cluster::Metadata;

type Result<T> = std::result::Result<T, BadRequestError>;

pub fn registration_attributes(
    metadata: &Metadata,
    global: bool,
    incoming: Option<&ActiveClusters>,
) -> Result<Option<ActiveClusters>> {
    if !global {
        return Ok(None);
    }
    let Some(scopes) = incoming.and_then(|c| c.active_clusters_by_cluster_attribute.as_ref())
    else {
        return Ok(None);
    };
    if scopes.is_empty() {
        return Ok(None);
    }
    let mut result = HashMap::new();
    for (scope, data) in scopes {
        let mut attributes = HashMap::new();
        for (name, info) in data.cluster_attributes.iter().flatten() {
            let cluster = metadata.clusters.get(&info.active_cluster_name).ok_or_else(|| BadRequestError::new(format!(
                "Cluster {} not found. Domain cannot be registered in this cluster for scope {scope:?} and attribute {name:?}", info.active_cluster_name)))?;
            let mut info = info.clone();
            info.failover_version = cluster.initial_failover_version;
            attributes.insert(name.clone(), info);
        }
        result.insert(
            scope.clone(),
            ClusterAttributeScope {
                cluster_attributes: Some(attributes),
            },
        );
    }
    Ok(Some(ActiveClusters {
        active_clusters_by_region: None,
        active_clusters_by_cluster_attribute: Some(result),
    }))
}

/// Validation precedes caller-version replacement in Go's update handler.
pub fn validate_incoming(metadata: &Metadata, incoming: Option<&ActiveClusters>) -> Result<()> {
    for scope in incoming
        .and_then(|c| c.active_clusters_by_cluster_attribute.as_ref())
        .iter()
        .flat_map(|s| s.values())
    {
        for info in scope.cluster_attributes.iter().flat_map(|a| a.values()) {
            if !metadata
                .clusters
                .get(&info.active_cluster_name)
                .is_some_and(|c| c.enabled)
            {
                return Err(BadRequestError::new(format!(
                    "Invalid active cluster name: {}",
                    info.active_cluster_name
                )));
            }
            if info.failover_version < 0 {
                return Err(BadRequestError::new(format!(
                    "invalid failover version: {}",
                    info.failover_version
                )));
            }
        }
    }
    Ok(())
}

pub fn validate_global(
    metadata: &Metadata,
    replication: &DomainReplicationConfiguration,
) -> Result<()> {
    if replication.active_cluster_name.is_empty() {
        return Err(BadRequestError::new(
            "ActiveClusterName is required for all global domains.",
        ));
    }
    let valid = |name: &str| -> Result<()> {
        if !metadata.clusters.get(name).is_some_and(|c| c.enabled) {
            return Err(BadRequestError::new(format!(
                "Invalid cluster name: {name}"
            )));
        }
        Ok(())
    };
    for cluster in &replication.clusters {
        valid(&cluster.cluster_name)?;
    }
    valid(&replication.active_cluster_name)?;
    let included = |name: &str| -> Result<()> {
        if !replication.clusters.iter().any(|c| c.cluster_name == name) {
            return Err(BadRequestError::new(
                "Active cluster is not contained in all clusters.",
            ));
        }
        Ok(())
    };
    included(&replication.active_cluster_name)?;
    for scope in replication
        .active_clusters
        .as_ref()
        .and_then(|c| c.active_clusters_by_cluster_attribute.as_ref())
        .iter()
        .flat_map(|s| s.values())
    {
        for info in scope.cluster_attributes.iter().flat_map(|a| a.values()) {
            valid(&info.active_cluster_name)?;
            included(&info.active_cluster_name)?;
        }
    }
    Ok(())
}

/// Merge replication state: equal versions never replace an existing attribute.
pub fn merge_scopes(
    existing: Option<&ActiveClusters>,
    incoming: Option<&ActiveClusters>,
) -> (Option<ActiveClusters>, bool) {
    let (Some(existing), Some(incoming)) = (existing, incoming) else {
        return (
            existing.or(incoming).cloned(),
            existing.is_none() && incoming.is_some(),
        );
    };
    let mut scopes = existing
        .active_clusters_by_cluster_attribute
        .clone()
        .unwrap_or_default();
    let mut changed = false;
    for (name, scope) in incoming
        .active_clusters_by_cluster_attribute
        .iter()
        .flatten()
    {
        if let Some(old) = scopes.get_mut(name) {
            let attributes = old.cluster_attributes.get_or_insert_with(HashMap::new);
            for (name, info) in scope.cluster_attributes.iter().flatten() {
                if attributes
                    .get(name)
                    .is_none_or(|old| info.failover_version > old.failover_version)
                {
                    attributes.insert(name.clone(), info.clone());
                    changed = true;
                }
            }
        } else {
            scopes.insert(name.clone(), scope.clone());
            changed = true;
        }
    }
    (
        Some(ActiveClusters {
            active_clusters_by_region: None,
            active_clusters_by_cluster_attribute: Some(scopes),
        }),
        changed,
    )
}

/// Compute versions from stored state, then merge. Validation is a separate
/// step because Go's helper is also called by configuration updates.
pub fn build_update(
    metadata: &Metadata,
    existing: Option<&ActiveClusters>,
    incoming: Option<&ActiveClusters>,
    use_new: bool,
) -> Result<(Option<ActiveClusters>, bool)> {
    let Some(incoming) = incoming.filter(|v| v.active_clusters_by_cluster_attribute.is_some())
    else {
        return Ok((existing.cloned(), false));
    };
    let mut incoming = incoming.clone();
    for (scope, data) in incoming
        .active_clusters_by_cluster_attribute
        .iter_mut()
        .flatten()
    {
        for (attribute, info) in data.cluster_attributes.iter_mut().flatten() {
            let version = existing
                .and_then(|e| e.get_failover_version_for_attribute(scope, attribute).ok())
                .unwrap_or(-1);
            info.failover_version = metadata
                .next_failover_version(&info.active_cluster_name, version, use_new)
                .map_err(BadRequestError::new)?;
        }
    }
    if existing.is_none() {
        return Ok((Some(incoming), true));
    }
    let (merged, changed) = merge_scopes(existing, Some(&incoming));
    Ok((if changed { merged } else { existing.cloned() }, changed))
}

#[cfg(test)]
mod tests {
    use super::*;
    use cadence_common::cluster::ClusterInformation;
    use serde_json::{json, Value};

    fn normalize(value: Option<&ActiveClusters>) -> Value {
        let Some(value) = value else {
            return Value::Null;
        };
        json!({"scopes": value.active_clusters_by_cluster_attribute.as_ref().map(|scopes| scopes.iter().map(|(name, data)| {
            (name.clone(), json!({"attributes": data.cluster_attributes}))
        }).collect::<serde_json::Map<_, _>>())})
    }

    #[test]
    fn active_active_go_fixtures() {
        let metadata = Metadata {
            failover_version_increment: 10,
            primary_cluster_name: "a".into(),
            current_cluster_name: "a".into(),
            clusters: std::collections::BTreeMap::from([
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
                        initial_failover_version: 4,
                        ..Default::default()
                    },
                ),
            ]),
        };
        let cases: Vec<Value> = serde_json::from_str(include_str!(
            "../../../../tests/fixtures/active-active.json"
        ))
        .unwrap();
        for case in cases {
            let existing: Option<ActiveClusters> =
                serde_json::from_value(case["existing"].clone()).unwrap();
            let incoming: Option<ActiveClusters> =
                serde_json::from_value(case["incoming"].clone()).unwrap();
            let mut active = None;
            let mut changed = false;
            let mut config_changed = false;
            let mut active_changed = false;
            let mut target = "a";
            let mut error = String::new();
            let use_new = case["useNew"].as_bool().unwrap_or(false);
            let result: Result<()> = (|| {
                match case["op"].as_str().unwrap() {
                    "register" => {
                        active = registration_attributes(
                            &metadata,
                            case["global"].as_bool().unwrap_or(false),
                            incoming.as_ref(),
                        )?
                    }
                    "merge" => {
                        (active, changed) = merge_scopes(existing.as_ref(), incoming.as_ref())
                    }
                    "build" => {
                        (active, changed) =
                            build_update(&metadata, existing.as_ref(), incoming.as_ref(), use_new)?
                    }
                    "validate" => validate_incoming(&metadata, incoming.as_ref())?,
                    "update" => {
                        validate_incoming(&metadata, incoming.as_ref())?;
                        let (result, changed) =
                            build_update(&metadata, existing.as_ref(), incoming.as_ref(), use_new)?;
                        active = result;
                        config_changed = changed;
                        active_changed = changed || case["target"].is_string();
                        target = case["target"].as_str().unwrap_or("a");
                    }
                    op => panic!("unknown active-active operation {op}"),
                }
                Ok(())
            })();
            if let Err(e) = result {
                error = e.message;
            }
            assert_eq!(
                json!({"active": normalize(active.as_ref()), "changed": changed, "configChanged": config_changed, "activeChanged": active_changed, "target": target, "error": error}),
                case["expected"],
                "{}",
                case["name"]
            );
        }
    }
}
