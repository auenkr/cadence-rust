//! Domain update controller state transitions from Go common/domain/handler.go.
use crate::{domain_replication, DomainRecord};
use cadence_api::{
    enums::ArchivalStatus, errors::BadRequestError, service_types::UpdateDomainRequest,
};
use cadence_common::cluster::Metadata;

type Result<T> = std::result::Result<T, BadRequestError>;
const MIXED: &str = "Cannot set active cluster to current cluster when other parameters are set.";
const LOCAL: &str = "Local domains cannot perform failovers or change replication configuration";
const PRIMARY: &str =
    "Cluster is not primary cluster, cannot do domain registration or domain update.";

pub struct PreparedUpdate {
    pub record: DomainRecord,
    pub write: bool,
    pub failover: bool,
}

pub fn prepare(
    current: &DomainRecord,
    request: &UpdateDomainRequest,
    metadata: &Metadata,
    now: i64,
) -> Result<PreparedUpdate> {
    prepare_inner(current, request, metadata, now, false, None)
}

pub fn prepare_failover(
    current: &DomainRecord,
    request: &UpdateDomainRequest,
    metadata: &Metadata,
    now: i64,
    reason: Option<&str>,
) -> Result<PreparedUpdate> {
    prepare_inner(current, request, metadata, now, true, reason)
}

fn prepare_inner(
    current: &DomainRecord,
    request: &UpdateDomainRequest,
    metadata: &Metadata,
    now: i64,
    force_failover: bool,
    reason: Option<&str>,
) -> Result<PreparedUpdate> {
    let replication = request.replication_configuration.as_ref();
    if !current.is_global_domain
        && (replication.is_some_and(|r| {
            r.active_cluster_name.is_some() || r.clusters.is_some() || r.active_clusters.is_some()
        }) || request.failover_timeout_in_seconds.is_some())
    {
        return Err(BadRequestError::new(LOCAL));
    }
    let failover = current.is_global_domain && (force_failover || request.is_failover());
    if failover && request.is_config_update() {
        return Err(BadRequestError::new(MIXED));
    }
    let mut record = current.clone();
    let mut changed = false;
    if !failover {
        if let Some(info) = &request.updated_info {
            if let Some(description) = &info.description {
                record.info.description.clone_from(description);
                changed = true;
            }
            if let Some(owner) = &info.owner_email {
                record.info.owner_email.clone_from(owner);
                changed = true;
            }
            if let Some(data) = &info.data {
                record
                    .info
                    .data
                    .get_or_insert_with(Default::default)
                    .extend(data.clone());
                changed = true;
            }
        }
        if let Some(config) = &request.configuration {
            if let Some(retention) = config.workflow_execution_retention_period_in_days {
                record
                    .configuration
                    .workflow_execution_retention_period_in_days = retention;
                changed = true;
            }
            if let Some(emit) = config.emit_metric {
                record.configuration.emit_metric = emit;
                changed = true;
            }
            if let Some(incoming) = &config.bad_binaries {
                let stored = record
                    .configuration
                    .bad_binaries
                    .get_or_insert_with(Default::default)
                    .binaries
                    .get_or_insert_with(Default::default);
                for (checksum, info) in incoming.binaries.iter().flatten() {
                    let mut info = info
                        .clone()
                        .ok_or_else(|| BadRequestError::new("Bad binary info is missing."))?;
                    info.created_time_nano = Some(now);
                    stored.insert(checksum.clone(), Some(info));
                }
                if stored.len() > 10 {
                    return Err(BadRequestError::new(
                        "Total resetBinaries cannot exceed the max limit: 10",
                    ));
                }
                // Go updateDomainConfiguration intentionally does not mark this alone
                // as a config change. Its response can contain an unpersisted merge.
            }
            // Full archival provider state transitions remain a separate port.
            if let Some(status) = config.history_archival_status {
                record.configuration.history_archival_status = Some(status);
                changed = true;
            }
            if let Some(uri) = &config.history_archival_uri {
                if !uri.is_empty() {
                    record.configuration.history_archival_uri.clone_from(uri);
                    changed = true;
                }
            }
            if let Some(status) = config.visibility_archival_status {
                record.configuration.visibility_archival_status = Some(status);
                changed = true;
            }
            if let Some(uri) = &config.visibility_archival_uri {
                if !uri.is_empty() {
                    record.configuration.visibility_archival_uri.clone_from(uri);
                    changed = true;
                }
            }
        }
        if let Some(checksum) = &request.delete_bad_binary {
            let removed = record
                .configuration
                .bad_binaries
                .as_mut()
                .and_then(|b| b.binaries.as_mut())
                .and_then(|b| b.remove(checksum));
            if removed.is_none() {
                return Err(BadRequestError::new(format!(
                    "Bad binary checksum {checksum} doesn't exists."
                )));
            }
            changed = true;
        }
    }
    let mut active_changed = false;
    let mut replication_changed = false;
    if current.is_global_domain {
        if let Some(replication) = replication {
            if let Some(clusters) = &replication.clusters {
                if !clusters.is_empty() {
                    record.replication.clusters.clone_from(clusters);
                    replication_changed = true;
                }
            }
            if let Some(target) = &replication.active_cluster_name {
                record.replication.active_cluster_name.clone_from(target);
                active_changed = true;
            }
            domain_replication::validate_incoming(metadata, replication.active_clusters.as_ref())?;
            let (attributes, attributes_changed) = domain_replication::build_update(
                metadata,
                current.replication.active_clusters.as_ref(),
                replication.active_clusters.as_ref(),
                false,
            )?;
            if attributes_changed {
                record.replication.active_clusters = attributes;
                replication_changed = true;
                active_changed = true;
            }
        }
    }
    if failover && !active_changed && !replication_changed {
        return Err(BadRequestError::new("a failover was requested, but there was no change detected, the configuration was not updated"));
    }
    if failover && request.failover_timeout_in_seconds.is_some() {
        if !active_changed || record.replication.is_active_active() {
            return Err(BadRequestError::new("Cannot start graceful failover without updating active cluster or in local domain."));
        }
        if record.replication.active_cluster_name != metadata.current_cluster_name {
            return Err(BadRequestError::new(
                "Cannot start the graceful failover from a to-be-passive cluster.",
            ));
        }
        if record.replication.active_cluster_name == current.replication.active_cluster_name {
            return Err(BadRequestError::new(
                "Cannot start the graceful failover from an active cluster to an active cluster.",
            ));
        }
        if current.failover_end_time.is_some() {
            return Err(BadRequestError::new(
                "Cannot start concurrent graceful failover.",
            ));
        }
    }
    changed |= replication_changed;
    if record
        .configuration
        .workflow_execution_retention_period_in_days
        < 1
    {
        return Err(BadRequestError::new(
            "A valid retention period is not set on request.",
        ));
    }
    if (record.configuration.history_archival_status == Some(ArchivalStatus::Enabled)
        && record.configuration.history_archival_uri.is_empty())
        || (record.configuration.visibility_archival_status == Some(ArchivalStatus::Enabled)
            && record.configuration.visibility_archival_uri.is_empty())
    {
        return Err(BadRequestError::new(
            "Invalid to enable archival without specifying a uri.",
        ));
    }
    if current.is_global_domain {
        domain_replication::validate_global(metadata, &record.replication)?;
        if changed && active_changed && !record.replication.is_active_active() {
            return Err(BadRequestError::new(MIXED));
        }
        if !active_changed && !metadata.is_primary_cluster() {
            return Err(BadRequestError::new(PRIMARY));
        }
    } else if record.replication.active_cluster_name != metadata.current_cluster_name
        || record.replication.clusters.len() != 1
        || record.replication.clusters[0].cluster_name != metadata.current_cluster_name
    {
        return Err(BadRequestError::new("Invalid local domain clusters"));
    }
    if failover {
        let version = current.failover_version.wrapping_add(i64::from(
            current.replication.is_active_active() || record.replication.is_active_active(),
        ));
        record.failover_version = metadata
            .next_failover_version(&record.replication.active_cluster_name, version, false)
            .map_err(BadRequestError::new)?;
        record.previous_failover_version = -1;
        record.failover_end_time = request
            .failover_timeout_in_seconds
            .map(|seconds| now.wrapping_add(i64::from(seconds).wrapping_mul(1_000_000_000)));
        if record.failover_end_time.is_some() {
            record.previous_failover_version = current.failover_version;
        }
        if !current.replication.is_active_active() && !record.replication.is_active_active() {
            append_failover_history(
                &mut record,
                current,
                now,
                request.failover_timeout_in_seconds.is_some(),
                reason,
            );
        }
    }
    let write = changed || active_changed;
    if changed {
        record.config_version = record.config_version.wrapping_add(1);
    }
    if write {
        record.last_updated_time_nanos = now;
    }
    Ok(PreparedUpdate {
        record,
        write,
        failover,
    })
}

#[derive(Default, serde::Serialize, serde::Deserialize)]
#[serde(default, rename_all = "camelCase")]
struct LegacyFailoverEvent {
    event_time: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    from_cluster: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    to_cluster: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    failover_type: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    reason: String,
}
fn append_failover_history(
    record: &mut DomainRecord,
    previous: &DomainRecord,
    now: i64,
    graceful: bool,
    reason: Option<&str>,
) {
    let data = record.info.data.get_or_insert_with(Default::default);
    let mut history: Vec<LegacyFailoverEvent> = data
        .get("FailoverHistory")
        .and_then(|s| serde_json::from_str(s).ok())
        .unwrap_or_default();
    let timestamp = chrono::DateTime::from_timestamp(
        now.div_euclid(1_000_000_000),
        now.rem_euclid(1_000_000_000) as u32,
    )
    .expect("domain timestamp");
    let timestamp = timestamp.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true);
    let timestamp = format!(
        "{}Z",
        timestamp
            .trim_end_matches('Z')
            .trim_end_matches('0')
            .trim_end_matches('.')
    );
    history.insert(
        0,
        LegacyFailoverEvent {
            event_time: timestamp,
            failover_type: if graceful { "Grace" } else { "Force" }.into(),
            from_cluster: previous.replication.active_cluster_name.clone(),
            to_cluster: record.replication.active_cluster_name.clone(),
            reason: reason.unwrap_or_default().into(),
        },
    );
    history.truncate(5);
    data.insert(
        "FailoverHistory".into(),
        serde_json::to_string(&history)
            .expect("domain failover history")
            .replace('&', "\\u0026")
            .replace('<', "\\u003c")
            .replace('>', "\\u003e")
            .replace('\u{2028}', "\\u2028")
            .replace('\u{2029}', "\\u2029"),
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use cadence_api::types::{
        ActiveClusters, DomainConfiguration, DomainInfo, DomainReplicationConfiguration,
    };
    use cadence_common::cluster::ClusterInformation;
    use serde_json::{json, Value};
    fn normalize_active(active: Option<&ActiveClusters>) -> Value {
        let Some(active) = active else {
            return Value::Null;
        };
        json!({"scopes": active.active_clusters_by_cluster_attribute.as_ref().map(|scopes| scopes.iter().map(|(scope, data)| (scope.clone(), json!({"attributes": data.cluster_attributes}))).collect::<serde_json::Map<_, _>>())})
    }
    fn normalize(record: &DomainRecord) -> Value {
        json!({"description":record.info.description,"owner":record.info.owner_email,"data":record.info.data,
            "retention":record.configuration.workflow_execution_retention_period_in_days,"emit":record.configuration.emit_metric,
            "bad":record.configuration.bad_binaries.as_ref().and_then(|b| b.binaries.as_ref()),
            "target":record.replication.active_cluster_name,"active":normalize_active(record.replication.active_clusters.as_ref()),"version":record.failover_version})
    }
    #[test]
    fn domain_update_go_fixtures() {
        let cases: Vec<Value> = serde_json::from_str(include_str!(
            "../../../../tests/fixtures/domain-update.json"
        ))
        .unwrap();
        for case in cases {
            let global = case["global"].as_bool().unwrap_or(false);
            let metadata = Metadata {
                failover_version_increment: 10,
                primary_cluster_name: "a".into(),
                current_cluster_name: case["currentCluster"].as_str().unwrap_or("a").into(),
                clusters: std::collections::BTreeMap::from([
                    (
                        "a".into(),
                        ClusterInformation {
                            enabled: true,
                            initial_failover_version: 0,
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
                ]),
            };
            let configuration: DomainConfiguration = serde_json::from_value(json!({"workflowExecutionRetentionPeriodInDays":7,"emitMetric":true,"badBinaries":{"binaries":{"old":{"reason":"old","createdTimeNano":1}}}})).unwrap();
            let mut info: DomainInfo = serde_json::from_value(json!({"name":"domain","description":"old","ownerEmail":"owner","data":{"keep":"old"},"uuid":"00000000-0000-0000-0000-000000000001"})).unwrap();
            if let Some(history) = case["initialHistory"].as_str() {
                info.data
                    .as_mut()
                    .unwrap()
                    .insert("FailoverHistory".into(), history.into());
            }
            let replication: DomainReplicationConfiguration = serde_json::from_value(json!({"activeClusterName":case["initialTarget"].as_str().unwrap_or("a"),"clusters":if global {json!([{"clusterName":"a"},{"clusterName":"b"}])}else{json!([{"clusterName":"a"}])},"activeClusters":case["existing"]})).unwrap();
            let current: DomainRecord = serde_json::from_value(json!({"info":info,"configuration":configuration,"replication":replication,"failover_version":if global{0}else{-24},"is_global_domain":global})).unwrap();
            let mut request: UpdateDomainRequest =
                serde_json::from_value(case["request"].clone()).unwrap();
            request.name = "domain".into();
            let mut stored = current.clone();
            let mut response = Value::Null;
            let mut error = String::new();
            let mut writes = 0;
            match prepare(&current, &request, &metadata, 1_700_000_000_000_000_000) {
                Err(e) => error = e.message,
                Ok(prepared) => {
                    response = normalize(&prepared.record);
                    if prepared.write {
                        stored = prepared.record;
                        writes = 1;
                    }
                }
            }
            assert_eq!(
                json!({"error":error,"writes":writes,"response":response,"stored":normalize(&stored),"configVersion":stored.config_version,"previousVersion":stored.previous_failover_version,"endTime":stored.failover_end_time}),
                case["expected"],
                "{}",
                case["name"]
            );
        }
    }
}
