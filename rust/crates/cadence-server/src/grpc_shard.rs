//! Native ShardDistributor adapters over shared in-memory ownership state.
//!
//! Upstream (`github.com/cadence-workflow/shard-manager`, pinned in
//! `go/go.mod`) coordinates a fleet through etcd: leaders assign shards to
//! executors per statically configured namespace. A single host owns every
//! shard, so namespaces spring into existence on first heartbeat (as
//! `ephemeral`, letting lookups auto-assign missing shards to this host),
//! heartbeats record executor state, and ownership reads come from that
//! state. Namespace-scoped typed errors map to `NOT_FOUND` with the
//! namespace/shard named; `DrainShards`, `UndrainShards` and
//! `GetDrainedShards` mirror upstream exactly and report `INTERNAL`
//! "not yet implemented" because upstream itself leaves them unimplemented.
//! State is memory-only like Go's cached coordination state.
use cadence_proto::sharddistributor as sd;
use std::collections::{HashMap, HashSet};
use tonic::{Request, Response, Status};

/// Single-host executor identity, matching the partition-owner convention.
pub fn host_executor_id() -> String {
    std::env::var("HOSTNAME").unwrap_or_else(|_| "cadence-rust".to_string())
}

/// Heartbeat metadata limits from upstream `handler/executor.go`.
const MAX_METADATA_KEYS: usize = 32;
const MAX_METADATA_KEY_LENGTH: usize = 128;
const MAX_METADATA_VALUE_SIZE: usize = 512 * 1024;

/// Live executor state inside a namespace.
#[derive(Debug, Clone, Default)]
pub struct ExecutorState {
    pub status: i32,
    pub last_heartbeat_millis: i64,
    pub metadata: HashMap<String, String>,
    /// Shard key to assignment status (`ASSIGNMENT_STATUS_READY = 1`).
    pub assigned_shards: HashMap<String, i32>,
}

/// Live namespace state: owners plus heartbeating executors.
#[derive(Debug, Clone, Default)]
pub struct NamespaceState {
    pub namespace_type: String,
    pub executors: HashMap<String, ExecutorState>,
    /// Shard key to owning executor ID.
    pub owners: HashMap<String, String>,
}

/// In-memory shard-distributor state held by [`crate::handlers::AppState`].
#[derive(Debug, Clone, Default)]
pub struct ShardDistributorState {
    pub namespaces: HashMap<String, NamespaceState>,
}

fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn millis_to_timestamp(millis: i64) -> Option<prost_types::Timestamp> {
    Some(prost_types::Timestamp {
        seconds: millis.div_euclid(1000),
        nanos: (millis.rem_euclid(1000) * 1_000_000) as i32,
    })
}

fn validate_metadata(metadata: &HashMap<String, String>) -> Result<(), Status> {
    if metadata.len() > MAX_METADATA_KEYS {
        return Err(Status::invalid_argument(format!(
            "invalid metadata: metadata has {} keys, which exceeds the maximum of {MAX_METADATA_KEYS}",
            metadata.len()
        )));
    }
    for (key, value) in metadata {
        if key.len() > MAX_METADATA_KEY_LENGTH {
            return Err(Status::invalid_argument(format!(
                "invalid metadata: metadata key {key:?} has length {}, which exceeds the maximum of {MAX_METADATA_KEY_LENGTH}",
                key.len()
            )));
        }
        if value.len() > MAX_METADATA_VALUE_SIZE {
            return Err(Status::invalid_argument(format!(
                "invalid metadata: metadata value for key {key:?} has size {} bytes, which exceeds the maximum of {MAX_METADATA_VALUE_SIZE} bytes",
                value.len()
            )));
        }
    }
    Ok(())
}

fn namespace_not_found(namespace: &str) -> Status {
    Status::not_found(format!("Namespace {namespace} not found."))
}

fn shard_not_found(namespace: &str, shard_key: &str) -> Status {
    Status::not_found(format!(
        "Shard {shard_key} not found in namespace {namespace}."
    ))
}

/// Main ShardDistributorAPI: namespace snapshots, lookups, drains, resets.
#[derive(Clone)]
pub struct ShardDistributor {
    pub state: crate::handlers::AppState,
}

impl ShardDistributor {
    fn lock(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, ShardDistributorState>, Status> {
        self.state
            .shard_distributor
            .lock()
            .map_err(|_| Status::internal("Shard distributor state is unavailable."))
    }

    fn owner_of(
        namespaces: &HashMap<String, NamespaceState>,
        namespace: &str,
        shard_key: &str,
    ) -> Result<String, Status> {
        let ns = namespaces
            .get(namespace)
            .ok_or_else(|| namespace_not_found(namespace))?;
        ns.owners
            .get(shard_key)
            .cloned()
            .ok_or_else(|| shard_not_found(namespace, shard_key))
    }
}

#[tonic::async_trait]
impl sd::shard_distributor_api_server::ShardDistributorApi for ShardDistributor {
    async fn get_shard_owner(
        &self,
        request: Request<sd::GetShardOwnerRequest>,
    ) -> Result<Response<sd::GetShardOwnerResponse>, Status> {
        let r = request.into_inner();
        let mut guard = self.lock()?;
        let known = guard.namespaces.contains_key(&r.namespace);
        if !known {
            return Err(namespace_not_found(&r.namespace));
        }
        let owner = match Self::owner_of(&guard.namespaces, &r.namespace, &r.shard_key) {
            Ok(owner) => owner,
            Err(_) => {
                // Ephemeral namespaces auto-assign missing shards to this
                // host, mirroring upstream's get-or-assign path.
                let ephemeral = guard
                    .namespaces
                    .get(&r.namespace)
                    .is_some_and(|ns| ns.namespace_type == "ephemeral");
                if !ephemeral {
                    return Err(shard_not_found(&r.namespace, &r.shard_key));
                }
                let host = host_executor_id();
                guard
                    .namespaces
                    .get_mut(&r.namespace)
                    .expect("namespace exists")
                    .owners
                    .insert(r.shard_key.clone(), host.clone());
                host
            }
        };
        Ok(Response::new(sd::GetShardOwnerResponse {
            owner,
            namespace: r.namespace,
            metadata: Default::default(),
        }))
    }

    async fn inspect_shard(
        &self,
        request: Request<sd::InspectShardRequest>,
    ) -> Result<Response<sd::InspectShardResponse>, Status> {
        let r = request.into_inner();
        let guard = self.lock()?;
        let owner = Self::owner_of(&guard.namespaces, &r.namespace, &r.shard_key)?;
        Ok(Response::new(sd::InspectShardResponse {
            owner,
            namespace: r.namespace,
            metadata: Default::default(),
        }))
    }

    async fn get_namespace_state(
        &self,
        request: Request<sd::GetNamespaceStateRequest>,
    ) -> Result<Response<sd::GetNamespaceStateResponse>, Status> {
        let r = request.into_inner();
        let guard = self.lock()?;
        let ns = guard
            .namespaces
            .get(&r.namespace)
            .ok_or_else(|| namespace_not_found(&r.namespace))?;
        let mut executors: Vec<sd::NamespaceExecutorState> = ns
            .executors
            .iter()
            .map(|(id, e)| sd::NamespaceExecutorState {
                executor_id: id.clone(),
                status: e.status,
                last_heartbeat: millis_to_timestamp(e.last_heartbeat_millis),
                metadata: e.metadata.clone(),
                assigned_shards: e
                    .assigned_shards
                    .iter()
                    .map(|(key, status)| sd::AssignedShardState {
                        shard_key: key.clone(),
                        assignment_status: *status,
                        assigned_state_mod_revision: 0,
                    })
                    .collect(),
            })
            .collect();
        executors.sort_by(|a, b| a.executor_id.cmp(&b.executor_id));
        Ok(Response::new(sd::GetNamespaceStateResponse {
            namespace: r.namespace,
            executors,
        }))
    }

    async fn watch_namespace_state(
        &self,
        request: Request<sd::WatchNamespaceStateRequest>,
    ) -> Result<Response<tonic::codegen::BoxStream<sd::WatchNamespaceStateResponse>>, Status> {
        let r = request.into_inner();
        let snapshot = {
            let guard = self.lock()?;
            let ns = guard
                .namespaces
                .get(&r.namespace)
                .ok_or_else(|| namespace_not_found(&r.namespace))?;
            let mut executors: Vec<sd::ExecutorInfo> = ns
                .executors
                .iter()
                .map(|(id, e)| {
                    let mut shards: Vec<sd::Shard> = e
                        .assigned_shards
                        .keys()
                        .map(|key| sd::Shard {
                            shard_key: key.clone(),
                        })
                        .collect();
                    shards.sort_by(|a, b| a.shard_key.cmp(&b.shard_key));
                    sd::ExecutorInfo {
                        executor_id: id.clone(),
                        metadata: e.metadata.clone(),
                        shards,
                    }
                })
                .collect();
            executors.sort_by(|a, b| a.executor_id.cmp(&b.executor_id));
            sd::WatchNamespaceStateResponse { executors }
        };
        // Single host has no background rebalancer; deliver the current
        // snapshot, then hold the stream open until the client cancels,
        // mirroring upstream's never-ending update stream.
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let _ = tx.send(Ok(snapshot)).await;
        tokio::spawn(async move {
            std::future::pending::<()>().await;
        });
        let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        Ok(Response::new(
            Box::pin(stream) as tonic::codegen::BoxStream<sd::WatchNamespaceStateResponse>,
        ))
    }

    async fn list_namespaces(
        &self,
        _request: Request<sd::ListNamespacesRequest>,
    ) -> Result<Response<sd::ListNamespacesResponse>, Status> {
        let guard = self.lock()?;
        let mut namespaces: Vec<sd::NamespaceConfig> = guard
            .namespaces
            .iter()
            .map(|(name, ns)| sd::NamespaceConfig {
                name: name.clone(),
                r#type: ns.namespace_type.clone(),
                mode: "onboarded".to_string(),
                shard_num: ns.owners.len() as i64,
            })
            .collect();
        namespaces.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(Response::new(sd::ListNamespacesResponse { namespaces }))
    }

    async fn drain_shards(
        &self,
        _request: Request<sd::DrainShardsRequest>,
    ) -> Result<Response<sd::DrainShardsResponse>, Status> {
        Err(Status::internal("DrainShards is not yet implemented"))
    }

    async fn undrain_shards(
        &self,
        _request: Request<sd::UndrainShardsRequest>,
    ) -> Result<Response<sd::UndrainShardsResponse>, Status> {
        Err(Status::internal("UndrainShards is not yet implemented"))
    }

    async fn get_drained_shards(
        &self,
        _request: Request<sd::GetDrainedShardsRequest>,
    ) -> Result<Response<sd::GetDrainedShardsResponse>, Status> {
        Err(Status::internal("GetDrainedShards is not yet implemented"))
    }

    async fn force_reset_namespace(
        &self,
        request: Request<sd::ForceResetNamespaceRequest>,
    ) -> Result<Response<sd::ForceResetNamespaceResponse>, Status> {
        let r = request.into_inner();
        let mut guard = self.lock()?;
        let ns = guard
            .namespaces
            .get_mut(&r.namespace)
            .ok_or_else(|| namespace_not_found(&r.namespace))?;
        let deleted = (ns.executors.len() + ns.owners.len()) as i64;
        ns.executors.clear();
        ns.owners.clear();
        Ok(Response::new(sd::ForceResetNamespaceResponse {
            deleted_keys: deleted,
        }))
    }
}

/// Executor API: heartbeats register executors and echo their reported
/// shards back as ready assignments with an onboarded migration mode.
#[derive(Clone)]
pub struct ShardExecutor {
    pub state: crate::handlers::AppState,
}

#[tonic::async_trait]
impl sd::shard_distributor_executor_api_server::ShardDistributorExecutorApi for ShardExecutor {
    async fn heartbeat(
        &self,
        request: Request<sd::HeartbeatRequest>,
    ) -> Result<Response<sd::HeartbeatResponse>, Status> {
        let r = request.into_inner();
        if r.namespace.is_empty() {
            return Err(Status::invalid_argument("Namespace is not set."));
        }
        if r.executor_id.is_empty() {
            return Err(Status::invalid_argument("Executor ID is not set."));
        }
        validate_metadata(&r.metadata)?;
        let mut guard = self
            .state
            .shard_distributor
            .lock()
            .map_err(|_| Status::internal("Shard distributor state is unavailable."))?;
        // Namespaces spring into existence on first heartbeat as ephemeral,
        // so a lone host can coordinate without static fleet config.
        let ns = guard.namespaces.entry(r.namespace.clone()).or_insert_with(|| {
            NamespaceState {
                namespace_type: "ephemeral".to_string(),
                ..Default::default()
            }
        });
        let mut assignments = HashMap::new();
        let mut assigned: HashSet<String> = HashSet::new();
        for (key, report) in &r.shard_status_reports {
            // shard_status 1 = READY, 2 = DONE; both keep the assignment.
            if report.status == 1 || report.status == 2 {
                assigned.insert(key.clone());
                assignments.insert(
                    key.clone(),
                    sd::ShardAssignment {
                        status: 1,
                    },
                );
            }
        }
        for key in assigned {
            ns.owners.insert(key, r.executor_id.clone());
        }
        // Shards this executor stopped reporting are released back to the
        // unassigned pool instead of going stale.
        ns.owners.retain(|key, owner| {
            *owner != r.executor_id || r.shard_status_reports.contains_key(key)
        });
        ns.executors.insert(
            r.executor_id.clone(),
            ExecutorState {
                status: r.status,
                last_heartbeat_millis: now_millis(),
                metadata: r.metadata,
                assigned_shards: assignments
                    .iter()
                    .map(|(k, v)| (k.clone(), v.status))
                    .collect(),
            },
        );
        Ok(Response::new(sd::HeartbeatResponse {
            shard_assignments: assignments,
            migration_mode: 4,
        }))
    }
}

/// Canary API: executor-to-executor ownership pings.
#[derive(Clone)]
pub struct ShardCanary {
    pub state: crate::handlers::AppState,
}

#[tonic::async_trait]
impl sd::shard_distributor_executor_canary_api_server::ShardDistributorExecutorCanaryApi
    for ShardCanary
{
    async fn ping(
        &self,
        request: Request<sd::PingRequest>,
    ) -> Result<Response<sd::PingResponse>, Status> {
        let r = request.into_inner();
        let guard = self
            .state
            .shard_distributor
            .lock()
            .map_err(|_| Status::internal("Shard distributor state is unavailable."))?;
        // Unknown namespaces report no ownership without an error, mirroring
        // upstream's canary handler.
        let owner = guard
            .namespaces
            .get(&r.namespace)
            .and_then(|ns| ns.owners.get(&r.shard_key).cloned())
            .unwrap_or_else(host_executor_id);
        let known = guard.namespaces.contains_key(&r.namespace);
        if !known {
            return Ok(Response::new(sd::PingResponse {
                executor_id: String::new(),
                owns_shard: false,
                shard_key: r.shard_key,
            }));
        }
        let host = host_executor_id();
        Ok(Response::new(sd::PingResponse {
            owns_shard: owner == host,
            executor_id: owner,
            shard_key: r.shard_key,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metadata_limits_match_upstream() {
        let mut metadata = HashMap::new();
        for i in 0..33 {
            metadata.insert(format!("k{i}"), "v".to_string());
        }
        assert!(validate_metadata(&metadata).is_err());
        let mut metadata = HashMap::new();
        metadata.insert("k".repeat(129), "v".to_string());
        assert!(validate_metadata(&metadata).is_err());
        let mut metadata = HashMap::new();
        metadata.insert("k".to_string(), "v".repeat(512 * 1024 + 1));
        assert!(validate_metadata(&metadata).is_err());
        assert!(validate_metadata(&HashMap::new()).is_ok());
    }

    #[test]
    fn namespace_state_tracks_owners_and_executors() {
        let mut state = ShardDistributorState::default();
        let ns = state.namespaces.entry("n".to_string()).or_default();
        ns.namespace_type = "ephemeral".to_string();
        ns.owners.insert("s".to_string(), "e".to_string());
        ns.executors.insert("e".to_string(), ExecutorState::default());
        assert_eq!(
            ShardDistributor::owner_of(&state.namespaces, "n", "s").unwrap(),
            "e"
        );
        assert!(ShardDistributor::owner_of(&state.namespaces, "n", "missing").is_err());
        assert!(ShardDistributor::owner_of(&state.namespaces, "missing", "s").is_err());
    }
}
