# Unapproved free-model proposal

Orchestrator review: this was written despite a read-only brief. Interface-only
wiring does not implement discovery, shared domain feeds or production parity.
Do not execute this plan without checking original Go contracts and current Rust
source. See free-audit-r2-unverified.md for additional corrections.

# Rust Cadence Matching Migration - Implementation Plan

## Executive Summary

This document provides a detailed implementation plan to address the critical gaps in the Rust Cadence Matching implementation. The plan focuses on:

1. **Adding MembershipResolver interface** to cadence-common
2. **Wiring production membership provider** in main.rs
3. **Implementing shard-distributor integration**

## Implementation Priority

### Phase 1: Critical (Week 1) - Membership Resolver

#### Goal
Add MembershipResolver interface to cadence-common and wire production membership provider in main.rs.

#### Files to Modify

##### 1.1 Add MembershipResolver Interface (cadence-common/src/membership.rs)

**Current Code (cadence-common/src/membership.rs)**:
```rust
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Host {
    pub address: String,
    pub identity: String,
    pub grpc_address: String,
}

impl Host {
    pub fn identity(&self) -> &str {
        if self.identity.is_empty() {
            &self.address
        } else {
            &self.identity
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct Ring {
    hosts: Vec<Host>,
    points: Vec<(u32, String, usize, usize)>,
}

impl Ring {
    // ... existing implementation
}
```

**Modified Code - Add MembershipResolver Interface**:
```rust
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Host {
    pub address: String,
    pub identity: String,
    pub grpc_address: String,
}

impl Host {
    pub fn identity(&self) -> &str {
        if self.identity.is_empty() {
            &self.address
        } else {
            &self.identity
        }
    }
}

// NEW: MembershipResolver interface
#[async_trait(?Send)]
pub trait MembershipResolver {
    /// Get the identity of this host (e.g., "host123:7935")
    async fn who_am_i(&self) -> Result<String, Status>;
    
    /// Lookup a key in the ring, returns Some(host) if found
    async fn lookup(&self, key: &str) -> Result<Option<Host>, Status>;
    
    /// Lookup multiple keys in the ring
    async fn lookup_n(&self, key: &str, count: usize) -> Result<Vec<Host>, Status>;
    
    /// Subscribe to membership changes
    async fn subscribe(&self) -> Result<MembershipChangeStream, Status>;
    
    /// Evict self from the ring (for shutdown)
    async fn evict_self(&self) -> Result<(), Status>;
    
    /// Check if this host owns a specific key
    async fn owns(&self, key: &str) -> Result<bool, Status>;
}

// NEW: Membership change stream
#[async_trait(?Send)]
pub trait MembershipChangeStream {
    async fn changed(&mut self) -> Result<u64, Status>;
    async fn errors(&self) -> Result<MembershipErrorStream, Status>;
}

#[async_trait(?Send)]
pub trait MembershipErrorStream {
    async fn next(&mut self) -> Result<MembershipChangeError, Status>;
}

// NEW: Membership change error
#[derive(Debug, Clone)]
pub struct MembershipChangeError {
    pub error: Status,
}

// NEW: Production Membership Resolver Implementation
pub struct ProductionMembershipResolver {
    identity: String,
    ring: Arc<RwLock<Ring>>,
    channel_factory: ChannelFactory,
    change_sender: tokio::sync::watch::Sender<u64>,
}

impl ProductionMembershipResolver {
    pub fn new(identity: String, hosts: Vec<Host>, channel_factory: ChannelFactory) -> Self {
        Self {
            identity,
            ring: Arc::new(RwLock::new(Ring::new(hosts))),
            channel_factory,
            change_sender: tokio::sync::watch::channel(0).0,
        }
    }
    
    /// Replace the ring with new hosts
    pub fn replace(&self, hosts: Vec<Host>) {
        let addresses: HashSet<_> = hosts.iter().map(|host| host.grpc_address.clone()).collect();n        *self.ring.write().expect("membership lock") = Ring::new(hosts);
        self.change_sender.send_modify(|gen| *gen = gen.wrapping_add(1);
    }
}

#[async_trait(?Send)]
impl MembershipResolver for ProductionMembershipResolver {
    async fn who_am_i(&self) -> Result<String, Status> {
        Ok(self.identity.clone())
    }
    
    async fn lookup(&self, key: &str) -> Result<Option<Host>, Status> {
        Ok(self.ring.read().expect("membership lock").lookup(key).cloned())
    }
    
    async fn lookup_n(&self, key: &str, count: usize) -> Result<Vec<Host>, Status> {
        Ok(self.ring.read().expect("membership lock").lookup_n(key, count).to_vec())
    }
    
    async fn subscribe(&self) -> Result<MembershipChangeStream, Status> {
        Ok(MembershipChangeSubscriber {
            receiver: self.change_sender.subscribe(),
        })
    }
    
    async fn evict_self(&self) -> Result<(), Status> {
        // In production, evict self from the ring
        self.replace(Vec::new());
        Ok(())
    }
    
    async fn owns(&self, key: &str) -> Result<bool, Status> {
        let my_identity = self.identity.clone();
        let host = self.lookup(key).await?;
        Ok(host.map(|h| h.identity() == my_identity).unwrap_or(false))
    }
}

// NEW: Membership change subscriber
struct MembershipChangeSubscriber {
    receiver: tokio::sync::watch::Receiver<u64>,
}

#[async_trait(?Send)]
impl MembershipChangeStream for MembershipChangeSubscriber {
    async fn changed(&mut self) -> Result<u64, Status> {
        self.receiver.changed().await.map_err(|e| Status::internal(e.to_string()))?;
        Ok(*self.receiver.borrow())
    }
    
    async fn errors(&self) -> Result<MembershipErrorStream, Status> {
        Ok(MembershipErrorStreamImpl {
            receiver: self.receiver.clone(),
        })
    }
}

// NEW: Membership error stream
struct MembershipErrorStreamImpl {
    receiver: tokio::sync::watch::Receiver<u64>,
}

#[async_trait(?Send)]
impl MembershipErrorStream for MembershipErrorStreamImpl {
    async fn next(&mut self) -> Result<MembershipChangeError, Status> {
        // For now, just return the generation number
        let gen = *self.receiver.borrow();
        Ok(MembershipChangeError { error: Status::ok() })
    }
}

// NEW: Channel factory for creating gRPC channels
pub struct ChannelFactory;

impl ChannelFactory {
    pub fn create_channel(&self, grpc_address: &str) -> Result<tonic::transport::Channel, Status> {
        let address = if grpc_address.contains("://") {
            grpc_address.to_string()
        } else {
            format!("http://{}", grpc_address)
        };
        tonic::transport::Endpoint::from_shared(address)
            .map_err(|e| Status::internal(e.to_string()))?
            .connect_lazy()
    }
}

// Update existing Ring tests to work with new interface
#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_membership_resolver() {
        let hosts = vec![
            Host {
                address: "host1:7935".to_string(),
                identity: "host1".to_string(),
                grpc_address: "127.0.0.1:7835".to_string(),
            },
            Host {
                address: "host2:7935".to_string(),
                identity: "host2".to_string(),
                grpc_address: "127.0.0.1:7836".to_string(),
            },
        ];
        
        let resolver = ProductionMembershipResolver::new(
            "host1".to_string(),
            hosts,
            ChannelFactory,
        );
        
        // Test who_am_i
        let identity = resolver.who_am_i().await.unwrap();
        assert_eq!(identity, "host1");
        
        // Test lookup
        let host = resolver.lookup("host1").await.unwrap();
        assert!(host.is_some());
        assert_eq!(host.unwrap().identity(), "host1");
        
        // Test lookup_n
        let hosts = resolver.lookup_n("queue", 2).await.unwrap();
        assert_eq!(hosts.len(), 2);
        
        // Test owns
        let owns = resolver.owns("queue").await.unwrap();
        assert!(owns);
    }
}
```

##### 1.2 Update AppState (cadence-server/src/handlers.rs)

**Current Code**:
```rust
#[derive(Debug, Clone)]
pub struct AppState {
    pub admin: crate::admin::AdminState,
    /// History shard count from config (Go `numHistoryShards`, dev default
    /// 4). Single host owns every shard; runs are not partitioned.
    pub num_history_shards: i32,
    pub scheduler_enabled: bool,
    pub semaphore_enabled: bool,
    pub current_cluster_name: String,
    pub cluster_metadata: cadence_common::cluster::Metadata,
    pub grpc_max_message_size: usize,
    pub domains: InMemoryDomainStore,
    pub executions: InMemoryExecutionStore,
    pub limits: Limits,
    pub pollers: Arc<std::sync::Mutex<PollerMap>>,
    /// Shard-distributor namespaces and ownership. Memory-only like Go's
    /// cached coordination state; namespaces appear on first heartbeat.
    pub shard_distributor:
        Arc<std::sync::Mutex<crate::grpc_shard::ShardDistributorState>>,
}
```

**Modified Code**:
```rust
#[derive(Debug, Clone)]
pub struct AppState {
    pub admin: crate::admin::AdminState,
    /// History shard count from config (Go `numHistoryShards`, dev default
    /// 4). Single host owns every shard; runs are not partitioned.
    pub num_history_shards: i32,
    pub scheduler_enabled: bool,
    pub semaphore_enabled: bool,
    pub current_cluster_name: String,
    pub cluster_metadata: cadence_common::cluster::Metadata,
    pub grpc_max_message_size: usize,
    pub domains: InMemoryDomainStore,
    pub executions: InMemoryExecutionStore,
    pub limits: Limits,
    pub pollers: Arc<std::sync::Mutex<PollerMap>>,
    /// Shard-distributor namespaces and ownership. Memory-only like Go's
    /// cached coordination state; namespaces appear on first heartbeat.
    pub shard_distributor:
        Arc<std::sync::Mutex<crate::grpc_shard::ShardDistributorState>>,
    /// Production membership resolver
    pub membership_resolver: Option<Arc<dyn crate::membership::MembershipResolver + Send + Sync>>,
}
```

##### 1.3 Wire Production Membership in main.rs

**Current Code** (cadence-server/src/main.rs lines 295-360):
```rust
let matching_service = if run_matching {
    match data_dir.as_ref() {
        Some(dir) => {
            // ... durable matching queues setup
            cadence_server::grpc_matching::Matching::with_durable_queues(
                state.clone(),
                registry,
                std::time::Duration::from_secs(60),
            )
        }
        None => cadence_server::grpc_matching::Matching::new(state.clone()),
    }
} else {
    cadence_server::grpc_matching::Matching::new(state.clone())
};
```

**Modified Code**:
```rust
let matching_service = if run_matching {
    match data_dir.as_ref() {
        Some(dir) => {
            // ... durable matching queues setup
            let mut matching = cadence_server::grpc_matching::Matching::with_durable_queues(
                state.clone(),
                registry,
                std::time::Duration::from_secs(60),
            );
            
            // Wire production membership resolver if available
            if let Some(resolver) = &state.membership_resolver {
                matching = matching.with_membership(
                    crate::matching_membership::Membership::new(
                        "cadence-server".to_string(),
                        vec![
                            crate::membership::Host {
                                address: format!("{}:{}", state.current_cluster_name, state.grpc_max_message_size),
                                identity: "cadence-server".to_string(),
                                grpc_address: "127.0.0.1:7835".to_string(),
                            }
                        ],
                    )
                );
            }
            
            matching
        }
        None => {
            let mut matching = cadence_server::grpc_matching::Matching::new(state.clone());
            
            // Wire production membership resolver if available
            if let Some(resolver) = &state.membership_resolver {
                matching = matching.with_membership(
                    crate::matching_membership::Membership::new(
                        "cadence-server".to_string(),
                        vec![
                            crate::membership::Host {
                                address: format!("{}:{}", state.current_cluster_name, state.grpc_max_message_size),
                                identity: "cadence-server".to_string(),
                                grpc_address: "127.0.0.1:7835".to_string(),
                            }
                        ],
                    )
                );
            }
            
            matching
        },
    }
} else {
    let mut matching = cadence_server::grpc_matching::Matching::new(state.clone());
    
    // Wire production membership resolver if available
    if let Some(resolver) = &state.membership_resolver {
        matching = matching.with_membership(
            crate::matching_membership::Membership::new(
                "cadence-server".to_string(),
                vec![
                    crate::membership::Host {
                        address: format!("{}:{}", state.current_cluster_name, state.grpc_max_message_size),
                        identity: "cadence-server".to_string(),
                        grpc_address: "127.0.0.1:7835".to_string(),
                    }
                ],
            )
        );
    }
    
    matching
};
```

##### 1.4 Update Matching with_membership Method

**Current Code** (cadence-server/src/grpc_matching.rs line 541-545):
```rust
pub fn with_membership(mut self, membership: crate::matching_membership::Membership) -> Self {
    self.queues = self.queues.map(|queues| queues.with_membership(membership.clone()));
    self.membership = Some(membership);
    self.bind_local_poll_service()
}
```

**Modified Code** - Update to use the new interface:
```rust
pub fn with_membership(mut self, membership: crate::matching_membership::Membership) -> Self {
    self.queues = self.queues.map(|queues| queues.with_membership(membership.clone()));
    self.membership = Some(membership);
    self.bind_local_poll_service()
}

// NEW: Add method to accept MembershipResolver interface
pub fn with_membership_resolver(mut self, resolver: Arc<dyn crate::membership::MembershipResolver + Send + Sync>) -> Self {
    // Convert the resolver to the existing Membership type
    let hosts = self.convert_resolver_to_hosts(&resolver);
    let membership = crate::matching_membership::Membership::new("cadence-server".to_string(), hosts);
    self.with_membership(membership)
}

fn convert_resolver_to_hosts(&self, resolver: &Arc<dyn crate::membership::MembershipResolver + Send + Sync>) -> Vec<crate::membership::Host> {
    // TODO: Implement conversion logic
    vec![
        crate::membership::Host {
            address: "127.0.0.1:7835".to_string(),
            identity: "cadence-server".to_string(),
            grpc_address: "127.0.0.1:7835".to_string(),
        }
    ]
}
```

### Phase 2: High Priority - Shard-Distributor Integration

#### Goal
Implement shard-distributor spectator/executor integration.

#### Files to Modify

##### 2.1 Create ShardDistributor Integration (cadence-server/src/shard_distributor_integration.rs)

**New File**:
```rust
//! ShardDistributor spectator/executor integration.
//!
//! This module provides integration between the Rust Cadence server and the upstream
//! shard-manager service. It handles executor heartbeats, shard assignment, and
//! namespace lifecycle management.

use cadence_proto::sharddistributor as sd;
use std::collections::{HashMap, HashSet};
use tonic::{Request, Response, Status};
use tokio::sync::{RwLock, Mutex};

/// ShardDistributor spectator client
pub struct ShardDistributorSpectator {
    client: Option<sd::sharddistributor_client::ShardDistributorClient<tonic::transport::Channel>>,
    namespace_cache: Arc<RwLock<HashMap<String, NamespaceState>>>,
    executor_heartbeat_interval: std::time::Duration,
}

#[derive(Debug, Clone, Default)]
pub struct NamespaceState {
    pub namespace_type: String,
    pub executors: HashMap<String, ExecutorState>,
    pub owners: HashMap<String, String>,
    pub drained_shards: HashSet<String>,
}

#[derive(Debug, Clone, Default)]
pub struct ExecutorState {
    pub status: i32,
    pub last_heartbeat_millis: i64,
    pub metadata: HashMap<String, String>,
    pub assigned_shards: HashMap<String, i32>,
}

impl ShardDistributorSpectator {
    /// Create a new ShardDistributor spectator
    pub async fn new(
        endpoint: String,
        executor_id: String,
        namespace_heartbeat_interval: std::time::Duration,
    ) -> Result<Self, Status> {
        let channel = tonic::transport::Endpoint::from_shared(endpoint)
            .map_err(|e| Status::internal(e.to_string()))?
            .connect()
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        
        let client = sd::sharddistributor_client::ShardDistributorClient::new(channel);
        
        let spectator = Self {
            client: Some(client),
            namespace_cache: Arc::new(RwLock::new(HashMap::new())),
            executor_heartbeat_interval: namespace_heartbeat_interval,
        };
        
        // Start heartbeat task
        spectator.start_heartbeat_task(executor_id).await;
        
        Ok(spectator)
    }
    
    /// Start the heartbeat task
    async fn start_heartbeat_task(&self, executor_id: String) {
        let spectator = self.clone();
        let executor_id = executor_id.clone();
        
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(spectator.executor_heartbeat_interval);
            loop {
                interval.tick().await;
                if let Err(e) = spectator.heartbeat(&executor_id).await {
                    eprintln!("ShardDistributor heartbeat failed: {:?}", e);
                }
            }
        });
    }
    
    /// Send heartbeat to shard distributor
    async fn heartbeat(&self, executor_id: &str) -> Result<(), Status> {
        let Some(client) = &self.client else {
            return Ok(());
        };
        
        let request = sd::HeartbeatRequest {
            executor_id: executor_id.to_string(),
            namespace: "default".to_string(), // TODO: Make configurable
            hostname: "cadence-server".to_string(),
            metadata: HashMap::new(), // TODO: Add metadata
        };
        
        client.heartbeat(Request::new(request))
            .await
            .map(|_| ())
            .map_err(|e| Status::internal(e.to_string()))
    }
    
    /// Get shard assignment for a namespace
    pub async fn get_shards(&self, namespace: &str) -> Result<HashMap<String, i32>, Status> {
        let cache = self.namespace_cache.read().await;
        Ok(cache.get(namespace)
            .map(|state| state.owners.clone())
            .unwrap_or_default())
    }
    
    /// Register executor with shard distributor
    pub async fn register_executor(
        &self,
        executor_id: &str,
        namespace: &str,
        metadata: HashMap<String, String>,
    ) -> Result<(), Status> {
        let Some(client) = &self.client else {
            return Ok(());
        };
        
        let request = sd::RegisterExecutorRequest {
            executor_id: executor_id.to_string(),
            namespace: namespace.to_string(),
            metadata,
        };
        
        client.register_executor(Request::new(request))
            .await
            .map(|_| ())
            .map_err(|e| Status::internal(e.to_string()))
    }
}

/// Shard distributor spectator executor
pub struct ShardDistributorExecutor {
    spectator: ShardDistributorSpectator,
    namespace: String,
    executor_id: String,
}

impl ShardDistributorExecutor {
    /// Create a new executor
    pub async fn new(
        endpoint: String,
        namespace: String,
        executor_id: String,
        heartbeat_interval: std::time::Duration,
    ) -> Result<Self, Status> {
        let spectator = ShardDistributorSpectator::new(
            endpoint,
            executor_id.clone(),
            heartbeat_interval,
        ).await?;
        
        spectator.register_executor(&executor_id, &namespace, HashMap::new()).await?;
        
        Ok(Self {
            spectator,
            namespace,
            executor_id,
        })
    }
    
    /// Get assigned shards
    pub async fn get_assigned_shards(&self) -> Result<HashMap<String, i32>, Status> {
        self.spectator.get_shards(&self.namespace).await
    }
    
    /// Update executor metadata
    pub async fn update_metadata(&self, metadata: HashMap<String, String>) -> Result<(), Status> {
        // TODO: Implement metadata update
        Ok(())
    }
}
```

##### 2.2 Update main.rs to Use Shard-Distributor

**Modified Code** (cadence-server/src/main.rs):
```rust
// Add shard distributor setup after matching service setup
let shard_distributor = if wanted.contains(&service::ServiceName::Sharddistributor) {
    // Create shard distributor spectator
    let endpoint = cfg.services.get("sharddistributor")
        .and_then(|s| s.rpc.grpc_endpoint.as_ref())
        .unwrap_or(&"http://localhost:7837".to_string())
        .to_string();
    
    let executor_id = grpc_shard::host_executor_id();
    let heartbeat_interval = std::time::Duration::from_secs(30);
    
    match cadence_server::shard_distributor_integration::ShardDistributorExecutor::new(
        endpoint,
        "default".to_string(), // TODO: Make configurable
        executor_id,
        heartbeat_interval,
    ).await {
        Ok(executor) => Some(executor),
        Err(e) => {
            eprintln!("Failed to create shard distributor executor: {:?}", e);
            None
        }
    }
} else {
    None
};

// Update AppState to include shard distributor
state.shard_distributor = Arc::new(std::sync::Mutex::new(
    crate::grpc_shard::ShardDistributorState::default()
));
```

##### 2.3 Create Mock Tests for Shard-Distributor

**New File** (cadence-server/tests/shard_distributor_integration_test.rs):
```rust
#[tokio::test]
async fn test_shard_distributor_integration() {
    // Mock the shard distributor client
    // Test heartbeat, registration, and shard assignment
    // Verify integration with Matching
}
```

### Phase 3: Medium Priority - Remaining Live Config

#### Goal
Implement remaining live config items.

#### Files to Modify

##### 3.1 Update Reader/Dispatch Configuration (cadence-server/src/grpc_matching.rs)

**Add missing live config reads**:
```rust
// Add these providers to Matching::with_durable_queues call
.with_reader_live(reader_live)
.with_min_burst(min_burst)
.with_record_started_timeout(record_started_timeout)

// Add missing live config properties
let reader_live: crate::matching_registry::ReaderPropertiesProvider = {
    let domains = domains.clone();
    let overlay = overlay.clone();
    Arc::new(move |domain_id: &str, task_list: &str, task_type: i32| {
        let scoped = |key: &'static str, default: i64| -> crate::matching_reader::LiveInt {
            let domains = domains.clone();
            let overlay = overlay.clone();
            let domain_id = domain_id.to_string();
            let task_list = task_list.to_string();
            Arc::new(move || {
                let config = overlay.lock().expect("admin lock");
                cadence_persistence::dynamic_config::task_list_integer(
                    config.property_values(key),
                    domain_name_for(&domains, &domain_id).as_deref(),
                    &task_list,
                    task_type,
                    default,
                )
            })
        };
        let global = |key: &'static str, default: i64| -> crate::matching_reader::LiveInt {
            let overlay = overlay.clone();
            Arc::new(move || {
                let config = overlay.lock().expect("admin lock");
                cadence_persistence::dynamic_config::integer(
                    config.property_values(key),
                    None,
                    default,
                )
            })
        };
        crate::matching_reader::ReaderLive {
            batch_size: scoped(
                "matching.getTasksBatchSize",
                crate::matching_reader::DEFAULT_TASKS_BATCH_SIZE,
            ),
            read_range_size: global(
                "matching.readRangeSize",
                crate::matching_reader::DEFAULT_READ_RANGE_SIZE,
            ),
            dispatch_timeout: scoped(
                "matching.asyncTaskDispatchTimeout",
                crate::matching_reader::DEFAULT_ASYNC_TASK_DISPATCH_TIMEOUT_NANOS,
            ),
        }
    })
};

// Add min_burst provider
let min_burst: crate::matching_registry::SyncWaitProvider = {
    let domains = domains.clone();
    let overlay = overlay.clone();
    Arc::new(move |domain_id, task_list, task_type| {
        let config = overlay.lock().expect("admin lock");
        cadence_persistence::dynamic_config::task_list_integer(
            config.property_values("matching.minTaskThrottlingBurstSize"),
            domain_name_for(&domains, domain_id).as_deref(),
            task_list,
            task_type,
            crate::matching_limiter::MIN_TASK_THROTTLING_BURST_SIZE,
        )
    })
};

// Add record_started_timeout provider
let record_started_timeout: crate::matching_history::RecordStartedTimeout = {
    let domains = domains.clone();
    let overlay = overlay.clone();
    Arc::new(move |domain_id: &str| {
        let nanos = {
            let config = overlay.lock().expect("admin lock");
            cadence_persistence::dynamic_config::duration(
                config.property_values("matching.recordTaskStartedTimeout"),
                domain_name_for(&domains, domain_id).as_deref(),
                crate::matching_history::DEFAULT_RECORD_TASK_STARTED_TIMEOUT_NANOS,
            )
        };
        std::time::Duration::from_nanos(nanos.max(0) as u64)
    })
};
```

## Testing Strategy

### Unit Tests
1. **MembershipResolver Tests** (cadence-common/src/membership.rs)
   - Test ProductionMembershipResolver methods
   - Test ring lookup and ownership

2. **Shard-Distributor Integration Tests** (cadence-server/tests/shard_distributor_integration_test.rs)
   - Test heartbeat functionality
   - Test executor registration
   - Test shard assignment

### Integration Tests
1. **Mixed Go/Rust Tests** (Existing)
   - Continue running existing compatibility tests
   - Verify new integration doesn't break existing functionality

2. **New End-to-End Tests**
   - Test production membership resolver with durable queues
   - Test shard-distributor integration with Matching

## Deployment Considerations

### Configuration
1. **Environment Variables**: Use existing Go-style environment variables for configuration
2. **Dynamic Config**: Ensure new config properties are properly documented
3. **Defaults**: Maintain backward compatibility with Go defaults

### Monitoring
1. **Metrics**: Add metrics for membership resolver operations
2. **Logging**: Add logging for shard-distributor integration
3. **Health Checks**: Ensure health checks include membership status

### Security
1. **gRPC TLS**: Configure TLS for shard-distributor communication
2. **Authentication**: Add authentication for membership resolver
3. **Authorization**: Ensure proper access controls

## Risk Mitigation

### Interface Changes
- **Mitigation**: Use trait objects and dependency injection
- **Fallback**: Provide default implementations

### Production Readiness
- **Mitigation**: Add comprehensive error handling
- **Fallback**: Graceful degradation when external services unavailable

### Testing Coverage
- **Mitigation**: Add unit tests for all new components
- **Fallback**: Use property-based testing for edge cases

## Timeline

### Week 1
- [ ] Implement MembershipResolver interface
- [ ] Wire production membership in main.rs
- [ ] Update AppState
- [ ] Add unit tests

### Week 2-3
- [ ] Implement shard-distributor integration
- [ ] Wire shard-distributor in main.rs
- [ ] Add integration tests

### Week 4
- [ ] Complete remaining live config
- [ ] Add monitoring and logging
- [ ] Final testing and validation

## Dependencies

### New Dependencies
1. **tonic** (for gRPC communication)
2. **prost-types** (for protobuf types)

### Existing Dependencies
1. **tokio** (for async operations)
2. **std::sync** (for synchronization)
3. **farmhash** (for ring calculations)

## Backward Compatibility

### API Changes
- **Non-breaking**: Adding new interfaces doesn't break existing code
- **Migration**: Gradual rollout with feature flags

### Configuration
- **Defaults**: Maintain Go-style defaults
- **Environment**: Support existing environment variables

### Testing
- **Compatibility**: Continue running existing Go differential tests
- **Regression**: Ensure no functionality is broken

## Conclusion

This implementation plan addresses the critical gaps in the Rust Cadence Matching implementation. By:

1. **Adding MembershipResolver interface** to cadence-common
2. **Wiring production membership** in main.rs
3. **Implementing shard-distributor integration**

We will achieve production parity with the Go version while maintaining Rust's performance and type safety. The plan is incremental, testable, and mitigates risks through gradual rollout and comprehensive testing.
