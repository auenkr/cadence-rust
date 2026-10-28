//! Outbound Matching client for the pinned shard-manager APIs.
//!
//! This module owns shard-manager assignments but deliberately receives
//! processor callbacks from its caller. The Matching registry integration can
//! therefore adapt its existing queue lifecycle instead of creating duplicate
//! managers.

use cadence_proto::sharddistributor as sd;
use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicU8, Ordering},
        Arc, Mutex, RwLock,
    },
    time::{Duration, Instant},
};
use tokio::{sync::watch, task::JoinHandle};

pub type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;
const RPC_TIMEOUT: Duration = Duration::from_secs(10);
const PROCESSOR_ASYNC_TIMEOUT: Duration = Duration::from_secs(10);
const DRAINING_HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(5);
const SPECTATOR_RETRY_INTERVAL: Duration = Duration::from_secs(1);
const SPECTATOR_STREAM_TIMEOUT: Duration = Duration::from_secs(10 * 60);
const SPECTATOR_RPC_RETRY_EXPIRATION: Duration = Duration::from_secs(15);
const SPECTATOR_RPC_RETRY_INITIAL: Duration = Duration::from_millis(200);
const SPECTATOR_RPC_RETRY_MAX: Duration = Duration::from_secs(10);
const LIFECYCLE_NEW: u8 = 0;
const LIFECYCLE_RUNNING: u8 = 1;
const LIFECYCLE_STOPPING: u8 = 2;
const LIFECYCLE_STOPPED: u8 = 3;

/// YARPC gRPC inbound routing metadata. These values match Go's
/// the Go shard-manager dispatcher, Matching caller, and protobuf encoding.
fn yarpc_grpc_request<T>(message: T) -> tonic::Request<T> {
    let mut request = tonic::Request::new(message);
    let metadata = request.metadata_mut();
    metadata.insert(
        "rpc-service",
        tonic::metadata::MetadataValue::from_static("shard-manager"),
    );
    metadata.insert(
        "rpc-caller",
        tonic::metadata::MetadataValue::from_static("cadence-matching"),
    );
    metadata.insert(
        "rpc-encoding",
        tonic::metadata::MetadataValue::from_static("proto"),
    );
    request
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LifecycleError(&'static str);

impl std::fmt::Display for LifecycleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}

impl std::error::Error for LifecycleError {}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ShardReport {
    pub status: sd::ShardStatus,
    pub load: f64,
}

/// Lifecycle callbacks for a shard processor. Implementations should capture
/// their existing Matching owner; the executor never constructs queue state.
pub trait ShardProcessor: Send + Sync + 'static {
    fn start(&self) -> BoxFuture<Result<(), String>>;
    fn stop(&self) -> BoxFuture<()>;
    fn report(&self) -> ShardReport;
    fn set_status(&self, status: sd::ShardStatus);
}

pub trait ShardProcessorFactory: Send + Sync + 'static {
    fn create(&self, shard_key: String) -> BoxFuture<Result<Arc<dyn ShardProcessor>, String>>;
    fn create_raw(&self, shard_key: Vec<u8>) -> BoxFuture<Result<Arc<dyn ShardProcessor>, String>> {
        match String::from_utf8(shard_key) {
            Ok(key) => self.create(key),
            Err(_) => Box::pin(async { Err("non-UTF-8 shard key unsupported by legacy factory".into()) }),
        }
    }
}

pub trait ExecutorRpc: Send + Sync + 'static {
    fn heartbeat(
        &self,
        request: sd::HeartbeatRequest,
        timeout: Duration,
    ) -> BoxFuture<Result<sd::HeartbeatResponse, String>>;
    fn heartbeat_raw(&self, request: RawHeartbeatRequest, timeout: Duration) -> BoxFuture<Result<RawHeartbeatResponse, String>> {
        let reports = request.reports.into_iter().map(|entry| {
            String::from_utf8(entry.key).map(|key| (key, entry.value.unwrap_or_default()))
        }).collect::<Result<HashMap<_, _>, _>>();
        let reports = match reports {
            Ok(reports) => reports,
            Err(_) => return Box::pin(async { Err("non-UTF-8 shard key unsupported by legacy RPC".into()) }),
        };
        let future = self.heartbeat(sd::HeartbeatRequest {
            namespace: request.namespace, executor_id: request.executor_id, status: request.status,
            shard_status_reports: reports, metadata: request.metadata,
        }, timeout);
        Box::pin(async move {
            let response = future.await?;
            Ok(RawHeartbeatResponse { assignments: response.shard_assignments.into_iter().map(|(key, value)| RawAssignmentEntry { key: key.into_bytes(), value: Some(value) }).collect(), migration_mode: response.migration_mode })
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardOwner {
    pub executor_id: String,
    pub metadata: HashMap<String, String>,
}

// Protobuf string and bytes have the same length-delimited wire encoding. Go
// strings may contain arbitrary bytes; only shard keys cross this boundary raw.
#[derive(Clone, PartialEq, prost::Message)]
struct RawOwnerRequest {
    #[prost(bytes = "vec", tag = "1")]
    shard_key: Vec<u8>,
    #[prost(string, tag = "2")]
    namespace: String,
}

#[derive(Clone, PartialEq, prost::Message)]
struct RawSnapshot {
    #[prost(message, repeated, tag = "1")]
    executors: Vec<RawExecutor>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct RawExecutor {
    #[prost(string, tag = "1")]
    executor_id: String,
    #[prost(map = "string, string", tag = "2")]
    metadata: HashMap<String, String>,
    #[prost(message, repeated, tag = "3")]
    shards: Vec<RawShard>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct RawShard {
    #[prost(bytes = "vec", tag = "1")]
    shard_key: Vec<u8>,
}

// Repeated map-entry messages retain Go string bytes while using protobuf's
// identical length-delimited representation. Repeated keys have map semantics
// (last entry wins) when assignments are applied.
#[derive(Clone, PartialEq, prost::Message)]
pub struct RawHeartbeatRequest {
    #[prost(string, tag="1")] pub namespace: String,
    #[prost(string, tag="2")] pub executor_id: String,
    #[prost(int32, tag="3")] pub status: i32,
    #[prost(message, repeated, tag="4")] pub reports: Vec<RawReportEntry>,
    #[prost(map="string, string", tag="5")] pub metadata: HashMap<String, String>,
}
#[derive(Clone, PartialEq, prost::Message)]
pub struct RawReportEntry {
    #[prost(bytes="vec", tag="1")] pub key: Vec<u8>,
    #[prost(message, optional, tag="2")] pub value: Option<sd::ShardStatusReport>,
}
#[derive(Clone, PartialEq, prost::Message)]
pub struct RawHeartbeatResponse {
    #[prost(message, repeated, tag="1")] pub assignments: Vec<RawAssignmentEntry>,
    #[prost(int32, tag="2")] pub migration_mode: i32,
}
#[derive(Clone, PartialEq, prost::Message)]
pub struct RawAssignmentEntry {
    #[prost(bytes="vec", tag="1")] pub key: Vec<u8>,
    #[prost(message, optional, tag="2")] pub value: Option<sd::ShardAssignment>,
}

type RawOwnerCache = Arc<RwLock<Option<HashMap<Vec<u8>, ShardOwner>>>>;

pub struct TonicSpectator {
    channel: tonic::transport::Channel,
    namespace: Arc<str>,
    enabled: Arc<dyn Fn() -> bool + Send + Sync>,
    owners: RawOwnerCache,
    first_state_tx: watch::Sender<bool>,
    first_state_rx: watch::Receiver<bool>,
    stop_tx: watch::Sender<bool>,
    stop_rx: watch::Receiver<bool>,
    task: Arc<tokio::sync::Mutex<Option<JoinHandle<()>>>>,
    lifecycle: Arc<AtomicU8>,
    stopped_tx: watch::Sender<bool>,
    stopped_rx: watch::Receiver<bool>,
    lifecycle_owner: bool,
}

impl Clone for TonicSpectator {
    fn clone(&self) -> Self {
        Self {
            channel: self.channel.clone(),
            namespace: self.namespace.clone(),
            enabled: self.enabled.clone(),
            owners: self.owners.clone(),
            first_state_tx: self.first_state_tx.clone(),
            first_state_rx: self.first_state_rx.clone(),
            stop_tx: self.stop_tx.clone(),
            stop_rx: self.stop_rx.clone(),
            task: self.task.clone(),
            lifecycle: self.lifecycle.clone(),
            stopped_tx: self.stopped_tx.clone(),
            stopped_rx: self.stopped_rx.clone(),
            lifecycle_owner: false,
        }
    }
}

impl TonicSpectator {
    pub fn new(
        channel: tonic::transport::Channel,
        namespace: impl Into<Arc<str>>,
        enabled: Arc<dyn Fn() -> bool + Send + Sync>,
    ) -> Self {
        let (first_state_tx, first_state_rx) = watch::channel(false);
        let (stop_tx, stop_rx) = watch::channel(false);
        let (stopped_tx, stopped_rx) = watch::channel(false);
        Self {
            channel,
            namespace: namespace.into(),
            enabled,
            owners: Arc::new(RwLock::new(None)),
            first_state_tx,
            first_state_rx,
            stop_tx,
            stop_rx,
            task: Arc::new(tokio::sync::Mutex::new(None)),
            lifecycle: Arc::new(AtomicU8::new(LIFECYCLE_NEW)),
            stopped_tx,
            stopped_rx,
            lifecycle_owner: true,
        }
    }

    pub async fn start(&self) -> Result<(), LifecycleError> {
        let mut task = self.task.lock().await;
        self.lifecycle
            .compare_exchange(
                LIFECYCLE_NEW,
                LIFECYCLE_RUNNING,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .map_err(|_| LifecycleError("spectator can only be started once"))?;
        let this = self.clone();
        *task = Some(tokio::spawn(async move {
            this.watch_loop().await;
            this.lifecycle.store(LIFECYCLE_STOPPED, Ordering::Release);
            let _ = this.stopped_tx.send(true);
        }));
        Ok(())
    }

    pub async fn shutdown(&self) {
        loop {
            match self.lifecycle.load(Ordering::Acquire) {
                LIFECYCLE_NEW => {
                    if self
                        .lifecycle
                        .compare_exchange(
                            LIFECYCLE_NEW,
                            LIFECYCLE_STOPPED,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        let _ = self.stop_tx.send(true);
                        self.reset_first_state();
                        let _ = self.stopped_tx.send(true);
                        break;
                    }
                }
                LIFECYCLE_RUNNING => {
                    if self
                        .lifecycle
                        .compare_exchange(
                            LIFECYCLE_RUNNING,
                            LIFECYCLE_STOPPING,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        let _ = self.stop_tx.send(true);
                        break;
                    }
                }
                LIFECYCLE_STOPPING | LIFECYCLE_STOPPED => break,
                _ => unreachable!("invalid spectator lifecycle"),
            }
        }
        self.wait_stopped().await;
        if let Some(task) = self.task.lock().await.take() {
            let _ = task.await;
        }
    }

    async fn wait_stopped(&self) {
        let mut stopped = self.stopped_rx.clone();
        while !*stopped.borrow_and_update() {
            if stopped.changed().await.is_err() {
                return;
            }
        }
    }

    /// Waits for the first complete namespace snapshot, then serves cached
    /// ownership and falls back to GetShardOwner on a cache miss, matching Go.
    pub async fn get_shard_owner(
        &self,
        shard_key: &str,
        deadline: tokio::time::Instant,
    ) -> Result<ShardOwner, String> {
        self.get_shard_owner_bytes(shard_key.as_bytes(), deadline)
            .await
    }

    pub async fn get_shard_owner_bytes(
        &self,
        shard_key: &[u8],
        deadline: tokio::time::Instant,
    ) -> Result<ShardOwner, String> {
        let mut first = self.first_state_rx.clone();
        let mut stop = self.stop_rx.clone();
        while !*first.borrow_and_update() {
            tokio::select! {
                _ = tokio::time::sleep_until(deadline) => return Err("spectator initial state deadline exceeded".into()),
                changed = stop.changed() => {
                    if changed.is_err() || *stop.borrow_and_update() {
                        return Err("spectator stopped before initial namespace state".into());
                    }
                }
                changed = first.changed() => {
                    if changed.is_err() { return Err("spectator stopped before initial namespace state".into()); }
                }
            }
        }
        if *stop.borrow_and_update() {
            return Err("spectator stopped before GetShardOwner".into());
        }
        if let Some(owner) = self
            .owners
            .read()
            .expect("SD spectator owners lock")
            .as_ref()
            .and_then(|owners| owners.get(shard_key))
            .cloned()
        {
            return Ok(owner);
        }
        let started = Instant::now();
        let mut retry_delay = SPECTATOR_RPC_RETRY_INITIAL;
        loop {
            if tokio::time::Instant::now() >= deadline {
                return Err("GetShardOwner caller deadline exceeded".into());
            }
            if *stop.borrow_and_update() {
                return Err("spectator stopped during GetShardOwner".into());
            }
            let mut client = tonic::client::Grpc::new(self.channel.clone());
            let call_timeout =
                RPC_TIMEOUT.min(deadline.saturating_duration_since(tokio::time::Instant::now()));
            let mut request = yarpc_grpc_request(RawOwnerRequest {
                shard_key: shard_key.to_vec(),
                namespace: self.namespace.to_string(),
            });
            request.set_timeout(call_timeout);
            let call =
                tokio::time::timeout(call_timeout, async {
                    client.ready().await.map_err(|error| {
                        tonic::Status::unknown(format!("service not ready: {error}"))
                    })?;
                    client.unary::<_, sd::GetShardOwnerResponse, _>(
                    request,
                    tonic::codegen::http::uri::PathAndQuery::from_static(
                        "/uber.cadence.sharddistributor.v1.ShardDistributorAPI/GetShardOwner",
                    ),
                    tonic::codec::ProstCodec::default(),
                ).await
                });
            let response = tokio::select! {
                response = call => response,
                _ = tokio::time::sleep_until(deadline) => return Err("GetShardOwner caller deadline exceeded".into()),
                changed = stop.changed() => {
                    if changed.is_err() || *stop.borrow_and_update() {
                        return Err("spectator stopped during GetShardOwner".into());
                    }
                    continue;
                }
            };
            match response {
                Ok(Ok(response)) => {
                    let response = response.into_inner();
                    return Ok(ShardOwner {
                        executor_id: response.owner,
                        metadata: response.metadata,
                    });
                }
                Ok(Err(error)) if !retryable_status(error.code()) => {
                    return Err(error.to_string());
                }
                Ok(Err(error)) => {
                    if started.elapsed() >= SPECTATOR_RPC_RETRY_EXPIRATION {
                        return Err(error.to_string());
                    }
                }
                Err(_) => {
                    if started.elapsed() >= SPECTATOR_RPC_RETRY_EXPIRATION {
                        return Err("GetShardOwner timed out".into());
                    }
                }
            }
            let remaining = SPECTATOR_RPC_RETRY_EXPIRATION.saturating_sub(started.elapsed());
            tokio::select! {
                _ = tokio::time::sleep(retry_delay.min(remaining)) => {},
                _ = tokio::time::sleep_until(deadline) => return Err("GetShardOwner caller deadline exceeded".into()),
                changed = stop.changed() => {
                    if changed.is_err() || *stop.borrow_and_update() {
                        return Err("spectator stopped during GetShardOwner".into());
                    }
                }
            }
            retry_delay = retry_delay.saturating_mul(2).min(SPECTATOR_RPC_RETRY_MAX);
        }
    }

    fn reset_first_state(&self) {
        *self.owners.write().expect("SD spectator owners lock") = None;
        if *self.first_state_rx.borrow() {
            let _ = self.first_state_tx.send(false);
        }
    }

    async fn watch_loop(&self) {
        let mut stop = self.stop_rx.clone();
        loop {
            if *stop.borrow_and_update() {
                self.reset_first_state();
                return;
            }
            if !(self.enabled)() {
                self.reset_first_state();
                tokio::select! {
                    _ = tokio::time::sleep(jitter(SPECTATOR_RETRY_INTERVAL)) => continue,
                    changed = stop.changed() => if changed.is_err() || *stop.borrow_and_update() { return; },
                }
            }
            let mut client = tonic::client::Grpc::new(self.channel.clone());
            let stream = tokio::select! {
                response = async {
                    client.ready().await.map_err(|error| tonic::Status::unknown(format!("service not ready: {error}")))?;
                    client.server_streaming::<_, RawSnapshot, _>(
                        yarpc_grpc_request(sd::WatchNamespaceStateRequest { namespace: self.namespace.to_string() }),
                        tonic::codegen::http::uri::PathAndQuery::from_static(
                            "/uber.cadence.sharddistributor.v1.ShardDistributorAPI/WatchNamespaceState",
                        ),
                        tonic::codec::ProstCodec::default(),
                    ).await
                } => response,
                changed = stop.changed() => {
                    if changed.is_err() || *stop.borrow_and_update() {
                        self.reset_first_state();
                        return;
                    }
                    continue;
                }
            };
            if let Ok(response) = stream {
                let mut stream = response.into_inner();
                let stream_timeout = tokio::time::sleep(SPECTATOR_STREAM_TIMEOUT);
                tokio::pin!(stream_timeout);
                loop {
                    tokio::select! {
                        _ = &mut stream_timeout => break,
                        _ = stop.changed() => {
                            if *stop.borrow_and_update() { return; }
                        }
                        item = stream.message() => match item {
                            Ok(Some(snapshot)) => {
                                let mut owners = HashMap::new();
                                for executor in snapshot.executors {
                                    let owner = ShardOwner {
                                        executor_id: executor.executor_id,
                                        metadata: executor.metadata,
                                    };
                                    for shard in executor.shards {
                                        owners.insert(shard.shard_key, owner.clone());
                                    }
                                }
                                *self.owners.write().expect("SD spectator owners lock") = Some(owners);
                                let _ = self.first_state_tx.send(true);
                            }
                            Ok(None) | Err(_) => break,
                        }
                    }
                    if !(self.enabled)() {
                        self.reset_first_state();
                        break;
                    }
                }
            }
            tokio::select! {
                _ = tokio::time::sleep(jitter(SPECTATOR_RETRY_INTERVAL)) => {},
                changed = stop.changed() => if changed.is_err() || *stop.borrow_and_update() { return; },
            }
        }
    }
}

impl Drop for TonicSpectator {
    fn drop(&mut self) {
        if self.lifecycle_owner {
            let _ = self.stop_tx.send(true);
        }
    }
}

#[derive(Clone)]
pub struct TonicExecutorRpc {
    channel: tonic::transport::Channel,
    client: Arc<
        tokio::sync::Mutex<
            sd::shard_distributor_executor_api_client::ShardDistributorExecutorApiClient<
                tonic::transport::Channel,
            >,
        >,
    >,
}

impl TonicExecutorRpc {
    pub fn from_channel(channel: tonic::transport::Channel) -> Self {
        Self {
            channel: channel.clone(),
            client: Arc::new(tokio::sync::Mutex::new(
                sd::shard_distributor_executor_api_client::ShardDistributorExecutorApiClient::new(
                    channel,
                ),
            )),
        }
    }

    pub fn connect_lazy(host_port: &str) -> Result<Self, tonic::transport::Error> {
        let address = if host_port.contains("://") {
            host_port.to_string()
        } else {
            format!("http://{host_port}")
        };
        let endpoint = tonic::transport::Endpoint::from_shared(address)?;
        Ok(Self::from_channel(endpoint.connect_lazy()))
    }
}

impl ExecutorRpc for TonicExecutorRpc {
    fn heartbeat(
        &self,
        request: sd::HeartbeatRequest,
        timeout: Duration,
    ) -> BoxFuture<Result<sd::HeartbeatResponse, String>> {
        let client = self.client.clone();
        Box::pin(async move {
            let deadline = tokio::time::Instant::now() + timeout;
            tokio::time::timeout(timeout, async move {
                let mut client = client.lock().await;
                let mut request = yarpc_grpc_request(request);
                request
                    .set_timeout(deadline.saturating_duration_since(tokio::time::Instant::now()));
                client
                    .heartbeat(request)
                    .await
                    .map(tonic::Response::into_inner)
                    .map_err(|error| error.to_string())
            })
            .await
            .map_err(|_| format!("shard-distributor heartbeat timed out after {timeout:?}"))?
        })
    }
    fn heartbeat_raw(&self, request: RawHeartbeatRequest, timeout: Duration) -> BoxFuture<Result<RawHeartbeatResponse, String>> {
        let channel = self.channel.clone();
        Box::pin(async move {
            tokio::time::timeout(timeout, async move {
                let mut client = tonic::client::Grpc::new(channel);
                client.ready().await.map_err(|error| format!("service not ready: {error}"))?;
                let mut request = yarpc_grpc_request(request);
                request.set_timeout(timeout);
                client.unary::<_, RawHeartbeatResponse, _>(request,
                    tonic::codegen::http::uri::PathAndQuery::from_static("/uber.cadence.sharddistributor.v1.ShardDistributorExecutorAPI/Heartbeat"),
                    tonic::codec::ProstCodec::default()).await.map(tonic::Response::into_inner).map_err(|error| error.to_string())
            }).await.map_err(|_| format!("shard-distributor heartbeat timed out after {timeout:?}"))?
        })
    }

}

/// Go `executorclient.NewNoopExecutor`: owns no shards, so every lookup reports
/// not-found and ownership falls back to the local hash ring.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoopShardExecutor;

impl NoopShardExecutor {
    pub fn get_shard_process(&self, shard_key: &str) -> Result<(), GetShardProcessError> {
        Err(GetShardProcessError::NotFound(shard_key.to_string()))
    }

    #[must_use]
    pub fn has_local_shard_process(&self, _shard_key: &str) -> bool {
        false
    }
}

/// The executor Go's `setupExecutor` installs for a given config.
#[derive(Debug)]
pub enum ShardExecutorSetup {
    /// No namespaces configured: Go installs the no-op executor.
    Noop(NoopShardExecutor),
    /// Exactly one namespace: Go installs the live executor with this report TTL.
    Live { report_ttl_nanos: i64 },
}

/// Go `setupExecutor` decision. Zero namespaces installs the no-op executor,
/// one installs the live executor, and more is rejected as Go's fatal error.
pub fn setup_shard_executor(
    config: &crate::config::ShardDistributorMatchingConfig,
) -> anyhow::Result<ShardExecutorSetup> {
    match config.namespaces.len() {
        0 => Ok(ShardExecutorSetup::Noop(NoopShardExecutor)),
        1 => Ok(ShardExecutorSetup::Live {
            report_ttl_nanos: config.report_ttl_nanos(),
        }),
        _ => anyhow::bail!("matching service does not support multiple namespaces"),
    }
}

#[derive(Debug)]
pub enum GetShardProcessError {
    NotFound(String),
    Heartbeat(String),
}

impl std::fmt::Display for GetShardProcessError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound(key) => write!(f, "shard process not found for shard key: {key}"),
            Self::Heartbeat(error) => {
                write!(
                    f,
                    "shard-distributor heartbeat and assignment failed: {error}"
                )
            }
        }
    }
}

impl std::error::Error for GetShardProcessError {}

const PROCESSOR_STARTING: u8 = 0;
const PROCESSOR_STARTED: u8 = 1;
const PROCESSOR_STOPPING: u8 = 2;

struct ManagedProcessor {
    processor: Arc<dyn ShardProcessor>,
    state: AtomicU8,
}

struct ExecutorState {
    processors: Mutex<HashMap<Vec<u8>, Arc<ManagedProcessor>>>,
    last_use: Mutex<HashMap<Vec<u8>, Instant>>,
    stopping: Mutex<Vec<JoinHandle<()>>>,
    assignment_lock: tokio::sync::Mutex<()>,
}

impl Default for ExecutorState {
    fn default() -> Self {
        Self {
            processors: Mutex::new(HashMap::new()),
            last_use: Mutex::new(HashMap::new()),
            stopping: Mutex::new(Vec::new()),
            assignment_lock: tokio::sync::Mutex::new(()),
        }
    }
}

pub struct ShardDistributorExecutor {
    namespace: Arc<str>,
    executor_id: Arc<str>,
    heartbeat_interval: Duration,
    ttl_shard: Duration,
    rpc: Arc<dyn ExecutorRpc>,
    factory: Arc<dyn ShardProcessorFactory>,
    enabled: Arc<dyn Fn() -> bool + Send + Sync>,
    metadata: Arc<Mutex<HashMap<String, String>>>,
    state: Arc<ExecutorState>,
    stop_tx: watch::Sender<bool>,
    stop_rx: watch::Receiver<bool>,
    drain_rx: Option<watch::Receiver<bool>>,
    supervisor: Arc<tokio::sync::Mutex<Option<JoinHandle<()>>>>,
    lifecycle: Arc<AtomicU8>,
    stopped_tx: watch::Sender<bool>,
    stopped_rx: watch::Receiver<bool>,
    lifecycle_owner: bool,
}

impl Clone for ShardDistributorExecutor {
    fn clone(&self) -> Self {
        Self {
            namespace: self.namespace.clone(),
            executor_id: self.executor_id.clone(),
            heartbeat_interval: self.heartbeat_interval,
            ttl_shard: self.ttl_shard,
            rpc: self.rpc.clone(),
            factory: self.factory.clone(),
            enabled: self.enabled.clone(),
            metadata: self.metadata.clone(),
            state: self.state.clone(),
            stop_tx: self.stop_tx.clone(),
            stop_rx: self.stop_rx.clone(),
            drain_rx: self.drain_rx.clone(),
            supervisor: self.supervisor.clone(),
            lifecycle: self.lifecycle.clone(),
            stopped_tx: self.stopped_tx.clone(),
            stopped_rx: self.stopped_rx.clone(),
            lifecycle_owner: false,
        }
    }
}

impl ShardDistributorExecutor {
    /// Create an executor for exactly the configured Matching namespace.
    /// A fresh UUID is generated for each executor instance, matching Go.
    pub fn new(
        config: &crate::config::ShardDistributorMatchingConfig,
        rpc: Arc<dyn ExecutorRpc>,
        factory: Arc<dyn ShardProcessorFactory>,
        enabled: Arc<dyn Fn() -> bool + Send + Sync>,
        metadata: HashMap<String, String>,
        drain_rx: Option<watch::Receiver<bool>>,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            config.namespaces.len() == 1,
            "shard-distributor executor requires exactly one namespace"
        );
        let namespace = &config.namespaces[0];
        anyhow::ensure!(!namespace.namespace.is_empty(), "namespace cannot be empty");
        anyhow::ensure!(
            namespace.heartbeat_interval_nanos > 0,
            "heartbeat_interval must be greater than 0"
        );
        let (stop_tx, stop_rx) = watch::channel(false);
        let (stopped_tx, stopped_rx) = watch::channel(false);
        Ok(Self {
            namespace: namespace.namespace.clone().into(),
            executor_id: uuid::Uuid::new_v4().to_string().into(),
            heartbeat_interval: Duration::from_nanos(namespace.heartbeat_interval_nanos as u64),
            ttl_shard: Duration::from_nanos(namespace.ttl_shard_nanos.max(0) as u64),
            rpc,
            factory,
            enabled,
            metadata: Arc::new(Mutex::new(metadata)),
            state: Arc::new(ExecutorState::default()),
            stop_tx,
            stop_rx,
            drain_rx,
            supervisor: Arc::new(tokio::sync::Mutex::new(None)),
            lifecycle: Arc::new(AtomicU8::new(LIFECYCLE_NEW)),
            stopped_tx,
            stopped_rx,
            lifecycle_owner: true,
        })
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn executor_id(&self) -> &str {
        &self.executor_id
    }

    pub fn set_metadata(&self, metadata: HashMap<String, String>) {
        *self.metadata.lock().expect("SD metadata lock") = metadata;
    }

    pub fn metadata(&self) -> HashMap<String, String> {
        self.metadata.lock().expect("SD metadata lock").clone()
    }

    /// Start the executor exactly once. A stopped executor cannot be restarted,
    /// matching the closed stop-channel lifecycle of the pinned Go client.
    pub async fn start(&self) -> Result<(), LifecycleError> {
        let mut supervisor_slot = self.supervisor.lock().await;
        self.lifecycle
            .compare_exchange(
                LIFECYCLE_NEW,
                LIFECYCLE_RUNNING,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .map_err(|_| LifecycleError("executor can only be started once"))?;
        let this = self.clone();
        *supervisor_slot = Some(tokio::spawn(async move {
            let heartbeat = this.clone();
            let heartbeat_loop = tokio::spawn(async move { heartbeat.heartbeat_loop().await });
            let cleanup = this.clone();
            let cleanup_loop = tokio::spawn(async move { cleanup.cleanup_loop().await });
            let _ = heartbeat_loop.await;
            let _ = cleanup_loop.await;
            this.lifecycle.store(LIFECYCLE_STOPPED, Ordering::Release);
            let _ = this.stopped_tx.send(true);
        }));
        Ok(())
    }

    /// Go's Stop closes the stop signal, stops all processors, reports
    /// DRAINING with a bounded RPC deadline, and joins both background loops.
    pub async fn shutdown(&self) {
        loop {
            match self.lifecycle.load(Ordering::Acquire) {
                LIFECYCLE_NEW => {
                    if self
                        .lifecycle
                        .compare_exchange(
                            LIFECYCLE_NEW,
                            LIFECYCLE_STOPPED,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        let _ = self.stop_tx.send(true);
                        self.stop_all_processors().await;
                        let _ = self.stopped_tx.send(true);
                        break;
                    }
                }
                LIFECYCLE_RUNNING => {
                    if self
                        .lifecycle
                        .compare_exchange(
                            LIFECYCLE_RUNNING,
                            LIFECYCLE_STOPPING,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        let _ = self.stop_tx.send(true);
                        break;
                    }
                }
                LIFECYCLE_STOPPING | LIFECYCLE_STOPPED => break,
                _ => unreachable!("invalid executor lifecycle"),
            }
        }
        self.wait_stopped().await;
        if let Some(supervisor) = self.supervisor.lock().await.take() {
            let _ = supervisor.await;
        }
    }

    async fn wait_stopped(&self) {
        let mut stopped = self.stopped_rx.clone();
        while !*stopped.borrow_and_update() {
            if stopped.changed().await.is_err() {
                return;
            }
        }
    }

    pub async fn get_shard_process(
        &self,
        shard_key: &str,
    ) -> Result<Arc<dyn ShardProcessor>, GetShardProcessError> {
        self.get_shard_process_raw(shard_key.as_bytes()).await
    }

    pub async fn get_shard_process_raw(&self, shard_key: &[u8]) -> Result<Arc<dyn ShardProcessor>, GetShardProcessError> {
        self.state.last_use.lock().expect("SD last-use lock").insert(shard_key.to_vec(), Instant::now());
        if let Some(processor) = self.processor(shard_key) { return Ok(processor); }
        let not_found = || GetShardProcessError::NotFound(String::from_utf8_lossy(shard_key).into_owned());
        if !(self.enabled)() { return Err(not_found()); }
        self.heartbeat_and_update().await.map_err(GetShardProcessError::Heartbeat)?;
        self.processor(shard_key).ok_or_else(not_found)
    }

    /// Return the current local processor snapshot without heartbeat I/O.
    /// QueueRegistry uses this only to recheck assignment while publishing or
    /// unloading managers after the async RPC path already refreshed it.
    pub(crate) fn has_local_shard_process(&self, shard_key: &str) -> bool {
        self.processor(shard_key.as_bytes()).is_some()
    }

    pub fn has_local_shard_process_raw(&self, shard_key: &[u8]) -> bool {
        self.processor(shard_key).is_some()
    }

    fn processor(&self, shard_key: &[u8]) -> Option<Arc<dyn ShardProcessor>> {
        self.state
            .processors
            .lock()
            .expect("SD processors lock")
            .get(shard_key)
            .filter(|managed| managed.state.load(Ordering::Acquire) != PROCESSOR_STOPPING)
            .map(|managed| managed.processor.clone())
    }

    fn heartbeat_request(&self, status: sd::ExecutorStatus) -> RawHeartbeatRequest {
        let reports = self
            .state
            .processors
            .lock()
            .expect("SD processors lock")
            .iter()
            .filter(|&(_, managed)| managed.state.load(Ordering::Acquire) == PROCESSOR_STARTED)
            .map(|(key, managed)| {
                let report = managed.processor.report();
                RawReportEntry {
                    key: key.clone(),
                    value: Some(sd::ShardStatusReport { status: report.status as i32, shard_load: report.load }),
                }
            })
            .collect();
        RawHeartbeatRequest {
            namespace: self.namespace.to_string(),
            executor_id: self.executor_id.to_string(),
            status: status as i32,
            reports,
            metadata: self.metadata(),
        }
    }

    async fn send_heartbeat(
        &self,
        status: sd::ExecutorStatus,
        timeout: Duration,
    ) -> Result<RawHeartbeatResponse, String> {
        self.rpc
            .heartbeat_raw(self.heartbeat_request(status), timeout)
            .await
    }

    async fn heartbeat_and_update(&self) -> Result<(), String> {
        let Ok(_assignment) = self.state.assignment_lock.try_lock() else {
            // The Go executor skips a concurrent tick while assignment is in
            // progress; GetShardProcess then performs its normal local recheck.
            return Ok(());
        };
        if *self.stop_rx.borrow() {
            return Err("shard-distributor executor stopped".into());
        }
        let response = self
            .send_heartbeat(sd::ExecutorStatus::Active, RPC_TIMEOUT)
            .await?;
        self.update_assignments(response.assignments.into_iter().map(|entry| (entry.key, entry.value.unwrap_or_default())).collect()).await;
        Ok(())
    }

    async fn update_assignments(&self, assignments: HashMap<Vec<u8>, sd::ShardAssignment>) {
        let to_stop: Vec<_> = self
            .state
            .processors
            .lock()
            .expect("SD processors lock")
            .keys()
            .filter_map(|key| {
                let ready = assignments.get(key).is_some_and(|assignment| {
                    assignment.status == sd::AssignmentStatus::Ready as i32
                });
                (!ready).then_some(key.clone())
            })
            .collect();
        for key in to_stop {
            self.stop_processor(&key);
        }
        for (key, assignment) in assignments {
            if assignment.status == sd::AssignmentStatus::Ready as i32 {
                self.start_processor(key).await;
            }
        }
    }

    async fn start_processor(&self, shard_key: Vec<u8>) {
        if self
            .state
            .processors
            .lock()
            .expect("SD processors lock")
            .contains_key(&shard_key)
        {
            return;
        }
        let processor = match self.factory.create_raw(shard_key.clone()).await {
            Ok(processor) => processor,
            Err(_) => return,
        };
        let managed = Arc::new(ManagedProcessor {
            processor: processor.clone(),
            state: AtomicU8::new(PROCESSOR_STARTING),
        });
        {
            let mut processors = self.state.processors.lock().expect("SD processors lock");
            if processors.contains_key(&shard_key) {
                return;
            }
            processors.insert(shard_key.clone(), managed.clone());
        }
        self.state
            .last_use
            .lock()
            .expect("SD last-use lock")
            .insert(shard_key.clone(), Instant::now());

        // Keep Go's timeout-watcher semantics: the operation continues if its
        // ten-second observation window expires. Its completion callback still
        // updates or removes the processor for a later heartbeat retry.
        let state = self.state.clone();
        let (completed_tx, completed_rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let started = processor.start().await.is_ok();
            if started {
                managed.state.store(PROCESSOR_STARTED, Ordering::Release);
            } else {
                let mut processors = state.processors.lock().expect("SD processors lock");
                if processors
                    .get(&shard_key)
                    .is_some_and(|current| Arc::ptr_eq(current, &managed))
                {
                    processors.remove(&shard_key);
                }
            }
            let _ = completed_tx.send(());
        });
        // Go starts processors asynchronously. Observe a ten-second timeout
        // without making assignment/get-shard calls wait for Start().
        tokio::spawn(async move {
            let _ = tokio::time::timeout(PROCESSOR_ASYNC_TIMEOUT, completed_rx).await;
        });
    }

    fn stop_processor(&self, shard_key: &[u8]) {
        let managed = self
            .state
            .processors
            .lock()
            .expect("SD processors lock")
            .remove(shard_key);
        let Some(managed) = managed else { return };
        managed.state.store(PROCESSOR_STOPPING, Ordering::Release);
        let processor = managed.processor.clone();
        let (completed_tx, completed_rx) = tokio::sync::oneshot::channel();
        let stop = tokio::spawn(async move {
            processor.stop().await;
            let _ = completed_tx.send(());
        });
        self.state
            .stopping
            .lock()
            .expect("SD stopping lock")
            .push(stop);
        // This timeout is only an observation window; the processor stop task
        // remains owned by `stopping` and shutdown joins it without cancelling.
        tokio::spawn(async move {
            let _ = tokio::time::timeout(PROCESSOR_ASYNC_TIMEOUT, completed_rx).await;
        });
    }

    async fn stop_all_processors(&self) {
        // An admission heartbeat may already be in flight when shutdown begins.
        // Wait for its assignment update before detaching the final processors.
        let _assignment = self.state.assignment_lock.lock().await;
        let keys: Vec<_> = self
            .state
            .processors
            .lock()
            .expect("SD processors lock")
            .keys()
            .cloned()
            .collect();
        for key in keys {
            self.stop_processor(&key);
        }
        let handles = std::mem::take(&mut *self.state.stopping.lock().expect("SD stopping lock"));
        for handle in handles {
            let _ = handle.await;
        }
    }

    async fn heartbeat_loop(&self) {
        let mut stop = self.stop_rx.clone();
        let mut drain = self.drain_rx.clone();
        loop {
            if *stop.borrow_and_update() {
                self.stop_all_processors().await;
                if (self.enabled)() {
                    let _ = self
                        .send_heartbeat(sd::ExecutorStatus::Draining, DRAINING_HEARTBEAT_TIMEOUT)
                        .await;
                }
                return;
            }
            let delay = jitter(self.heartbeat_interval);
            tokio::select! {
                _ = tokio::time::sleep(delay) => {
                    if (self.enabled)() {
                        let _ = self.heartbeat_and_update().await;
                    }
                }
                changed = stop.changed() => {
                    if changed.is_err() || *stop.borrow_and_update() {
                        self.stop_all_processors().await;
                        if (self.enabled)() {
                            let _ = self.send_heartbeat(sd::ExecutorStatus::Draining, DRAINING_HEARTBEAT_TIMEOUT).await;
                        }
                        return;
                    }
                }
                changed = wait_for_drain(&mut drain) => {
                    if changed {
                        self.stop_all_processors().await;
                        if (self.enabled)() {
                            let _ = self.send_heartbeat(sd::ExecutorStatus::Draining, DRAINING_HEARTBEAT_TIMEOUT).await;
                        }
                        if !self.wait_for_undrain(&mut drain, &mut stop).await {
                            return;
                        }
                    }
                }
            }
        }
    }

    async fn wait_for_undrain(
        &self,
        drain: &mut Option<watch::Receiver<bool>>,
        stop: &mut watch::Receiver<bool>,
    ) -> bool {
        let Some(drain) = drain else { return false };
        loop {
            if *stop.borrow_and_update() {
                return false;
            }
            tokio::select! {
                changed = stop.changed() => {
                    if changed.is_err() || *stop.borrow_and_update() { return false; }
                }
                changed = drain.changed() => {
                    if changed.is_err() { return false; }
                    if !*drain.borrow_and_update() { return true; }
                }
            }
        }
    }

    async fn cleanup_loop(&self) {
        if self.ttl_shard.is_zero() {
            return;
        }
        let mut stop = self.stop_rx.clone();
        loop {
            if *stop.borrow_and_update() {
                return;
            }
            tokio::select! {
                _ = tokio::time::sleep(jitter(self.ttl_shard)) => {
                    if !(self.enabled)() { continue; }
                    let now = Instant::now();
                    let expired: Vec<_> = {
                        let mut last_use = self.state.last_use.lock().expect("SD last-use lock");
                        let expired = last_use.iter()
                            .filter_map(|(key, last)| (now.duration_since(*last) > self.ttl_shard).then_some(key.clone()))
                            .collect::<Vec<_>>();
                        for key in &expired { last_use.remove(key); }
                        expired
                    };
                    for key in expired {
                        if let Some(processor) = self.processor(&key) {
                            processor.set_status(sd::ShardStatus::Done);
                        }
                    }
                }
                changed = stop.changed() => {
                    if changed.is_err() || *stop.borrow_and_update() { return; }
                }
            }
        }
    }
}

impl Drop for ShardDistributorExecutor {
    fn drop(&mut self) {
        // The owner dropping its handle cancels both loops; spawned clones keep
        // the state alive long enough to stop processors and report draining.
        if self.lifecycle_owner {
            let _ = self.stop_tx.send(true);
        }
    }
}

async fn wait_for_drain(drain: &mut Option<watch::Receiver<bool>>) -> bool {
    let Some(drain) = drain else {
        return std::future::pending().await;
    };
    loop {
        if *drain.borrow_and_update() {
            return true;
        }
        if drain.changed().await.is_err() {
            return false;
        }
    }
}

fn jitter(interval: Duration) -> Duration {
    let spread = (interval.as_nanos() / 10).min(u64::MAX as u128) as u64;
    let width = spread.saturating_mul(2).saturating_add(1);
    let jitter_nanos = if spread == 0 {
        0
    } else {
        rand::random::<u64>() % width
    };
    interval
        .saturating_sub(Duration::from_nanos(spread))
        .saturating_add(Duration::from_nanos(jitter_nanos))
}

fn retryable_status(code: tonic::Code) -> bool {
    matches!(
        code,
        tonic::Code::Unavailable
            | tonic::Code::Unknown
            | tonic::Code::Cancelled
            | tonic::Code::Internal
            | tonic::Code::DeadlineExceeded
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicUsize};
    use tokio_stream::wrappers::TcpListenerStream;

    fn yarpc_headers<T>(request: &tonic::Request<T>) -> [String; 4] {
        ["rpc-service", "rpc-caller", "rpc-encoding", "grpc-timeout"].map(|key| {
            request
                .metadata()
                .get(key)
                .and_then(|value| value.to_str().ok())
                .unwrap_or_default()
                .to_owned()
        })
    }

    #[derive(Clone)]
    struct FakeExecutorService {
        assigned: Arc<AtomicBool>,
        fail: Arc<AtomicBool>,
        requests: Arc<Mutex<Vec<sd::HeartbeatRequest>>>,
        yarpc_metadata: Arc<Mutex<Vec<[String; 4]>>>,
    }

    #[tonic::async_trait]
    impl sd::shard_distributor_executor_api_server::ShardDistributorExecutorApi
        for FakeExecutorService
    {
        async fn heartbeat(
            &self,
            request: tonic::Request<sd::HeartbeatRequest>,
        ) -> Result<tonic::Response<sd::HeartbeatResponse>, tonic::Status> {
            self.yarpc_metadata
                .lock()
                .unwrap()
                .push(yarpc_headers(&request));
            let request = request.into_inner();
            self.requests
                .lock()
                .expect("fake request lock")
                .push(request.clone());
            if self.fail.load(Ordering::SeqCst) {
                return Err(tonic::Status::unavailable(
                    "source-derived transient failure",
                ));
            }
            let shard_assignments = if self.assigned.load(Ordering::SeqCst) {
                HashMap::from([(
                    "task-list-a".to_string(),
                    sd::ShardAssignment {
                        status: sd::AssignmentStatus::Ready as i32,
                    },
                )])
            } else {
                HashMap::new()
            };
            Ok(tonic::Response::new(sd::HeartbeatResponse {
                shard_assignments,
                migration_mode: sd::MigrationMode::Onboarded as i32,
            }))
        }
    }

    #[derive(Default)]
    struct FakeProcessor {
        starts: AtomicUsize,
        stops: Arc<AtomicUsize>,
        stop_complete: Arc<tokio::sync::Notify>,
        status: AtomicU8,
    }

    impl ShardProcessor for FakeProcessor {
        fn start(&self) -> BoxFuture<Result<(), String>> {
            self.starts.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Ok(()) })
        }
        fn stop(&self) -> BoxFuture<()> {
            let stops = self.stops.clone();
            let stop_complete = self.stop_complete.clone();
            Box::pin(async move {
                stops.fetch_add(1, Ordering::SeqCst);
                stop_complete.notify_one();
            })
        }
        fn report(&self) -> ShardReport {
            ShardReport {
                status: sd::ShardStatus::Ready,
                load: 0.25,
            }
        }
        fn set_status(&self, status: sd::ShardStatus) {
            self.status.store(status as u8, Ordering::SeqCst);
        }
    }

    struct FakeFactory(Arc<FakeProcessor>);
    impl ShardProcessorFactory for FakeFactory {
        fn create(&self, _shard_key: String) -> BoxFuture<Result<Arc<dyn ShardProcessor>, String>> {
            let processor: Arc<dyn ShardProcessor> = self.0.clone();
            Box::pin(async move { Ok(processor) })
        }
    }

    #[derive(Clone)]
    struct FakeSpectatorService {
        get_requests: Arc<Mutex<Vec<sd::GetShardOwnerRequest>>>,
        yarpc_metadata: Arc<Mutex<Vec<[String; 4]>>>,
    }

    #[tonic::async_trait]
    impl sd::shard_distributor_api_server::ShardDistributorApi for FakeSpectatorService {
        async fn get_shard_owner(
            &self,
            request: tonic::Request<sd::GetShardOwnerRequest>,
        ) -> Result<tonic::Response<sd::GetShardOwnerResponse>, tonic::Status> {
            self.yarpc_metadata
                .lock()
                .unwrap()
                .push(yarpc_headers(&request));
            let request = request.into_inner();
            self.get_requests.lock().unwrap().push(request.clone());
            Ok(tonic::Response::new(sd::GetShardOwnerResponse {
                owner: "source-go-executor".into(),
                namespace: request.namespace,
                metadata: HashMap::from([("grpc".into(), "7835".into())]),
            }))
        }

        async fn inspect_shard(
            &self,
            _request: tonic::Request<sd::InspectShardRequest>,
        ) -> Result<tonic::Response<sd::InspectShardResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("unused"))
        }

        async fn get_namespace_state(
            &self,
            _request: tonic::Request<sd::GetNamespaceStateRequest>,
        ) -> Result<tonic::Response<sd::GetNamespaceStateResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("unused"))
        }

        async fn watch_namespace_state(
            &self,
            request: tonic::Request<sd::WatchNamespaceStateRequest>,
        ) -> Result<
            tonic::Response<tonic::codegen::BoxStream<sd::WatchNamespaceStateResponse>>,
            tonic::Status,
        > {
            self.yarpc_metadata
                .lock()
                .unwrap()
                .push(yarpc_headers(&request));
            let stream = tokio_stream::iter(vec![Ok(sd::WatchNamespaceStateResponse {
                executors: Vec::new(),
            })]);
            Ok(tonic::Response::new(Box::pin(stream)))
        }

        async fn list_namespaces(
            &self,
            _request: tonic::Request<sd::ListNamespacesRequest>,
        ) -> Result<tonic::Response<sd::ListNamespacesResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("unused"))
        }

        async fn drain_shards(
            &self,
            _request: tonic::Request<sd::DrainShardsRequest>,
        ) -> Result<tonic::Response<sd::DrainShardsResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("unused"))
        }

        async fn undrain_shards(
            &self,
            _request: tonic::Request<sd::UndrainShardsRequest>,
        ) -> Result<tonic::Response<sd::UndrainShardsResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("unused"))
        }

        async fn get_drained_shards(
            &self,
            _request: tonic::Request<sd::GetDrainedShardsRequest>,
        ) -> Result<tonic::Response<sd::GetDrainedShardsResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("unused"))
        }

        async fn force_reset_namespace(
            &self,
            _request: tonic::Request<sd::ForceResetNamespaceRequest>,
        ) -> Result<tonic::Response<sd::ForceResetNamespaceResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("unused"))
        }
    }

    #[tokio::test]
    async fn pinned_executor_heartbeat_assignment_and_drain_contract() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let assigned = Arc::new(AtomicBool::new(true));
        let fail = Arc::new(AtomicBool::new(false));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let yarpc_metadata = Arc::new(Mutex::new(Vec::new()));
        let service = FakeExecutorService {
            assigned: assigned.clone(),
            fail: fail.clone(),
            requests: requests.clone(),
            yarpc_metadata: yarpc_metadata.clone(),
        };
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(
                    sd::shard_distributor_executor_api_server::ShardDistributorExecutorApiServer::new(service),
                )
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        let channel = tonic::transport::Endpoint::from_shared(format!("http://{address}"))
            .unwrap()
            .connect_lazy();
        let rpc = Arc::new(TonicExecutorRpc::from_channel(channel));
        let processor = Arc::new(FakeProcessor::default());
        let config = crate::config::ShardDistributorMatchingConfig {
            namespaces: vec![crate::config::ShardDistributorNamespaceConfig {
                namespace: "cadence-matching-dev".into(),
                heartbeat_interval_nanos: 1_000_000_000,
                ttl_shard_nanos: 0,
                ttl_report_nanos: 60_000_000_000,
            }],
            peer_ttl_nanos: 0,
        };
        let executor = ShardDistributorExecutor::new(
            &config,
            rpc,
            Arc::new(FakeFactory(processor.clone())),
            Arc::new(|| true),
            HashMap::from([
                ("grpc".into(), "7835".into()),
                ("hostIP".into(), "127.0.0.1".into()),
                ("tchannel".into(), "7935".into()),
            ]),
            None,
        )
        .unwrap();

        let owned = executor.get_shard_process("task-list-a").await.unwrap();
        assert_eq!(owned.report().load, 0.25);
        tokio::time::sleep(Duration::from_millis(5)).await;
        assert_eq!(processor.starts.load(Ordering::SeqCst), 1);
        executor.heartbeat_and_update().await.unwrap();
        let report = requests.lock().unwrap().last().unwrap().clone();
        assert_eq!(report.namespace, "cadence-matching-dev");
        assert!(!report.executor_id.is_empty());
        assert_eq!(report.status, sd::ExecutorStatus::Active as i32);
        assert_eq!(report.metadata["grpc"], "7835");
        assert_eq!(report.metadata["tchannel"], "7935");
        assert_eq!(report.metadata["hostIP"], "127.0.0.1");
        assert_eq!(
            &yarpc_metadata.lock().unwrap().last().unwrap()[..3],
            &["shard-manager", "cadence-matching", "proto"]
        );
        assert!(
            !yarpc_metadata.lock().unwrap().last().unwrap()[3].is_empty(),
            "Heartbeat has an RPC deadline"
        );
        assert_eq!(
            report.shard_status_reports["task-list-a"].status,
            sd::ShardStatus::Ready as i32
        );
        assert_eq!(report.shard_status_reports["task-list-a"].shard_load, 0.25);

        assigned.store(false, Ordering::SeqCst);
        executor.heartbeat_and_update().await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), processor.stop_complete.notified())
            .await
            .expect("asynchronous shard processor stop should complete");
        assert_eq!(processor.stops.load(Ordering::SeqCst), 1);
        assert!(matches!(
            executor.get_shard_process("task-list-b").await,
            Err(GetShardProcessError::NotFound(key)) if key == "task-list-b"
        ));
        fail.store(true, Ordering::SeqCst);
        assert!(matches!(
            executor.get_shard_process("task-list-b").await,
            Err(GetShardProcessError::Heartbeat(_))
        ));
        fail.store(false, Ordering::SeqCst);
        executor.start().await.unwrap();
        executor.shutdown().await;
        assert!(executor.start().await.is_err(), "shutdown is terminal");
        assert_eq!(
            requests.lock().unwrap().last().unwrap().status,
            sd::ExecutorStatus::Draining as i32
        );
        server.abort();
    }

    struct RawTestRpc {
        assigned: Arc<AtomicBool>,
        reports: Arc<Mutex<Vec<Vec<u8>>>>,
    }
    impl ExecutorRpc for RawTestRpc {
        fn heartbeat(&self, _: sd::HeartbeatRequest, _: Duration) -> BoxFuture<Result<sd::HeartbeatResponse, String>> {
            Box::pin(async { Err("unexpected string RPC".into()) })
        }
        fn heartbeat_raw(&self, request: RawHeartbeatRequest, _: Duration) -> BoxFuture<Result<RawHeartbeatResponse, String>> {
            self.reports.lock().unwrap().extend(request.reports.into_iter().map(|report| report.key));
            let assigned = self.assigned.load(Ordering::SeqCst);
            Box::pin(async move { Ok(RawHeartbeatResponse {
                assignments: if assigned { vec![RawAssignmentEntry { key: b"queue-\xff".to_vec(), value: Some(sd::ShardAssignment { status: sd::AssignmentStatus::Ready as i32 }) }] } else { vec![] },
                migration_mode: 0,
            }) })
        }
    }
    struct RawTestFactory { keys: Arc<Mutex<Vec<Vec<u8>>>>, processor: Arc<FakeProcessor> }
    impl ShardProcessorFactory for RawTestFactory {
        fn create(&self, _: String) -> BoxFuture<Result<Arc<dyn ShardProcessor>, String>> {
            Box::pin(async { Err("unexpected string factory".into()) })
        }
        fn create_raw(&self, key: Vec<u8>) -> BoxFuture<Result<Arc<dyn ShardProcessor>, String>> {
            self.keys.lock().unwrap().push(key);
            let processor: Arc<dyn ShardProcessor> = self.processor.clone();
            Box::pin(async move { Ok(processor) })
        }
    }
    #[tokio::test]
    async fn raw_executor_assigns_reports_and_revokes_exact_bytes() {
        let assigned = Arc::new(AtomicBool::new(true));
        let reports = Arc::new(Mutex::new(Vec::new()));
        let keys = Arc::new(Mutex::new(Vec::new()));
        let processor = Arc::new(FakeProcessor::default());
        let config = crate::config::ShardDistributorMatchingConfig {
            namespaces: vec![crate::config::ShardDistributorNamespaceConfig {
                namespace: "matching".into(), heartbeat_interval_nanos: 60_000_000_000,
                ttl_shard_nanos: 0, ttl_report_nanos: 0,
            }], peer_ttl_nanos: 0,
        };
        let executor = ShardDistributorExecutor::new(&config,
            Arc::new(RawTestRpc { assigned: assigned.clone(), reports: reports.clone() }),
            Arc::new(RawTestFactory { keys: keys.clone(), processor: processor.clone() }),
            Arc::new(|| true), HashMap::new(), None).unwrap();
        executor.get_shard_process_raw(b"queue-\xff").await.unwrap();
        assert_eq!(keys.lock().unwrap().as_slice(), &[b"queue-\xff".to_vec()]);
        tokio::time::timeout(Duration::from_secs(1), async {
            while executor.state.processors.lock().unwrap().get(b"queue-\xff".as_slice()).is_none_or(|p| p.state.load(Ordering::Acquire) != PROCESSOR_STARTED) { tokio::task::yield_now().await; }
        }).await.unwrap();
        executor.heartbeat_and_update().await.unwrap();
        assert_eq!(reports.lock().unwrap().as_slice(), &[b"queue-\xff".to_vec()]);
        assigned.store(false, Ordering::SeqCst);
        executor.heartbeat_and_update().await.unwrap();
        assert!(!executor.has_local_shard_process_raw(b"queue-\xff"));
        tokio::time::timeout(Duration::from_secs(1), processor.stop_complete.notified()).await.unwrap();
        executor.shutdown().await;
        assert_eq!(processor.stops.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn raw_heartbeat_map_entries_keep_arbitrary_bytes_on_wire() {
        use prost::Message;
        let response = RawHeartbeatResponse { assignments: vec![RawAssignmentEntry {
            key: b"q\xff".to_vec(), value: Some(sd::ShardAssignment { status: sd::AssignmentStatus::Ready as i32 }),
        }], migration_mode: 0 };
        let decoded = RawHeartbeatResponse::decode(response.encode_to_vec().as_slice()).unwrap();
        assert_eq!(decoded.assignments[0].key, b"q\xff");
        let request = RawHeartbeatRequest { namespace: "ns".into(), executor_id: "id".into(), status: 0,
            reports: vec![RawReportEntry { key: b"q\xff".to_vec(), value: Some(sd::ShardStatusReport { status: 0, shard_load: 0.5 }) }], metadata: HashMap::new() };
        let decoded = RawHeartbeatRequest::decode(request.encode_to_vec().as_slice()).unwrap();
        assert_eq!(decoded.reports[0].key, b"q\xff");
    }

    #[test]
    fn go_executor_ttl_and_jitter_boundaries_are_preserved() {
        for _ in 0..100 {
            let interval = jitter(Duration::from_secs(10));
            assert!((Duration::from_secs(9)..=Duration::from_secs(11)).contains(&interval));
        }
    }

    #[tokio::test]
    async fn spectator_waits_for_initial_snapshot_and_returns_cached_owner() {
        let channel = tonic::transport::Endpoint::from_static("http://127.0.0.1:1").connect_lazy();
        let spectator = TonicSpectator::new(channel, "matching", Arc::new(|| true));
        *spectator.owners.write().unwrap() = Some(HashMap::from([(
            b"task-list-a".to_vec(),
            ShardOwner {
                executor_id: "executor-a".into(),
                metadata: HashMap::from([("grpc".into(), "7835".into())]),
            },
        )]));
        let _ = spectator.first_state_tx.send(true);
        let owner = spectator
            .get_shard_owner(
                "task-list-a",
                tokio::time::Instant::now() + Duration::from_secs(1),
            )
            .await
            .unwrap();
        assert_eq!(owner.executor_id, "executor-a");
        assert_eq!(owner.metadata["grpc"], "7835");
        spectator.reset_first_state();
        assert!(!*spectator.first_state_rx.borrow());
        assert!(spectator.owners.read().unwrap().is_none());
    }

    type RawOwnerRequests = Arc<Mutex<Vec<(Vec<u8>, [String; 4])>>>;

    #[derive(Clone)]
    struct RawSpectatorService {
        requests: RawOwnerRequests,
    }

    impl tonic::server::NamedService for RawSpectatorService {
        const NAME: &'static str = "uber.cadence.sharddistributor.v1.ShardDistributorAPI";
    }

    impl<B> tonic::codegen::Service<tonic::codegen::http::Request<B>> for RawSpectatorService
    where
        B: tonic::codegen::Body + Send + 'static,
        B::Error: Into<tonic::codegen::StdError> + Send + 'static,
    {
        type Response = tonic::codegen::http::Response<tonic::body::BoxBody>;
        type Error = std::convert::Infallible;
        type Future = tonic::codegen::BoxFuture<Self::Response, Self::Error>;

        fn poll_ready(
            &mut self,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), Self::Error>> {
            std::task::Poll::Ready(Ok(()))
        }

        fn call(&mut self, request: tonic::codegen::http::Request<B>) -> Self::Future {
            #[derive(Clone)]
            struct Get(RawOwnerRequests);
            impl tonic::server::UnaryService<RawOwnerRequest> for Get {
                type Response = sd::GetShardOwnerResponse;
                type Future =
                    tonic::codegen::BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                fn call(&mut self, request: tonic::Request<RawOwnerRequest>) -> Self::Future {
                    let headers = yarpc_headers(&request);
                    self.0
                        .lock()
                        .unwrap()
                        .push((request.into_inner().shard_key, headers));
                    Box::pin(async {
                        Ok(tonic::Response::new(sd::GetShardOwnerResponse {
                            owner: "rpc-owner".into(),
                            namespace: "matching".into(),
                            metadata: HashMap::new(),
                        }))
                    })
                }
            }
            struct Watch;
            impl tonic::server::ServerStreamingService<sd::WatchNamespaceStateRequest> for Watch {
                type Response = RawSnapshot;
                type ResponseStream = tonic::codegen::BoxStream<Self::Response>;
                type Future =
                    tonic::codegen::BoxFuture<tonic::Response<Self::ResponseStream>, tonic::Status>;
                fn call(
                    &mut self,
                    _request: tonic::Request<sd::WatchNamespaceStateRequest>,
                ) -> Self::Future {
                    let snapshot = RawSnapshot {
                        executors: [
                            (vec![0xff], "owner-ff"),
                            (vec![0xfe], "owner-fe"),
                            ("�".as_bytes().to_vec(), "owner-replacement"),
                        ]
                        .into_iter()
                        .map(|(shard_key, owner)| RawExecutor {
                            executor_id: owner.into(),
                            metadata: HashMap::new(),
                            shards: vec![RawShard { shard_key }],
                        })
                        .collect(),
                    };
                    Box::pin(async move {
                        let stream: tonic::codegen::BoxStream<RawSnapshot> =
                            Box::pin(tokio_stream::iter(vec![Ok(snapshot)]));
                        Ok(tonic::Response::new(stream))
                    })
                }
            }
            let path = request.uri().path().to_owned();
            let requests = self.requests.clone();
            Box::pin(async move {
                let response = if path.ends_with("/GetShardOwner") {
                    tonic::server::Grpc::new(tonic::codec::ProstCodec::default())
                        .unary(Get(requests), request)
                        .await
                } else {
                    tonic::server::Grpc::new(tonic::codec::ProstCodec::default())
                        .server_streaming(Watch, request)
                        .await
                };
                Ok(response)
            })
        }
    }

    #[tokio::test]
    async fn raw_snapshot_cache_and_owner_rpc_preserve_key_bytes() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let service = RawSpectatorService {
            requests: requests.clone(),
        };
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(service)
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        let channel = tonic::transport::Endpoint::from_shared(format!("http://{address}"))
            .unwrap()
            .connect_lazy();
        let spectator = TonicSpectator::new(channel, "matching", Arc::new(|| true));
        tokio::time::timeout(Duration::from_secs(2), spectator.start())
            .await
            .unwrap()
            .unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        for (key, expected_owner) in [
            (&[0xff][..], "owner-ff"),
            (&[0xfe][..], "owner-fe"),
            ("�".as_bytes(), "owner-replacement"),
        ] {
            let cached = tokio::time::timeout(
                Duration::from_secs(2),
                spectator.get_shard_owner_bytes(key, deadline),
            )
            .await
            .unwrap()
            .unwrap();
            assert_eq!(cached.executor_id, expected_owner);
        }
        assert!(requests.lock().unwrap().is_empty());
        let missing = [0xfd, 0, b'b'];
        let owner = tokio::time::timeout(
            Duration::from_secs(2),
            spectator.get_shard_owner_bytes(&missing, deadline),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(owner.executor_id, "rpc-owner");
        {
            let observed = requests.lock().unwrap();
            assert_eq!(observed.len(), 1, "cache hits must not issue owner RPCs");
            assert_eq!(
                observed[0].0, missing,
                "GetShardOwner must preserve raw key bytes"
            );
            assert_eq!(
                &observed[0].1[..3],
                &["shard-manager", "cadence-matching", "proto"]
            );
            assert!(
                !observed[0].1[3].is_empty(),
                "GetShardOwner carries its deadline"
            );
        }
        tokio::time::timeout(Duration::from_secs(2), spectator.shutdown())
            .await
            .unwrap();
        server.abort();
        let _ = tokio::time::timeout(Duration::from_secs(2), server).await;
    }

    #[tokio::test]
    async fn spectator_fake_tonic_peer_covers_snapshot_fallback_and_terminal_shutdown() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let get_requests = Arc::new(Mutex::new(Vec::new()));
        let yarpc_metadata = Arc::new(Mutex::new(Vec::new()));
        let server = tokio::spawn({
            let service = FakeSpectatorService {
                get_requests: get_requests.clone(),
                yarpc_metadata: yarpc_metadata.clone(),
            };
            async move {
                tonic::transport::Server::builder()
                    .add_service(
                        sd::shard_distributor_api_server::ShardDistributorApiServer::new(service),
                    )
                    .serve_with_incoming(TcpListenerStream::new(listener))
                    .await
                    .unwrap();
            }
        });
        let channel = tonic::transport::Endpoint::from_shared(format!("http://{address}"))
            .unwrap()
            .connect_lazy();
        let spectator = TonicSpectator::new(channel, "matching", Arc::new(|| true));
        spectator.start().await.unwrap();
        let owner = tokio::time::timeout(
            Duration::from_secs(2),
            spectator.get_shard_owner(
                "remote-task-list",
                tokio::time::Instant::now() + Duration::from_secs(2),
            ),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(owner.executor_id, "source-go-executor");
        assert_eq!(owner.metadata["grpc"], "7835");
        assert_eq!(get_requests.lock().unwrap()[0].namespace, "matching");
        assert_eq!(
            get_requests.lock().unwrap()[0].shard_key,
            "remote-task-list"
        );
        {
            let metadata = yarpc_metadata.lock().unwrap();
            assert!(metadata.len() >= 2, "watch and GetShardOwner were called");
            assert!(metadata
                .iter()
                .all(|headers| headers[..3] == ["shard-manager", "cadence-matching", "proto"]));
            assert!(
                metadata.iter().any(|headers| headers[3].is_empty()),
                "long-lived WatchNamespaceState has no RPC deadline"
            );
            assert!(
                metadata.iter().any(|headers| !headers[3].is_empty()),
                "GetShardOwner carries its RPC deadline"
            );
        }

        let (first_shutdown, second_shutdown) =
            tokio::join!(spectator.shutdown(), spectator.shutdown());
        let _ = (first_shutdown, second_shutdown);
        assert!(spectator.start().await.is_err(), "shutdown is terminal");
        assert!(spectator
            .get_shard_owner(
                "remote-task-list",
                tokio::time::Instant::now() + Duration::from_secs(10),
            )
            .await
            .is_err());
        server.abort();
    }

    #[tokio::test]
    async fn spectator_initial_snapshot_wait_obeys_caller_deadline() {
        let channel = tonic::transport::Endpoint::from_static("http://127.0.0.1:1").connect_lazy();
        let spectator = TonicSpectator::new(channel, "matching", Arc::new(|| true));
        let error = spectator
            .get_shard_owner(
                "task-list-a",
                tokio::time::Instant::now() + Duration::from_millis(1),
            )
            .await
            .unwrap_err();
        assert!(error.contains("deadline"));
    }

    #[tokio::test]
    async fn spectator_shutdown_before_start_is_terminal_and_wakes_waiters() {
        let channel = tonic::transport::Endpoint::from_static("http://127.0.0.1:1").connect_lazy();
        let spectator = TonicSpectator::new(channel, "matching", Arc::new(|| true));
        let waiting = {
            let spectator = spectator.clone();
            tokio::spawn(async move {
                spectator
                    .get_shard_owner(
                        "task-list-a",
                        tokio::time::Instant::now() + Duration::from_secs(10),
                    )
                    .await
            })
        };
        spectator.shutdown().await;
        assert!(waiting.await.unwrap().is_err());
        assert!(spectator.start().await.is_err());
    }

    #[tokio::test]
    async fn executor_shutdown_before_start_is_terminal() {
        let config = crate::config::ShardDistributorMatchingConfig {
            namespaces: vec![crate::config::ShardDistributorNamespaceConfig {
                namespace: "matching".into(),
                heartbeat_interval_nanos: 1_000_000_000,
                ttl_shard_nanos: 0,
                ttl_report_nanos: 60_000_000_000,
            }],
            peer_ttl_nanos: 0,
        };
        let channel = tonic::transport::Endpoint::from_static("http://127.0.0.1:1").connect_lazy();
        let executor = ShardDistributorExecutor::new(
            &config,
            Arc::new(TonicExecutorRpc::from_channel(channel)),
            Arc::new(FakeFactory(Arc::new(FakeProcessor::default()))),
            Arc::new(|| true),
            HashMap::new(),
            None,
        )
        .unwrap();
        let (first_shutdown, second_shutdown) =
            tokio::join!(executor.shutdown(), executor.shutdown());
        let _ = (first_shutdown, second_shutdown);
        assert!(executor.start().await.is_err());
    }
}
