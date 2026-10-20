//! Matching task-list ownership routing during shard-distributor onboarding.
//!
//! This adapter selects between the current Ringpop membership snapshot and
//! the real shard-distributor spectator. It returns endpoint details and the
//! selected path; the current Matching queue API is synchronous, so runtime
//! integration must await this adapter before constructing/reusing a manager.

use crate::{
    matching_membership::Membership,
    matching_shard_distributor::{BoxFuture, ShardOwner, TonicSpectator},
};
use cadence_common::membership::Host;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use thiserror::Error;
use tokio::task::JoinHandle;
use tokio::time::Instant;

pub const SHARD_DISTRIBUTOR_LOOKUP_TIMEOUT: Duration = Duration::from_secs(1);
const PORT_TCHANNEL: &str = "tchannel";
const PORT_GRPC: &str = "grpc";
const HOST_IP: &str = "hostIP";

/// Go `membership.SingleProvider.Lookup` identity plus named endpoints.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedHost {
    /// Go `HostInfo.GetAddress()`, normally the TChannel address.
    pub address: String,
    pub identity: String,
    /// Successfully discovered named ports. Invalid/missing ports are omitted.
    pub named_ports: HashMap<String, u16>,
    /// Ring-pop's explicit gRPC endpoint when available; shard-distributor
    /// owners derive this from `hostIP` and the named grpc port.
    pub grpc_address: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RoutingPath {
    HashRing,
    ShardDistributor,
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum RoutingError {
    #[error("Ringpop could not select an owner: {0}")]
    RingUnavailable(String),
    #[error("shard distributor owner lookup failed: {0}")]
    ShardDistributor(String),
    #[error("hostIP not found in shard distributor owner metadata")]
    MissingHostIp,
    #[error("could not connect to selected Matching owner: {0}")]
    Endpoint(String, RoutingPath),
}

impl RoutingError {
    /// Preserve Go's routing-path metric choice even when a lookup fails.
    #[must_use]
    pub fn path(&self) -> RoutingPath {
        match self {
            Self::RingUnavailable(_) => RoutingPath::HashRing,
            Self::ShardDistributor(_) | Self::MissingHostIp => RoutingPath::ShardDistributor,
            Self::Endpoint(_, path) => *path,
        }
    }
}

/// Async boundary over the shard-distributor spectator API. The production
/// implementation below uses `TonicSpectator`; this trait lets routing tests
/// assert keys/deadlines without a network client or static route substitutes.
pub trait ShardOwnerLookup: Send + Sync + 'static {
    fn get_shard_owner(
        &self,
        shard_key: String,
        deadline: Instant,
    ) -> BoxFuture<Result<ShardOwner, String>>;
}

impl ShardOwnerLookup for TonicSpectator {
    fn get_shard_owner(
        &self,
        shard_key: String,
        deadline: Instant,
    ) -> BoxFuture<Result<ShardOwner, String>> {
        let spectator = self.clone();
        Box::pin(async move { spectator.get_shard_owner(&shard_key, deadline).await })
    }
}

/// Dynamic Matching routing policy. The property closures are evaluated on
/// every lookup, matching Go's live dynamic-config reads.
pub struct MatchingShardRouter {
    ring: Membership,
    spectator: Option<Arc<dyn ShardOwnerLookup>>,
    percentage_onboarded: Arc<dyn Fn() -> i64 + Send + Sync>,
    exclude_short_lived_task_lists: Arc<dyn Fn() -> bool + Send + Sync>,
    peer_ttl: Duration,
    peer_channels: Arc<Mutex<HashMap<String, (tonic::transport::Channel, Instant)>>>,
}

impl MatchingShardRouter {
    pub fn new(
        ring: Membership,
        spectator: Option<Arc<dyn ShardOwnerLookup>>,
        percentage_onboarded: Arc<dyn Fn() -> i64 + Send + Sync>,
        exclude_short_lived_task_lists: Arc<dyn Fn() -> bool + Send + Sync>,
    ) -> Self {
        Self {
            ring,
            spectator,
            percentage_onboarded,
            exclude_short_lived_task_lists,
            peer_ttl: Duration::from_secs(120),
            peer_channels: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Configure Go's spectator peer-cache TTL. The caller passes the
    /// normalized `clientcommon.Config.GetPeerTTL()` value.
    #[must_use]
    pub fn with_peer_ttl(mut self, peer_ttl: Duration) -> Self {
        self.peer_ttl = peer_ttl.max(Duration::from_millis(1));
        self
    }

    /// Start Go's half-TTL stale-peer eviction loop and return its owner.
    /// Dropping the sender stops the loop; callers should join the task during
    /// Matching shutdown.
    pub fn start_peer_cleanup(&self) -> (tokio::sync::oneshot::Sender<()>, JoinHandle<()>) {
        let channels = self.peer_channels.clone();
        let peer_ttl = self.peer_ttl;
        let (stop, mut stopped) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let mut interval = tokio::time::interval((peer_ttl / 2).max(Duration::from_millis(1)));
            loop {
                tokio::select! {
                    _ = &mut stopped => break,
                    _ = interval.tick() => {
                        let now = Instant::now();
                        channels
                            .lock()
                            .expect("Matching shard peer cache lock")
                            .retain(|_, (_, last_used)| now.duration_since(*last_used) <= peer_ttl);
                    }
                }
            }
        });
        (stop, task)
    }

    /// Production convenience constructor using the accepted cached/RPC
    /// `TonicSpectator` implementation.
    pub fn with_tonic_spectator(
        ring: Membership,
        spectator: Option<TonicSpectator>,
        percentage_onboarded: Arc<dyn Fn() -> i64 + Send + Sync>,
        exclude_short_lived_task_lists: Arc<dyn Fn() -> bool + Send + Sync>,
    ) -> Self {
        Self::new(
            ring,
            spectator.map(|client| Arc::new(client) as Arc<dyn ShardOwnerLookup>),
            percentage_onboarded,
            exclude_short_lived_task_lists,
        )
    }

    /// Resolve one task-list owner. Eligible requests use an independent
    /// one-second spectator deadline, as Go's `context.WithTimeout` does;
    /// errors from an eligible spectator lookup propagate without Ringpop
    /// fallback. Excluded requests and missing spectators use the current ring.
    pub async fn resolve(
        &self,
        task_list_name: &str,
    ) -> Result<(ResolvedHost, RoutingPath), RoutingError> {
        let percentage = (self.percentage_onboarded)() as u64;
        let excluded = task_list_excluded_from_shard_distributor(
            task_list_name,
            percentage,
            (self.exclude_short_lived_task_lists)(),
        );
        let Some(spectator) = self.spectator.as_ref().filter(|_| !excluded) else {
            return self.resolve_from_ring(task_list_name);
        };

        let deadline = Instant::now() + SHARD_DISTRIBUTOR_LOOKUP_TIMEOUT;
        let owner = spectator
            .get_shard_owner(task_list_name.to_owned(), deadline)
            .await
            .map_err(RoutingError::ShardDistributor)?;
        Ok((
            resolved_from_shard_owner(owner)?,
            RoutingPath::ShardDistributor,
        ))
    }

    /// Go `SingleProvider.LookupN` always delegates to Ringpop, even for a
    /// shard-distributor eligible key.
    pub fn resolve_n(
        &self,
        task_list_name: &str,
        count: usize,
    ) -> Result<(Vec<ResolvedHost>, RoutingPath), RoutingError> {
        let hosts = self.ring.owners_n(task_list_name, count);
        if hosts.is_empty() {
            return Err(RoutingError::RingUnavailable(
                "Not enough hosts to serve the request".into(),
            ));
        }
        Ok((
            hosts.into_iter().map(resolved_from_ring_host).collect(),
            RoutingPath::HashRing,
        ))
    }

    /// Go `SingleProvider.AddressToHost` always resolves through the ring.
    pub fn address_to_host(&self, address: &str) -> Result<ResolvedHost, RoutingError> {
        let host = self
            .ring
            .owner_by_address(address)
            .ok_or_else(|| RoutingError::RingUnavailable(format!("host not found: {address}")))?;
        Ok(resolved_from_ring_host(host))
    }

    /// Go's Members and MemberCount are also ring-only views.
    pub fn members(&self) -> Vec<ResolvedHost> {
        self.ring
            .hosts()
            .into_iter()
            .map(resolved_from_ring_host)
            .collect()
    }

    #[must_use]
    pub fn member_count(&self) -> usize {
        self.ring.hosts().len()
    }

    /// Resolve a task-list key through the Go-compatible resolver and obtain
    /// the matching gRPC channel to the selected owner.
    pub async fn remote_channel(
        &self,
        task_list_name: &str,
    ) -> Result<(Option<tonic::transport::Channel>, RoutingPath), RoutingError> {
        let (owner, path) = self.resolve(task_list_name).await?;
        let channel = match path {
            RoutingPath::HashRing => self
                .ring
                .remote_channel_for_owner(&owner.identity, &owner.grpc_address)
                .map_err(|error| RoutingError::Endpoint(error.to_string(), path))?,
            RoutingPath::ShardDistributor => self
                .spectator_remote_channel(&owner.grpc_address)
                .map_err(|error| RoutingError::Endpoint(error.to_string(), path))?,
        };
        Ok((channel, path))
    }

    fn spectator_remote_channel(
        &self,
        grpc_address: &str,
    ) -> Result<Option<tonic::transport::Channel>, RoutingError> {
        if grpc_address.is_empty() {
            return Err(RoutingError::Endpoint(
                "Matching owner has no gRPC address".into(),
                RoutingPath::ShardDistributor,
            ));
        }
        let mut peers = self
            .peer_channels
            .lock()
            .expect("Matching shard peer cache lock");
        if let Some((channel, last_used)) = peers.get_mut(grpc_address) {
            *last_used = Instant::now();
            return Ok(Some(channel.clone()));
        }
        let address = if grpc_address.contains("://") {
            grpc_address.to_owned()
        } else {
            format!("http://{grpc_address}")
        };
        let channel = tonic::transport::Endpoint::from_shared(address)
            .map_err(|error| {
                RoutingError::Endpoint(error.to_string(), RoutingPath::ShardDistributor)
            })?
            .connect_lazy();
        peers.insert(grpc_address.to_owned(), (channel.clone(), Instant::now()));
        Ok(Some(channel))
    }

    fn resolve_from_ring(
        &self,
        task_list_name: &str,
    ) -> Result<(ResolvedHost, RoutingPath), RoutingError> {
        let host = self
            .ring
            .owner(task_list_name)
            .map_err(|error| RoutingError::RingUnavailable(error.message().to_owned()))?;
        Ok((resolved_from_ring_host(host), RoutingPath::HashRing))
    }
}

/// Go `TaskListExcludedFromShardDistributor` and `belowPercentage`.
#[must_use]
pub fn task_list_excluded_from_shard_distributor(
    task_list_name: &str,
    percentage_onboarded: u64,
    exclude_short_lived_task_lists: bool,
) -> bool {
    (exclude_short_lived_task_lists && contains_uuid(task_list_name))
        || cadence_common::membership::fingerprint64(task_list_name.as_bytes()) % 100
            >= percentage_onboarded
}

fn contains_uuid(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.windows(36).any(|candidate| {
        [8, 13, 18, 23]
            .iter()
            .all(|index| candidate[*index] == b'-')
            && candidate
                .iter()
                .enumerate()
                .all(|(index, byte)| [8, 13, 18, 23].contains(&index) || byte.is_ascii_hexdigit())
    })
}

fn resolved_from_ring_host(host: Host) -> ResolvedHost {
    // Read the derived identity before moving the address fields out of Host.
    let identity = host.identity().to_owned();
    let mut named_ports = HashMap::new();
    if let Some(port) = endpoint_port(&host.address) {
        named_ports.insert(PORT_TCHANNEL.to_owned(), port);
    }
    if let Some(port) = endpoint_port(&host.grpc_address) {
        named_ports.insert(PORT_GRPC.to_owned(), port);
    }
    ResolvedHost {
        address: host.address,
        identity,
        named_ports,
        grpc_address: host.grpc_address,
    }
}

fn resolved_from_shard_owner(owner: ShardOwner) -> Result<ResolvedHost, RoutingError> {
    let host_ip = owner
        .metadata
        .get(HOST_IP)
        .filter(|host| !host.is_empty())
        .ok_or(RoutingError::MissingHostIp)?;
    let mut named_ports = HashMap::new();
    for name in [PORT_TCHANNEL, PORT_GRPC] {
        if let Some(port) = owner.metadata.get(name).and_then(|value| parse_port(value)) {
            named_ports.insert(name.to_owned(), port);
        }
    }
    // Go indexes an absent TChannel map entry as zero before net.JoinHostPort.
    let address = join_host_port(
        host_ip,
        named_ports.get(PORT_TCHANNEL).copied().unwrap_or_default(),
    );
    let grpc_address = named_ports
        .get(PORT_GRPC)
        .map(|port| join_host_port(host_ip, *port))
        .unwrap_or_default();
    Ok(ResolvedHost {
        address,
        identity: owner.executor_id,
        named_ports,
        grpc_address,
    })
}

fn parse_port(value: &str) -> Option<u16> {
    // Go strconv.ParseUint(s, 10, 16) accepts decimal digits only: no sign,
    // whitespace, or base-0 underscore syntax.
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    value.parse::<u16>().ok()
}

fn endpoint_port(endpoint: &str) -> Option<u16> {
    let endpoint = endpoint.strip_prefix("http://").unwrap_or(endpoint);
    let endpoint = endpoint.strip_prefix("https://").unwrap_or(endpoint);
    let port = if let Some(rest) = endpoint.strip_prefix('[') {
        rest.split_once("]:")?.1
    } else {
        endpoint.rsplit_once(':')?.1
    };
    parse_port(port)
}

fn join_host_port(host: &str, port: u16) -> String {
    let host = host
        .strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
        .unwrap_or(host);
    if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Mutex,
        atomic::{AtomicI64, AtomicUsize, Ordering},
    };

    struct FakeSpectator {
        owner: Mutex<Result<ShardOwner, String>>,
        calls: AtomicUsize,
        seen: Mutex<Vec<(String, Instant)>>,
    }

    impl FakeSpectator {
        fn new(owner: Result<ShardOwner, String>) -> Self {
            Self {
                owner: Mutex::new(owner),
                calls: AtomicUsize::new(0),
                seen: Mutex::new(Vec::new()),
            }
        }
    }

    impl ShardOwnerLookup for FakeSpectator {
        fn get_shard_owner(
            &self,
            shard_key: String,
            deadline: Instant,
        ) -> BoxFuture<Result<ShardOwner, String>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.seen.lock().unwrap().push((shard_key, deadline));
            let owner = self.owner.lock().unwrap().clone();
            Box::pin(async move { owner })
        }
    }

    fn ring() -> Membership {
        Membership::new(
            "self",
            vec![Host {
                address: "10.0.0.1:7933".into(),
                identity: "ring-owner".into(),
                grpc_address: "10.0.0.1:7833".into(),
            }],
        )
    }

    fn shard_owner(metadata: &[(&str, &str)]) -> ShardOwner {
        ShardOwner {
            executor_id: "sd-owner".into(),
            metadata: metadata
                .iter()
                .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
                .collect(),
        }
    }

    #[test]
    fn go_farmhash_percentage_and_uuid_exclusion_vectors() {
        assert_eq!(
            cadence_common::membership::fingerprint64(b"my-task-list") % 100,
            58
        );
        assert!(!task_list_excluded_from_shard_distributor(
            "my-task-list",
            60,
            true
        ));
        assert!(task_list_excluded_from_shard_distributor(
            "my-task-list",
            58,
            true
        ));
        assert!(task_list_excluded_from_shard_distributor(
            "my-task-list",
            0,
            false
        ));
        assert!(!task_list_excluded_from_shard_distributor(
            "my-task-list",
            100,
            false
        ));
        assert!(task_list_excluded_from_shard_distributor(
            "prefix-550E8400-E29B-41D4-A716-446655440000-suffix",
            100,
            true
        ));
        assert!(!task_list_excluded_from_shard_distributor(
            "prefix-550e8400-e29b-41d4-a716-suffix",
            100,
            true
        ));
        assert!(!task_list_excluded_from_shard_distributor(
            "550e8400-e29b-41d4-a716-446655440000",
            100,
            false
        ));
    }

    #[tokio::test]
    async fn excluded_and_missing_spectator_routes_use_live_ring_snapshot() {
        let ring = ring();
        let spectator = Arc::new(FakeSpectator::new(Err("must not be called".into())));
        let percentage = Arc::new(AtomicI64::new(0));
        let router = MatchingShardRouter::new(
            ring.clone(),
            Some(spectator.clone()),
            Arc::new({
                let percentage = percentage.clone();
                move || percentage.load(Ordering::SeqCst)
            }),
            Arc::new(|| false),
        );
        let (route, path) = router.resolve("queue").await.unwrap();
        assert_eq!(path, RoutingPath::HashRing);
        assert_eq!(route.identity, "ring-owner");
        assert_eq!(route.address, "10.0.0.1:7933");
        assert_eq!(spectator.calls.load(Ordering::SeqCst), 0);

        ring.replace(vec![Host {
            address: "10.0.0.2:7933".into(),
            identity: "replacement-ring-owner".into(),
            grpc_address: "10.0.0.2:7833".into(),
        }]);
        let (updated, _) = router.resolve("queue").await.unwrap();
        assert_eq!(updated.identity, "replacement-ring-owner");

        percentage.store(100, Ordering::SeqCst);
        let router_without_spectator =
            MatchingShardRouter::new(ring, None, Arc::new(|| 100), Arc::new(|| false));
        assert_eq!(
            router_without_spectator.resolve("queue").await.unwrap().1,
            RoutingPath::HashRing
        );
    }

    #[tokio::test]
    async fn eligible_route_uses_spectator_and_parses_named_ports_and_ipv6() {
        let spectator = Arc::new(FakeSpectator::new(Ok(shard_owner(&[
            (HOST_IP, "2001:db8::1"),
            (PORT_TCHANNEL, "7933"),
            (PORT_GRPC, "7833"),
            ("extra", "9"),
        ]))));
        let router = MatchingShardRouter::new(
            ring(),
            Some(spectator.clone()),
            Arc::new(|| 100),
            Arc::new(|| false),
        );
        let (route, path) = router.resolve("queue-name").await.unwrap();
        assert_eq!(path, RoutingPath::ShardDistributor);
        assert_eq!(route.identity, "sd-owner");
        assert_eq!(route.address, "[2001:db8::1]:7933");
        assert_eq!(route.grpc_address, "[2001:db8::1]:7833");
        assert_eq!(route.named_ports.get(PORT_TCHANNEL), Some(&7933));
        assert_eq!(route.named_ports.get(PORT_GRPC), Some(&7833));
        assert!(!route.named_ports.contains_key("extra"));
        let seen = spectator.seen.lock().unwrap();
        assert_eq!(seen[0].0, "queue-name");
        let deadline_remaining = seen[0].1.saturating_duration_since(Instant::now());
        assert!(!deadline_remaining.is_zero());
        assert!(deadline_remaining <= SHARD_DISTRIBUTOR_LOOKUP_TIMEOUT);
    }

    #[tokio::test]
    async fn spectator_errors_and_missing_host_ip_do_not_fall_back() {
        let spectator = Arc::new(FakeSpectator::new(Err("owner unavailable".into())));
        let router = MatchingShardRouter::new(
            ring(),
            Some(spectator.clone()),
            Arc::new(|| 100),
            Arc::new(|| false),
        );
        let channel_error = match router.remote_channel("queue").await {
            Err(error) => error,
            Ok(_) => panic!("spectator error must not fall back to Ringpop"),
        };
        assert_eq!(channel_error.path(), RoutingPath::ShardDistributor);
        let error = router.resolve("queue").await.unwrap_err();
        assert_eq!(
            error,
            RoutingError::ShardDistributor("owner unavailable".into())
        );
        assert_eq!(error.path(), RoutingPath::ShardDistributor);
        *spectator.owner.lock().unwrap() = Ok(shard_owner(&[(PORT_TCHANNEL, "7933")]));
        let error = router.resolve("queue").await.unwrap_err();
        assert_eq!(error, RoutingError::MissingHostIp);
        assert_eq!(error.path(), RoutingPath::ShardDistributor);
        assert_eq!(spectator.calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn empty_ring_returns_typed_hash_ring_error() {
        let router = MatchingShardRouter::new(
            Membership::new("self", Vec::new()),
            None,
            Arc::new(|| 0),
            Arc::new(|| false),
        );
        let error = router.resolve("queue").await.unwrap_err();
        assert!(matches!(error, RoutingError::RingUnavailable(_)));
        assert_eq!(error.path(), RoutingPath::HashRing);
    }

    #[tokio::test]
    async fn lookup_n_and_address_conversion_always_delegate_to_ring() {
        let spectator = Arc::new(FakeSpectator::new(Err("must not be called".into())));
        let router = MatchingShardRouter::new(
            ring(),
            Some(spectator.clone()),
            Arc::new(|| 100),
            Arc::new(|| false),
        );
        let (owners, path) = router.resolve_n("queue", 1).unwrap();
        assert_eq!(path, RoutingPath::HashRing);
        assert_eq!(owners.len(), 1);
        assert_eq!(owners[0].identity, "ring-owner");
        assert_eq!(router.member_count(), 1);
        assert_eq!(router.members().len(), 1);
        assert_eq!(
            router.address_to_host("10.0.0.1:7933").unwrap().identity,
            "ring-owner"
        );
        assert_eq!(
            router.resolve_n("queue", 0).unwrap_err().path(),
            RoutingPath::HashRing
        );
        assert_eq!(spectator.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn invalid_ports_are_omitted_and_tchannel_address_defaults_to_zero() {
        let spectator = Arc::new(FakeSpectator::new(Ok(shard_owner(&[
            (HOST_IP, "127.0.0.1"),
            (PORT_TCHANNEL, "65536"),
            (PORT_GRPC, " 7833"),
        ]))));
        let router = MatchingShardRouter::new(
            ring(),
            Some(spectator),
            Arc::new(|| 100),
            Arc::new(|| false),
        );
        let (route, _) = router.resolve("queue").await.unwrap();
        assert_eq!(route.address, "127.0.0.1:0");
        assert!(route.named_ports.is_empty());
        assert!(route.grpc_address.is_empty());

        assert_eq!(parse_port("07833"), Some(7833));
        assert_eq!(parse_port("+7833"), None);
        assert_eq!(parse_port("-1"), None);
        assert_eq!(parse_port(" 7833"), None);
        assert_eq!(parse_port("7_833"), None);
        assert_eq!(parse_port("65536"), None);
    }

    #[tokio::test]
    async fn policy_is_read_per_lookup_and_uuid_exclusion_precedes_spectator() {
        let spectator = Arc::new(FakeSpectator::new(Ok(shard_owner(&[(
            HOST_IP,
            "127.0.0.1",
        )]))));
        let percent = Arc::new(AtomicI64::new(100));
        let exclude_uuid = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let router = MatchingShardRouter::new(
            ring(),
            Some(spectator.clone()),
            Arc::new({
                let percent = percent.clone();
                move || percent.load(Ordering::SeqCst)
            }),
            Arc::new({
                let exclude_uuid = exclude_uuid.clone();
                move || exclude_uuid.load(Ordering::SeqCst)
            }),
        );
        let uuid_name = "list-550e8400-e29b-41d4-a716-446655440000";
        assert_eq!(
            router.resolve(uuid_name).await.unwrap().1,
            RoutingPath::HashRing
        );
        assert_eq!(spectator.calls.load(Ordering::SeqCst), 0);
        exclude_uuid.store(false, Ordering::SeqCst);
        assert_eq!(
            router.resolve(uuid_name).await.unwrap().1,
            RoutingPath::ShardDistributor
        );
        percent.store(0, Ordering::SeqCst);
        assert_eq!(
            router.resolve("regular-list").await.unwrap().1,
            RoutingPath::HashRing
        );
        percent.store(100, Ordering::SeqCst);
        assert_eq!(
            router.resolve("regular-list").await.unwrap().1,
            RoutingPath::ShardDistributor
        );
        // Go converts the dynamic-property int to uint64 before the modulo
        // comparison; Rust's signed-to-unsigned cast has the same wraparound.
        percent.store(-1, Ordering::SeqCst);
        assert_eq!(
            router.resolve("negative-percentage").await.unwrap().1,
            RoutingPath::ShardDistributor
        );
        assert_eq!(spectator.calls.load(Ordering::SeqCst), 4);
    }
}
