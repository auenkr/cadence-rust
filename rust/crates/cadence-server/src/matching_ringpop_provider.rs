//! Concrete Matching peer provider over the local Ringpop node.
//!
//! The caller binds the shared Matching TChannel listener first and passes
//! its actual local address here. `handler()` is installed on that same
//! listener; `start()` then performs configured JOIN/bootstrap, starts SWIM,
//! marks Ringpop ready, installs the provider's change listener, and only then
//! publishes the Go-compatible service and port labels.

use crate::{
    config::RingpopConfig,
    matching_peer_provider::{
        MatchingPeerProvider, PeerChange, PeerMember, PeerStatus, ProviderError,
    },
    ringpop_discovery::{DiscoverProvider, Provider as DiscoveryProvider},
    ringpop_provider::{join_cluster, JoinGroupState, RingpopNode, SystemJoinClock},
};
use cadence_common::{
    ringpop_bootstrap_selection::{BootstrapSelection, Options},
    ringpop_wire::{Member, ALIVE, FAULTY, LEAVE, SUSPECT, TOMBSTONE},
};
use rand::seq::SliceRandom;
use std::{
    collections::{BTreeMap, HashMap},
    net::{IpAddr, SocketAddr},
    sync::{Arc, RwLock},
};
#[cfg(test)]
use tokio::sync::Notify;
use tokio::sync::{broadcast, watch, Mutex};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MatchingPeerSnapshot {
    pub generation: u64,
    pub members: Arc<Vec<PeerMember>>,
}

enum Lifecycle {
    Initialized,
    Started {
        stop: watch::Sender<bool>,
        monitor: tokio::task::JoinHandle<()>,
        healer: tokio::task::JoinHandle<()>,
    },
    Stopping {
        cleanup: tokio::task::JoinHandle<()>,
    },
    Stopped,
}

/// Ringpop-backed Matching provider. The immutable snapshot lets callers that
/// perform ring selection and named-port resolution retain one membership
/// generation for both operations.
pub struct MatchingRingpopProvider {
    node: RingpopNode,
    local_address: String,
    service: String,
    discovery: Arc<DiscoveryProvider>,
    max_join_duration_nanos: i64,
    ports: BTreeMap<String, u16>,
    lifecycle: Mutex<Lifecycle>,
    snapshot: Arc<RwLock<MatchingPeerSnapshot>>,
    events: broadcast::Sender<PeerChange>,
    #[cfg(test)]
    healer_test_trigger: Arc<Notify>,
}

impl MatchingRingpopProvider {
    /// Build a provider after the shared TChannel listener is bound. `service`
    /// is Go's full role label, e.g. `cadence-matching`; `listener_addr` must
    /// be that listener's actual `local_addr()`, not its wildcard bind address.
    pub fn new(
        mut config: RingpopConfig,
        service: impl Into<String>,
        listener_addr: SocketAddr,
        grpc_port: u16,
    ) -> Result<Self, ProviderError> {
        if listener_addr.port() == 0 {
            return Err(ProviderError(
                "Ringpop requires the bound TChannel listener port".into(),
            ));
        }
        config
            .validate()
            .map_err(|error| ProviderError(format!("validating Ringpop config: {error}")))?;
        let discovery = DiscoveryProvider::from_config(&config)
            .map_err(|error| ProviderError(format!("Ringpop discovery provider: {error}")))?;
        let advertised_ip = if config.broadcast_address.is_empty() {
            if listener_addr.ip().is_unspecified() {
                select_advertised_ip().map_err(|error| ProviderError(error.to_string()))?
            } else {
                listener_addr.ip()
            }
        } else {
            config
                .broadcast_address
                .parse::<IpAddr>()
                .map_err(|error| {
                    ProviderError(format!(
                        "invalid Ringpop broadcastAddress {:?}: {error}",
                        config.broadcast_address
                    ))
                })?
        };
        let address = SocketAddr::new(advertised_ip, listener_addr.port()).to_string();
        let local = Member {
            address: address.clone(),
            status: ALIVE.to_owned(),
            incarnation: wall_clock_millis(),
            labels: None,
        };
        let node = RingpopNode::new(config.name.clone(), local, false);
        let mut ports = BTreeMap::new();
        ports.insert("tchannel".to_owned(), listener_addr.port());
        ports.insert("grpc".to_owned(), grpc_port);
        let (events, _) = broadcast::channel(32);
        Ok(Self {
            node,
            local_address: address,
            service: service.into(),
            discovery: Arc::new(discovery),
            max_join_duration_nanos: config.max_join_duration_nanos,
            ports,
            lifecycle: Mutex::new(Lifecycle::Initialized),
            snapshot: Arc::new(RwLock::new(MatchingPeerSnapshot {
                generation: 0,
                members: Arc::new(Vec::new()),
            })),
            events,
            #[cfg(test)]
            healer_test_trigger: Arc::new(Notify::new()),
        })
    }

    pub fn node(&self) -> &RingpopNode {
        &self.node
    }

    /// Install this handler on the same TChannel listener used for Matching.
    pub fn handler(&self) -> crate::ringpop_tchannel::RingpopHandler {
        self.node.handler()
    }

    pub fn snapshot(&self) -> MatchingPeerSnapshot {
        self.snapshot
            .read()
            .expect("Matching Ringpop snapshot lock")
            .clone()
    }

    fn current_members(node: &RingpopNode) -> Vec<PeerMember> {
        let (members, _) = node.membership_snapshot();
        members
            .into_iter()
            .map(|member| {
                let status = match member.status.as_str() {
                    ALIVE => PeerStatus::Alive,
                    SUSPECT => PeerStatus::Suspect,
                    FAULTY => PeerStatus::Faulty,
                    LEAVE => PeerStatus::Left,
                    _ => PeerStatus::Unknown,
                };
                let labels: HashMap<_, _> = member.labels.unwrap_or_default().into_iter().collect();
                let identity = labels
                    .get("__identity")
                    .cloned()
                    .unwrap_or_else(|| member.address.clone());
                let named_ports = labels
                    .iter()
                    .filter_map(|(name, value)| {
                        matches!(name.as_str(), "tchannel" | "grpc")
                            .then(|| parse_port_label(value).map(|port| (name.clone(), port)))
                            .flatten()
                    })
                    .collect();
                PeerMember {
                    address: member.address,
                    identity,
                    status,
                    labels,
                    named_ports,
                }
            })
            .collect()
    }

    fn publish_snapshot(
        snapshot: &RwLock<MatchingPeerSnapshot>,
        events: &broadcast::Sender<PeerChange>,
        members: Vec<PeerMember>,
    ) {
        let mut current = snapshot.write().expect("Matching Ringpop snapshot lock");
        if current.members.as_ref() == &members {
            return;
        }
        current.generation = current.generation.wrapping_add(1);
        current.members = Arc::new(members);
        let _ = events.send(PeerChange);
    }

    fn service_members(members: &[PeerMember], service: &str) -> Vec<PeerMember> {
        members
            .iter()
            .filter(|member| {
                member.labels.get("serviceName").map(String::as_str) == Some(service)
                    && matches!(member.status, PeerStatus::Alive | PeerStatus::Suspect)
            })
            .cloned()
            .collect()
    }
}

impl MatchingPeerProvider for MatchingRingpopProvider {
    async fn start(&self) -> Result<(), ProviderError> {
        let mut lifecycle = self.lifecycle.lock().await;
        match &*lifecycle {
            Lifecycle::Started { .. } => return Ok(()),
            Lifecycle::Stopping { .. } => {
                return Err(ProviderError("Ringpop provider is stopping".into()));
            }
            Lifecycle::Stopped => {
                return Err(ProviderError(
                    "Ringpop provider cannot start after stop".into(),
                ));
            }
            Lifecycle::Initialized => {}
        }

        let seeds = self
            .discovery
            .hosts()
            .map_err(|error| ProviderError(format!("discovering Ringpop seeds: {error}")))?;
        let selection = BootstrapSelection::new(
            &self.local_address,
            &seeds,
            Options::from_signed(0, self.max_join_duration_nanos, 0, 0),
        );
        let mut sender = JoinGroupState::new(selection);
        let mut clock = SystemJoinClock::default();
        join_cluster(&self.node, &mut sender, &mut clock)
            .await
            .map_err(|error| {
                ProviderError(format!(
                    "bootstrap Ringpop: {}; joined={:?}",
                    error.0, error.1
                ))
            })?;

        // Go Bootstrap starts gossip, then marks the node ready. The provider
        // listener and role/port labels are installed only after this point.
        self.node
            .start_swim()
            .map_err(|error| ProviderError(format!("starting Ringpop SWIM: {error}")))?;
        self.node.set_ready(true);

        let (stop, mut stopped) = watch::channel(false);
        let mut healer_stopped = stop.subscribe();
        let mut membership_changes = self.node.subscribe_membership_changes();
        let node = self.node.clone();
        let snapshot = Arc::clone(&self.snapshot);
        let events = self.events.clone();
        let monitor = tokio::spawn(async move {
            loop {
                tokio::select! {
                    changed = stopped.changed() => {
                        if changed.is_err() || *stopped.borrow() { break; }
                    }
                    changed = membership_changes.changed() => {
                        if changed.is_err() { break; }
                        MatchingRingpopProvider::publish_snapshot(
                            &snapshot,
                            &events,
                            MatchingRingpopProvider::current_members(&node),
                        );
                    }
                }
            }
        });

        let healer_node = self.node.clone();
        let discovery = Arc::clone(&self.discovery);
        #[cfg(test)]
        let healer_test_trigger = Arc::clone(&self.healer_test_trigger);
        let healer = tokio::spawn(async move {
            let period = std::time::Duration::from_secs(30);
            let mut previous_host_list_size = 0usize;
            loop {
                #[cfg(test)]
                let requested_tick = healer_test_trigger.notified();
                #[cfg(not(test))]
                let requested_tick = std::future::pending::<()>();
                tokio::select! {
                    _ = tokio::time::sleep(period) => {},
                    _ = requested_tick => {},
                    changed = healer_stopped.changed() => {
                        if changed.is_err() || *healer_stopped.borrow() { break; }
                    }
                }
                let reachable = healer_node
                    .membership_snapshot()
                    .0
                    .iter()
                    .filter(|member| member.status == ALIVE || member.status == SUSPECT)
                    .count();
                let probability = healer_probability(&mut previous_host_list_size, reachable, 3.0);
                if rand::random::<f64>() >= probability {
                    continue;
                }
                let hosts = match discovery.hosts() {
                    Ok(hosts) => hosts,
                    Err(_) => continue,
                };
                previous_host_list_size = hosts.len();
                let members = healer_node.membership_snapshot().0;
                let mut candidates: Vec<_> = hosts
                    .into_iter()
                    .filter(|host| {
                        members
                            .iter()
                            .find(|member| member.address == *host)
                            .is_none_or(|member| {
                                matches!(member.status.as_str(), FAULTY | LEAVE | TOMBSTONE)
                            })
                    })
                    .collect();
                candidates.shuffle(&mut rand::thread_rng());
                let mut failures = 0;
                while !candidates.is_empty() && failures < 10 {
                    let target = remove_address(&mut candidates, 0);
                    match healer_node.attempt_heal(&target).await {
                        Ok(known) => {
                            for address in known {
                                remove_all_addresses(&mut candidates, &address);
                            }
                        }
                        Err(_) => failures += 1,
                    }
                }
            }
        });

        let mut labels = BTreeMap::new();
        labels.insert("serviceName".to_owned(), self.service.clone());
        for (name, port) in &self.ports {
            labels.insert(name.clone(), port.to_string());
        }
        self.node.set_local_labels(&labels);
        Self::publish_snapshot(
            &self.snapshot,
            &self.events,
            Self::current_members(&self.node),
        );
        *lifecycle = Lifecycle::Started {
            stop,
            monitor,
            healer,
        };
        Ok(())
    }

    async fn members(&self, service: &str) -> Result<Vec<PeerMember>, ProviderError> {
        Ok(Self::service_members(&self.snapshot().members, service))
    }

    async fn subscribe(
        &self,
        _service: &str,
    ) -> Result<broadcast::Receiver<PeerChange>, ProviderError> {
        if matches!(*self.lifecycle.lock().await, Lifecycle::Initialized) {
            return Err(ProviderError("Ringpop provider has not started".into()));
        }
        Ok(self.events.subscribe())
    }

    async fn self_evict(&self) -> Result<(), ProviderError> {
        self.node
            .self_evict()
            .await
            .map_err(|error| ProviderError(error.to_owned()))
    }

    async fn stop(&self) -> Result<(), ProviderError> {
        let mut lifecycle = self.lifecycle.lock().await;
        let current = std::mem::replace(&mut *lifecycle, Lifecycle::Stopped);
        match current {
            Lifecycle::Started {
                stop,
                monitor,
                healer,
            } => {
                // Remove the provider's Ringpop listener before destroying the
                // node, matching Go Provider.Stop's RemoveListener → Destroy.
                let _ = stop.send(true);
                let node = self.node.clone();
                let cleanup = tokio::spawn(async move {
                    let _ = monitor.await;
                    let _ = healer.await;
                    node.destroy_and_wait().await;
                });
                *lifecycle = Lifecycle::Stopping { cleanup };
            }
            Lifecycle::Stopping { cleanup } => {
                *lifecycle = Lifecycle::Stopping { cleanup };
            }
            Lifecycle::Initialized => {
                // Startup can fail after JOIN or partial runtime initialization.
                // Always give the node an owned cleanup task, even before the
                // membership monitor reached Started.
                let node = self.node.clone();
                let cleanup = tokio::spawn(async move { node.destroy_and_wait().await });
                *lifecycle = Lifecycle::Stopping { cleanup };
            }
            Lifecycle::Stopped => return Ok(()),
        }
        if let Lifecycle::Stopping { cleanup } = &mut *lifecycle {
            cleanup
                .await
                .map_err(|error| ProviderError(format!("joining Ringpop cleanup: {error}")))?;
        }
        *lifecycle = Lifecycle::Stopped;
        Ok(())
    }
}

fn remove_address(addresses: &mut Vec<String>, index: usize) -> String {
    let address = addresses[index].clone();
    remove_all_addresses(addresses, &address);
    address
}

fn healer_probability(previous_host_list_size: &mut usize, reachable: usize, base: f64) -> f64 {
    *previous_host_list_size = (*previous_host_list_size).max(reachable).max(1);
    base / *previous_host_list_size as f64
}

fn remove_all_addresses(addresses: &mut Vec<String>, address: &str) {
    let mut index = 0;
    while index < addresses.len() {
        if addresses[index] == address {
            addresses.swap_remove(index);
        } else {
            index += 1;
        }
    }
}

fn wall_clock_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}

fn parse_port_label(label: &str) -> Option<u16> {
    if label.is_empty() || !label.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    label.parse().ok()
}

fn select_advertised_ip() -> anyhow::Result<IpAddr> {
    use std::ptr;

    let mut first = ptr::null_mut();
    let result = unsafe { libc::getifaddrs(&mut first) };
    if result != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    struct IfAddrsGuard(*mut libc::ifaddrs);
    impl Drop for IfAddrsGuard {
        fn drop(&mut self) {
            unsafe { libc::freeifaddrs(self.0) };
        }
    }
    let _guard = IfAddrsGuard(first);
    let mut best: Option<(i32, IpAddr)> = None;
    let mut entry = first;
    while !entry.is_null() {
        let current = unsafe { &*entry };
        if !current.ifa_addr.is_null() {
            let ip = match unsafe { (*current.ifa_addr).sa_family as i32 } {
                libc::AF_INET => {
                    let address = unsafe { &*(current.ifa_addr.cast::<libc::sockaddr_in>()) };
                    Some(IpAddr::V4(std::net::Ipv4Addr::from(u32::from_be(
                        address.sin_addr.s_addr,
                    ))))
                }
                libc::AF_INET6 => {
                    let address = unsafe { &*(current.ifa_addr.cast::<libc::sockaddr_in6>()) };
                    Some(IpAddr::V6(std::net::Ipv6Addr::from(
                        address.sin6_addr.s6_addr,
                    )))
                }
                _ => None,
            };
            if let Some(ip) = ip {
                let flags = current.ifa_flags as i32;
                let score = listen_ip_score(
                    ip.is_ipv4(),
                    ip.is_loopback(),
                    flags & libc::IFF_LOOPBACK != 0,
                    flags & libc::IFF_UP != 0,
                );
                if best.as_ref().is_none_or(|(previous, _)| score > *previous) {
                    best = Some((score, ip));
                }
            }
        }
        entry = current.ifa_next;
    }
    best.map(|(_, ip)| ip)
        .ok_or_else(|| anyhow::anyhow!("no addresses to advertise for Ringpop"))
}

fn listen_ip_score(is_ipv4: bool, ip_loopback: bool, interface_loopback: bool, up: bool) -> i32 {
    let mut score = if is_ipv4 { 300 } else { 0 };
    if !interface_loopback && !ip_loopback {
        score += 100;
        if up {
            score += 100;
        }
    }
    score
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::BootstrapMode;
    use cadence_common::ringpop_wire::Member;
    use tokio::net::TcpListener;

    #[test]
    fn healer_probability_keeps_go_previous_host_floor_and_includes_local_reachability() {
        let mut previous = 0;
        assert_eq!(healer_probability(&mut previous, 1, 3.0), 3.0);
        assert_eq!(previous, 1);
        assert_eq!(healer_probability(&mut previous, 4, 3.0), 0.75);
        assert_eq!(previous, 4);
        // CountReachableMembers can shrink, but Go retains the earlier
        // discovery/reachability divisor until Hosts() reports a larger list.
        assert_eq!(healer_probability(&mut previous, 1, 3.0), 0.75);
        assert_eq!(previous, 4);
    }

    #[test]
    fn go_del_swaps_and_removes_all_duplicate_addresses() {
        let mut addresses = vec!["a".into(), "b".into(), "a".into(), "c".into()];
        assert_eq!(remove_address(&mut addresses, 0), "a");
        assert_eq!(addresses.len(), 2);
        assert!(!addresses.iter().any(|address| address == "a"));
    }

    #[test]
    fn go_healer_expected_trace_covers_probability_candidate_and_retry_edges() {
        let oracle: serde_json::Value = serde_json::from_str(include_str!(
            "../tests/fixtures/ringpop_healer_go_expected.json"
        ))
        .unwrap();
        assert_eq!(oracle["localFaultyWins"]["forRemote"][0]["status"], SUSPECT);
        assert_eq!(oracle["localFaultyWins"]["forRemote"][0]["source"], "");
        assert_eq!(
            oracle["localFaultyWins"]["forRemote"][0]["sourceIncarnation"],
            0
        );
        assert_eq!(oracle["remoteFaultyWins"]["forLocal"][0]["status"], SUSPECT);
        assert_eq!(
            oracle["remoteFaultyWins"]["forLocal"][0]["sourceIncarnation"],
            0
        );
        assert_eq!(
            oracle["noConflict"]["forLocal"].as_array().unwrap().len(),
            0
        );
        assert_eq!(oracle["failureCap"], 10);
        assert_eq!(oracle["localHostFaultyCandidate"], 1);
        assert_eq!(oracle["duplicateTargetAttempts"], 2);
        assert_eq!(oracle["delDuplicateRemaining"][0], "other");
        assert_eq!(oracle["failedDiscoveryPreviousHostCount"], 7);
        assert_eq!(oracle["unknownHost"]["attempts"], 1);
        assert_eq!(oracle["unknownHost"]["successfulTargets"], 0);
        assert_eq!(oracle["maxReverseFullSyncJobs"], 5);
        assert_eq!(oracle["reverseFullSyncJobsStarted"], 5);
        assert_eq!(oracle["partitionHealPeriodNanos"], 30_000_000_000i64);
        assert_eq!(oracle["stopWaitsForActiveHeal"], true);
        assert_eq!(oracle["noPendingChangesFullSync"]["fullSync"], true);
        assert_eq!(oracle["noPendingChangesFullSync"]["membersReturned"], 1);
        assert_eq!(oracle["probabilityLocalReachable"].as_f64(), Some(3.0));
        assert_eq!(oracle["probabilityDoesNotShrink"].as_f64(), Some(0.375));
        assert_eq!(oracle["twoPartitionRepair"]["membersA"], 2);
        assert_eq!(oracle["twoPartitionRepair"]["membersB"], 2);
        assert_eq!(oracle["twoPartitionRepair"]["allAlive"], true);
    }

    #[tokio::test]
    async fn stop_waits_for_active_healer_join_response() {
        use crate::{
            grpc_matching::Matching, handlers::AppState, ringpop_tchannel::RingpopHandler,
        };
        use std::{future::Future, task::Poll};
        use tokio::{
            net::TcpListener,
            sync::Notify,
            time::{timeout, Duration},
        };

        let discovery_file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(discovery_file.path(), b"[]").unwrap();

        let provider_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let provider_address = provider_listener.local_addr().unwrap();
        let provider = Arc::new(
            MatchingRingpopProvider::new(
                RingpopConfig {
                    name: "healer-stop-test".into(),
                    bootstrap_mode: BootstrapMode::File,
                    bootstrap_file: discovery_file.path().display().to_string(),
                    max_join_duration_nanos: 1_000_000_000,
                    ..Default::default()
                },
                "cadence-matching",
                provider_address,
                7835,
            )
            .unwrap(),
        );
        let provider_server = tokio::spawn(crate::tchannel::serve_matching_with_ringpop_listener(
            provider_listener,
            Matching::new(AppState::default()),
            provider.handler(),
        ));
        provider.start().await.unwrap();

        let peer_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let peer_address = peer_listener.local_addr().unwrap();
        let peer = RingpopNode::new(
            "healer-stop-test",
            Member {
                address: peer_address.to_string(),
                status: ALIVE.into(),
                incarnation: 2,
                labels: None,
            },
            false,
        );
        peer.set_ready(true);

        // The actual TChannel listener decodes the JOIN before this gate holds
        // its response. The healer therefore owns a real in-flight RPC while
        // shutdown begins.
        let join_received = Arc::new(Notify::new());
        let release_join = Arc::new(Notify::new());
        let peer_handler = peer.handler();
        let gated_handler = RingpopHandler::new({
            let join_received = Arc::clone(&join_received);
            let release_join = Arc::clone(&release_join);
            move |method, headers, body| {
                let peer_handler = peer_handler.clone();
                let join_received = Arc::clone(&join_received);
                let release_join = Arc::clone(&release_join);
                async move {
                    if method == "/protocol/join" {
                        join_received.notify_one();
                        release_join.notified().await;
                    }
                    peer_handler.call(method, headers, body).await
                }
            }
        });
        let peer_server = tokio::spawn(crate::tchannel::serve_matching_with_ringpop_listener(
            peer_listener,
            Matching::new(AppState::default()),
            gated_handler,
        ));

        // Startup consumed the empty discovery list and bootstrapped locally.
        // The next Hosts read returns the peer as a heal candidate.
        std::fs::write(
            discovery_file.path(),
            serde_json::to_vec(&[peer_address.to_string()]).unwrap(),
        )
        .unwrap();
        provider.healer_test_trigger.notify_one();
        timeout(Duration::from_secs(2), join_received.notified())
            .await
            .expect("healer did not issue JOIN to the gated peer");

        // Poll stop once: it sends the shutdown signal and starts owned cleanup,
        // then must remain pending on the active healer task.
        let mut stop = Box::pin(provider.stop());
        let waiting_for_healer = std::future::poll_fn(|cx| match stop.as_mut().poll(cx) {
            Poll::Pending => Poll::Ready(true),
            Poll::Ready(_) => Poll::Ready(false),
        })
        .await;
        assert!(
            waiting_for_healer,
            "provider stop completed while the healer JOIN response was blocked"
        );

        release_join.notify_one();
        timeout(Duration::from_secs(3), stop)
            .await
            .expect("provider cleanup did not finish after releasing JOIN")
            .expect("provider cleanup failed");
        peer.destroy_and_wait().await;
        provider_server.abort();
        peer_server.abort();
        let _ = provider_server.await;
        let _ = peer_server.await;
    }

    #[test]
    fn provider_snapshot_retains_other_roles_and_only_known_ports() {
        let member = Member {
            address: "127.0.0.1:7935".into(),
            status: ALIVE.into(),
            incarnation: 1,
            labels: Some(BTreeMap::from([
                ("serviceName".into(), "cadence-history".into()),
                ("grpc".into(), "7835".into()),
                ("tchannel".into(), "7935".into()),
                ("worker".into(), "9000".into()),
            ])),
        };
        let node = RingpopNode::new("cadence", member, false);
        let members = MatchingRingpopProvider::current_members(&node);
        assert_eq!(members.len(), 1);
        assert_eq!(members[0].labels["serviceName"], "cadence-history");
        assert_eq!(members[0].named_ports.len(), 2);
        assert_eq!(members[0].named_ports["grpc"], 7835);
        assert_eq!(members[0].named_ports["tchannel"], 7935);
        assert_eq!(
            MatchingRingpopProvider::service_members(&members, "cadence-history").len(),
            1
        );
        assert!(MatchingRingpopProvider::service_members(&members, "cadence-matching").is_empty());
    }

    #[test]
    fn local_label_update_publishes_an_accepted_membership_change() {
        let member = Member {
            address: "127.0.0.1:7935".into(),
            status: ALIVE.into(),
            incarnation: 1,
            ..Default::default()
        };
        let node = RingpopNode::new("cadence", member, false);
        let changes = node.subscribe_membership_changes();
        node.set_local_labels(&BTreeMap::from([(
            "serviceName".into(),
            "cadence-matching".into(),
        )]));
        assert!(changes.has_changed().unwrap());
    }

    #[test]
    fn advertised_ip_score_matches_go_listen_ip_up_and_loopback_rules() {
        assert_eq!(listen_ip_score(true, false, false, true), 500);
        assert_eq!(listen_ip_score(true, false, false, false), 400);
        assert_eq!(listen_ip_score(false, false, false, true), 200);
        assert_eq!(listen_ip_score(true, true, true, true), 300);
        assert_eq!(listen_ip_score(true, false, true, true), 300);
    }

    #[tokio::test]
    async fn two_shared_tchannel_providers_discover_roles_and_self_evict() {
        let listener_a = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let listener_b = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr_a = listener_a.local_addr().unwrap();
        let addr_b = listener_b.local_addr().unwrap();
        let provider_a = Arc::new(
            MatchingRingpopProvider::new(
                RingpopConfig {
                    name: "cadence".into(),
                    bootstrap_mode: BootstrapMode::Hosts,
                    bootstrap_hosts: vec![addr_b.to_string()],
                    max_join_duration_nanos: 1_000_000_000,
                    ..Default::default()
                },
                "cadence-matching",
                addr_a,
                7835,
            )
            .unwrap(),
        );
        let provider_b = Arc::new(
            MatchingRingpopProvider::new(
                RingpopConfig {
                    name: "cadence".into(),
                    bootstrap_mode: BootstrapMode::Hosts,
                    bootstrap_hosts: vec![addr_a.to_string()],
                    max_join_duration_nanos: 1_000_000_000,
                    ..Default::default()
                },
                "cadence-history",
                addr_b,
                7836,
            )
            .unwrap(),
        );
        let server_a = tokio::spawn(crate::tchannel::serve_matching_with_ringpop_listener(
            listener_a,
            crate::grpc_matching::Matching::new(crate::handlers::AppState::default()),
            provider_a.handler(),
        ));
        let server_b = tokio::spawn(crate::tchannel::serve_matching_with_ringpop_listener(
            listener_b,
            crate::grpc_matching::Matching::new(crate::handlers::AppState::default()),
            provider_b.handler(),
        ));

        let (started_a, started_b) = tokio::join!(provider_a.start(), provider_b.start());
        assert!(
            started_a.is_ok(),
            "provider A bootstrap failed: {started_a:?}"
        );
        assert!(
            started_b.is_ok(),
            "provider B bootstrap failed: {started_b:?}"
        );
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if provider_a
                    .members("cadence-history")
                    .await
                    .is_ok_and(|members| members.len() == 1)
                {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("provider A did not observe History role labels");
        let matching_members = provider_a.members("cadence-matching").await.unwrap();
        assert_eq!(matching_members.len(), 1);
        assert_eq!(matching_members[0].address, addr_a.to_string());

        provider_a.self_evict().await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if provider_b
                    .members("cadence-matching")
                    .await
                    .is_ok_and(|members| members.is_empty())
                {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("provider B retained the self-evicted Matching peer");

        provider_a.stop().await.unwrap();
        server_a.abort();
        let _ = server_a.await;
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        let restarted_listener = TcpListener::bind(addr_a).await.unwrap();
        let restarted_provider = Arc::new(
            MatchingRingpopProvider::new(
                RingpopConfig {
                    name: "cadence".into(),
                    bootstrap_mode: BootstrapMode::Hosts,
                    bootstrap_hosts: vec![addr_b.to_string()],
                    max_join_duration_nanos: 1_000_000_000,
                    ..Default::default()
                },
                "cadence-matching",
                addr_a,
                7835,
            )
            .unwrap(),
        );
        let restarted_server = tokio::spawn(crate::tchannel::serve_matching_with_ringpop_listener(
            restarted_listener,
            crate::grpc_matching::Matching::new(crate::handlers::AppState::default()),
            restarted_provider.handler(),
        ));
        restarted_provider.start().await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if provider_b
                    .members("cadence-matching")
                    .await
                    .is_ok_and(|members| members.len() == 1)
                {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("restarted provider did not restore Matching membership");

        restarted_provider.stop().await.unwrap();
        provider_b.stop().await.unwrap();
        restarted_server.abort();
        server_b.abort();
    }
}
