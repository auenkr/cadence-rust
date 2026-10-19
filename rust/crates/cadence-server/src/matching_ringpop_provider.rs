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
    ringpop_state_transitions::StateTimeouts,
    ringpop_wire::{Member, ALIVE, FAULTY, LEAVE, SUSPECT},
};
use std::{
    collections::{BTreeMap, HashMap},
    net::{IpAddr, SocketAddr},
    sync::{Arc, RwLock},
};
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
    service: String,
    discovery: DiscoveryProvider,
    max_join_duration_nanos: i64,
    ports: BTreeMap<String, u16>,
    lifecycle: Mutex<Lifecycle>,
    snapshot: Arc<RwLock<MatchingPeerSnapshot>>,
    events: broadcast::Sender<PeerChange>,
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
            address,
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
            service: service.into(),
            discovery,
            max_join_duration_nanos: config.max_join_duration_nanos,
            ports,
            lifecycle: Mutex::new(Lifecycle::Initialized),
            snapshot: Arc::new(RwLock::new(MatchingPeerSnapshot {
                generation: 0,
                members: Arc::new(Vec::new()),
            })),
            events,
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
            .filter_map(|member| {
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
                Some(PeerMember {
                    address: member.address,
                    identity,
                    status,
                    labels,
                    named_ports,
                })
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
            &self.node.membership_snapshot().0[0].address,
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
        *lifecycle = Lifecycle::Started { stop, monitor };
        Ok(())
    }

    async fn members(&self, service: &str) -> Result<Vec<PeerMember>, ProviderError> {
        if service != self.service {
            return Ok(Vec::new());
        }
        Ok(self
            .snapshot()
            .members
            .iter()
            .filter(|member| {
                member.labels.get("serviceName").map(String::as_str) == Some(service)
                    && matches!(member.status, PeerStatus::Alive | PeerStatus::Suspect)
            })
            .cloned()
            .collect())
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
            Lifecycle::Started { stop, monitor } => {
                // Remove the provider's Ringpop listener before destroying the
                // node, matching Go Provider.Stop's RemoveListener → Destroy.
                let _ = stop.send(true);
                let node = self.node.clone();
                let cleanup = tokio::spawn(async move {
                    let _ = monitor.await;
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
    let mut best = None;
    for interface in if_addrs::get_if_addrs()? {
        let ip = interface.ip();
        let mut score = if ip.is_ipv4() { 300 } else { 0 };
        if !interface.is_loopback() && !ip.is_loopback() {
            score += 200;
        }
        if best.as_ref().is_none_or(|(previous, _)| score > *previous) {
            best = Some((score, ip));
        }
    }
    best.map(|(_, ip)| ip)
        .ok_or_else(|| anyhow::anyhow!("no addresses to advertise for Ringpop"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use cadence_common::ringpop_wire::Member;

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
            ..Default::default()
        };
        let node = RingpopNode::new("cadence", member, false);
        let members = MatchingRingpopProvider::current_members(&node);
        assert_eq!(members.len(), 1);
        assert_eq!(members[0].labels["serviceName"], "cadence-history");
        assert_eq!(members[0].named_ports.len(), 2);
        assert_eq!(members[0].named_ports["grpc"], 7835);
        assert_eq!(members[0].named_ports["tchannel"], 7935);
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
        let mut changes = node.subscribe_membership_changes();
        node.set_local_labels(&BTreeMap::from([(
            "serviceName".into(),
            "cadence-matching".into(),
        )]));
        assert!(changes.has_changed().unwrap());
    }
}
