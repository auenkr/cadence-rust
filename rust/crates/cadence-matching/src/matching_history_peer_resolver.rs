//! Matching's outbound History peer placement, matching Go's
//! `client/history/peer_resolver.go` contract.
//!
//! The owner supplies a refreshed History membership snapshot. Resolving on
//! demand keeps membership refreshes and redirect lookups visible without
//! caching stale peers here.
use cadence_common::membership::{Host, Ring};
use std::sync::Arc;
use tonic::Status;

pub const HISTORY_SERVICE: &str = "history";

/// Transport name from Go membership's `PortGRPC` / `PortTchannel` selection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NamedPort {
    Grpc,
    Tchannel,
}

/// Supply the current History membership snapshot. Runtime integration should
/// adapt the shared discovery provider here; production callers must not use a
/// fixed host list.
pub trait HistoryMembership: Send + Sync + 'static {
    fn hosts(&self) -> Result<Vec<Host>, Status>;
    /// Resolve each member's named endpoints from one membership snapshot.
    /// Adapters with richer provider data should override this to avoid mixing
    /// a host selection from one snapshot with ports from another.
    fn resolved_hosts(&self) -> Result<Vec<ResolvedHistoryHost>, Status> {
        Ok(self
            .hosts()?
            .into_iter()
            .map(|host| {
                let grpc_address = self.named_address(&host, NamedPort::Grpc).ok();
                let tchannel_address = self.named_address(&host, NamedPort::Tchannel).ok();
                ResolvedHistoryHost {
                    host,
                    grpc_address,
                    tchannel_address,
                }
            })
            .collect())
    }
    fn named_address(&self, host: &Host, port: NamedPort) -> Result<String, Status> {
        match port {
            NamedPort::Grpc if !host.grpc_address.is_empty() => Ok(host.grpc_address.clone()),
            // The common Host type currently has no separate TChannel field.
            // An adapter can override this using the provider's named ports.
            NamedPort::Tchannel | NamedPort::Grpc => Err(Status::unavailable(
                "History member has no requested named transport address.",
            )),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ResolvedHistoryHost {
    pub host: Host,
    pub grpc_address: Option<String>,
    pub tchannel_address: Option<String>,
}

#[derive(Clone)]
pub struct MatchingHistoryPeerResolver {
    shard_count: u32,
    named_port: NamedPort,
    membership: Arc<dyn HistoryMembership>,
}

impl MatchingHistoryPeerResolver {
    pub fn new(
        shard_count: u32,
        named_port: NamedPort,
        membership: Arc<dyn HistoryMembership>,
    ) -> Result<Self, Status> {
        if shard_count == 0 {
            return Err(Status::invalid_argument(
                "History shard count must be positive.",
            ));
        }
        Ok(Self {
            shard_count,
            named_port,
            membership,
        })
    }

    pub fn workflow(&self, workflow_id: &str) -> Result<String, Status> {
        self.shard_key(workflow_id)
    }

    pub fn domain(&self, domain_id: &str) -> Result<String, Status> {
        self.shard_key(domain_id)
    }

    /// Raw-byte twin of `workflow`: hashes the exact Go string bytes.
    pub fn workflow_bytes(&self, workflow_id: &[u8]) -> Result<String, Status> {
        self.shard_key_bytes(workflow_id)
    }

    /// Raw-byte twin of `domain`: hashes the exact Go string bytes.
    pub fn domain_bytes(&self, domain_id: &[u8]) -> Result<String, Status> {
        self.shard_key_bytes(domain_id)
    }

    fn shard_key_bytes(&self, value: &[u8]) -> Result<String, Status> {
        self.shard_id(cadence_common::membership::fingerprint32(value) % self.shard_count)
    }

    fn shard_key(&self, value: &str) -> Result<String, Status> {
        // Go common.WorkflowIDToHistoryShard and DomainIDToHistoryShard use
        // farm.Fingerprint32 % shard count; History lookup then hashes
        // string(rune(shardID)), not the decimal representation.
        let shard_id =
            cadence_common::membership::fingerprint32(value.as_bytes()) % self.shard_count;
        self.shard_id(shard_id)
    }

    pub fn shard_id(&self, shard_id: u32) -> Result<String, Status> {
        // Go conversion from int to rune and then string replaces invalid
        // code points (including surrogate values and values above U+10FFFF)
        // with U+FFFD. Preserve that behavior for the public shard API.
        let shard_rune = char::from_u32(shard_id).unwrap_or(char::REPLACEMENT_CHARACTER);
        self.owner(&shard_rune.to_string())
    }

    pub fn host_address(&self, address: &str) -> Result<String, Status> {
        let hosts = self.membership.resolved_hosts()?;
        let host = hosts
            .into_iter()
            .find(|host| host.host.address == address)
            .ok_or_else(|| {
                Status::unavailable(format!("History member {address:?} was not found."))
            })?;
        self.endpoint(&host)
    }

    pub fn all_peers(&self) -> Result<Vec<String>, Status> {
        self.membership
            .resolved_hosts()?
            .iter()
            .map(|host| self.endpoint(host))
            .collect()
    }

    fn owner(&self, key: &str) -> Result<String, Status> {
        let hosts = self.membership.resolved_hosts()?;
        let ring_hosts: Vec<_> = hosts.iter().map(|peer| peer.host.clone()).collect();
        Ring::new(ring_hosts)
            .lookup(key)
            .and_then(|host| hosts.iter().find(|peer| peer.host.address == host.address))
            .map(|host| self.endpoint(host))
            .transpose()?
            .ok_or_else(|| Status::internal("Not enough hosts to serve the request"))
    }

    fn endpoint(&self, host: &ResolvedHistoryHost) -> Result<String, Status> {
        match self.named_port {
            NamedPort::Grpc => host.grpc_address.clone(),
            NamedPort::Tchannel => host.tchannel_address.clone(),
        }
        .ok_or_else(|| {
            Status::unavailable(format!(
                "History member {:?} has no requested named transport address.",
                host.host.address
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{collections::HashMap, sync::RwLock};
    struct Snapshot(Vec<Host>);
    impl HistoryMembership for Snapshot {
        fn hosts(&self) -> Result<Vec<Host>, Status> {
            Ok(self.0.clone())
        }
    }
    fn resolver() -> MatchingHistoryPeerResolver {
        MatchingHistoryPeerResolver::new(
            1024,
            NamedPort::Grpc,
            Arc::new(Snapshot(vec![Host {
                address: "history-ring:7935".into(),
                identity: "history-a".into(),
                grpc_address: "history-grpc:7833".into(),
            }])),
        )
        .unwrap()
    }
    #[test]
    fn workflow_and_domain_share_go_hash_and_rune_shard_lookup() {
        let r = resolver();
        let workflow = "workflow-17";
        let shard = cadence_common::membership::fingerprint32(workflow.as_bytes()) % 1024;
        assert_eq!(r.workflow(workflow).unwrap(), r.shard_id(shard).unwrap());
        assert_eq!(
            r.domain("domain-5").unwrap(),
            r.shard_id(cadence_common::membership::fingerprint32(b"domain-5") % 1024)
                .unwrap()
        );
        assert_eq!(r.workflow(workflow).unwrap(), "history-grpc:7833");
    }
    #[test]
    fn shard_key_matches_go_rune_replacement_for_invalid_scalar() {
        let r = resolver();
        assert_eq!(r.shard_id(0xD800).unwrap(), r.shard_id(0xFFFD).unwrap());
        assert_eq!(r.shard_id(0x11_0000).unwrap(), r.shard_id(0xFFFD).unwrap());
    }
    #[test]
    fn address_redirect_and_all_members_use_selected_named_endpoint() {
        let r = resolver();
        assert_eq!(
            r.host_address("history-ring:7935").unwrap(),
            "history-grpc:7833"
        );
        assert_eq!(r.all_peers().unwrap(), vec!["history-grpc:7833"]);
        assert_eq!(
            r.host_address("not-a-member").unwrap_err().code(),
            tonic::Code::Unavailable
        );
    }

    struct LiveSnapshot(RwLock<Vec<Host>>);
    impl HistoryMembership for LiveSnapshot {
        fn hosts(&self) -> Result<Vec<Host>, Status> {
            Ok(self.0.read().unwrap().clone())
        }
    }
    fn host(id: &str) -> Host {
        Host {
            address: format!("{id}:7935"),
            identity: id.into(),
            grpc_address: format!("{id}:7833"),
        }
    }
    #[test]
    fn two_peer_placement_refresh_and_owner_redirect_use_current_membership() {
        let membership = Arc::new(LiveSnapshot(RwLock::new(vec![
            host("peer-a"),
            host("peer-b"),
        ])));
        let resolver =
            MatchingHistoryPeerResolver::new(1024, NamedPort::Grpc, membership.clone()).unwrap();
        // Select a key owned by B according to the pinned ring implementation.
        let key = (0..1000)
            .map(|n| format!("workflow-{n}"))
            .find(|key| resolver.workflow(key).unwrap() == "peer-b:7833")
            .unwrap();
        assert_eq!(resolver.workflow(&key).unwrap(), "peer-b:7833");
        // Go's ShardOwnershipLostError carries the membership address; redirect
        // resolution maps it through the current named gRPC port.
        assert_eq!(resolver.host_address("peer-a:7935").unwrap(), "peer-a:7833");
        membership
            .0
            .write()
            .unwrap()
            .retain(|peer| peer.identity == "peer-a");
        assert_eq!(resolver.workflow(&key).unwrap(), "peer-a:7833");
        assert_eq!(
            resolver.host_address("peer-b:7935").unwrap_err().code(),
            tonic::Code::Unavailable
        );
    }

    #[derive(Clone)]
    struct OracleMembership {
        hosts: Vec<Host>,
        named_ports: HashMap<String, (String, String)>,
    }
    impl HistoryMembership for OracleMembership {
        fn hosts(&self) -> Result<Vec<Host>, Status> {
            Ok(self.hosts.clone())
        }
        fn named_address(&self, host: &Host, port: NamedPort) -> Result<String, Status> {
            let (grpc, tchannel) = self.named_ports.get(&host.address).ok_or_else(|| {
                Status::unavailable(format!(
                    "History member {:?} has no named ports.",
                    host.address
                ))
            })?;
            Ok(match port {
                NamedPort::Grpc => grpc.clone(),
                NamedPort::Tchannel => tchannel.clone(),
            })
        }
    }

    fn oracle_membership(fixture: &serde_json::Value) -> OracleMembership {
        let mut named_ports = HashMap::new();
        let hosts = fixture["hosts"]
            .as_array()
            .expect("oracle hosts")
            .iter()
            .map(|host| {
                let address = host["address"].as_str().expect("host address").to_owned();
                let identity = host["identity"].as_str().expect("host identity").to_owned();
                let grpc = host["grpc"].as_str().expect("gRPC port").to_owned();
                let tchannel = host["tchannel"].as_str().expect("TChannel port").to_owned();
                named_ports.insert(address.clone(), (grpc.clone(), tchannel));
                Host {
                    address,
                    identity,
                    grpc_address: grpc,
                }
            })
            .collect();
        OracleMembership { hosts, named_ports }
    }

    fn route(resolver: &MatchingHistoryPeerResolver, key: &str) -> String {
        if let Some(workflow) = key.strip_prefix("workflow:") {
            resolver.workflow(workflow).unwrap()
        } else if let Some(domain) = key.strip_prefix("domain:") {
            resolver.domain(domain).unwrap()
        } else if let Some(shard) = key.strip_prefix("shard:") {
            resolver
                .shard_id(shard.parse().expect("oracle shard ID"))
                .unwrap()
        } else {
            panic!("unknown oracle route key: {key}")
        }
    }

    /// Fixed values emitted by the Go production `PeerResolver` over the real
    /// Go membership Ring (source hashes are in the adjacent evidence manifest).
    /// Expected routes are read only from that accepted fixture.
    #[test]
    fn matching_history_placement_matches_accepted_go_peer_oracle() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../tests/matching-history-peer-oracle/evidence/matching_history_peer_oracle.json"
        ))
        .expect("accepted Go oracle fixture");
        let membership = oracle_membership(&fixture);
        assert_eq!(
            membership.hosts.len(),
            3,
            "fixture must retain exact Go ring"
        );
        assert_eq!(
            membership
                .hosts
                .iter()
                .map(|host| (host.address.as_str(), host.identity.as_str()))
                .collect::<Vec<_>>(),
            vec![
                ("10.8.0.1:7934", "history-a"),
                ("10.8.0.2:7934", "history-b"),
                ("10.8.0.3:7934", "history-c"),
            ],
            "use the oracle's exact membership addresses and identities"
        );

        let grpc =
            MatchingHistoryPeerResolver::new(32, NamedPort::Grpc, Arc::new(membership.clone()))
                .unwrap();
        let routes = fixture["routes"].as_object().expect("oracle routes");
        assert_eq!(
            routes.len(),
            17,
            "compare every Go workflow/domain/shard route"
        );
        for (key, expected) in routes {
            assert_eq!(
                route(&grpc, key),
                expected.as_str().unwrap(),
                "Go placement mismatch for {key:?}"
            );
        }
        assert_eq!(
            fixture["shardKeys"]["shard:1114112"].as_str(),
            Some("�"),
            "Go converts an invalid rune to U+FFFD"
        );

        let tchannel =
            MatchingHistoryPeerResolver::new(32, NamedPort::Tchannel, Arc::new(membership.clone()))
                .unwrap();
        let mut grpc_peers = grpc.all_peers().unwrap();
        grpc_peers.sort();
        assert_eq!(
            grpc_peers,
            fixture["allGrpc"]
                .as_array()
                .unwrap()
                .iter()
                .map(|peer| peer.as_str().unwrap().to_owned())
                .collect::<Vec<_>>()
        );
        let mut tchannel_peers = tchannel.all_peers().unwrap();
        tchannel_peers.sort();
        assert_eq!(
            tchannel_peers,
            fixture["allTchannel"]
                .as_array()
                .unwrap()
                .iter()
                .map(|peer| peer.as_str().unwrap().to_owned())
                .collect::<Vec<_>>()
        );
        assert_eq!(
            grpc.host_address(&membership.hosts[1].address).unwrap(),
            fixture["addressGrpc"].as_str().unwrap()
        );
        assert_eq!(
            tchannel.host_address(&membership.hosts[2].address).unwrap(),
            fixture["addressTchannel"].as_str().unwrap()
        );

        let mut reduced = membership;
        reduced.hosts.truncate(2);
        let grpc_after_removal =
            MatchingHistoryPeerResolver::new(32, NamedPort::Grpc, Arc::new(reduced.clone()))
                .unwrap();
        let removed_routes = fixture["afterRemoval"]
            .as_object()
            .expect("post-removal routes");
        for (key, expected) in removed_routes {
            assert_eq!(
                route(&grpc_after_removal, key),
                expected.as_str().unwrap(),
                "Go post-removal placement mismatch for {key:?}"
            );
        }
        let mut remaining = grpc_after_removal.all_peers().unwrap();
        remaining.sort();
        assert_eq!(
            remaining,
            fixture["remaining"]
                .as_array()
                .unwrap()
                .iter()
                .map(|peer| peer.as_str().unwrap().to_owned())
                .collect::<Vec<_>>()
        );
        assert_eq!(
            grpc_after_removal
                .host_address("10.8.0.3:7934")
                .unwrap_err()
                .code(),
            tonic::Code::Unavailable,
            "removed Go ring member must no longer resolve"
        );
    }
}
