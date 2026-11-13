//! Port of `NotifyFailoverMarkers` from go/client/history/client.go: tokens are
//! grouped by the History host owning each marker's domain shard
//! (`peerResolver.FromDomainID`) and each group is sent to that peer over RPC.
//! Go has no local fast path in the client, so every group goes over gRPC.
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use cadence_common::membership::{fingerprint32, Host};

use crate::failover::{FailoverMarkerToken, HistoryClient};
use crate::wiring::resources::membership::MembershipResolver;

/// Go `peerResolver.FromDomainID`.
pub trait DomainPeerResolver: Send + Sync {
    /// Returns the owning host (its gRPC address is the dial target).
    fn resolve_domain(&self, domain_id: &str) -> Result<Host, String>;
}

/// Go `common.DomainIDToHistoryShard`.
pub fn domain_id_to_history_shard(domain_id: &str, number_of_shards: i32) -> i32 {
    (fingerprint32(domain_id.as_bytes()) % (number_of_shards.max(1) as u32)) as i32
}

/// `peerResolver` over the History membership ring.
pub struct MembershipPeerResolver {
    pub membership: Arc<MembershipResolver>,
    pub number_of_shards: i32,
}

impl DomainPeerResolver for MembershipPeerResolver {
    fn resolve_domain(&self, domain_id: &str) -> Result<Host, String> {
        let shard = domain_id_to_history_shard(domain_id, self.number_of_shards);
        crate::lookup::history_server_by_shard_id(&self.membership, shard).map_err(|e| e.to_string())
    }
}

/// Groups tokens per peer (map keyed by peer, as in Go `requestsByPeer`). Any
/// resolution error fails the whole call.
pub fn group_tokens_by_peer(
    resolver: &dyn DomainPeerResolver,
    tokens: Vec<FailoverMarkerToken>,
) -> Result<BTreeMap<String, (Host, Vec<FailoverMarkerToken>)>, String> {
    let mut by_peer: BTreeMap<String, (Host, Vec<FailoverMarkerToken>)> = BTreeMap::new();
    for token in tokens {
        let domain_id = token.failover_marker.domain_id.clone().unwrap_or_default();
        let host = resolver.resolve_domain(&domain_id)?;
        by_peer.entry(host.address.clone()).or_insert_with(|| (host, Vec::new())).1.push(token);
    }
    Ok(by_peer)
}

fn to_pb(tokens: &[FailoverMarkerToken]) -> cadence_proto::history::NotifyFailoverMarkersRequest {
    cadence_proto::history::NotifyFailoverMarkersRequest {
        failover_marker_tokens: tokens
            .iter()
            .map(|t| cadence_proto::admin::FailoverMarkerToken {
                shard_ids: t.shard_ids.clone(),
                failover_marker: Some(cadence_proto::admin::FailoverMarkerAttributes {
                    domain_id: t.failover_marker.domain_id.clone().unwrap_or_default(),
                    failover_version: t.failover_marker.failover_version.unwrap_or_default(),
                    creation_time: t.failover_marker.creation_time.map(cadence_proto::convert::timestamp_from_nanos),
                }),
            })
            .collect(),
    }
}

/// `history.Client.NotifyFailoverMarkers` routing through the History peers.
pub struct RoutingFailoverHistoryClient {
    resolver: Arc<dyn DomainPeerResolver>,
}

impl RoutingFailoverHistoryClient {
    pub fn new(resolver: Arc<dyn DomainPeerResolver>) -> Arc<Self> {
        Arc::new(Self { resolver })
    }
}

#[async_trait]
impl HistoryClient for RoutingFailoverHistoryClient {
    async fn notify_failover_markers(&self, tokens: Vec<FailoverMarkerToken>, timeout: Duration) -> Result<(), String> {
        let groups = group_tokens_by_peer(self.resolver.as_ref(), tokens)?;
        // errgroup: all peers in parallel, first error wins.
        let mut set = tokio::task::JoinSet::new();
        for (_, (host, toks)) in groups {
            set.spawn(async move {
                if host.grpc_address.is_empty() {
                    return Err(format!("history host {} has no gRPC address", host.address));
                }
                let channel = cadence_service_core::grpc_tls::endpoint_channel(
                    &host.grpc_address,
                    crate::client_tls::history_client_tls().as_ref(),
                )
                .map_err(|e| e.to_string())?;
                let mut client = cadence_proto::history::history_api_client::HistoryApiClient::new(channel);
                let mut req = tonic::Request::new(to_pb(&toks));
                req.set_timeout(timeout);
                client.notify_failover_markers(req).await.map(|_| ()).map_err(|e| e.to_string())
            });
        }
        let mut first: Result<(), String> = Ok(());
        while let Some(r) = set.join_next().await {
            let r = r.unwrap_or_else(|e| Err(format!("panic: {e}")));
            if first.is_ok() {
                first = r;
            }
        }
        first
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cadence_api::types::FailoverMarkerAttributes;

    struct Fixed(BTreeMap<String, Host>);
    impl DomainPeerResolver for Fixed {
        fn resolve_domain(&self, d: &str) -> Result<Host, String> {
            self.0.get(d).cloned().ok_or_else(|| format!("no peer for {d}"))
        }
    }
    fn host(a: &str) -> Host {
        Host { address: a.into(), identity: a.into(), grpc_address: format!("{a}:7") }
    }
    fn tok(d: &str) -> FailoverMarkerToken {
        FailoverMarkerToken {
            shard_ids: vec![1],
            failover_marker: FailoverMarkerAttributes {
                domain_id: Some(d.into()),
                failover_version: Some(3),
                creation_time: Some(5),
            },
        }
    }

    #[test]
    fn groups_tokens_by_owning_peer_preserving_order() {
        let r = Fixed(BTreeMap::from([("a".into(), host("h1")), ("b".into(), host("h2")), ("c".into(), host("h1"))]));
        let g = group_tokens_by_peer(&r, vec![tok("a"), tok("b"), tok("c")]).unwrap();
        assert_eq!(g.len(), 2);
        let h1: Vec<_> = g["h1"].1.iter().map(|t| t.failover_marker.domain_id.clone().unwrap()).collect();
        assert_eq!(h1, vec!["a", "c"]);
        assert_eq!(g["h2"].1.len(), 1);
    }

    #[test]
    fn resolver_error_fails_whole_request() {
        let r = Fixed(BTreeMap::from([("a".into(), host("h1"))]));
        assert!(group_tokens_by_peer(&r, vec![tok("a"), tok("zzz")]).is_err());
    }

    #[test]
    fn empty_tokens_have_no_groups() {
        let r = Fixed(BTreeMap::new());
        assert!(group_tokens_by_peer(&r, vec![]).unwrap().is_empty());
    }

    #[test]
    fn membership_resolver_routes_domain_via_shard_owner() {
        let m = Arc::new(MembershipResolver::new("h1", vec![host("h1")]));
        let r = MembershipPeerResolver { membership: m, number_of_shards: 4 };
        assert_eq!(r.resolve_domain("any-domain").unwrap().address, "h1");
        let empty = MembershipPeerResolver { membership: Arc::new(MembershipResolver::new("x", vec![])), number_of_shards: 4 };
        assert!(empty.resolve_domain("any-domain").is_err());
    }

    #[test]
    fn domain_shard_in_range() {
        for d in ["a", "domain-1", ""] {
            let s = domain_id_to_history_shard(d, 16);
            assert!((0..16).contains(&s));
        }
    }

    #[test]
    fn pb_conversion_keeps_fields() {
        let r = to_pb(&[tok("a")]);
        let t = &r.failover_marker_tokens[0];
        assert_eq!(t.shard_ids, vec![1]);
        let m = t.failover_marker.as_ref().unwrap();
        assert_eq!((m.domain_id.as_str(), m.failover_version), ("a", 3));
        assert!(m.creation_time.is_some());
    }
}
