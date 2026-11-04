//! Feeds [`MembershipResolver`] from Ringpop membership events
//! (Go `membership.Resolver` subscribes to `ringpop` change events and rebuilds
//! its hash ring on every change; `common/membership/resolver.go`).
use std::sync::Arc;

use cadence_common::membership::Host;
use cadence_matching::matching_peer_provider::{MatchingPeerProvider, PeerMember};
use cadence_matching::matching_ringpop_provider::MatchingRingpopProvider;
use tokio::sync::broadcast::error::RecvError;
use tokio::task::JoinHandle;

use super::membership::MembershipResolver;

fn hosts_for(members: &[PeerMember], service: &str) -> Vec<Host> {
    members.iter().filter_map(|m| m.matching_host(service)).collect()
}

/// Pull the current Ringpop snapshot into the resolver once.
pub fn refresh_from_snapshot(
    resolver: &MembershipResolver,
    provider: &MatchingRingpopProvider,
    service: &str,
) {
    let snap = provider.snapshot();
    resolver.update_hosts(hosts_for(&snap.members, service));
}

/// Subscribe to Ringpop change events; every event (a hint only) re-reads the
/// snapshot and rebuilds the ring. Runs until the sender is dropped.
pub async fn spawn_membership_feed(
    resolver: Arc<MembershipResolver>,
    provider: Arc<MatchingRingpopProvider>,
    service: String,
) -> Result<JoinHandle<()>, String> {
    let mut rx = provider.subscribe(&service).await.map_err(|e| e.to_string())?;
    refresh_from_snapshot(&resolver, &provider, &service);
    Ok(tokio::spawn(async move {
        while matches!(rx.recv().await, Ok(_) | Err(RecvError::Lagged(_))) {
            refresh_from_snapshot(&resolver, &provider, &service);
        }
    }))
}
