//! Task-list -> matching-host channel resolver for `GrpcQueryMatchingClient`
//! (Go `matching.Client` peer resolver `FromTaskList`: ring lookup of the
//! task list name over the live `cadence-matching` members).
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tonic::transport::{Channel, Endpoint};

use crate::wiring::resources::membership::MembershipResolver;
use crate::wiring::resources::query_matching::ChannelResolver;

/// Resolves matching gRPC channels from a Matching-service `MembershipResolver`.
/// Channels are lazy and cached per host gRPC address.
pub struct MatchingChannelResolver {
    matching_ring: Arc<MembershipResolver>,
    channels: Mutex<HashMap<String, Channel>>,
}

impl MatchingChannelResolver {
    pub fn new(matching_ring: Arc<MembershipResolver>) -> Self {
        Self { matching_ring, channels: Mutex::new(HashMap::new()) }
    }

    /// Address of the matching host owning `task_list` (gRPC address when
    /// advertised, otherwise the host address).
    pub fn address_for(&self, task_list: &str) -> Result<String, String> {
        let host = self
            .matching_ring
            .lookup(task_list)
            .ok_or_else(|| format!("no matching hosts available for task list {task_list}"))?;
        Ok(if host.grpc_address.is_empty() { host.address } else { host.grpc_address })
    }

    pub fn channel_for(&self, task_list: &str) -> Result<Channel, String> {
        let addr = self.address_for(task_list)?;
        let mut channels = self.channels.lock().map_err(|e| e.to_string())?;
        if let Some(c) = channels.get(&addr) {
            return Ok(c.clone());
        }
        let endpoint = Endpoint::from_shared(format!("http://{addr}")).map_err(|e| e.to_string())?;
        let channel = endpoint.connect_lazy();
        channels.insert(addr, channel.clone());
        Ok(channel)
    }

    /// Adapter for `GrpcQueryMatchingClient::new`.
    pub fn into_channel_resolver(self: Arc<Self>) -> ChannelResolver {
        Arc::new(move |task_list: &str| self.channel_for(task_list))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cadence_common::membership::Host;

    fn host(a: &str, g: &str) -> Host {
        Host { address: a.into(), identity: a.into(), grpc_address: g.into() }
    }

    #[test]
    fn empty_ring_errors() {
        let r = MatchingChannelResolver::new(Arc::new(MembershipResolver::new("h:1", vec![])));
        assert!(r.address_for("tl").is_err());
    }

    #[test]
    fn prefers_grpc_address_and_is_stable() {
        let ring = Arc::new(MembershipResolver::new("h:1", vec![host("m1:7939", "m1:7835")]));
        let r = MatchingChannelResolver::new(ring);
        assert_eq!(r.address_for("tl").unwrap(), "m1:7835");
        assert_eq!(r.address_for("tl").unwrap(), r.address_for("tl").unwrap());
    }

    #[tokio::test]
    async fn caches_channels() {
        let ring = Arc::new(MembershipResolver::new("h:1", vec![host("m1:7939", "m1:7835")]));
        let r = MatchingChannelResolver::new(ring);
        r.channel_for("a").unwrap();
        r.channel_for("b").unwrap();
        assert_eq!(r.channels.lock().unwrap().len(), 1);
    }
}
