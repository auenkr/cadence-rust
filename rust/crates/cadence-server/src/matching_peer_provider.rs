//! Boundary for Matching peer discovery. Implementations must provide a real
//! distributed provider; this module intentionally does not implement Ringpop.
use cadence_common::membership::Host;
use std::fmt;
use tokio::sync::broadcast;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PeerStatus {
    Alive,
    Suspect,
    Faulty,
    Left,
    Unknown,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PeerMember {
    pub address: String,
    pub identity: String,
    pub status: PeerStatus,
    pub labels: std::collections::HashMap<String, String>,
}

impl PeerMember {
    /// Apply Ringpop's role/status filter and HostInfo address/identity/port mapping.
    pub fn matching_host(&self, service: &str) -> Option<Host> {
        if !matches!(self.status, PeerStatus::Alive | PeerStatus::Suspect)
            || self.labels.get("serviceName").map(String::as_str) != Some(service)
        {
            return None;
        }
        let host = self
            .address
            .rsplit_once(':')
            .map(|(h, _)| h)
            .unwrap_or(&self.address);
        let grpc = self
            .labels
            .get("grpc")
            .filter(|p| p.parse::<u16>().is_ok())
            .map(|p| format!("{host}:{p}"))
            .unwrap_or_default();
        Some(Host {
            address: self.address.clone(),
            identity: self.identity.clone(),
            grpc_address: grpc,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PeerChange;

#[derive(Debug, Clone)]
pub struct ProviderError(pub String);
impl fmt::Display for ProviderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for ProviderError {}

/// Async infrastructure seam. Events are hints only: consumers re-fetch snapshots.
#[allow(async_fn_in_trait)]
pub trait MatchingPeerProvider: Send + Sync + 'static {
    async fn start(&self) -> Result<(), ProviderError>;
    async fn members(&self, service: &str) -> Result<Vec<PeerMember>, ProviderError>;
    async fn subscribe(
        &self,
        service: &str,
    ) -> Result<broadcast::Receiver<PeerChange>, ProviderError>;
    async fn self_evict(&self) -> Result<(), ProviderError>;
    async fn stop(&self) -> Result<(), ProviderError>;
}
