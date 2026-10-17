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
        let host = host_from_host_port(&self.address).unwrap_or_default();
        let grpc = self.labels.get("grpc")
            .and_then(|p| label_to_port(p).map(|port| format!("{host}:{port}")))
            .unwrap_or_default();
        Some(Host {
            address: self.address.clone(),
            identity: if self.identity.is_empty() {
                self.address.clone()
            } else {
                self.identity.clone()
            },
            grpc_address: grpc,
        })
    }
}

fn label_to_port(label: &str) -> Option<u16> {
    if label.is_empty() || !label.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    label.parse::<u16>().ok()
}

fn host_from_host_port(address: &str) -> Option<String> {
    if let Some(rest) = address.strip_prefix('[') {
        if let Some((host, suffix)) = rest.split_once(']') {
            if suffix.starts_with(':') && !suffix[1..].is_empty() {
                return Some(format!("[{host}]"));
            }
        }
        return None;
    }
    match address.rsplit_once(':') {
        Some((host, port)) if !port.is_empty() && !host.contains(':') => {
            Some(host.to_owned())
        }
        _ => None,
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
pub trait MatchingPeerProvider: Send + Sync + 'static {
    fn start(&self) -> impl std::future::Future<Output = Result<(), ProviderError>> + Send;
    fn members(&self, service: &str) -> impl std::future::Future<Output = Result<Vec<PeerMember>, ProviderError>> + Send;
    fn subscribe(
        &self,
        service: &str,
    ) -> impl std::future::Future<Output = Result<broadcast::Receiver<PeerChange>, ProviderError>> + Send;
    fn self_evict(&self) -> impl std::future::Future<Output = Result<(), ProviderError>> + Send;
    fn stop(&self) -> impl std::future::Future<Output = Result<(), ProviderError>> + Send;
}
