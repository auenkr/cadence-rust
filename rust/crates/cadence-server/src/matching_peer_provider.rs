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
    /// Successfully parsed numeric Ringpop port labels. Malformed labels are
    /// omitted, matching Go's warning-and-continue conversion behavior.
    pub named_ports: std::collections::HashMap<String, u16>,
}

impl PeerMember {
    /// Apply Ringpop's role/status filter and HostInfo address/identity/port mapping.
    pub fn matching_host(&self, service: &str) -> Option<Host> {
        if !matches!(self.status, PeerStatus::Alive | PeerStatus::Suspect)
            || self.labels.get("serviceName").map(String::as_str) != Some(service)
        {
            return None;
        }
        let grpc = self.named_address("grpc").unwrap_or_default();
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

    /// Resolve a port label against this member's advertised identity address.
    /// Legacy TChannel peers omit the label, so their address port is the
    /// fallback used by Go's Ringpop provider.
    pub fn named_address(&self, name: &str) -> Option<String> {
        // Go ignores SplitHostPort errors when constructing HostInfo, then
        // JoinHostPort formats a valid named port against the empty host.
        let host = host_from_host_port(&self.address).unwrap_or_default();
        let port = match self.labels.get(name) {
            Some(_) => self.named_ports.get(name).copied(),
            None if name == "tchannel" => port_from_host_port(&self.address),
            None => None,
        }?;
        Some(join_host_port(&host, &port.to_string()))
    }

    pub fn tchannel_address(&self) -> Option<String> {
        self.named_address("tchannel")
    }
}

fn host_from_host_port(address: &str) -> Option<String> {
    let colon = address.rfind(':')?;
    let host = if address.starts_with('[') {
        let end = address.find(']')?;
        match end + 1 {
            n if n == address.len() => return None,
            n if n == colon => &address[1..end],
            n if address.as_bytes()[n] == b':' => return None,
            _ => return None,
        }
    } else {
        let host = &address[..colon];
        if host.contains(':') {
            return None;
        }
        host
    };
    if let Some(after_open) = address.strip_prefix('[') {
        let end = address.find(']')?;
        if after_open.contains('[') || address[end + 1..].contains(']') {
            return None;
        }
    } else {
        if address.contains('[') || address.contains(']') {
            return None;
        }
    }
    Some(host.to_owned())
}

fn port_from_host_port(address: &str) -> Option<u16> {
    let (_, port) = address.rsplit_once(':')?;
    port.parse().ok()
}

fn join_host_port(host: &str, port: &str) -> String {
    if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

#[cfg(test)]
mod named_port_tests {
    use super::*;

    #[test]
    fn role_filter_and_named_ports_preserve_legacy_tchannel_fallback() {
        let member = PeerMember {
            address: "[2001:db8::1]:7935".into(),
            identity: "stable-id".into(),
            status: PeerStatus::Alive,
            labels: std::collections::HashMap::from([
                ("serviceName".into(), "cadence-matching".into()),
                ("grpc".into(), "7835".into()),
            ]),
            named_ports: std::collections::HashMap::from([("grpc".into(), 7835)]),
        };
        let host = member.matching_host("cadence-matching").unwrap();
        assert_eq!(host.identity, "stable-id");
        assert_eq!(host.grpc_address, "[2001:db8::1]:7835");
        assert_eq!(
            member.tchannel_address().as_deref(),
            Some("[2001:db8::1]:7935")
        );
        assert!(member.matching_host("matching").is_none());
        assert!(PeerMember {
            status: PeerStatus::Faulty,
            ..member
        }
        .matching_host("cadence-matching")
        .is_none());
    }

    #[test]
    fn malformed_present_tchannel_label_does_not_use_address_fallback() {
        let member = PeerMember {
            address: "127.0.0.1:7935".into(),
            identity: "".into(),
            status: PeerStatus::Suspect,
            labels: std::collections::HashMap::from([("tchannel".into(), "bad".into())]),
            named_ports: std::collections::HashMap::new(),
        };
        assert_eq!(member.identity, "");
        assert_eq!(member.tchannel_address(), None);
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
    fn members(
        &self,
        service: &str,
    ) -> impl std::future::Future<Output = Result<Vec<PeerMember>, ProviderError>> + Send;
    fn subscribe(
        &self,
        service: &str,
    ) -> impl std::future::Future<Output = Result<broadcast::Receiver<PeerChange>, ProviderError>> + Send;
    fn self_evict(&self) -> impl std::future::Future<Output = Result<(), ProviderError>> + Send;
    fn stop(&self) -> impl std::future::Future<Output = Result<(), ProviderError>> + Send;
}
