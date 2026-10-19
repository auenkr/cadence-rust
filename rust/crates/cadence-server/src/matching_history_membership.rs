//! Adapt live Ringpop membership into the History peer snapshot used by
//! Matching. This mirrors Cadence's Go ringpop provider's membership filter,
//! identity label, and named gRPC port handling.
use crate::matching_history_peer_resolver::HistoryMembership;
use crate::ringpop_provider::RingpopNode;
use cadence_common::{
    membership::Host,
    ringpop_wire::{Member, ALIVE, SUSPECT},
};
use std::sync::Arc;
use tonic::Status;

const SERVICE_LABEL: &str = "serviceName";
const IDENTITY_LABEL: &str = "__identity";
const GRPC_PORT_LABEL: &str = "grpc";
const TCHANNEL_PORT_LABEL: &str = "tchannel";

/// Reads the current Ringpop memberlist on every call, so placement immediately
/// reflects joins, failures, and recovery without a second cached view.
pub struct RingpopHistoryMembership {
    node: Arc<RingpopNode>,
}

impl RingpopHistoryMembership {
    pub fn new(node: Arc<RingpopNode>) -> Self {
        Self { node }
    }
}

impl HistoryMembership for RingpopHistoryMembership {
    fn hosts(&self) -> Result<Vec<Host>, Status> {
        let (members, _) = self.node.membership_snapshot();
        Ok(history_hosts(&members))
    }

    fn named_address(
        &self,
        host: &Host,
        port: crate::matching_history_peer_resolver::NamedPort,
    ) -> Result<String, Status> {
        let port_name = match port {
            crate::matching_history_peer_resolver::NamedPort::Grpc => GRPC_PORT_LABEL,
            crate::matching_history_peer_resolver::NamedPort::Tchannel => TCHANNEL_PORT_LABEL,
        };
        let (members, _) = self.node.membership_snapshot();
        let member = members
            .iter()
            .find(|member| member.address == host.address)
            .ok_or_else(|| {
                Status::unavailable(format!("History member {:?} was not found.", host.address))
            })?;
        named_address(member, port_name).ok_or_else(|| {
            Status::unavailable(format!("History member has no valid {port_name} port."))
        })
    }
}

fn history_hosts(members: &[Member]) -> Vec<Host> {
    members
        .iter()
        .filter_map(|member| history_host(member))
        .collect()
}

fn history_host(member: &Member) -> Option<Host> {
    let labels = member.labels.as_ref();
    if labels?.get(SERVICE_LABEL).map(String::as_str) != Some("cadence-history")
        || (member.status != ALIVE && member.status != SUSPECT)
    {
        return None;
    }

    let identity = labels
        .and_then(|labels| labels.get(IDENTITY_LABEL))
        .cloned()
        .unwrap_or_else(|| member.address.clone());

    // Go's provider uses a named grpc label only. Unlike its TChannel port,
    // grpc has no address-port fallback for legacy members.
    let grpc_address = named_address(member, GRPC_PORT_LABEL).unwrap_or_default();

    Some(Host {
        address: member.address.clone(),
        identity,
        grpc_address,
    })
}

fn named_address(member: &Member, name: &str) -> Option<String> {
    let labels = member.labels.as_ref()?;
    let port = match labels.get(name) {
        Some(label) => parse_go_port(label),
        None if name == TCHANNEL_PORT_LABEL => {
            let (_, address_port) = go_split_host_port(&member.address)?;
            parse_go_port(address_port)
        }
        None => None,
    }?;

    // Go NewDetailedHostInfo ignores SplitHostPort errors and retains an empty
    // host; GetNamedAddress then returns JoinHostPort("", port), i.e. ":port".
    let host = go_split_host_port(&member.address)
        .map(|(host, _)| host)
        .unwrap_or("");
    Some(go_join_host_port(host, port))
}

fn parse_go_port(value: &str) -> Option<u16> {
    // strconv.ParseUint(..., 10, 16) rejects signs and whitespace.
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    value.parse().ok()
}

/// The successful forms of Go net.SplitHostPort, including DNS names and
/// bracketed IPv6. Go rejects unbracketed IPv6 literals here.
fn go_split_host_port(address: &str) -> Option<(&str, &str)> {
    let last_colon = address.rfind(':')?;
    if let Some(rest) = address.strip_prefix('[') {
        let close = rest.find(']')?;
        let close = close + 1;
        if close + 1 != last_colon
            || address[1..].contains('[')
            || address[close + 1..].contains(']')
        {
            return None;
        }
        let host = &rest[..close - 1];
        let port = &address[last_colon + 1..];
        return Some((host, port));
    }

    let host = &address[..last_colon];
    let port = &address[last_colon + 1..];
    if host.contains(':') || address.contains('[') || address.contains(']') {
        return None;
    }
    Some((host, port))
}

fn go_join_host_port(host: &str, port: u16) -> String {
    if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cadence_common::ringpop_wire::{Change, Ping};
    use std::collections::BTreeMap;

    fn member(address: &str, status: &str, labels: &[(&str, &str)]) -> Member {
        Member {
            address: address.into(),
            status: status.into(),
            incarnation: 1,
            labels: Some(
                labels
                    .iter()
                    .map(|(key, value)| ((*key).into(), (*value).into()))
                    .collect::<BTreeMap<_, _>>(),
            ),
        }
    }

    #[test]
    fn filters_go_reachable_history_members_and_maps_identity_and_grpc() {
        let members = vec![
            member(
                "[2001:db8::1]:7933",
                ALIVE,
                &[
                    ("serviceName", "cadence-history"),
                    ("__identity", "host-a"),
                    ("grpc", "7833"),
                ],
            ),
            member(
                "history-two:7933",
                SUSPECT,
                &[("serviceName", "cadence-history"), ("grpc", "07834")],
            ),
            member("wrong-service:7933", ALIVE, &[("serviceName", "matching")]),
            member(
                "faulty-history:7933",
                "faulty",
                &[("serviceName", "cadence-history")],
            ),
        ];

        assert_eq!(
            history_hosts(&members),
            vec![
                Host {
                    address: "[2001:db8::1]:7933".into(),
                    identity: "host-a".into(),
                    grpc_address: "[2001:db8::1]:7833".into(),
                },
                Host {
                    address: "history-two:7933".into(),
                    identity: "history-two:7933".into(),
                    grpc_address: "history-two:7834".into(),
                }
            ]
        );

        let empty_identity = member(
            "empty-identity:7933",
            ALIVE,
            &[("serviceName", "cadence-history"), ("__identity", "")],
        );
        assert_eq!(
            history_host(&empty_identity).unwrap().identity(),
            "empty-identity:7933"
        );
    }

    #[test]
    fn go_port_and_split_host_port_edge_cases_are_preserved() {
        assert_eq!(parse_go_port("+42"), None);
        assert_eq!(parse_go_port("65535"), Some(65535));
        assert_eq!(parse_go_port("65536"), None);
        assert_eq!(parse_go_port(" 42"), None);
        assert_eq!(go_split_host_port("2001:db8::1:7933"), None);
        assert_eq!(go_split_host_port(":7933"), Some(("", "7933")));
        assert_eq!(go_split_host_port("history:"), Some(("history", "")));
        assert_eq!(
            go_split_host_port("[2001:db8::1]:7933"),
            Some(("2001:db8::1", "7933"))
        );

        let malformed = member(
            "unbracketed:host:7933",
            ALIVE,
            &[("serviceName", "cadence-history"), ("grpc", "7833")],
        );
        assert_eq!(history_host(&malformed).unwrap().grpc_address, ":7833");
    }

    #[test]
    fn grpc_has_no_address_port_fallback() {
        let legacy = member(
            "legacy-history:7933",
            ALIVE,
            &[("serviceName", "cadence-history")],
        );
        assert_eq!(history_host(&legacy).unwrap().grpc_address, "");
        let invalid = member(
            "legacy-history:7933",
            ALIVE,
            &[("serviceName", "cadence-history"), ("grpc", "nope")],
        );
        assert_eq!(history_host(&invalid).unwrap().grpc_address, "");
    }

    #[test]
    fn tchannel_uses_the_go_legacy_address_port_fallback() {
        let legacy = member(
            "legacy-history:7933",
            ALIVE,
            &[("serviceName", "cadence-history")],
        );
        assert_eq!(
            named_address(&legacy, TCHANNEL_PORT_LABEL),
            Some("legacy-history:7933".into())
        );
        let malformed = member(
            "legacy-history:bad",
            ALIVE,
            &[("serviceName", "cadence-history")],
        );
        assert_eq!(named_address(&malformed, TCHANNEL_PORT_LABEL), None);
    }

    #[test]
    fn reads_membership_snapshot_again_after_member_changes() {
        let node = Arc::new(RingpopNode::new(
            "cadence-history",
            Member {
                address: "local-history:7933".into(),
                status: ALIVE.into(),
                incarnation: 1,
                labels: Some(BTreeMap::from([(
                    "serviceName".into(),
                    "cadence-history".into(),
                )])),
            },
            false,
        ));
        let adapter = RingpopHistoryMembership::new(node.clone());
        assert_eq!(adapter.hosts().unwrap().len(), 1);

        let member_labels = BTreeMap::from([
            ("serviceName".into(), "cadence-history".into()),
            ("grpc".into(), "7934".into()),
        ]);
        let ping = Ping {
            app: "cadence-history".into(),
            source: "seed:7933".into(),
            source_incarnation: 1,
            changes: Some(vec![Change {
                source: "seed:7933".into(),
                source_incarnation: 1,
                address: "remote-history:7933".into(),
                incarnation: 1,
                status: ALIVE.into(),
                labels: Some(member_labels.clone()),
                ..Default::default()
            }]),
            ..Default::default()
        };
        node.handle_ping_with(&serde_json::to_vec(&ping).unwrap(), || 2, |_| 0)
            .unwrap();
        assert_eq!(adapter.hosts().unwrap().len(), 2);

        let ping = Ping {
            app: "cadence-history".into(),
            source: "seed:7933".into(),
            source_incarnation: 2,
            changes: Some(vec![Change {
                source: "seed:7933".into(),
                source_incarnation: 2,
                address: "remote-history:7933".into(),
                incarnation: 2,
                status: "faulty".into(),
                labels: Some(member_labels),
                ..Default::default()
            }]),
            ..Default::default()
        };
        node.handle_ping_with(&serde_json::to_vec(&ping).unwrap(), || 3, |_| 0)
            .unwrap();
        assert_eq!(adapter.hosts().unwrap().len(), 1);
    }
}
