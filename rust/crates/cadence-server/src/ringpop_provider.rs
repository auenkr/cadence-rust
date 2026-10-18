//! Bounded inbound Ringpop v0.10.0 JOIN and direct PING state.
//!
//! This handler deliberately does not bootstrap, probe, schedule timers, or
//! declare readiness. The owner must complete bootstrap and call `set_ready`.
use cadence_common::ringpop_disseminator::Disseminator;
use cadence_common::ringpop_memberlist::Memberlist;
use cadence_common::ringpop_wire::{
    Change, JoinRequest, JoinResponse, Member, Ping, ALIVE, SUSPECT,
};
use serde::de::DeserializeOwned;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use crate::ringpop_tchannel::{RingpopError, RingpopHandler, RingpopResponse};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RingpopRequestCounts {
    pub join: u64,
    pub ping: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RingpopLifecycle {
    Bootstrapping,
    Ready,
    Stopped,
}

#[derive(Clone)]
pub struct RingpopNode {
    state: Arc<Mutex<State>>,
}

struct State {
    app: String,
    address: String,
    ready: bool,
    stopped: bool,
    requires_app_in_ping: bool,
    memberlist: Memberlist,
    disseminator: Disseminator,
    counts: RingpopRequestCounts,
}

impl RingpopNode {
    pub fn new(app: impl Into<String>, local_member: Member, requires_app_in_ping: bool) -> Self {
        let mut disseminator = Disseminator::default();
        // swim.NewNode records its initial local member with an empty source;
        // preserve this pre-bootstrap delta and its nil/empty wire behavior.
        disseminator.record(Change {
            address: local_member.address.clone(),
            incarnation: local_member.incarnation,
            status: local_member.status.clone(),
            labels: local_member.labels.clone(),
            ..Default::default()
        });
        Self {
            state: Arc::new(Mutex::new(State {
                app: app.into(),
                address: local_member.address.clone(),
                ready: false,
                stopped: false,
                requires_app_in_ping,
                memberlist: Memberlist::new(local_member),
                disseminator,
                counts: RingpopRequestCounts::default(),
            })),
        }
    }

    /// Bootstrap owner controls readiness. `false` also supports draining.
    pub fn set_ready(&self, ready: bool) {
        self.state.lock().expect("Ringpop state poisoned").ready = ready;
    }

    pub fn stop(&self) {
        self.state.lock().expect("Ringpop state poisoned").stopped = true;
    }

    pub fn lifecycle(&self) -> RingpopLifecycle {
        let state = self.state.lock().expect("Ringpop state poisoned");
        if state.stopped {
            RingpopLifecycle::Stopped
        } else if state.ready {
            RingpopLifecycle::Ready
        } else {
            RingpopLifecycle::Bootstrapping
        }
    }

    pub fn is_ready(&self) -> bool {
        self.state.lock().expect("Ringpop state poisoned").ready
    }

    pub fn request_counts(&self) -> RingpopRequestCounts {
        self.state.lock().expect("Ringpop state poisoned").counts
    }

    pub fn dissemination_counts(&self) -> BTreeMap<String, usize> {
        self.state
            .lock()
            .expect("Ringpop state poisoned")
            .disseminator
            .propagation_counts()
    }

    pub fn handler(&self) -> RingpopHandler {
        let state = Arc::clone(&self.state);
        RingpopHandler::new(move |method, _headers, body| {
            let state = Arc::clone(&state);
            async move {
                let result = match method.as_str() {
                    "/protocol/join" => handle_join(&state, &body),
                    "/protocol/ping" => handle_ping(
                        &state,
                        &body,
                        || {
                            std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_millis() as i64
                        },
                        |len| rand::Rng::gen_range(&mut rand::thread_rng(), 0..len),
                    ),
                    _ => Err(RingpopError::System(format!(
                        "unknown Ringpop method: {method}"
                    ))),
                }?;
                Ok(RingpopResponse {
                    headers: None,
                    body: result,
                })
            }
        })
    }

    /// Deterministic direct-handler entry for tests and owners that inject the
    /// same clock and insertion source used by their memberlist lifecycle.
    pub fn handle_ping_with<F, R>(
        &self,
        body: &[u8],
        next_incarnation: F,
        insert_position: R,
    ) -> Result<Vec<u8>, RingpopError>
    where
        F: FnMut() -> i64,
        R: FnMut(usize) -> usize,
    {
        handle_ping(&self.state, body, next_incarnation, insert_position)
    }
}

fn decode<T: DeserializeOwned>(body: &[u8]) -> Result<T, RingpopError> {
    serde_json::from_slice(body).map_err(|e| RingpopError::System(e.to_string()))
}

fn encode<T: serde::Serialize>(value: &T) -> Result<Vec<u8>, RingpopError> {
    serde_json::to_vec(value).map_err(|e| RingpopError::System(e.to_string()))
}

fn handle_join(state: &Arc<Mutex<State>>, body: &[u8]) -> Result<Vec<u8>, RingpopError> {
    let request: JoinRequest = decode(body)?;
    let mut node = state.lock().expect("Ringpop state poisoned");
    node.counts.join = node.counts.join.saturating_add(1);
    if node.address == request.source {
        return Err(RingpopError::Application(format!(
            "A node tried joining a cluster by attempting to join itself. The node ,{}, must join someone else.", request.source
        )));
    }
    if node.app != request.app {
        return Err(RingpopError::Application(format!(
            "A node tried joining a different app cluster. The expected app, {}, did not match the actual app ,{}", node.app, request.app
        )));
    }
    let local = node
        .memberlist
        .member(&node.address)
        .expect("local member")
        .clone();
    let membership = node
        .memberlist
        .members()
        .iter()
        .map(|member| {
            Change {
                source: local.address.clone(),
                source_incarnation: local.incarnation,
                address: member.address.clone(),
                incarnation: member.incarnation,
                status: member.status.clone(),
                tombstone: false,
                labels: member.labels.clone(),
                timestamp: Default::default(),
            }
            .validate_outgoing()
        })
        .collect();
    encode(&JoinResponse {
        app: node.app.clone(),
        coordinator: node.address.clone(),
        membership: Some(membership),
        membership_checksum: node.memberlist.checksum(),
    })
}

fn handle_ping<F, R>(
    state: &Arc<Mutex<State>>,
    body: &[u8],
    next_incarnation: F,
    insert_position: R,
) -> Result<Vec<u8>, RingpopError>
where
    F: FnMut() -> i64,
    R: FnMut(usize) -> usize,
{
    let request: Ping = decode(body)?;
    let mut node = state.lock().expect("Ringpop state poisoned");
    if !node.ready {
        return Err(RingpopError::Application(
            "node is not ready to handle requests".into(),
        ));
    }
    if request.app.is_empty() {
        if node.requires_app_in_ping {
            return Err(RingpopError::Application(
                "Pinged ringpop requires app name".into(),
            ));
        }
    } else if request.app != node.app {
        return Err(RingpopError::Application(
            "Pinged ringpop has a different app name".into(),
        ));
    }
    node.counts.ping = node.counts.ping.saturating_add(1);
    let local_address = node.address.clone();
    let update = node.memberlist.update(
        request.changes.as_deref().unwrap_or_default(),
        next_incarnation,
        insert_position,
    );
    let has_accepted_changes = !update.accepted.is_empty();
    for change in update.accepted {
        node.disseminator.record(change);
    }
    let local_incarnation = node
        .memberlist
        .member(&local_address)
        .expect("local member")
        .incarnation;
    let local_checksum = node.memberlist.checksum();
    let members = node.memberlist.members().to_vec();
    if has_accepted_changes {
        let pingable_members = members
            .iter()
            .filter(|member| {
                member.address != local_address
                    && (member.status == ALIVE || member.status == SUSPECT)
            })
            .count();
        node.disseminator.adjust_max_propagations(pingable_members);
    }
    let (changes, _full_sync) = node.disseminator.issue_receiver(
        &request.source,
        request.source_incarnation,
        request.checksum,
        local_checksum,
        &members,
        &local_address,
        local_incarnation,
    );
    encode(&Ping {
        checksum: local_checksum,
        changes: Some(changes),
        source: local_address,
        source_incarnation: local_incarnation,
        app: String::new(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use cadence_common::ringpop_wire::{Change, Ping, ALIVE};
    use std::collections::BTreeMap;

    fn node() -> RingpopNode {
        RingpopNode::new(
            "cadence-matching",
            Member {
                address: "127.0.0.1:7933".into(),
                status: ALIVE.into(),
                incarnation: 100,
                labels: Some(BTreeMap::from([("serviceName".into(), "matching".into())])),
            },
            true,
        )
    }

    #[test]
    fn join_returns_go_shaped_membership_and_checksum_without_setting_ready() {
        let node = node();
        let response = handle_join(
            &node.state,
            br#"{"app":"cadence-matching","source":"127.0.0.2:7933"}"#,
        )
        .unwrap();
        let response: JoinResponse = serde_json::from_slice(&response).unwrap();
        assert_eq!(response.coordinator, "127.0.0.1:7933");
        assert_eq!(response.membership.unwrap().len(), 1);
        assert_eq!(
            response.membership_checksum,
            node.state.lock().unwrap().memberlist.checksum()
        );
        assert!(!node.is_ready());
    }

    #[test]
    fn join_rejects_unknown_app_self_join_and_malformed_body_with_pinned_messages() {
        let node = node();
        for (body, expected) in [
            (br#"{"app":"wrong","source":"remote"}"#.as_slice(), "A node tried joining a different app cluster. The expected app, cadence-matching, did not match the actual app ,wrong"),
            (br#"{"app":"cadence-matching","source":"127.0.0.1:7933"}"#.as_slice(), "A node tried joining a cluster by attempting to join itself. The node ,127.0.0.1:7933, must join someone else."),
        ] {
            let error = handle_join(&node.state, body).unwrap_err();
            assert_eq!(error, RingpopError::Application(expected.into()));
        }
        assert!(matches!(
            handle_join(&node.state, b"{"),
            Err(RingpopError::System(_))
        ));
        assert_eq!(node.request_counts().join, 2);
    }

    #[test]
    fn ping_requires_readiness_then_merges_and_returns_receiver_checksum() {
        let node = node();
        let request = Ping {
            app: "cadence-matching".into(),
            source: "127.0.0.2:7933".into(),
            source_incarnation: 200,
            checksum: 1,
            changes: Some(vec![Change {
                source: "127.0.0.2:7933".into(),
                source_incarnation: 200,
                address: "127.0.0.2:7933".into(),
                incarnation: 200,
                status: ALIVE.into(),
                ..Default::default()
            }]),
        };
        let body = serde_json::to_vec(&request).unwrap();
        assert_eq!(
            handle_ping(&node.state, &body, || 101, |_| 0).unwrap_err(),
            RingpopError::Application("node is not ready to handle requests".into())
        );
        assert_eq!(node.request_counts().ping, 0);
        node.set_ready(true);
        let raw = node.handle_ping_with(&body, || 101, |_| 0).unwrap();
        let response: Ping = serde_json::from_slice(&raw).unwrap();
        assert_eq!(response.source, "127.0.0.1:7933");
        assert_eq!(response.source_incarnation, 100);
        assert_eq!(response.app, "");
        assert!(node
            .state
            .lock()
            .unwrap()
            .memberlist
            .member("127.0.0.2:7933")
            .is_some());
        assert_eq!(
            response.checksum,
            node.state.lock().unwrap().memberlist.checksum()
        );
        let initial_reply_changes = response.changes.unwrap();
        assert_eq!(initial_reply_changes.len(), 1);
        assert_eq!(
            initial_reply_changes[0].source, "",
            "Go records its initial local delta without a source"
        );
        assert_eq!(node.dissemination_counts().get("127.0.0.1:7933"), Some(&1));
        // Drain the constructor's local delta exactly at Ringpop's 15-ping
        // propagation bound. The next checksum mismatch has no sender delta
        // to return and therefore exercises MembershipAsChanges full sync.
        for _ in 0..14 {
            node.handle_ping_with(&body, || 101, |_| 0).unwrap();
        }
        let full_sync_raw = node.handle_ping_with(&body, || 101, |_| 0).unwrap();
        let full_sync: Ping = serde_json::from_slice(&full_sync_raw).unwrap();
        let reply_changes = full_sync.changes.unwrap();
        assert_eq!(
            reply_changes.len(),
            2,
            "checksum mismatch with no sender deltas triggers full sync"
        );
        assert!(reply_changes
            .iter()
            .any(|c| c.address == "127.0.0.2:7933" && c.source == "127.0.0.1:7933"));
        assert!(reply_changes
            .iter()
            .any(|c| c.address == "127.0.0.1:7933" && c.status == ALIVE));
        assert_eq!(node.request_counts().ping, 16);
    }

    #[test]
    fn ping_app_errors_match_ringpop_and_malformed_json_is_rejected() {
        let node = node();
        node.set_ready(true);
        let unknown = serde_json::to_vec(&Ping {
            source: "remote".into(),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(
            handle_ping(&node.state, &unknown, || 1, |_| 0).unwrap_err(),
            RingpopError::Application("Pinged ringpop requires app name".into())
        );
        let wrong = serde_json::to_vec(&Ping {
            app: "other".into(),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(
            handle_ping(&node.state, &wrong, || 1, |_| 0).unwrap_err(),
            RingpopError::Application("Pinged ringpop has a different app name".into())
        );
        assert!(matches!(
            handle_ping(&node.state, b"{", || 1, |_| 0),
            Err(RingpopError::System(_))
        ));
        assert_eq!(node.request_counts().ping, 0);
    }

    #[test]
    fn original_go_handler_json_fixture_matches_join_and_ping_responses() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../tests/fixtures/ringpop_inbound_go.json"))
                .unwrap();
        let node = RingpopNode::new(
            "test",
            Member {
                address: "<local>".into(),
                status: ALIVE.into(),
                incarnation: 0,
                labels: None,
            },
            true,
        );
        let join = handle_join(
            &node.state,
            br#"{"app":"test","source":"127.0.0.2:7933","incarnationNumber":200}"#,
        )
        .unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&join).unwrap(),
            fixture["join"]
        );
        for (key, body) in [
            (
                "join_wrong_app",
                br#"{"app":"wrong","source":"127.0.0.2:7933"}"#.as_slice(),
            ),
            (
                "join_self",
                br#"{"app":"test","source":"<local>"}"#.as_slice(),
            ),
        ] {
            let error = handle_join(&node.state, body).unwrap_err();
            assert_eq!(
                error,
                RingpopError::Application(fixture[key].as_str().unwrap().to_owned())
            );
        }
        assert_eq!(
            handle_ping(&node.state, br#"{}"#, || 0, |_| 0).unwrap_err(),
            RingpopError::Application(fixture["ping_before_ready"].as_str().unwrap().to_owned())
        );
        node.set_ready(true);
        let ping = handle_ping(&node.state, br#"{"source":"127.0.0.2:7933","sourceIncarnationNumber":200,"checksum":0,"changes":null,"app":"test"}"#, || 0, |_| 0).unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&ping).unwrap(),
            fixture["ping"]
        );
        let wrong_ping = br#"{"app":"wrong","source":"127.0.0.2:7933"}"#;
        assert_eq!(
            handle_ping(&node.state, wrong_ping, || 0, |_| 0).unwrap_err(),
            RingpopError::Application(fixture["ping_wrong_app"].as_str().unwrap().to_owned())
        );
    }

    #[test]
    fn join_snapshot_preserves_member_labels_and_tombstone_wire_normalization() {
        let node = node();
        node.set_ready(true);
        let labels = BTreeMap::from([
            ("zone".to_owned(), "west".to_owned()),
            ("rack".to_owned(), "r2".to_owned()),
        ]);
        let alive = Ping {
            app: "cadence-matching".into(),
            source: "127.0.0.2:7933".into(),
            source_incarnation: 200,
            changes: Some(vec![Change {
                source: "127.0.0.2:7933".into(),
                source_incarnation: 200,
                address: "127.0.0.2:7933".into(),
                incarnation: 200,
                status: ALIVE.into(),
                labels: Some(labels.clone()),
                ..Default::default()
            }]),
            ..Default::default()
        };
        node.handle_ping_with(
            &serde_json::to_vec(&alive).unwrap(),
            || unreachable!(),
            |_| 0,
        )
        .unwrap();
        let tombstone = Ping {
            app: "cadence-matching".into(),
            source: "127.0.0.3:7933".into(),
            source_incarnation: 300,
            changes: Some(vec![Change {
                source: "127.0.0.3:7933".into(),
                source_incarnation: 300,
                address: "127.0.0.2:7933".into(),
                incarnation: 201,
                status: "faulty".into(),
                tombstone: true,
                labels: Some(labels.clone()),
                ..Default::default()
            }]),
            ..Default::default()
        };
        node.handle_ping_with(
            &serde_json::to_vec(&tombstone).unwrap(),
            || unreachable!(),
            |_| 0,
        )
        .unwrap();
        let raw = handle_join(
            &node.state,
            br#"{"app":"cadence-matching","source":"127.0.0.4:7933"}"#,
        )
        .unwrap();
        let response: JoinResponse = serde_json::from_slice(&raw).unwrap();
        let remote = response
            .membership
            .unwrap()
            .into_iter()
            .find(|change| change.address == "127.0.0.2:7933")
            .unwrap();
        assert_eq!(remote.status, "faulty");
        assert!(remote.tombstone);
        assert_eq!(remote.labels, Some(labels));
        assert_eq!(remote.source, "127.0.0.1:7933");
        assert_eq!(remote.source_incarnation, 100);
    }

    #[test]
    fn ping_local_refutation_uses_injected_incarnation_in_response_and_full_state() {
        let node = node();
        node.set_ready(true);
        let request = Ping {
            app: "cadence-matching".into(),
            source: "127.0.0.2:7933".into(),
            source_incarnation: 200,
            changes: Some(vec![Change {
                source: "127.0.0.2:7933".into(),
                source_incarnation: 200,
                address: "127.0.0.1:7933".into(),
                incarnation: 101,
                status: "suspect".into(),
                ..Default::default()
            }]),
            ..Default::default()
        };
        let raw = node
            .handle_ping_with(
                &serde_json::to_vec(&request).unwrap(),
                || 111,
                |_| unreachable!(),
            )
            .unwrap();
        let response: Ping = serde_json::from_slice(&raw).unwrap();
        assert_eq!(response.source_incarnation, 111);
        assert_eq!(
            node.state
                .lock()
                .unwrap()
                .memberlist
                .member("127.0.0.1:7933")
                .unwrap()
                .incarnation,
            111
        );
        assert_eq!(
            response.checksum,
            node.state.lock().unwrap().memberlist.checksum()
        );
    }

    #[tokio::test]
    async fn handler_dispatches_only_registered_go_method_names_and_stop_preserves_ready() {
        let node = node();
        node.set_ready(true);
        let handler = node.handler();
        let join = handler
            .call(
                "/protocol/join".into(),
                None,
                br#"{"app":"cadence-matching","source":"remote"}"#.to_vec(),
            )
            .await
            .unwrap();
        assert!(serde_json::from_slice::<JoinResponse>(&join.body).is_ok());
        for method in ["join", "ping"] {
            assert!(matches!(
                handler.call(method.into(), None, b"{}".to_vec()).await,
                Err(RingpopError::System(_))
            ));
        }
        node.stop();
        assert_eq!(node.lifecycle(), RingpopLifecycle::Stopped);
        assert!(node.is_ready(), "Go Stop leaves the Ready flag intact");
        let ping = br#"{"app":"cadence-matching","source":"remote"}"#;
        assert!(node.handle_ping_with(ping, || 1, |_| 0).is_ok());
        node.set_ready(false);
        assert_eq!(node.lifecycle(), RingpopLifecycle::Stopped);
        assert_eq!(
            node.handle_ping_with(ping, || 1, |_| 0).unwrap_err(),
            RingpopError::Application("node is not ready to handle requests".into())
        );
    }
}
