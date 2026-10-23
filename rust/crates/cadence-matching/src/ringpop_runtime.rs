//! Owned Ringpop SWIM probe loop adapter.

use crate::ringpop_provider::RingpopNode;
use crate::tchannel_json_client::JsonCallError;
use cadence_common::ringpop_wire::Ping;
use std::{future::Future, pin::Pin, time::Duration};

pub type ProbeFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Injectable transport boundary for deterministic SWIM probe traces.
pub trait ProbeTransport: Send + Sync {
    fn direct_ping<'a>(
        &'a self,
        node: &'a RingpopNode,
        target: &'a str,
        timeout: Duration,
    ) -> ProbeFuture<'a, Result<Ping, JsonCallError>>;

    fn indirect_ping<'a>(
        &'a self,
        node: &'a RingpopNode,
        target: &'a str,
        amount: usize,
        timeout: Duration,
    ) -> ProbeFuture<'a, (bool, Vec<JsonCallError>)>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProbeOutcome {
    NoPingableMember,
    DirectSuccess {
        target: String,
    },
    IndirectSuccess {
        target: String,
        prior_errors: usize,
    },
    Inconclusive {
        target: String,
        errors: usize,
    },
    Suspect {
        target: String,
        incarnation: i64,
        errors: usize,
    },
}

pub struct TChannelProbeTransport;

impl ProbeTransport for TChannelProbeTransport {
    fn direct_ping<'a>(
        &'a self,
        node: &'a RingpopNode,
        target: &'a str,
        timeout: Duration,
    ) -> ProbeFuture<'a, Result<Ping, JsonCallError>> {
        Box::pin(async move { node.ping(target, timeout).await })
    }

    fn indirect_ping<'a>(
        &'a self,
        node: &'a RingpopNode,
        target: &'a str,
        amount: usize,
        timeout: Duration,
    ) -> ProbeFuture<'a, (bool, Vec<JsonCallError>)> {
        Box::pin(async move { node.indirect_ping(target, amount, timeout).await })
    }
}

/// Run exactly one sequential Go `pingNextMember` round. Selection randomness
/// is injected by the caller; the transport can be a real TChannel adapter or
/// a deterministic differential-test oracle.
pub async fn ping_next_member_with<R>(
    node: &RingpopNode,
    transport: &dyn ProbeTransport,
    mut random_below: R,
) -> ProbeOutcome
where
    R: FnMut(usize) -> usize,
{
    let Some(member) = node.next_pingable_member_with(&mut random_below) else {
        return ProbeOutcome::NoPingableMember;
    };
    let timeout = node.ping_timeout();
    if transport
        .direct_ping(node, &member.address, timeout)
        .await
        .is_ok()
    {
        return ProbeOutcome::DirectSuccess {
            target: member.address,
        };
    }

    let (reached, errors) = transport
        .indirect_ping(
            node,
            &member.address,
            node.ping_request_size(),
            node.ping_request_timeout(),
        )
        .await;
    let error_count = errors.len();
    if error_count == node.ping_request_size() {
        return ProbeOutcome::Inconclusive {
            target: member.address,
            errors: error_count,
        };
    }
    if reached {
        return ProbeOutcome::IndirectSuccess {
            target: member.address,
            prior_errors: error_count,
        };
    }

    node.make_suspect(&member.address, member.incarnation);
    ProbeOutcome::Suspect {
        target: member.address,
        incarnation: member.incarnation,
        errors: error_count,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cadence_common::ringpop_wire::{Change, Member, Ping, ALIVE};
    use std::sync::{Arc, Mutex};
    use tokio::sync::Notify;

    fn go_oracle() -> serde_json::Value {
        serde_json::from_str(include_str!(
            "../../../../tests/fixtures/ringpop_swim_runtime_oracle.json"
        ))
        .unwrap()
    }

    struct ScriptedTransport {
        direct_ok: bool,
        reached: bool,
        errors: usize,
        calls: Arc<Mutex<Vec<String>>>,
    }

    impl ProbeTransport for ScriptedTransport {
        fn direct_ping<'a>(
            &'a self,
            _node: &'a RingpopNode,
            target: &'a str,
            _timeout: Duration,
        ) -> ProbeFuture<'a, Result<Ping, JsonCallError>> {
            let calls = Arc::clone(&self.calls);
            let target = target.to_owned();
            let direct_ok = self.direct_ok;
            Box::pin(async move {
                calls.lock().unwrap().push(format!("direct:{target}"));
                if direct_ok {
                    Ok(Ping::default())
                } else {
                    Err(JsonCallError::Transport(anyhow::anyhow!("direct failed")))
                }
            })
        }

        fn indirect_ping<'a>(
            &'a self,
            _node: &'a RingpopNode,
            target: &'a str,
            amount: usize,
            _timeout: Duration,
        ) -> ProbeFuture<'a, (bool, Vec<JsonCallError>)> {
            let calls = Arc::clone(&self.calls);
            let target = target.to_owned();
            let reached = self.reached;
            let errors = self.errors;
            Box::pin(async move {
                calls
                    .lock()
                    .unwrap()
                    .push(format!("indirect:{target}:{amount}"));
                (
                    reached,
                    (0..errors)
                        .map(|_| JsonCallError::Transport(anyhow::anyhow!("helper failed")))
                        .collect(),
                )
            })
        }
    }

    fn node_with_members(remotes: &[&str], configured_helpers: usize) -> RingpopNode {
        let node = RingpopNode::new_with_swim_config(
            "test",
            Member {
                address: "local:7933".into(),
                status: ALIVE.into(),
                incarnation: 10,
                labels: None,
            },
            false,
            Duration::from_millis(1),
            Duration::from_millis(1),
            configured_helpers,
            Duration::from_millis(1),
            cadence_common::ringpop_state_transitions::StateTimeouts {
                suspect_ns: 5,
                faulty_ns: 10,
                tombstone_ns: 20,
            },
        );
        node.set_ready(true);
        let request = Ping {
            source: "seed:7933".into(),
            source_incarnation: 1,
            app: "test".into(),
            changes: Some(
                remotes
                    .iter()
                    .map(|address| Change {
                        source: "seed:7933".into(),
                        source_incarnation: 1,
                        address: (*address).into(),
                        incarnation: 1,
                        status: ALIVE.into(),
                        ..Default::default()
                    })
                    .collect(),
            ),
            ..Default::default()
        };
        node.handle_ping_with(&serde_json::to_vec(&request).unwrap(), || 11, |len| len - 1)
            .unwrap();
        node
    }

    fn transport(direct_ok: bool, reached: bool, errors: usize) -> ScriptedTransport {
        ScriptedTransport {
            direct_ok,
            reached,
            errors,
            calls: Arc::new(Mutex::new(Vec::new())),
        }
    }

    struct BlockingTransport {
        entered: Arc<Notify>,
        release: Arc<Notify>,
    }

    impl ProbeTransport for BlockingTransport {
        fn direct_ping<'a>(
            &'a self,
            _node: &'a RingpopNode,
            _target: &'a str,
            _timeout: Duration,
        ) -> ProbeFuture<'a, Result<Ping, JsonCallError>> {
            let entered = Arc::clone(&self.entered);
            let release = Arc::clone(&self.release);
            Box::pin(async move {
                entered.notify_one();
                release.notified().await;
                Ok(Ping::default())
            })
        }

        fn indirect_ping<'a>(
            &'a self,
            _node: &'a RingpopNode,
            _target: &'a str,
            _amount: usize,
            _timeout: Duration,
        ) -> ProbeFuture<'a, (bool, Vec<JsonCallError>)> {
            Box::pin(async { (false, Vec::new()) })
        }
    }

    #[tokio::test]
    async fn direct_success_does_not_launch_indirect_helpers() {
        let node = node_with_members(&["target:7933"], 3);
        let transport = transport(true, false, 0);
        let result = ping_next_member_with(&node, &transport, |_| 0).await;
        assert_eq!(
            result,
            ProbeOutcome::DirectSuccess {
                target: "target:7933".into()
            }
        );
        assert_eq!(go_oracle()["directSuccess"], "alive");
        assert_eq!(*transport.calls.lock().unwrap(), ["direct:target:7933"]);
    }

    #[tokio::test]
    async fn stop_and_wait_finishes_inflight_probe_before_returning() {
        let node = node_with_members(&["target:7933", "due:7933"], 1);
        let transport = Arc::new(BlockingTransport {
            entered: Arc::new(Notify::new()),
            release: Arc::new(Notify::new()),
        });
        let entered = Arc::clone(&transport.entered);
        let release = Arc::clone(&transport.release);
        node.start_swim_with_transport(transport).unwrap();

        tokio::time::timeout(Duration::from_secs(1), entered.notified())
            .await
            .expect("the SWIM loop should begin a direct probe");
        // Create a due transition after the probe is already blocked. Go
        // Node.Stop waits for gossip.Stop before stopping this timer loop.
        node.make_suspect("due:7933", 1);
        let stopping_node = node.clone();
        let stop = tokio::spawn(async move { stopping_node.stop_and_wait().await });
        tokio::task::yield_now().await;
        assert!(
            !stop.is_finished(),
            "stop must wait for the in-flight probe"
        );
        assert_eq!(node.start_swim(), Err("SWIM shutdown in progress"));
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if node
                    .membership_snapshot()
                    .0
                    .iter()
                    .any(|member| member.address == "due:7933" && member.status == "faulty")
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("transition timers must remain active while probe shutdown waits");
        let concurrent_stopping_node = node.clone();
        let concurrent_stop =
            tokio::spawn(async move { concurrent_stopping_node.stop_and_wait().await });
        tokio::task::yield_now().await;
        assert!(
            !concurrent_stop.is_finished(),
            "concurrent stop callers must share shutdown completion"
        );
        release.notify_one();
        tokio::time::timeout(Duration::from_secs(1), stop)
            .await
            .expect("stop should complete after the probe returns")
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), concurrent_stop)
            .await
            .expect("concurrent stop should observe the same completion")
            .unwrap();
        assert!(node.is_ready(), "SWIM stop must preserve readiness");
        assert_eq!(
            node.lifecycle(),
            crate::ringpop_provider::RingpopLifecycle::Stopped
        );
        node.start_swim().unwrap();
        node.destroy_and_wait().await;
        assert!(node.is_destroyed());
    }

    #[tokio::test]
    async fn stop_and_wait_is_cancel_safe_and_stop_before_start_disables_transitions() {
        let never_started = node_with_members(&["target:7933"], 1);
        never_started.stop_and_wait().await;
        assert_eq!(
            never_started.lifecycle(),
            crate::ringpop_provider::RingpopLifecycle::Stopped
        );

        let node = node_with_members(&["target:7933"], 1);
        let transport = Arc::new(BlockingTransport {
            entered: Arc::new(Notify::new()),
            release: Arc::new(Notify::new()),
        });
        let entered = Arc::clone(&transport.entered);
        let release = Arc::clone(&transport.release);
        node.start_swim_with_transport(transport).unwrap();
        tokio::time::timeout(Duration::from_secs(1), entered.notified())
            .await
            .expect("probe should enter before shutdown");

        let first_node = node.clone();
        let first_stop = tokio::spawn(async move { first_node.stop_and_wait().await });
        tokio::task::yield_now().await;
        first_stop.abort();
        release.notify_one();

        tokio::time::timeout(Duration::from_secs(1), node.clone().stop_and_wait())
            .await
            .expect("cleanup must continue after the initiating waiter is cancelled");
        assert_eq!(
            node.lifecycle(),
            crate::ringpop_provider::RingpopLifecycle::Stopped
        );
        node.start_swim()
            .expect("completed shutdown permits restart");
        node.stop_and_wait().await;
    }

    #[tokio::test]
    async fn inconclusive_guard_uses_configured_size_not_sparse_helper_pool() {
        let sparse = node_with_members(&["target:7933", "helper:7933"], 3);
        let one_error = transport(false, false, 1);
        assert!(matches!(
            ping_next_member_with(&sparse, &one_error, |_| 0).await,
            ProbeOutcome::Suspect {
                target,
                incarnation: 1,
                errors: 1,
            } if target == "target:7933"
        ));
        assert_eq!(
            sparse
                .membership_snapshot()
                .0
                .iter()
                .find(|member| member.address == "target:7933")
                .unwrap()
                .status,
            cadence_common::ringpop_wire::SUSPECT
        );
        assert_eq!(go_oracle()["sparseHelpers"], "suspect");

        let full = node_with_members(
            &[
                "target:7933",
                "helper-1:7933",
                "helper-2:7933",
                "helper-3:7933",
            ],
            3,
        );
        let all_errors = transport(false, false, 3);
        assert!(matches!(
            ping_next_member_with(&full, &all_errors, |_| 0).await,
            ProbeOutcome::Inconclusive { errors: 3, .. }
        ));
        assert_eq!(go_oracle()["allErrors"], "alive");
    }

    #[tokio::test]
    async fn empty_indirect_pool_falls_through_to_suspect_for_nonzero_configuration() {
        let node = node_with_members(&["target:7933"], 3);
        let no_helpers = transport(false, false, 0);
        assert!(matches!(
            ping_next_member_with(&node, &no_helpers, |_| 0).await,
            ProbeOutcome::Suspect { errors: 0, .. }
        ));
        assert_eq!(go_oracle()["noHelpers"], "suspect");
    }

    #[tokio::test]
    async fn indirect_positive_after_prior_helper_error_keeps_member_alive() {
        let node = node_with_members(&["target:7933"], 3);
        let reached = transport(false, true, 1);
        assert_eq!(
            ping_next_member_with(&node, &reached, |_| 0).await,
            ProbeOutcome::IndirectSuccess {
                target: "target:7933".into(),
                prior_errors: 1,
            }
        );
        assert_eq!(
            node.membership_snapshot()
                .0
                .iter()
                .find(|member| member.address == "target:7933")
                .unwrap()
                .status,
            ALIVE
        );
    }
}
