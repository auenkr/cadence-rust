//! Byte-preserving twin of `matching_sync`. Task data, source domain and
//! isolation groups are Go strings and stay `GoString` end to end: group
//! equality, the unknown-group fallback and partition-config rewriting never
//! go through `String` or U+FFFD replacement.
//!
//! Semantics intentionally mirror `matching_sync::SyncMatcher` line by line:
//! FIFO offers/pollers, empty task group matches every poller, a poll for an
//! unconfigured group falls back to the default ("") group, and a producer
//! succeeds only when the poller reports a successful History start.
pub use crate::matching_sync::{request_deadline, sync_match_budget};
use cadence_common::go_string::GoString;
use cadence_persistence::matching_native_task_store::AllocatedTaskBytes;
use std::{
    collections::{BTreeMap, HashSet, VecDeque},
    sync::Mutex,
    time::Duration,
};
use tokio::{
    sync::{oneshot, watch},
    time::Instant,
};
use tonic::Status;

pub struct NativeSyncTask {
    pub task: AllocatedTaskBytes,
    pub source_domain: GoString,
    /// Transient Go `InternalTask.forwardedFrom` state; never persisted.
    pub forwarded: bool,
    response: oneshot::Sender<Result<(), Status>>,
}
impl NativeSyncTask {
    pub fn finish(self, result: Result<(), Status>) {
        let _ = self.response.send(result);
    }
}
struct Poller {
    id: u64,
    group: GoString,
    response: oneshot::Sender<NativeSyncTask>,
}
struct Offer {
    id: u64,
    group: GoString,
    task: NativeSyncTask,
    accepted: oneshot::Sender<()>,
}
#[derive(Default)]
struct State {
    sequence: u64,
    pollers: VecDeque<Poller>,
    offers: VecDeque<Offer>,
}
pub struct NativeSyncMatcher {
    state: Mutex<State>,
    groups: HashSet<GoString>,
    stop: watch::Receiver<bool>,
}
struct Guard<'a> {
    matcher: &'a NativeSyncMatcher,
    id: u64,
    offer: bool,
}
impl Drop for Guard<'_> {
    fn drop(&mut self) {
        let mut state = self
            .matcher
            .state
            .lock()
            .expect("Matching native sync matcher lock");
        if self.offer {
            state.offers.retain(|offer| offer.id != self.id);
        } else {
            state.pollers.retain(|poller| poller.id != self.id);
        }
    }
}

/// Byte-keyed `task_isolation::dispatch_partition_config`.
fn dispatch_partition_config(
    config: Option<BTreeMap<GoString, GoString>>,
    group: &GoString,
) -> Option<BTreeMap<GoString, GoString>> {
    let map = config?;
    let key = GoString::from("isolation-group");
    let Some(original) = map.get(&key) else {
        return Some(map);
    };
    let original_key = GoString::from("original-isolation-group");
    let wf_key = GoString::from("wf-id");
    let original = map.get(&original_key).unwrap_or(original).clone();
    let wf = map.get(&wf_key).cloned().unwrap_or_default();
    Some(BTreeMap::from([
        (key, group.clone()),
        (original_key, original),
        (wf_key, wf),
    ]))
}

impl NativeSyncMatcher {
    pub fn new(groups: &[GoString], stop: watch::Receiver<bool>) -> Self {
        Self {
            state: Mutex::new(State::default()),
            groups: groups.iter().filter(|g| !g.is_empty()).cloned().collect(),
            stop,
        }
    }
    pub fn stop_signal(&self) -> watch::Receiver<bool> {
        self.stop.clone()
    }
    fn poll_group(&self, group: &GoString) -> GoString {
        if self.groups.contains(group) {
            group.clone()
        } else {
            GoString::default()
        }
    }
    fn compatible(task: &GoString, poller: &GoString) -> bool {
        task.is_empty() || task == poller
    }
    pub async fn poll(&self, group: &GoString) -> Option<NativeSyncTask> {
        let mut stop = self.stop.clone();
        if *stop.borrow() {
            return None;
        }
        let group = self.poll_group(group);
        let (id, receiver) = {
            let mut state = self
                .state
                .lock()
                .expect("Matching native sync matcher lock");
            if let Some(index) = state
                .offers
                .iter()
                .position(|offer| Self::compatible(&offer.group, &group))
            {
                let offer = state.offers.remove(index).expect("selected offer");
                let _ = offer.accepted.send(());
                return Some(offer.task);
            }
            state.sequence = state.sequence.wrapping_add(1);
            let id = state.sequence;
            let (response, receiver) = oneshot::channel();
            state.pollers.push_back(Poller {
                id,
                group,
                response,
            });
            (id, receiver)
        };
        let _guard = Guard {
            matcher: self,
            id,
            offer: false,
        };
        tokio::select! { result = receiver => result.ok(), _ = stop.changed() => None }
    }
    pub async fn offer(
        &self,
        mut task: AllocatedTaskBytes,
        group: &GoString,
        source_domain: &GoString,
        forwarded: bool,
        local_wait: Duration,
        deadline: Instant,
    ) -> bool {
        let mut stop = self.stop.clone();
        if *stop.borrow() || Instant::now() >= deadline {
            return false;
        }
        let group = self.poll_group(group);
        task.task_id = 0;
        task.data.partition_config =
            dispatch_partition_config(task.data.partition_config.take(), &group);
        let (response, result) = oneshot::channel();
        let mut task = Some(NativeSyncTask {
            task,
            source_domain: source_domain.clone(),
            forwarded,
            response,
        });
        let queued = {
            let mut state = self
                .state
                .lock()
                .expect("Matching native sync matcher lock");
            while let Some(index) = state
                .pollers
                .iter()
                .position(|poller| Self::compatible(&group, &poller.group))
            {
                let poller = state.pollers.remove(index).expect("selected poller");
                match poller.response.send(task.take().expect("unclaimed task")) {
                    Ok(()) => break,
                    Err(returned) => task = Some(returned),
                }
            }
            if let Some(task) = task.take() {
                if local_wait.is_zero() {
                    return false;
                }
                state.sequence = state.sequence.wrapping_add(1);
                let id = state.sequence;
                let (accepted, receiver) = oneshot::channel();
                state.offers.push_back(Offer {
                    id,
                    group: group.clone(),
                    task,
                    accepted,
                });
                Some((id, receiver))
            } else {
                None
            }
        };
        let Some(queued) = queued else {
            return Self::response(result, deadline).await;
        };
        let _guard = Guard {
            matcher: self,
            id: queued.0,
            offer: true,
        };
        let local_deadline = deadline.min(Instant::now() + local_wait);
        let accepted = tokio::select! {
            biased;
            result = queued.1 => result.is_ok(),
            _ = tokio::time::sleep_until(local_deadline) => {
                // Resolve a handoff at the timeout boundary under the poll
                // lock; never withdraw an already accepted task.
                let mut state = self.state.lock().expect("Matching native sync matcher lock");
                if let Some(index) = state.offers.iter().position(|offer| offer.id == queued.0) { state.offers.remove(index); false } else { true }
            }
            _ = stop.changed() => {
                // A poller may already have removed the offer before shutdown.
                // Resolve ownership under the same lock used by poll().
                let mut state = self.state.lock().expect("Matching native sync matcher lock");
                if let Some(index) = state.offers.iter().position(|offer| offer.id == queued.0) { state.offers.remove(index); false } else { true }
            },
        };
        accepted && Self::response(result, deadline).await
    }
    async fn response(
        result: oneshot::Receiver<Result<(), Status>>,
        deadline: Instant,
    ) -> bool {
        tokio::select! {
            result = result => matches!(result, Ok(Ok(()))),
            _ = tokio::time::sleep_until(deadline) => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cadence_persistence::matching_blob::TaskDataBytes;
    use std::sync::Arc;

    const FF: &[u8] = &[0xff];
    const FE: &[u8] = &[0xfe];
    const REPLACEMENT: &str = "\u{fffd}";

    fn gs(bytes: &[u8]) -> GoString {
        GoString::from(bytes)
    }
    fn fixture(groups: &[&[u8]]) -> (watch::Sender<bool>, Arc<NativeSyncMatcher>) {
        let (stop, stopped) = watch::channel(false);
        let groups: Vec<GoString> = groups.iter().map(|g| gs(g)).collect();
        (stop, Arc::new(NativeSyncMatcher::new(&groups, stopped)))
    }
    fn task(workflow: &[u8], group: &[u8]) -> AllocatedTaskBytes {
        AllocatedTaskBytes {
            task_id: 77,
            data: TaskDataBytes {
                workflow_id: gs(workflow),
                run_id: None,
                raw_run_id: None,
                schedule_id: 3,
                expiry_nanos: 0,
                created_nanos: 0,
                partition_config: Some(BTreeMap::from([(gs(b"isolation-group"), gs(group))])),
            },
        }
    }
    fn spawn_offer(
        matcher: &Arc<NativeSyncMatcher>,
        task: AllocatedTaskBytes,
        group: &[u8],
        source: &[u8],
        wait: Duration,
    ) -> tokio::task::JoinHandle<bool> {
        let matcher = matcher.clone();
        let (group, source) = (gs(group), gs(source));
        tokio::spawn(async move {
            matcher
                .offer(
                    task,
                    &group,
                    &source,
                    false,
                    wait,
                    Instant::now() + Duration::from_secs(2),
                )
                .await
        })
    }

    #[tokio::test]
    async fn raw_task_and_domain_bytes_are_delivered_and_outcome_is_acknowledged() {
        let (_stop, matcher) = fixture(&[b"a"]);
        let workflow: &[u8] = &[b'w', 0xff, 0xc3];
        let source: &[u8] = &[0xfe, b'd'];
        let add = spawn_offer(
            &matcher,
            task(workflow, b"a"),
            b"a",
            source,
            Duration::from_secs(1),
        );
        let received = matcher.poll(&gs(b"a")).await.unwrap();
        assert_eq!(received.task.task_id, 0);
        assert_eq!(received.task.data.workflow_id.as_bytes(), workflow);
        assert_eq!(received.source_domain.as_bytes(), source);
        assert!(!received.forwarded);
        assert!(!add.is_finished(), "handoff alone is not an ack");
        let config = received.task.data.partition_config.clone().unwrap();
        assert_eq!(config[&gs(b"original-isolation-group")], gs(b"a"));
        received.finish(Ok(()));
        assert!(add.await.unwrap());
    }

    #[tokio::test]
    async fn claimed_forwarded_offer_waits_for_response_after_shutdown() {
        let (stop, matcher) = fixture(&[]);
        let add = spawn_offer(&matcher, task(b"w", b""), b"", b"d", Duration::from_secs(1));
        tokio::task::yield_now().await;
        let received = matcher.poll(&gs(b"")).await.unwrap();
        stop.send(true).unwrap();
        tokio::task::yield_now().await;
        assert!(!add.is_finished());
        received.finish(Ok(()));
        assert!(tokio::time::timeout(Duration::from_secs(1), add).await.unwrap().unwrap());
    }

    #[tokio::test]
    async fn history_failure_and_dropped_task_are_not_accepted() {
        let (_stop, matcher) = fixture(&[b"a"]);
        let add = spawn_offer(
            &matcher,
            task(b"w", b"a"),
            b"a",
            b"d",
            Duration::from_secs(1),
        );
        matcher
            .poll(&gs(b"a"))
            .await
            .unwrap()
            .finish(Err(Status::unavailable("history failed")));
        assert!(!add.await.unwrap());
        let add = spawn_offer(
            &matcher,
            task(b"w", b"a"),
            b"a",
            b"d",
            Duration::from_secs(1),
        );
        drop(matcher.poll(&gs(b"a")).await.unwrap());
        assert!(!add.await.unwrap());
    }

    #[tokio::test(start_paused = true)]
    async fn invalid_utf8_groups_ff_fe_and_replacement_are_distinct() {
        let replacement = REPLACEMENT.as_bytes();
        let (_stop, matcher) = fixture(&[FF, FE, replacement]);
        // Offer for ff must not reach pollers for fe or U+FFFD.
        for other in [FE, replacement] {
            let poller = matcher.clone();
            let other = gs(other);
            let poll =
                tokio::spawn(async move { poller.poll(&other).await.map(|t| t.finish(Ok(()))) });
            tokio::task::yield_now().await;
            assert!(
                !matcher
                    .offer(
                        task(b"w", FF),
                        &gs(FF),
                        &gs(b"d"),
                        false,
                        Duration::from_millis(50),
                        Instant::now() + Duration::from_millis(50)
                    )
                    .await
            );
            poll.abort();
        }
        // Same bytes do match, and the rewritten config keeps raw bytes.
        let add = spawn_offer(&matcher, task(b"w", FF), FF, b"d", Duration::from_secs(1));
        let received = matcher.poll(&gs(FF)).await.unwrap();
        let config = received.task.data.partition_config.clone().unwrap();
        assert_eq!(config[&gs(b"isolation-group")].as_bytes(), FF);
        assert_eq!(config[&gs(b"original-isolation-group")].as_bytes(), FF);
        received.finish(Ok(()));
        assert!(add.await.unwrap());
    }

    #[tokio::test(start_paused = true)]
    async fn unknown_group_falls_back_to_default_group() {
        let (_stop, matcher) = fixture(&[b"a"]);
        let poller = matcher.clone();
        let poll =
            tokio::spawn(async move { poller.poll(&gs(FF)).await.map(|t| t.finish(Ok(()))) });
        tokio::task::yield_now().await;
        assert!(
            !matcher
                .offer(
                    task(b"w", b"a"),
                    &gs(b"a"),
                    &gs(b"d"),
                    false,
                    Duration::from_millis(100),
                    Instant::now() + Duration::from_millis(100)
                )
                .await,
            "isolated offer is not taken by unknown-group poll"
        );
        // Unknown offer group is treated as default and rewritten to "".
        let add = spawn_offer(&matcher, task(b"w", FE), FE, b"d", Duration::from_secs(1));
        let sent = tokio::time::timeout(Duration::from_secs(1), poll).await;
        assert!(sent.unwrap().unwrap().is_some());
        assert!(add.await.unwrap());
    }

    #[tokio::test]
    async fn default_group_offer_reaches_any_poller_with_empty_group() {
        let (_stop, matcher) = fixture(&[b"a"]);
        let add = spawn_offer(&matcher, task(b"w", b""), b"", b"d", Duration::from_secs(1));
        tokio::task::yield_now().await;
        let received = matcher.poll(&gs(b"a")).await.unwrap();
        let config = received.task.data.partition_config.clone().unwrap();
        assert_eq!(config[&gs(b"isolation-group")].as_bytes(), b"");
        received.finish(Ok(()));
        assert!(add.await.unwrap());
    }

    #[tokio::test]
    async fn forwarded_bit_is_transient_and_preserved() {
        let (_stop, matcher) = fixture(&[b"a"]);
        let producer = matcher.clone();
        let add = tokio::spawn(async move {
            producer
                .offer(
                    task(b"w", b"a"),
                    &gs(b"a"),
                    &gs(b"d"),
                    true,
                    Duration::from_secs(1),
                    Instant::now() + Duration::from_secs(2),
                )
                .await
        });
        let received = matcher.poll(&gs(b"a")).await.unwrap();
        assert!(received.forwarded);
        received.finish(Ok(()));
        assert!(add.await.unwrap());
    }

    #[tokio::test]
    async fn fifo_offers_are_delivered_in_order() {
        let (_stop, matcher) = fixture(&[]);
        let first = spawn_offer(
            &matcher,
            task(b"one", b""),
            b"",
            b"d",
            Duration::from_secs(1),
        );
        tokio::task::yield_now().await;
        let second = spawn_offer(
            &matcher,
            task(b"two", b""),
            b"",
            b"d",
            Duration::from_secs(1),
        );
        tokio::task::yield_now().await;
        for expected in [&b"one"[..], b"two"] {
            let received = matcher.poll(&gs(b"")).await.unwrap();
            assert_eq!(received.task.data.workflow_id.as_bytes(), expected);
            received.finish(Ok(()));
        }
        assert!(first.await.unwrap() && second.await.unwrap());
    }

    #[tokio::test(start_paused = true)]
    async fn unaccepted_offers_and_aborted_polls_are_withdrawn() {
        let (stop, matcher) = fixture(&[b"a"]);
        assert!(
            !matcher
                .offer(
                    task(b"w", b"a"),
                    &gs(b"a"),
                    &gs(b"d"),
                    false,
                    Duration::from_millis(1),
                    Instant::now() + Duration::from_secs(1)
                )
                .await
        );
        assert_eq!(matcher.state.lock().unwrap().offers.len(), 0);
        let poller = matcher.clone();
        let poll = tokio::spawn(async move { poller.poll(&gs(b"a")).await });
        tokio::task::yield_now().await;
        poll.abort();
        let _ = poll.await;
        assert_eq!(matcher.state.lock().unwrap().pollers.len(), 0);
        // Zero wait without a poller is neither matched nor queued.
        assert!(
            !matcher
                .offer(
                    task(b"w", b"a"),
                    &gs(b"a"),
                    &gs(b"d"),
                    false,
                    Duration::ZERO,
                    Instant::now() + Duration::from_secs(1)
                )
                .await
        );
        assert_eq!(matcher.state.lock().unwrap().offers.len(), 0);
        // Cancelling the offer future itself removes the queued offer.
        let add = spawn_offer(
            &matcher,
            task(b"w", b"a"),
            b"a",
            b"d",
            Duration::from_secs(1),
        );
        tokio::task::yield_now().await;
        add.abort();
        let _ = add.await;
        assert_eq!(matcher.state.lock().unwrap().offers.len(), 0);
        stop.send_replace(true);
        assert!(matcher.poll(&gs(b"a")).await.is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn late_history_outcome_after_deadline_is_not_accepted() {
        let (_stop, matcher) = fixture(&[]);
        let poller = matcher.clone();
        let poll = tokio::spawn(async move {
            if let Some(received) = poller.poll(&gs(b"")).await {
                tokio::time::sleep(Duration::from_millis(500)).await;
                received.finish(Ok(()));
            }
        });
        tokio::task::yield_now().await;
        assert!(
            !matcher
                .offer(
                    task(b"w", b""),
                    &gs(b""),
                    &gs(b"d"),
                    false,
                    Duration::from_millis(100),
                    Instant::now() + Duration::from_millis(100)
                )
                .await
        );
        poll.await.unwrap();
    }
}
