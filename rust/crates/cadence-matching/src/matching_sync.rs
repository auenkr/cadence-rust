//! Ephemeral, cancellation-safe synchronous task matching. A producer succeeds
//! only when the poller reports History's start outcome, never merely on handoff.
use cadence_persistence::{matching_blob::TaskData, matching_sqlite::AllocatedTask};
use std::{
    collections::{HashSet, VecDeque},
    sync::Mutex,
    time::Duration,
};
use tokio::{
    sync::{oneshot, watch},
    time::Instant,
};
use tonic::Status;

pub struct SyncTask {
    pub task: AllocatedTask,
    pub source_domain: String,
    /// Transient Go `InternalTask.forwardedFrom` state. Go does not persist
    /// this bit with TaskData: it exists only for a successful sync handoff.
    pub forwarded: bool,
    response: oneshot::Sender<Result<(), Status>>,
}
impl SyncTask {
    pub fn finish(self, result: Result<(), Status>) {
        let _ = self.response.send(result);
    }
}
struct Poller {
    id: u64,
    group: String,
    response: oneshot::Sender<SyncTask>,
}
struct Offer {
    id: u64,
    group: String,
    task: SyncTask,
    accepted: oneshot::Sender<()>,
}
#[derive(Default)]
struct State {
    sequence: u64,
    pollers: VecDeque<Poller>,
    offers: VecDeque<Offer>,
}
pub struct SyncMatcher {
    state: Mutex<State>,
    groups: HashSet<String>,
    stop: watch::Receiver<bool>,
}
struct Guard<'a> {
    matcher: &'a SyncMatcher,
    id: u64,
    offer: bool,
}
impl Drop for Guard<'_> {
    fn drop(&mut self) {
        let mut state = self
            .matcher
            .state
            .lock()
            .expect("Matching sync matcher lock");
        if self.offer {
            state.offers.retain(|offer| offer.id != self.id);
        } else {
            state.pollers.retain(|poller| poller.id != self.id);
        }
    }
}
impl SyncMatcher {
    pub fn new(groups: &[String], stop: watch::Receiver<bool>) -> Self {
        Self {
            state: Mutex::new(State::default()),
            groups: groups.iter().filter(|g| !g.is_empty()).cloned().collect(),
            stop,
        }
    }
    pub fn stop_signal(&self) -> watch::Receiver<bool> {
        self.stop.clone()
    }
    fn poll_group<'a>(&self, group: &'a str) -> &'a str {
        if self.groups.contains(group) {
            group
        } else {
            ""
        }
    }
    fn compatible(task: &str, poller: &str) -> bool {
        task.is_empty() || task == poller
    }
    pub async fn poll(&self, group: &str) -> Option<SyncTask> {
        let mut stop = self.stop.clone();
        if *stop.borrow() {
            return None;
        }
        let group = self.poll_group(group);
        let (id, receiver) = {
            let mut state = self.state.lock().expect("Matching sync matcher lock");
            if let Some(index) = state
                .offers
                .iter()
                .position(|offer| Self::compatible(&offer.group, group))
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
                group: group.into(),
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
        mut data: TaskData,
        group: &str,
        source_domain: &str,
        forwarded: bool,
        local_wait: Duration,
        deadline: Instant,
    ) -> bool {
        let mut stop = self.stop.clone();
        if *stop.borrow() || Instant::now() >= deadline {
            return false;
        }
        let group = self.poll_group(group);
        data.partition_config = cadence_common::task_isolation::dispatch_partition_config(
            &data.partition_config,
            group,
        );
        let (response, result) = oneshot::channel();
        let mut task = Some(SyncTask {
            task: AllocatedTask { task_id: 0, data },
            source_domain: source_domain.into(),
            forwarded,
            response,
        });
        let queued = {
            let mut state = self.state.lock().expect("Matching sync matcher lock");
            while let Some(index) = state
                .pollers
                .iter()
                .position(|poller| Self::compatible(group, &poller.group))
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
                    group: group.into(),
                    task,
                    accepted,
                });
                Some((id, receiver))
            } else {
                None
            }
        };
        let Some(queued) = queued else {
            return Self::response(result, stop, deadline).await;
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
                // Resolve a handoff at the timeout boundary while holding the
                // same lock as poll. Never withdraw a task already accepted.
                let mut state = self.state.lock().expect("Matching sync matcher lock");
                if let Some(index) = state.offers.iter().position(|offer| offer.id == queued.0) { state.offers.remove(index); false } else { true }
            }
            _ = stop.changed() => false,
        };
        accepted && Self::response(result, stop, deadline).await
    }
    async fn response(
        result: oneshot::Receiver<Result<(), Status>>,
        mut stop: watch::Receiver<bool>,
        deadline: Instant,
    ) -> bool {
        tokio::select! {
            result = result => matches!(result, Ok(Ok(()))),
            _ = tokio::time::sleep_until(deadline) => false,
            _ = stop.changed() => false,
        }
    }
}

/// Go newChildContext(ctx, 200ms, 1s): leave parent time for persistence.
pub fn sync_match_budget(parent_remaining: Option<Duration>) -> Duration {
    let maximum = Duration::from_millis(200);
    parent_remaining
        .map(|remaining| {
            remaining
                .saturating_sub(Duration::from_secs(1))
                .min(maximum)
        })
        .unwrap_or(maximum)
}

/// Preserve the incoming gRPC timeout before consuming its Request. Tonic's
/// request metadata retains the grpc-timeout header for application budgets.
pub fn request_deadline<T>(request: &tonic::Request<T>) -> Option<Instant> {
    let text = request.metadata().get("grpc-timeout")?.to_str().ok()?;
    let unit = text.as_bytes().last().copied()?;
    let amount = text
        .get(..text.len().checked_sub(1)?)?
        .parse::<u64>()
        .ok()?;
    let duration = match unit {
        b'H' => Duration::from_secs(amount.checked_mul(3600)?),
        b'M' => Duration::from_secs(amount.checked_mul(60)?),
        b'S' => Duration::from_secs(amount),
        b'm' => Duration::from_millis(amount),
        b'u' => Duration::from_micros(amount),
        b'n' => Duration::from_nanos(amount),
        _ => return None,
    };
    Instant::now().checked_add(duration)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    fn fixture() -> (watch::Sender<bool>, Arc<SyncMatcher>) {
        let (stop, stopped) = watch::channel(false);
        (
            stop,
            Arc::new(SyncMatcher::new(&["a".into(), "b".into()], stopped)),
        )
    }
    fn task() -> TaskData {
        TaskData {
            workflow_id: "workflow".into(),
            partition_config: Some(std::collections::BTreeMap::from([(
                "isolation-group".into(),
                "a".into(),
            )])),
            ..Default::default()
        }
    }
    #[test]
    fn sync_match_child_context_preserves_parent_tailroom() {
        assert_eq!(sync_match_budget(None), Duration::from_millis(200));
        assert_eq!(
            sync_match_budget(Some(Duration::from_secs(5))),
            Duration::from_millis(200)
        );
        assert_eq!(
            sync_match_budget(Some(Duration::from_millis(1100))),
            Duration::from_millis(100)
        );
        assert_eq!(
            sync_match_budget(Some(Duration::from_secs(1))),
            Duration::ZERO
        );
        assert_eq!(
            sync_match_budget(Some(Duration::from_millis(10))),
            Duration::ZERO
        );
        let mut request = tonic::Request::new(());
        assert!(request_deadline(&request).is_none());
        request.set_timeout(Duration::from_secs(2));
        let remaining = request_deadline(&request)
            .unwrap()
            .saturating_duration_since(Instant::now());
        assert!(remaining > Duration::from_millis(1900) && remaining <= Duration::from_secs(2));
    }
    #[tokio::test]
    async fn sync_match_waits_for_history_completion_and_preserves_original_metadata() {
        let (_stop, matcher) = fixture();
        let original = task();
        let offered = original.clone();
        let producer = matcher.clone();
        let add = tokio::spawn(async move {
            producer
                .offer(
                    offered,
                    "a",
                    "00000000-0000-0000-0000-000000000001",
                    false,
                    Duration::from_secs(1),
                    Instant::now() + Duration::from_secs(2),
                )
                .await
        });
        let received = matcher.poll("a").await.unwrap();
        assert_eq!(received.task.task_id, 0);
        assert!(
            !add.is_finished(),
            "handoff alone does not acknowledge AddTask"
        );
        let config = received.task.data.partition_config.as_ref().unwrap();
        assert_eq!(config["original-isolation-group"], "a");
        assert_eq!(original.partition_config.unwrap().len(), 1);
        received.finish(Ok(()));
        assert!(add.await.unwrap());
    }
    #[tokio::test]
    async fn sync_match_preserves_only_the_transient_forwarded_request_signal() {
        let (_stop, matcher) = fixture();
        let producer = matcher.clone();
        let add = tokio::spawn(async move {
            producer
                .offer(
                    task(),
                    "a",
                    "00000000-0000-0000-0000-000000000001",
                    true,
                    Duration::from_secs(1),
                    Instant::now() + Duration::from_secs(2),
                )
                .await
        });
        let received = matcher.poll("a").await.unwrap();
        assert!(received.forwarded);
        received.finish(Ok(()));
        assert!(add.await.unwrap());
    }
    #[tokio::test]
    async fn sync_match_does_not_deliver_to_other_or_unknown_groups() {
        let (_stop, matcher) = fixture();
        for group in ["b", "unknown", ""] {
            let producer = matcher.clone();
            let add = tokio::spawn(async move {
                producer
                    .offer(
                        task(),
                        "a",
                        "00000000-0000-0000-0000-000000000001",
                        false,
                        Duration::from_millis(10),
                        Instant::now() + Duration::from_secs(1),
                    )
                    .await
            });
            assert!(
                tokio::time::timeout(Duration::from_millis(20), matcher.poll(group))
                    .await
                    .is_err()
            );
            assert!(!add.await.unwrap());
        }
        let producer = matcher.clone();
        let add = tokio::spawn(async move {
            producer
                .offer(
                    task(),
                    "",
                    "00000000-0000-0000-0000-000000000001",
                    false,
                    Duration::from_secs(1),
                    Instant::now() + Duration::from_secs(2),
                )
                .await
        });
        let received = matcher.poll("unknown").await.unwrap();
        assert_eq!(
            received.task.data.partition_config.as_ref().unwrap()["isolation-group"],
            ""
        );
        received.finish(Ok(()));
        assert!(add.await.unwrap());
    }
    #[tokio::test]
    async fn sync_match_cancellation_and_history_failure_allow_persistence_fallback() {
        let (stop, matcher) = fixture();
        let producer = matcher.clone();
        let add = tokio::spawn(async move {
            producer
                .offer(
                    task(),
                    "a",
                    "00000000-0000-0000-0000-000000000001",
                    false,
                    Duration::from_secs(1),
                    Instant::now() + Duration::from_secs(2),
                )
                .await
        });
        let received = matcher.poll("a").await.unwrap();
        received.finish(Err(Status::unavailable("history failed")));
        assert!(!add.await.unwrap());
        let producer = matcher.clone();
        let add = tokio::spawn(async move {
            producer
                .offer(
                    task(),
                    "a",
                    "00000000-0000-0000-0000-000000000001",
                    false,
                    Duration::from_secs(1),
                    Instant::now() + Duration::from_secs(2),
                )
                .await
        });
        let received = matcher.poll("a").await.unwrap();
        drop(received);
        assert!(!add.await.unwrap());
        let poller = matcher.clone();
        let poll = tokio::spawn(async move { poller.poll("a").await });
        tokio::task::yield_now().await;
        poll.abort();
        let _ = poll.await;
        assert!(
            !matcher
                .offer(
                    task(),
                    "a",
                    "00000000-0000-0000-0000-000000000001",
                    false,
                    Duration::ZERO,
                    Instant::now() + Duration::from_secs(1)
                )
                .await
        );
        let producer = matcher.clone();
        let add = tokio::spawn(async move {
            producer
                .offer(
                    task(),
                    "a",
                    "00000000-0000-0000-0000-000000000001",
                    false,
                    Duration::from_secs(1),
                    Instant::now() + Duration::from_secs(2),
                )
                .await
        });
        tokio::task::yield_now().await;
        stop.send_replace(true);
        assert!(!add.await.unwrap());
        assert!(matcher.poll("a").await.is_none());
    }
    #[tokio::test]
    async fn sync_match_zero_wait_claims_registered_poller_and_timeouts_withdraw_offers() {
        let (_stop, matcher) = fixture();
        let poller = matcher.clone();
        let poll = tokio::spawn(async move {
            poller.poll("a").await.unwrap().finish(Ok(()));
        });
        tokio::task::yield_now().await;
        assert!(
            matcher
                .offer(
                    task(),
                    "a",
                    "00000000-0000-0000-0000-000000000001",
                    false,
                    Duration::ZERO,
                    Instant::now() + Duration::from_secs(1)
                )
                .await
        );
        poll.await.unwrap();
        assert!(
            !matcher
                .offer(
                    task(),
                    "a",
                    "00000000-0000-0000-0000-000000000001",
                    false,
                    Duration::from_millis(1),
                    Instant::now() + Duration::from_secs(1)
                )
                .await
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(1), matcher.poll("a"))
                .await
                .is_err()
        );
        assert_eq!(matcher.state.lock().unwrap().offers.len(), 0);
        assert_eq!(matcher.state.lock().unwrap().pollers.len(), 0);
    }
}
