//! Service-scoped refresh lifecycle that atomically feeds Matching membership.
use crate::{
    matching_membership::Membership,
    matching_peer_provider::{MatchingPeerProvider, PeerMember, ProviderError},
};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    sync::{broadcast, watch},
    task::JoinHandle,
    time::{interval_at, Instant, MissedTickBehavior},
};

const PERIOD: Duration = Duration::from_secs(2);
const MIN_REFRESH: Duration = Duration::from_secs(1);

pub struct MatchingMembershipLifecycle<P: MatchingPeerProvider> {
    service: String,
    provider: Arc<P>,
    membership: Membership,
    stopped: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
    shutdown: Option<watch::Sender<bool>>,
}
impl<P: MatchingPeerProvider> MatchingMembershipLifecycle<P> {
    pub fn new(service: impl Into<String>, provider: Arc<P>, membership: Membership) -> Self {
        Self {
            service: service.into(),
            provider,
            membership,
            stopped: Arc::new(AtomicBool::new(false)),
            worker: None,
            shutdown: None,
        }
    }
    /// Provider start, watch registration, and initial fetch are synchronous barriers.
    pub async fn start(&mut self) -> Result<(), ProviderError> {
        if self.worker.is_some() || self.stopped.load(Ordering::Acquire) {
            return Ok(());
        }
        if let Err(error) = self.provider.start().await {
            let _ = self.provider.stop().await;
            return Err(error);
        }
        let events = match self.provider.subscribe(&self.service).await {
            Ok(events) => events,
            Err(error) => {
                let _ = self.provider.stop().await;
                return Err(error);
            }
        };
        let initial_changed = match self.refresh().await {
            Ok(changed) => changed,
            Err(e) => {
                let _ = self.provider.stop().await;
                return Err(e);
            }
        };
        let (tx, mut stop) = watch::channel(false);
        self.shutdown = Some(tx);
        let provider = self.provider.clone();
        let membership = self.membership.clone();
        let service = self.service.clone();
        let stopped = self.stopped.clone();
        self.worker = Some(tokio::spawn(async move {
            let mut events = events;
            let mut events_open = true;
            let now = Instant::now();
            let mut ticker = interval_at(now + PERIOD, PERIOD);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
            // Go leaves `members.refreshed` at its zero value until an
            // address set changes. Failed and no-op snapshots do not throttle.
            let mut last_changed = initial_changed.then(Instant::now);
            loop {
                tokio::select! {
                    _ = stop.changed() => if *stop.borrow() { break },
                    event = events.recv(), if events_open => {
                        match event {
                            Ok(_) | Err(broadcast::error::RecvError::Lagged(_)) => {
                                // Go's refreshChan has capacity one: collapse
                                // all currently queued provider hints into this
                                // single snapshot request.
                                loop {
                                    match events.try_recv() {
                                        Ok(_) | Err(broadcast::error::TryRecvError::Lagged(_)) => continue,
                                        Err(broadcast::error::TryRecvError::Empty) => break,
                                        Err(broadcast::error::TryRecvError::Closed) => {
                                            events_open = false;
                                            break;
                                        }
                                    }
                                }
                                if last_changed.is_none_or(|last| last.elapsed() >= MIN_REFRESH) {
                                    if let Ok(hosts) = snapshot(provider.as_ref(), &service).await {
                                        if membership.replace_if_addresses_changed(hosts) {
                                            last_changed = Some(Instant::now());
                                        }
                                    }
                                }
                            }
                            Err(broadcast::error::RecvError::Closed) => events_open = false,
                        }
                    },
                    _ = ticker.tick() => {
                        if last_changed.is_none_or(|last| last.elapsed() >= MIN_REFRESH) {
                            if let Ok(hosts) = snapshot(provider.as_ref(), &service).await {
                                if membership.replace_if_addresses_changed(hosts) {
                                    last_changed = Some(Instant::now());
                                }
                            }
                        }
                    },
                }
                if stopped.load(Ordering::Acquire) {
                    break;
                }
            }
        }));
        Ok(())
    }
    async fn refresh(&self) -> Result<bool, ProviderError> {
        Ok(self
            .membership
            .replace_if_addresses_changed(snapshot(self.provider.as_ref(), &self.service).await?))
    }
    pub fn membership(&self) -> &Membership {
        &self.membership
    }
    /// Withdrawal precedes provider teardown; emptying local placement follows worker exit.
    pub async fn shutdown(&mut self) -> Result<(), ProviderError> {
        if self.worker.is_none() && self.stopped.load(Ordering::Acquire) {
            return Ok(());
        }
        if self.stopped.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        let eviction = self.provider.self_evict().await;
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(true);
        }
        if let Some(worker) = self.worker.take() {
            worker.abort();
            let _ = worker.await;
        }
        self.membership.replace(Vec::new());
        let stop = self.provider.stop().await;
        eviction.and(stop)
    }
}
impl<P: MatchingPeerProvider> Drop for MatchingMembershipLifecycle<P> {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(true);
        }
        // A provider snapshot can be stuck forever. A shutdown watch cannot
        // interrupt an in-progress await, so abort the detached worker on Drop.
        if let Some(worker) = self.worker.take() {
            worker.abort();
        }
    }
}
async fn snapshot<P: MatchingPeerProvider>(
    provider: &P,
    service: &str,
) -> Result<Vec<cadence_common::membership::Host>, ProviderError> {
    Ok(provider
        .members(service)
        .await?
        .into_iter()
        .filter_map(|m: PeerMember| m.matching_host(service))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::matching_peer_provider::{PeerChange, PeerStatus};
    use std::{
        collections::HashMap,
        sync::{atomic::AtomicUsize, Mutex},
    };
    use tokio::sync::broadcast;

    // These in-process fakes validate resolver lifecycle semantics only; they are
    // explicitly not Ringpop or network interoperability tests.
    struct Fake {
        peers: Mutex<Vec<PeerMember>>,
        tx: broadcast::Sender<PeerChange>,
        fail: AtomicBool,
        fetches: AtomicUsize,
        order: Mutex<Vec<&'static str>>,
        fail_subscribe: AtomicBool,
        close_events: AtomicBool,
        hang: AtomicBool,
        snapshot_started: tokio::sync::Notify,
        cancelled_snapshots: AtomicUsize,
    }
    impl Fake {
        fn new() -> Self {
            let (tx, _) = broadcast::channel(16);
            Self {
                peers: Mutex::new(vec![]),
                tx,
                fail: AtomicBool::new(false),
                fetches: AtomicUsize::new(0),
                order: Mutex::new(vec![]),
                fail_subscribe: AtomicBool::new(false),
                close_events: AtomicBool::new(false),
                hang: AtomicBool::new(false),
                snapshot_started: tokio::sync::Notify::new(),
                cancelled_snapshots: AtomicUsize::new(0),
            }
        }
        fn set(&self, peers: Vec<PeerMember>) {
            *self.peers.lock().unwrap() = peers;
            let _ = self.tx.send(PeerChange);
        }
    }
    impl MatchingPeerProvider for Fake {
        async fn start(&self) -> Result<(), ProviderError> {
            self.order.lock().unwrap().push("start");
            Ok(())
        }
        async fn members(&self, _: &str) -> Result<Vec<PeerMember>, ProviderError> {
            self.fetches.fetch_add(1, Ordering::SeqCst);
            if self.hang.load(Ordering::SeqCst) {
                struct CancelGuard<'a>(&'a AtomicUsize);
                impl Drop for CancelGuard<'_> {
                    fn drop(&mut self) {
                        self.0.fetch_add(1, Ordering::SeqCst);
                    }
                }
                let _guard = CancelGuard(&self.cancelled_snapshots);
                self.snapshot_started.notify_one();
                std::future::pending::<()>().await;
            }
            if self.fail.load(Ordering::SeqCst) {
                return Err(ProviderError("injected snapshot failure".into()));
            }
            Ok(self.peers.lock().unwrap().clone())
        }
        async fn subscribe(
            &self,
            _: &str,
        ) -> Result<broadcast::Receiver<PeerChange>, ProviderError> {
            self.order.lock().unwrap().push("subscribe");
            if self.fail_subscribe.load(Ordering::SeqCst) {
                return Err(ProviderError("injected subscribe failure".into()));
            }
            if self.close_events.load(Ordering::SeqCst) {
                let (tx, rx) = broadcast::channel(1);
                drop(tx);
                return Ok(rx);
            }
            Ok(self.tx.subscribe())
        }
        async fn self_evict(&self) -> Result<(), ProviderError> {
            self.order.lock().unwrap().push("evict");
            Ok(())
        }
        async fn stop(&self) -> Result<(), ProviderError> {
            self.order.lock().unwrap().push("stop");
            Ok(())
        }
    }
    fn member(
        address: &str,
        identity: &str,
        service: &str,
        status: PeerStatus,
        grpc: &str,
    ) -> PeerMember {
        PeerMember {
            address: address.into(),
            identity: identity.into(),
            status,
            labels: HashMap::from([
                ("serviceName".into(), service.into()),
                ("grpc".into(), grpc.into()),
            ]),
        }
    }
    #[tokio::test]
    async fn initial_snapshot_maps_role_status_identity_and_endpoint() {
        let p = Arc::new(Fake::new());
        p.set(vec![
            member(
                "node:7935",
                "stable-id",
                "matching",
                PeerStatus::Suspect,
                "7835",
            ),
            member("wrong:1", "wrong", "history", PeerStatus::Alive, "2"),
            member("dead:1", "dead", "matching", PeerStatus::Faulty, "2"),
        ]);
        let m = Membership::new("self", vec![]);
        let mut lifecycle = MatchingMembershipLifecycle::new("matching", p.clone(), m.clone());
        lifecycle.start().await.unwrap();
        let owner = m.owner("key").unwrap();
        assert_eq!(owner.address, "node:7935");
        assert_eq!(owner.identity(), "stable-id");
        assert_eq!(owner.grpc_address, "node:7835");
        lifecycle.shutdown().await.unwrap();
    }
    #[tokio::test]
    async fn mutable_provider_updates_coalesce_and_failures_preserve_last_good() {
        let p = Arc::new(Fake::new());
        p.set(vec![member(
            "one:7",
            "one",
            "matching",
            PeerStatus::Alive,
            "8",
        )]);
        let m = Membership::new("self", vec![]);
        let mut lifecycle = MatchingMembershipLifecycle::new("matching", p.clone(), m.clone());
        lifecycle.start().await.unwrap();
        let mut changed = m.subscribe();
        p.set(vec![member(
            "two:7",
            "two",
            "matching",
            PeerStatus::Alive,
            "9",
        )]);
        tokio::time::timeout(Duration::from_millis(2600), changed.changed())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(m.owner("key").unwrap().address, "two:7");
        assert_eq!(
            p.fetches.load(Ordering::SeqCst),
            2,
            "burst of hints must coalesce"
        );
        let generation = *changed.borrow();
        p.fail.store(true, Ordering::SeqCst);
        for _ in 0..12 {
            p.set(vec![]);
        }
        tokio::time::sleep(Duration::from_millis(1100)).await;
        assert_eq!(*changed.borrow(), generation);
        assert_eq!(m.owner("key").unwrap().address, "two:7");
        lifecycle.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn hint_inside_min_refresh_waits_for_two_second_ticker() {
        let p = Arc::new(Fake::new());
        p.set(vec![member(
            "one:7",
            "one",
            "matching",
            PeerStatus::Alive,
            "8",
        )]);
        let m = Membership::new("self", vec![]);
        let mut lifecycle = MatchingMembershipLifecycle::new("matching", p.clone(), m.clone());
        lifecycle.start().await.unwrap();
        tokio::task::yield_now().await;
        assert_eq!(p.fetches.load(Ordering::SeqCst), 1);

        tokio::time::advance(Duration::from_millis(200)).await;
        p.set(vec![member(
            "two:7",
            "two",
            "matching",
            PeerStatus::Alive,
            "9",
        )]);
        tokio::task::yield_now().await;
        assert_eq!(
            p.fetches.load(Ordering::SeqCst),
            1,
            "Go suppresses refreshes less than one second after a changed view"
        );
        assert_eq!(m.owner("key").unwrap().address, "one:7");

        tokio::time::advance(Duration::from_millis(1800)).await;
        tokio::task::yield_now().await;
        assert_eq!(
            p.fetches.load(Ordering::SeqCst),
            2,
            "the next 2s ticker re-fetches the dropped hint"
        );
        assert_eq!(m.owner("key").unwrap().address, "two:7");
        lifecycle.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn failed_and_unchanged_snapshots_preserve_go_refresh_timestamp() {
        let p = Arc::new(Fake::new());
        p.set(vec![member(
            "one:7",
            "one",
            "matching",
            PeerStatus::Alive,
            "8",
        )]);
        let mut lifecycle = MatchingMembershipLifecycle::new(
            "matching",
            p.clone(),
            Membership::new("self", vec![]),
        );
        lifecycle.start().await.unwrap();
        tokio::task::yield_now().await;
        assert_eq!(p.fetches.load(Ordering::SeqCst), 1);
        tokio::time::advance(Duration::from_millis(1100)).await;
        p.fail.store(true, Ordering::SeqCst);
        for expected in 2..=5 {
            p.set(vec![member(
                "one:7",
                "one",
                "matching",
                PeerStatus::Alive,
                "8",
            )]);
            tokio::task::yield_now().await;
            assert_eq!(p.fetches.load(Ordering::SeqCst), expected);
        }
        p.fail.store(false, Ordering::SeqCst);
        for expected in 6..=8 {
            p.set(vec![member(
                "one:7",
                "one",
                "matching",
                PeerStatus::Alive,
                "8",
            )]);
            tokio::task::yield_now().await;
            assert_eq!(p.fetches.load(Ordering::SeqCst), expected);
        }
        lifecycle.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn dropping_lifecycle_aborts_a_stuck_snapshot_worker() {
        let p = Arc::new(Fake::new());
        p.set(vec![member(
            "one:7",
            "one",
            "matching",
            PeerStatus::Alive,
            "8",
        )]);
        let mut lifecycle = MatchingMembershipLifecycle::new(
            "matching",
            p.clone(),
            Membership::new("self", vec![]),
        );
        lifecycle.start().await.unwrap();
        tokio::task::yield_now().await;
        p.hang.store(true, Ordering::SeqCst);
        tokio::time::advance(Duration::from_secs(1)).await;
        p.set(vec![]);
        p.snapshot_started.notified().await;
        drop(lifecycle);
        tokio::task::yield_now().await;
        assert_eq!(p.cancelled_snapshots.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn mapping_matches_go_port_and_identity_boundaries() {
        let make = |address: &str, grpc: &str, identity: &str| {
            member(address, identity, "matching", PeerStatus::Alive, grpc)
        };
        for (label, expected) in [
            ("0", true),
            ("65535", true),
            ("65536", false),
            ("-1", false),
            ("+1", false),
            ("", false),
        ] {
            let host = make("[::1]:7935", label, "")
                .matching_host("matching")
                .unwrap();
            assert_eq!(!host.grpc_address.is_empty(), expected, "label {label:?}");
            assert_eq!(host.identity, "[::1]:7935");
            if expected {
                let port = label.parse::<u16>().unwrap();
                assert_eq!(host.grpc_address, format!("[::1]:{port}"));
            }
        }
        let cases = [
            ("host:7935", "host:80"),
            (":7935", ":80"),
            ("host:", "host:80"),
            ("[::1]:7935", "[::1]:80"),
            ("[bad]:7935", "[bad]:80"),
            ("[::1]x:7935", ":80"),
            ("bad:host:7935", ":80"),
            ("broken", ":80"),
            ("host]:7935", ":80"),
            ("[host]]:7935", ":80"),
            ("[host:7935", ":80"),
            ("[host]", ":80"),
        ];
        for (address, expected) in cases {
            let actual = make(address, "80", "id")
                .matching_host("matching")
                .unwrap()
                .grpc_address;
            assert_eq!(
                actual, expected,
                "Go SplitHostPort/JoinHostPort case {address:?}"
            );
        }
        assert!(make("bad:host:7935", "65536", "id")
            .matching_host("matching")
            .unwrap()
            .grpc_address
            .is_empty());
        let leading_zero = make("host:7935", "00080", "id")
            .matching_host("matching")
            .unwrap();
        assert_eq!(leading_zero.grpc_address, "host:80");
    }
    #[tokio::test]
    async fn shutdown_self_evicts_before_stop_and_clears_placement() {
        let p = Arc::new(Fake::new());
        p.set(vec![member(
            "one:7",
            "one",
            "matching",
            PeerStatus::Alive,
            "8",
        )]);
        let m = Membership::new("self", vec![]);
        let mut lifecycle = MatchingMembershipLifecycle::new("matching", p.clone(), m.clone());
        lifecycle.start().await.unwrap();
        lifecycle.shutdown().await.unwrap();
        assert!(m.owner("key").is_err());
        assert_eq!(
            *p.order.lock().unwrap(),
            vec!["start", "subscribe", "evict", "stop"]
        );
    }

    #[tokio::test]
    async fn startup_failures_stop_provider_and_start_is_idempotent() {
        let p = Arc::new(Fake::new());
        p.fail_subscribe.store(true, Ordering::SeqCst);
        let mut lifecycle = MatchingMembershipLifecycle::new(
            "matching",
            p.clone(),
            Membership::new("self", vec![]),
        );
        assert!(lifecycle.start().await.is_err());
        assert_eq!(*p.order.lock().unwrap(), vec!["start", "subscribe", "stop"]);
        p.fail_subscribe.store(false, Ordering::SeqCst);
        lifecycle.start().await.unwrap();
        lifecycle.start().await.unwrap();
        assert_eq!(
            p.order
                .lock()
                .unwrap()
                .iter()
                .filter(|&&v| v == "start")
                .count(),
            2
        );
        lifecycle.shutdown().await.unwrap();
        lifecycle.shutdown().await.unwrap();
        assert_eq!(
            p.order
                .lock()
                .unwrap()
                .iter()
                .filter(|&&v| v == "stop")
                .count(),
            2
        );
    }

    #[tokio::test]
    async fn closed_event_stream_keeps_periodic_refresh_alive() {
        let p = Arc::new(Fake::new());
        p.close_events.store(true, Ordering::SeqCst);
        p.set(vec![member(
            "one:7",
            "one",
            "matching",
            PeerStatus::Alive,
            "8",
        )]);
        let m = Membership::new("self", vec![]);
        let mut lifecycle = MatchingMembershipLifecycle::new("matching", p.clone(), m.clone());
        lifecycle.start().await.unwrap();
        let initial_fetches = p.fetches.load(Ordering::SeqCst);
        *p.peers.lock().unwrap() = vec![member("two:7", "two", "matching", PeerStatus::Alive, "9")];
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if m.owner("key").is_ok_and(|owner| owner.address == "two:7") {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("periodic refresh should run after the watch closes");
        assert!(p.fetches.load(Ordering::SeqCst) > initial_fetches);
        lifecycle.shutdown().await.unwrap();
    }
}
