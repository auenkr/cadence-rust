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
    sync::watch,
    task::JoinHandle,
    time::{interval, Instant, MissedTickBehavior},
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
        self.provider.start().await?;
        let events = self.provider.subscribe(&self.service).await?;
        match self.refresh().await {
            Ok(()) => {}
            Err(e) => {
                let _ = self.provider.stop().await;
                return Err(e);
            }
        }
        let (tx, mut stop) = watch::channel(false);
        self.shutdown = Some(tx);
        let provider = self.provider.clone();
        let membership = self.membership.clone();
        let service = self.service.clone();
        let stopped = self.stopped.clone();
        self.worker = Some(tokio::spawn(async move {
            let mut events = events;
            let mut ticker = interval(PERIOD);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
            let mut last = Instant::now();
            loop {
                tokio::select! {
                    _ = stop.changed() => if *stop.borrow() { break },
                    event = events.recv() => { if event.is_err() { continue; } if last.elapsed() >= MIN_REFRESH {
                        if let Ok(hosts) = snapshot(provider.as_ref(), &service).await { membership.replace(hosts); last = Instant::now(); }
                    } },
                    _ = ticker.tick() => { if last.elapsed() >= MIN_REFRESH {
                        if let Ok(hosts) = snapshot(provider.as_ref(), &service).await { membership.replace(hosts); last = Instant::now(); }
                    } },
                }
                if stopped.load(Ordering::Acquire) {
                    break;
                }
            }
        }));
        Ok(())
    }
    async fn refresh(&self) -> Result<(), ProviderError> {
        self.membership
            .replace(snapshot(self.provider.as_ref(), &self.service).await?);
        Ok(())
    }
    pub fn membership(&self) -> &Membership {
        &self.membership
    }
    /// Withdrawal precedes provider teardown; emptying local placement follows worker exit.
    pub async fn shutdown(&mut self) -> Result<(), ProviderError> {
        if self.stopped.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        let eviction = self.provider.self_evict().await;
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(true);
        }
        if let Some(worker) = self.worker.take() {
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
    use std::{collections::HashMap, sync::Mutex};
    use tokio::sync::broadcast;

    // These in-process fakes validate resolver lifecycle semantics only; they are
    // explicitly not Ringpop or network interoperability tests.
    struct Fake {
        peers: Mutex<Vec<PeerMember>>,
        tx: broadcast::Sender<PeerChange>,
        fail: AtomicBool,
        order: Mutex<Vec<&'static str>>,
    }
    impl Fake {
        fn new() -> Self {
            let (tx, _) = broadcast::channel(16);
            Self {
                peers: Mutex::new(vec![]),
                tx,
                fail: AtomicBool::new(false),
                order: Mutex::new(vec![]),
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
        tokio::time::timeout(Duration::from_millis(300), changed.changed())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(m.owner("key").unwrap().address, "two:7");
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
}
