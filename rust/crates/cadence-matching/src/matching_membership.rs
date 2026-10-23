//! Matching's atomic membership snapshots and Go-compatible ownership errors.
use cadence_common::membership::{Host, Ring};
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex, RwLock},
};
use tonic::Status;

#[derive(Clone)]
pub struct Membership {
    identity: Arc<str>,
    ring: Arc<RwLock<Ring>>,
    channels: Arc<Mutex<HashMap<String, tonic::transport::Channel>>>,
    changed: tokio::sync::watch::Sender<u64>,
    /// Go hashring `refreshChan`: an empty-ring lookup asks the lifecycle worker
    /// to refresh. Notify stores at most one permit, like Go's capacity-1 channel.
    refresh: Arc<tokio::sync::Notify>,
    /// Go's Matching outbound TLS, taken from Matching's own `rpc.tls`.
    peer_tls: Option<crate::grpc_tls::ClientTls>,
    metrics: Arc<RwLock<crate::matching_metrics::MatchingMetrics>>,
}
impl Membership {
    pub fn new(identity: impl Into<Arc<str>>, hosts: Vec<Host>) -> Self {
        Self {
            identity: identity.into(),
            ring: Arc::new(RwLock::new(Ring::new(hosts))),
            channels: Arc::default(),
            changed: tokio::sync::watch::channel(0).0,
            refresh: Arc::default(),
            peer_tls: None,
            metrics: Arc::new(RwLock::new(
                crate::matching_metrics::MatchingMetrics::default(),
            )),
        }
    }
    pub fn with_metrics(self, sink: Arc<dyn crate::matching_metrics::MetricSink>) -> Self {
        *self
            .metrics
            .write()
            .expect("Matching membership metrics lock") =
            crate::matching_metrics::MatchingMetrics::new(sink);
        self
    }

    pub(crate) fn record_routing_path(&self, path: &str) {
        self.metrics
            .read()
            .expect("Matching membership metrics lock")
            .routing_path(path);
    }

    pub(crate) fn record_onboarding(&self, percentage: i64) {
        self.metrics
            .read()
            .expect("Matching membership metrics lock")
            .onboarding_percentage(percentage);
    }

    #[must_use]
    pub fn with_peer_tls(mut self, peer_tls: Option<crate::grpc_tls::ClientTls>) -> Self {
        self.peer_tls = peer_tls;
        self
    }
    pub(crate) fn peer_tls(&self) -> Option<&crate::grpc_tls::ClientTls> {
        self.peer_tls.as_ref()
    }
    /// Publish placement before waking unloaders. Readers see a complete ring.
    pub fn replace(&self, hosts: Vec<Host>) {
        let addresses: HashSet<_> = hosts.iter().map(|host| host.grpc_address.clone()).collect();
        *self.ring.write().expect("Matching membership lock") = Ring::new(hosts);
        self.channels
            .lock()
            .expect("Matching channel cache lock")
            .retain(|address, _| addresses.contains(address));
        self.changed
            .send_modify(|generation| *generation = generation.wrapping_add(1));
    }
    /// Match Go hashring's address-only diff: metadata changes for an existing
    /// placement address do not rebuild the ring or advance its refresh time.
    pub fn replace_if_addresses_changed(&self, hosts: Vec<Host>) -> bool {
        let changed = {
            let ring = self.ring.read().expect("Matching membership lock");
            let old: HashSet<_> = ring
                .hosts()
                .iter()
                .map(|host| host.address.as_str())
                .collect();
            let new: HashSet<_> = hosts.iter().map(|host| host.address.as_str()).collect();
            old != new
        };
        if changed {
            // Go Ring.emitHashIdentifier sorts placement addresses descending,
            // including a trailing newline, and trims FarmHash32 modulo 1000.
            let mut addresses: Vec<_> = hosts.iter().map(|host| host.address.as_str()).collect();
            addresses.sort_unstable_by(|a, b| b.cmp(a));
            let mut view = addresses.join("\n");
            if !addresses.is_empty() {
                view.push('\n');
            }
            let identifier = cadence_common::membership::hash32(view.as_bytes()) % 1000;
            self.replace(hosts);
            self.metrics
                .read()
                .expect("Matching membership metrics lock")
                .membership_view(&self.identity, identifier);
        }
        changed
    }
    pub fn subscribe(&self) -> tokio::sync::watch::Receiver<u64> {
        self.changed.subscribe()
    }
    pub fn owner(&self, key: &str) -> Result<Host, Status> {
        let owner = self
            .ring
            .read()
            .expect("Matching membership lock")
            .lookup(key)
            .cloned();
        owner.ok_or_else(|| {
            // Go Ring.LookupRaw calls signalSelf on an empty-ring lookup.
            self.refresh.notify_one();
            Status::internal("Not enough hosts to serve the request")
        })
    }
    /// Shared with the lifecycle worker, which refreshes from the provider when signalled.
    pub(crate) fn refresh_signal(&self) -> Arc<tokio::sync::Notify> {
        self.refresh.clone()
    }
    /// Ring-only multi-host lookup used by Matching's shard-distributor
    /// resolver, whose Go `LookupN` deliberately does not use the spectator.
    pub(crate) fn owners_n(&self, key: &str, count: usize) -> Vec<Host> {
        self.ring
            .read()
            .expect("Matching membership lock")
            .lookup_n(key, count)
            .into_iter()
            .cloned()
            .collect()
    }
    pub(crate) fn owner_by_address(&self, address: &str) -> Option<Host> {
        let host = self
            .ring
            .read()
            .expect("Matching membership lock")
            .hosts()
            .iter()
            .find(|host| host.address.as_str() == address)
            .cloned();
        if host.is_none() {
            self.metrics
                .read()
                .expect("Matching membership metrics lock")
                .resolver_host_not_found();
        }
        host
    }
    pub(crate) fn hosts(&self) -> Vec<Host> {
        self.ring
            .read()
            .expect("Matching membership lock")
            .hosts()
            .to_vec()
    }
    pub fn owns(&self, key: &str) -> Result<bool, Status> {
        Ok(self.owner(key)?.identity() == self.identity.as_ref())
    }
    pub fn remote_channel(&self, key: &str) -> Result<Option<tonic::transport::Channel>, Status> {
        let owner = self.owner(key)?;
        self.remote_channel_for_owner(owner.identity(), &owner.grpc_address)
    }
    pub(crate) fn remote_channel_for_owner(
        &self,
        identity: &str,
        grpc_address: &str,
    ) -> Result<Option<tonic::transport::Channel>, Status> {
        if identity == self.identity.as_ref() {
            return Ok(None);
        }
        if grpc_address.is_empty() {
            return Err(Status::unavailable("Matching owner has no gRPC address"));
        }
        let mut channels = self.channels.lock().expect("Matching channel cache lock");
        if let Some(channel) = channels.get(grpc_address) {
            return Ok(Some(channel.clone()));
        }
        let channel = crate::grpc_tls::endpoint_channel(grpc_address, self.peer_tls.as_ref())
            .map_err(|error| Status::internal(error.to_string()))?;
        channels.insert(grpc_address.to_owned(), channel.clone());
        Ok(Some(channel))
    }
    /// Resolve a host through the live ring and return its gRPC channel,
    /// including when the owner is this process. Internal Matching refreshes
    /// use the same RPC contract for local and remote partitions so an
    /// unloaded local partition is created and refreshed just like Go's
    /// Matching client call.
    pub(crate) fn channel_for_task_list_owner(
        &self,
        key: &str,
    ) -> Result<(Host, tonic::transport::Channel), Status> {
        let owner = self.owner(key)?;
        let grpc_address = if owner.identity() == self.identity.as_ref() {
            self.ring
                .read()
                .expect("Matching membership lock")
                .hosts()
                .iter()
                .find(|host| host.identity() == self.identity.as_ref())
                .map(|host| host.grpc_address.clone())
                .filter(|address| !address.is_empty())
                .ok_or_else(|| Status::unavailable("Local Matching host has no gRPC address"))?
        } else {
            owner.grpc_address.clone()
        };
        if grpc_address.is_empty() {
            return Err(Status::unavailable("Matching owner has no gRPC address"));
        }
        let mut channels = self.channels.lock().expect("Matching channel cache lock");
        let channel = if let Some(channel) = channels.get(&grpc_address) {
            channel.clone()
        } else {
            let channel = crate::grpc_tls::endpoint_channel(&grpc_address, self.peer_tls.as_ref())
                .map_err(|error| Status::internal(error.to_string()))?;
            channels.insert(grpc_address, channel.clone());
            channel
        };
        Ok((owner, channel))
    }
    pub fn check_task_list(&self, name: &str) -> Result<(), Status> {
        // Go errIfShardOwnershipLost wraps the lookup failure.
        let owner = self.owner(name).map_err(|error| {
            Status::internal(format!(
                "failed to lookup task list owner: {}",
                error.message()
            ))
        })?;
        if owner.identity() == self.identity.as_ref() {
            return Ok(());
        }
        Err(self.task_list_not_owned_by(name, owner.identity()))
    }
    pub(crate) fn task_list_not_owned_by(&self, name: &str, owner: &str) -> Status {
        crate::grpc::detailed_status_type(
            tonic::Code::Aborted,
            format!(
                "task list is not owned by this host: OwnedBy: {}, Me: {}, Tasklist: {name}",
                owner, self.identity
            ),
            "type.googleapis.com/uber.cadence.shared.v1.TaskListNotOwnedByHostError".into(),
            cadence_proto::shared::TaskListNotOwnedByHostError {
                owned_by_identity: owner.into(),
                my_identity: self.identity.to_string(),
                task_list_name: name.into(),
            },
        )
    }
    pub fn check_semaphore(
        &self,
        id: &crate::matching_semaphore::Identifier,
    ) -> Result<(), Status> {
        let bucket_id = id.to_log_string();
        // Go errIfSemaphoreOwnershipLost wraps the lookup failure with the bucket id.
        let owner = self.owner(&id.ring_key()).map_err(|error| {
            Status::internal(format!(
                "failed to look up the owner of semaphore bucket {bucket_id}: {}",
                error.message()
            ))
        })?;
        if owner.identity() == self.identity.as_ref() {
            return Ok(());
        }
        Err(crate::grpc::detailed_status_type(tonic::Code::Aborted,
            format!("semaphore bucket is not owned by this host: OwnedBy: {}, Me: {}, BucketID: {bucket_id}", owner.identity(), self.identity),
            "type.googleapis.com/uber.cadence.shared.v1.SemaphoreNotOwnedByHostError".into(),
            cadence_proto::shared::SemaphoreNotOwnedByHostError { owned_by_identity: owner.identity().into(), my_identity: self.identity.to_string(), bucket_id }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;
    #[derive(Message)]
    struct Details {
        #[prost(message, repeated, tag = "3")]
        errors: Vec<prost_types::Any>,
    }
    #[tokio::test]
    async fn matching_membership_reuses_channels_and_evicts_removed_endpoints() {
        let host = |address: &str| Host {
            address: "other:7935".into(),
            identity: "other".into(),
            grpc_address: address.into(),
        };
        let membership = Membership::new("self", vec![host("127.0.0.1:7835")]);
        for _ in 0..3 {
            assert!(membership.remote_channel("queue").unwrap().is_some());
        }
        assert_eq!(membership.channels.lock().unwrap().len(), 1);
        membership.replace(vec![host("127.0.0.1:7835")]);
        assert_eq!(membership.channels.lock().unwrap().len(), 1);
        membership.replace(vec![host("127.0.0.1:7836")]);
        assert!(membership.channels.lock().unwrap().is_empty());
        assert!(membership.remote_channel("queue").unwrap().is_some());
        assert!(membership
            .channels
            .lock()
            .unwrap()
            .contains_key("127.0.0.1:7836"));
        membership.replace(vec![]);
        assert!(membership.channels.lock().unwrap().is_empty());
    }
    #[tokio::test]
    async fn matching_membership_typed_errors_and_atomic_notifications() {
        let host = |name: &str| Host {
            address: format!("{name}:7935"),
            identity: name.into(),
            grpc_address: format!("{name}:7835"),
        };
        let membership = Membership::new("self", vec![host("other")]);
        let mut changes = membership.subscribe();
        let error = membership.check_task_list("queue").unwrap_err();
        assert_eq!(error.code(), tonic::Code::Aborted);
        let detail = Details::decode(error.details()).unwrap().errors.remove(0);
        assert_eq!(
            detail.type_url,
            "type.googleapis.com/uber.cadence.shared.v1.TaskListNotOwnedByHostError"
        );
        let detail =
            cadence_proto::shared::TaskListNotOwnedByHostError::decode(detail.value.as_slice())
                .unwrap();
        assert_eq!(
            (
                detail.owned_by_identity.as_str(),
                detail.my_identity.as_str(),
                detail.task_list_name.as_str()
            ),
            ("other", "self", "queue")
        );
        let id = crate::matching_semaphore::Identifier::new("domain", "semaphore", 0).unwrap();
        let error = membership.check_semaphore(&id).unwrap_err();
        assert_eq!(error.code(), tonic::Code::Aborted);
        let detail = Details::decode(error.details()).unwrap().errors.remove(0);
        assert_eq!(
            detail.type_url,
            "type.googleapis.com/uber.cadence.shared.v1.SemaphoreNotOwnedByHostError"
        );
        let detail =
            cadence_proto::shared::SemaphoreNotOwnedByHostError::decode(detail.value.as_slice())
                .unwrap();
        assert_eq!(detail.bucket_id, "domain/semaphore/0");
        membership.replace(vec![host("self")]);
        changes.changed().await.unwrap();
        membership.check_task_list("queue").unwrap();
        membership.check_semaphore(&id).unwrap();
        membership.replace(vec![]);
        assert_eq!(
            membership.owner("queue").unwrap_err().message(),
            "Not enough hosts to serve the request"
        );
    }
}

#[cfg(test)]
mod lookup_error_parity_tests {
    use super::*;

    #[test]
    fn empty_ring_ownership_errors_wrap_lookup_failure_like_go_engine() {
        let membership = Membership::new("self", vec![]);
        let error = membership.check_task_list("queue").unwrap_err();
        assert_eq!(error.code(), tonic::Code::Internal);
        assert_eq!(
            error.message(),
            "failed to lookup task list owner: Not enough hosts to serve the request"
        );
        let id = crate::matching_semaphore::Identifier::new("domain", "semaphore", 0).unwrap();
        let error = membership.check_semaphore(&id).unwrap_err();
        assert_eq!(error.code(), tonic::Code::Internal);
        assert_eq!(
            error.message(),
            "failed to look up the owner of semaphore bucket domain/semaphore/0: Not enough hosts to serve the request"
        );
        // Raw lookup (Go getHostInfo / Ring.Lookup) stays unwrapped.
        assert_eq!(
            membership.owner("queue").unwrap_err().message(),
            "Not enough hosts to serve the request"
        );
    }
}

#[cfg(test)]
mod metric_parity_tests {
    use super::*;
    use crate::matching_metrics::{MetricKind, MetricValue, RecordingMetricSink};

    #[test]
    fn changed_ring_view_uses_go_gauge_scope_once() {
        let sink = Arc::new(RecordingMetricSink::default());
        let membership = Membership::new("local", vec![]).with_metrics(sink.clone());
        let hosts = vec![Host {
            address: "peer:7935".into(),
            identity: "peer".into(),
            grpc_address: "peer:7835".into(),
        }];
        assert!(membership.replace_if_addresses_changed(hosts.clone()));
        assert!(!membership.replace_if_addresses_changed(hosts));
        let events = sink.events();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].name, "hashring_view_identifier");
        assert_eq!(events[0].kind, MetricKind::Gauge);
        assert_eq!(
            events[0].tags,
            std::collections::BTreeMap::from([
                ("operation".into(), "Hashring".into()),
                ("service".into(), "cadence-matching".into()),
                ("host".into(), "local".into()),
            ])
        );
    }

    #[test]
    fn missing_address_uses_go_resolver_scope() {
        let sink = Arc::new(RecordingMetricSink::default());
        let membership = Membership::new("local", vec![]).with_metrics(sink.clone());
        assert!(membership.owner_by_address("absent").is_none());
        let events = sink.events();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].name, "ring_resolver_error");
        assert_eq!(events[0].kind, MetricKind::Counter);
        assert_eq!(events[0].value, MetricValue::Count(1));
        assert_eq!(events[0].tags.len(), 1);
        assert_eq!(events[0].tags["operation"], "ResolverHostNotFound");
    }
}

#[cfg(test)]
mod go_membership_port_tests {
    use super::*;
    use crate::matching_metrics::{MetricValue, RecordingMetricSink};

    fn host(address: &str) -> Host {
        Host {
            address: address.into(),
            identity: String::new(),
            grpc_address: String::new(),
        }
    }

    // Go hashring_test.go TestDiffMemberMakesCorrectDiff. Go returns the added and
    // removed lists; Rust only reports whether the address set changed.
    #[test]
    fn go_diff_cases_report_change_only_for_address_set_differences() {
        let cases: [(&str, Vec<&str>, Vec<&str>, bool); 5] = [
            ("empty and one added", vec![], vec!["a"], true),
            ("non-empty and added", vec!["a"], vec!["a", "b"], true),
            ("empty and nothing has changed", vec![], vec![], false),
            ("multiple hosts, but no change", vec!["a", "b", "c"], vec!["c", "b", "a"], false),
            ("multiple hosts, add/delete", vec!["a", "b", "c"], vec!["b", "e", "f"], true),
        ];
        for (name, current, next, changed) in cases {
            let membership = Membership::new("local", current.into_iter().map(host).collect());
            assert_eq!(
                membership.replace_if_addresses_changed(next.into_iter().map(host).collect()),
                changed,
                "{name}"
            );
        }
    }

    // Go hashring_test.go TestMemberCountReturnsNumber.
    #[test]
    fn go_member_count_tracks_replaced_ring_hosts() {
        let membership = Membership::new("local", vec![]);
        assert_eq!(membership.hosts().len(), 0);
        membership.replace(vec![host("127"), host("128")]);
        assert_eq!(membership.hosts().len(), 2);
    }

    // Go resolver_test.go TestMethodsAreRoutedToARing: Lookup("key") on the
    // two-member ring {127, 128} resolves to 127.
    #[test]
    fn go_lookup_routes_key_on_two_member_ring() {
        let membership = Membership::new("local", vec![host("127"), host("128")]);
        assert_eq!(membership.owner("key").unwrap().address, "127");
    }

    // Go hashring_test.go TestRefreshWillNotifySubscribers and
    // TestRefreshUpdatesRingOnlyWhenRingHasChanged: subscribers wake only when the
    // address set changes; a metadata-only refresh is ignored.
    #[test]
    fn go_subscribers_wake_only_when_placement_addresses_change() {
        let membership = Membership::new("local", vec![host("127")]);
        let mut changed = membership.subscribe();
        changed.borrow_and_update();

        let same_address_new_identity = vec![Host {
            address: "127".into(),
            identity: "other".into(),
            grpc_address: String::new(),
        }];
        assert!(!membership.replace_if_addresses_changed(same_address_new_identity));
        assert!(!changed.has_changed().unwrap());

        assert!(membership.replace_if_addresses_changed(vec![host("127"), host("128")]));
        assert!(changed.has_changed().unwrap());
    }

    // Go hashring_test.go TestLookupAndRefreshRaceCondition: owner lookups keep
    // succeeding while refreshes publish new placements.
    #[test]
    fn go_lookup_and_refresh_race_never_observes_missing_owner() {
        let membership = Membership::new("local", vec![host("127"), host("128")]);
        let lookups = membership.clone();
        let refresher = membership.clone();
        let refreshed = vec![host("127"), host("128"), host("129")];
        let base = vec![host("127"), host("128")];

        let lookup_thread = std::thread::spawn(move || {
            for _ in 0..50 {
                lookups.owner("a").expect("owner during refresh");
            }
        });
        let refresh_thread = std::thread::spawn(move || {
            for index in 0..50 {
                let next = if index % 2 == 0 { refreshed.clone() } else { base.clone() };
                refresher.replace(next);
            }
        });
        lookup_thread.join().unwrap();
        refresh_thread.join().unwrap();
    }

    // Go hashring_test.go TestEmitHashringView. Go's emitHashIdentifier hashes the
    // descending address list with farm.Hash32, as does the Rust view identifier.
    #[test]
    fn go_hashring_view_identifier_matches_go_emit_vectors() {
        fn identifier(addresses: &[&str]) -> f64 {
            let sink = Arc::new(RecordingMetricSink::default());
            // Seed a different address so the first replace always changes the view.
            let membership =
                Membership::new("host123", vec![host("seed:1")]).with_metrics(sink.clone());
            membership.replace_if_addresses_changed(addresses.iter().map(|a| host(a)).collect());
            let events = sink.events();
            assert_eq!(events.len(), 1);
            match events[0].value {
                MetricValue::Number(value) => value,
                ref other => panic!("unexpected metric value {other:?}"),
            }
        }

        assert_eq!(identifier(&["10.0.0.1:1234", "10.0.0.2:1234", "10.0.0.3:1234"]), 835.0);
        assert_eq!(identifier(&["10.0.0.3:1234", "10.0.0.1:1234", "10.0.0.2:1234"]), 835.0);
        assert_eq!(identifier(&[]), 242.0);
    }
}
