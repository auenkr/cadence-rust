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
}
impl Membership {
    pub fn new(identity: impl Into<Arc<str>>, hosts: Vec<Host>) -> Self {
        Self {
            identity: identity.into(),
            ring: Arc::new(RwLock::new(Ring::new(hosts))),
            channels: Arc::default(),
            changed: tokio::sync::watch::channel(0).0,
        }
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
            self.replace(hosts);
        }
        changed
    }
    pub fn subscribe(&self) -> tokio::sync::watch::Receiver<u64> {
        self.changed.subscribe()
    }
    pub fn owner(&self, key: &str) -> Result<Host, Status> {
        self.ring
            .read()
            .expect("Matching membership lock")
            .lookup(key)
            .cloned()
            .ok_or_else(|| Status::internal("Not enough hosts to serve the request"))
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
        self.ring
            .read()
            .expect("Matching membership lock")
            .hosts()
            .iter()
            .find(|host| host.address.as_str() == address)
            .cloned()
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
        let address = if grpc_address.contains("://") {
            grpc_address.to_owned()
        } else {
            format!("http://{grpc_address}")
        };
        let channel = tonic::transport::Endpoint::from_shared(address)
            .map_err(|error| Status::internal(error.to_string()))?
            .connect_lazy();
        channels.insert(grpc_address.to_owned(), channel.clone());
        Ok(Some(channel))
    }
    pub fn check_task_list(&self, name: &str) -> Result<(), Status> {
        let owner = self.owner(name)?;
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
        let owner = self.owner(&id.ring_key())?;
        if owner.identity() == self.identity.as_ref() {
            return Ok(());
        }
        let bucket_id = id.to_log_string();
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
