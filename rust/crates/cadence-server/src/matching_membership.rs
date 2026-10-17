//! Matching's atomic membership snapshots and Go-compatible ownership errors.
use cadence_common::membership::{Host, Ring};
use std::sync::{Arc, RwLock};
use tonic::Status;

#[derive(Clone)]
pub struct Membership {
    identity: Arc<str>,
    ring: Arc<RwLock<Ring>>,
    changed: tokio::sync::watch::Sender<u64>,
}
impl Membership {
    pub fn new(identity: impl Into<Arc<str>>, hosts: Vec<Host>) -> Self {
        Self {
            identity: identity.into(),
            ring: Arc::new(RwLock::new(Ring::new(hosts))),
            changed: tokio::sync::watch::channel(0).0,
        }
    }
    /// Publish placement before waking unloaders. Readers see a complete ring.
    pub fn replace(&self, hosts: Vec<Host>) {
        *self.ring.write().expect("Matching membership lock") = Ring::new(hosts);
        self.changed
            .send_modify(|generation| *generation = generation.wrapping_add(1));
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
    pub fn owns(&self, key: &str) -> Result<bool, Status> {
        Ok(self.owner(key)?.identity() == self.identity.as_ref())
    }
    pub fn remote_channel(&self, key: &str) -> Result<Option<tonic::transport::Channel>, Status> {
        let owner = self.owner(key)?;
        if owner.identity() == self.identity.as_ref() {
            return Ok(None);
        }
        if owner.grpc_address.is_empty() {
            return Err(Status::unavailable("Matching owner has no gRPC address"));
        }
        let address = if owner.grpc_address.contains("://") {
            owner.grpc_address
        } else {
            format!("http://{}", owner.grpc_address)
        };
        tonic::transport::Endpoint::from_shared(address)
            .map(|endpoint| Some(endpoint.connect_lazy()))
            .map_err(|error| Status::internal(error.to_string()))
    }
    pub fn check_task_list(&self, name: &str) -> Result<(), Status> {
        let owner = self.owner(name)?;
        if owner.identity() == self.identity.as_ref() {
            return Ok(());
        }
        Err(crate::grpc::detailed_status_type(
            tonic::Code::Aborted,
            format!(
                "task list is not owned by this host: OwnedBy: {}, Me: {}, Tasklist: {name}",
                owner.identity(),
                self.identity
            ),
            "type.googleapis.com/uber.cadence.shared.v1.TaskListNotOwnedByHostError".into(),
            cadence_proto::shared::TaskListNotOwnedByHostError {
                owned_by_identity: owner.identity().into(),
                my_identity: self.identity.to_string(),
                task_list_name: name.into(),
            },
        ))
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
