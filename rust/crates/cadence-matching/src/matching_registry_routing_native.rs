//! Native live-routing attachment contracts for `NativeQueueRegistry`.
//!
//! `NativeQueueRegistry::with_live_routing` owns the attachment implementation
//! and is kept beside the registry's private weak-state machinery. This module
//! gives host assembly one stable import location for the required callback
//! types. The local callback signature is
//! `Fn(NativeQueueRegistry) -> Arc<dyn NativeMatchingApi>` and is invoked once
//! per local parent dispatch, after the registry state has been upgraded from
//! a `Weak`; it must build a service from that call's registry and must not
//! retain the registry in an owner resolver. `NativePartitionRefresher` must
//! resolve the owner of the raw partition name, build the generated native
//! refresh request, preserve `deadline`, and treat owner-resolution failures
//! as logged success while propagating actual RPC errors.
//!
//! This file is deliberately byte-oriented: callers route `GoString` bytes
//! directly through `Membership` or `MatchingShardRouter`. There is no UTF-8
//! conversion or replacement-character fallback.

pub use crate::matching_registry_native::{
    NativeLocalPollServiceBuilder, NativePartitionRefresher, NativeParentRouter,
};


/// A publish-after-construction service slot. Only weak references are stored:
/// a registry owner cannot keep its host (and consequently itself) alive.
#[derive(Clone, Default)]
pub struct NativeServiceSlot {
    service: std::sync::Arc<std::sync::RwLock<Option<std::sync::Weak<dyn cadence_proto::native::NativeMatchingApi>>>>,
}

impl NativeServiceSlot {
    /// Publish the same service retained by the launcher/runtime before admission.
    pub fn publish(&self, service: &std::sync::Arc<dyn cadence_proto::native::NativeMatchingApi>) {
        *self.service.write().expect("native service slot lock") = Some(std::sync::Arc::downgrade(service));
    }

    /// Local forwarding fails closed until publication and after host teardown.
    pub fn factory(&self) -> crate::matching_registry_native::NativeLocalPollFactory {
        let slot = self.clone();
        std::sync::Arc::new(move |host| {
            let _registry = host.registry()?;
            let service = slot.service.read().expect("native service slot lock").as_ref()?.upgrade()?;
            Some(std::sync::Arc::new(crate::matching_forward_native::InProcessNativeMatching(service)))
        })
    }
}

/// Go partition notifications use the membership ring even when task dispatch
/// uses the shard distributor. Resolve local owners through their advertised
/// endpoint too, so an unloaded local partition follows ordinary RPC creation.
pub fn partition_refresher(
    membership: crate::matching_membership::Membership,
) -> NativePartitionRefresher {
    std::sync::Arc::new(move |notification| {
        let membership = membership.clone();
        Box::pin(async move {
            let channel = match membership.channel_for_task_list_owner_bytes(notification.task_list.as_bytes()) {
                Ok((_, channel)) => channel,
                Err(error) => {
                    tracing::warn!(task_list = ?notification.task_list.as_bytes(), %error, "resolving Matching partition refresh owner failed");
                    return Ok(());
                }
            };
            let deadline = notification.deadline;
            let mut request = tonic::Request::new(refresh_request(notification));
            if let Some(deadline) = deadline {
                request.set_timeout(deadline.saturating_duration_since(tokio::time::Instant::now()));
            }
            crate::matching_dispatch::matching_yarpc_grpc_metadata(&mut request);
            let mut client = cadence_proto::native_service::matching_api_client::MatchingApiClient::new(channel);
            let call = client.refresh_task_list_partition_config(request);
            match deadline {
                Some(deadline) => tokio::time::timeout_at(deadline, call).await
                    .map_err(|_| tonic::Status::deadline_exceeded("context deadline exceeded"))?,
                None => call.await,
            }.map(|_| ())
        })
    })
}

fn refresh_request(
    notification: crate::matching_registry_native::NativePartitionRefresh,
) -> cadence_proto::native::matching::RefreshTaskListPartitionConfigRequest {
    use cadence_proto::native::{api, matching};
    let config = notification.config;
    let partitions = |map: std::collections::HashMap<i32, Vec<cadence_common::go_string::GoString>>| {
        map.into_iter().map(|(id, isolation_groups)| (id, api::TaskListPartition { isolation_groups })).collect()
    };
    matching::RefreshTaskListPartitionConfigRequest {
        domain_id: notification.domain_id.to_string().into(),
        task_list: Some(api::TaskList {
            name: notification.task_list,
            kind: i32::from(notification.kind) + 1,
            ..Default::default()
        }),
        task_list_type: notification.task_type + 1,
        partition_config: Some(api::TaskListPartitionConfig {
            version: config.version,
            num_read_partitions: config.read_partitions.len() as i32,
            num_write_partitions: config.write_partitions.len() as i32,
            read_partitions: partitions(config.read_partitions),
            write_partitions: partitions(config.write_partitions),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cadence_common::go_string::GoString;
    use crate::{matching_partition_native::NativeStoredPartitionConfig, matching_registry_native::NativePartitionRefresh};
    use prost::Message;

    #[test]
    fn refresh_request_keeps_raw_partition_and_groups() {
        let groups = vec![GoString::from(&b"\xff"[..]), GoString::from(&b"\xfe"[..]), GoString::from("\u{fffd}")];
        let config = NativeStoredPartitionConfig {
            version: 19,
            read_partitions: std::collections::HashMap::from([(2, groups.clone())]),
            write_partitions: std::collections::HashMap::from([(0, groups.clone())]),
        };
        let request = refresh_request(NativePartitionRefresh {
            domain_id: uuid::Uuid::nil(), task_list: GoString::from(&b"/__cadence_sys/root\xff/2"[..]),
            kind: 0, task_type: 1, config, deadline: None,
        });
        let decoded = cadence_proto::native::matching::RefreshTaskListPartitionConfigRequest::decode(request.encode_to_vec().as_slice()).unwrap();
        assert_eq!(decoded.task_list.unwrap().name.as_bytes(), b"/__cadence_sys/root\xff/2");
        assert_eq!(decoded.task_list_type, 2);
        let config = decoded.partition_config.unwrap();
        assert_eq!(config.version, 19);
        assert_eq!(config.num_read_partitions, 1);
        assert_eq!(config.num_write_partitions, 1);
        assert_eq!(config.read_partitions[&2].isolation_groups, groups);
        assert_eq!(config.write_partitions[&0].isolation_groups, groups);
    }
}
