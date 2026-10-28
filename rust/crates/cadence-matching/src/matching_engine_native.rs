//! Native Matching engine foundations. The four RPCs are not implemented yet.
//!
//! This module is deliberately not wired into the service: `get` by itself is
//! not the handler ownership check, and bypassing the forwarding chain can lose
//! tasks. See memory/matching-native-engine-20261028.md.
use std::collections::BTreeMap;

use cadence_common::go_string::GoString;
use cadence_persistence::matching_blob::{TaskDataBytes, GO_ZERO_TIME_NANOS};
use cadence_proto::native::api;

use crate::matching_registry_native::NativeQueueRegistry;

/// Shared state for the native engine. Domain, admission, ownership and
/// configuration boundaries still need to be supplied before RPCs can run.
#[derive(Clone)]
pub struct NativeMatchingEngine {
    pub registry: NativeQueueRegistry,
}

impl NativeMatchingEngine {
    pub fn new(registry: NativeQueueRegistry) -> Self {
        Self { registry }
    }
}

/// Construct the common AddDecisionTask/AddActivityTask payload exactly as in
/// `grpc_matching.rs`: expiry is assigned by the writer, and an empty map is
/// represented as absent. Keep the request run ID as bytes until persistence.
fn add_task_data(
    execution: Option<&api::WorkflowExecution>,
    schedule_id: i64,
    partition_config: BTreeMap<GoString, GoString>,
    now: i64,
) -> TaskDataBytes {
    let execution = execution.cloned().unwrap_or_default();
    TaskDataBytes {
        workflow_id: execution.workflow_id,
        schedule_id,
        created_nanos: now,
        expiry_nanos: GO_ZERO_TIME_NANOS,
        partition_config: if partition_config.is_empty() {
            None
        } else {
            Some(partition_config)
        },
        ..Default::default()
    }
    .with_request_run_id(execution.run_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_payload_preserves_arbitrary_string_bytes() {
        let execution = api::WorkflowExecution {
            workflow_id: vec![0xff, 0, 0x80].into(),
            run_id: vec![0xfe, 0, 0xff].into(),
        };
        let mut config = BTreeMap::new();
        config.insert(vec![0xff].into(), vec![0xfe, 0].into());
        config.insert(vec![0xfe].into(), vec![0xff, 0].into());
        let data = add_task_data(Some(&execution), 42, config.clone(), 123);
        assert_eq!(data.workflow_id.as_bytes(), &[0xff, 0, 0x80]);
        assert_eq!(data.run_id_bytes(), vec![0xfe, 0, 0xff]);
        assert_eq!(data.partition_config, Some(config));
        assert_eq!((data.schedule_id, data.created_nanos, data.expiry_nanos), (42, 123, GO_ZERO_TIME_NANOS));
    }

    #[test]
    fn absent_execution_and_empty_config_follow_go_getters() {
        let data = add_task_data(None, 1, BTreeMap::new(), 2);
        assert!(data.workflow_id.is_empty());
        assert!(data.run_id_bytes().is_empty());
        assert_eq!(data.partition_config, None);
    }
}
