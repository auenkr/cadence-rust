//! Go `mutableStateBuilder.startTransactionHandleDecisionFailover` and the
//! cluster metadata (`clusterMetadata`, `canReplicateEvents`) it needs.

use std::sync::Arc;

use cadence_api::enums::DecisionTaskFailedCause;

use super::mutable_state_builder::{TypedMutableState, TypedMutableStateError, EMPTY_VERSION};
use super::mutable_state_methods_decision::DecisionEnv;
use super::workflow::IDENTITY_HISTORY_SERVICE;

type Result<T> = std::result::Result<T, TypedMutableStateError>;

/// Go `clusterMetadata.ClusterNameForFailoverVersion`.
pub type ClusterNameForVersionFn = Arc<dyn Fn(i64) -> std::result::Result<String, String> + Send + Sync>;

/// Cluster metadata held by Go's `mutableStateBuilder` (`e.clusterMetadata`) plus the
/// result of Go `canReplicateEvents` (domain replication policy is multi cluster and
/// `EnableReplicationTaskGeneration(domainID, workflowID)`), evaluated at load time.
#[derive(Clone)]
pub struct FailoverCluster {
    /// Go `clusterMetadata.GetCurrentClusterName()`.
    pub current_cluster: String,
    pub cluster_name_for_failover_version: ClusterNameForVersionFn,
    /// Go `canReplicateEvents()`.
    pub can_replicate_events: bool,
}

impl std::fmt::Debug for FailoverCluster {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FailoverCluster")
            .field("current_cluster", &self.current_cluster)
            .field("can_replicate_events", &self.can_replicate_events)
            .finish()
    }
}

impl TypedMutableState {
    fn failover_cluster_name(&self, cluster: &FailoverCluster, version: i64) -> Result<String> {
        (cluster.cluster_name_for_failover_version)(version).map_err(TypedMutableStateError::Internal)
    }

    /// Go `startTransactionHandleDecisionFailover`. Returns `true` when the in-flight
    /// decision was failed and a new one scheduled, so the caller must flush the state.
    pub fn start_transaction_handle_decision_failover(
        &mut self,
        env: &DecisionEnv,
        incoming_task_version: i64,
    ) -> Result<bool> {
        let cluster = match &self.failover {
            Some(c) if c.can_replicate_events => c.clone(),
            _ => return Ok(false),
        };
        if !self.is_workflow_execution_running() {
            return Ok(false);
        }

        // Handling mutable state turn from standby to active, while having a decision on the fly.
        let decision = match self.get_in_flight_decision() {
            Some(d) if d.version < self.get_current_version() => d,
            // no pending decision, no buffered events, or decision has higher / equal version
            _ => return Ok(false),
        };

        let last_write_version = self.get_last_write_version()?;
        if last_write_version != decision.version {
            return Err(TypedMutableStateError::Internal(format!(
                "mutableStateBuilder encounter mismatch version, decision: {}, last write version {}",
                decision.version, last_write_version
            )));
        }
        let last_write_source_cluster = self.failover_cluster_name(&cluster, last_write_version)?;
        let current_version = self.get_current_version();
        let current_version_cluster = self.failover_cluster_name(&cluster, current_version)?;
        let current_cluster = cluster.current_cluster.as_str();

        // handle case 5
        let incoming_task_source_cluster = self.failover_cluster_name(&cluster, incoming_task_version)?;
        if incoming_task_version != EMPTY_VERSION
            && current_version_cluster != current_cluster
            && incoming_task_source_cluster == current_cluster
        {
            return Ok(false);
        }

        const PASSIVE_BUFFERED: &str = "mutableStateBuilder encounter previous passive workflow with buffered events";
        // handle case 4
        if last_write_source_cluster != current_cluster && current_version_cluster != current_cluster {
            if self.has_buffered_events() {
                return Err(TypedMutableStateError::Internal(PASSIVE_BUFFERED.into()));
            }
            return Ok(false);
        }

        // handle case 1 & 2
        let mut flush_buffer_version = last_write_version;
        // handle case 3
        if last_write_source_cluster != current_cluster && current_version_cluster == current_cluster {
            if self.has_buffered_events() {
                return Err(TypedMutableStateError::Internal(PASSIVE_BUFFERED.into()));
            }
            flush_buffer_version = current_version;
        }

        // this workflow was previous active (whether it has buffered events or not),
        // the in flight decision must be failed to guarantee all events within same
        // event batch shard the same version
        self.update_current_version(flush_buffer_version, true)?;

        // Go `FailDecision(e, decision, FailoverCloseDecision)`.
        self.add_decision_task_failed_event(
            env,
            decision.schedule_id,
            decision.started_id,
            DecisionTaskFailedCause::FailoverCloseDecision,
            Vec::new(),
            IDENTITY_HISTORY_SERVICE,
            "",
            "",
            "",
            "",
            0,
            "",
        )?;
        self.flush_buffered_events()?;

        // Go `ScheduleDecision(e)`.
        if !self.has_pending_decision() {
            self.add_decision_task_scheduled_event(env, false)
                .map_err(|_| TypedMutableStateError::Internal("Failed to add decision scheduled event.".into()))?;
        }
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use cadence_api::enums::EventType;
    use cadence_api::types::{VersionHistories, VersionHistory, VersionHistoryItem};

    use super::*;
    use crate::execution::mutable_state_builder::EMPTY_EVENT_ID;
    use crate::execution::mutable_state_methods_common::WORKFLOW_STATE_RUNNING;

    const CURRENT: &str = "cluster-a";

    /// Failover versions: 1 -> cluster-a, 2 -> cluster-b, 3 -> cluster-a, 4 -> cluster-b, -24 -> current.
    fn cluster_for(version: i64) -> std::result::Result<String, String> {
        match version {
            EMPTY_VERSION => Ok(CURRENT.into()),
            v if v > 0 && v % 2 == 1 => Ok("cluster-a".into()),
            v if v > 0 => Ok("cluster-b".into()),
            v => Err(format!("unknown version {v}")),
        }
    }

    fn env() -> DecisionEnv {
        DecisionEnv { now_nanos: 1_000_000_000, ..Default::default() }
    }

    /// A running workflow whose last write (event 4) has `decision_version`, with decision
    /// 3/4 in flight, and `current_version` taken from the domain.
    fn state(decision_version: i64, current_version: i64, replicable: bool) -> TypedMutableState {
        let mut s = TypedMutableState::new();
        s.execution_info.state = WORKFLOW_STATE_RUNNING;
        s.execution_info.domain_id = "domain".into();
        s.execution_info.workflow_id = "wf".into();
        s.execution_info.run_id = "run".into();
        s.execution_info.task_list = "tl".into();
        s.execution_info.next_event_id = 5;
        s.execution_info.last_processed_event = EMPTY_EVENT_ID;
        s.execution_info.decision_version = decision_version;
        s.execution_info.decision_schedule_id = 3;
        s.execution_info.decision_started_id = 4;
        s.execution_info.decision_timeout_nanos = 10_000_000_000;
        s.version_histories = Some(VersionHistories {
            current_version_history_index: 0,
            histories: vec![VersionHistory {
                branch_token: vec![1],
                items: vec![VersionHistoryItem { event_id: 4, version: decision_version }],
            }],
        });
        s.current_version = current_version;
        s.failover = Some(FailoverCluster {
            current_cluster: CURRENT.into(),
            cluster_name_for_failover_version: Arc::new(cluster_for),
            can_replicate_events: replicable,
        });
        s
    }

    fn event_types(s: &TypedMutableState) -> Vec<EventType> {
        s.history_events.iter().filter_map(|e| e.event_type).collect()
    }

    #[test]
    fn noop_when_replication_disabled_or_unset() {
        let mut s = state(1, 3, false);
        assert!(!s.start_transaction_handle_decision_failover(&env(), EMPTY_VERSION).unwrap());
        s.failover = None;
        assert!(!s.start_transaction_handle_decision_failover(&env(), EMPTY_VERSION).unwrap());
        assert!(s.history_events.is_empty());
    }

    #[test]
    fn noop_when_not_running_or_no_in_flight_decision_or_version_not_lower() {
        let mut s = state(1, 3, true);
        s.execution_info.state = crate::execution::mutable_state_methods_common::WORKFLOW_STATE_COMPLETED;
        assert!(!s.start_transaction_handle_decision_failover(&env(), EMPTY_VERSION).unwrap());

        let mut s = state(1, 3, true);
        s.execution_info.decision_started_id = EMPTY_EVENT_ID;
        assert!(!s.start_transaction_handle_decision_failover(&env(), EMPTY_VERSION).unwrap());

        let mut s = state(3, 3, true);
        assert!(!s.start_transaction_handle_decision_failover(&env(), EMPTY_VERSION).unwrap());
        let mut s = state(3, 1, true);
        assert!(!s.start_transaction_handle_decision_failover(&env(), EMPTY_VERSION).unwrap());
    }

    #[test]
    fn mismatched_last_write_version_is_internal_error() {
        let mut s = state(1, 3, true);
        s.execution_info.decision_version = 2;
        let err = s.start_transaction_handle_decision_failover(&env(), EMPTY_VERSION).unwrap_err();
        assert_eq!(
            err.to_string(),
            "mutableStateBuilder encounter mismatch version, decision: 2, last write version 1"
        );
    }

    #[test]
    fn active_to_active_fails_decision_with_last_write_version_and_reschedules() {
        // decision written by cluster-a at version 1, current version 3 also cluster-a (case 2).
        let mut s = state(1, 3, true);
        assert!(s.start_transaction_handle_decision_failover(&env(), EMPTY_VERSION).unwrap());
        assert_eq!(s.get_current_version(), 1);
        assert_eq!(event_types(&s), vec![EventType::DecisionTaskFailed, EventType::DecisionTaskScheduled]);
        let failed = s.history_events[0].decision_task_failed_event_attributes.as_ref().unwrap();
        assert_eq!(failed.cause, Some(DecisionTaskFailedCause::FailoverCloseDecision));
        assert_eq!(failed.identity, IDENTITY_HISTORY_SERVICE);
        assert_eq!((failed.scheduled_event_id, failed.started_event_id), (3, 4));
        assert!(s.history_events.iter().all(|e| e.version == 1));
        assert!(s.has_pending_decision());
        assert!(!s.has_in_flight_decision());
    }

    #[test]
    fn active_to_passive_uses_last_write_version() {
        // cluster-a wrote version 1, domain failed over to cluster-b version 4 (case 1).
        let mut s = state(1, 4, true);
        assert!(s.start_transaction_handle_decision_failover(&env(), EMPTY_VERSION).unwrap());
        assert_eq!(s.get_current_version(), 1);
        assert_eq!(event_types(&s), vec![EventType::DecisionTaskFailed, EventType::DecisionTaskScheduled]);
    }

    #[test]
    fn passive_to_active_uses_current_version() {
        // cluster-b wrote version 2, domain is now active in cluster-a version 3 (case 3).
        let mut s = state(2, 3, true);
        assert!(s.start_transaction_handle_decision_failover(&env(), EMPTY_VERSION).unwrap());
        assert_eq!(s.get_current_version(), 3);
        assert!(s.history_events.iter().all(|e| e.version == 3));
        assert_eq!(event_types(&s), vec![EventType::DecisionTaskFailed, EventType::DecisionTaskScheduled]);
    }

    #[test]
    fn passive_to_active_with_buffered_events_errors() {
        let mut s = state(2, 3, true);
        s.buffered_events.push(Default::default());
        let err = s.start_transaction_handle_decision_failover(&env(), EMPTY_VERSION).unwrap_err();
        assert_eq!(
            err.to_string(),
            "mutableStateBuilder encounter previous passive workflow with buffered events"
        );
    }

    #[test]
    fn passive_to_passive_is_noop_and_checks_buffered_events() {
        // cluster-b wrote version 2, domain stays in cluster-b version 4 (case 4).
        let mut s = state(2, 4, true);
        assert!(!s.start_transaction_handle_decision_failover(&env(), EMPTY_VERSION).unwrap());
        assert!(s.history_events.is_empty());
        s.update_buffered_events.push(Default::default());
        assert!(s.start_transaction_handle_decision_failover(&env(), EMPTY_VERSION).is_err());
    }

    #[test]
    fn incoming_task_from_current_cluster_on_passive_workflow_is_noop() {
        // case 5: current version is cluster-b (passive), incoming task version 1 is cluster-a (current).
        let mut s = state(2, 4, true);
        s.buffered_events.push(Default::default());
        assert!(!s.start_transaction_handle_decision_failover(&env(), 1).unwrap());
        // the same state without that incoming version hits the case 4 sanity check
        assert!(s.start_transaction_handle_decision_failover(&env(), 2).is_err());
    }

    #[test]
    fn unknown_version_cluster_lookup_error_is_returned() {
        let mut s = state(1, 3, true);
        assert!(s.start_transaction_handle_decision_failover(&env(), -99).is_err());
    }
}
