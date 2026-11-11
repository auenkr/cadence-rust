//! group: assembly/engine_refresher
//!
//! Go `execution.mutableStateTaskRefresherImpl.RefreshTasks` over the engine
//! mutable state. Events Go reads through the events cache (start event,
//! completion event, cancel/signal initiated events) are read from the history
//! branch first (async); the task regeneration then runs synchronously.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use cadence_api::enums::{ContinueAsNewInitiator, EventType};
use cadence_api::service_types::HistoryEvent;

use crate::engine::deps::{DomainCache, HResult, HistoryError, MutableStateRef, EMPTY_EVENT_ID};
use crate::engine::deps_ms::MutableState;
use crate::engine::runtime::MutableStateTaskRefresher;
use crate::wiring::assembly::engine_state::EngineMutableState;
use crate::wiring::assembly::engine_tasks::{BACKOFF_TYPE_CRON, BACKOFF_TYPE_RETRY};

const NANOS: i64 = 1_000_000_000;

/// `NewMutableStateTaskRefresher`.
pub struct EngineTaskRefresher {
    domains: Arc<dyn DomainCache>,
}

impl EngineTaskRefresher {
    pub fn new(domains: Arc<dyn DomainCache>) -> Self {
        Self { domains }
    }

    fn target_domain_id(&self, own_domain_id: &str, name: &str) -> HResult<String> {
        if name.is_empty() {
            return Ok(own_domain_id.to_string());
        }
        self.domains.get_domain(name).map(|d| d.info.id.clone())
    }
}

#[async_trait]
impl MutableStateTaskRefresher for EngineTaskRefresher {
    async fn refresh_tasks(&self, start_timestamp: i64, state: MutableStateRef) -> HResult<()> {
        let state = state
            .as_any_arc()
            .and_then(|a| a.downcast::<EngineMutableState>().ok())
            .ok_or_else(|| HistoryError::internal("mutable state is not an EngineMutableState"))?;
        let (closed, cancels, signals, own_domain) = {
            let g = state.inner.lock().unwrap();
            (
                !g.exec.is_running(),
                g.pc_cancels.iter().map(|(k, v)| (*k, v.initiated_event_batch_id)).collect::<Vec<_>>(),
                g.pc_signals.iter().map(|(k, v)| (*k, v.initiated_event_batch_id)).collect::<Vec<_>>(),
                g.exec.domain_id.clone(),
            )
        };
        let start_event = state.ev_start_event().await?;
        let completion = if closed {
            Some(state.ev_completion_event().await?)
        } else {
            None
        };
        if closed && completion.is_none() {
            return Err(HistoryError::internal("unable to get workflow completion event"));
        }
        let mut initiated: BTreeMap<i64, HistoryEvent> = BTreeMap::new();
        for (id, batch) in cancels.into_iter().chain(signals) {
            let event = state
                .read_events(batch, id + 1)
                .await?
                .into_iter()
                .find(|e| e.event_id == id)
                .ok_or_else(|| HistoryError::internal(format!("unable to find initiated event {id} while refreshing tasks")))?;
            initiated.insert(id, event);
        }
        // Target domain ids are resolved before the (synchronous) generation.
        let mut targets: BTreeMap<i64, String> = BTreeMap::new();
        for (id, event) in &initiated {
            let name = match event.event_type {
                Some(EventType::RequestCancelExternalWorkflowExecutionInitiated) => event
                    .request_cancel_external_workflow_execution_initiated_event_attributes
                    .as_ref()
                    .map(|a| a.domain.clone()),
                _ => event
                    .signal_external_workflow_execution_initiated_event_attributes
                    .as_ref()
                    .map(|a| a.domain.clone()),
            }
            .unwrap_or_default();
            targets.insert(*id, self.target_domain_id(&own_domain, &name)?);
        }
        state.refresh_tasks_sync(start_timestamp, &start_event, completion.as_ref(), &initiated, &targets)
    }
}

impl EngineMutableState {
    /// Go `RefreshTasks` (workflow start, close, record started, decision,
    /// activity, timer, child, request cancel, signal; search attributes only
    /// with advanced visibility writing, which this assembly does not enable).
    fn refresh_tasks_sync(
        &self,
        start_time: i64,
        start_event: &HistoryEvent,
        completion: Option<&HistoryEvent>,
        initiated: &BTreeMap<i64, HistoryEvent>,
        targets: &BTreeMap<i64, String>,
    ) -> HResult<()> {
        let sticky = self.is_sticky_task_list_enabled();
        let params = self.params.clone();
        let attrs = start_event.workflow_execution_started_event_attributes.clone().unwrap_or_default();
        let backoff = attrs.first_decision_task_backoff_seconds.unwrap_or(0) as i64;
        let mut g = self.inner.lock().unwrap();

        // refreshTasksForWorkflowStart
        let mut timeout_at = start_time + g.exec.workflow_timeout as i64 * NANOS + backoff * NANOS;
        let expiration = g.pc_exec.expiration_time_unix_nanos;
        if attrs.attempt > 0 && expiration != 0 && timeout_at > expiration {
            timeout_at = expiration;
        }
        g.gen_workflow_start_timer(timeout_at, start_event.version);
        let has_pending = g.pc_exec.decision_schedule_id != EMPTY_EVENT_ID;
        if !(has_pending || g.pc_exec.last_processed_event != EMPTY_EVENT_ID) && backoff > 0 {
            let backoff_type = match attrs.initiator {
                None | Some(ContinueAsNewInitiator::CronSchedule) => BACKOFF_TYPE_CRON,
                Some(ContinueAsNewInitiator::RetryPolicy) => BACKOFF_TYPE_RETRY,
                Some(_) => {
                    return Err(HistoryError::internal("encounter continue as new iterator & first decision delay not 0"))
                }
            };
            let start_ts = start_event.timestamp.unwrap_or(0) * 1_000_000;
            g.gen_delayed_decision_timer(start_ts + backoff * NANOS, backoff_type, start_event.version);
        }

        // refreshTasksForWorkflowClose / RecordWorkflowStarted
        if let Some(event) = completion {
            let ms = event.timestamp.unwrap_or(0);
            g.gen_workflow_close_tasks(&params, ms * 1_000_000, event.version);
        } else {
            g.gen_record_workflow_started(start_event.version);
        }

        // refreshTasksForDecision
        if has_pending {
            let schedule_id = g.pc_exec.decision_schedule_id;
            if g.pc_exec.decision_started_id != EMPTY_EVENT_ID {
                g.gen_decision_start_tasks(schedule_id);
            } else {
                g.gen_decision_schedule_tasks(sticky, &params);
            }
        }

        // refreshTasksForActivity
        let schedule_ids: Vec<i64> = g.pc_acts.keys().copied().collect();
        for sid in schedule_ids {
            let started = match g.pc_acts.get_mut(&sid) {
                Some(a) => {
                    a.timer_task_status = 0;
                    a.started_id != EMPTY_EVENT_ID
                }
                None => continue,
            };
            g.refresh_activity(sid);
            if !started {
                g.gen_activity_transfer_task(sid);
            }
        }
        g.create_next_activity_timer();

        // refreshTasksForTimer
        let ids: Vec<String> = g.timers.keys().cloned().collect();
        for id in ids {
            if let Some(t) = g.timers.get_mut(&id) {
                t.task_id = 0;
            }
            g.upsert_timers.insert(id);
        }
        g.create_next_user_timer();

        // refreshTasksForChildWorkflow
        let children: Vec<(i64, String)> = g
            .pc_children
            .iter()
            .filter(|(_, c)| c.started_id == EMPTY_EVENT_ID)
            .map(|(id, c)| (*id, c.domain_id.clone()))
            .collect();
        for (id, domain) in children {
            g.gen_child_workflow_task(id, &domain);
        }

        // refreshTasksForRequestCancelExternalWorkflow
        let cancels: Vec<(i64, i64)> = g.pc_cancels.iter().map(|(id, c)| (*id, c.version)).collect();
        for (id, version) in cancels {
            let event = initiated.get(&id).ok_or_else(|| HistoryError::internal("missing initiated event"))?;
            let a = event
                .request_cancel_external_workflow_execution_initiated_event_attributes
                .clone()
                .ok_or_else(|| HistoryError::internal("missing request cancel initiated attributes"))?;
            let exec = a.workflow_execution.unwrap_or_default();
            let domain = targets.get(&id).cloned().unwrap_or_default();
            g.gen_external_task(true, id, version, &domain, &exec.workflow_id, &exec.run_id, a.child_workflow_only);
        }

        // refreshTasksForSignalExternalWorkflow (the Rust attributes do not carry ChildWorkflowOnly)
        let signals: Vec<(i64, i64)> = g.pc_signals.iter().map(|(id, s)| (*id, s.version)).collect();
        for (id, version) in signals {
            let event = initiated.get(&id).ok_or_else(|| HistoryError::internal("missing initiated event"))?;
            let a = event
                .signal_external_workflow_execution_initiated_event_attributes
                .clone()
                .ok_or_else(|| HistoryError::internal("missing signal initiated attributes"))?;
            let exec = a.workflow_execution.unwrap_or_default();
            let domain = targets.get(&id).cloned().unwrap_or_default();
            g.gen_external_task(false, id, version, &domain, &exec.workflow_id, &exec.run_id, false);
        }
        g.dirty = true;
        Ok(())
    }
}
