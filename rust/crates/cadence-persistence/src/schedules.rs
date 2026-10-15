//! Native schedule engine ported from Go's scheduler worker
//! (`go/service/worker/scheduler/`) plus the frontend CRUD surface
//! (`go/service/frontend/api/handler_schedule.go`).
//!
//! Go implements schedules as `cadence-scheduler:<id>` workflows driven by a
//! scheduler worker. A single-host port cannot rely on worker execution, so
//! schedules live natively in the execution store: the same cron evaluation,
//! overlap, catch-up, backfill and pause semantics, with target workflows
//! started directly by the server's background loop.

use std::collections::VecDeque;

use cadence_api::enums::{
    ScheduleCatchUpPolicy, ScheduleOverlapPolicy, WorkflowExecutionCloseStatus,
};
use cadence_api::schedule::{
    BackfillInfo, ScheduleAction, ScheduleInfo, ScheduleListEntry, SchedulePauseInfo,
    SchedulePolicies, ScheduleSpec, ScheduleState,
};
use cadence_api::types::{Memo, SearchAttributes, TaskList};

/// Default catch-up window mirroring Go's `defaultCatchUpWindow` (365 days).
pub const DEFAULT_CATCH_UP_WINDOW_NANOS: i64 = 365 * 24 * 60 * 60 * 1_000_000_000;
/// Go's `defaultTaskStartToCloseTimeoutSeconds` applied at fire time when the
/// action leaves the decision timeout unset.
pub const DEFAULT_TASK_START_TO_CLOSE_TIMEOUT_SECONDS: i32 = 10;
/// Bound on cron iterations per schedule per tick, mirroring Go's budgeted
/// fire processing.
const MAX_FIRES_PER_TICK: usize = 100;

/// Durable cursor for a queued Go scheduler BackfillRequest.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PendingBackfill {
    pub backfill_id: String,
    pub cursor_nanos: i64,
    pub end_nanos: i64,
    pub overlap: ScheduleOverlapPolicy,
}

/// A stored schedule: configuration plus runtime firing state.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ScheduleRecord {
    pub domain: String,
    pub schedule_id: String,
    pub spec: ScheduleSpec,
    pub action: ScheduleAction,
    pub policies: SchedulePolicies,
    pub paused: bool,
    pub pause_reason: String,
    pub paused_by: String,
    pub paused_at_nanos: Option<i64>,
    pub memo: Option<Memo>,
    pub search_attributes: Option<SearchAttributes>,
    pub create_nanos: i64,
    pub update_nanos: i64,
    /// Watermark: last fire time fully processed (nanos).
    pub last_fire_nanos: Option<i64>,
    pub last_run_nanos: Option<i64>,
    pub next_run_nanos: Option<i64>,
    pub total_runs: i64,
    pub missed_runs: i64,
    pub skipped_runs: i64,
    /// Fired target runs still believed open: (workflow_id, run_id).
    pub running: Vec<(String, String)>,
    /// Buffered fire times (BUFFER overlap policy).
    pub buffer: VecDeque<i64>,
    /// Backfill accounting; descriptions expose only pending entries.
    pub backfills: Vec<BackfillInfo>,
    #[serde(default)]
    pub pending_backfills: VecDeque<PendingBackfill>,
    /// One-shot catch-up override from UnpauseSchedule.
    pub unpause_catchup: Option<ScheduleCatchUpPolicy>,
    /// Go LastProcessedTime: includes catch-up fires skipped by policy.
    #[serde(default)]
    pub last_processed_nanos: Option<i64>,
    /// Continue catch-up on later ticks when the per-tick budget is exhausted.
    #[serde(default)]
    pub catch_up_pending: bool,
    /// Tombstone for deleted schedules (idempotent delete like Go).
    pub deleted_at_nanos: Option<i64>,
}

impl ScheduleRecord {
    fn state(&self) -> ScheduleState {
        ScheduleState {
            paused: self.paused,
            pause_info: self.paused.then(|| SchedulePauseInfo {
                reason: self.pause_reason.clone(),
                paused_time_nano: self.paused_at_nanos,
                paused_by: self.paused_by.clone(),
            }),
        }
    }

    fn info(&self) -> ScheduleInfo {
        ScheduleInfo {
            last_run_time_nano: self.last_run_nanos,
            next_run_time_nano: self.next_run_nanos,
            total_runs: self.total_runs,
            create_time_nano: Some(self.create_nanos),
            last_update_time_nano: Some(self.update_nanos),
            ongoing_backfills: self
                .pending_backfills
                .iter()
                .filter_map(|pending| {
                    self.backfills
                        .iter()
                        .find(|b| b.backfill_id == pending.backfill_id)
                        .cloned()
                })
                .collect(),
            missed_runs: self.missed_runs,
            skipped_runs: self.skipped_runs,
            buffered_fire_count: self.buffer.len() as i64,
            running_workflow_count: self.running.len() as i64,
        }
    }

    pub fn list_entry(&self) -> ScheduleListEntry {
        ScheduleListEntry {
            schedule_id: self.schedule_id.clone(),
            workflow_type: self
                .action
                .start_workflow
                .as_ref()
                .and_then(|a| a.workflow_type.clone()),
            state: Some(self.state()),
            cron_expression: self.spec.cron_expression.clone(),
            memo: self.memo.clone(),
            search_attributes: self.search_attributes.clone(),
        }
    }
}

/// Errors for schedule CRUD, mirroring Go's frontend mapping: duplicate
/// schedules are `BadRequest` ("already exists"), unknown schedules are
/// `EntityNotExists`.
#[derive(Debug, thiserror::Error)]
pub enum ScheduleError {
    #[error(transparent)]
    NotFound(#[from] cadence_api::errors::EntityNotExistsError),
    #[error("{0}")]
    Invalid(String),
    #[error("{0}")]
    Exists(String),
}

pub(crate) fn now_nanos() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

/// Deterministic fire request ID mirroring Go's `generateRequestID`
/// (UUIDv5 over schedule, fire time and trigger source), so redelivered
/// fires deduplicate server-side.
pub fn fire_request_id(schedule_id: &str, fire_nanos: i64, trigger: &str) -> String {
    uuid::Uuid::new_v5(
        &uuid::Uuid::NAMESPACE_OID,
        format!("{schedule_id}-{fire_nanos}-{trigger}").as_bytes(),
    )
    .to_string()
}

/// Fired workflow ID mirroring Go's `generateWorkflowID`: the action prefix
/// (defaulting to the schedule ID) plus the fire time in RFC3339 UTC.
pub fn fire_workflow_id(prefix: &str, schedule_id: &str, fire_nanos: i64) -> String {
    let prefix = if prefix.is_empty() {
        schedule_id
    } else {
        prefix
    };
    let dt = cadence_common::cron::naive_from_nanos(fire_nanos);
    format!(
        "{prefix}-{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        dt.date().year(),
        dt.date().month(),
        dt.date().day(),
        dt.time().hour(),
        dt.time().minute(),
        dt.time().second(),
    )
}

use chrono::{Datelike, Timelike};

/// Next fire time after `after_nanos` for a cron expression, in nanos.
/// Returns `None` for unparseable or never-firing schedules.
pub fn next_fire_nanos(cron_expression: &str, after_nanos: i64) -> Option<i64> {
    let schedule = cadence_common::cron::parse_standard(cron_expression).ok()?;
    let after = cadence_common::cron::naive_from_nanos(after_nanos);
    let next = schedule.next(after)?;
    next.and_utc().timestamp_nanos_opt()
}

/// Go scheduler `computeNextRunTime`: inclusive StartTime/EndTime bounds.
/// Before StartTime Go uses StartTime minus one second as the cron cursor.
pub fn next_spec_fire_nanos(spec: &ScheduleSpec, after_nanos: i64) -> Option<i64> {
    let schedule = cadence_common::cron::parse_standard(&spec.cron_expression).ok()?;
    next_spec_fire(&schedule, spec, after_nanos)
}

fn next_spec_fire(
    schedule: &cadence_common::cron::Schedule,
    spec: &ScheduleSpec,
    after_nanos: i64,
) -> Option<i64> {
    let cursor = match spec.start_time_nano {
        Some(start) if after_nanos < start => start.checked_sub(1_000_000_000)?,
        _ => after_nanos,
    };
    let next = schedule
        .next(cadence_common::cron::naive_from_nanos(cursor))?
        .and_utc()
        .timestamp_nanos_opt()?;
    if spec.end_time_nano.is_some_and(|end| next > end) {
        None
    } else {
        Some(next)
    }
}

/// Fire times in `(from_nanos, to_nanos]`, bounded like Go's budgeted batches.
pub fn fires_in_range(
    cron_expression: &str,
    from_nanos: i64,
    to_nanos: i64,
    limit: usize,
) -> Vec<i64> {
    spec_fires_in_range(
        &ScheduleSpec {
            cron_expression: cron_expression.to_string(),
            ..Default::default()
        },
        from_nanos,
        to_nanos,
        limit,
    )
}

pub fn spec_fires_in_range(
    spec: &ScheduleSpec,
    from_nanos: i64,
    to_nanos: i64,
    limit: usize,
) -> Vec<i64> {
    let Ok(schedule) = cadence_common::cron::parse_standard(&spec.cron_expression) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    let mut cursor = from_nanos;
    for _ in 0..limit {
        match next_spec_fire(&schedule, spec, cursor) {
            Some(next) if next <= to_nanos => {
                out.push(next);
                cursor = next;
            }
            _ => break,
        }
    }
    out
}

/// Apply a catch-up policy to missed fire times, mirroring Go's
/// `applyMissedRunPolicy`. Returns `(to_fire, skipped)`.
pub fn apply_catch_up_policy(
    policy: ScheduleCatchUpPolicy,
    window_nanos: i64,
    missed: &[i64],
    now_nanos: i64,
) -> (Vec<i64>, i64) {
    let eligible: Vec<i64> = missed
        .iter()
        .copied()
        .filter(|t| window_nanos <= 0 || now_nanos.saturating_sub(*t) <= window_nanos)
        .collect();
    let out_of_window = (missed.len() - eligible.len()) as i64;
    match policy {
        ScheduleCatchUpPolicy::One => match eligible.last() {
            Some(last) => (vec![*last], out_of_window + (eligible.len() as i64 - 1)),
            None => (Vec::new(), missed.len() as i64),
        },
        ScheduleCatchUpPolicy::All => (eligible, out_of_window),
        // Skip and Invalid both skip everything, like Go's default branch.
        ScheduleCatchUpPolicy::Skip | ScheduleCatchUpPolicy::Invalid => {
            (Vec::new(), missed.len() as i64)
        }
        _ => (Vec::new(), missed.len() as i64),
    }
}

fn catch_up_window_nanos(policies: &SchedulePolicies) -> i64 {
    let uses_window = matches!(
        policies.catch_up_policy,
        Some(ScheduleCatchUpPolicy::One) | Some(ScheduleCatchUpPolicy::All)
    );
    if !uses_window {
        return 0;
    }
    match policies.catch_up_window_in_seconds {
        Some(w) if w > 0 => i64::from(w) * 1_000_000_000,
        _ => DEFAULT_CATCH_UP_WINDOW_NANOS,
    }
}

impl crate::executions::InMemoryExecutionStore {
    fn schedule_key(
        inner: &crate::executions::Inner,
        domain: &str,
        schedule_id: &str,
    ) -> (String, String) {
        (inner.domain_id(domain), schedule_id.to_string())
    }

    /// Go restarts scheduler workflows through their catch-up path. Restored
    /// native schedules likewise apply policy before resuming live timers.
    pub(crate) fn resume_schedules_after_restart(&self) {
        let mut inner = self.inner.write().expect("execution store lock");
        for record in inner.schedules.values_mut() {
            if !record.paused && !record.deleted() {
                record.catch_up_pending = true;
            }
        }
    }

    /// Create a schedule, mirroring Go `CreateSchedule` after validation: a
    /// running schedule with the same ID is a user-visible "already exists"
    /// `BadRequest` (Go converts `WorkflowExecutionAlreadyStartedError`).
    pub fn create_schedule(
        &self,
        domain: &str,
        req: &cadence_api::schedule::CreateScheduleRequest,
    ) -> Result<String, ScheduleError> {
        let mut inner = self.inner.write().expect("execution store lock");
        let namespace = inner.domain_id(domain);
        let key = (namespace.clone(), req.schedule_id.clone());
        if inner.schedules.get(&key).is_some_and(|r| !r.deleted()) {
            return Err(ScheduleError::Exists(format!(
                "schedule {:?} already exists in domain {:?}",
                req.schedule_id, req.domain
            )));
        }
        let now = now_nanos();
        let paused = req.state.as_ref().is_some_and(|s| s.paused);
        let (pause_reason, paused_by, paused_at) =
            match req.state.as_ref().and_then(|s| s.pause_info.as_ref()) {
                Some(info) if paused => (info.reason.clone(), info.paused_by.clone(), Some(now)),
                _ => (String::new(), String::new(), paused.then_some(now)),
            };
        let record = ScheduleRecord {
            domain: namespace.clone(),
            schedule_id: req.schedule_id.clone(),
            spec: req.spec.clone().unwrap_or_default(),
            action: req.action.clone().unwrap_or_default(),
            policies: req.policies.clone().unwrap_or_default(),
            paused,
            pause_reason,
            paused_by,
            paused_at_nanos: paused_at,
            memo: req.memo.clone(),
            search_attributes: req.search_attributes.clone(),
            create_nanos: now,
            update_nanos: now,
            last_fire_nanos: None,
            last_run_nanos: None,
            next_run_nanos: if paused {
                None
            } else {
                next_spec_fire_nanos(&req.spec.clone().unwrap_or_default(), now)
            },
            total_runs: 0,
            missed_runs: 0,
            skipped_runs: 0,
            running: Vec::new(),
            buffer: VecDeque::new(),
            backfills: Vec::new(),
            pending_backfills: VecDeque::new(),
            unpause_catchup: None,
            last_processed_nanos: None,
            catch_up_pending: false,
            deleted_at_nanos: None,
        };
        inner.schedules.insert(key, record);
        Ok(req.schedule_id.clone())
    }

    /// Describe a schedule with freshly computed next-fire info.
    pub fn describe_schedule(
        &self,
        domain: &str,
        schedule_id: &str,
    ) -> Result<cadence_api::schedule::DescribeScheduleResponse, ScheduleError> {
        let mut inner = self.inner.write().expect("execution store lock");
        let key = Self::schedule_key(&inner, domain, schedule_id);
        let record = inner.schedules.get_mut(&key).ok_or_else(|| {
            ScheduleError::NotFound(cadence_api::errors::EntityNotExistsError::new(format!(
                "schedule {schedule_id:?} does not exist in domain {domain:?}"
            )))
        })?;
        if record.deleted() {
            return Err(ScheduleError::NotFound(
                cadence_api::errors::EntityNotExistsError::new(format!(
                    "schedule {schedule_id:?} does not exist in domain {domain:?}"
                )),
            ));
        }
        record.refresh_next_run(now_nanos());
        Ok(cadence_api::schedule::DescribeScheduleResponse {
            spec: Some(record.spec.clone()),
            action: Some(record.action.clone()),
            policies: Some(record.policies.clone()),
            state: Some(record.state()),
            info: Some(record.info()),
            memo: record.memo.clone(),
            search_attributes: record.search_attributes.clone(),
        })
    }

    /// Update schedule spec/action/policies/search attributes wholesale.
    /// At least one field must be set, like Go.
    pub fn update_schedule(
        &self,
        req: &cadence_api::schedule::UpdateScheduleRequest,
    ) -> Result<(), ScheduleError> {
        let mut inner = self.inner.write().expect("execution store lock");
        let key = Self::schedule_key(&inner, &req.domain, &req.schedule_id);
        let record = inner.schedules.get_mut(&key).ok_or_else(|| {
            ScheduleError::NotFound(cadence_api::errors::EntityNotExistsError::new(format!(
                "schedule {:?} does not exist in domain {:?}",
                req.schedule_id, req.domain
            )))
        })?;
        if record.deleted() {
            return Err(ScheduleError::NotFound(
                cadence_api::errors::EntityNotExistsError::new(format!(
                    "schedule {:?} does not exist in domain {:?}",
                    req.schedule_id, req.domain
                )),
            ));
        }
        if let Some(spec) = req.spec.clone() {
            record.spec = spec;
            record.last_fire_nanos = None;
        }
        if let Some(action) = req.action.clone() {
            record.action = action;
        }
        if let Some(policies) = req.policies.clone() {
            record.policies = policies;
        }
        if let Some(attrs) = req.search_attributes.clone() {
            record.search_attributes = Some(attrs);
        }
        let now = now_nanos();
        record.update_nanos = now;
        record.refresh_next_run(now);
        Ok(())
    }

    /// Delete a schedule: tombstone the record idempotently like Go's
    /// signal-then-terminate path (already-gone is success).
    pub fn delete_schedule(&self, domain: &str, schedule_id: &str) -> Result<(), ScheduleError> {
        let mut inner = self.inner.write().expect("execution store lock");
        let key = Self::schedule_key(&inner, domain, schedule_id);
        match inner.schedules.get_mut(&key) {
            Some(record) if !record.deleted() => {
                record.deleted_at_nanos = Some(now_nanos());
                Ok(())
            }
            // Already gone is success, mirroring Go's idempotent delete.
            _ => Ok(()),
        }
    }

    /// Pause a schedule, recording reason/actor like Go's pause signal.
    pub fn pause_schedule(
        &self,
        domain: &str,
        schedule_id: &str,
        reason: &str,
        identity: &str,
    ) -> Result<(), ScheduleError> {
        let mut inner = self.inner.write().expect("execution store lock");
        let key = Self::schedule_key(&inner, domain, schedule_id);
        let record = inner.schedules.get_mut(&key).ok_or_else(|| {
            ScheduleError::NotFound(cadence_api::errors::EntityNotExistsError::new(format!(
                "schedule {schedule_id:?} does not exist in domain {domain:?}"
            )))
        })?;
        if record.deleted() {
            return Err(ScheduleError::NotFound(
                cadence_api::errors::EntityNotExistsError::new(format!(
                    "schedule {schedule_id:?} does not exist in domain {domain:?}"
                )),
            ));
        }
        record.paused = true;
        record.pause_reason = reason.to_string();
        record.paused_by = identity.to_string();
        record.paused_at_nanos = Some(now_nanos());
        record.update_nanos = now_nanos();
        Ok(())
    }

    /// Unpause a schedule, applying the catch-up policy (or one-shot
    /// override) to fires missed while paused, like Go's unpause signal.
    pub fn unpause_schedule(
        &self,
        domain: &str,
        schedule_id: &str,
        catch_up: Option<ScheduleCatchUpPolicy>,
    ) -> Result<(), ScheduleError> {
        let key = {
            let mut inner = self.inner.write().expect("execution store lock");
            let key = Self::schedule_key(&inner, domain, schedule_id);
            let record = inner
                .schedules
                .get_mut(&key)
                .filter(|r| !r.deleted())
                .ok_or_else(|| {
                    ScheduleError::NotFound(cadence_api::errors::EntityNotExistsError::new(
                        format!("schedule {schedule_id:?} does not exist in domain {domain:?}"),
                    ))
                })?;
            record.paused = false;
            record.pause_reason.clear();
            record.paused_by.clear();
            record.paused_at_nanos = None;
            record.unpause_catchup = catch_up.filter(|p| *p != ScheduleCatchUpPolicy::Invalid);
            record.catch_up_pending = true;
            record.update_nanos = now_nanos();
            key
        };
        self.process_schedule_catch_up(&key.0, &key.1, now_nanos());
        Ok(())
    }

    /// Go processMissedRunsAt, with the per-execution activity budget mapped
    /// to a bounded tick. Persist both the watermark and one-shot override
    /// across batches so skipped fires cannot become live fires on restart.
    fn process_schedule_catch_up(&self, domain: &str, id: &str, now: i64) -> usize {
        let (spec, watermark, policy, window) = {
            let inner = self.inner.read().expect("execution store lock");
            let Some(record) = inner.schedules.get(&(domain.to_string(), id.to_string())) else {
                return 0;
            };
            if record.paused || record.deleted() || !record.catch_up_pending {
                return 0;
            }
            (
                record.spec.clone(),
                record.catch_up_watermark(),
                record
                    .unpause_catchup
                    .or(record.policies.catch_up_policy)
                    .unwrap_or(ScheduleCatchUpPolicy::Skip),
                catch_up_window_nanos(&record.policies),
            )
        };
        // Go caps each scan at 1000, independently of the activity budget.
        let missed = spec_fires_in_range(&spec, watermark, now, 1000);
        let (to_fire, skipped) = apply_catch_up_policy(policy, window, &missed, now);
        let processed = to_fire.len().min(MAX_FIRES_PER_TICK);
        let mut started = 0;
        for fire in to_fire.iter().take(processed) {
            if self.fire_schedule_at(
                domain,
                id,
                *fire,
                ScheduleOverlapPolicy::Invalid,
                "schedule",
            ) {
                started += 1;
            }
        }
        let mut inner = self.inner.write().expect("execution store lock");
        if let Some(record) = inner
            .schedules
            .get_mut(&(domain.to_string(), id.to_string()))
        {
            record.skipped_runs += skipped;
            let partial = processed < to_fire.len();
            let last = if partial {
                to_fire.get(processed - 1)
            } else {
                missed.last()
            };
            if let Some(last) = last {
                record.last_processed_nanos =
                    Some(record.last_processed_nanos.unwrap_or(*last).max(*last));
            }
            record.catch_up_pending = partial || missed.len() == 1000;
            if !record.catch_up_pending {
                record.unpause_catchup = None;
            }
            record.refresh_next_run(now);
        }
        started
    }

    /// Queue a backfill like Go handleBackfill. Paused requests stay queued,
    /// pending IDs deduplicate, and a full queue ignores further signals.
    pub fn backfill_schedule(
        &self,
        domain: &str,
        schedule_id: &str,
        start_nanos: i64,
        end_nanos: i64,
        overlap: Option<ScheduleOverlapPolicy>,
        backfill_id: &str,
    ) -> Result<(), ScheduleError> {
        let key = {
            let mut inner = self.inner.write().expect("execution store lock");
            let key = Self::schedule_key(&inner, domain, schedule_id);
            let record = inner
                .schedules
                .get_mut(&key)
                .filter(|r| !r.deleted())
                .ok_or_else(|| {
                    ScheduleError::NotFound(cadence_api::errors::EntityNotExistsError::new(
                        format!("schedule {schedule_id:?} does not exist in domain {domain:?}"),
                    ))
                })?;
            if end_nanos <= start_nanos
                || record.pending_backfills.len() >= 10
                || (!backfill_id.is_empty()
                    && record
                        .pending_backfills
                        .iter()
                        .any(|b| b.backfill_id == backfill_id))
            {
                return Ok(());
            }
            let id = if backfill_id.is_empty() {
                uuid::Uuid::new_v4().to_string()
            } else {
                backfill_id.to_string()
            };
            let cursor = start_nanos.saturating_sub(1_000_000_000);
            let total = spec_fires_in_range(&record.spec, cursor, end_nanos, 100_000).len() as i32;
            record.backfills.retain(|b| b.backfill_id != id);
            record.backfills.push(BackfillInfo {
                backfill_id: id.clone(),
                start_time_nano: Some(start_nanos),
                end_time_nano: Some(end_nanos),
                runs_completed: 0,
                runs_total: total,
            });
            record.pending_backfills.push_back(PendingBackfill {
                backfill_id: id,
                cursor_nanos: cursor,
                end_nanos,
                overlap: overlap.unwrap_or(ScheduleOverlapPolicy::Invalid),
            });
            key
        };
        self.process_schedule_backfills(&key.0, &key.1);
        Ok(())
    }

    /// Budgeted Go processBackfills: completed/overlap-skipped/buffered fires
    /// all advance progress. Remaining cursors survive pause and restart.
    fn process_schedule_backfills(&self, domain: &str, id: &str) -> usize {
        let key = (domain.to_string(), id.to_string());
        let mut budget = MAX_FIRES_PER_TICK;
        let mut started = 0;
        while budget > 0 {
            let (spec, pending) = {
                let inner = self.inner.read().expect("execution store lock");
                let Some(record) = inner.schedules.get(&key) else {
                    break;
                };
                if record.paused || record.deleted() {
                    break;
                }
                let Some(pending) = record.pending_backfills.front() else {
                    break;
                };
                (record.spec.clone(), pending.clone())
            };
            let fires =
                spec_fires_in_range(&spec, pending.cursor_nanos, pending.end_nanos, budget + 1);
            let count = fires.len().min(budget);
            for fire in fires.iter().take(count) {
                if self.fire_schedule_at(
                    domain,
                    id,
                    *fire,
                    pending.overlap,
                    &format!("backfill:{}", pending.backfill_id),
                ) {
                    started += 1;
                }
            }
            let mut inner = self.inner.write().expect("execution store lock");
            let Some(record) = inner.schedules.get_mut(&key) else {
                break;
            };
            if let Some(info) = record
                .backfills
                .iter_mut()
                .find(|b| b.backfill_id == pending.backfill_id)
            {
                info.runs_completed += count as i32;
                if let Some(next) = fires.get(count) {
                    info.start_time_nano = Some(*next);
                }
            }
            if fires.len() > count {
                if let Some(front) = record.pending_backfills.front_mut() {
                    front.cursor_nanos = fires[count - 1];
                }
            } else {
                record.pending_backfills.pop_front();
            }
            budget -= count;
        }
        started
    }

    /// List schedules in a domain with opaque offset cursors. Returns
    /// `(entries, next_page_token)`; an empty token ends pagination.
    pub fn list_schedules(
        &self,
        domain: &str,
        page_size: usize,
        page_token: &[u8],
    ) -> (Vec<ScheduleListEntry>, Vec<u8>) {
        let inner = self.inner.read().expect("execution store lock");
        let namespace = inner.domain_id(domain);
        let mut ids: Vec<&String> = inner
            .schedules
            .iter()
            .filter(|((d, _), r)| d == &namespace && !r.deleted())
            .map(|((_, id), _)| id)
            .collect();
        ids.sort();
        let offset: usize = if page_token.is_empty() {
            0
        } else {
            std::str::from_utf8(page_token)
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0)
        };
        let ids_len = ids.len();
        let entries: Vec<ScheduleListEntry> = ids
            .into_iter()
            .skip(offset)
            .take(page_size.max(1))
            .filter_map(|id| {
                inner
                    .schedules
                    .get(&(namespace.clone(), id.clone()))
                    .map(ScheduleRecord::list_entry)
            })
            .collect();
        let next = offset + entries.len();
        let next_page_token = if next < ids_len {
            next.to_string().into_bytes()
        } else {
            Vec::new()
        };
        (entries, next_page_token)
    }

    /// Fire all due schedules at `now_nanos`. Returns the number of target
    /// workflows started. Called by the server's background loop.
    pub fn fire_due_schedules(&self, now_nanos: i64) -> usize {
        let catch_up: Vec<(String, String)> = {
            let inner = self.inner.read().expect("execution store lock");
            inner
                .schedules
                .iter()
                .filter(|(_, r)| r.catch_up_pending && !r.paused && !r.deleted())
                .map(|(key, _)| key.clone())
                .collect()
        };
        let backfills: Vec<(String, String)> = {
            let inner = self.inner.read().expect("execution store lock");
            inner
                .schedules
                .iter()
                .filter(|(_, r)| !r.pending_backfills.is_empty() && !r.paused && !r.deleted())
                .map(|(key, _)| key.clone())
                .collect()
        };
        let mut started = 0;
        for (domain, id) in backfills {
            started += self.process_schedule_backfills(&domain, &id);
        }
        for (domain, id) in &catch_up {
            started += self.process_schedule_catch_up(domain, id, now_nanos);
        }
        // Collect due fires first to keep borrows simple.
        let due: Vec<(String, String, i64)> = {
            let inner = self.inner.read().expect("execution store lock");
            let mut due = Vec::new();
            for ((domain, id), record) in inner.schedules.iter() {
                if record.paused
                    || record.deleted()
                    || catch_up.contains(&(domain.clone(), id.clone()))
                {
                    continue;
                }
                let from = record.catch_up_watermark();
                for fire in spec_fires_in_range(&record.spec, from, now_nanos, MAX_FIRES_PER_TICK) {
                    due.push((domain.clone(), id.clone(), fire));
                }
            }
            due
        };
        for (domain, id, fire) in due {
            if self.fire_schedule_at(
                &domain,
                &id,
                fire,
                ScheduleOverlapPolicy::Invalid,
                "schedule",
            ) {
                started += 1;
            }
        }
        // Drain buffers for BUFFER schedules with nothing running.
        self.drain_schedule_buffers();
        // Refresh next-run hints.
        {
            let mut inner = self.inner.write().expect("execution store lock");
            for record in inner.schedules.values_mut() {
                record.refresh_next_run(now_nanos);
            }
        }
        started
    }

    /// Fire one scheduled time, applying the effective overlap policy.
    /// Returns `true` when a target workflow was started.
    fn fire_schedule_at(
        &self,
        domain: &str,
        schedule_id: &str,
        fire_nanos: i64,
        overlap_override: ScheduleOverlapPolicy,
        trigger: &str,
    ) -> bool {
        use cadence_api::types::StartWorkflowExecutionRequest;
        // Snapshot the firing inputs.
        let (action, overlap, concurrency_limit, prefix) = {
            let inner = self.inner.read().expect("execution store lock");
            match inner
                .schedules
                .get(&(domain.to_string(), schedule_id.to_string()))
            {
                Some(r) if !r.deleted() && !r.paused => (
                    r.action.clone(),
                    if overlap_override == ScheduleOverlapPolicy::Invalid {
                        r.policies
                            .overlap_policy
                            .unwrap_or(ScheduleOverlapPolicy::Invalid)
                    } else {
                        overlap_override
                    },
                    r.policies.concurrency_limit,
                    r.action
                        .start_workflow
                        .as_ref()
                        .map(|a| a.workflow_id_prefix.clone())
                        .unwrap_or_default(),
                ),
                _ => return false,
            }
        };
        let sw = match action.start_workflow {
            Some(sw) => sw,
            None => return false,
        };
        // Prune running list to actually-open runs and detect overlap.
        let running: Vec<(String, String)> = {
            let inner = self.inner.read().expect("execution store lock");
            inner
                .schedules
                .get(&(domain.to_string(), schedule_id.to_string()))
                .map(|r| {
                    r.running
                        .iter()
                        .filter(|(w, run)| {
                            inner
                                .by_run
                                .get(&(domain.to_string(), w.clone(), run.clone()))
                                .is_some_and(|rec| rec.is_open())
                        })
                        .cloned()
                        .collect()
                })
                .unwrap_or_default()
        };
        {
            let mut inner = self.inner.write().expect("execution store lock");
            if let Some(r) = inner
                .schedules
                .get_mut(&(domain.to_string(), schedule_id.to_string()))
            {
                r.running = running.clone();
            }
        }
        let effective = if overlap == ScheduleOverlapPolicy::Invalid {
            ScheduleOverlapPolicy::SkipNew
        } else {
            overlap
        };
        match effective {
            ScheduleOverlapPolicy::SkipNew => {
                if !running.is_empty() {
                    self.note_schedule_skip(domain, schedule_id, fire_nanos);
                    return false;
                }
            }
            ScheduleOverlapPolicy::Concurrent => {
                if concurrency_limit > 0 && running.len() >= concurrency_limit as usize {
                    self.note_schedule_skip(domain, schedule_id, fire_nanos);
                    return false;
                }
            }
            ScheduleOverlapPolicy::Buffer => {
                if !running.is_empty() {
                    let mut inner = self.inner.write().expect("execution store lock");
                    if let Some(r) = inner
                        .schedules
                        .get_mut(&(domain.to_string(), schedule_id.to_string()))
                    {
                        let limit = r.policies.buffer_limit;
                        if limit <= 0 || (r.buffer.len() as i32) < limit {
                            if !r.buffer.contains(&fire_nanos) {
                                r.buffer.push_back(fire_nanos);
                            }
                        } else {
                            r.skipped_runs += 1;
                        }
                        r.last_fire_nanos = Some(fire_nanos);
                    }
                    return false;
                }
            }
            ScheduleOverlapPolicy::CancelPrevious | ScheduleOverlapPolicy::TerminatePrevious => {
                for (w, run) in &running {
                    self.close_schedule_target(domain, w, run, effective);
                }
            }
            ScheduleOverlapPolicy::Invalid => {
                self.note_schedule_skip(domain, schedule_id, fire_nanos);
                return false;
            }
            _ => {
                self.note_schedule_skip(domain, schedule_id, fire_nanos);
                return false;
            }
        }
        let workflow_id = fire_workflow_id(&prefix, schedule_id, fire_nanos);
        let task_timeout = match sw.task_start_to_close_timeout_seconds {
            Some(v) if v > 0 => Some(v),
            _ => Some(DEFAULT_TASK_START_TO_CLOSE_TIMEOUT_SECONDS),
        };
        let req = StartWorkflowExecutionRequest {
            domain: self.domain_name_str(domain),
            workflow_id: workflow_id.clone(),
            workflow_type: sw.workflow_type.clone(),
            task_list: sw.task_list.clone().map(|t| TaskList {
                name: t.name.clone(),
                ..Default::default()
            }),
            input: sw.input.clone(),
            execution_start_to_close_timeout_seconds: sw.execution_start_to_close_timeout_seconds,
            task_start_to_close_timeout_seconds: task_timeout,
            identity: format!("schedule:{schedule_id}"),
            request_id: fire_request_id(schedule_id, fire_nanos, trigger),
            workflow_id_reuse_policy: Some(
                cadence_api::enums::WorkflowIDReusePolicy::AllowDuplicate,
            ),
            retry_policy: sw.retry_policy.clone(),
            memo: sw.memo.clone(),
            search_attributes: sw.search_attributes.clone(),
            ..Default::default()
        };
        let run_id = match self.start(&req) {
            Ok(resp) => resp.run_id,
            Err(crate::executions::StartError::AlreadyStarted(e)) if !e.run_id.is_empty() => {
                e.run_id
            }
            Err(_) => return false,
        };
        {
            let mut inner = self.inner.write().expect("execution store lock");
            if let Some(r) = inner
                .schedules
                .get_mut(&(domain.to_string(), schedule_id.to_string()))
            {
                r.running.push((workflow_id, run_id));
                r.total_runs += 1;
                r.last_run_nanos = Some(fire_nanos);
                r.last_fire_nanos = Some(fire_nanos);
                r.update_nanos = now_nanos();
                // pause_on_failure is evaluated on close, not here.
            }
        }
        // pause_on_failure: if the just-started run already closed as failed
        // (e.g. synchronous validation failure), pause immediately.
        self.check_schedule_pause_on_failure(domain, schedule_id);
        true
    }

    /// Record an overlap skip and advance the watermark past the fire.
    fn note_schedule_skip(&self, domain: &str, schedule_id: &str, fire_nanos: i64) {
        let mut inner = self.inner.write().expect("execution store lock");
        if let Some(r) = inner
            .schedules
            .get_mut(&(domain.to_string(), schedule_id.to_string()))
        {
            r.skipped_runs += 1;
            r.last_fire_nanos = Some(fire_nanos);
        }
    }

    /// Close a running target for CANCEL_PREVIOUS / TERMINATE_PREVIOUS.
    fn close_schedule_target(
        &self,
        domain: &str,
        workflow_id: &str,
        run_id: &str,
        overlap: ScheduleOverlapPolicy,
    ) {
        use cadence_api::service_types::HistoryEvent;
        let mut inner = self.inner.write().expect("execution store lock");
        let key = (
            domain.to_string(),
            workflow_id.to_string(),
            run_id.to_string(),
        );
        if !inner.by_run.get(&key).is_some_and(|r| r.is_open()) {
            return;
        }
        let (status, event_type, event) = if overlap == ScheduleOverlapPolicy::CancelPrevious {
            (
                WorkflowExecutionCloseStatus::Canceled,
                cadence_api::enums::EventType::WorkflowExecutionCanceled,
                HistoryEvent {
                    workflow_execution_canceled_event_attributes: Some(
                        cadence_api::tasks::WorkflowExecutionCanceledEventAttributes {
                            decision_task_completed_event_id: 0,
                            ..Default::default()
                        },
                    ),
                    ..Default::default()
                },
            )
        } else {
            (
                WorkflowExecutionCloseStatus::Terminated,
                cadence_api::enums::EventType::WorkflowExecutionTerminated,
                HistoryEvent {
                    workflow_execution_terminated_event_attributes: Some(
                        cadence_api::service_types::WorkflowExecutionTerminatedEventAttributes {
                            reason: "terminated by schedule overlap policy".to_string(),
                            ..Default::default()
                        },
                    ),
                    ..Default::default()
                },
            )
        };
        Self::close_run_locked(
            &mut inner,
            domain,
            workflow_id,
            run_id,
            status,
            None,
            Vec::new(),
            event,
            event_type,
        );
    }

    /// Pause schedules whose tracked runs just closed as failed when
    /// pause_on_failure is set, mirroring the scheduler worker.
    fn check_schedule_pause_on_failure(&self, domain: &str, schedule_id: &str) {
        let failures: Vec<(String, String)> = {
            let inner = self.inner.read().expect("execution store lock");
            let Some(record) = inner
                .schedules
                .get(&(domain.to_string(), schedule_id.to_string()))
            else {
                return;
            };
            if !record.policies.pause_on_failure {
                return;
            }
            record
                .running
                .iter()
                .filter(|(w, run)| {
                    inner
                        .by_run
                        .get(&(domain.to_string(), w.clone(), run.clone()))
                        .is_some_and(|rec| {
                            rec.close_status == Some(WorkflowExecutionCloseStatus::Failed)
                        })
                })
                .cloned()
                .collect()
        };
        if failures.is_empty() {
            return;
        }
        let mut inner = self.inner.write().expect("execution store lock");
        if let Some(record) = inner
            .schedules
            .get_mut(&(domain.to_string(), schedule_id.to_string()))
        {
            if !record.paused {
                record.paused = true;
                record.pause_reason = "paused on workflow failure".to_string();
                record.paused_by = format!("schedule:{schedule_id}");
                record.paused_at_nanos = Some(now_nanos());
                record.update_nanos = now_nanos();
            }
        }
    }

    /// Drain one buffered fire per BUFFER schedule with nothing running.
    fn drain_schedule_buffers(&self) {
        let ready: Vec<(String, String, i64)> = {
            let inner = self.inner.read().expect("execution store lock");
            inner
                .schedules
                .iter()
                .filter(|(_, r)| {
                    !r.paused
                        && !r.deleted()
                        && r.policies.overlap_policy == Some(ScheduleOverlapPolicy::Buffer)
                        && !r.buffer.is_empty()
                        && r.running.iter().all(|(w, run)| {
                            inner
                                .by_run
                                .get(&(((r.domain).clone()), w.clone(), run.clone()))
                                .is_none_or(|rec| !rec.is_open())
                        })
                })
                .filter_map(|((d, id), r)| r.buffer.front().map(|f| (d.clone(), id.clone(), *f)))
                .collect()
        };
        for (domain, id, fire) in ready {
            {
                let mut inner = self.inner.write().expect("execution store lock");
                if let Some(r) = inner.schedules.get_mut(&(domain.clone(), id.clone())) {
                    r.buffer.pop_front();
                }
            }
            self.fire_schedule_at(&domain, &id, fire, ScheduleOverlapPolicy::Buffer, "buffer");
        }
    }

    fn domain_name_str(&self, namespace: &str) -> String {
        let inner = self.inner.read().expect("execution store lock");
        inner.domain_name(namespace)
    }
}

impl ScheduleRecord {
    fn catch_up_watermark(&self) -> i64 {
        self.create_nanos
            .max(self.last_fire_nanos.unwrap_or(self.create_nanos))
            .max(self.last_processed_nanos.unwrap_or(self.create_nanos))
    }

    fn deleted(&self) -> bool {
        self.deleted_at_nanos.is_some()
    }

    fn refresh_next_run(&mut self, now_nanos: i64) {
        if self.paused || self.deleted() {
            self.next_run_nanos = None;
            return;
        }
        self.next_run_nanos = next_spec_fire_nanos(&self.spec, now_nanos);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executions::InMemoryExecutionStore;
    use cadence_api::types::{TaskList, WorkflowType};

    fn test_store() -> InMemoryExecutionStore {
        let store = InMemoryExecutionStore::new();
        store.register_domain_name("d");
        store
    }

    fn create_req(id: &str, cron: &str) -> cadence_api::schedule::CreateScheduleRequest {
        cadence_api::schedule::CreateScheduleRequest {
            domain: "d".to_string(),
            schedule_id: id.to_string(),
            spec: Some(ScheduleSpec {
                cron_expression: cron.to_string(),
                ..Default::default()
            }),
            action: Some(ScheduleAction {
                start_workflow: Some(cadence_api::schedule::ScheduleStartWorkflowAction {
                    workflow_type: Some(WorkflowType {
                        name: "wt".to_string(),
                    }),
                    task_list: Some(TaskList {
                        name: "tl".to_string(),
                        ..Default::default()
                    }),
                    workflow_id_prefix: "sched".to_string(),
                    execution_start_to_close_timeout_seconds: Some(60),
                    task_start_to_close_timeout_seconds: Some(10),
                    ..Default::default()
                }),
            }),
            ..Default::default()
        }
    }

    fn nanos(value: &str) -> i64 {
        chrono::DateTime::parse_from_rfc3339(value)
            .unwrap()
            .timestamp_nanos_opt()
            .unwrap()
    }

    fn catch_up_store(policy: ScheduleCatchUpPolicy) -> InMemoryExecutionStore {
        let store = test_store();
        let mut req = create_req("catch-up", "* * * * *");
        req.policies = Some(SchedulePolicies {
            overlap_policy: Some(ScheduleOverlapPolicy::Concurrent),
            catch_up_policy: Some(ScheduleCatchUpPolicy::Skip),
            ..Default::default()
        });
        store.create_schedule("d", &req).unwrap();
        {
            let mut inner = store.inner.write().unwrap();
            let key = InMemoryExecutionStore::schedule_key(&inner, "d", "catch-up");
            let r = inner.schedules.get_mut(&key).unwrap();
            r.create_nanos = nanos("2026-01-15T10:00:00Z");
            r.unpause_catchup = Some(policy);
            r.catch_up_pending = true;
        }
        store
    }

    #[test]
    fn scheduler_catch_up_fixtures() {
        #[derive(serde::Deserialize)]
        struct Round {
            attempted: usize,
            skipped: i64,
            watermark: i64,
            pending: bool,
            #[serde(rename = "override")]
            override_policy: i32,
        }
        #[derive(serde::Deserialize)]
        struct Case {
            name: String,
            policy: i32,
            minutes: i64,
            rounds: Vec<Round>,
        }
        let cases: Vec<Case> = serde_json::from_str(include_str!(
            "../../../../tests/fixtures/scheduler-catchup.json"
        ))
        .unwrap();
        let base = nanos("2026-01-15T10:00:00Z");
        for case in cases {
            let store = catch_up_store(ScheduleCatchUpPolicy::from_i32(case.policy).unwrap());
            let now = base + case.minutes * 60_000_000_000;
            for want in case.rounds {
                assert_eq!(
                    store.fire_due_schedules(now),
                    want.attempted,
                    "{} attempted",
                    case.name
                );
                let inner = store.inner.read().unwrap();
                let key = InMemoryExecutionStore::schedule_key(&inner, "d", "catch-up");
                let r = inner.schedules.get(&key).unwrap();
                assert_eq!(r.skipped_runs, want.skipped, "{} skipped", case.name);
                assert_eq!(
                    r.last_processed_nanos,
                    Some(base + want.watermark * 60_000_000_000),
                    "{} watermark",
                    case.name
                );
                assert_eq!(r.catch_up_pending, want.pending, "{} pending", case.name);
                assert_eq!(
                    r.unpause_catchup.unwrap_or(ScheduleCatchUpPolicy::Invalid) as i32,
                    want.override_policy,
                    "{} override",
                    case.name
                );
            }
        }
    }

    #[test]
    fn catch_up_skips_advance_watermark_without_live_replay() {
        for (policy, runs, skips) in [
            (ScheduleCatchUpPolicy::Skip, 0, 5),
            (ScheduleCatchUpPolicy::One, 1, 4),
            (ScheduleCatchUpPolicy::All, 5, 0),
        ] {
            let store = catch_up_store(policy);
            let now = nanos("2026-01-15T10:05:00Z");
            assert_eq!(store.fire_due_schedules(now), runs as usize);
            assert_eq!(store.fire_due_schedules(now), 0);
            let inner = store.inner.read().unwrap();
            let key = InMemoryExecutionStore::schedule_key(&inner, "d", "catch-up");
            let r = inner.schedules.get(&key).unwrap();
            assert_eq!(r.total_runs, runs);
            assert_eq!(r.skipped_runs, skips);
            assert_eq!(r.missed_runs, 0);
            assert_eq!(r.last_processed_nanos, Some(now));
            assert_eq!(r.unpause_catchup, None);
            assert!(!r.catch_up_pending);
        }
    }

    #[test]
    fn restored_schedule_applies_configured_policy_to_downtime() {
        let store = catch_up_store(ScheduleCatchUpPolicy::Skip);
        {
            let mut inner = store.inner.write().unwrap();
            let key = InMemoryExecutionStore::schedule_key(&inner, "d", "catch-up");
            let r = inner.schedules.get_mut(&key).unwrap();
            r.catch_up_pending = false;
            r.unpause_catchup = None;
        }
        let path =
            std::env::temp_dir().join(format!("cadence-recovery-{}.json", uuid::Uuid::new_v4()));
        store.save_to_file(&path).unwrap();
        let restored = InMemoryExecutionStore::load_from_file(&path).unwrap();
        std::fs::remove_file(path).unwrap();
        let now = nanos("2026-01-15T10:05:00Z");
        assert_eq!(restored.fire_due_schedules(now), 0);
        assert_eq!(restored.fire_due_schedules(now), 0);
        let inner = restored.inner.read().unwrap();
        let key = InMemoryExecutionStore::schedule_key(&inner, "d", "catch-up");
        let r = inner.schedules.get(&key).unwrap();
        assert_eq!(r.skipped_runs, 5);
        assert_eq!(r.last_processed_nanos, Some(now));
        assert!(!r.catch_up_pending);
    }

    #[test]
    fn old_snapshot_schedule_fields_default_without_unpausing() {
        let store = catch_up_store(ScheduleCatchUpPolicy::All);
        let inner = store.inner.read().unwrap();
        let key = InMemoryExecutionStore::schedule_key(&inner, "d", "catch-up");
        let mut value = serde_json::to_value(inner.schedules.get(&key).unwrap()).unwrap();
        let object = value.as_object_mut().unwrap();
        object.remove("last_processed_nanos");
        object.remove("catch_up_pending");
        object.insert("paused".to_string(), serde_json::Value::Bool(true));
        let record: ScheduleRecord = serde_json::from_value(value).unwrap();
        assert_eq!(record.last_processed_nanos, None);
        assert!(!record.catch_up_pending);
        assert!(record.paused);
    }

    #[test]
    fn catch_up_override_survives_batches_and_snapshot_restart() {
        let mut store = catch_up_store(ScheduleCatchUpPolicy::All);
        let now = nanos("2026-01-15T14:10:00Z"); // 250 fires
        assert_eq!(store.fire_due_schedules(now), 100);
        let path =
            std::env::temp_dir().join(format!("cadence-catchup-{}.json", uuid::Uuid::new_v4()));
        store.save_to_file(&path).unwrap();
        store = InMemoryExecutionStore::load_from_file(&path).unwrap();
        std::fs::remove_file(path).unwrap();
        {
            let inner = store.inner.read().unwrap();
            let key = InMemoryExecutionStore::schedule_key(&inner, "d", "catch-up");
            let r = inner.schedules.get(&key).unwrap();
            assert!(r.catch_up_pending);
            assert_eq!(r.unpause_catchup, Some(ScheduleCatchUpPolicy::All));
        }
        assert_eq!(store.fire_due_schedules(now), 100);
        assert_eq!(store.fire_due_schedules(now), 50);
        assert_eq!(store.fire_due_schedules(now), 0);
        let inner = store.inner.read().unwrap();
        let key = InMemoryExecutionStore::schedule_key(&inner, "d", "catch-up");
        let r = inner.schedules.get(&key).unwrap();
        assert_eq!(r.total_runs, 250);
        assert_eq!(r.skipped_runs, 0);
        assert_eq!(r.last_processed_nanos, Some(now));
        assert_eq!(r.unpause_catchup, None);
        assert!(!r.catch_up_pending);
    }

    #[test]
    fn scheduler_backfill_fixtures() {
        #[derive(serde::Deserialize)]
        struct Signal {
            id: String,
            start: i64,
            end: i64,
        }
        #[derive(serde::Deserialize)]
        struct Round {
            attempted: usize,
            pending: usize,
            #[serde(default)]
            completed: i32,
            #[serde(default)]
            start: i64,
        }
        #[derive(serde::Deserialize)]
        struct Case {
            name: String,
            signals: Vec<Signal>,
            accepted: Vec<bool>,
            queued: usize,
            total: i32,
            rounds: Vec<Round>,
        }
        let cases: Vec<Case> = serde_json::from_str(include_str!(
            "../../../../tests/fixtures/scheduler-backfill.json"
        ))
        .unwrap();
        let base = nanos("2026-01-15T10:00:00Z");
        for case in cases {
            let mut store = test_store();
            let mut req = create_req("backfill", "* * * * *");
            req.state = Some(ScheduleState {
                paused: true,
                ..Default::default()
            });
            req.policies = Some(SchedulePolicies {
                overlap_policy: Some(ScheduleOverlapPolicy::Concurrent),
                ..Default::default()
            });
            store.create_schedule("d", &req).unwrap();
            for (i, sig) in case.signals.iter().enumerate() {
                let before = store
                    .describe_schedule("d", "backfill")
                    .unwrap()
                    .info
                    .unwrap()
                    .ongoing_backfills
                    .len();
                store
                    .backfill_schedule(
                        "d",
                        "backfill",
                        base + sig.start * 60_000_000_000,
                        base + sig.end * 60_000_000_000,
                        None,
                        &sig.id,
                    )
                    .unwrap();
                let after = store
                    .describe_schedule("d", "backfill")
                    .unwrap()
                    .info
                    .unwrap()
                    .ongoing_backfills
                    .len();
                assert_eq!(
                    after > before,
                    case.accepted[i],
                    "{} signal {}",
                    case.name,
                    i
                );
            }
            let info = store
                .describe_schedule("d", "backfill")
                .unwrap()
                .info
                .unwrap();
            assert_eq!(
                info.ongoing_backfills.len(),
                case.queued,
                "{} queued",
                case.name
            );
            assert_eq!(
                info.ongoing_backfills[0].runs_total, case.total,
                "{} total",
                case.name
            );
            assert_eq!(info.ongoing_backfills[0].runs_completed, 0);
            assert_eq!(info.total_runs, 0);
            // A paused queue must survive the same snapshot recovery as workflows.
            let path = std::env::temp_dir()
                .join(format!("cadence-backfill-{}.json", uuid::Uuid::new_v4()));
            store.save_to_file(&path).unwrap();
            store = InMemoryExecutionStore::load_from_file(&path).unwrap();
            std::fs::remove_file(path).unwrap();
            let namespace = {
                let mut inner = store.inner.write().unwrap();
                let key = InMemoryExecutionStore::schedule_key(&inner, "d", "backfill");
                let record = inner.schedules.get_mut(&key).unwrap();
                assert!(record.paused);
                record.paused = false;
                key.0
            };
            for want in case.rounds {
                assert_eq!(
                    store.process_schedule_backfills(&namespace, "backfill"),
                    want.attempted,
                    "{} attempted",
                    case.name
                );
                let info = store
                    .describe_schedule("d", "backfill")
                    .unwrap()
                    .info
                    .unwrap();
                assert_eq!(
                    info.ongoing_backfills.len(),
                    want.pending,
                    "{} pending",
                    case.name
                );
                if want.pending > 0 {
                    assert_eq!(info.ongoing_backfills[0].runs_completed, want.completed);
                    assert_eq!(
                        info.ongoing_backfills[0].start_time_nano,
                        Some(base + want.start * 60_000_000_000)
                    );
                }
            }
        }
    }

    #[test]
    fn scheduler_boundary_fixtures() {
        #[derive(serde::Deserialize)]
        struct Case {
            name: String,
            cron: String,
            after: String,
            until: String,
            start: Option<String>,
            end: Option<String>,
            next: Option<String>,
            fires: Vec<String>,
        }
        let cases: Vec<Case> = serde_json::from_str(include_str!(
            "../../../../tests/fixtures/scheduler-boundaries.json"
        ))
        .unwrap();
        for case in cases {
            let spec = ScheduleSpec {
                cron_expression: case.cron,
                start_time_nano: case.start.as_deref().map(nanos),
                end_time_nano: case.end.as_deref().map(nanos),
                ..Default::default()
            };
            assert_eq!(
                next_spec_fire_nanos(&spec, nanos(&case.after)),
                case.next.as_deref().map(nanos),
                "{} next",
                case.name
            );
            assert_eq!(
                spec_fires_in_range(&spec, nanos(&case.after), nanos(&case.until), 1000),
                case.fires.iter().map(|v| nanos(v)).collect::<Vec<_>>(),
                "{} fires",
                case.name
            );
        }
    }

    #[test]
    fn scheduler_tick_after_end_preserves_inclusive_fires() {
        let store = test_store();
        let start = nanos("2026-06-01T00:00:00Z");
        let end = nanos("2026-06-01T01:00:00Z");
        let mut req = create_req("bounded", "0 * * * *");
        req.spec.as_mut().unwrap().start_time_nano = Some(start);
        req.spec.as_mut().unwrap().end_time_nano = Some(end);
        req.policies = Some(SchedulePolicies {
            overlap_policy: Some(ScheduleOverlapPolicy::Concurrent),
            ..Default::default()
        });
        store.create_schedule("d", &req).unwrap();
        {
            let mut inner = store.inner.write().unwrap();
            let key = InMemoryExecutionStore::schedule_key(&inner, "d", "bounded");
            let record = inner.schedules.get_mut(&key).unwrap();
            record.create_nanos = start - 60_000_000_000;
            record.refresh_next_run(record.create_nanos);
            assert_eq!(record.next_run_nanos, Some(start));
        }
        assert_eq!(store.fire_due_schedules(end + 60_000_000_000), 2);
        assert_eq!(store.fire_due_schedules(end + 120_000_000_000), 0);
        let inner = store.inner.read().unwrap();
        let key = InMemoryExecutionStore::schedule_key(&inner, "d", "bounded");
        let record = inner.schedules.get(&key).unwrap();
        assert_eq!(record.total_runs, 2);
        assert_eq!(record.next_run_nanos, None);
    }

    #[test]
    fn backfill_includes_endpoints_and_respects_spec() {
        let store = test_store();
        let start = nanos("2026-06-01T00:00:00Z");
        let end = nanos("2026-06-01T01:00:00Z");
        let mut req = create_req("bounded-backfill", "0 * * * *");
        req.spec.as_mut().unwrap().start_time_nano = Some(start);
        req.spec.as_mut().unwrap().end_time_nano = Some(end);
        req.policies = Some(SchedulePolicies {
            overlap_policy: Some(ScheduleOverlapPolicy::Concurrent),
            ..Default::default()
        });
        store.create_schedule("d", &req).unwrap();
        store
            .backfill_schedule(
                "d",
                "bounded-backfill",
                start - 3_600_000_000_000,
                end + 3_600_000_000_000,
                None,
                "bounds",
            )
            .unwrap();
        let inner = store.inner.read().unwrap();
        let key = InMemoryExecutionStore::schedule_key(&inner, "d", "bounded-backfill");
        let record = inner.schedules.get(&key).unwrap();
        assert_eq!(record.backfills[0].runs_total, 2);
        assert_eq!(record.total_runs, 2);
    }

    #[test]
    fn fire_ids_and_request_ids_match_go() {
        assert_eq!(
            fire_workflow_id("pre", "sched", 1_700_000_000_000_000_000),
            "pre-2023-11-14T22:13:20Z"
        );
        assert_eq!(
            fire_workflow_id("", "sched", 1_700_000_000_000_000_000),
            "sched-2023-11-14T22:13:20Z"
        );
        // Deterministic request IDs deduplicate redelivered fires.
        assert_eq!(
            fire_request_id("s", 7, "schedule"),
            fire_request_id("s", 7, "schedule")
        );
        assert_ne!(
            fire_request_id("s", 7, "schedule"),
            fire_request_id("s", 7, "backfill:x")
        );
    }

    #[test]
    fn catch_up_policy_matches_go() {
        let missed = vec![100, 200, 300];
        let (fire, skipped) = apply_catch_up_policy(ScheduleCatchUpPolicy::Skip, 0, &missed, 1000);
        assert!(fire.is_empty() && skipped == 3);
        let (fire, skipped) = apply_catch_up_policy(ScheduleCatchUpPolicy::One, 0, &missed, 1000);
        assert_eq!(fire, vec![300]);
        assert_eq!(skipped, 2);
        let (fire, skipped) = apply_catch_up_policy(ScheduleCatchUpPolicy::All, 0, &missed, 1000);
        assert_eq!(fire, missed);
        assert_eq!(skipped, 0);
        // Out-of-window fires are skipped even under ALL.
        let (fire, skipped) =
            apply_catch_up_policy(ScheduleCatchUpPolicy::All, 150, &[100, 900], 1000);
        assert_eq!(fire, vec![900]);
        assert_eq!(skipped, 1);
        // Invalid defaults to skip like Go.
        let (fire, skipped) =
            apply_catch_up_policy(ScheduleCatchUpPolicy::Invalid, 0, &missed, 1000);
        assert!(fire.is_empty() && skipped == 3);
    }

    #[test]
    fn crud_pause_unpause_delete_flow() {
        let store = test_store();
        let id = store
            .create_schedule("d", &create_req("s1", "* * * * *"))
            .unwrap();
        assert_eq!(id, "s1");
        // Duplicate creation is an exists error.
        assert!(matches!(
            store.create_schedule("d", &create_req("s1", "* * * * *")),
            Err(ScheduleError::Exists(_))
        ));
        let desc = store.describe_schedule("d", "s1").unwrap();
        assert_eq!(desc.spec.unwrap().cron_expression, "* * * * *");
        assert!(!desc.state.unwrap().paused);
        store.pause_schedule("d", "s1", "hold", "tester").unwrap();
        let desc = store.describe_schedule("d", "s1").unwrap();
        let state = desc.state.unwrap();
        assert!(state.paused);
        assert_eq!(state.pause_info.unwrap().reason, "hold");
        // Paused schedules do not fire.
        assert_eq!(store.fire_due_schedules(i64::MAX), 0);
        store
            .unpause_schedule("d", "s1", Some(ScheduleCatchUpPolicy::Skip))
            .unwrap();
        let desc = store.describe_schedule("d", "s1").unwrap();
        assert!(!desc.state.unwrap().paused);
        // Unknown schedules are not found.
        assert!(matches!(
            store.describe_schedule("d", "nope"),
            Err(ScheduleError::NotFound(_))
        ));
        // Delete is idempotent; the schedule disappears from list/describe.
        store.delete_schedule("d", "s1").unwrap();
        store.delete_schedule("d", "s1").unwrap();
        assert!(store.list_schedules("d", 10, &[]).0.is_empty());
        assert!(matches!(
            store.describe_schedule("d", "s1"),
            Err(ScheduleError::NotFound(_))
        ));
    }

    #[test]
    fn firing_starts_target_workflows_with_skip_overlap() {
        let store = test_store();
        store
            .create_schedule("d", &create_req("s1", "* * * * *"))
            .unwrap();
        // Fire after creation: the first due fire starts, the rest skip
        // while it runs (SKIP_NEW overlap).
        let started = store.fire_due_schedules(2_000_000_000_000_000_000);
        assert!(started >= 1);
        let desc = store.describe_schedule("d", "s1").unwrap();
        let info = desc.info.unwrap();
        assert!(info.total_runs >= 1);
        assert!(info.skipped_runs >= 1);
        // The target workflow exists with the Go-style fire ID.
        let wf_id = fire_workflow_id("sched", "s1", info.last_run_time_nano.unwrap());
        let inner = store.inner.read().expect("execution store lock");
        let domain = inner.domain_id("d");
        assert!(inner.by_workflow.contains_key(&(domain, wf_id)));
    }

    #[test]
    fn backfill_executes_range_and_tracks_progress() {
        let store = test_store();
        store
            .create_schedule("d", &create_req("s1", "0 * * * *"))
            .unwrap();
        // One hour range on an hourly cron: exactly one fire.
        let start = 1_700_000_100_000_000_000;
        store
            .backfill_schedule("d", "s1", start, start + 3_600_000_000_000, None, "bf-1")
            .unwrap();
        // A retry after draining is accepted; SKIP_NEW avoids an overlapping run.
        store
            .backfill_schedule("d", "s1", start, start + 3_600_000_000_000, None, "bf-1")
            .unwrap();
        let desc = store.describe_schedule("d", "s1").unwrap();
        let info = desc.info.unwrap();
        assert!(info.ongoing_backfills.is_empty());
        let inner = store.inner.read().unwrap();
        let key = InMemoryExecutionStore::schedule_key(&inner, "d", "s1");
        let record = inner.schedules.get(&key).unwrap();
        assert_eq!(
            record.backfills[0].runs_completed,
            record.backfills[0].runs_total
        );
        assert_eq!(info.total_runs, record.backfills[0].runs_total as i64);
    }
}
