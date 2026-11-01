//! Go-compatible JSON wire format for the scheduler workflow that Frontend
//! starts, signals and queries (`go/service/worker/scheduler/types.go` and
//! `go/service/frontend/api/handler_schedule.go`). The Go Worker runs the
//! scheduler workflow, so payloads must be byte-compatible with Go's
//! `encoding/json`: `time.Time` is RFC3339, `time.Duration` is nanoseconds and
//! schedule enums are their string names.

#![allow(clippy::result_large_err)]

use cadence_api::{
    enums::{ScheduleCatchUpPolicy, ScheduleOverlapPolicy},
    errors::{CadenceError, EntityNotExistsError, InternalServiceError},
    schedule::{
        BackfillInfo, BackfillScheduleRequest, CreateScheduleRequest, DescribeScheduleResponse,
        PauseScheduleRequest, SchedulePauseInfo, ScheduleInfo, ScheduleSpec, ScheduleState,
        SchedulePolicies, UnpauseScheduleRequest, UpdateScheduleRequest,
    },
};
use chrono::{DateTime, SecondsFormat};
use serde_json::{json, Map, Value};

pub const WORKFLOW_TYPE_NAME: &str = "cadence-scheduler";
pub const TASK_LIST_NAME: &str = "cadence-scheduler";
pub const SIGNAL_PAUSE: &str = "scheduler-pause";
pub const SIGNAL_UNPAUSE: &str = "scheduler-unpause";
pub const SIGNAL_UPDATE: &str = "scheduler-update";
pub const SIGNAL_BACKFILL: &str = "scheduler-backfill";
pub const SIGNAL_DELETE: &str = "scheduler-delete";
pub const QUERY_DESCRIBE: &str = "scheduler-describe";

/// ~10 years, as Go's `schedulerWorkflowExecutionTimeout`.
pub const EXECUTION_TIMEOUT_SECONDS: i32 = (10u64 * 365 * 24 * 3600) as i32;
pub const DECISION_TIMEOUT_SECONDS: i32 = 10;
pub const DESCRIBE_RETRY_ATTEMPTS: u32 = 5;
pub const DESCRIBE_RETRY_INTERVAL_SECS: u64 = 1;

const ZERO_TIME: &str = "0001-01-01T00:00:00Z";
const NANOS_PER_SECOND: i64 = 1_000_000_000;

/// Go `time.Time` JSON; absent times are the zero time (Go's `omitempty` does
/// not omit struct values).
#[must_use]
pub fn go_time(nanos: Option<i64>) -> Value {
    match nanos.filter(|n| *n != 0) {
        Some(n) => {
            let secs = n.div_euclid(NANOS_PER_SECOND);
            let sub = n.rem_euclid(NANOS_PER_SECOND) as u32;
            match DateTime::from_timestamp(secs, sub) {
                Some(t) => Value::String(t.to_rfc3339_opts(SecondsFormat::AutoSi, true)),
                None => Value::String(ZERO_TIME.to_string()),
            }
        }
        None => Value::String(ZERO_TIME.to_string()),
    }
}

/// Inverse of [`go_time`]; the zero time and anything unparsable map to None.
#[must_use]
pub fn parse_go_time(value: Option<&Value>) -> Option<i64> {
    let text = value?.as_str()?;
    if text == ZERO_TIME {
        return None;
    }
    let t = DateTime::parse_from_rfc3339(text).ok()?;
    t.timestamp()
        .checked_mul(NANOS_PER_SECOND)?
        .checked_add(i64::from(t.timestamp_subsec_nanos()))
}

fn insert_if(map: &mut Map<String, Value>, key: &str, value: Option<Value>) {
    if let Some(v) = value {
        map.insert(key.to_string(), v);
    }
}

#[must_use]
pub fn spec_to_go(spec: &ScheduleSpec) -> Value {
    let mut m = Map::new();
    if !spec.cron_expression.is_empty() {
        m.insert("cronExpression".into(), json!(spec.cron_expression));
    }
    m.insert("startTime".into(), go_time(spec.start_time_nano));
    m.insert("endTime".into(), go_time(spec.end_time_nano));
    if let Some(j) = spec.jitter_in_seconds.filter(|j| *j != 0) {
        m.insert("jitter".into(), json!(i64::from(j) * NANOS_PER_SECOND));
    }
    Value::Object(m)
}

#[must_use]
pub fn spec_from_go(value: &Value) -> ScheduleSpec {
    ScheduleSpec {
        cron_expression: value
            .get("cronExpression")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        start_time_nano: parse_go_time(value.get("startTime")),
        end_time_nano: parse_go_time(value.get("endTime")),
        jitter_in_seconds: value
            .get("jitter")
            .and_then(Value::as_i64)
            .map(|ns| (ns / NANOS_PER_SECOND) as i32)
            .filter(|s| *s != 0),
    }
}

#[must_use]
pub fn policies_to_go(policies: &SchedulePolicies) -> Value {
    let mut m = Map::new();
    if let Some(p) = policies
        .overlap_policy
        .filter(|p| *p != ScheduleOverlapPolicy::Invalid)
    {
        m.insert("overlapPolicy".into(), json!(p.as_str()));
    }
    if let Some(p) = policies
        .catch_up_policy
        .filter(|p| *p != ScheduleCatchUpPolicy::Invalid)
    {
        m.insert("catchUpPolicy".into(), json!(p.as_str()));
    }
    if let Some(w) = policies.catch_up_window_in_seconds.filter(|w| *w != 0) {
        m.insert("catchUpWindow".into(), json!(i64::from(w) * NANOS_PER_SECOND));
    }
    if policies.pause_on_failure {
        m.insert("pauseOnFailure".into(), json!(true));
    }
    if policies.buffer_limit != 0 {
        m.insert("bufferLimit".into(), json!(policies.buffer_limit));
    }
    if policies.concurrency_limit != 0 {
        m.insert("concurrencyLimit".into(), json!(policies.concurrency_limit));
    }
    Value::Object(m)
}

#[must_use]
pub fn policies_from_go(value: &Value) -> SchedulePolicies {
    SchedulePolicies {
        overlap_policy: value
            .get("overlapPolicy")
            .and_then(Value::as_str)
            .and_then(ScheduleOverlapPolicy::parse),
        catch_up_policy: value
            .get("catchUpPolicy")
            .and_then(Value::as_str)
            .and_then(ScheduleCatchUpPolicy::parse),
        catch_up_window_in_seconds: value
            .get("catchUpWindow")
            .and_then(Value::as_i64)
            .map(|ns| (ns / NANOS_PER_SECOND) as i32)
            .filter(|s| *s != 0),
        pause_on_failure: value
            .get("pauseOnFailure")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        buffer_limit: value
            .get("bufferLimit")
            .and_then(Value::as_i64)
            .unwrap_or(0) as i32,
        concurrency_limit: value
            .get("concurrencyLimit")
            .and_then(Value::as_i64)
            .unwrap_or(0) as i32,
    }
}

fn to_value<T: serde::Serialize>(value: &T) -> Value {
    serde_json::to_value(value).unwrap_or(Value::Null)
}

fn from_value<T: serde::de::DeserializeOwned>(value: Option<&Value>) -> Option<T> {
    value
        .filter(|v| !v.is_null())
        .and_then(|v| serde_json::from_value(v.clone()).ok())
}

/// `SchedulerWorkflowInput` JSON for `CreateSchedule`.
#[must_use]
pub fn scheduler_input(request: &CreateScheduleRequest) -> Vec<u8> {
    let mut m = Map::new();
    m.insert("domain".into(), json!(request.domain));
    m.insert("scheduleId".into(), json!(request.schedule_id));
    m.insert(
        "spec".into(),
        request
            .spec
            .as_ref()
            .map(spec_to_go)
            .unwrap_or_else(|| spec_to_go(&ScheduleSpec::default())),
    );
    m.insert(
        "action".into(),
        request
            .action
            .as_ref()
            .map(to_value)
            .unwrap_or_else(|| json!({})),
    );
    m.insert(
        "policies".into(),
        request
            .policies
            .as_ref()
            .map(policies_to_go)
            .unwrap_or_else(|| json!({})),
    );
    insert_if(
        &mut m,
        "searchAttributes",
        request.search_attributes.as_ref().map(to_value),
    );
    insert_if(&mut m, "memo", request.memo.as_ref().map(to_value));
    let mut state = Map::new();
    let paused = request.state.as_ref().is_some_and(|s| s.paused);
    state.insert("paused".into(), json!(paused));
    if paused {
        if let Some(info) = request.state.as_ref().and_then(|s| s.pause_info.as_ref()) {
            if !info.reason.is_empty() {
                state.insert("pauseReason".into(), json!(info.reason));
            }
            if !info.paused_by.is_empty() {
                state.insert("pausedBy".into(), json!(info.paused_by));
            }
        }
    }
    for key in ["totalRuns", "missedRuns", "skippedRuns", "iterations"] {
        state.insert(key.into(), json!(0));
    }
    m.insert("state".into(), Value::Object(state));
    serde_json::to_vec(&Value::Object(m)).unwrap_or_default()
}

#[must_use]
pub fn update_signal(request: &UpdateScheduleRequest) -> Vec<u8> {
    let mut m = Map::new();
    insert_if(&mut m, "spec", request.spec.as_ref().map(spec_to_go));
    insert_if(&mut m, "action", request.action.as_ref().map(to_value));
    insert_if(&mut m, "policies", request.policies.as_ref().map(policies_to_go));
    insert_if(
        &mut m,
        "searchAttributes",
        request.search_attributes.as_ref().map(to_value),
    );
    serde_json::to_vec(&Value::Object(m)).unwrap_or_default()
}

#[must_use]
pub fn pause_signal(request: &PauseScheduleRequest) -> Vec<u8> {
    let mut m = Map::new();
    if !request.reason.is_empty() {
        m.insert("reason".into(), json!(request.reason));
    }
    if !request.identity.is_empty() {
        m.insert("pausedBy".into(), json!(request.identity));
    }
    serde_json::to_vec(&Value::Object(m)).unwrap_or_default()
}

#[must_use]
pub fn unpause_signal(request: &UnpauseScheduleRequest) -> Vec<u8> {
    let mut m = Map::new();
    if !request.reason.is_empty() {
        m.insert("reason".into(), json!(request.reason));
    }
    if let Some(p) = request
        .catch_up_policy
        .filter(|p| *p != ScheduleCatchUpPolicy::Invalid)
    {
        m.insert("catchUpPolicy".into(), json!(p.as_str()));
    }
    serde_json::to_vec(&Value::Object(m)).unwrap_or_default()
}

/// `BackfillID` must already be resolved (generated when empty).
#[must_use]
pub fn backfill_signal(request: &BackfillScheduleRequest, backfill_id: &str) -> Vec<u8> {
    let mut m = Map::new();
    m.insert("startTime".into(), go_time(request.start_time_nano));
    m.insert("endTime".into(), go_time(request.end_time_nano));
    m.insert(
        "overlapPolicy".into(),
        json!(request
            .overlap_policy
            .unwrap_or(ScheduleOverlapPolicy::Invalid)
            .as_str()),
    );
    if !backfill_id.is_empty() {
        m.insert("backfillId".into(), json!(backfill_id));
    }
    serde_json::to_vec(&Value::Object(m)).unwrap_or_default()
}

/// Decode the `scheduler-describe` query result into the public response,
/// mirroring `DescribeSchedule`'s field mapping.
pub fn describe_response(bytes: &[u8]) -> Result<DescribeScheduleResponse, CadenceError> {
    let v: Value = serde_json::from_slice(bytes).map_err(|e| {
        CadenceError::Internal(InternalServiceError::new(format!(
            "failed to deserialize scheduler describe response: {e}"
        )))
    })?;
    let paused = v.get("paused").and_then(Value::as_bool).unwrap_or(false);
    let text = |key: &str| {
        v.get(key)
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string()
    };
    let int = |key: &str| v.get(key).and_then(Value::as_i64).unwrap_or(0);
    let backfills: Vec<BackfillInfo> = v
        .get("ongoingBackfills")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .map(|b| BackfillInfo {
                    backfill_id: b
                        .get("backfillId")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string(),
                    start_time_nano: parse_go_time(b.get("startTime")),
                    end_time_nano: parse_go_time(b.get("endTime")),
                    runs_completed: b.get("runsCompleted").and_then(Value::as_i64).unwrap_or(0)
                        as i32,
                    runs_total: b.get("runsTotal").and_then(Value::as_i64).unwrap_or(0) as i32,
                })
                .collect()
        })
        .unwrap_or_default();
    Ok(DescribeScheduleResponse {
        spec: Some(spec_from_go(v.get("spec").unwrap_or(&Value::Null))),
        action: from_value(v.get("action")).or_else(|| Some(Default::default())),
        policies: Some(policies_from_go(v.get("policies").unwrap_or(&Value::Null))),
        state: Some(ScheduleState {
            paused,
            pause_info: paused.then(|| SchedulePauseInfo {
                reason: text("pauseReason"),
                paused_time_nano: parse_go_time(v.get("pausedAt")),
                paused_by: text("pausedBy"),
            }),
        }),
        info: Some(ScheduleInfo {
            last_run_time_nano: parse_go_time(v.get("lastRunTime")),
            next_run_time_nano: parse_go_time(v.get("nextRunTime")),
            total_runs: int("totalRuns"),
            missed_runs: int("missedRuns"),
            skipped_runs: int("skippedRuns"),
            buffered_fire_count: int("bufferedFireCount"),
            running_workflow_count: int("runningWorkflowCount"),
            create_time_nano: parse_go_time(v.get("createTime")),
            last_update_time_nano: parse_go_time(v.get("lastUpdateTime")),
            ongoing_backfills: backfills,
        }),
        memo: from_value(v.get("memo")),
        search_attributes: from_value(v.get("searchAttributes")),
    })
}

/// Go `normalizeScheduleError` for entity-not-exists; other errors pass through.
#[must_use]
pub fn normalize_schedule_error(
    error: CadenceError,
    schedule_id: &str,
    domain: &str,
) -> CadenceError {
    match error {
        CadenceError::EntityNotExists(_) => CadenceError::EntityNotExists(
            EntityNotExistsError::new(format!(
                "schedule {schedule_id:?} not found in domain {domain:?}"
            )),
        ),
        other => other,
    }
}

/// Go `scheduleNotOperationalError`.
#[must_use]
pub fn not_operational_error(schedule_id: &str, domain: &str, close_status: &str) -> CadenceError {
    CadenceError::Internal(InternalServiceError::new(format!(
        "schedule {schedule_id:?} in domain {domain:?} is not operational: scheduler workflow ended with status {close_status}"
    )))
}

/// Go `isScheduleAlreadyGone`, used by idempotent DeleteSchedule.
#[must_use]
pub fn is_schedule_already_gone(error: &CadenceError) -> bool {
    matches!(
        error,
        CadenceError::EntityNotExists(_) | CadenceError::WorkflowAlreadyCompleted(_)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn go_time_round_trips_and_zero_is_absent() {
        assert_eq!(go_time(None), json!(ZERO_TIME));
        assert_eq!(parse_go_time(Some(&json!(ZERO_TIME))), None);
        let nanos = 1_700_000_000_123_456_789;
        let text = go_time(Some(nanos));
        assert_eq!(text, json!("2023-11-14T22:13:20.123456789Z"));
        assert_eq!(parse_go_time(Some(&text)), Some(nanos));
    }

    #[test]
    fn durations_and_enums_use_go_encodings() {
        let policies = SchedulePolicies {
            overlap_policy: Some(ScheduleOverlapPolicy::Buffer),
            catch_up_policy: Some(ScheduleCatchUpPolicy::One),
            catch_up_window_in_seconds: Some(60),
            buffer_limit: 3,
            ..Default::default()
        };
        let go = policies_to_go(&policies);
        assert_eq!(go["overlapPolicy"], "BUFFER");
        assert_eq!(go["catchUpPolicy"], "ONE");
        assert_eq!(go["catchUpWindow"], 60_000_000_000i64);
        assert_eq!(policies_from_go(&go), policies);
        let spec = ScheduleSpec {
            cron_expression: "* * * * *".into(),
            jitter_in_seconds: Some(5),
            ..Default::default()
        };
        assert_eq!(spec_to_go(&spec)["jitter"], 5_000_000_000i64);
        assert_eq!(spec_from_go(&spec_to_go(&spec)), spec);
    }

    #[test]
    fn scheduler_input_carries_state_and_pause_info() {
        let request: CreateScheduleRequest = serde_json::from_str(
            r#"{"domain":"d","scheduleId":"s","spec":{"cronExpression":"0 0 * * *"},"state":{"paused":true,"pauseInfo":{"reason":"r","pausedBy":"me"}}}"#,
        )
        .unwrap();
        let v: Value = serde_json::from_slice(&scheduler_input(&request)).unwrap();
        assert_eq!(v["scheduleId"], "s");
        assert_eq!(v["state"]["paused"], true);
        assert_eq!(v["state"]["pauseReason"], "r");
        assert_eq!(v["state"]["pausedBy"], "me");
        assert_eq!(v["spec"]["cronExpression"], "0 0 * * *");
    }

    #[test]
    fn describe_decodes_go_query_result() {
        let body = br#"{"scheduleId":"s","domain":"d","spec":{"cronExpression":"* * * * *","startTime":"0001-01-01T00:00:00Z"},"action":{},"policies":{"overlapPolicy":"SKIP_NEW"},"paused":true,"pauseReason":"x","pausedBy":"y","pausedAt":"2023-11-14T22:13:20Z","totalRuns":4,"nextRunTime":"2023-11-14T22:14:00Z"}"#;
        let r = describe_response(body).unwrap();
        let state = r.state.unwrap();
        assert!(state.paused);
        assert_eq!(state.pause_info.unwrap().paused_by, "y");
        let info = r.info.unwrap();
        assert_eq!(info.total_runs, 4);
        assert_eq!(info.next_run_time_nano, Some(1_700_000_040_000_000_000));
        assert_eq!(
            r.policies.unwrap().overlap_policy,
            Some(ScheduleOverlapPolicy::SkipNew)
        );
        assert!(describe_response(b"nope").is_err());
    }

    #[test]
    fn error_normalisation_matches_go() {
        let e = normalize_schedule_error(
            CadenceError::EntityNotExists(EntityNotExistsError::new("x")),
            "s",
            "d",
        );
        assert_eq!(e.to_string(), "schedule \"s\" not found in domain \"d\"");
        assert!(is_schedule_already_gone(&e));
    }
}
