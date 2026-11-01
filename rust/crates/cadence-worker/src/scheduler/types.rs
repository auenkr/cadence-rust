//! Go-compatible JSON types of the scheduler (`scheduler/types.go` and
//! `common/types/schedule.go`). Field names, `omitempty` behaviour, string
//! enums, RFC 3339 times (Go `time.Time`, including the zero time) and
//! nanosecond durations match what Go's `encoding/json` produces so that
//! workflow inputs, signals and query results interoperate with the Go server
//! and clients.

use std::collections::BTreeMap;

use base64::Engine;
use chrono::{DateTime, FixedOffset, TimeZone};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

// ---------------------------------------------------------------- time

/// Unix seconds of Go's zero `time.Time` (0001-01-01T00:00:00Z).
pub const ZERO_UNIX_SECS: i64 = -62_135_596_800;

/// Go `time.Time`: an instant plus the UTC offset of its location (used by the
/// JSON encoding and by cron evaluation of schedules without a `TZ=`).
#[derive(Clone, Copy, Debug)]
pub struct GoTime {
    pub secs: i64,
    pub nanos: u32,
    pub offset: i32,
}

impl PartialEq for GoTime {
    fn eq(&self, other: &Self) -> bool {
        self.secs == other.secs && self.nanos == other.nanos
    }
}
impl Eq for GoTime {}

impl PartialOrd for GoTime {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for GoTime {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (self.secs, self.nanos).cmp(&(other.secs, other.nanos))
    }
}

impl Default for GoTime {
    fn default() -> Self {
        Self::ZERO
    }
}

impl GoTime {
    pub const ZERO: GoTime = GoTime {
        secs: ZERO_UNIX_SECS,
        nanos: 0,
        offset: 0,
    };

    pub fn is_zero(&self) -> bool {
        self.secs == ZERO_UNIX_SECS && self.nanos == 0
    }

    pub fn after(&self, other: &GoTime) -> bool {
        self > other
    }

    pub fn before(&self, other: &GoTime) -> bool {
        self < other
    }

    /// Go `time.Unix(0, n)`: the process-local location of that instant.
    pub fn from_unix_nanos(n: i64) -> Self {
        let secs = n.div_euclid(1_000_000_000);
        let nanos = n.rem_euclid(1_000_000_000) as u32;
        let offset = crate::scheduler::cron::Zone::Local.offset_at(secs);
        GoTime {
            secs,
            nanos,
            offset: if offset == 0 { 0 } else { offset },
        }
    }

    /// Go `UnixNano` (wraps like Go outside the int64 range).
    pub fn unix_nano(&self) -> i64 {
        self.secs
            .wrapping_mul(1_000_000_000)
            .wrapping_add(i64::from(self.nanos))
    }

    /// Go `t.Add(d)` for a duration in nanoseconds, keeping the offset.
    pub fn add_nanos(&self, d: i64) -> GoTime {
        let total = i128::from(self.secs) * 1_000_000_000 + i128::from(self.nanos) + i128::from(d);
        GoTime {
            secs: total.div_euclid(1_000_000_000) as i64,
            nanos: total.rem_euclid(1_000_000_000) as u32,
            offset: self.offset,
        }
    }

    /// Go `t.Add(time.Second)` style helper.
    pub fn add_secs(&self, s: i64) -> GoTime {
        self.add_nanos(s.saturating_mul(1_000_000_000))
    }

    /// Go `t.Sub(u)` saturating to the int64 nanosecond range.
    pub fn sub(&self, other: &GoTime) -> i64 {
        let d = (i128::from(self.secs) - i128::from(other.secs)) * 1_000_000_000
            + i128::from(self.nanos)
            - i128::from(other.nanos);
        d.clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64
    }

    fn datetime(&self) -> Option<DateTime<FixedOffset>> {
        let off = FixedOffset::east_opt(self.offset)?;
        Some(off.from_utc_datetime(&DateTime::from_timestamp(self.secs, self.nanos)?.naive_utc()))
    }

    /// Go `time.RFC3339Nano` (trailing zeros of the fraction trimmed).
    pub fn format_rfc3339_nano(&self) -> String {
        let Some(dt) = self.datetime() else {
            return "0001-01-01T00:00:00Z".to_owned();
        };
        let mut s = dt.format("%Y-%m-%dT%H:%M:%S").to_string();
        if self.nanos != 0 {
            let f = format!("{:09}", self.nanos);
            s.push('.');
            s.push_str(f.trim_end_matches('0'));
        }
        if self.offset == 0 {
            s.push('Z');
        } else {
            let sign = if self.offset < 0 { '-' } else { '+' };
            let a = self.offset.abs();
            s.push_str(&format!("{sign}{:02}:{:02}", a / 3600, a % 3600 / 60));
        }
        s
    }

    /// Go `time.RFC3339` (whole seconds) in UTC, like
    /// `t.UTC().Format(time.RFC3339)`.
    pub fn format_rfc3339_utc(&self) -> String {
        match DateTime::from_timestamp(self.secs, 0) {
            Some(dt) => dt.format("%Y-%m-%dT%H:%M:%SZ").to_string(),
            None => "0001-01-01T00:00:00Z".to_owned(),
        }
    }

    pub fn parse_rfc3339(s: &str) -> Result<GoTime, String> {
        let dt = DateTime::parse_from_rfc3339(s)
            .map_err(|e| format!("parsing time {s:?} as RFC 3339: {e}"))?;
        let utc = dt.to_utc();
        Ok(GoTime {
            secs: utc.timestamp(),
            nanos: utc.timestamp_subsec_nanos(),
            offset: dt.offset().local_minus_utc(),
        })
    }
}

impl Serialize for GoTime {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&self.format_rfc3339_nano())
    }
}

impl<'de> Deserialize<'de> for GoTime {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        // Go: JSON null leaves the zero time.
        match Option::<String>::deserialize(d)? {
            None => Ok(GoTime::ZERO),
            Some(s) => GoTime::parse_rfc3339(&s).map_err(serde::de::Error::custom),
        }
    }
}

// ------------------------------------------------------- bytes helpers

pub(crate) mod b64 {
    use super::*;

    pub fn serialize<S: Serializer>(v: &[u8], s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&base64::engine::general_purpose::STANDARD.encode(v))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
        match Option::<String>::deserialize(d)? {
            None => Ok(Vec::new()),
            Some(s) => base64::engine::general_purpose::STANDARD
                .decode(s)
                .map_err(serde::de::Error::custom),
        }
    }
}

pub(crate) mod b64_map {
    use super::*;

    pub fn serialize<S: Serializer>(
        v: &BTreeMap<String, Vec<u8>>,
        s: S,
    ) -> Result<S::Ok, S::Error> {
        s.collect_map(v.iter().map(|(k, v)| {
            (
                k,
                base64::engine::general_purpose::STANDARD.encode(v),
            )
        }))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        d: D,
    ) -> Result<BTreeMap<String, Vec<u8>>, D::Error> {
        let raw = Option::<BTreeMap<String, Option<String>>>::deserialize(d)?.unwrap_or_default();
        raw.into_iter()
            .map(|(k, v)| {
                let bytes = match v {
                    None => Vec::new(),
                    Some(s) => base64::engine::general_purpose::STANDARD
                        .decode(s)
                        .map_err(serde::de::Error::custom)?,
                };
                Ok((k, bytes))
            })
            .collect()
    }
}

fn is_zero_i32(v: &i32) -> bool {
    *v == 0
}
fn is_zero_i64(v: &i64) -> bool {
    *v == 0
}
fn is_zero_f64(v: &f64) -> bool {
    *v == 0.0
}
fn is_false(v: &bool) -> bool {
    !*v
}

// --------------------------------------------------------------- enums

macro_rules! go_enum {
    ($name:ident, $label:literal, [$($c:ident = $v:literal : $s:literal),* $(,)?]) => {
        /// Go enum with `MarshalText`/`UnmarshalText`: JSON string form.
        #[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
        pub struct $name(pub i32);

        #[allow(non_upper_case_globals, dead_code)]
        impl $name {
            $(pub const $c: $name = $name($v);)*

            pub fn as_go_string(&self) -> String {
                match self.0 {
                    $($v => $s.to_owned(),)*
                    n => format!(concat!($label, "({})"), n),
                }
            }
        }

        impl Serialize for $name {
            fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                s.serialize_str(&self.as_go_string())
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
                // Go: a JSON null is a no-op, leaving the zero value.
                let Some(raw) = Option::<String>::deserialize(d)? else {
                    return Ok($name(0));
                };
                let up = raw.to_uppercase();
                match up.as_str() {
                    $($s => Ok($name($v)),)*
                    other => other.parse::<i32>().map($name).map_err(|e| {
                        serde::de::Error::custom(format!(
                            "unknown enum value {:?} for {:?}: {}",
                            other, $label, e
                        ))
                    }),
                }
            }
        }
    };
}

go_enum!(
    OverlapPolicy,
    "ScheduleOverlapPolicy",
    [
        INVALID = 0: "INVALID",
        SKIP_NEW = 1: "SKIP_NEW",
        BUFFER = 2: "BUFFER",
        CONCURRENT = 3: "CONCURRENT",
        CANCEL_PREVIOUS = 4: "CANCEL_PREVIOUS",
        TERMINATE_PREVIOUS = 5: "TERMINATE_PREVIOUS",
    ]
);

go_enum!(
    CatchUpPolicy,
    "ScheduleCatchUpPolicy",
    [INVALID = 0: "INVALID", SKIP = 1: "SKIP", ONE = 2: "ONE", ALL = 3: "ALL"]
);

go_enum!(
    TaskListKind,
    "TaskListKind",
    [NORMAL = 0: "NORMAL", STICKY = 1: "STICKY", EPHEMERAL = 2: "EPHEMERAL"]
);

impl OverlapPolicy {
    pub fn is_invalid(&self) -> bool {
        self.0 == 0
    }
}
impl CatchUpPolicy {
    pub fn is_invalid(&self) -> bool {
        self.0 == 0
    }
}

fn zero_overlap(v: &OverlapPolicy) -> bool {
    v.0 == 0
}
fn zero_catch_up(v: &CatchUpPolicy) -> bool {
    v.0 == 0
}

// ------------------------------------------------------ shared types

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct WorkflowType {
    #[serde(skip_serializing_if = "String::is_empty")]
    pub name: String,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct TaskList {
    #[serde(skip_serializing_if = "String::is_empty")]
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<TaskListKind>,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub base_name: String,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct RetryPolicy {
    #[serde(skip_serializing_if = "is_zero_i32")]
    pub initial_interval_in_seconds: i32,
    #[serde(skip_serializing_if = "is_zero_f64")]
    pub backoff_coefficient: f64,
    #[serde(skip_serializing_if = "is_zero_i32")]
    pub maximum_interval_in_seconds: i32,
    #[serde(skip_serializing_if = "is_zero_i32")]
    pub maximum_attempts: i32,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub non_retriable_error_reasons: Vec<String>,
    #[serde(skip_serializing_if = "is_zero_i32")]
    pub expiration_interval_in_seconds: i32,
    #[serde(skip_serializing_if = "is_zero_f64")]
    pub jitter_coefficient: f64,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Memo {
    #[serde(with = "b64_map", skip_serializing_if = "BTreeMap::is_empty")]
    pub fields: BTreeMap<String, Vec<u8>>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct SearchAttributes {
    #[serde(with = "b64_map", skip_serializing_if = "BTreeMap::is_empty")]
    pub indexed_fields: BTreeMap<String, Vec<u8>>,
}

// ------------------------------------------------- schedule payloads

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct ScheduleSpec {
    #[serde(skip_serializing_if = "String::is_empty")]
    pub cron_expression: String,
    pub start_time: GoTime,
    pub end_time: GoTime,
    /// Go `time.Duration` (nanoseconds).
    #[serde(skip_serializing_if = "is_zero_i64")]
    pub jitter: i64,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct StartWorkflowAction {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workflow_type: Option<WorkflowType>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_list: Option<TaskList>,
    #[serde(with = "b64", skip_serializing_if = "Vec::is_empty")]
    pub input: Vec<u8>,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub workflow_id_prefix: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub execution_start_to_close_timeout_seconds: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_start_to_close_timeout_seconds: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_policy: Option<RetryPolicy>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub memo: Option<Memo>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub search_attributes: Option<SearchAttributes>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct ScheduleAction {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_workflow: Option<StartWorkflowAction>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct SchedulePolicies {
    #[serde(skip_serializing_if = "zero_overlap")]
    pub overlap_policy: OverlapPolicy,
    #[serde(skip_serializing_if = "zero_catch_up")]
    pub catch_up_policy: CatchUpPolicy,
    /// Go `time.Duration` (nanoseconds).
    #[serde(skip_serializing_if = "is_zero_i64")]
    pub catch_up_window: i64,
    #[serde(skip_serializing_if = "is_false")]
    pub pause_on_failure: bool,
    #[serde(skip_serializing_if = "is_zero_i32")]
    pub buffer_limit: i32,
    #[serde(skip_serializing_if = "is_zero_i32")]
    pub concurrency_limit: i32,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct BackfillInfo {
    #[serde(skip_serializing_if = "String::is_empty")]
    pub backfill_id: String,
    pub start_time: GoTime,
    pub end_time: GoTime,
    #[serde(skip_serializing_if = "is_zero_i32")]
    pub runs_completed: i32,
    #[serde(skip_serializing_if = "is_zero_i32")]
    pub runs_total: i32,
}

// ------------------------------------------------ scheduler workflow

pub const TRIGGER_SOURCE_SCHEDULE: &str = "schedule";
pub const TRIGGER_SOURCE_BACKFILL: &str = "backfill";

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct RunningWorkflowInfo {
    pub workflow_id: String,
    pub run_id: String,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct BufferedFire {
    pub scheduled_time: GoTime,
    pub trigger_source: String,
    #[serde(skip_serializing_if = "zero_overlap")]
    pub overlap_policy: OverlapPolicy,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub backfill_id: String,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct BackfillRequest {
    pub start_time: GoTime,
    pub end_time: GoTime,
    pub overlap_policy: OverlapPolicy,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub backfill_id: String,
    #[serde(skip_serializing_if = "is_zero_i32")]
    pub runs_total: i32,
    #[serde(skip_serializing_if = "is_zero_i32")]
    pub runs_completed: i32,
    #[serde(skip_serializing_if = "is_false")]
    pub runs_total_computed: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct SchedulerWorkflowState {
    pub paused: bool,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub pause_reason: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub paused_by: String,
    /// Go `json:"-"`: transient, not persisted across ContinueAsNew.
    #[serde(skip)]
    pub deleted: bool,
    pub last_run_time: GoTime,
    pub last_processed_time: GoTime,
    pub next_run_time: GoTime,
    pub total_runs: i64,
    pub missed_runs: i64,
    pub skipped_runs: i64,
    pub iterations: i64,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub pending_backfills: Vec<BackfillRequest>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub buffered_fires: Vec<BufferedFire>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_started_workflow: Option<RunningWorkflowInfo>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub running_workflows: Vec<RunningWorkflowInfo>,
    #[serde(skip_serializing_if = "zero_catch_up")]
    pub unpause_catch_up_policy: CatchUpPolicy,
    pub create_time: GoTime,
    pub last_update_time: GoTime,
    pub paused_at: GoTime,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct SchedulerWorkflowInput {
    pub domain: String,
    pub schedule_id: String,
    pub spec: ScheduleSpec,
    pub action: ScheduleAction,
    pub policies: SchedulePolicies,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub search_attributes: Option<SearchAttributes>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub memo: Option<Memo>,
    pub state: SchedulerWorkflowState,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct PauseSignal {
    #[serde(skip_serializing_if = "String::is_empty")]
    pub reason: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub paused_by: String,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct UnpauseSignal {
    #[serde(skip_serializing_if = "String::is_empty")]
    pub reason: String,
    #[serde(skip_serializing_if = "zero_catch_up")]
    pub catch_up_policy: CatchUpPolicy,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct UpdateSignal {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub spec: Option<ScheduleSpec>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub action: Option<ScheduleAction>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub policies: Option<SchedulePolicies>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub search_attributes: Option<SearchAttributes>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct BackfillSignal {
    pub start_time: GoTime,
    pub end_time: GoTime,
    pub overlap_policy: OverlapPolicy,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub backfill_id: String,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct ScheduleDescription {
    pub schedule_id: String,
    pub domain: String,
    pub spec: ScheduleSpec,
    pub action: ScheduleAction,
    pub policies: SchedulePolicies,
    pub paused: bool,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub pause_reason: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub paused_by: String,
    pub paused_at: GoTime,
    pub last_run_time: GoTime,
    pub next_run_time: GoTime,
    pub total_runs: i64,
    pub missed_runs: i64,
    pub skipped_runs: i64,
    pub buffered_fire_count: i64,
    pub running_workflow_count: i64,
    pub create_time: GoTime,
    pub last_update_time: GoTime,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub memo: Option<Memo>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub search_attributes: Option<SearchAttributes>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub ongoing_backfills: Vec<BackfillInfo>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct ProcessFireRequest {
    pub domain: String,
    pub schedule_id: String,
    pub action: StartWorkflowAction,
    pub scheduled_time: GoTime,
    pub trigger_source: String,
    pub overlap_policy: OverlapPolicy,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_started_workflow: Option<RunningWorkflowInfo>,
    #[serde(skip_serializing_if = "is_zero_i32")]
    pub concurrency_limit: i32,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub running_workflows: Vec<RunningWorkflowInfo>,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub backfill_id: String,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct ProcessFireResult {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_workflow: Option<RunningWorkflowInfo>,
    pub total_delta: i64,
    pub skipped_delta: i64,
    #[serde(skip_serializing_if = "is_false")]
    pub buffered: bool,
    /// Go distinguishes nil from empty: `None` leaves the tracked set alone.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub active_workflows: Option<Vec<RunningWorkflowInfo>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_time_json_matches_go() {
        let v = serde_json::to_string(&GoTime::ZERO).unwrap();
        assert_eq!(v, "\"0001-01-01T00:00:00Z\"");
        let back: GoTime = serde_json::from_str(&v).unwrap();
        assert!(back.is_zero());
    }

    #[test]
    fn time_nano_fraction_and_offset_roundtrip() {
        let t: GoTime = serde_json::from_str("\"2026-11-01T10:20:30.120000+05:30\"").unwrap();
        assert_eq!(t.offset, 19_800);
        assert_eq!(t.nanos, 120_000_000);
        assert_eq!(t.format_rfc3339_nano(), "2026-11-01T10:20:30.12+05:30");
        assert_eq!(t.format_rfc3339_utc(), "2026-11-01T04:50:30Z");
        let u = GoTime::parse_rfc3339("2026-11-01T04:50:30.12Z").unwrap();
        assert_eq!(t, u);
    }

    #[test]
    fn enums_are_strings_and_omitempty() {
        let p = SchedulePolicies {
            overlap_policy: OverlapPolicy::BUFFER,
            catch_up_policy: CatchUpPolicy::ALL,
            catch_up_window: 5,
            buffer_limit: 3,
            ..Default::default()
        };
        assert_eq!(
            serde_json::to_string(&p).unwrap(),
            r#"{"overlapPolicy":"BUFFER","catchUpPolicy":"ALL","catchUpWindow":5,"bufferLimit":3}"#
        );
        assert_eq!(
            serde_json::to_string(&SchedulePolicies::default()).unwrap(),
            "{}"
        );
        let q: SchedulePolicies =
            serde_json::from_str(r#"{"overlapPolicy":"skip_new","catchUpPolicy":"7"}"#).unwrap();
        assert_eq!(q.overlap_policy, OverlapPolicy::SKIP_NEW);
        assert_eq!(q.catch_up_policy.as_go_string(), "ScheduleCatchUpPolicy(7)");
    }

    #[test]
    fn state_json_always_emits_times_and_skips_deleted() {
        let mut st = SchedulerWorkflowState::default();
        st.deleted = true;
        let s = serde_json::to_string(&st).unwrap();
        assert_eq!(
            s,
            concat!(
                r#"{"paused":false,"lastRunTime":"0001-01-01T00:00:00Z","#,
                r#""lastProcessedTime":"0001-01-01T00:00:00Z","nextRunTime":"0001-01-01T00:00:00Z","#,
                r#""totalRuns":0,"missedRuns":0,"skippedRuns":0,"iterations":0,"#,
                r#""createTime":"0001-01-01T00:00:00Z","lastUpdateTime":"0001-01-01T00:00:00Z","#,
                r#""pausedAt":"0001-01-01T00:00:00Z"}"#
            )
        );
        let back: SchedulerWorkflowState = serde_json::from_str(&s).unwrap();
        assert!(!back.deleted);
    }

    #[test]
    fn go_encoded_input_decodes() {
        let json = r#"{"domain":"d","scheduleId":"s","spec":{"cronExpression":"*/5 * * * *","startTime":"0001-01-01T00:00:00Z","endTime":"0001-01-01T00:00:00Z"},"action":{"startWorkflow":{"workflowType":{"name":"wf"},"taskList":{"name":"tl","kind":"NORMAL"},"input":"aGVsbG8=","memo":{"fields":{"k":"dg=="}}}},"policies":{"overlapPolicy":"SKIP_NEW"},"state":{"paused":false,"lastRunTime":"0001-01-01T00:00:00Z","lastProcessedTime":"0001-01-01T00:00:00Z","nextRunTime":"0001-01-01T00:00:00Z","totalRuns":0,"missedRuns":0,"skippedRuns":0,"iterations":0,"createTime":"0001-01-01T00:00:00Z","lastUpdateTime":"0001-01-01T00:00:00Z","pausedAt":"0001-01-01T00:00:00Z"}}"#;
        let input: SchedulerWorkflowInput = serde_json::from_str(json).unwrap();
        let sw = input.action.start_workflow.as_ref().unwrap();
        assert_eq!(sw.input, b"hello");
        assert_eq!(sw.task_list.as_ref().unwrap().kind, Some(TaskListKind::NORMAL));
        assert_eq!(serde_json::to_string(&input).unwrap(), json);
    }

    #[test]
    fn active_workflows_nil_vs_empty() {
        let none = ProcessFireResult::default();
        assert!(!serde_json::to_string(&none).unwrap().contains("activeWorkflows"));
        let r: ProcessFireResult = serde_json::from_str(r#"{"activeWorkflows":[]}"#).unwrap();
        assert_eq!(r.active_workflows, Some(vec![]));
    }
}
