//! Cron schedule support ported from `github.com/robfig/cron/v3` (the
//! `ParseStandard` path used by Cadence) plus `go/common/backoff/cron.go`.
//!
//! Parsing, field bounds, named months/days, `N/step` handling and the
//! day-of-month/day-of-week star semantics are ported line by line from
//! `parser.go` / `spec.go` so accepted and rejected specs match Go.
//!
//! Time base: like the Go code this operates on wall-clock fields. Go's
//! parser defaults to `time.Local`; Cadence servers run in UTC containers, so
//! this port uses UTC (see `parse_standard` for the `CRON_TZ` limitation).
use std::time::Duration;

use chrono::{Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike};

/// Nanoseconds since the epoch to a UTC wall-clock time.
pub fn naive_from_nanos(nanos: i64) -> NaiveDateTime {
    const NANOS_PER_SECOND: i64 = 1_000_000_000;
    chrono::DateTime::from_timestamp(
        nanos.div_euclid(NANOS_PER_SECOND),
        (nanos.rem_euclid(NANOS_PER_SECOND)) as u32,
    )
    .map(|t| t.naive_utc())
    .unwrap_or_default()
}

/// Go's `rand.Int31n(max + 1)` seconds, used for `JitterStartSeconds`.
pub fn jitter_seconds<R: rand::Rng>(max: i32, rng: &mut R) -> Duration {
    if max <= 0 {
        return Duration::ZERO;
    }
    Duration::from_secs(u64::from(rng.gen_range(0..=max as u32)))
}

// Go: starBit = 1 << 63
const STAR_BIT: u64 = 1 << 63;

/// Field bounds plus name tables, mirroring `spec.go`.
#[derive(Clone, Copy)]
struct Bounds {
    min: u32,
    max: u32,
}

const SECONDS: Bounds = Bounds { min: 0, max: 59 };
const MINUTES: Bounds = Bounds { min: 0, max: 59 };
const HOURS: Bounds = Bounds { min: 0, max: 23 };
const DOM: Bounds = Bounds { min: 1, max: 31 };
const MONTHS: Bounds = Bounds { min: 1, max: 12 };
const DOW: Bounds = Bounds { min: 0, max: 6 };

fn month_names(expr: &str) -> Option<u32> {
    Some(match expr.to_ascii_lowercase().as_str() {
        "jan" => 1,
        "feb" => 2,
        "mar" => 3,
        "apr" => 4,
        "may" => 5,
        "jun" => 6,
        "jul" => 7,
        "aug" => 8,
        "sep" => 9,
        "oct" => 10,
        "nov" => 11,
        "dec" => 12,
        _ => return None,
    })
}

fn dow_names(expr: &str) -> Option<u32> {
    Some(match expr.to_ascii_lowercase().as_str() {
        "sun" => 0,
        "mon" => 1,
        "tue" => 2,
        "wed" => 3,
        "thu" => 4,
        "fri" => 5,
        "sat" => 6,
        _ => return None,
    })
}

/// `getBits(min, max, step)` from `parser.go`.
fn get_bits(min: u32, max: u32, step: u32) -> u64 {
    if step == 1 {
        // Go: ^(MaxUint64 << (max + 1)) & (MaxUint64 << min)
        // max <= 59 so `max + 1` never overflows the 64-bit shift.
        return !(u64::MAX << (max + 1)) & (u64::MAX << min);
    }
    let mut bits = 0u64;
    let mut i = min;
    while i <= max {
        bits |= 1 << i;
        i += step;
    }
    bits
}

/// `all(bounds)`: every bit in range plus the star bit.
fn all(b: Bounds) -> u64 {
    get_bits(b.min, b.max, 1) | STAR_BIT
}

/// `parseIntOrName` from `parser.go`.
fn parse_int_or_name(expr: &str, names: fn(&str) -> Option<u32>) -> Result<u32, String> {
    if let Some(v) = names(expr) {
        return Ok(v);
    }
    must_parse_int(expr)
}

/// `mustParseInt` from `parser.go`: optional sign, decimal only, non-negative.
fn must_parse_int(expr: &str) -> Result<u32, String> {
    let err = || format!("failed to parse int from {expr}: {expr}");
    let trimmed = expr.strip_prefix('+').unwrap_or(expr);
    let value: i64 = trimmed.parse().map_err(|_| err())?;
    if value < 0 {
        return Err(format!("negative number ({value}) not allowed: {expr}"));
    }
    u32::try_from(value).map_err(|_| err())
}

/// `getRange` from `parser.go`.
fn get_range(expr: &str, b: Bounds, names: fn(&str) -> Option<u32>) -> Result<u64, String> {
    let start;
    let mut end;
    let mut extra = 0u64;

    let range_and_step: Vec<&str> = expr.split('/').collect();
    let low_and_high: Vec<&str> = range_and_step[0].split('-').collect();
    let single_digit = low_and_high.len() == 1;

    if low_and_high[0] == "*" || low_and_high[0] == "?" {
        start = b.min;
        end = b.max;
        extra = STAR_BIT;
    } else {
        start = parse_int_or_name(low_and_high[0], names)?;
        match low_and_high.len() {
            1 => end = start,
            2 => end = parse_int_or_name(low_and_high[1], names)?,
            _ => return Err(format!("too many hyphens: {expr}")),
        }
    }

    let step = match range_and_step.len() {
        1 => 1,
        2 => {
            let step = must_parse_int(range_and_step[1])?;
            // Special handling: "N/step" means "N-max/step".
            if single_digit {
                end = b.max;
            }
            if step > 1 {
                extra = 0;
            }
            step
        }
        _ => return Err(format!("too many slashes: {expr}")),
    };

    if start < b.min {
        return Err(format!(
            "beginning of range ({start}) below minimum ({}): {}",
            b.min, expr
        ));
    }
    if end > b.max {
        return Err(format!(
            "end of range ({end}) above maximum ({}): {}",
            b.max, expr
        ));
    }
    if start > end {
        return Err(format!(
            "beginning of range ({start}) beyond end of range ({end}): {}",
            expr
        ));
    }
    if step == 0 {
        return Err(format!("step of range should be a positive number: {expr}"));
    }

    Ok(get_bits(start, end, step) | extra)
}

/// `getField` from `parser.go`: comma separated list of ranges.
fn get_field(field: &str, b: Bounds, names: fn(&str) -> Option<u32>) -> Result<u64, String> {
    let mut bits = 0u64;
    for expr in field.split(',') {
        bits |= get_range(expr, b, names)?;
    }
    Ok(bits)
}

/// A parsed cron schedule: either a spec schedule or `@every <delay>`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Schedule {
    Spec(SpecSchedule),
    Every(Duration),
}

/// `SpecSchedule` from `spec.go`, restricted to the standard 5-field grammar.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpecSchedule {
    pub second: u64,
    pub minute: u64,
    pub hour: u64,
    pub dom: u64,
    pub month: u64,
    pub dow: u64,
}

impl Schedule {
    /// `Next(t)`: the next activation time strictly greater than `t`,
    /// `None` when nothing is found within five years (Go returns the
    /// zero time).
    pub fn next(&self, t: NaiveDateTime) -> Option<NaiveDateTime> {
        match self {
            Schedule::Spec(s) => s.next(t),
            Schedule::Every(delay) => {
                // ConstantDelaySchedule.Next: t.Add(delay - t.Nanosecond())
                let nanos = t.and_utc().timestamp_nanos_opt()? % 1_000_000_000;
                let nanos = if nanos < 0 {
                    nanos + 1_000_000_000
                } else {
                    nanos
                };
                let delay_nanos = delay.as_nanos() as i64;
                let offset = delay_nanos - nanos;
                t.checked_add_signed(chrono::Duration::nanoseconds(offset))
            }
        }
    }
}

impl SpecSchedule {
    /// `dayMatches` from `spec.go`.
    fn day_matches(&self, t: &NaiveDateTime) -> bool {
        let dom_match = (1u64 << t.day()) & self.dom != 0;
        let dow_match = (1u64 << t.weekday().num_days_from_sunday()) & self.dow != 0;
        if self.dom & STAR_BIT != 0 || self.dow & STAR_BIT != 0 {
            return dom_match && dow_match;
        }
        dom_match || dow_match
    }

    fn next(&self, t: NaiveDateTime) -> Option<NaiveDateTime> {
        let mut t = t;
        // Start at the earliest possible time (the upcoming second).
        let nanos = t.and_utc().timestamp_nanos_opt()?;
        let frac = nanos.rem_euclid(1_000_000_000);
        t = t.checked_add_signed(chrono::Duration::nanoseconds(1_000_000_000 - frac))?;

        let mut added = false;
        let year_limit = t.year() + 5;

        'wrap: loop {
            if t.year() > year_limit {
                return None;
            }

            // Find the first applicable month.
            while (1u64 << t.month()) & self.month == 0 {
                if !added {
                    added = true;
                    t = midnight_of(NaiveDate::from_ymd_opt(t.year(), t.month(), 1)?);
                }
                t = add_month(t)?;
                if t.month() == 1 {
                    continue 'wrap;
                }
            }

            // Now get a day in that month.
            while !self.day_matches(&t) {
                if !added {
                    added = true;
                    t = midnight_of(t.date());
                }
                t = t.checked_add_signed(chrono::Duration::days(1))?;
                if t.day() == 1 {
                    continue 'wrap;
                }
            }

            while (1u64 << t.hour()) & self.hour == 0 {
                if !added {
                    added = true;
                    t = truncate_hour(t)?;
                }
                t = t.checked_add_signed(chrono::Duration::hours(1))?;
                if t.hour() == 0 {
                    continue 'wrap;
                }
            }

            while (1u64 << t.minute()) & self.minute == 0 {
                if !added {
                    added = true;
                    t = truncate_minute(t)?;
                }
                t = t.checked_add_signed(chrono::Duration::minutes(1))?;
                if t.minute() == 0 {
                    continue 'wrap;
                }
            }

            while (1u64 << t.second()) & self.second == 0 {
                if !added {
                    added = true;
                    t = truncate_second(t)?;
                }
                t = t.checked_add_signed(chrono::Duration::seconds(1))?;
                if t.second() == 0 {
                    continue 'wrap;
                }
            }

            return Some(t);
        }
    }
}

fn midnight_of(date: NaiveDate) -> NaiveDateTime {
    date.and_time(NaiveTime::MIN)
}

fn truncate_hour(t: NaiveDateTime) -> Option<NaiveDateTime> {
    t.date()
        .and_time(NaiveTime::from_hms_opt(t.hour(), 0, 0)?)
        .into()
}

fn truncate_minute(t: NaiveDateTime) -> Option<NaiveDateTime> {
    t.date()
        .and_time(NaiveTime::from_hms_opt(t.hour(), t.minute(), 0)?)
        .into()
}

fn truncate_second(t: NaiveDateTime) -> Option<NaiveDateTime> {
    t.with_nanosecond(0)
}

/// `t.AddDate(0, 1, 0)` for the month loop. Go's `time.Date` normalizes
/// out-of-range days into the following month (Oct 31 -> Dec 1), so this
/// matches that rollover instead of clamping to the month end.
fn add_month(t: NaiveDateTime) -> Option<NaiveDateTime> {
    let (mut year, mut month, mut day) = (t.year(), t.month(), t.day());
    month += 1;
    if month > 12 {
        month = 1;
        year += 1;
    }
    let dim = days_in_month(year, month);
    if day > dim {
        day -= dim;
        month += 1;
        if month > 12 {
            month = 1;
            year += 1;
        }
    }
    NaiveDate::from_ymd_opt(year, month, day).map(|d| d.and_time(t.time()))
}

fn days_in_month(year: i32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if (year % 4 == 0 && year % 100 != 0) || year % 400 == 0 {
                29
            } else {
                28
            }
        }
        _ => 30,
    }
}

/// `time.ParseDuration` for the subset used by cron descriptors:
/// a sequence of `<float><unit>` with units `ns`, `us`/`µs`, `ms`, `s`,
/// `m`, `h`, or the special string `0`.
fn parse_go_duration(s: &str) -> Result<Duration, String> {
    let s = s.trim();
    if s == "0" {
        return Ok(Duration::ZERO);
    }
    let mut rest = s;
    let mut total = Duration::ZERO;
    while !rest.is_empty() {
        // Split at the first unit character.
        let end = rest
            .find(|c: char| !(c.is_ascii_digit() || c == '.'))
            .unwrap_or(rest.len());
        let (number, tail) = rest.split_at(end);
        if number.is_empty() {
            return Err(format!("time: invalid duration {s:?}"));
        }
        let unit_end = tail
            .char_indices()
            .find(|(_, c)| !c.is_ascii_alphabetic() && *c != 'µ')
            .map(|(i, _)| i)
            .unwrap_or(tail.len());
        let (unit, next) = tail.split_at(unit_end);
        let value: f64 = number
            .parse()
            .map_err(|_| format!("time: invalid duration {s:?}"))?;
        if value < 0.0 {
            return Err(format!("time: invalid duration {s:?}"));
        }
        let unit_nanos: f64 = match unit {
            "ns" => 1.0,
            "us" | "µs" | "μs" => 1_000.0,
            "ms" => 1_000_000.0,
            "s" => 1_000_000_000.0,
            "m" => 60.0 * 1_000_000_000.0,
            "h" => 3_600.0 * 1_000_000_000.0,
            _ => return Err(format!("time: unknown unit {unit:?} in duration {s:?}")),
        };
        total += Duration::from_nanos((value * unit_nanos).round() as u64);
        rest = next;
    }
    if total.is_zero() && !s.is_empty() {
        // A non-empty spec that produced no duration is invalid.
        if s.chars().all(|c| c.is_ascii_digit() || c == '.') {
            return Err(format!("time: missing unit in duration {s:?}"));
        }
    }
    Ok(total)
}

/// `Every(duration)` from `constantdelay.go`: sub-second delays round up to one
/// second and sub-second components are truncated.
fn every(duration: Duration) -> Duration {
    let duration = if duration < Duration::from_secs(1) {
        Duration::from_secs(1)
    } else {
        duration
    };
    let nanos = duration.as_nanos();
    Duration::from_nanos((nanos - nanos % 1_000_000_000).min(u128::from(u64::MAX)) as u64)
}

/// `parseDescriptor` from `parser.go`.
fn parse_descriptor(descriptor: &str) -> Result<Schedule, String> {
    match descriptor {
        "@yearly" | "@annually" => Ok(Schedule::Spec(SpecSchedule {
            second: 1 << SECONDS.min,
            minute: 1 << MINUTES.min,
            hour: 1 << HOURS.min,
            dom: 1 << DOM.min,
            month: 1 << MONTHS.min,
            dow: all(DOW),
        })),
        "@monthly" => Ok(Schedule::Spec(SpecSchedule {
            second: 1 << SECONDS.min,
            minute: 1 << MINUTES.min,
            hour: 1 << HOURS.min,
            dom: 1 << DOM.min,
            month: all(MONTHS),
            dow: all(DOW),
        })),
        "@weekly" => Ok(Schedule::Spec(SpecSchedule {
            second: 1 << SECONDS.min,
            minute: 1 << MINUTES.min,
            hour: 1 << HOURS.min,
            dom: all(DOM),
            month: all(MONTHS),
            dow: 1 << DOW.min,
        })),
        "@daily" | "@midnight" => Ok(Schedule::Spec(SpecSchedule {
            second: 1 << SECONDS.min,
            minute: 1 << MINUTES.min,
            hour: 1 << HOURS.min,
            dom: all(DOM),
            month: all(MONTHS),
            dow: all(DOW),
        })),
        "@hourly" => Ok(Schedule::Spec(SpecSchedule {
            second: 1 << SECONDS.min,
            minute: 1 << MINUTES.min,
            hour: all(HOURS),
            dom: all(DOM),
            month: all(MONTHS),
            dow: all(DOW),
        })),
        _ => {
            const EVERY: &str = "@every ";
            match descriptor.strip_prefix(EVERY) {
                Some(d) => Ok(Schedule::Every(every(parse_go_duration(d)?))),
                None => Err(format!("unrecognized descriptor: {descriptor}")),
            }
        }
    }
}

/// `ParseStandard` from `parser.go`: 5 fields (minute, hour, day of month,
/// month, day of week) or a `@descriptor`. Like Go, the seconds field is
/// defaulted to `0` so schedules fire on the minute boundary.
pub fn parse_standard(spec: &str) -> Result<Schedule, String> {
    if spec.is_empty() {
        return Err("empty spec string".to_owned());
    }
    // Go resolves `TZ=`/`CRON_TZ=` through the system zone database. Cadence
    // servers run in UTC, so only UTC zones are accepted here; anything else
    // is a hard error instead of silently using the wrong wall clock.
    let spec = if let Some(rest) = spec
        .strip_prefix("TZ=")
        .or_else(|| spec.strip_prefix("CRON_TZ="))
    {
        let (zone, body) = match rest.split_once(' ') {
            Some((zone, body)) => (zone, body.trim()),
            None => (rest, ""),
        };
        if !zone.eq_ignore_ascii_case("UTC") && !zone.is_empty() {
            return Err(format!(
                "provided bad location {zone}: only UTC zones are supported by this server"
            ));
        }
        body
    } else {
        spec
    };
    if spec.starts_with('@') {
        return parse_descriptor(spec);
    }
    let fields: Vec<&str> = spec.split_whitespace().collect();
    // Standard parser options: Minute | Hour | Dom | Month | Dow.
    if fields.len() != 5 {
        return Err(format!(
            "expected exactly 5 fields, found {}: {:?}",
            fields.len(),
            fields
        ));
    }
    // defaults = ["0", "0", "0", "*", "*", "*"]; second is not configurable.
    let second = get_field("0", SECONDS, |_| None)?;
    let minute = get_field(fields[0], MINUTES, |_| None)?;
    let hour = get_field(fields[1], HOURS, |_| None)?;
    let dom = get_field(fields[2], DOM, |_| None)?;
    let month = get_field(fields[3], MONTHS, month_names)?;
    let dow = get_field(fields[4], DOW, dow_names)?;
    Ok(Schedule::Spec(SpecSchedule {
        second,
        minute,
        hour,
        dom,
        month,
        dow,
    }))
}

/// `ValidateSchedule` from `go/common/backoff/cron.go`.
pub fn validate_schedule(cron_schedule: &str, now: NaiveDateTime) -> Result<Schedule, String> {
    let sched = parse_standard(cron_schedule).map_err(|e| {
        format!("Invalid CronSchedule, failed to parse: {cron_schedule:?}, err: {e}")
    })?;
    if sched.next(now).is_none() {
        return Err(format!(
            "Invalid CronSchedule, no next firing time found, maybe impossible date: {cron_schedule:?}"
        ));
    }
    Ok(sched)
}

/// `GetBackoffForNextSchedule` from `go/common/backoff/cron.go`.
///
/// `jitter` carries Go's `rand.Int31n(jitterStartSeconds+1)` seconds so the
/// caller controls randomness.
pub fn get_backoff_for_next_schedule(
    sched: &Schedule,
    start: NaiveDateTime,
    close: NaiveDateTime,
    overlap_policy: Buffered,
    jitter: Duration,
) -> Result<Duration, String> {
    let mut next = sched
        .next(start)
        .ok_or_else(|| "invalid CronSchedule, no next firing time found".to_owned())?;
    let rounded = if next < close {
        match overlap_policy {
            Buffered::Skipped => {
                while next < close {
                    next = sched.next(next).ok_or_else(|| {
                        "invalid CronSchedule, no next firing time found".to_owned()
                    })?;
                }
                round_up_seconds(next - close)
            }
            // BufferOne and Go's switch default both start immediately.
            Buffered::BufferOne | Buffered::Other => Duration::ZERO,
        }
    } else {
        round_up_seconds(next - close)
    };
    Ok(rounded + jitter)
}

/// Go's `time.Second * time.Duration(math.Ceil(interval.Seconds()))`.
fn round_up_seconds(interval: chrono::Duration) -> Duration {
    let nanos = interval.num_nanoseconds().unwrap_or(i64::MAX);
    let secs = nanos.div_euclid(1_000_000_000);
    let rem = nanos.rem_euclid(1_000_000_000);
    let secs = if rem > 0 { secs + 1 } else { secs };
    Duration::from_secs(secs.max(0) as u64)
}

/// Mirrors Go's `CronOverlapPolicy` switch: `Skipped` (0) and `BufferOne` (1);
/// any other value leaves the backoff at zero, as in Go's default case.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Buffered {
    Skipped,
    BufferOne,
    Other,
}

impl Buffered {
    pub fn from_go_value(v: Option<i32>) -> Self {
        match v {
            Some(0) | None => Buffered::Skipped,
            Some(1) => Buffered::BufferOne,
            _ => Buffered::Other,
        }
    }
}

/// `GetBackoffForNextScheduleInSeconds` from `go/common/backoff/cron.go`.
pub fn get_backoff_for_next_schedule_in_seconds(
    cron_schedule: &str,
    start: NaiveDateTime,
    close: NaiveDateTime,
    overlap_policy: Option<i32>,
    now: NaiveDateTime,
    jitter: Duration,
) -> Result<i32, String> {
    let sched = validate_schedule(cron_schedule, now)?;
    let duration = get_backoff_for_next_schedule(
        &sched,
        start,
        close,
        Buffered::from_go_value(overlap_policy),
        jitter,
    )?;
    // Go: int32(math.Ceil(backoffDuration.Seconds()))
    let secs = duration.as_nanos() as f64 / 1_000_000_000.0;
    Ok(secs.ceil() as i32)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dt(s: &str) -> NaiveDateTime {
        NaiveDate::from_ymd_opt(
            s[..4].parse().unwrap(),
            s[5..7].parse().unwrap(),
            s[8..10].parse().unwrap(),
        )
        .unwrap()
        .and_time(
            NaiveTime::from_hms_opt(
                s[11..13].parse().unwrap(),
                if s.len() > 13 {
                    s[14..16].parse().unwrap()
                } else {
                    0
                },
                if s.len() > 16 {
                    s[17..19].parse().unwrap()
                } else {
                    0
                },
            )
            .unwrap(),
        )
    }

    fn backoff(cron: &str, start: &str, end: &str, policy: Buffered) -> Result<Duration, String> {
        let sched = parse_standard(cron)?;
        get_backoff_for_next_schedule(&sched, dt(start), dt(end), policy, Duration::ZERO)
    }

    // Ported from go/common/backoff/cron_test.go TestCron. Go's cases carry
    // non-UTC offsets and assume a UTC test machine, so the expectations below
    // are the UTC-equivalent results for the same UTC wall-clock inputs.
    #[test]
    fn go_cron_table_matches() {
        let cases = [
            ("0 10 * * *", "2018-12-17T08:00:00", "", 2 * 3600),
            ("0 10 * * *", "2018-12-18T02:00:00", "", 8 * 3600),
            ("0 * * * *", "2018-12-17T08:08:00", "", 52 * 60),
            ("0 * * * *", "2018-12-17T09:00:00", "", 3600),
            ("* * * * *", "2018-12-17T08:08:18", "", 42),
            (
                "0 10 * * *",
                "2018-12-17T08:00:00",
                "2018-12-20T00:00:00",
                10 * 3600,
            ),
            (
                "0 10 * * *",
                "2018-12-17T08:00:00",
                "2018-12-17T09:00:00",
                3600,
            ),
            (
                "*/10 * * * *",
                "2018-12-17T00:04:00",
                "2018-12-17T01:02:00",
                8 * 60,
            ),
            (
                "@every 5h",
                "2018-12-17T08:00:00",
                "2018-12-17T09:00:00",
                4 * 3600,
            ),
            (
                "@every 5h",
                "2018-12-17T08:00:00",
                "2018-12-18T00:00:00",
                4 * 3600,
            ),
            ("0 3 * * 0-6", "2018-12-17T08:00:00", "", 19 * 3600),
        ];
        for (cron, start, end, expected) in cases {
            let got = backoff(
                cron,
                start,
                if end.is_empty() { start } else { end },
                Buffered::Skipped,
            )
            .unwrap_or_else(|e| panic!("{cron} {start} {end}: {e}"));
            assert_eq!(got, Duration::from_secs(expected), "{cron} {start} {end}");
        }
    }

    #[test]
    fn invalid_spec_is_rejected() {
        assert!(parse_standard("invalid-cron-spec").is_err());
        assert!(parse_standard("").is_err());
        // Impossible date: Feb 30 never fires, so ValidateSchedule rejects it
        // even though the spec itself parses.
        assert!(parse_standard("0 0 30 2 *").is_ok());
        assert!(validate_schedule("0 0 30 2 *", dt("2018-12-17T00:00:00")).is_err());
    }

    // Ported from go/common/backoff/cron_test.go
    // TestCronWithoutBufferOneCronWorkflow.
    #[test]
    fn skipped_policy_skips_missed_runs() {
        assert_eq!(
            backoff(
                "0 10 * * *",
                "2018-12-17T10:00:00",
                "2018-12-17T11:00:00",
                Buffered::Skipped
            )
            .unwrap(),
            Duration::from_secs(23 * 3600)
        );
        assert_eq!(
            backoff(
                "0 10 * * *",
                "2018-12-17T10:00:00",
                "2018-12-18T10:45:00",
                Buffered::Skipped
            )
            .unwrap(),
            Duration::from_secs(23 * 3600 + 15 * 60)
        );
    }

    // Ported from go/common/backoff/cron_test.go
    // TestCronWithBufferOneCronWorkflow.
    #[test]
    fn buffer_one_policy_starts_immediately() {
        assert_eq!(
            backoff(
                "0 10 * * *",
                "2018-12-17T10:00:00",
                "2018-12-17T11:00:00",
                Buffered::BufferOne
            )
            .unwrap(),
            Duration::from_secs(23 * 3600)
        );
        assert_eq!(
            backoff(
                "0 10 * * *",
                "2018-12-17T10:00:00",
                "2018-12-18T10:45:00",
                Buffered::BufferOne
            )
            .unwrap(),
            Duration::ZERO
        );
        assert_eq!(
            backoff(
                "0 10 * * *",
                "2018-12-17T10:00:00",
                "2018-12-17T10:30:00",
                Buffered::BufferOne
            )
            .unwrap(),
            Duration::from_secs(23 * 3600 + 30 * 60)
        );
        assert_eq!(
            backoff(
                "0 10 * * *",
                "2018-12-17T10:00:00",
                "2018-12-30T10:45:00",
                Buffered::BufferOne
            )
            .unwrap(),
            Duration::ZERO
        );
    }

    // Ported from go/common/backoff/cron_test.go TestCronWithJitterStart.
    #[test]
    fn jitter_adds_bounded_noise() {
        let sched = parse_standard("* * * * *").unwrap();
        let start = dt("2018-12-17T08:00:00");
        let no_jitter =
            get_backoff_for_next_schedule(&sched, start, start, Buffered::Skipped, Duration::ZERO)
                .unwrap();
        assert_eq!(no_jitter, Duration::from_secs(60));
        let with_jitter = get_backoff_for_next_schedule(
            &sched,
            start,
            start,
            Buffered::Skipped,
            Duration::from_secs(7),
        )
        .unwrap();
        assert_eq!(with_jitter, Duration::from_secs(67));

        // @every is measured from the given time, not aligned to the minute.
        let every = parse_standard("@every 60s").unwrap();
        let got = get_backoff_for_next_schedule(
            &every,
            dt("2018-12-17T08:00:45"),
            dt("2018-12-17T08:00:45"),
            Buffered::Skipped,
            Duration::ZERO,
        )
        .unwrap();
        assert_eq!(got, Duration::from_secs(60));
    }

    #[test]
    fn next_times_follow_the_spec() {
        let sched = parse_standard("0 10 * * *").unwrap();
        let t = sched.next(dt("2018-12-17T08:00:00")).unwrap();
        assert_eq!(t, dt("2018-12-17T10:00:00"));
        assert_eq!(sched.next(t).unwrap(), dt("2018-12-18T10:00:00"));

        // Day-of-week names and ranges.
        let sched = parse_standard("0 12 * * MON-FRI").unwrap();
        let friday = sched.next(dt("2018-12-14T09:00:00")).unwrap();
        assert_eq!(friday, dt("2018-12-14T12:00:00"));
        assert_eq!(sched.next(friday).unwrap(), dt("2018-12-17T12:00:00"));

        // Month names.
        let sched = parse_standard("0 0 1 JAN *").unwrap();
        assert_eq!(
            sched.next(dt("2018-12-17T00:00:00")).unwrap(),
            dt("2019-01-01T00:00:00")
        );

        // Step values, including "N/step" meaning "N-max/step".
        let sched = parse_standard("5/15 * * * *").unwrap();
        assert_eq!(
            sched.next(dt("2018-12-17T08:00:00")).unwrap(),
            dt("2018-12-17T08:05:00")
        );
        assert_eq!(
            sched.next(dt("2018-12-17T08:05:00")).unwrap(),
            dt("2018-12-17T08:20:00")
        );

        // Dom and Dow both restricted means OR, matching cron semantics.
        let sched = parse_standard("0 0 1 * FRI").unwrap();
        assert_eq!(
            sched.next(dt("2023-10-01T00:00:00")).unwrap(),
            dt("2023-10-06T00:00:00")
        );
        assert_eq!(
            sched.next(dt("2023-10-26T00:00:00")).unwrap(),
            dt("2023-10-27T00:00:00")
        );
        assert_eq!(
            sched.next(dt("2023-10-27T00:00:00")).unwrap(),
            dt("2023-11-01T00:00:00")
        );

        // Both dom and dow unrestricted stays an AND of two always-true sets.
        let sched = parse_standard("0 0 * * *").unwrap();
        assert_eq!(
            sched.next(dt("2018-12-17T00:00:00")).unwrap(),
            dt("2018-12-18T00:00:00")
        );
    }

    #[test]
    fn descriptors_parse() {
        for spec in [
            "@yearly",
            "@annually",
            "@monthly",
            "@weekly",
            "@daily",
            "@midnight",
            "@hourly",
            "@every 1h30m",
            "@every 500ms",
            "@every 90ms",
        ] {
            assert!(parse_standard(spec).is_ok(), "{spec}");
        }
        assert!(parse_standard("@nope").is_err());
        assert!(parse_standard("@every 1x").is_err());
    }

    #[test]
    fn in_seconds_helper_matches_go() {
        let secs = get_backoff_for_next_schedule_in_seconds(
            "0 10 * * *",
            dt("2018-12-17T08:00:00"),
            dt("2018-12-17T08:00:00"),
            Some(0),
            dt("2018-12-17T08:00:00"),
            Duration::ZERO,
        )
        .unwrap();
        assert_eq!(secs, 2 * 3600);
    }

    #[test]
    fn non_utc_zones_are_rejected() {
        assert!(parse_standard("CRON_TZ=America/New_York 5 16 * * *").is_err());
        assert!(parse_standard("TZ=UTC 5 16 * * *").is_ok());
    }
}
