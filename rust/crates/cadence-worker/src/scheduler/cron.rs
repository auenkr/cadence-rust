//! Port of robfig/cron v3.0.1 `ParseStandard` and `Schedule.Next` (parser.go,
//! spec.go, constantdelay.go), including Go's `time.Date`/`AddDate` wall-clock
//! arithmetic and `time.ParseDuration`, so schedules fire at the same
//! instants as the Go scheduler workflow.

use chrono::{Offset, TimeZone};
use chrono_tz::Tz;

use super::types::GoTime;

const STAR_BIT: u64 = 1 << 63;

// ------------------------------------------------------------- zones

/// A Go `*time.Location` as far as cron needs it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Zone {
    Local,
    Utc,
    Named(Tz),
    Fixed(i32),
}

impl Zone {
    /// UTC offset in seconds at the given Unix instant.
    pub fn offset_at(&self, utc_secs: i64) -> i32 {
        match self {
            Zone::Utc => 0,
            Zone::Fixed(o) => *o,
            Zone::Local => chrono::Local
                .timestamp_opt(utc_secs, 0)
                .single()
                .map(|d| d.offset().fix().local_minus_utc())
                .unwrap_or(0),
            Zone::Named(tz) => tz
                .timestamp_opt(utc_secs, 0)
                .single()
                .map(|d| d.offset().fix().local_minus_utc())
                .unwrap_or(0),
        }
    }

    /// Go `time.LoadLocation`.
    pub fn load(name: &str) -> Result<Zone, String> {
        match name {
            "" | "UTC" => Ok(Zone::Utc),
            "Local" => Ok(Zone::Local),
            other => other
                .parse::<Tz>()
                .map(Zone::Named)
                .map_err(|_| format!("unknown time zone {other}")),
        }
    }
}

/// The location of an input time (offset 0 is treated as UTC).
fn zone_of(t: &GoTime) -> Zone {
    if t.offset == 0 {
        Zone::Utc
    } else if Zone::Local.offset_at(t.secs) == t.offset {
        Zone::Local
    } else {
        Zone::Fixed(t.offset)
    }
}

fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y.rem_euclid(400);
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }, m, d)
}

#[derive(Clone, Copy)]
struct Wall {
    year: i64,
    month: i64,
    day: i64,
    hour: i64,
    min: i64,
    sec: i64,
    weekday: i64,
}

fn wall(secs: i64, zone: Zone) -> Wall {
    let local = secs + i64::from(zone.offset_at(secs));
    let days = local.div_euclid(86_400);
    let rem = local.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    Wall {
        year,
        month,
        day,
        hour: rem / 3600,
        min: rem % 3600 / 60,
        sec: rem % 60,
        weekday: (days + 4).rem_euclid(7),
    }
}

/// Go `time.Date(y, mo, d, h, mi, s, 0, loc)` with normalisation.
fn date(y: i64, mo: i64, d: i64, h: i64, mi: i64, s: i64, zone: Zone) -> i64 {
    let m = mo - 1;
    let y = y + m.div_euclid(12);
    let m = m.rem_euclid(12);
    let days = days_from_civil(y, m + 1, 1) + (d - 1);
    let mut unix = days * 86_400 + h * 3600 + mi * 60 + s;
    let mut off = zone.offset_at(unix);
    if off != 0 {
        let utc = unix - i64::from(off);
        off = zone.offset_at(utc);
        // Go keeps the first offset when utc lies in the same zone period;
        // equal offsets make both branches agree.
    }
    unix -= i64::from(off);
    unix
}

// --------------------------------------------------------- schedules

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SpecSchedule {
    pub second: u64,
    pub minute: u64,
    pub hour: u64,
    pub dom: u64,
    pub month: u64,
    pub dow: u64,
    pub location: Zone,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Schedule {
    Spec(SpecSchedule),
    /// `ConstantDelaySchedule` delay in nanoseconds.
    Every(i64),
}

impl Schedule {
    pub fn next(&self, t: &GoTime) -> GoTime {
        match self {
            Schedule::Every(delay) => t.add_nanos(delay - i64::from(t.nanos)),
            Schedule::Spec(s) => s.next(t),
        }
    }
}

fn day_matches(s: &SpecSchedule, w: &Wall) -> bool {
    let dom_match = (1u64 << w.day) & s.dom > 0;
    let dow_match = (1u64 << w.weekday) & s.dow > 0;
    if s.dom & STAR_BIT > 0 || s.dow & STAR_BIT > 0 {
        dom_match && dow_match
    } else {
        dom_match || dow_match
    }
}

impl SpecSchedule {
    pub fn next(&self, input: &GoTime) -> GoTime {
        let orig = zone_of(input);
        let loc = if self.location == Zone::Local {
            orig
        } else {
            self.location
        };
        // t = t.Add(1s - nanos)
        let mut t = input.secs + 1;
        let mut added = false;
        let year_limit = wall(t, loc).year + 5;
        'wrap: loop {
            let w = wall(t, loc);
            if w.year > year_limit {
                return GoTime::ZERO;
            }
            // Month.
            loop {
                let w = wall(t, loc);
                if (1u64 << w.month) & self.month != 0 {
                    break;
                }
                if !added {
                    added = true;
                    t = date(w.year, w.month, 1, 0, 0, 0, loc);
                }
                let w = wall(t, loc);
                t = date(w.year, w.month + 1, w.day, w.hour, w.min, w.sec, loc);
                if wall(t, loc).month == 1 {
                    continue 'wrap;
                }
            }
            // Day.
            loop {
                let w = wall(t, loc);
                if day_matches(self, &w) {
                    break;
                }
                if !added {
                    added = true;
                    t = date(w.year, w.month, w.day, 0, 0, 0, loc);
                }
                let w = wall(t, loc);
                t = date(w.year, w.month, w.day + 1, w.hour, w.min, w.sec, loc);
                let w = wall(t, loc);
                if w.hour != 0 {
                    if w.hour > 12 {
                        t += (24 - w.hour) * 3600;
                    } else {
                        t -= w.hour * 3600;
                    }
                }
                if wall(t, loc).day == 1 {
                    continue 'wrap;
                }
            }
            // Hour.
            loop {
                let w = wall(t, loc);
                if (1u64 << w.hour) & self.hour != 0 {
                    break;
                }
                if !added {
                    added = true;
                    t = date(w.year, w.month, w.day, w.hour, 0, 0, loc);
                }
                t += 3600;
                if wall(t, loc).hour == 0 {
                    continue 'wrap;
                }
            }
            // Minute.
            loop {
                let w = wall(t, loc);
                if (1u64 << w.min) & self.minute != 0 {
                    break;
                }
                if !added {
                    added = true;
                    t -= t.rem_euclid(60);
                }
                t += 60;
                if wall(t, loc).min == 0 {
                    continue 'wrap;
                }
            }
            // Second.
            loop {
                let w = wall(t, loc);
                if (1u64 << w.sec) & self.second != 0 {
                    break;
                }
                if !added {
                    added = true;
                }
                t += 1;
                if wall(t, loc).sec == 0 {
                    continue 'wrap;
                }
            }
            break;
        }
        GoTime {
            secs: t,
            nanos: 0,
            offset: orig.offset_at(t),
        }
    }
}

// ------------------------------------------------------------ parser

struct Bounds {
    min: u64,
    max: u64,
    names: &'static [(&'static str, u64)],
}

const SECONDS: Bounds = Bounds { min: 0, max: 59, names: &[] };
const MINUTES: Bounds = Bounds { min: 0, max: 59, names: &[] };
const HOURS: Bounds = Bounds { min: 0, max: 23, names: &[] };
const DOM: Bounds = Bounds { min: 1, max: 31, names: &[] };
const MONTHS: Bounds = Bounds {
    min: 1,
    max: 12,
    names: &[
        ("jan", 1),
        ("feb", 2),
        ("mar", 3),
        ("apr", 4),
        ("may", 5),
        ("jun", 6),
        ("jul", 7),
        ("aug", 8),
        ("sep", 9),
        ("oct", 10),
        ("nov", 11),
        ("dec", 12),
    ],
};
const DOW: Bounds = Bounds {
    min: 0,
    max: 6,
    names: &[
        ("sun", 0),
        ("mon", 1),
        ("tue", 2),
        ("wed", 3),
        ("thu", 4),
        ("fri", 5),
        ("sat", 6),
    ],
};

fn get_bits(min: u64, max: u64, step: u64) -> u64 {
    if step == 1 {
        return !(u64::MAX << (max + 1)) & (u64::MAX << min);
    }
    let mut bits = 0u64;
    let mut i = min;
    while i <= max {
        bits |= 1 << i;
        match i.checked_add(step) {
            Some(n) => i = n,
            None => break,
        }
    }
    bits
}

fn all(b: &Bounds) -> u64 {
    get_bits(b.min, b.max, 1) | STAR_BIT
}

/// Go `strconv.Atoi` error text.
fn atoi(expr: &str) -> Result<i64, String> {
    let digits = expr.strip_prefix(['+', '-']).unwrap_or(expr);
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return Err(format!("strconv.Atoi: parsing {expr:?}: invalid syntax"));
    }
    expr.parse::<i64>()
        .map_err(|_| format!("strconv.Atoi: parsing {expr:?}: value out of range"))
}

fn must_parse_int(expr: &str) -> Result<u64, String> {
    let n = atoi(expr).map_err(|e| format!("failed to parse int from {expr}: {e}"))?;
    if n < 0 {
        return Err(format!("negative number ({n}) not allowed: {expr}"));
    }
    Ok(n as u64)
}

fn parse_int_or_name(expr: &str, names: &[(&str, u64)]) -> Result<u64, String> {
    let lower = expr.to_lowercase();
    if let Some((_, v)) = names.iter().find(|(n, _)| *n == lower) {
        return Ok(*v);
    }
    must_parse_int(expr)
}

fn get_range(expr: &str, r: &Bounds) -> Result<u64, String> {
    let range_and_step: Vec<&str> = expr.split('/').collect();
    let low_and_high: Vec<&str> = range_and_step[0].split('-').collect();
    let single_digit = low_and_high.len() == 1;
    let (start, mut end, step);
    let mut extra = 0u64;
    if low_and_high[0] == "*" || low_and_high[0] == "?" {
        start = r.min;
        end = r.max;
        extra = STAR_BIT;
    } else {
        start = parse_int_or_name(low_and_high[0], r.names)?;
        end = match low_and_high.len() {
            1 => start,
            2 => parse_int_or_name(low_and_high[1], r.names)?,
            _ => return Err(format!("too many hyphens: {expr}")),
        };
    }
    match range_and_step.len() {
        1 => step = 1,
        2 => {
            step = must_parse_int(range_and_step[1])?;
            if single_digit {
                end = r.max;
            }
            if step > 1 {
                extra = 0;
            }
        }
        _ => return Err(format!("too many slashes: {expr}")),
    }
    if start < r.min {
        return Err(format!(
            "beginning of range ({start}) below minimum ({}): {expr}",
            r.min
        ));
    }
    if end > r.max {
        return Err(format!(
            "end of range ({end}) above maximum ({}): {expr}",
            r.max
        ));
    }
    if start > end {
        return Err(format!(
            "beginning of range ({start}) beyond end of range ({end}): {expr}"
        ));
    }
    if step == 0 {
        return Err(format!("step of range should be a positive number: {expr}"));
    }
    Ok(get_bits(start, end, step) | extra)
}

fn get_field(field: &str, r: &Bounds) -> Result<u64, String> {
    let mut bits = 0;
    for expr in field.split(',').filter(|s| !s.is_empty()) {
        bits |= get_range(expr, r)?;
    }
    Ok(bits)
}

/// `cron.ParseStandard`.
pub fn parse_standard(spec: &str) -> Result<Schedule, String> {
    if spec.is_empty() {
        return Err("empty spec string".to_owned());
    }
    let mut loc = Zone::Local;
    let mut spec = spec;
    if spec.starts_with("TZ=") || spec.starts_with("CRON_TZ=") {
        // Go panics when there is no space; surface an error instead.
        let i = spec
            .find(' ')
            .ok_or_else(|| "provided bad location: missing space after time zone".to_owned())?;
        let eq = spec.find('=').unwrap_or(0);
        let name = &spec[eq + 1..i];
        loc = Zone::load(name).map_err(|e| format!("provided bad location {name}: {e}"))?;
        spec = spec[i..].trim();
    }
    if spec.starts_with('@') {
        return parse_descriptor(spec, loc);
    }
    let fields: Vec<&str> = spec.split_whitespace().collect();
    if fields.len() != 5 {
        return Err(format!(
            "expected exactly 5 fields, found {}: [{}]",
            fields.len(),
            fields.join(" ")
        ));
    }
    Ok(Schedule::Spec(SpecSchedule {
        second: 1,
        minute: get_field(fields[0], &MINUTES)?,
        hour: get_field(fields[1], &HOURS)?,
        dom: get_field(fields[2], &DOM)?,
        month: get_field(fields[3], &MONTHS)?,
        dow: get_field(fields[4], &DOW)?,
        location: loc,
    }))
}

fn parse_descriptor(descriptor: &str, loc: Zone) -> Result<Schedule, String> {
    let mk = |minute, hour, dom, month, dow| {
        Ok(Schedule::Spec(SpecSchedule {
            second: 1,
            minute,
            hour,
            dom,
            month,
            dow,
            location: loc,
        }))
    };
    let _ = SECONDS;
    match descriptor {
        "@yearly" | "@annually" => mk(1, 1, 1 << DOM.min, 1 << MONTHS.min, all(&DOW)),
        "@monthly" => mk(1, 1, 1 << DOM.min, all(&MONTHS), all(&DOW)),
        "@weekly" => mk(1, 1, all(&DOM), all(&MONTHS), 1 << DOW.min),
        "@daily" | "@midnight" => mk(1, 1, all(&DOM), all(&MONTHS), all(&DOW)),
        "@hourly" => mk(1, all(&HOURS), all(&DOM), all(&MONTHS), all(&DOW)),
        _ => {
            if let Some(rest) = descriptor.strip_prefix("@every ") {
                let d = parse_duration(rest)
                    .map_err(|e| format!("failed to parse duration {descriptor}: {e}"))?;
                return Ok(every(d));
            }
            Err(format!("unrecognized descriptor: {descriptor}"))
        }
    }
}

/// `cron.Every`.
pub fn every(d: i64) -> Schedule {
    let d = d.max(1_000_000_000);
    Schedule::Every(d - d % 1_000_000_000)
}

// ------------------------------------------------- time.ParseDuration

fn quote(s: &str) -> String {
    format!("{s:?}")
}

fn unit_nanos(u: &str) -> Option<u64> {
    Some(match u {
        "ns" => 1,
        "us" | "\u{b5}s" | "\u{3bc}s" => 1_000,
        "ms" => 1_000_000,
        "s" => 1_000_000_000,
        "m" => 60_000_000_000,
        "h" => 3_600_000_000_000,
        _ => return None,
    })
}

const POW63: u64 = 1 << 63;

/// Go `time.ParseDuration`; returns nanoseconds.
pub fn parse_duration(orig: &str) -> Result<i64, String> {
    let invalid = || format!("time: invalid duration {}", quote(orig));
    let mut s = orig.as_bytes();
    let mut d: u64 = 0;
    let mut neg = false;
    if let Some(&c) = s.first() {
        if c == b'-' || c == b'+' {
            neg = c == b'-';
            s = &s[1..];
        }
    }
    if s == b"0" {
        return Ok(0);
    }
    if s.is_empty() {
        return Err(invalid());
    }
    while !s.is_empty() {
        if !(s[0] == b'.' || s[0].is_ascii_digit()) {
            return Err(invalid());
        }
        // leadingInt
        let before = s.len();
        let mut v: u64 = 0;
        let mut i = 0;
        while i < s.len() && s[i].is_ascii_digit() {
            if v > POW63 / 10 {
                return Err(invalid());
            }
            v = v * 10 + u64::from(s[i] - b'0');
            if v > POW63 {
                return Err(invalid());
            }
            i += 1;
        }
        s = &s[i..];
        let pre = before != s.len();
        let mut f: u64 = 0;
        let mut scale: f64 = 1.0;
        let mut post = false;
        if !s.is_empty() && s[0] == b'.' {
            s = &s[1..];
            let pl = s.len();
            let mut overflow = false;
            let mut i = 0;
            while i < s.len() && s[i].is_ascii_digit() {
                if !overflow {
                    if f > (POW63 - 1) / 10 {
                        overflow = true;
                    } else {
                        let y = f * 10 + u64::from(s[i] - b'0');
                        if y > POW63 {
                            overflow = true;
                        } else {
                            f = y;
                            scale *= 10.0;
                        }
                    }
                }
                i += 1;
            }
            s = &s[i..];
            post = pl != s.len();
        }
        if !pre && !post {
            return Err(invalid());
        }
        let mut i = 0;
        while i < s.len() && !(s[i] == b'.' || s[i].is_ascii_digit()) {
            i += 1;
        }
        if i == 0 {
            return Err(format!("time: missing unit in duration {}", quote(orig)));
        }
        let u = String::from_utf8_lossy(&s[..i]).into_owned();
        s = &s[i..];
        let Some(unit) = unit_nanos(&u) else {
            return Err(format!(
                "time: unknown unit {} in duration {}",
                quote(&u),
                quote(orig)
            ));
        };
        if v > POW63 / unit {
            return Err(invalid());
        }
        v *= unit;
        if f > 0 {
            v += (f as f64 * (unit as f64 / scale)) as u64;
            if v > POW63 {
                return Err(invalid());
            }
        }
        d += v;
        if d > POW63 {
            return Err(invalid());
        }
    }
    if neg {
        return Ok((d as i64).wrapping_neg());
    }
    if d > POW63 - 1 {
        return Err(invalid());
    }
    Ok(d as i64)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(s: &str) -> GoTime {
        GoTime::parse_rfc3339(s).unwrap()
    }

    fn next(spec: &str, from: &str) -> String {
        let sched = parse_standard(spec).unwrap();
        let n = sched.next(&t(from));
        if n.is_zero() {
            return "zero".to_owned();
        }
        n.format_rfc3339_nano()
    }

    #[test]
    fn go_cron_vectors_utc() {
        let u = |s: &str| format!("TZ=UTC {s}");
        assert_eq!(next(&u("30 * * * *"), "2012-07-09T14:45:00Z"), "2012-07-09T15:30:00Z");
        assert_eq!(next(&u("0 0 1 1 *"), "2012-07-09T00:00:00Z"), "2013-01-01T00:00:00Z");
        assert_eq!(next(&u("0 0 29 2 *"), "2013-03-01T00:00:00Z"), "2016-02-29T00:00:00Z");
        assert_eq!(next(&u("@hourly"), "2012-07-09T14:45:30Z"), "2012-07-09T15:00:00Z");
        assert_eq!(next(&u("0 0 1 * 1"), "2012-07-09T00:00:00Z"), "2012-07-16T00:00:00Z");
        assert_eq!(next(&u("*/15 * * * *"), "2012-07-09T14:45:00Z"), "2012-07-09T15:00:00Z");
        assert_eq!(next(&u("0 0 * * MON-FRI"), "2012-07-13T00:00:00Z"), "2012-07-16T00:00:00Z");
        assert_eq!(next(&u("@weekly"), "2012-07-09T00:00:00Z"), "2012-07-15T00:00:00Z");
        assert_eq!(next(&u("@monthly"), "2012-07-09T00:00:00Z"), "2012-08-01T00:00:00Z");
        assert_eq!(next(&u("@yearly"), "2012-07-09T00:00:00Z"), "2013-01-01T00:00:00Z");
        assert_eq!(next(&u("0 0 31 2 *"), "2012-07-09T00:00:00Z"), "zero");
    }

    #[test]
    fn dst_spring_forward_new_york() {
        assert_eq!(
            next("TZ=America/New_York 30 2 11 3 *", "2012-03-11T05:00:00Z"),
            "2013-03-11T06:30:00Z"
        );
    }

    #[test]
    fn every_truncates_to_seconds() {
        let s = parse_standard("@every 90s").unwrap();
        assert_eq!(s, Schedule::Every(90_000_000_000));
        let n = s.next(&t("2012-07-09T14:45:00.5Z"));
        assert_eq!(n.format_rfc3339_nano(), "2012-07-09T14:46:30Z");
        assert_eq!(parse_standard("@every 100ms").unwrap(), Schedule::Every(1_000_000_000));
    }

    #[test]
    fn parse_errors_match_go() {
        let e = |s: &str| parse_standard(s).unwrap_err();
        assert_eq!(e(""), "empty spec string");
        assert_eq!(e("* * * *"), "expected exactly 5 fields, found 4: [* * * *]");
        assert_eq!(e("@bogus"), "unrecognized descriptor: @bogus");
        assert_eq!(e("1-2-3 * * * *"), "too many hyphens: 1-2-3");
        assert_eq!(e("1/2/3 * * * *"), "too many slashes: 1/2/3");
        assert_eq!(e("60 * * * *"), "end of range (60) above maximum (59): 60");
        assert_eq!(e("* * 0 * *"), "beginning of range (0) below minimum (1): 0");
        assert_eq!(e("5-1 * * * *"), "beginning of range (5) beyond end of range (1): 5-1");
        assert_eq!(e("*/0 * * * *"), "step of range should be a positive number: */0");
        assert_eq!(
            e("x * * * *"),
            "failed to parse int from x: strconv.Atoi: parsing \"x\": invalid syntax"
        );
        assert_eq!(e("@every 5"), "failed to parse duration @every 5: time: missing unit in duration \"5\"");
        assert!(e("TZ=Nope/Zone * * * * *").starts_with("provided bad location Nope/Zone: "));
    }

    #[test]
    fn parse_duration_go_vectors() {
        assert_eq!(parse_duration("1h30m").unwrap(), 5_400_000_000_000);
        assert_eq!(parse_duration("1.5s").unwrap(), 1_500_000_000);
        assert_eq!(parse_duration("-2m").unwrap(), -120_000_000_000);
        assert_eq!(parse_duration("0").unwrap(), 0);
        assert_eq!(parse_duration("300ms").unwrap(), 300_000_000);
        assert_eq!(parse_duration("").unwrap_err(), "time: invalid duration \"\"");
        assert_eq!(parse_duration("1x").unwrap_err(), "time: unknown unit \"x\" in duration \"1x\"");
    }

    #[test]
    fn dom_dow_or_semantics() {
        // Both restricted: either may match.
        assert_eq!(next("TZ=UTC 0 0 13 * FRI", "2012-07-09T00:00:00Z"), "2012-07-13T00:00:00Z");
    }
}
