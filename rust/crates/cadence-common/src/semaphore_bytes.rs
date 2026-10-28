//! Byte preserving Go semaphore owner codec (`go/common/semaphore/owner.go`).
use crate::go_string::GoString;

const SEP: u8 = b':';

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OwnerBytes {
    pub workflow_id: GoString,
    pub run_id: GoString,
    pub hold_id: i64,
}

pub fn encode(owner: &OwnerBytes) -> GoString {
    let mut out = owner.workflow_id.len().to_string().into_bytes();
    out.push(SEP);
    out.extend_from_slice(owner.workflow_id.as_bytes());
    out.push(SEP);
    out.extend_from_slice(owner.run_id.as_bytes());
    out.push(SEP);
    out.extend_from_slice(owner.hold_id.to_string().as_bytes());
    GoString::from(out)
}

/// Parse canonical Go owner IDs. All offsets and lengths are byte offsets.
pub fn parse_owner_bytes(input: &[u8]) -> Result<OwnerBytes, String> {
    let err = |s: String| format!("owner_id {} {s}", GoString::from(input).go_quoted_string());
    let Some(sep) = input.iter().position(|b| *b == SEP) else {
        return Err(err("has no length prefix".into()));
    };
    let prefix = &input[..sep];
    let length = parse_go_int(prefix).filter(|n| *n >= 0).and_then(|n| usize::try_from(n).ok())
        .ok_or_else(|| err(format!("has an invalid length prefix {}", GoString::from(prefix).go_quoted_string())))?;
    if prefix != length.to_string().as_bytes() {
        return Err(err(format!("has a non-canonical length prefix {}", GoString::from(prefix).go_quoted_string())));
    }
    let rest = &input[sep + 1..];
    if length >= rest.len() { return Err(err(format!("is shorter than its length prefix {length} claims"))); }
    if rest[length] != SEP { return Err(err(format!("has no separator after its {length}-byte workflow id"))); }
    let workflow = &rest[..length];
    let tail = &rest[length + 1..];
    let Some(mid) = tail.iter().rposition(|b| *b == SEP) else {
        return Err(err("has no separator between run id and hold id".into()));
    };
    let hold_text = &tail[mid + 1..];
    let hold = parse_go_i64(hold_text).ok_or_else(|| err(format!("has an invalid hold id {}", GoString::from(hold_text).go_quoted_string())))?;
    if hold_text != hold.to_string().as_bytes() {
        return Err(err(format!("has a non-canonical hold id {}", GoString::from(hold_text).go_quoted_string())));
    }
    Ok(OwnerBytes { workflow_id: workflow.into(), run_id: tail[..mid].into(), hold_id: hold })
}

// strconv.Atoi/ParseInt decimal syntax: optional sign, then at least one ASCII digit.
fn parse_go_int(b: &[u8]) -> Option<i64> { parse_signed(b) }
fn parse_go_i64(b: &[u8]) -> Option<i64> { parse_signed(b) }
fn parse_signed(b: &[u8]) -> Option<i64> {
    if b.is_empty() { return None; }
    let (negative, digits) = match b[0] { b'-' => (true, &b[1..]), b'+' => (false, &b[1..]), _ => (false, b) };
    if digits.is_empty() || !digits.iter().all(u8::is_ascii_digit) { return None; }
    // Accumulate as an unsigned magnitude so the one extra negative value
    // (`MinInt64`) remains representable, as it is in strconv.ParseInt.
    let mut n: u64 = 0;
    for d in digits {
        n = n.checked_mul(10)?.checked_add(u64::from(*d - b'0'))?;
    }
    if negative {
        if n == (i64::MAX as u64) + 1 { Some(i64::MIN) }
        else { i64::try_from(n).ok()?.checked_neg() }
    } else { i64::try_from(n).ok() }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test] fn arbitrary_bytes_round_trip_and_distinguish_replacement() {
        for workflow in [vec![0xff, b':', 0, 0xfe], "�".as_bytes().to_vec(), "é".as_bytes().to_vec()] {
            let owner = OwnerBytes { workflow_id: workflow.into(), run_id: vec![0xfe, b':', 0xff].into(), hold_id: -7 };
            let encoded = encode(&owner);
            assert_eq!(parse_owner_bytes(encoded.as_bytes()).unwrap(), owner);
        }
        assert_ne!(encode(&OwnerBytes { workflow_id: vec![0xff].into(), ..OwnerBytes::default() }), encode(&OwnerBytes { workflow_id: "�".into(), ..OwnerBytes::default() }));
    }
    #[test] fn malformed_and_numeric_spellings_follow_go_canonical_rules() {
        for s in [&b""[..], b"0", b"+1:x:r:1", b"01:x:r:1", b"2:\xc3:r:1", b"1:x:r:+1", b"1:x:r:-0", b"1:x:r:01", b"1:x:r:9223372036854775808"] { assert!(parse_owner_bytes(s).is_err(), "{s:?}"); }
        assert_eq!(parse_owner_bytes(b"1:x:r:-9223372036854775808").unwrap().hold_id, i64::MIN);
        assert_eq!(parse_owner_bytes(b"1:x:r:+1").unwrap_err(), "owner_id \"1:x:r:+1\" has a non-canonical hold id \"+1\"");
        assert_eq!(parse_owner_bytes(b"\xff").unwrap_err(), "owner_id \"\\xff\" has no length prefix");
        let split_unicode = parse_owner_bytes(b"1:\xc3:\xfe:1").unwrap();
        assert_eq!(split_unicode.workflow_id.as_bytes(), &[0xc3]);
        assert_eq!(split_unicode.run_id.as_bytes(), &[0xfe]);
    }
}
