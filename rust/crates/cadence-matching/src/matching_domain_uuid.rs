//! Go matching handler `h.domainName(id)` backend parity for malformed domain IDs.
//!
//! The handler (go/service/matching/handler/handler.go:559) swallows every *returned*
//! domain-cache error, so ordinary unknown/empty lookups give "" and the request goes on.
//! The SQL domain store (sql_domain_store.go:206-207) calls `MustParseUUID` on a non-empty
//! ID and panics; `CapturePanic` turns that into a plain `panic object is not error: ...`
//! error before throttling, the deadline check or the engine run. Cassandra (gocql
//! `ParseUUID` returns an error) never panics. This module models only that panic; the
//! engine/manager domain lookups keep returning their typed errors.

use tonic::Status;

/// How the configured persistence backend treats a non-empty, non-UUID domain ID.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DomainIdParse {
    /// Cassandra (and tests/embedded development): the lookup error is swallowed.
    #[default]
    Lenient,
    /// sqlite, mysql, postgres: google/uuid v1.6 `MustParse` panic.
    GoogleUuidPanic,
}

fn is_hex(byte: u8) -> bool {
    byte.is_ascii_hexdigit()
}

/// google/uuid v1.6.0 `Parse` error text, or `None` when Go accepts the string.
pub fn go_uuid_parse_error(input: &str) -> Option<String> {
    go_uuid_parse_error_bytes(input.as_bytes())
}

/// Parse the raw bytes of a Go string without changing its identity.
pub fn go_uuid_parse_error_bytes(bytes: &[u8]) -> Option<String> {
    let body: &[u8] = match bytes.len() {
        36 => bytes,
        45 => {
            if !bytes[..9].eq_ignore_ascii_case(b"urn:uuid:") {
                return Some(format!(
                    "invalid urn prefix: {}",
                    cadence_common::go_string::GoString::from(&bytes[..9]).go_quoted_string()
                ));
            }
            &bytes[9..]
        }
        // Go only drops the first byte and ignores the last: any bracket characters pass.
        38 => &bytes[1..],
        32 => {
            return if bytes.iter().all(|b| is_hex(*b)) {
                None
            } else {
                Some("invalid UUID format".to_string())
            };
        }
        n => return Some(format!("invalid UUID length: {n}")),
    };
    if body[8] != b'-' || body[13] != b'-' || body[18] != b'-' || body[23] != b'-' {
        return Some("invalid UUID format".to_string());
    }
    for start in [0, 2, 4, 6, 9, 11, 14, 16, 19, 21, 24, 26, 28, 30, 32, 34] {
        if !is_hex(body[start]) || !is_hex(body[start + 1]) {
            return Some("invalid UUID format".to_string());
        }
    }
    None
}

/// gocql ParseUUID accepts hyphens at any even hex-digit boundary.
pub fn cassandra_uuid_bytes(input: &[u8]) -> Result<uuid::Uuid, String> {
    let mut digits = [0u8; 32];
    let mut count = 0;
    for &byte in input {
        if byte == b'-' && count % 2 == 0 {
            continue;
        }
        if !byte.is_ascii_hexdigit() || count == digits.len() {
            return Err(format!(
                "invalid UUID {}",
                cadence_common::go_string::GoString::from(input).go_quoted_string()
            ));
        }
        digits[count] = byte;
        count += 1;
    }
    if count != digits.len() {
        return Err(format!(
            "invalid UUID {}",
            cadence_common::go_string::GoString::from(input).go_quoted_string()
        ));
    }
    // All bytes are ASCII hexadecimal and exactly 32 digits long.
    Ok(
        uuid::Uuid::parse_str(std::str::from_utf8(&digits).expect("ASCII hex"))
            .expect("32 hex digits"),
    )
}

/// Pre-handler lookup equivalent. `Err` is the captured-panic status Go returns; `Ok` means
#[allow(clippy::result_large_err)]
/// continue (empty, valid, or lenient backend). It performs no lookup and has no side effects.
pub fn domain_lookup_guard(
    mode: DomainIdParse,
    domain_id: &str,
    operation: &'static str,
) -> Result<(), Status> {
    if mode != DomainIdParse::GoogleUuidPanic || domain_id.is_empty() {
        return Ok(());
    }
    match go_uuid_parse_error(domain_id) {
        None => Ok(()),
        Some(error) => Err(crate::matching_panic::captured_panic_status(
            operation,
            format!("uuid: Parse({domain_id}): {error}"),
        )),
    }
}

/// Model SQL MustParseUUID after a cache miss, including arbitrary Go string bytes.
#[allow(clippy::result_large_err)]
pub fn raw_sql_lookup_guard(id: &[u8]) -> Result<(), Status> {
    if id.is_empty() {
        return Ok(());
    }
    if let Some(error) = go_uuid_parse_error_bytes(id) {
        let mut panic = b"uuid: Parse(".to_vec();
        panic.extend_from_slice(id);
        panic.extend_from_slice(format!("): {error}").as_bytes());
        let quoted = cadence_common::go_string::GoString::from(panic.as_slice()).go_quoted_string();
        return Err(Status::unknown(format!(
            "panic object is not error: {quoted}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const GOOD: &str = "0123abcd-4567-89ef-ABCD-0123456789ab";

    #[test]
    fn length_errors() {
        for n in [0usize, 3, 31, 33, 35, 37, 39, 44, 46] {
            assert_eq!(
                go_uuid_parse_error(&"a".repeat(n)),
                Some(format!("invalid UUID length: {n}"))
            );
        }
    }

    #[test]
    fn accepted_forms() {
        assert_eq!(go_uuid_parse_error(GOOD), None);
        assert_eq!(go_uuid_parse_error(&GOOD.replace('-', "")), None);
        assert_eq!(go_uuid_parse_error(&format!("urn:uuid:{GOOD}")), None);
        assert_eq!(go_uuid_parse_error(&format!("URN:UUID:{GOOD}")), None);
        assert_eq!(go_uuid_parse_error(&format!("{{{GOOD}}}")), None);
        // 38 bytes: first and last are arbitrary.
        assert_eq!(go_uuid_parse_error(&format!("<{GOOD}!")), None);
        assert_eq!(go_uuid_parse_error(&format!("x{GOOD}y")), None);
    }

    #[test]
    fn format_and_prefix_errors() {
        assert_eq!(
            go_uuid_parse_error(&format!("urn:xxxx:{GOOD}")),
            Some("invalid urn prefix: \"urn:xxxx:\"".to_string())
        );
        let bad_hyphen = GOOD.replacen('-', "x", 1);
        assert_eq!(
            go_uuid_parse_error(&bad_hyphen),
            Some("invalid UUID format".into())
        );
        let bad_hex = GOOD.replacen('0', "g", 1);
        assert_eq!(
            go_uuid_parse_error(&bad_hex),
            Some("invalid UUID format".into())
        );
        assert_eq!(
            go_uuid_parse_error(&"z".repeat(32)),
            Some("invalid UUID format".into())
        );
    }

    #[test]
    fn raw_bytes_follow_go_parser_and_quoting() {
        assert_eq!(
            go_uuid_parse_error_bytes(&[0xff]),
            Some("invalid UUID length: 1".into())
        );
        let mut bracketed = format!("x{GOOD}y").into_bytes();
        bracketed[0] = 0xff;
        assert_eq!(go_uuid_parse_error_bytes(&bracketed), None);
        let error = raw_sql_lookup_guard(&[0xff]).unwrap_err();
        assert_eq!(error.code(), tonic::Code::Unknown);
        assert_eq!(
            error.message(),
            r#"panic object is not error: "uuid: Parse(\xff): invalid UUID length: 1""#
        );
    }

    #[test]
    fn cassandra_parser_follows_gocql_nibble_boundaries() {
        let expected = uuid::Uuid::parse_str(GOOD).unwrap();
        for raw in [
            GOOD.to_string(),
            GOOD.replace('-', ""),
            format!("--{}--", GOOD),
            format!("{}-", GOOD),
        ] {
            assert_eq!(cassandra_uuid_bytes(raw.as_bytes()).unwrap(), expected);
        }
        for raw in [
            &b"\xff"[..],
            b"0-123abcd-4567-89ef-ABCD-0123456789ab",
            b"{0123abcd-4567-89ef-ABCD-0123456789ab}",
        ] {
            let error = cassandra_uuid_bytes(raw).unwrap_err();
            assert_eq!(
                error,
                format!(
                    "invalid UUID {}",
                    cadence_common::go_string::GoString::from(raw).go_quoted_string()
                )
            );
        }
    }

    #[test]
    fn guard_modes() {
        let sql = DomainIdParse::GoogleUuidPanic;
        let error = domain_lookup_guard(sql, "abc", "op").unwrap_err();
        assert_eq!(error.code(), tonic::Code::Unknown);
        assert_eq!(
            error.message(),
            r#"panic object is not error: "uuid: Parse(abc): invalid UUID length: 3""#
        );
        assert!(crate::matching_panic::is_captured_panic(&error));
        assert!(domain_lookup_guard(sql, "", "op").is_ok());
        assert!(domain_lookup_guard(sql, GOOD, "op").is_ok());
        assert!(domain_lookup_guard(DomainIdParse::Lenient, "abc", "op").is_ok());
    }
}
