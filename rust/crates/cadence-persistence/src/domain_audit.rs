//! Go-compatible domain audit log blobs (`domain_audit_log.state_before` and
//! `state_after`). Go serializes each snapshot as a raw ThriftRW
//! `DescribeDomainResponse` (no version byte), optionally snappy-compressed,
//! so Go's `ListFailoverHistory` can read rows written by Rust and the reverse.

use crate::matching_blob::BlobEncoding;
use crate::matching_domain_metadata::GoDomainBlob;
use cadence_api::service_types::DescribeDomainResponse;
use cadence_thrift::{
    protocol::{Reader, Writer},
    value::{read_struct, write_struct},
    FromThrift, ToThrift,
};

/// Go `DomainAuditOperationTypeFailover`, the `operation_type` column value
/// that `ListFailoverHistory` filters on.
pub const DOMAIN_AUDIT_OPERATION_TYPE_FAILOVER: i32 = 5;

/// One `domain_audit_log` row with Go's column meaning.
#[derive(Debug, Clone, PartialEq)]
pub struct DomainAuditLog {
    pub event_id: String,
    pub domain_id: String,
    pub operation_type: i32,
    pub created_time_nanos: i64,
    pub last_updated_time_nanos: i64,
    pub identity: String,
    pub identity_type: String,
    pub comment: String,
    pub state_before: GoDomainBlob,
    pub state_after: GoDomainBlob,
}

/// One cursor page of audit rows, newest first.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct DomainAuditLogPage {
    pub logs: Vec<DomainAuditLog>,
    pub next_page_token: Option<Vec<u8>>,
}

/// Opaque cursor. Go gob-encodes `(created_time, event_id)`; clients only echo
/// the bytes back, so the Rust layout is private to this frontend.
pub fn encode_page_token(created_time_nanos: i64, event_id: &str) -> Vec<u8> {
    format!("{created_time_nanos}:{event_id}").into_bytes()
}

pub fn decode_page_token(token: &[u8]) -> Result<(i64, String), String> {
    let text = std::str::from_utf8(token).map_err(|_| "unable to decode next page token")?;
    let (nanos, event_id) = text
        .split_once(':')
        .ok_or("unable to decode next page token")?;
    let nanos = nanos
        .parse()
        .map_err(|_| "unable to decode next page token".to_string())?;
    Ok((nanos, event_id.to_string()))
}

/// UUIDv7 event ID as Go's `uuid.NewV7`: 48-bit unix millisecond prefix,
/// version 7 and RFC 4122 variant bits, random tail.
pub fn new_event_id(now_nanos: i64) -> String {
    let mut bytes = *uuid::Uuid::new_v4().as_bytes();
    let millis = (now_nanos / 1_000_000).max(0) as u64;
    bytes[..6].copy_from_slice(&millis.to_be_bytes()[2..]);
    bytes[6] = (bytes[6] & 0x0f) | 0x70;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    uuid::Uuid::from_bytes(bytes).to_string()
}

/// Go's SQLite DSN writes DATETIME as fixed-width UTC nanoseconds
/// (`_timefmt=2006-01-02T15:04:05.000000000Z07:00`), so range scans compare
/// lexicographically.
pub fn format_sqlite_datetime(nanos: i64) -> Result<String, String> {
    let (secs, sub) = (nanos.div_euclid(1_000_000_000), nanos.rem_euclid(1_000_000_000));
    let time = chrono::DateTime::from_timestamp(secs, sub as u32)
        .ok_or_else(|| format!("timestamp out of range: {nanos}"))?;
    Ok(time.format("%Y-%m-%dT%H:%M:%S%.9fZ").to_string())
}

/// Parse the fixed-width text produced by [`format_sqlite_datetime`].
pub fn parse_sqlite_datetime(text: &str) -> Result<i64, String> {
    let time = chrono::DateTime::parse_from_rfc3339(text)
        .map_err(|error| format!("invalid DATETIME {text}: {error}"))?;
    Ok(time.timestamp() * 1_000_000_000 + i64::from(time.timestamp_subsec_nanos()))
}

/// Encode a domain snapshot the way Go's `serializeGetDomainResponse` does
/// for the audit log (ThriftRW + snappy).
pub fn encode_domain_state(state: &DescribeDomainResponse) -> Result<GoDomainBlob, String> {
    encode_domain_state_with(state, BlobEncoding::ThriftRWSnappy)
}

fn encode_domain_state_with(
    state: &DescribeDomainResponse,
    encoding: BlobEncoding,
) -> Result<GoDomainBlob, String> {
    let mut writer = Writer::new();
    write_struct(&mut writer, &state.to_thrift());
    let data = encoding
        .encode(&writer.into_bytes())
        .map_err(|error| error.to_string())?;
    Ok(GoDomainBlob {
        data,
        encoding: encoding.as_str().to_string(),
    })
}

/// Decode a Go audit snapshot. Go's `deserializeGetDomainResponse` accepts only
/// `thriftrw` and `thriftrw_snappy`; any other encoding is an error.
pub fn decode_domain_state(blob: &GoDomainBlob) -> Result<DescribeDomainResponse, String> {
    let encoding = match blob.encoding.as_str() {
        "thriftrw" => BlobEncoding::ThriftRW,
        "thriftrw_snappy" => BlobEncoding::ThriftRWSnappy,
        other => return Err(format!("unknown encoding type: {other}")),
    };
    let bytes = encoding
        .decode(&blob.data)
        .map_err(|error| error.to_string())?;
    let mut reader = Reader::new(&bytes);
    let value = read_struct(&mut reader).map_err(|error| error.to_string())?;
    if reader.remaining() != 0 {
        return Err("trailing domain audit ThriftRW bytes".into());
    }
    DescribeDomainResponse::from_thrift(&value).map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use cadence_api::types::{DomainConfiguration, DomainInfo, DomainReplicationConfiguration};

    fn sample(active: &str, version: i64) -> DescribeDomainResponse {
        DescribeDomainResponse {
            domain_info: Some(DomainInfo {
                name: "audit-domain".into(),
                ..Default::default()
            }),
            configuration: Some(DomainConfiguration::default()),
            replication_configuration: Some(DomainReplicationConfiguration {
                active_cluster_name: active.into(),
                ..Default::default()
            }),
            failover_version: version,
            is_global_domain: true,
        }
    }

    #[test]
    fn domain_state_round_trips_for_every_encoding() {
        let cases = [
            (BlobEncoding::ThriftRW, "thriftrw"),
            (BlobEncoding::ThriftRWSnappy, "thriftrw_snappy"),
        ];
        for (encoding, name) in cases {
            for state in [DescribeDomainResponse::default(), sample("cluster-a", 12)] {
                let blob = encode_domain_state_with(&state, encoding).unwrap();
                assert_eq!(blob.encoding, name);
                assert_eq!(decode_domain_state(&blob).unwrap(), state, "{name}");
            }
        }
    }

    #[test]
    fn default_encoding_is_snappy_thriftrw() {
        let blob = encode_domain_state(&sample("cluster-a", 3)).unwrap();
        assert_eq!(blob.encoding, "thriftrw_snappy");
        assert_eq!(decode_domain_state(&blob).unwrap(), sample("cluster-a", 3));
    }

    #[test]
    fn rejects_unknown_encoding_and_trailing_bytes() {
        let unknown = GoDomainBlob {
            data: vec![],
            encoding: "json".into(),
        };
        assert!(decode_domain_state(&unknown).is_err());

        let mut bytes = Writer::new();
        write_struct(&mut bytes, &sample("cluster-a", 1).to_thrift());
        let mut data = bytes.into_bytes();
        data.push(0);
        let trailing = GoDomainBlob {
            data,
            encoding: "thriftrw".into(),
        };
        assert!(decode_domain_state(&trailing).is_err());
    }

    #[test]
    fn sqlite_audit_rows_page_newest_first_and_decode() {
        use crate::matching_domain_metadata::{DomainMetadataPersistence, MatchingDomainMetadataStore};
        let connection = rusqlite::Connection::open_in_memory().unwrap();
        connection
            .execute_batch(
                "CREATE TABLE domain_audit_log (
                    domain_id VARCHAR(255) NOT NULL, event_id VARCHAR(255) NOT NULL,
                    state_before BLOB NOT NULL, state_before_encoding VARCHAR(16) NOT NULL,
                    state_after BLOB NOT NULL, state_after_encoding VARCHAR(16) NOT NULL,
                    operation_type INT NOT NULL, created_time DATETIME NOT NULL,
                    last_updated_time DATETIME NOT NULL, identity VARCHAR(255) NOT NULL,
                    identity_type VARCHAR(255) NOT NULL, comment VARCHAR(255) NOT NULL DEFAULT '',
                    PRIMARY KEY (domain_id, operation_type, created_time, event_id));",
            )
            .unwrap();
        let store = MatchingDomainMetadataStore::from_connection(connection, "cluster-a");
        assert!(store.supports_domain_audit_log());

        let domain_id = "0190a000-0000-7000-8000-000000000001";
        let state = encode_domain_state(&sample("cluster-a", 1)).unwrap();
        let row = |nanos: i64, operation_type: i32| DomainAuditLog {
            event_id: new_event_id(nanos),
            domain_id: domain_id.into(),
            operation_type,
            created_time_nanos: nanos,
            last_updated_time_nanos: nanos,
            identity: String::new(),
            identity_type: String::new(),
            comment: format!("at {nanos}"),
            state_before: state.clone(),
            state_after: state.clone(),
        };
        for nanos in [1_000_000_000_i64, 2_000_000_000, 3_000_000_000] {
            store.create_domain_audit_log(&row(nanos, 5)).unwrap();
        }
        // Other operation types are excluded by the operation-type filter.
        store.create_domain_audit_log(&row(4_000_000_000, 2)).unwrap();

        let first = store
            .get_domain_audit_logs(domain_id, DOMAIN_AUDIT_OPERATION_TYPE_FAILOVER, 2, None)
            .unwrap();
        let created: Vec<i64> = first.logs.iter().map(|l| l.created_time_nanos).collect();
        assert_eq!(created, vec![3_000_000_000, 2_000_000_000]);
        let token = first.next_page_token.clone().expect("full page has a token");

        let second = store
            .get_domain_audit_logs(
                domain_id,
                DOMAIN_AUDIT_OPERATION_TYPE_FAILOVER,
                2,
                Some(&token),
            )
            .unwrap();
        assert_eq!(second.logs.len(), 1);
        assert_eq!(second.logs[0].created_time_nanos, 1_000_000_000);
        assert_eq!(second.next_page_token, None);
        assert_eq!(
            decode_domain_state(&second.logs[0].state_after).unwrap(),
            sample("cluster-a", 1)
        );
    }

    #[test]
    fn failover_operation_matches_go_enum_value() {
        // Go: Invalid=0, Create=1, Update=2, Deprecate=3, Delete=4, Failover=5.
        assert_eq!(DOMAIN_AUDIT_OPERATION_TYPE_FAILOVER, 5);
    }

    #[test]
    fn page_token_round_trips_and_rejects_garbage() {
        let token = encode_page_token(1_700_000_000_123_456_789, "0190-abc");
        assert_eq!(
            decode_page_token(&token).unwrap(),
            (1_700_000_000_123_456_789, "0190-abc".to_string())
        );
        assert!(decode_page_token(b"not-a-token").is_err());
        assert!(decode_page_token(b"x:y").is_err());
    }

    #[test]
    fn event_ids_are_uuid_v7_with_millisecond_prefix() {
        let now_nanos = 1_700_000_000_123_000_000;
        let id = uuid::Uuid::parse_str(&new_event_id(now_nanos)).unwrap();
        assert_eq!(id.get_version_num(), 7);
        assert_eq!(id.get_variant(), uuid::Variant::RFC4122);
        let prefix = &id.as_bytes()[..6];
        let mut millis = [0u8; 8];
        millis[2..].copy_from_slice(prefix);
        assert_eq!(u64::from_be_bytes(millis), 1_700_000_000_123);
    }

    #[test]
    fn sqlite_datetime_is_fixed_width_and_round_trips() {
        let cases = [
            (0_i64, "1970-01-01T00:00:00.000000000Z"),
            (1_700_000_000_000_000_001, "2023-11-14T22:13:20.000000001Z"),
        ];
        for (nanos, text) in cases {
            assert_eq!(format_sqlite_datetime(nanos).unwrap(), text);
            assert_eq!(parse_sqlite_datetime(text).unwrap(), nanos);
        }
        assert_eq!(
            format_sqlite_datetime(1_500_000_000).unwrap().len(),
            "1970-01-01T00:00:01.500000000Z".len()
        );
    }
}
