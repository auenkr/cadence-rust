//! Go-compatible domain audit log rows (`domain_audit_log`). Go serializes each
//! state snapshot as a raw ThriftRW `DescribeDomainResponse` (no version byte),
//! optionally snappy-compressed, plus the `FailoverInfo` that carries the
//! failover expiry. Go's `ListFailoverHistory` reads rows written by Rust and
//! the reverse, so both sides must agree on these bytes.

use crate::matching_blob::BlobEncoding;
use crate::matching_domain_metadata::GoDomainBlob;
use cadence_api::enums::FailoverType;
use cadence_api::service_types::DescribeDomainResponse;
use cadence_api::types::{
    ActiveClusterInfo, ClusterAttribute, ClusterFailover, DomainReplicationConfiguration,
    FailoverEvent,
};
use cadence_thrift::{
    protocol::{Reader, Writer},
    value::{read_struct, write_struct, Struct, Value},
    FromThrift, ToThrift,
};

/// Go `DomainAuditOperationTypeFailover`, the `operation_type` column value
/// that `ListFailoverHistory` filters on.
pub const DOMAIN_AUDIT_OPERATION_TYPE_FAILOVER: i32 = 5;

/// Thrift field of `DescribeDomainResponse.failoverInfo` and its
/// `failoverExpireTimestamp`, from `shared.thrift`.
const FAILOVER_INFO_FIELD: i16 = 60;
const FAILOVER_EXPIRE_FIELD: i16 = 30;
const FAILOVER_INFO_VERSION_FIELD: i16 = 10;

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

/// One domain snapshot as Go's `GetDomainResponse` stores it: the public
/// describe view plus the failover expiry Go keeps in `FailoverInfo`. A `Some`
/// expiry marks a graceful failover (Go's `FailoverEndTime != nil`).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct DomainAuditState {
    pub describe: DescribeDomainResponse,
    pub failover_expire_nanos: Option<i64>,
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

/// Encode a snapshot the way Go's `serializeGetDomainResponse` does for the
/// audit log (ThriftRW + snappy).
pub fn encode_domain_state(state: &DomainAuditState) -> Result<GoDomainBlob, String> {
    encode_domain_state_with(state, BlobEncoding::ThriftRWSnappy)
}

fn encode_domain_state_with(
    state: &DomainAuditState,
    encoding: BlobEncoding,
) -> Result<GoDomainBlob, String> {
    let mut value = state.describe.to_thrift();
    if let Some(expire) = state.failover_expire_nanos {
        let info = Struct {
            fields: vec![
                (
                    FAILOVER_INFO_VERSION_FIELD,
                    Value::I64(state.describe.failover_version),
                ),
                (FAILOVER_EXPIRE_FIELD, Value::I64(expire)),
            ],
        };
        value
            .fields
            .push((FAILOVER_INFO_FIELD, Value::Struct(info)));
    }
    let mut writer = Writer::new();
    write_struct(&mut writer, &value);
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
pub fn decode_domain_state(blob: &GoDomainBlob) -> Result<DomainAuditState, String> {
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
    let describe = DescribeDomainResponse::from_thrift(&value).map_err(|error| error.to_string())?;
    let failover_expire_nanos = match value
        .fields
        .iter()
        .find(|(id, _)| *id == FAILOVER_INFO_FIELD)
    {
        Some((_, Value::Struct(info))) => info
            .opt_i64(FAILOVER_EXPIRE_FIELD)
            .map_err(|error| error.to_string())?,
        _ => None,
    };
    Ok(DomainAuditState {
        describe,
        failover_expire_nanos,
    })
}

/// Go `ToFailoverEvents` for one audit row. Returns `None` when neither the
/// active cluster nor any cluster attribute changed, matching Go's nil filter.
pub fn failover_event(log: &DomainAuditLog) -> Result<Option<FailoverEvent>, String> {
    let before = decode_domain_state(&log.state_before)?;
    let after = decode_domain_state(&log.state_after)?;
    let before_rep: DomainReplicationConfiguration = before
        .describe
        .replication_configuration
        .clone()
        .unwrap_or_default();
    let after_rep: DomainReplicationConfiguration = after
        .describe
        .replication_configuration
        .clone()
        .unwrap_or_default();

    let mut cluster_failovers = Vec::new();
    if before_rep.active_cluster_name != after_rep.active_cluster_name {
        cluster_failovers.push(ClusterFailover {
            from_cluster: Some(ActiveClusterInfo {
                active_cluster_name: before_rep.active_cluster_name.clone(),
                failover_version: before.describe.failover_version,
            }),
            to_cluster: Some(ActiveClusterInfo {
                active_cluster_name: after_rep.active_cluster_name.clone(),
                failover_version: after.describe.failover_version,
            }),
            cluster_attribute: None,
        });
    }

    // Per-attribute changes in sorted scope/name order, mirroring Go.
    let attrs_after = |scope: &str| {
        after_rep
            .active_clusters
            .as_ref()
            .and_then(|c| c.active_clusters_by_cluster_attribute.as_ref())
            .and_then(|m| m.get(scope))
            .and_then(|s| s.cluster_attributes.as_ref())
    };
    let attrs_before = |scope: &str| {
        before_rep
            .active_clusters
            .as_ref()
            .and_then(|c| c.active_clusters_by_cluster_attribute.as_ref())
            .and_then(|m| m.get(scope))
            .and_then(|s| s.cluster_attributes.as_ref())
    };
    let mut scopes: Vec<&String> = after_rep
        .active_clusters
        .as_ref()
        .and_then(|c| c.active_clusters_by_cluster_attribute.as_ref())
        .map(|m| m.keys().collect())
        .unwrap_or_default();
    scopes.sort();
    for scope in scopes {
        let after_attrs = attrs_after(scope);
        let before_attrs = attrs_before(scope);
        let mut names: Vec<&String> = after_attrs.map(|m| m.keys().collect()).unwrap_or_default();
        names.sort();
        for name in names {
            let after_info = after_attrs.and_then(|m| m.get(name));
            let before_info = before_attrs.and_then(|m| m.get(name));
            let changed = match (before_info, after_info) {
                (Some(b), Some(a)) => b.failover_version != a.failover_version,
                (None, Some(_)) => true,
                _ => false,
            };
            if !changed {
                continue;
            }
            cluster_failovers.push(ClusterFailover {
                from_cluster: before_info.cloned(),
                to_cluster: after_info.cloned(),
                cluster_attribute: Some(ClusterAttribute {
                    scope: scope.clone(),
                    name: name.clone(),
                }),
            });
        }
    }

    if cluster_failovers.is_empty() {
        return Ok(None);
    }
    let failover_type = if after.failover_expire_nanos.is_some() {
        FailoverType::Graceful
    } else {
        FailoverType::Force
    };
    Ok(Some(FailoverEvent {
        id: Some(log.event_id.clone()),
        created_time: Some(log.created_time_nanos),
        failover_type: Some(failover_type),
        cluster_failovers,
    }))
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

    fn state(active: &str, version: i64) -> DomainAuditState {
        DomainAuditState {
            describe: sample(active, version),
            failover_expire_nanos: None,
        }
    }

    #[test]
    fn domain_state_round_trips_for_every_encoding() {
        let cases = [
            (BlobEncoding::ThriftRW, "thriftrw"),
            (BlobEncoding::ThriftRWSnappy, "thriftrw_snappy"),
        ];
        for (encoding, name) in cases {
            let graceful = DomainAuditState {
                describe: sample("cluster-a", 12),
                failover_expire_nanos: Some(99),
            };
            for snapshot in [
                DomainAuditState::default(),
                state("cluster-a", 12),
                graceful,
            ] {
                let blob = encode_domain_state_with(&snapshot, encoding).unwrap();
                assert_eq!(blob.encoding, name);
                assert_eq!(decode_domain_state(&blob).unwrap(), snapshot, "{name}");
            }
        }
    }

    #[test]
    fn default_encoding_is_snappy_thriftrw() {
        let blob = encode_domain_state(&state("cluster-a", 3)).unwrap();
        assert_eq!(blob.encoding, "thriftrw_snappy");
        assert_eq!(decode_domain_state(&blob).unwrap(), state("cluster-a", 3));
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

    fn audit_row(event: &str, nanos: i64, before: &DomainAuditState, after: &DomainAuditState) -> DomainAuditLog {
        DomainAuditLog {
            event_id: event.into(),
            domain_id: "domain-1".into(),
            operation_type: DOMAIN_AUDIT_OPERATION_TYPE_FAILOVER,
            created_time_nanos: nanos,
            last_updated_time_nanos: nanos,
            identity: String::new(),
            identity_type: String::new(),
            comment: String::new(),
            state_before: encode_domain_state(before).unwrap(),
            state_after: encode_domain_state(after).unwrap(),
        }
    }

    #[test]
    fn failover_event_reports_cluster_change_and_failover_type() {
        let before = state("cluster-a", 1);
        let mut after = state("cluster-b", 2);
        after.failover_expire_nanos = Some(500);
        let event = failover_event(&audit_row("e1", 42, &before, &after))
            .unwrap()
            .expect("active cluster changed");
        assert_eq!(event.failover_type, Some(FailoverType::Graceful));
        assert_eq!(event.created_time, Some(42));
        let change = &event.cluster_failovers[0];
        assert_eq!(change.from_cluster.as_ref().unwrap().active_cluster_name, "cluster-a");
        assert_eq!(change.from_cluster.as_ref().unwrap().failover_version, 1);
        assert_eq!(change.to_cluster.as_ref().unwrap().active_cluster_name, "cluster-b");
        assert_eq!(change.to_cluster.as_ref().unwrap().failover_version, 2);
    }

    #[test]
    fn failover_event_is_none_without_cluster_change() {
        let same = state("cluster-a", 1);
        assert_eq!(
            failover_event(&audit_row("e2", 1, &same, &same)).unwrap(),
            None
        );
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

        let snapshot = state("cluster-a", 1);
        let row = |nanos: i64, operation_type: i32| DomainAuditLog {
            operation_type,
            comment: format!("at {nanos}"),
            ..audit_row(&new_event_id(nanos), nanos, &snapshot, &snapshot)
        };
        for nanos in [1_000_000_000_i64, 2_000_000_000, 3_000_000_000] {
            store.create_domain_audit_log(&row(nanos, 5)).unwrap();
        }
        // Other operation types are excluded by the operation-type filter.
        store.create_domain_audit_log(&row(4_000_000_000, 2)).unwrap();

        let first = store
            .get_domain_audit_logs("domain-1", DOMAIN_AUDIT_OPERATION_TYPE_FAILOVER, 2, None)
            .unwrap();
        let created: Vec<i64> = first.logs.iter().map(|l| l.created_time_nanos).collect();
        assert_eq!(created, vec![3_000_000_000, 2_000_000_000]);
        let token = first.next_page_token.clone().expect("full page has a token");

        let second = store
            .get_domain_audit_logs("domain-1", DOMAIN_AUDIT_OPERATION_TYPE_FAILOVER, 2, Some(&token))
            .unwrap();
        assert_eq!(second.logs.len(), 1);
        assert_eq!(second.logs[0].created_time_nanos, 1_000_000_000);
        assert_eq!(second.next_page_token, None);
        assert_eq!(decode_domain_state(&second.logs[0].state_after).unwrap(), snapshot);
    }
}
