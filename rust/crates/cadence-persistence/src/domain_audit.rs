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
    fn failover_operation_matches_go_enum_value() {
        // Go: Invalid=0, Create=1, Update=2, Deprecate=3, Delete=4, Failover=5.
        assert_eq!(DOMAIN_AUDIT_OPERATION_TYPE_FAILOVER, 5);
    }
}
