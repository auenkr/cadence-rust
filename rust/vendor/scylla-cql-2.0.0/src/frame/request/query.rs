//! CQL protocol-level representation of a `QUERY` request.

use std::borrow::Cow;

use crate::frame::versioned_header::CqlProtocolVersion;
use crate::frame::{frame_errors::CqlRequestSerializationError, types::SerialConsistency};
use crate::serialize::row::SerializedValues;
use bytes::{Buf, BufMut};

use crate::{
    frame::request::{RequestOpcode, SerializableRequest},
    frame::types,
};

use super::{DeserializableRequest, RequestDeserializationError};

// Re-export for backward compatibility.
pub use crate::frame::frame_errors::{QueryParametersSerializationError, QuerySerializationError};

// Query flags
const FLAG_VALUES: u8 = 0x01;
const FLAG_SKIP_METADATA: u8 = 0x02;
const FLAG_PAGE_SIZE: u8 = 0x04;
const FLAG_WITH_PAGING_STATE: u8 = 0x08;
const FLAG_WITH_SERIAL_CONSISTENCY: u8 = 0x10;
const FLAG_WITH_DEFAULT_TIMESTAMP: u8 = 0x20;
const FLAG_WITH_NAMES_FOR_VALUES: u8 = 0x40;
const ALL_FLAGS: u8 = FLAG_VALUES
    | FLAG_SKIP_METADATA
    | FLAG_PAGE_SIZE
    | FLAG_WITH_PAGING_STATE
    | FLAG_WITH_SERIAL_CONSISTENCY
    | FLAG_WITH_DEFAULT_TIMESTAMP
    | FLAG_WITH_NAMES_FOR_VALUES;

/// CQL protocol-level representation of an `QUERY` request,
/// used to execute a single unprepared statement.
#[cfg_attr(test, derive(Debug, PartialEq, Eq))]
pub struct Query<'q> {
    /// CQL statement string to execute.
    pub contents: Cow<'q, str>,

    /// Various parameters controlling the execution of the statement.
    pub parameters: QueryParameters<'q>,
}

impl SerializableRequest for Query<'_> {
    const OPCODE: RequestOpcode = RequestOpcode::Query;

    fn serialize(&self, buf: &mut Vec<u8>) -> Result<(), CqlRequestSerializationError> {
        self.serialize_for_version(buf, CqlProtocolVersion::V4)
    }

    fn serialize_with_version(
        &self,
        buf: &mut Vec<u8>,
        version: CqlProtocolVersion,
    ) -> Result<(), CqlRequestSerializationError> {
        self.serialize_for_version(buf, version)
    }
}

impl Query<'_> {
    /// Serializes this QUERY body using the selected native protocol version.
    /// Existing [`SerializableRequest`] callers retain v4 behavior.
    pub fn serialize_for_version(
        &self,
        buf: &mut Vec<u8>,
        version: CqlProtocolVersion,
    ) -> Result<(), CqlRequestSerializationError> {
        types::write_long_string(&self.contents, buf)
            .map_err(QuerySerializationError::StatementStringSerialization)?;
        self.parameters
            .serialize_for_version(buf, version)
            .map_err(QuerySerializationError::QueryParametersSerialization)?;
        Ok(())
    }
}

impl DeserializableRequest for Query<'_> {
    fn deserialize(buf: &mut &[u8]) -> Result<Self, RequestDeserializationError> {
        Self::deserialize_for_version(buf, CqlProtocolVersion::V4)
    }
}

impl Query<'_> {
    /// Deserializes a QUERY body using its native protocol version.
    pub fn deserialize_for_version(
        buf: &mut &[u8],
        version: CqlProtocolVersion,
    ) -> Result<Query<'static>, RequestDeserializationError> {
        let contents = Cow::Owned(types::read_long_string(buf)?.to_owned());
        let parameters = QueryParameters::deserialize_for_version(buf, version)?;

        Ok(Query {
            contents,
            parameters,
        })
    }
}

/// Various parameters controlling the execution of the statement.
#[cfg_attr(test, derive(Debug, PartialEq, Eq, Clone))]
pub struct QueryParameters<'a> {
    /// Consistency level for the query.
    pub consistency: types::Consistency,

    /// Serial consistency level for the query, if specified.
    pub serial_consistency: Option<types::SerialConsistency>,

    /// Client-side-assigned timestamp for the query, if specified.
    pub timestamp: Option<i64>,

    /// Maximum number of rows to return for the query, if specified.
    /// If not specified, the server will not page the result, and instead return all rows
    /// in a single response. This is not recommended for large result sets, as it puts
    /// a lot of pressure on the server and network, as well as brings high memory usage
    /// on both client and server sides.
    pub page_size: Option<i32>,

    /// Paging state for the query, used to resume fetching results from a previous query.
    /// If empty paging state is provided, the query will start from the beginning.
    pub paging_state: PagingState,

    /// Whether to skip metadata for the values in the result set.
    /// This is an optimisation that can be used when the client does not need
    /// the metadata for the values, because it has already been fetched upon
    /// preparing the statement.
    pub skip_metadata: bool,

    /// Values bound to the statements.
    pub values: Cow<'a, SerializedValues>,
}

impl Default for QueryParameters<'_> {
    fn default() -> Self {
        Self {
            consistency: Default::default(),
            serial_consistency: None,
            timestamp: None,
            page_size: None,
            paging_state: PagingState::start(),
            skip_metadata: false,
            values: Cow::Borrowed(SerializedValues::EMPTY),
        }
    }
}

impl QueryParameters<'_> {
    /// Serializes the parameters into the provided buffer.
    pub fn serialize(
        &self,
        buf: &mut impl BufMut,
    ) -> Result<(), QueryParametersSerializationError> {
        self.serialize_for_version(buf, CqlProtocolVersion::V4)
    }

    /// Serializes query parameters using the selected native protocol version.
    /// V2 supports skip-metadata; v3 adds timestamps and named values (named values remain
    /// unsupported by this Rust representation).
    pub fn serialize_for_version(
        &self,
        buf: &mut impl BufMut,
        version: CqlProtocolVersion,
    ) -> Result<(), QueryParametersSerializationError> {
        types::write_consistency(self.consistency, buf);

        let paging_state_bytes = self.paging_state.as_bytes_slice();

        let mut flags = 0;
        if !self.values.is_empty() {
            flags |= FLAG_VALUES;
        }

        if self.skip_metadata {
            flags |= FLAG_SKIP_METADATA;
        }

        if self.page_size.is_some() {
            flags |= FLAG_PAGE_SIZE;
        }

        if paging_state_bytes.is_some() {
            flags |= FLAG_WITH_PAGING_STATE;
        }

        if self.serial_consistency.is_some() {
            flags |= FLAG_WITH_SERIAL_CONSISTENCY;
        }

        if self.timestamp.is_some() && version != CqlProtocolVersion::V2 {
            flags |= FLAG_WITH_DEFAULT_TIMESTAMP;
        }

        buf.put_u8(flags);

        if !self.values.is_empty() {
            self.values.write_to_request(buf);
        }

        if let Some(page_size) = self.page_size {
            types::write_int(page_size, buf);
        }

        if let Some(paging_state_bytes) = paging_state_bytes {
            types::write_bytes(paging_state_bytes, buf)?;
        }

        if let Some(serial_consistency) = self.serial_consistency {
            types::write_serial_consistency(serial_consistency, buf);
        }

        if let Some(timestamp) = self.timestamp.filter(|_| version != CqlProtocolVersion::V2) {
            types::write_long(timestamp, buf);
        }

        Ok(())
    }
}

impl QueryParameters<'_> {
    /// Deserializes the parameters from the provided buffer.
    pub fn deserialize(buf: &mut &[u8]) -> Result<Self, RequestDeserializationError> {
        Self::deserialize_for_version(buf, CqlProtocolVersion::V4)
    }

    /// Deserializes query parameters from a body using its native protocol version.
    pub fn deserialize_for_version(
        buf: &mut &[u8],
        version: CqlProtocolVersion,
    ) -> Result<Self, RequestDeserializationError> {
        let consistency = types::read_consistency(buf)?;

        let flags = buf.get_u8();
        let supported_flags = match version {
            CqlProtocolVersion::V2 => {
                ALL_FLAGS & !(FLAG_WITH_DEFAULT_TIMESTAMP | FLAG_WITH_NAMES_FOR_VALUES)
            }
            CqlProtocolVersion::V3 | CqlProtocolVersion::V4 => ALL_FLAGS,
        };
        let unknown_flags = flags & !supported_flags;
        if unknown_flags != 0 {
            return Err(RequestDeserializationError::UnknownFlags {
                flags: unknown_flags,
            });
        }
        let values_flag = (flags & FLAG_VALUES) != 0;
        let skip_metadata = (flags & FLAG_SKIP_METADATA) != 0;
        let page_size_flag = (flags & FLAG_PAGE_SIZE) != 0;
        let paging_state_flag = (flags & FLAG_WITH_PAGING_STATE) != 0;
        let serial_consistency_flag = (flags & FLAG_WITH_SERIAL_CONSISTENCY) != 0;
        let default_timestamp_flag =
            version != CqlProtocolVersion::V2 && (flags & FLAG_WITH_DEFAULT_TIMESTAMP) != 0;
        let values_have_names_flag =
            version != CqlProtocolVersion::V2 && (flags & FLAG_WITH_NAMES_FOR_VALUES) != 0;

        if values_have_names_flag {
            return Err(RequestDeserializationError::NamedValuesUnsupported);
        }

        let values = Cow::Owned(if values_flag {
            SerializedValues::new_from_frame(buf)?
        } else {
            SerializedValues::new()
        });

        let page_size = page_size_flag.then(|| types::read_int(buf)).transpose()?;
        let paging_state = if paging_state_flag {
            PagingState::new_from_raw_bytes(types::read_bytes(buf)?)
        } else {
            PagingState::start()
        };
        let serial_consistency = serial_consistency_flag
            .then(|| types::read_consistency(buf))
            .transpose()?
            .map(
                |consistency| match SerialConsistency::try_from(consistency) {
                    Ok(serial_consistency) => Ok(serial_consistency),
                    Err(_) => Err(RequestDeserializationError::ExpectedSerialConsistency(
                        consistency,
                    )),
                },
            )
            .transpose()?;
        let timestamp = if default_timestamp_flag {
            Some(types::read_long(buf)?)
        } else {
            None
        };

        Ok(Self {
            consistency,
            serial_consistency,
            timestamp,
            page_size,
            paging_state,
            skip_metadata,
            values,
        })
    }
}

// Re-export from scylla-cql-core for backward compatibility.
pub use scylla_cql_core::frame::request::query::{PagingState, PagingStateResponse};

#[cfg(test)]
mod version_tests {
    use super::*;

    #[test]
    fn empty_query_parameters_have_same_wire_fixture_for_v2_v3_v4() {
        // CQL consistency LOCAL_QUORUM (0x0006), then a zero query-flags byte.
        let expected = [0x00, 0x06, 0x00];
        for version in [
            CqlProtocolVersion::V2,
            CqlProtocolVersion::V3,
            CqlProtocolVersion::V4,
        ] {
            let mut body = Vec::new();
            QueryParameters::default()
                .serialize_for_version(&mut body, version)
                .unwrap();
            assert_eq!(body, expected, "protocol {version:?}");
            let decoded =
                QueryParameters::deserialize_for_version(&mut &body[..], version).unwrap();
            assert_eq!(decoded.consistency, types::Consistency::LocalQuorum);
        }
    }

    #[test]
    fn v2_rejects_v3_query_parameter_flags() {
        // Consistency LOCAL_QUORUM and the v3+ default-timestamp flag.
        let body = [0x00, 0x06, FLAG_WITH_DEFAULT_TIMESTAMP];
        assert!(
            QueryParameters::deserialize_for_version(&mut &body[..], CqlProtocolVersion::V2,)
                .is_err()
        );
    }

    #[test]
    fn query_flags_follow_gocql_v2_and_v3_gates() {
        // Pinned gocql frame.go: skip metadata is v2+, timestamp is v3+.
        let mut params = QueryParameters {
            skip_metadata: true,
            ..Default::default()
        };
        let mut body = Vec::new();
        params
            .serialize_for_version(&mut body, CqlProtocolVersion::V2)
            .unwrap();
        assert_eq!(body, [0x00, 0x06, FLAG_SKIP_METADATA]);

        params.timestamp = Some(1);
        body.clear();
        params
            .serialize_for_version(&mut body, CqlProtocolVersion::V2)
            .unwrap();
        assert_eq!(body, [0x00, 0x06, FLAG_SKIP_METADATA]);

        body.clear();
        params
            .serialize_for_version(&mut body, CqlProtocolVersion::V3)
            .unwrap();
        assert_eq!(
            body,
            [
                0x00,
                0x06,
                FLAG_SKIP_METADATA | FLAG_WITH_DEFAULT_TIMESTAMP,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                1
            ]
        );
    }
}
