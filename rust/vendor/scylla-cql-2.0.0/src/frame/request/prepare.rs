//! CQL protocol-level representation of a `PREPARE` request.

use crate::frame::frame_errors::CqlRequestSerializationError;

use crate::{
    frame::request::{RequestOpcode, SerializableRequest},
    frame::types,
    frame::versioned_header::CqlProtocolVersion,
};

// Re-export for backward compatibility.
pub use crate::frame::frame_errors::PrepareSerializationError;

/// CQL protocol-level representation of an `PREPARE` request,
/// used to prepare a single statement for further execution.
pub struct Prepare<'a> {
    /// CQL statement string to prepare.
    pub query: &'a str,
}

impl SerializableRequest for Prepare<'_> {
    const OPCODE: RequestOpcode = RequestOpcode::Prepare;

    fn serialize(&self, buf: &mut Vec<u8>) -> Result<(), CqlRequestSerializationError> {
        self.serialize_for_version(buf, CqlProtocolVersion::V4)
    }

    fn serialize_with_version(&self, buf: &mut Vec<u8>, version: CqlProtocolVersion) -> Result<(), CqlRequestSerializationError> {
        self.serialize_for_version(buf, version)
    }
}

impl Prepare<'_> {
    /// Serializes this PREPARE body. The CQL query string has the same representation in v2-v4.
    pub fn serialize_for_version(
        &self,
        buf: &mut Vec<u8>,
        _version: CqlProtocolVersion,
    ) -> Result<(), CqlRequestSerializationError> {
        types::write_long_string(self.query, buf)
            .map_err(PrepareSerializationError::StatementStringSerialization)?;
        Ok(())
    }
}
