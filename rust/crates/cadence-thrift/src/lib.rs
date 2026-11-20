//! Thrift binary-protocol transport for the Cadence Rust port.
//!
//! Implements the Apache Thrift binary protocol (strict envelopes) spoken by
//! Cadence's YARPC handlers, with per-struct converters whose field IDs come
//! from the IDL. See `protocol.rs` (codec), `value.rs` (value model and
//! field builders), and `convert.rs` (struct converters).

pub mod convert;
pub mod cross_cluster;
pub mod history_blob;
pub mod protocol;
pub mod service;
pub mod value;

pub use protocol::{Error, MessageType, Reader, Result, TType, Writer};
pub use service::{find_method, MethodDef};
pub use value::{FromI32, FromThrift, FromValue, Struct, ToThrift, ToValue, Value};

/// Go indexer.Message deletion, encoded with the ThriftRW version prefix.
pub fn encode_visibility_delete(
    domain_id: &str,
    workflow_id: &str,
    run_id: &str,
    version: i64,
) -> Vec<u8> {
    use value::{write_struct, Struct, Value};
    let mut writer = Writer::new();
    write_struct(
        &mut writer,
        &Struct {
            fields: vec![
                (10, Value::I32(1)),
                (20, Value::Bytes(domain_id.as_bytes().to_vec())),
                (30, Value::Bytes(workflow_id.as_bytes().to_vec())),
                (40, Value::Bytes(run_id.as_bytes().to_vec())),
                (50, Value::I64(version)),
            ],
        },
    );
    let mut bytes = vec![0x59];
    bytes.extend(writer.into_bytes());
    bytes
}
