//! Thrift binary-protocol transport for the Cadence Rust port.
//!
//! Implements the Apache Thrift binary protocol (strict envelopes) spoken by
//! Cadence's YARPC handlers, with per-struct converters whose field IDs come
//! from the IDL. See `protocol.rs` (codec), `value.rs` (value model and
//! field builders), and `convert.rs` (struct converters).

pub mod convert;
pub mod protocol;
pub mod service;
pub mod value;

pub use protocol::{Error, MessageType, Reader, Result, TType, Writer};
pub use service::{MethodDef, find_method};
pub use value::{FromI32, FromThrift, FromValue, Struct, ToThrift, ToValue, Value};
