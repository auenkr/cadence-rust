//! Stable public API types for the ScyllaDB Rust driver.
//!
//! This crate contains CQL data types and traits that form part of the stable
//! public API of the `scylla` driver crate. It is re-exported by both `scylla`
//! and `scylla-cql`, ensuring type identity is preserved across versions.

pub use scylla_macros::DeserializeRow;
pub use scylla_macros::DeserializeValue;
pub use scylla_macros::SerializeRow;
pub use scylla_macros::SerializeValue;

pub mod deserialize;
pub mod frame;
pub(crate) mod pretty;
pub mod utils;

pub mod serialize;
pub mod value;

/// Protocol-version-aware CQL collection value codec.
///
/// The driver defaults to its existing v4 collection implementation. Callers
/// implementing protocol v2/v3 must use this module at the collection value
/// boundary for both encoding and decoding.
pub mod protocol_collections;

#[doc(hidden)]
pub mod _macro_internal;

#[cfg(test)]
mod macros_tests;

#[cfg(doctest)]
mod macros_doctests;
