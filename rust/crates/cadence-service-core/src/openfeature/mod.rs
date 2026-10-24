//! OpenFeature dynamic-config support, mirroring Go's
//! `common/dynamicconfig/openfeatureclient` and
//! `common/dynamicconfig/openfeatureprovider`.
//!
//! The client semantics, provider contract, process-wide registration,
//! Unleash evaluator, and Matching runtime reader adapter are implemented.

pub mod client;
pub mod provider;
pub mod unleash;
pub mod runtime;
pub mod unleash_engine;
