//! OpenFeature dynamic-config support, mirroring Go's
//! `common/dynamicconfig/openfeatureclient` and
//! `common/dynamicconfig/openfeatureprovider`.
//!
//! Status: the client semantics, provider contract, process-wide registration
//! and the unleash configuration step are ported. The unleash evaluation
//! engine and the Matching reader wiring are not; see `unleash.rs` and the
//! notes in `memory/evidence/matching-openfeature-20261024/`.

pub mod client;
pub mod provider;
pub mod unleash;
pub mod runtime;
