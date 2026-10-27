//! Tonic Matching service and client using lossless native message types.
//!
//! The ordinary `crate::matching` generated service remains the public UTF-8 API.
#[allow(clippy::all, deprecated, unused_imports)]
mod generated {
    include!(concat!(
        env!("OUT_DIR"),
        "/native-service/uber.cadence.matching.v1.rs"
    ));
}
pub use generated::*;

#[allow(clippy::all, deprecated, unused_imports)]
mod bridge {
    include!(concat!(env!("OUT_DIR"), "/native-service/native_bridge.rs"));
}
