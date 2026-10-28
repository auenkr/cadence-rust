//! Native live-routing attachment contracts for `NativeQueueRegistry`.
//!
//! `NativeQueueRegistry::with_live_routing` owns the attachment implementation
//! and is kept beside the registry's private weak-state machinery. This module
//! gives host assembly one stable import location for the required callback
//! types. The local callback signature is
//! `Fn(NativeQueueRegistry) -> Arc<dyn NativeMatchingApi>` and is invoked once
//! per local parent dispatch, after the registry state has been upgraded from
//! a `Weak`; it must build a service from that call's registry and must not
//! retain the registry in an owner resolver. `NativePartitionRefresher` must
//! resolve the owner of the raw partition name, build the generated native
//! refresh request, preserve `deadline`, and treat owner-resolution failures
//! as logged success while propagating actual RPC errors.
//!
//! This file is deliberately byte-oriented: callers route `GoString` bytes
//! directly through `Membership` or `MatchingShardRouter`. There is no UTF-8
//! conversion or replacement-character fallback.

pub use crate::matching_registry_native::{
    NativeLocalPollServiceBuilder, NativePartitionRefresher, NativeParentRouter,
};
