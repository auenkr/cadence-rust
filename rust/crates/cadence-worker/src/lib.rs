//! Cadence worker service. Not migrated to Rust yet: the crate and binary exist so
//! the workspace layout is ready and a Go worker can be mixed with Rust services.

/// Service name as used by `--services`.
pub const SERVICE_NAME: &str = "worker";
