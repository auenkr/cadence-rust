//! Rust Frontend service entry point and handler skeleton.
//!
//! The public transport/API registration currently reuses the shared server
//! runtime used by the Rust Matching binary. Frontend workflow behavior is
//! split by Go's original API domains so implementation can proceed module by
//! module without changing the wire surface.

pub mod config;
pub mod handler_admin;
pub mod handler_domain;
pub mod handler_schedule;
pub mod handler_workflow;
pub mod handler_workflow_async;
pub mod handler_workflow_history;
pub mod history_client;
pub mod lifecycle;
pub mod matching_client;
pub mod request_validator;
pub mod wrappers;

/// Service name as used by `--services` and the Go service registry.
pub const SERVICE_NAME: &str = "frontend";

/// Run the Frontend process with the shared Cadence CLI/config/listener
/// lifecycle. The shared runtime owns public gRPC, TChannel/Thrift, optional
/// HTTP and health endpoints on the addresses from the Go service config.
pub async fn run() -> anyhow::Result<()> {
    cadence_matching::launcher::run_frontend(
        "cadence-frontend",
        Some(&[cadence_service_core::service::ServiceName::Frontend]),
    )
    .await
}
