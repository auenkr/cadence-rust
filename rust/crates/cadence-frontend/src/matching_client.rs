//! Outbound client construction for the original Go Matching service.
use std::time::Duration;

/// Build a lazy gRPC channel using the configured Go
/// `services.matching.rpc.grpcPort` address.
pub fn connect(host: &str, port: u16) -> anyhow::Result<tonic::transport::Channel> {
    let endpoint = format!("http://{host}:{port}")
        .parse::<tonic::transport::Endpoint>()?
        .connect_timeout(Duration::from_secs(10));
    Ok(endpoint.connect_lazy())
}
