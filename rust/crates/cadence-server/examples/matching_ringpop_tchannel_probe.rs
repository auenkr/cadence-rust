//! Test-only TChannel listener proving Matching Thrift and Ringpop JSON share
//! the public production listener API.
use cadence_common::ringpop_wire::{JoinRequest, JoinResponse, Ping, PingRequest, PingResponse};
use cadence_server::{
    grpc_matching::Matching,
    handlers::AppState,
    ringpop_tchannel::{RingpopError, RingpopHandler, RingpopResponse},
    tchannel,
};
use std::net::SocketAddr;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let addr: SocketAddr = std::env::args()
        .skip_while(|arg| arg != "--addr")
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:0".to_string())
        .parse()?;
    let ringpop = RingpopHandler::new(|method, headers, body| async move {
        match method.as_str() {
            "/protocol/header-probe" => {
                let body = serde_json::to_vec(&headers)
                    .map_err(|error| RingpopError::System(error.to_string()))?;
                Ok(RingpopResponse { headers, body })
            }
            "/protocol/join" => {
                let request: JoinRequest = serde_json::from_slice(&body)
                    .map_err(|error| RingpopError::System(error.to_string()))?;
                let response = JoinResponse {
                    app: request.app,
                    coordinator: "127.0.0.1:7934".into(),
                    membership: Some(vec![]),
                    membership_checksum: 0,
                };
                Ok(RingpopResponse {
                    headers: None,
                    body: serde_json::to_vec(&response)
                        .map_err(|error| RingpopError::System(error.to_string()))?,
                })
            }
            "/protocol/ping" => {
                let request: Ping = serde_json::from_slice(&body)
                    .map_err(|error| RingpopError::System(error.to_string()))?;
                Ok(RingpopResponse {
                    headers: None,
                    body: serde_json::to_vec(&request)
                        .map_err(|error| RingpopError::System(error.to_string()))?,
                })
            }
            "/protocol/ping-req" => {
                let request: PingRequest = serde_json::from_slice(&body)
                    .map_err(|error| RingpopError::System(error.to_string()))?;
                let response = PingResponse {
                    ping_status: true,
                    target: request.target,
                    changes: request.changes,
                };
                Ok(RingpopResponse {
                    headers: None,
                    body: serde_json::to_vec(&response)
                        .map_err(|error| RingpopError::System(error.to_string()))?,
                })
            }
            _ => Err(RingpopError::System("unknown Ringpop method".into())),
        }
    });
    let matching = Matching::new(AppState::default());
    println!("listening on {addr}");
    tchannel::serve_matching_with_ringpop(addr, matching, ringpop).await
}
