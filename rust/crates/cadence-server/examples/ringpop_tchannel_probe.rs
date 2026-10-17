//! Test-only JSON wire probe for the Go TChannel interoperability test.
//! This is not a production Ringpop responder or SWIM implementation.

use cadence_common::ringpop_wire::{JoinRequest, JoinResponse, Ping, PingRequest, PingResponse};
use cadence_server::ringpop_tchannel::{RingpopError, RingpopHandler, RingpopResponse};
use cadence_server::tchannel;
use std::net::SocketAddr;

fn parse_addr() -> SocketAddr {
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == "--addr" {
            if let Some(v) = args.next() {
                return v.parse().expect("bad addr");
            }
        }
    }
    std::env::var("RINGPOP_TCHANNEL_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:0".to_string())
        .parse()
        .expect("bad addr")
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let addr = parse_addr();

    let handler = RingpopHandler::new(|method, headers, body| async move {
        match method.as_str() {
            "/protocol/header-probe" => {
                let body = serde_json::to_vec(&headers)
                    .map_err(|e| RingpopError::System(format!("JSON encode failed: {e}")))?;
                Ok(RingpopResponse { headers, body })
            }
            "/protocol/slow-probe" => {
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                Ok(RingpopResponse {
                    headers,
                    body: b"{}".to_vec(),
                })
            }
            "/protocol/join" => {
                let req: JoinRequest = serde_json::from_slice(&body).map_err(|e| {
                    RingpopError::System(format!("Malformed TChannel JSON body: {e}"))
                })?;
                let resp = JoinResponse {
                    app: req.app,
                    coordinator: "127.0.0.1:7934".to_string(),
                    membership: Some(vec![]),
                    membership_checksum: 0,
                };
                let body = serde_json::to_vec(&resp)
                    .map_err(|e| RingpopError::System(format!("JSON encode failed: {e}")))?;
                Ok(RingpopResponse {
                    headers: None,
                    body,
                })
            }
            "/protocol/ping" => {
                let req: Ping = serde_json::from_slice(&body).map_err(|e| {
                    RingpopError::System(format!("Malformed TChannel JSON body: {e}"))
                })?;
                if req.app.is_empty() {
                    return Err(RingpopError::Application(
                        "Pinged ringpop requires app name".to_string(),
                    ));
                }
                if req.app != "cadence" {
                    return Err(RingpopError::Application(
                        "Pinged ringpop has a different app name".to_string(),
                    ));
                }
                let body = serde_json::to_vec(&req)
                    .map_err(|e| RingpopError::System(format!("JSON encode failed: {e}")))?;
                Ok(RingpopResponse {
                    headers: None,
                    body,
                })
            }
            "/protocol/ping-req" => {
                let req: PingRequest = serde_json::from_slice(&body).map_err(|e| {
                    RingpopError::System(format!("Malformed TChannel JSON body: {e}"))
                })?;
                let resp = PingResponse {
                    ping_status: true,
                    target: req.target,
                    changes: req.changes,
                };
                let body = serde_json::to_vec(&resp)
                    .map_err(|e| RingpopError::System(format!("JSON encode failed: {e}")))?;
                Ok(RingpopResponse {
                    headers: None,
                    body,
                })
            }
            _ => Err(RingpopError::System("Unknown TChannel method".to_string())),
        }
    });

    println!("listening on {addr}");
    tchannel::serve_ringpop(addr, handler).await
}
