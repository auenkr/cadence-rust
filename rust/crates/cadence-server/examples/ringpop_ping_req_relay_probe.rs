//! Test-only real Rust PING-REQ relay for the pinned Go interoperability test.
use cadence_common::ringpop_wire::{Member, ALIVE};
use cadence_server::{ringpop_provider::RingpopNode, tchannel};
use std::{collections::BTreeMap, time::Duration};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let addr: std::net::SocketAddr = std::env::args().nth(1).expect("listen address").parse()?;
    let timeout = std::env::var("RINGPOP_RELAY_PING_TIMEOUT_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(1500);
    let node = RingpopNode::new_with_ping_timeout(
        "cadence",
        Member {
            address: addr.to_string(),
            status: ALIVE.into(),
            incarnation: 100,
            labels: Some(BTreeMap::from([("serviceName".into(), "matching".into())])),
        },
        true,
        Duration::from_millis(timeout),
    );
    node.set_ready(true);
    if let Ok(path) = std::env::var("RINGPOP_RELAY_STATE_FILE") {
        let counted = node.clone();
        tokio::spawn(async move {
            loop {
                let counts = counted.request_counts();
                let (members, checksum) = counted.membership_snapshot();
                let state = serde_json::json!({
                    "ping_req": counts.ping_req,
                    "checksum": checksum,
                    "members": members.iter().map(|member| member.address.as_str()).collect::<Vec<_>>(),
                    "dissemination": counted.dissemination_counts(),
                });
                let _ = std::fs::write(path.as_str(), format!("{state}\n"));
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        });
    }
    tchannel::serve_ringpop(addr, node.handler()).await?;
    Ok(())
}
