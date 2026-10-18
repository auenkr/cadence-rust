//! Test-only server for exercising the real RingpopNode handler over TChannel.
use cadence_common::ringpop_wire::{Member, ALIVE};
use cadence_server::{ringpop_provider::RingpopNode, tchannel};
use std::collections::BTreeMap;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let addr: std::net::SocketAddr = std::env::args().nth(1).expect("listen address").parse()?;
    let node = RingpopNode::new(
        "cadence",
        Member {
            address: "127.0.0.1:7934".into(),
            status: ALIVE.into(),
            incarnation: 100,
            labels: Some(BTreeMap::from([("serviceName".into(), "matching".into())])),
        },
        true,
    );
    let ready_node = node.clone();
    tokio::spawn(async move {
        if let Ok(path) = std::env::var("RINGPOP_NODE_READY_FILE") {
            while !std::path::Path::new(&path).exists() {
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            }
        } else {
            tokio::time::sleep(std::time::Duration::from_secs(3)).await;
        }
        ready_node.set_ready(true);
    });
    if let Ok(path) = std::env::var("RINGPOP_NODE_STATE_FILE") {
        let counted = node.clone();
        tokio::spawn(async move {
            loop {
                let counts = counted.request_counts();
                let _ = std::fs::write(
                    &path,
                    format!("{{\"join\":{},\"ping\":{}}}\n", counts.join, counts.ping),
                );
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        });
    }
    tchannel::serve_ringpop(addr, node.handler()).await?;
    Ok(())
}
