//! Exercise the owned Rust SWIM probe loop against a live Go Ringpop node.
use cadence_common::ringpop_wire::{Member, ALIVE};
use cadence_server::ringpop_provider::RingpopNode;
use std::time::Duration;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let target = args.next().expect("Go Ringpop target address");
    let local = args.next().expect("Rust advertised address");
    let node = RingpopNode::new_with_swim_config(
        "test",
        Member {
            address: local.clone(),
            status: ALIVE.into(),
            incarnation: 100,
            labels: None,
        },
        false,
        Duration::from_secs(1),
        Duration::from_secs(1),
        3,
        Duration::from_millis(20),
        cadence_common::ringpop_state_transitions::StateTimeouts::default(),
    );
    node.join_seed(&target, Duration::from_secs(1)).await?;
    node.set_ready(true);
    node.start_swim().map_err(anyhow::Error::msg)?;
    // Keep the process alive while the Go peer's gossip loop observes this
    // node. The compatibility test checks the ALIVE transition before Stop.
    tokio::time::sleep(Duration::from_secs(1)).await;
    node.stop();
    let (members, checksum) = node.membership_snapshot();
    println!(
        "{}",
        serde_json::json!({
            "ready": node.is_ready(),
            "lifecycle": format!("{:?}", node.lifecycle()),
            "members": members,
            "checksum": checksum,
            "rustAddress": local,
            "goTarget": target,
        })
    );
    Ok(())
}
