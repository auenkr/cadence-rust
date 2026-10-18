//! One-shot Rust direct PING probe used by the pinned Go interoperability test.
use cadence_common::ringpop_wire::{Member, ALIVE};
use cadence_server::ringpop_provider::RingpopNode;
use std::time::Duration;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let target = args.next().expect("target host:port");
    let local = args.next().expect("local host:port");
    let node = RingpopNode::new(
        "cadence",
        Member {
            address: local,
            status: ALIVE.into(),
            incarnation: 100,
            labels: None,
        },
        true,
    );
    let response = node.ping(&target, Duration::from_secs(2)).await?;
    let (members, checksum) = node.membership_snapshot();
    let max_p = node.max_propagations();
    println!(
        "{}",
        serde_json::json!({"response": response, "members": members,
        "checksum": checksum, "queued": node.dissemination_counts(), "maxP": max_p})
    );
    Ok(())
}
