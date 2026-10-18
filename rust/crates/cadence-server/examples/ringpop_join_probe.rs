//! One-shot Rust JOIN sender probe used by the pinned Go interoperability test.
use cadence_common::ringpop_wire::{Member, ALIVE};
use cadence_server::ringpop_provider::RingpopNode;
use std::time::Duration;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let seed = args.next().expect("seed host:port");
    let local = args.next().expect("local host:port");
    let app = args.next().unwrap_or_else(|| "cadence".to_owned());
    let node = RingpopNode::new(
        app,
        Member {
            address: local,
            status: ALIVE.into(),
            incarnation: 100,
            labels: None,
        },
        true,
    );
    let response = node.join_seed(&seed, Duration::from_secs(2)).await?;
    let (members, checksum) = node.membership_snapshot();
    println!(
        "{}",
        serde_json::json!({"response": response, "members": members, "checksum": checksum})
    );
    Ok(())
}
