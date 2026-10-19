//! Test-only Rust indirect probe against pinned Go Ringpop peers.
use cadence_common::ringpop_wire::{Member, ALIVE};
use cadence_server::ringpop_provider::RingpopNode;
use std::{collections::BTreeMap, time::Duration};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let helper = args.next().expect("Go helper address");
    let local = args.next().expect("Rust requester address");
    let target = args.next().expect("Go target address");
    let timeout = Duration::from_secs(3);
    let node = RingpopNode::new(
        "cadence",
        Member {
            address: local,
            status: ALIVE.into(),
            incarnation: 100,
            labels: Some(BTreeMap::from([("serviceName".into(), "matching".into())])),
        },
        true,
    );
    node.join_seed(&helper, timeout).await?;
    let (reached, errors) = node.indirect_ping(&target, 1, timeout).await;
    let (members, checksum) = node.membership_snapshot();
    println!(
        "{}",
        serde_json::json!({
            "reached": reached,
            "errors": errors.len(),
            "members": members,
            "checksum": checksum,
            "dissemination": node.dissemination_counts(),
        })
    );
    Ok(())
}
