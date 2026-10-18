//! One-shot Rust JOIN sender probe used by the pinned Go interoperability test.
use cadence_common::ringpop_bootstrap_selection::{BootstrapSelection, Options};
use cadence_common::ringpop_wire::{Member, ALIVE};
use cadence_server::ringpop_provider::{JoinGroupState, RingpopNode};
use std::time::Duration;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let first = args.next().expect("seed host:port or --group");
    let group = first == "--group";
    let local = args.next().expect("local host:port");
    let app = if group {
        "cadence".to_owned()
    } else {
        args.next().unwrap_or_else(|| "cadence".to_owned())
    };
    let seeds = if group {
        args.collect::<Vec<_>>()
    } else {
        vec![first.clone()]
    };
    let node = RingpopNode::new(
        app,
        Member {
            address: local.clone(),
            status: ALIVE.into(),
            incarnation: 100,
            labels: None,
        },
        true,
    );
    if group {
        let selection = BootstrapSelection::new(&local, &seeds, Options::default());
        let mut sender = JoinGroupState::new(selection);
        let result = node.join_group(&mut sender, &[]).await;
        let (members, checksum) = node.membership_snapshot();
        println!(
            "{}",
            serde_json::json!({"result": result, "members": members, "checksum": checksum})
        );
        return Ok(());
    }
    let response = node.join_seed(&first, Duration::from_secs(2)).await?;
    let (members, checksum) = node.membership_snapshot();
    println!(
        "{}",
        serde_json::json!({"response": response, "members": members, "checksum": checksum})
    );
    Ok(())
}
