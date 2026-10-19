use anyhow::{Context, Result};
use cadence_server::{
    config::{BootstrapMode, RingpopConfig},
    grpc_matching::Matching,
    handlers::AppState,
    matching_peer_provider::MatchingPeerProvider,
    matching_ringpop_provider::MatchingRingpopProvider,
    tchannel,
};
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    net::TcpListener,
    task::JoinHandle,
};

async fn wait_for_go_command(
    reader: &mut BufReader<tokio::io::Stdin>,
    expected: &str,
) -> Result<()> {
    let mut command = String::new();
    let bytes = reader.read_line(&mut command).await?;
    anyhow::ensure!(bytes != 0, "Go peer closed stdin before {expected:?}");
    anyhow::ensure!(
        command.trim_end() == expected,
        "expected Go command {expected:?}, got {command:?}"
    );
    Ok(())
}

async fn start_provider(
    bind: SocketAddr,
    seed: &str,
) -> Result<(Arc<MatchingRingpopProvider>, JoinHandle<Result<()>>)> {
    let listener = TcpListener::bind(bind).await?;
    let local = listener.local_addr()?;
    let provider = Arc::new(MatchingRingpopProvider::new(
        RingpopConfig {
            name: "cadence".into(),
            bootstrap_mode: BootstrapMode::Hosts,
            bootstrap_hosts: vec![seed.to_owned()],
            max_join_duration_nanos: 10_000_000_000,
            ..Default::default()
        },
        "cadence-matching",
        local,
        7234,
    )?);
    let server = tokio::spawn(tchannel::serve_matching_with_ringpop_listener(
        listener,
        Matching::new(AppState::default()),
        provider.handler(),
    ));
    provider
        .start()
        .await
        .context("starting Rust Matching Ringpop")?;
    wait_for_history(&provider).await?;
    Ok((provider, server))
}

async fn wait_for_history(provider: &MatchingRingpopProvider) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if provider
                .members("cadence-history")
                .await
                .is_ok_and(|members| !members.is_empty())
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .context("Go History peer did not converge")
}

async fn stop_provider(
    provider: Arc<MatchingRingpopProvider>,
    server: JoinHandle<Result<()>>,
) -> Result<()> {
    provider.stop().await?;
    server.abort();
    let _ = server.await;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut go_commands = BufReader::new(tokio::io::stdin());
    let mut args = std::env::args().skip(1);
    let bind: SocketAddr = args
        .next()
        .context("usage: matching_ringpop_go_peer_probe <bind> <go-seed>")?
        .parse()?;
    let seed = args.next().context("missing Go Ringpop seed")?;

    let (first, first_server) = start_provider(bind, &seed).await?;
    println!(
        "rust-peer-ready {}",
        first.node().membership_snapshot().0.len()
    );
    wait_for_go_command(&mut go_commands, "evict").await?;
    first.self_evict().await?;
    wait_for_go_command(&mut go_commands, "restart").await?;
    stop_provider(first, first_server).await?;

    let (restarted, restarted_server) = start_provider(bind, &seed).await?;
    println!(
        "rust-peer-recovered {}",
        restarted.node().membership_snapshot().0.len()
    );
    wait_for_go_command(&mut go_commands, "stop").await?;
    stop_provider(restarted, restarted_server).await
}
