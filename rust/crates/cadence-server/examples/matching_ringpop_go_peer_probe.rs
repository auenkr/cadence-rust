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
use tokio::{net::TcpListener, task::JoinHandle};

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
    first.self_evict().await?;
    // Leave time for Go's listener and provider event callback to observe the
    // FAULTY update before stopping the shared listener.
    tokio::time::sleep(Duration::from_secs(2)).await;
    stop_provider(first, first_server).await?;
    tokio::time::sleep(Duration::from_millis(25)).await;

    let (restarted, restarted_server) = start_provider(bind, &seed).await?;
    println!(
        "rust-peer-recovered {}",
        restarted.node().membership_snapshot().0.len()
    );
    tokio::time::sleep(Duration::from_secs(2)).await;
    stop_provider(restarted, restarted_server).await
}
