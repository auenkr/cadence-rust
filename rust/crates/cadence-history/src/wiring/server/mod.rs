#![allow(clippy::result_large_err)]
//! Serving layer for the History service (port of go/service/history/service.go
//! and the thrift/grpc wrappers around handler/handler.go).
//!
//! `HistoryServer::start(cfg, handler)` binds the Thrift TChannel and gRPC
//! listeners and dispatches to the `Handler` trait object.
pub mod errors;
pub mod grpc;
pub mod limiter;
pub mod tchannel;
pub mod thrift_g1; pub mod thrift_types_g1; pub mod grpc_g1;
pub mod thrift_g2; pub mod thrift_types_g2; pub mod grpc_g2;
pub mod thrift_g3; pub mod thrift_types_g3; pub mod grpc_g3;

use crate::handler::Handler;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tokio::task::JoinHandle;

#[derive(Clone)]
pub struct ServerConfig {
    /// `services.history.rpc` Thrift TChannel bind address.
    pub thrift_addr: SocketAddr,
    /// `services.history.rpc` gRPC bind address.
    pub grpc_addr: SocketAddr,
    pub max_message_size: usize,
    /// Per-host request rate limit (requests per second); <= 0 disables.
    pub rps: f64,
    /// Time allowed for in-flight work to finish at shutdown.
    pub shutdown_grace: Duration,
    /// Ringpop gossip handler served on the Thrift TChannel listener (`ringpop` service).
    pub ringpop: Option<cadence_service_core::ringpop_tchannel::RingpopHandler>,
    /// Go `rpc.tls` for the gRPC listener (the TChannel listener stays plaintext, as in Go).
    pub grpc_tls: Option<tonic::transport::ServerTlsConfig>,
    /// Go-compatible TLS cases tonic cannot express (no server identity, empty client CA pool).
    pub grpc_tls_custom: Option<Arc<rustls::ServerConfig>>,
}

impl ServerConfig {
    pub fn new(thrift_addr: SocketAddr, grpc_addr: SocketAddr) -> Self {
        Self {
            thrift_addr,
            grpc_addr,
            max_message_size: 4 * 1024 * 1024,
            rps: 0.0,
            shutdown_grace: Duration::from_secs(10),
            ringpop: None,
            grpc_tls: None,
            grpc_tls_custom: None,
        }
    }
}

/// Shared state of both transports.
#[derive(Clone)]
pub struct ServerState {
    pub handler: Arc<dyn Handler>,
    pub limiter: Arc<limiter::RateLimiter>,
    pub max_message_size: usize,
    pub ringpop: Option<cadence_service_core::ringpop_tchannel::RingpopHandler>,
}

pub struct HistoryServer;

pub struct RunningServer {
    pub thrift_addr: SocketAddr,
    pub grpc_addr: SocketAddr,
    handler: Arc<dyn Handler>,
    grace: Duration,
    stop: watch::Sender<bool>,
    tasks: Vec<JoinHandle<()>>,
}

impl HistoryServer {
    pub async fn start(cfg: ServerConfig, handler: Arc<dyn Handler>) -> anyhow::Result<RunningServer> {
        let thrift = tokio::net::TcpListener::bind(cfg.thrift_addr).await?;
        let grpc_listener = tokio::net::TcpListener::bind(cfg.grpc_addr).await?;
        Self::start_with_listeners(cfg, handler, thrift, grpc_listener).await
    }

    /// Like [`start`](Self::start) on already-bound listeners (Ringpop needs the bound
    /// TChannel port before it can advertise the host).
    pub async fn start_with_listeners(
        cfg: ServerConfig,
        handler: Arc<dyn Handler>,
        thrift: tokio::net::TcpListener,
        grpc_listener: tokio::net::TcpListener,
    ) -> anyhow::Result<RunningServer> {
        let state = ServerState {
            handler: handler.clone(),
            limiter: Arc::new(limiter::RateLimiter::new(cfg.rps)),
            max_message_size: cfg.max_message_size,
            ringpop: cfg.ringpop.clone(),
        };
        let thrift_addr = thrift.local_addr()?;
        let grpc_addr = grpc_listener.local_addr()?;
        let (stop, rx) = watch::channel(false);
        handler.start();
        let t1 = tokio::spawn(tchannel::serve(thrift, state.clone(), rx.clone()));
        let t2 = tokio::spawn(grpc::serve(grpc_listener, state, rx, cfg.grpc_tls.clone(), cfg.grpc_tls_custom.clone()));
        Ok(RunningServer { thrift_addr, grpc_addr, handler, grace: cfg.shutdown_grace, stop, tasks: vec![t1, t2] })
    }
}

impl RunningServer {
    /// Graceful shutdown: handler.prepare_to_stop, stop listeners, handler.stop.
    pub async fn shutdown(self) {
        let _ = self.handler.prepare_to_stop(self.grace).await;
        self.stop_now().await;
    }

    /// Stop listeners and the handler without the `prepare_to_stop` drain (the caller
    /// already ran it, as Go `Service.Stop` does).
    pub async fn stop_now(self) {
        let _ = self.stop.send(true);
        for t in self.tasks {
            let _ = tokio::time::timeout(self.grace, t).await;
        }
        self.handler.stop();
    }
}

#[cfg(test)]
pub(crate) mod test_support;
#[cfg(test)]
mod tests;
