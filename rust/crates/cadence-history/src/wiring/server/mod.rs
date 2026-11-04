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

#[derive(Debug, Clone)]
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
}

impl ServerConfig {
    pub fn new(thrift_addr: SocketAddr, grpc_addr: SocketAddr) -> Self {
        Self {
            thrift_addr,
            grpc_addr,
            max_message_size: 4 * 1024 * 1024,
            rps: 0.0,
            shutdown_grace: Duration::from_secs(10),
        }
    }
}

/// Shared state of both transports.
#[derive(Clone)]
pub struct ServerState {
    pub handler: Arc<dyn Handler>,
    pub limiter: Arc<limiter::RateLimiter>,
    pub max_message_size: usize,
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
        let state = ServerState {
            handler: handler.clone(),
            limiter: Arc::new(limiter::RateLimiter::new(cfg.rps)),
            max_message_size: cfg.max_message_size,
        };
        let thrift = tokio::net::TcpListener::bind(cfg.thrift_addr).await?;
        let grpc_listener = tokio::net::TcpListener::bind(cfg.grpc_addr).await?;
        let thrift_addr = thrift.local_addr()?;
        let grpc_addr = grpc_listener.local_addr()?;
        let (stop, rx) = watch::channel(false);
        handler.start();
        let t1 = tokio::spawn(tchannel::serve(thrift, state.clone(), rx.clone()));
        let t2 = tokio::spawn(grpc::serve(grpc_listener, state, rx));
        Ok(RunningServer { thrift_addr, grpc_addr, handler, grace: cfg.shutdown_grace, stop, tasks: vec![t1, t2] })
    }
}

impl RunningServer {
    /// Graceful shutdown: handler.prepare_to_stop, stop listeners, handler.stop.
    pub async fn shutdown(self) {
        let _ = self.handler.prepare_to_stop(self.grace).await;
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
