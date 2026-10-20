use cadence_server::{config, handlers, service};
use clap::{Parser, Subcommand};

fn matching_http_addr(rpc: &config::RpcConfig) -> anyhow::Result<Option<std::net::SocketAddr>> {
    let Some(http) = rpc.http.as_ref() else {
        return Ok(None);
    };
    if rpc.bind_on_local_host && !rpc.bind_on_ip.is_empty() {
        anyhow::bail!("bindOnLocalHost and bindOnIP are mutually exclusive");
    }
    if http.port == 0 {
        anyhow::bail!("HTTP port is not set");
    }
    let ip = if rpc.bind_on_local_host {
        std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)
    } else if !rpc.bind_on_ip.is_empty() {
        rpc.bind_on_ip.parse().map_err(|_| {
            anyhow::anyhow!(
                "unable to parse bindOnIP value or it is not an IPv4 or IPv6 address: {}",
                rpc.bind_on_ip
            )
        })?
    } else {
        // Match Go common/rpc/localip.go's address score and first-highest tie rule.
        let interfaces = if_addrs::get_if_addrs()?;
        let mut best: Option<(i32, std::net::IpAddr)> = None;
        for iface in interfaces {
            let ip = iface.ip();
            let mut score = if ip.is_ipv4() { 300 } else { 0 };
            if !iface.is_loopback() && !ip.is_loopback() {
                // getifaddrs omits interface flags; active addresses are treated as UP.
                score += 200;
            }
            if best.as_ref().is_none_or(|(previous, _)| score > *previous) {
                best = Some((score, ip));
            }
        }
        best.map(|(_, ip)| ip)
            .ok_or_else(|| anyhow::anyhow!("no addresses to listen on"))?
    };
    Ok(Some(std::net::SocketAddr::new(ip, http.port)))
}

fn dynamic_bool_property(
    overlay: std::sync::Arc<std::sync::Mutex<cadence_server::admin::DynamicConfigOverlay>>,
    name: &str,
    default: bool,
) -> bool {
    let Ok(overlay) = overlay.lock() else {
        return default;
    };
    overlay
        .property_values(name)
        .iter()
        .filter(|value| value.filters.is_empty())
        .filter_map(|value| value.value.as_ref())
        .filter(|blob| blob.encoding_type == Some(cadence_api::enums::EncodingType::JSON))
        .filter_map(|blob| serde_json::from_slice::<bool>(&blob.data).ok())
        .next_back()
        .unwrap_or(default)
}

async fn shutdown_signal() -> anyhow::Result<()> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result.map_err(anyhow::Error::from),
            _ = terminate.recv() => Ok(()),
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await.map_err(anyhow::Error::from)
    }
}

async fn matching_http_tls(
    http: &config::HttpConfig,
) -> anyhow::Result<Option<axum_server::tls_rustls::RustlsConfig>> {
    let mode = http.tls_mode.as_deref().unwrap_or("disabled");
    if !matches!(mode, "disabled" | "permissive" | "enforced") {
        anyhow::bail!("unknown TLSMode {mode:?}; expected disabled, permissive, or enforced");
    }
    let tls = http.tls.as_ref();
    let Some(tls) = tls.filter(|tls| tls.enabled) else {
        return Ok(None);
    };
    if tls.require_client_auth
        || !tls.ca_file.is_empty()
        || !tls.ca_files.is_empty()
        || tls.enable_host_verification
        || !tls.server_name.is_empty()
        || !tls.ssl_mode.is_empty()
    {
        anyhow::bail!("Matching HTTP TLS configured CA/client-auth/host-verification/serverName/sslmode options are unsupported");
    }
    if mode == "disabled" {
        if !tls.cert_file.is_empty() && !tls.key_file.is_empty() {
            axum_server::tls_rustls::RustlsConfig::from_pem_file(&tls.cert_file, &tls.key_file)
                .await
                .map_err(|error| anyhow::anyhow!("creating TLS config for HTTP: {error}"))?;
        }
        return Ok(None);
    }
    let config = if !tls.cert_file.is_empty() && !tls.key_file.is_empty() {
        axum_server::tls_rustls::RustlsConfig::from_pem_file(&tls.cert_file, &tls.key_file)
            .await
            .map_err(|error| {
                anyhow::anyhow!("loading Matching HTTP TLS certificate/key: {error}")
            })?
    } else {
        let no_certificate = std::sync::Arc::new(NoServerCertificate);
        let server = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_cert_resolver(no_certificate);
        axum_server::tls_rustls::RustlsConfig::from_config(std::sync::Arc::new(server))
    };
    Ok(Some(config))
}

#[derive(Debug)]
struct NoServerCertificate;
impl rustls::server::ResolvesServerCert for NoServerCertificate {
    fn resolve(
        &self,
        _client_hello: rustls::server::ClientHello<'_>,
    ) -> Option<std::sync::Arc<rustls::sign::CertifiedKey>> {
        None
    }
}

async fn accept_matching_tls<S>(
    acceptor: tokio_rustls::TlsAcceptor,
    stream: S,
    timeout: std::time::Duration,
) -> std::io::Result<tokio_rustls::server::TlsStream<S>>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    match tokio::time::timeout(timeout, acceptor.accept(stream)).await {
        Ok(Ok(tls)) => Ok(tls),
        Ok(Err(error)) => Err(std::io::Error::other(error)),
        Err(_) => Err(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "TLS handshake timed out",
        )),
    }
}

async fn serve_matching_http_permissive(
    addr: std::net::SocketAddr,
    app: axum::Router,
    config: axum_server::tls_rustls::RustlsConfig,
) -> anyhow::Result<()> {
    use axum::serve::Listener;
    use std::io;
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use std::time::Duration;
    use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, ReadBuf};
    use tokio::sync::mpsc;
    use tokio::task::JoinSet;

    trait Io: AsyncRead + AsyncWrite {}
    impl<T: AsyncRead + AsyncWrite> Io for T {}
    type BoxIo = Box<dyn Io + Unpin + Send>;

    struct PrefixedStream {
        prefix: std::io::Cursor<Vec<u8>>,
        stream: tokio::net::TcpStream,
    }
    impl AsyncRead for PrefixedStream {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            if self.prefix.position() < self.prefix.get_ref().len() as u64 {
                let remaining = &self.prefix.get_ref()[self.prefix.position() as usize..];
                let n = remaining.len().min(buf.remaining());
                buf.put_slice(&remaining[..n]);
                let position = self.prefix.position();
                self.prefix.set_position(position + n as u64);
                return Poll::Ready(Ok(()));
            }
            Pin::new(&mut self.stream).poll_read(cx, buf)
        }
    }
    impl AsyncWrite for PrefixedStream {
        fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            Pin::new(&mut self.stream).poll_write(cx, buf)
        }
        fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Pin::new(&mut self.stream).poll_flush(cx)
        }
        fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Pin::new(&mut self.stream).poll_shutdown(cx)
        }
    }
    enum Stream {
        Plain(PrefixedStream),
        Tls(Box<tokio_rustls::server::TlsStream<PrefixedStream>>),
    }
    impl AsyncRead for Stream {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            match &mut *self {
                Self::Plain(s) => Pin::new(s).poll_read(cx, buf),
                Self::Tls(s) => Pin::new(s).poll_read(cx, buf),
            }
        }
    }
    impl AsyncWrite for Stream {
        fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            match &mut *self {
                Self::Plain(s) => Pin::new(s).poll_write(cx, buf),
                Self::Tls(s) => Pin::new(s).poll_write(cx, buf),
            }
        }
        fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            match &mut *self {
                Self::Plain(s) => Pin::new(s).poll_flush(cx),
                Self::Tls(s) => Pin::new(s).poll_flush(cx),
            }
        }
        fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            match &mut *self {
                Self::Plain(s) => Pin::new(s).poll_shutdown(cx),
                Self::Tls(s) => Pin::new(s).poll_shutdown(cx),
            }
        }
    }
    const SNIFF_TIMEOUT: Duration = Duration::from_secs(15);
    const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(120);
    type ConnectionResult = (io::Result<Stream>, std::net::SocketAddr);
    struct MixedListener {
        listener: tokio::net::TcpListener,
        acceptor: tokio_rustls::TlsAcceptor,
        addr: std::net::SocketAddr,
        completed: mpsc::UnboundedReceiver<ConnectionResult>,
        sender: mpsc::UnboundedSender<ConnectionResult>,
        tasks: JoinSet<()>,
    }
    async fn classify_and_handshake(
        stream: tokio::net::TcpStream,
        acceptor: tokio_rustls::TlsAcceptor,
        peer: std::net::SocketAddr,
        sniff_timeout: Duration,
        handshake_timeout: Duration,
    ) -> ConnectionResult {
        let mut stream = stream;
        let mut header = [0; 5];
        let n = match tokio::time::timeout(sniff_timeout, stream.read(&mut header)).await {
            Ok(Ok(n)) if n > 0 => n,
            Ok(Ok(_)) => return (Err(io::Error::from(io::ErrorKind::UnexpectedEof)), peer),
            Ok(Err(error)) => return (Err(error), peer),
            Err(_) => {
                return (
                    Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "TLS sniff timed out",
                    )),
                    peer,
                )
            }
        };
        let stream = PrefixedStream {
            prefix: std::io::Cursor::new(header[..n].to_vec()),
            stream,
        };
        // YARPC's isTLSClientHelloRecord performs one Read. A short read is
        // deliberately classified as plaintext, even when it is fragmented TLS.
        let is_tls = n == 5 && header[0] == 0x16 && header[1] == 0x03 && header[2] >= 1;
        let connection = if is_tls {
            match accept_matching_tls(acceptor, stream, handshake_timeout).await {
                Ok(tls) => Stream::Tls(Box::new(tls)),
                Err(error) => return (Err(error), peer),
            }
        } else {
            Stream::Plain(stream)
        };
        (Ok(connection), peer)
    }
    impl Listener for MixedListener {
        type Io = BoxIo;
        type Addr = std::net::SocketAddr;
        async fn accept(&mut self) -> (Self::Io, Self::Addr) {
            loop {
                tokio::select! {
                    accepted = self.listener.accept() => {
                        let (stream, peer) = match accepted { Ok(v) => v, Err(_) => continue };
                        let tx = self.sender.clone();
                        let acceptor = self.acceptor.clone();
                        self.tasks.spawn(async move {
                            let result = classify_and_handshake(stream, acceptor, peer, SNIFF_TIMEOUT, HANDSHAKE_TIMEOUT).await;
                            let _ = tx.send(result);
                        });
                    }
                    result = self.completed.recv() => {
                        let Some((Ok(io), peer)) = result else { continue; };
                        return (Box::new(io), peer);
                    }
                    _ = self.tasks.join_next(), if !self.tasks.is_empty() => {}
                }
            }
        }
        fn local_addr(&self) -> io::Result<Self::Addr> {
            Ok(self.addr)
        }
    }
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let addr = listener.local_addr()?;
    // Go's muxlistener starts one goroutine per accepted connection and has no
    // application-level pending-connection cap.
    let (sender, completed) = mpsc::unbounded_channel();
    let listener = MixedListener {
        listener,
        acceptor: tokio_rustls::TlsAcceptor::from(config.get_inner()),
        addr,
        completed,
        sender,
        tasks: JoinSet::new(),
    };
    axum::serve(listener, app).await?;
    Ok(())
}

#[cfg(test)]
mod matching_http_listener_tests {
    use super::*;
    #[tokio::test(start_paused = true)]
    async fn tls_handshake_timeout_is_injectable() {
        use tokio::io::AsyncWriteExt;

        let config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_cert_resolver(std::sync::Arc::new(NoServerCertificate));
        let acceptor = tokio_rustls::TlsAcceptor::from(std::sync::Arc::new(config));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let accepting = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            accept_matching_tls(acceptor, stream, std::time::Duration::from_millis(10))
                .await
                .unwrap_err()
                .kind()
        });
        let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();
        client
            .write_all(&[0x16, 0x03, 0x03, 0x00, 0x01])
            .await
            .unwrap();
        assert_eq!(accepting.await.unwrap(), std::io::ErrorKind::TimedOut);
    }

    #[test]
    fn configured_listener_uses_bind_policy() {
        let rpc: config::RpcConfig =
            serde_yaml::from_str("bindOnLocalHost: true\nhttp: {port: 8801}").unwrap();
        assert_eq!(
            matching_http_addr(&rpc).unwrap().unwrap().to_string(),
            "127.0.0.1:8801"
        );
    }
    #[tokio::test]
    async fn rejects_invalid_configuration_and_detects_enabled_tls() {
        let rpc: config::RpcConfig = serde_yaml::from_str("http: {port: 0}").unwrap();
        assert!(matching_http_addr(&rpc)
            .unwrap_err()
            .to_string()
            .contains("HTTP port is not set"));
        let tls: config::HttpConfig = serde_yaml::from_str("tls: {enabled: true}").unwrap();
        assert!(matching_http_tls(&tls).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn validates_tls_mode_and_certificate_configuration() {
        let enforced_without_tls: config::HttpConfig =
            serde_yaml::from_str("TLSMode: enforced").unwrap();
        assert!(matching_http_tls(&enforced_without_tls)
            .await
            .unwrap()
            .is_none());
        let enforced_with_tls_disabled: config::HttpConfig =
            serde_yaml::from_str("TLSMode: enforced\ntls: {enabled: false}").unwrap();
        assert!(matching_http_tls(&enforced_with_tls_disabled)
            .await
            .unwrap()
            .is_none());
        let permissive_without_tls: config::HttpConfig =
            serde_yaml::from_str("TLSMode: permissive").unwrap();
        assert!(matching_http_tls(&permissive_without_tls)
            .await
            .unwrap()
            .is_none());

        let uppercase: config::HttpConfig = serde_yaml::from_str(
            "TLSMode: Enforced\ntls: {enabled: true, certFile: cert, keyFile: key}",
        )
        .unwrap();
        assert!(matching_http_tls(&uppercase)
            .await
            .err()
            .unwrap()
            .to_string()
            .contains("unknown TLSMode"));
        let empty: config::HttpConfig = serde_yaml::from_str("TLSMode: ''").unwrap();
        assert!(matching_http_tls(&empty)
            .await
            .err()
            .unwrap()
            .to_string()
            .contains("unknown TLSMode"));

        let permissive: config::HttpConfig = serde_yaml::from_str(
            "TLSMode: permissive\ntls: {enabled: true, certFile: cert, keyFile: key}",
        )
        .unwrap();
        assert!(matching_http_tls(&permissive)
            .await
            .err()
            .unwrap()
            .to_string()
            .contains("loading Matching HTTP TLS certificate/key"));

        let missing_cert: config::HttpConfig =
            serde_yaml::from_str("TLSMode: enforced\ntls: {enabled: true, keyFile: key}").unwrap();
        assert!(matching_http_tls(&missing_cert).await.unwrap().is_some());

        let invalid_cert = tempfile::NamedTempFile::new().unwrap();
        let invalid_key = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(invalid_cert.path(), b"not a certificate").unwrap();
        std::fs::write(invalid_key.path(), b"not a key").unwrap();
        let invalid_pair: config::HttpConfig = serde_yaml::from_str(&format!(
            "TLSMode: enforced\ntls: {{enabled: true, certFile: {:?}, keyFile: {:?}}}",
            invalid_cert.path().to_string_lossy(),
            invalid_key.path().to_string_lossy(),
        ))
        .unwrap();
        assert!(matching_http_tls(&invalid_pair)
            .await
            .err()
            .unwrap()
            .to_string()
            .contains("loading Matching HTTP TLS certificate/key"));
    }
}

#[derive(Debug, Parser)]
#[command(
    name = "cadence-server",
    version,
    about = "Cadence workflow engine (Rust port)"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Start one or more services (default: all).
    Start {
        /// YAML config file with Cadence keys; defaults to embedded development settings.
        #[arg(long)]
        config: Option<String>,
        /// Process root used to resolve relative dynamic-config paths (Go --root semantics).
        #[arg(long)]
        root: Option<String>,
        /// Services to run. Empty means all.
        #[arg(long, value_delimiter = ',')]
        services: Vec<String>,
        /// HTTP port for the JSON API.
        #[arg(long, default_value_t = 8080)]
        port: u16,
        /// Cadence protobuf gRPC frontend port.
        #[arg(long)]
        grpc_port: Option<u16>,
        /// Native internal history gRPC port (config history.rpc.grpcPort).
        #[arg(long)]
        history_grpc_port: Option<u16>,
        /// Native internal matching gRPC port (config matching.rpc.grpcPort).
        #[arg(long)]
        matching_grpc_port: Option<u16>,
        /// Native TChannel Thrift frontend port (config rpc.port by default).
        #[arg(long)]
        tchannel_port: Option<u16>,
        /// Native internal Matching Thrift/TChannel port (matching.rpc.port).
        #[arg(long)]
        matching_tchannel_port: Option<u16>,
        /// Directory for JSON snapshots (domains.json, executions.json).
        /// Enables restart durability; omitted means purely in-memory.
        #[arg(long)]
        data_dir: Option<String>,
    },
    /// Print API compatibility info.
    Version,
    /// Check a config file parses and validates.
    CheckConfig {
        #[arg(long)]
        config: String,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Start {
            config,
            root,
            services,
            port,
            grpc_port,
            history_grpc_port,
            matching_grpc_port,
            tchannel_port,
            matching_tchannel_port,
            data_dir,
        } => {
            // The no-config CLI is an explicit embedded development mode. It
            // keeps the in-memory domain view used by standalone compatibility
            // tests; an explicit YAML config must provide Matching's real
            // supported persistence backend.
            let embedded_development = config.is_none();
            let cfg = match config {
                Some(path) => config::Config::from_file(path)?,
                None => config::Config::development()?,
            };
            cfg.validate()?;
            let process_root = root
                .map(std::path::PathBuf::from)
                .unwrap_or(std::env::current_dir()?);
            let dynamic_config_provider = cfg.selected_dynamic_config_client();
            let dynamic_config_poll = match dynamic_config_provider {
                Some((_, provider)) if !provider.poll_interval.is_empty() => {
                    let nanos = cadence_server::config::parse_go_duration(&provider.poll_interval)?;
                    std::time::Duration::from_nanos(nanos.max(5_000_000_000) as u64)
                }
                _ => std::time::Duration::from_secs(5),
            };
            let dynamic_config_path = if let Some((client, provider)) = dynamic_config_provider {
                if !client.eq_ignore_ascii_case("filebased") {
                    anyhow::bail!("dynamic config client {client:?} is not supported by Rust Matching; only filebased is currently ported");
                }
                if provider.filepath.is_empty() {
                    eprintln!("warning: filebased dynamic config has no filepath; using no-op configuration");
                    None
                } else {
                    let path = std::path::PathBuf::from(&provider.filepath);
                    let path = if path.is_absolute() {
                        path
                    } else {
                        process_root.join(path)
                    };
                    Some(path)
                }
            } else {
                None
            };
            let wanted: Vec<service::ServiceName> = if services.is_empty() {
                service::ServiceName::all().to_vec()
            } else {
                services
                    .iter()
                    .map(|s| {
                        service::ServiceName::parse(s)
                            .ok_or_else(|| anyhow::anyhow!("unknown service {s:?}"))
                    })
                    .collect::<Result<_, _>>()?
            };
            let mut registry = service::ServiceRegistry::new();
            for s in &wanted {
                println!("starting {s} (api {})", cadence_api::API_COMPAT_VERSION);
                registry.mark_healthy(*s);
            }
            // `worker.enableScheduler` dynamic config is not ported yet; the
            // static file flag or CADENCE_ENABLE_SCHEDULER=1 enables schedules.
            let semaphore_enabled = cfg.enable_distributed_semaphore
                || std::env::var("CADENCE_ENABLE_SEMAPHORE")
                    .is_ok_and(|v| v == "1" || v.eq_ignore_ascii_case("true"));
            let scheduler_enabled = cfg.enable_scheduler
                || std::env::var("CADENCE_ENABLE_SCHEDULER")
                    .is_ok_and(|v| v == "1" || v.eq_ignore_ascii_case("true"));
            let num_history_shards = if cfg.persistence.num_history_shards > 0 {
                cfg.persistence.num_history_shards
            } else {
                4
            };
            let mut state = handlers::AppState {
                scheduler_enabled,
                semaphore_enabled,
                num_history_shards,
                ..Default::default()
            };
            let mut dynamic_config_loaded_at = None;
            let dynamic_config_loaded = if let Some(path) = dynamic_config_path.as_ref() {
                match cadence_server::admin::DynamicConfigOverlay::from_file(path) {
                    Ok(snapshot) => {
                        *state
                            .admin
                            .dynamic_config
                            .lock()
                            .expect("dynamic config lock") = snapshot;
                        dynamic_config_loaded_at = Some(std::time::SystemTime::now());
                        true
                    }
                    Err(error) => {
                        eprintln!("warning: loading dynamic config {path:?}: {error}; using no-op configuration");
                        false
                    }
                }
            } else {
                false
            };
            state.matching_http_isolated = wanted.contains(&service::ServiceName::Matching)
                && cfg
                    .services
                    .get("matching")
                    .is_some_and(|matching| matching.rpc.http.is_some());
            let metadata = cfg
                .cluster_metadata
                .as_ref()
                .expect("validated cluster metadata");
            state
                .current_cluster_name
                .clone_from(&metadata.current_cluster_name);
            state.cluster_metadata = metadata.metadata();
            let frontend_rpc = cfg.services.get("frontend").map(|s| &s.rpc);
            if let Some(limit) = frontend_rpc
                .map(|r| r.grpc_max_msg_size)
                .filter(|size| *size > 0)
            {
                state.grpc_max_message_size = limit;
            }
            // Optional file durability: load snapshots, then persist every
            // 5 seconds. Crash losses are bounded by the interval.
            let snapshot_paths: Option<(std::path::PathBuf, std::path::PathBuf)> =
                data_dir.as_ref().map(|dir| {
                    let dir = std::path::PathBuf::from(dir);
                    (dir.join("domains.json"), dir.join("executions.json"))
                });
            if let Some((ref domains_path, ref executions_path)) = snapshot_paths {
                if let Some(parent) = domains_path.parent() {
                    if !parent.as_os_str().is_empty() {
                        std::fs::create_dir_all(parent)?;
                    }
                }
                if domains_path.exists() {
                    match cadence_persistence::InMemoryDomainStore::load_from_file(domains_path) {
                        Ok(loaded) => state.domains = loaded,
                        Err(e) => eprintln!("warning: ignoring {domains_path:?}: {e}"),
                    }
                }
                if executions_path.exists() {
                    match cadence_persistence::InMemoryExecutionStore::load_from_file(
                        executions_path,
                    ) {
                        Ok(loaded) => {
                            state.executions = loaded;
                        }
                        Err(e) => eprintln!("warning: ignoring {executions_path:?}: {e}"),
                    }
                }
            }
            // UUID namespace registration also upgrades older name-keyed snapshots.
            state.executions.sync_domains(
                &state
                    .domains
                    .list()
                    .domains
                    .into_iter()
                    .filter_map(|desc| desc.domain_info.map(|info| (info.name, info.uuid)))
                    .collect::<Vec<_>>(),
            );
            // Go's timer queue must keep advancing while development snapshots
            // serialize or write large histories. Never block an async RPC worker
            // with snapshot CPU or filesystem work.
            if let Some((domains_path, executions_path)) = snapshot_paths.clone() {
                let snapshot_store = state.executions.clone();
                let snapshot_domains = state.domains.clone();
                tokio::spawn(async move {
                    let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
                    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                    loop {
                        interval.tick().await;
                        let store = snapshot_store.clone();
                        let domains = snapshot_domains.clone();
                        let domains_path = domains_path.clone();
                        let executions_path = executions_path.clone();
                        // Await one write so snapshots cannot overlap or race
                        // their atomic temporary-file rename.
                        if let Err(error) = tokio::task::spawn_blocking(move || {
                            if let Err(error) = domains.save_to_file(domains_path) {
                                eprintln!("warning: snapshot domains failed: {error}");
                            }
                            if let Err(error) = store.save_to_file(executions_path) {
                                eprintln!("warning: snapshot executions failed: {error}");
                            }
                        })
                        .await
                        {
                            eprintln!("warning: snapshot task failed: {error}");
                        }
                    }
                });
            }
            let timer_store = state.executions.clone();
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(std::time::Duration::from_millis(500));
                interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    interval.tick().await;
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_millis() as i64)
                        .unwrap_or(0);
                    timer_store.expire_due_workflows(now);
                    timer_store.expire_due_decisions(now);
                    timer_store.fire_due_timers(now);
                    timer_store.expire_due_activities(now);
                    timer_store.requeue_due_activities(now);
                    timer_store.fire_due_backoffs(now);
                    // Scheduler worker: fire due schedule actions into target
                    // workflows, mirroring Go's scheduler workflow ticks.
                    timer_store.fire_due_schedules(now.saturating_mul(1_000_000));
                    // Worker-role async-request consumer, mirroring Go's
                    // async-queue consumer draining into frontend Start calls.
                    for request in timer_store.drain_async_requests() {
                        if !timer_store.consume_async_request(&request) {
                            timer_store.requeue_async_request(request);
                        }
                    }
                }
            });
            let grpc_state = state.clone();
            let history_state = state.clone();
            let tchannel_state = state.clone();
            let run_frontend = wanted.contains(&service::ServiceName::Frontend);
            let app = if run_frontend {
                handlers::router(state.clone())
            } else {
                handlers::health_router()
            };
            let addr = std::net::SocketAddr::from(([0, 0, 0, 0], port));
            println!(
                "cadence-server running on {addr}: {}",
                wanted
                    .iter()
                    .map(|s| s.as_str())
                    .collect::<Vec<_>>()
                    .join(",")
            );
            let listener = tokio::net::TcpListener::bind(addr).await?;
            let grpc_port = grpc_port
                .or_else(|| frontend_rpc.map(|r| r.grpc_port).filter(|p| *p > 0))
                .unwrap_or(7833);
            let grpc_bind = if frontend_rpc.is_some_and(|r| r.bind_on_local_host) {
                [127, 0, 0, 1]
            } else {
                [0, 0, 0, 0]
            };
            let grpc_addr = std::net::SocketAddr::from((grpc_bind, grpc_port));
            let tchannel_port = tchannel_port
                .or_else(|| frontend_rpc.map(|r| r.port).filter(|p| *p > 0))
                .unwrap_or(7933);
            let tchannel_addr = std::net::SocketAddr::from((grpc_bind, tchannel_port));
            if run_frontend {
                println!("Cadence gRPC frontend listening on {grpc_addr}");
                println!("Cadence TChannel frontend listening on {tchannel_addr}");
            }
            let history_rpc = cfg.services.get("history").map(|s| &s.rpc);
            let history_addr = std::net::SocketAddr::from((
                if history_rpc.is_some_and(|r| r.bind_on_local_host) {
                    [127, 0, 0, 1]
                } else {
                    [0, 0, 0, 0]
                },
                history_grpc_port
                    .or_else(|| history_rpc.map(|r| r.grpc_port).filter(|p| *p > 0))
                    .unwrap_or(7834),
            ));
            let run_history = wanted.contains(&service::ServiceName::History);
            if run_history {
                println!("Cadence internal history gRPC listening on {history_addr}");
            }
            // The listener task is spawned and must own its configuration.
            let matching_rpc = cfg.services.get("matching").map(|s| s.rpc.clone());
            let matching_addr = std::net::SocketAddr::from((
                if matching_rpc.as_ref().is_some_and(|r| r.bind_on_local_host) {
                    [127, 0, 0, 1]
                } else {
                    [0, 0, 0, 0]
                },
                matching_grpc_port
                    .or_else(|| {
                        matching_rpc
                            .as_ref()
                            .map(|r| r.grpc_port)
                            .filter(|p| *p > 0)
                    })
                    .unwrap_or(7835),
            ));
            let matching_tchannel_addr = std::net::SocketAddr::new(
                matching_addr.ip(),
                matching_tchannel_port
                    .or_else(|| matching_rpc.as_ref().map(|r| r.port).filter(|p| *p > 0))
                    .unwrap_or(7935),
            );
            let run_matching = wanted.contains(&service::ServiceName::Matching);
            // Reserve the production gRPC endpoint before Ringpop can publish
            // it. A conflicting port therefore fails startup before peers see
            // a label for a listener that could never start.
            let mut matching_grpc_listener = if run_matching && !embedded_development {
                Some(tokio::net::TcpListener::bind(matching_addr).await?)
            } else {
                None
            };
            let matching_addr = matching_grpc_listener
                .as_ref()
                .map(tokio::net::TcpListener::local_addr)
                .transpose()?
                .unwrap_or(matching_addr);
            // Production Matching shares its prebound TChannel socket with
            // Ringpop. Binding before provider construction ensures the
            // advertised port is the actual listener port. The explicit
            // no-config CLI remains the isolated embedded compatibility mode.
            let mut matching_tchannel_listener = if run_matching && !embedded_development {
                Some(tokio::net::TcpListener::bind(matching_tchannel_addr).await?)
            } else {
                None
            };
            let matching_ringpop_provider = if let Some(listener) = &matching_tchannel_listener {
                Some(std::sync::Arc::new(
                    cadence_server::matching_ringpop_provider::MatchingRingpopProvider::new(
                        cfg.ringpop.clone(),
                        "cadence-matching",
                        listener.local_addr()?,
                        matching_addr.port(),
                    )?,
                ))
            } else {
                None
            };
            let matching_membership = matching_ringpop_provider.as_ref().map(|provider| {
                cadence_server::matching_membership::Membership::new(
                    provider.local_identity().to_owned(),
                    Vec::new(),
                )
            });
            let mut matching_membership_lifecycle = matching_ringpop_provider
                .as_ref()
                .zip(matching_membership.clone())
                .map(|(provider, membership)| {
                    cadence_server::matching_membership_lifecycle::MatchingMembershipLifecycle::new(
                        "cadence-matching",
                        provider.clone(),
                        membership,
                    )
                });
            let matching_http_addr = if run_matching {
                matching_rpc
                    .as_ref()
                    .map(matching_http_addr)
                    .transpose()?
                    .flatten()
            } else {
                None
            };
            let matching_http_tls_config = if run_matching {
                match matching_rpc.as_ref().and_then(|rpc| rpc.http.as_ref()) {
                    Some(http) => matching_http_tls(http).await?,
                    None => None,
                }
            } else {
                None
            };
            if run_matching {
                if let Some(addr) = matching_http_addr {
                    println!("Cadence internal Matching HTTP listening on {addr}");
                }
            }
            if run_matching {
                println!("Cadence internal matching gRPC listening on {matching_addr}");
                println!("Cadence internal matching Thrift/TChannel listening on {matching_tchannel_addr}");
            }
            let matching_history = if run_matching {
                let history_peer = format!("http://127.0.0.1:{}", history_addr.port());
                let history_channel =
                    tonic::transport::Endpoint::from_shared(history_peer)?.connect_lazy();
                let history = cadence_server::matching_history::MatchingHistoryClient::new(
                    history_channel,
                    std::time::Duration::from_secs(1),
                    32 * 1024 * 1024,
                );
                if let Some(provider) = matching_ringpop_provider.as_ref() {
                    if !dynamic_bool_property(
                        state.admin.dynamic_config.clone(),
                        "system.enableGRPCOutbound",
                        true,
                    ) {
                        anyhow::bail!("Matching History routing currently requires system.enableGRPCOutbound=true; TChannel History outbound is not yet implemented");
                    }
                    let history_membership = std::sync::Arc::new(
                        cadence_server::matching_history_membership::RingpopHistoryMembership::new(
                            std::sync::Arc::new(provider.node().clone()),
                        ),
                    );
                    let history_resolver = std::sync::Arc::new(
                        cadence_server::matching_history_peer_resolver::MatchingHistoryPeerResolver::new(
                            num_history_shards as u32,
                            cadence_server::matching_history_peer_resolver::NamedPort::Grpc,
                            history_membership,
                        )?,
                    );
                    Some(history.with_peer_resolver(history_resolver))
                } else {
                    Some(history)
                }
            } else {
                None
            };
            let matching_domain_cache = if run_matching && !embedded_development {
                Some(
                    cadence_server::matching_domain_cache::MatchingDomainCache::from_default_store(
                        &cfg.persistence,
                        &state.current_cluster_name,
                    )?,
                )
            } else {
                None
            };
            // Explicit production config starts the shared metadata cache and
            // opens Matching queues on the configured defaultStore. The
            // private data-dir database is only an embedded-development path.
            let mut matching_runtime = if run_matching {
                let drain = cadence_server::matching_runtime::live_shutdown_drain_duration(
                    state.admin.dynamic_config.clone(),
                );
                let source = cadence_server::matching_task_store::select_source(
                    embedded_development,
                    data_dir.as_deref(),
                );
                Some(match source {
                    cadence_server::matching_task_store::MatchingTaskStoreSource::ConfiguredDefaultStore => {
                        let store = cadence_server::matching_task_store::SqliteTaskStoreConfig::from_default_store(&cfg.persistence)?.open_store()?;
                        let history = matching_history
                            .clone()
                            .expect("Matching history client is constructed when Matching runs");
                        let cache = matching_domain_cache.clone().ok_or_else(|| {
                            anyhow::anyhow!("configured Matching persistence requires its production domain cache")
                        })?;
                        cadence_server::matching_runtime::MatchingRuntime::with_durable_queues_and_domain_cache(
                            state.clone(),
                            store,
                            history,
                            std::num::NonZeroU32::new(100000).expect("task id range"),
                            std::time::Duration::from_secs(60),
                            drain,
                            cache,
                        ).await?
                    }
                    cadence_server::matching_task_store::MatchingTaskStoreSource::EmbeddedDataDir(dir) => {
                        let path = std::path::PathBuf::from(dir).join("matching.db");
                        let store = if path.exists() {
                            cadence_persistence::matching_sqlite::SqliteTaskStore::open(
                                &path,
                                cadence_persistence::matching_blob::BlobEncoding::ThriftRWSnappy,
                            )
                        } else {
                            cadence_persistence::matching_sqlite::SqliteTaskStore::create(
                                &path,
                                cadence_persistence::matching_blob::BlobEncoding::ThriftRWSnappy,
                            )
                        }?;
                        // Embedded development keeps its local History
                        // adapter; configured production uses the shared
                        // discovery-backed client prepared above.
                        let history = matching_history
                            .clone()
                            .expect("Matching history client is constructed when Matching runs");
                        match matching_domain_cache.clone() {
                            Some(cache) => cadence_server::matching_runtime::MatchingRuntime::with_durable_queues_and_domain_cache(
                                state.clone(),
                                std::sync::Arc::new(store),
                                history,
                                std::num::NonZeroU32::new(100000).expect("task id range"),
                                std::time::Duration::from_secs(60),
                                drain,
                                cache,
                            ).await?,
                            None => cadence_server::matching_runtime::MatchingRuntime::with_durable_queues(
                                state.clone(),
                                std::sync::Arc::new(store),
                                history,
                                std::num::NonZeroU32::new(100000).expect("task id range"),
                                std::time::Duration::from_secs(60),
                                drain,
                            ),
                        }
                    }
                    cadence_server::matching_task_store::MatchingTaskStoreSource::InMemory => match matching_domain_cache.clone() {
                        Some(cache) => cadence_server::matching_runtime::MatchingRuntime::in_memory_with_domain_cache(
                            state.clone(), drain, cache,
                        ).await?,
                        None => cadence_server::matching_runtime::MatchingRuntime::in_memory(
                            state.clone(), drain,
                        ),
                    },
                })
            } else {
                None
            };
            if dynamic_config_loaded {
                if let (Some(runtime), Some(path)) =
                    (matching_runtime.take(), dynamic_config_path.clone())
                {
                    matching_runtime = Some(
                        runtime.with_dynamic_config_file(
                            path,
                            dynamic_config_poll,
                            dynamic_config_loaded_at
                                .expect("successful initial dynamic-config load has timestamp"),
                        ),
                    );
                }
            }
            if let Some(membership) = matching_membership.clone() {
                if let Some(runtime) = matching_runtime.take() {
                    matching_runtime = Some(runtime.with_membership(membership));
                }
            }
            let matching_service = matching_runtime
                .as_ref()
                .map(|runtime| runtime.matching().clone())
                .unwrap_or_else(|| cadence_server::grpc_matching::Matching::new(state.clone()));
            let matching_tchannel_service = matching_service.clone();
            // The Ringpop JOIN handler must be reachable on the shared socket
            // before provider bootstrap starts. It is not advertised until the
            // provider has completed JOIN and published its service labels.
            let (mut matching_tchannel_task, mut matching_tchannel_done) = if run_matching {
                if let Some(listener) = matching_tchannel_listener.take() {
                    let provider = matching_ringpop_provider
                        .as_ref()
                        .expect("prebound production listener has a Ringpop provider");
                    let ringpop_handler = provider.handler();
                    let matching = matching_tchannel_service.clone();
                    let (done_tx, done_rx) = tokio::sync::oneshot::channel();
                    let task = tokio::spawn(async move {
                        let result =
                            cadence_server::tchannel::serve_matching_with_ringpop_listener(
                                listener,
                                matching,
                                ringpop_handler,
                            )
                            .await;
                        let _ = done_tx.send(result);
                    });
                    (Some(task), Some(done_rx))
                } else {
                    (None, None)
                }
            } else {
                (None, None)
            };
            let mut shutdown_during_start = false;
            if let Some(lifecycle) = matching_membership_lifecycle.as_mut() {
                let start_result = tokio::select! {
                    result = lifecycle.start() => result.map_err(anyhow::Error::new),
                    signal = shutdown_signal() => {
                        shutdown_during_start = signal.is_ok();
                        Err(signal.err().unwrap_or_else(|| anyhow::anyhow!("shutdown requested during Matching Ringpop bootstrap")))
                    },
                    result = async {
                        match matching_tchannel_done.as_mut() {
                            Some(listener_done) => listener_done.await,
                            None => std::future::pending().await,
                        }
                    } => Err(anyhow::anyhow!("shared Matching TChannel/Ringpop listener stopped during Ringpop bootstrap: {result:?}")),
                };
                if let Err(start_error) = start_result {
                    let mut cleanup_errors = Vec::new();
                    // Withdraw while the shared Ringpop listener is still
                    // accepting traffic, then drain and stop Matching.
                    if let Some(lifecycle) = matching_membership_lifecycle.as_mut() {
                        if let Err(error) = lifecycle.withdraw().await {
                            cleanup_errors.push(format!("withdrawing membership: {error}"));
                        }
                    }
                    if let Some(runtime) = matching_runtime.as_mut() {
                        if let Err(error) = runtime.shutdown().await {
                            cleanup_errors.push(format!("stopping Matching runtime: {error:#}"));
                        }
                    }
                    if let Some(lifecycle) = matching_membership_lifecycle.as_mut() {
                        if let Err(error) = lifecycle.stop_after_drain().await {
                            cleanup_errors.push(format!("stopping membership: {error}"));
                        }
                    }
                    if let Some(task) = matching_tchannel_task.take() {
                        task.abort();
                        if let Err(error) = task.await {
                            if !error.is_cancelled() {
                                cleanup_errors
                                    .push(format!("joining shared TChannel listener: {error}"));
                            }
                        }
                    }
                    if cleanup_errors.is_empty() {
                        if shutdown_during_start {
                            println!("shutting down");
                            return Ok(());
                        }
                        return Err(start_error.context("starting Matching Ringpop membership"));
                    }
                    anyhow::bail!(
                        "starting Matching Ringpop membership failed: {start_error:#}; cleanup also failed: {}",
                        cleanup_errors.join("; ")
                    );
                }
            }
            let app = if run_matching {
                if matching_http_addr.is_some() {
                    app
                } else {
                    let app = app.merge(cadence_server::thrift_matching::combined_router(
                        matching_service.clone(),
                    ));
                    if run_frontend {
                        app
                    } else {
                        app.merge(cadence_server::thrift_matching::router(
                            matching_service.clone(),
                        ))
                    }
                }
            } else {
                app
            };
            let mut serving = tokio::spawn(async move {
                tokio::try_join!(
                    async {
                        axum::serve(listener, app)
                            .await
                            .map_err(anyhow::Error::from)
                    },
                    async {
                        if run_frontend {
                            cadence_server::grpc::serve(grpc_addr, grpc_state).await
                        } else {
                            std::future::pending::<anyhow::Result<()>>().await
                        }
                    },
                    async {
                        if run_frontend {
                            cadence_server::tchannel::serve(tchannel_addr, tchannel_state).await
                        } else {
                            std::future::pending::<anyhow::Result<()>>().await
                        }
                    },
                    async {
                        if run_history {
                            cadence_server::grpc_internal::serve(history_addr, history_state).await
                        } else {
                            std::future::pending::<anyhow::Result<()>>().await
                        }
                    },
                    async {
                        if run_matching {
                            if let Some(done) = matching_tchannel_done.as_mut() {
                                match done.await {
                                    Ok(result) => result,
                                    Err(error) => Err(anyhow::Error::new(error)),
                                }
                            } else {
                                cadence_server::tchannel::serve_matching(
                                    matching_tchannel_addr,
                                    matching_tchannel_service,
                                )
                                .await
                            }
                        } else {
                            std::future::pending::<anyhow::Result<()>>().await
                        }
                    },
                    async {
                        if run_matching {
                            if let Some(listener) = matching_grpc_listener.take() {
                                cadence_server::grpc_matching::serve_with_listener(
                                    listener,
                                    matching_service.clone(),
                                )
                                .await
                            } else {
                                cadence_server::grpc_matching::serve_with(
                                    matching_addr,
                                    matching_service.clone(),
                                )
                                .await
                            }
                        } else {
                            std::future::pending::<anyhow::Result<()>>().await
                        }
                    },
                    async {
                        if run_matching {
                            if let Some(addr) = matching_http_addr {
                                let http = matching_rpc
                                    .as_ref()
                                    .and_then(|rpc| rpc.http.as_ref())
                                    .expect("configured Matching HTTP address");
                                let app = cadence_server::thrift_matching::router_with_procedures(
                                    matching_service.clone(),
                                    http.procedures.clone(),
                                );
                                let http = matching_rpc
                                    .as_ref()
                                    .and_then(|rpc| rpc.http.as_ref())
                                    .expect("configured Matching HTTP");
                                if http.tls_mode.as_deref() == Some("enforced")
                                    && http.tls.as_ref().is_some_and(|tls| tls.enabled)
                                {
                                    let tls_config = matching_http_tls_config
                                        .clone()
                                        .expect("validated enforced TLS configuration");
                                    axum_server::bind_rustls(addr, tls_config)
                                        .serve(app.into_make_service())
                                        .await
                                        .map_err(anyhow::Error::from)
                                } else if http.tls_mode.as_deref() == Some("permissive")
                                    && http.tls.as_ref().is_some_and(|tls| tls.enabled)
                                {
                                    let tls_config = matching_http_tls_config
                                        .clone()
                                        .expect("validated permissive TLS configuration");
                                    serve_matching_http_permissive(addr, app, tls_config).await
                                } else {
                                    let listener = tokio::net::TcpListener::bind(addr).await?;
                                    axum::serve(listener, app)
                                        .await
                                        .map_err(anyhow::Error::from)
                                }
                            } else {
                                std::future::pending::<anyhow::Result<()>>().await
                            }
                        } else {
                            std::future::pending::<anyhow::Result<()>>().await
                        }
                    },
                )?;
                anyhow::Ok(())
            });
            let (mut serving_result, signal_result) = tokio::select! {
                result = &mut serving => (
                    Some(result.map_err(anyhow::Error::from).and_then(|result| result)),
                    None,
                ),
                signal = shutdown_signal() => (None, Some(signal)),
            };
            // On a signal, the listener task remains active while shutdown
            // drains traffic. Listener errors have already ended that task.
            let withdrawal = if let Some(lifecycle) = matching_membership_lifecycle.as_mut() {
                lifecycle.withdraw().await.map_err(anyhow::Error::new)
            } else {
                Ok(())
            };
            let shutdown = if let Some(runtime) = matching_runtime.as_mut() {
                runtime.shutdown().await
            } else {
                Ok(())
            };
            // Runtime shutdown evicts before draining and stops Matching's
            // handler/store after the drain. Only then remove the membership
            // watcher and destroy the shared Ringpop node.
            let membership_shutdown =
                if let Some(lifecycle) = matching_membership_lifecycle.as_mut() {
                    lifecycle
                        .stop_after_drain()
                        .await
                        .map_err(anyhow::Error::new)
                } else {
                    Ok(())
                };
            if serving_result.is_none() {
                if serving.is_finished() {
                    serving_result = Some(
                        serving
                            .await
                            .map_err(anyhow::Error::from)
                            .and_then(|result| result),
                    );
                } else if signal_result.is_some() {
                    serving.abort();
                    let _ = serving.await;
                }
            }
            if let Some(task) = matching_tchannel_task.take() {
                task.abort();
                let _ = task.await;
            }
            let mut shutdown_errors = Vec::new();
            if let Some(Err(error)) = serving_result {
                shutdown_errors.push(format!("serving Matching cluster: {error:#}"));
            }
            if let Some(Err(error)) = signal_result {
                shutdown_errors.push(format!("waiting for shutdown signal: {error:#}"));
            }
            if let Err(error) = withdrawal {
                shutdown_errors.push(format!("withdrawing Matching membership: {error:#}"));
            }
            if let Err(error) = shutdown {
                shutdown_errors.push(format!("stopping Matching runtime: {error:#}"));
            }
            if let Err(error) = membership_shutdown {
                shutdown_errors.push(format!("stopping Matching membership: {error:#}"));
            }
            if !shutdown_errors.is_empty() {
                anyhow::bail!("Matching shutdown failed: {}", shutdown_errors.join("; "));
            }
            println!("shutting down");
        }
        Command::Version => {
            println!("cadence-server (rust) {}", cadence_api::API_COMPAT_VERSION);
        }
        Command::CheckConfig { config } => {
            let cfg = config::Config::from_file(&config)?;
            cfg.validate()?;
            println!("config OK: {}", config);
        }
    }
    Ok(())
}
