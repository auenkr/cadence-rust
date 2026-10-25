//! Go-compatible `rpc.tls` for Matching's gRPC listener and gRPC peers.
//!
//! Go's `config.TLS.ToTLSConfig` produces one `crypto/tls` config per service
//! RPC block. The inbound config secures that service's gRPC listener, and
//! `rpc.NewParams` dials the History and Matching outbounds with the config
//! of the target service. This module mirrors both sides.

use std::{io, sync::Arc};

use anyhow::Context;
use hyper_util::rt::TokioIo;
use rustls::{
    client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
    crypto::CryptoProvider,
    pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime},
    ClientConfig, DigitallySignedStruct, RootCertStore, ServerConfig, SignatureScheme,
};
use tokio::net::TcpStream;
use tokio_rustls::{client::TlsStream, TlsConnector};
use tonic::{
    codegen::http::Uri,
    transport::{Certificate, Channel, Endpoint, Identity, Server, ServerTlsConfig},
};

use crate::config::TlsConfig;

/// Go `ToTLSConfig` for the inbound gRPC listener. Disabled TLS returns `None`,
/// which keeps the listener plaintext like Go's nil inbound credentials.
pub fn server_tls(tls: &TlsConfig) -> anyhow::Result<Option<ServerTlsConfig>> {
    if !tls.enabled {
        return Ok(None);
    }
    install_tls_crypto_provider();
    // Go reads the CA files while building the config, so a missing file fails startup.
    let ca_certs = ca_certs(tls)?;
    let (cert_pem, key_pem) = read_identity(tls)?;
    // Go's LoadX509KeyPair rejects a mismatched pair up front; so does this.
    let (certs, key) = parse_identity(&cert_pem, &key_pem)?;
    ServerConfig::builder_with_provider(provider())
        .with_safe_default_protocol_versions()?
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .context("rpc.tls certFile/keyFile pair is not valid")?;
    let mut server = ServerTlsConfig::new().identity(Identity::from_pem(cert_pem, key_pem));
    if tls.require_client_auth {
        // Go sets ClientCAs to the CA pool, or the system roots when no CA is configured.
        let ca_pem = der_to_pem(&ca_certs);
        server = server.client_ca_root(Certificate::from_pem(ca_pem));
    }
    Ok(Some(server))
}

/// Build rustls server TLS for Go-compatible cases tonic cannot represent:
/// either an absent server identity, or required client auth with an explicit
/// CA file/list that parsed to an empty certificate pool.
pub fn server_tls_custom_incoming(tls: &TlsConfig) -> anyhow::Result<Option<Arc<ServerConfig>>> {
    let explicit_ca = !tls.ca_file.is_empty() || !tls.ca_files.is_empty();
    let missing_identity = tls.cert_file.is_empty() || tls.key_file.is_empty();
    if !tls.enabled || (!missing_identity && !(tls.require_client_auth && explicit_ca)) {
        return Ok(None);
    }
    install_tls_crypto_provider();
    let ca_certs = ca_certs(tls)?;
    let mut roots = RootCertStore::empty();
    let mut ignored_invalid_ca = false;
    for cert in &ca_certs {
        if roots.add(cert.clone()).is_err() {
            ignored_invalid_ca = true;
        }
    }
    let empty_client_ca = tls.require_client_auth && roots.is_empty();
    let needs_compatible_ca_filter = tls.require_client_auth && ignored_invalid_ca;
    if !missing_identity && !empty_client_ca && !needs_compatible_ca_filter {
        return Ok(None);
    }
    let builder =
        ServerConfig::builder_with_provider(provider()).with_safe_default_protocol_versions()?;
    let builder = if tls.require_client_auth {
        let verifier: Arc<dyn rustls::server::danger::ClientCertVerifier> = if roots.is_empty() {
            Arc::new(EmptyClientCaPool)
        } else {
            rustls::server::WebPkiClientVerifier::builder(Arc::new(roots)).build()?
        };
        builder.with_client_cert_verifier(verifier)
    } else {
        builder.with_no_client_auth()
    };
    let mut server = if missing_identity {
        builder.with_cert_resolver(Arc::new(NoServerCertificate))
    } else {
        let (cert_pem, key_pem) = read_identity(tls)?;
        let (certs, key) = parse_identity(&cert_pem, &key_pem)?;
        builder
            .with_single_cert(certs, key)
            .context("rpc.tls certFile/keyFile pair is not valid")?
    };
    server.alpn_protocols = vec![b"h2".to_vec()];
    Ok(Some(Arc::new(server)))
}

#[derive(Debug)]
struct NoServerCertificate;
impl rustls::server::ResolvesServerCert for NoServerCertificate {
    fn resolve(
        &self,
        _: rustls::server::ClientHello<'_>,
    ) -> Option<Arc<rustls::sign::CertifiedKey>> {
        None
    }
}

#[derive(Debug)]
struct EmptyClientCaPool;
impl rustls::server::danger::ClientCertVerifier for EmptyClientCaPool {
    fn root_hint_subjects(&self) -> &[rustls::DistinguishedName] {
        &[]
    }
    fn verify_client_cert(
        &self,
        _: &CertificateDer<'_>,
        _: &[CertificateDer<'_>],
        _: UnixTime,
    ) -> Result<rustls::server::danger::ClientCertVerified, rustls::Error> {
        Err(rustls::Error::InvalidCertificate(
            rustls::CertificateError::UnknownIssuer,
        ))
    }
    fn verify_tls12_signature(
        &self,
        _: &[u8],
        _: &CertificateDer<'_>,
        _: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Err(rustls::Error::InvalidCertificate(
            rustls::CertificateError::UnknownIssuer,
        ))
    }
    fn verify_tls13_signature(
        &self,
        m: &[u8],
        c: &CertificateDer<'_>,
        s: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.verify_tls12_signature(m, c, s)
    }
    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

/// Outbound TLS for a gRPC peer, built from the target service's `rpc.tls`.
#[derive(Clone)]
pub struct ClientTls {
    config: Arc<ClientConfig>,
    /// Go's `tlsConfig.ServerName`; empty means "use the dialed host".
    server_name: Option<String>,
}

/// Go `ToTLSConfig` for a client dialing a gRPC peer. Disabled TLS returns `None`.
pub fn client_tls(tls: &TlsConfig) -> anyhow::Result<Option<ClientTls>> {
    if !tls.enabled {
        return Ok(None);
    }
    install_tls_crypto_provider();
    let provider = provider();
    let ca_certs = ca_certs(tls)?;
    let builder = ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()?;
    let builder = if tls.enable_host_verification {
        let mut roots = RootCertStore::empty();
        for cert in ca_certs {
            // Go's AppendCertsFromPEM skips certificates it cannot parse.
            let _ = roots.add(cert);
        }
        builder.with_root_certificates(roots)
    } else {
        // Go's InsecureSkipVerify: the chain and name are not checked, but the
        // handshake signature still is.
        builder
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(SkipChainVerification {
                provider: provider.clone(),
            }))
    };
    let mut config = if tls.cert_file.is_empty() || tls.key_file.is_empty() {
        builder.with_no_client_auth()
    } else {
        let (cert_pem, key_pem) = read_identity(tls)?;
        let (certs, key) = parse_identity(&cert_pem, &key_pem)?;
        builder
            .with_client_auth_cert(certs, key)
            .context("rpc.tls certFile/keyFile pair is not valid")?
    };
    // grpc-go negotiates HTTP/2 over ALPN, and tonic's server requires it.
    config.alpn_protocols = vec![b"h2".to_vec()];
    Ok(Some(ClientTls {
        config: Arc::new(config),
        server_name: (!tls.server_name.is_empty()).then(|| tls.server_name.clone()),
    }))
}

/// Build a lazy gRPC channel to `address`, using TLS when `tls` is set.
/// Matches `Endpoint::from_shared(...).connect_lazy()` when TLS is off.
pub fn endpoint_channel(
    address: &str,
    tls: Option<&ClientTls>,
) -> Result<Channel, tonic::transport::Error> {
    let uri = if address.contains("://") {
        address.to_owned()
    } else {
        format!("http://{address}")
    };
    let endpoint = Endpoint::from_shared(uri)?;
    Ok(match tls {
        None => endpoint.connect_lazy(),
        Some(tls) => {
            let tls = tls.clone();
            endpoint.connect_with_connector_lazy(tower::service_fn(move |uri| {
                let tls = tls.clone();
                async move { tls.connect(&uri).await }
            }))
        }
    })
}

/// Build the tonic server, applying TLS when configured.
pub fn server_builder(tls: Option<ServerTlsConfig>) -> anyhow::Result<Server> {
    let server = Server::builder();
    Ok(match tls {
        Some(tls) => server.tls_config(tls)?,
        None => server,
    })
}

impl ClientTls {
    async fn connect(&self, uri: &Uri) -> io::Result<TokioIo<TlsStream<TcpStream>>> {
        let host = uri
            .host()
            .ok_or_else(|| io::Error::other("gRPC peer address has no host"))?
            .trim_start_matches('[')
            .trim_end_matches(']');
        let port = uri.port_u16().unwrap_or(443);
        let name = self.server_name.as_deref().unwrap_or(host);
        let server_name = ServerName::try_from(name.to_owned()).map_err(io::Error::other)?;
        let tcp = TcpStream::connect((host, port)).await?;
        let stream = TlsConnector::from(self.config.clone())
            .connect(server_name, tcp)
            .await?;
        Ok(TokioIo::new(stream))
    }
}

/// Go's CA pool: `caFiles` plus `caFile`. With no CA file Go leaves the pool
/// nil, which means the host's system roots.
fn ca_certs(tls: &TlsConfig) -> anyhow::Result<Vec<CertificateDer<'static>>> {
    let mut files: Vec<&str> = tls.ca_files.iter().map(String::as_str).collect();
    if !tls.ca_file.is_empty() {
        files.push(&tls.ca_file);
    }
    if files.is_empty() {
        return Ok(rustls_native_certs::load_native_certs().certs);
    }
    let mut certs = Vec::new();
    for file in files {
        let pem = std::fs::read(file).with_context(|| format!("reading CA file {file}"))?;
        // Go's AppendCertsFromPEM skips blocks it cannot parse.
        certs.extend(rustls_pemfile::certs(&mut pem.as_slice()).filter_map(Result::ok));
    }
    Ok(certs)
}

fn read_identity(tls: &TlsConfig) -> anyhow::Result<(Vec<u8>, Vec<u8>)> {
    if tls.cert_file.is_empty() || tls.key_file.is_empty() {
        anyhow::bail!("rpc.tls requires both certFile and keyFile for a server certificate");
    }
    let cert = std::fs::read(&tls.cert_file)
        .with_context(|| format!("reading TLS certFile {}", tls.cert_file))?;
    let key = std::fs::read(&tls.key_file)
        .with_context(|| format!("reading TLS keyFile {}", tls.key_file))?;
    Ok((cert, key))
}

fn parse_identity(
    cert_pem: &[u8],
    key_pem: &[u8],
) -> anyhow::Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)> {
    let certs = rustls_pemfile::certs(&mut &*cert_pem)
        .collect::<Result<Vec<_>, _>>()
        .context("parsing TLS certFile")?;
    let key = rustls_pemfile::private_key(&mut &*key_pem)
        .context("parsing TLS keyFile")?
        .context("TLS keyFile contains no private key")?;
    Ok((certs, key))
}

fn der_to_pem(certs: &[CertificateDer<'_>]) -> Vec<u8> {
    use base64::{engine::general_purpose::STANDARD, Engine};
    let mut pem = String::new();
    for cert in certs {
        pem.push_str("-----BEGIN CERTIFICATE-----\n");
        for line in STANDARD.encode(cert).as_bytes().chunks(64) {
            pem.push_str(&String::from_utf8_lossy(line));
            pem.push('\n');
        }
        pem.push_str("-----END CERTIFICATE-----\n");
    }
    pem.into_bytes()
}

fn provider() -> Arc<CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

/// Go `InsecureSkipVerify` for peer dials: accept any server chain and name,
/// but keep verifying the handshake signature like Go does.
#[derive(Debug)]
struct SkipChainVerification {
    provider: Arc<CryptoProvider>,
}

impl ServerCertVerifier for SkipChainVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

pub fn install_tls_crypto_provider() {
    // Err means a provider is already installed, which is fine.
    let _ = rustls::crypto::ring::default_provider().install_default();
}

#[cfg(test)]
mod tests {
    use std::{net::SocketAddr, path::Path, time::Duration};

    use tonic_reflection::pb::v1::{
        server_reflection_client::ServerReflectionClient,
        server_reflection_request::MessageRequest, ServerReflectionRequest,
    };

    use super::*;
    use crate::config::RpcConfig;

    struct Ca {
        cert: rcgen::Certificate,
        key: rcgen::KeyPair,
    }

    fn new_ca() -> Ca {
        let mut params = rcgen::CertificateParams::new(Vec::new()).unwrap();
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let key = rcgen::KeyPair::generate().unwrap();
        let cert = params.self_signed(&key).unwrap();
        Ca { cert, key }
    }

    /// Returns `(certificate PEM, private key PEM)` for `names`, signed by `ca`.
    fn issue(ca: &Ca, names: &[&str]) -> (String, String) {
        let params = rcgen::CertificateParams::new(
            names
                .iter()
                .map(|name| name.to_string())
                .collect::<Vec<_>>(),
        )
        .unwrap();
        let key = rcgen::KeyPair::generate().unwrap();
        let cert = params.signed_by(&key, &ca.cert, &ca.key).unwrap();
        (cert.pem(), key.serialize_pem())
    }

    fn write(dir: &Path, name: &str, contents: &str) -> String {
        let path = dir.join(name);
        std::fs::write(&path, contents).unwrap();
        path.to_string_lossy().into_owned()
    }

    async fn serve_reflection(tls: Option<ServerTlsConfig>) -> SocketAddr {
        let reflection = tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(cadence_proto::FILE_DESCRIPTOR_SET)
            .build_v1()
            .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = server_builder(tls).unwrap().add_service(reflection);
        tokio::spawn(
            server.serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
        );
        addr
    }

    /// A real gRPC round trip; a handshake or client-auth failure surfaces as `Err`.
    async fn call(addr: SocketAddr, tls: Option<&ClientTls>) -> Result<(), String> {
        let exchange = async {
            let channel = endpoint_channel(&addr.to_string(), tls).map_err(|e| e.to_string())?;
            let mut client = ServerReflectionClient::new(channel);
            let request = ServerReflectionRequest {
                host: String::new(),
                message_request: Some(MessageRequest::ListServices(String::new())),
            };
            let mut stream = client
                .server_reflection_info(tokio_stream::once(request))
                .await
                .map_err(|status| status.to_string())?
                .into_inner();
            match stream
                .message()
                .await
                .map_err(|status| status.to_string())?
            {
                Some(_) => Ok(()),
                None => Err("reflection stream closed".to_owned()),
            }
        };
        tokio::time::timeout(Duration::from_secs(10), exchange)
            .await
            .map_err(|_| "gRPC call timed out".to_owned())?
    }

    fn server_config(
        dir: &Path,
        ca_file: &str,
        identity: &(String, String),
        client_auth: bool,
    ) -> TlsConfig {
        TlsConfig {
            enabled: true,
            cert_file: write(dir, "server-cert.pem", &identity.0),
            key_file: write(dir, "server-key.pem", &identity.1),
            ca_file: ca_file.to_owned(),
            require_client_auth: client_auth,
            ..Default::default()
        }
    }

    #[test]
    fn rpc_tls_parses_go_yaml_names() {
        let rpc: RpcConfig = serde_yaml::from_str(
            "tls:\n  enabled: true\n  certFile: /c.pem\n  keyFile: /k.pem\n  caFiles: [/ca1.pem, /ca2.pem]\n  enableHostVerification: true\n  requireClientAuth: true\n  serverName: matching.internal\n",
        )
        .unwrap();
        assert!(rpc.tls.enabled);
        assert_eq!(rpc.tls.cert_file, "/c.pem");
        assert_eq!(rpc.tls.key_file, "/k.pem");
        assert_eq!(rpc.tls.ca_files, vec!["/ca1.pem", "/ca2.pem"]);
        assert!(rpc.tls.enable_host_verification);
        assert!(rpc.tls.require_client_auth);
        assert_eq!(rpc.tls.server_name, "matching.internal");
        let absent: RpcConfig = serde_yaml::from_str("port: 7835").unwrap();
        assert_eq!(absent.tls, TlsConfig::default());
    }

    #[test]
    fn disabled_rpc_tls_stays_plaintext() {
        let disabled = TlsConfig {
            cert_file: "/does/not/exist.pem".into(),
            ..Default::default()
        };
        assert!(server_tls(&disabled).unwrap().is_none());
        assert!(client_tls(&disabled).unwrap().is_none());
    }

    #[test]
    fn server_tls_requires_a_matching_certificate_and_key() {
        let dir = tempfile::tempdir().unwrap();
        let ca = new_ca();
        let (cert, _) = issue(&ca, &["localhost"]);
        let (_, other_key) = issue(&ca, &["localhost"]);
        let mismatched = TlsConfig {
            enabled: true,
            cert_file: write(dir.path(), "cert.pem", &cert),
            key_file: write(dir.path(), "other-key.pem", &other_key),
            ..Default::default()
        };
        assert!(server_tls(&mismatched).is_err());
        let malformed = TlsConfig {
            enabled: true,
            cert_file: write(dir.path(), "malformed-cert.pem", "not a certificate"),
            key_file: write(dir.path(), "malformed-key.pem", "not a private key"),
            ..Default::default()
        };
        assert!(server_tls(&malformed).is_err());
        let unreadable_pair = TlsConfig {
            enabled: true,
            cert_file: "/missing/server-cert.pem".into(),
            key_file: "/missing/server-key.pem".into(),
            ..Default::default()
        };
        assert!(server_tls(&unreadable_pair).is_err());
        let missing = TlsConfig {
            enabled: true,
            ..Default::default()
        };
        assert!(server_tls(&missing).is_err());
    }

    #[test]
    fn missing_server_identity_keeps_tls_enabled_without_a_certificate() {
        let dir = tempfile::tempdir().unwrap();
        let invalid_ca = dir.path().join("invalid-ca.pem");
        std::fs::write(&invalid_ca, b"not a PEM certificate").unwrap();
        let valid_ca = new_ca();
        let valid_ca_file = write(dir.path(), "valid-ca.pem", &valid_ca.cert.pem());
        for config in [
            TlsConfig {
                enabled: true,
                ..Default::default()
            },
            TlsConfig {
                enabled: true,
                cert_file: "/ignored/cert.pem".into(),
                ..Default::default()
            },
            TlsConfig {
                enabled: true,
                key_file: "/ignored/key.pem".into(),
                ..Default::default()
            },
            TlsConfig {
                enabled: true,
                ca_file: invalid_ca.to_string_lossy().into_owned(),
                require_client_auth: true,
                ..Default::default()
            },
            TlsConfig {
                enabled: true,
                ca_file: valid_ca_file,
                require_client_auth: true,
                ..Default::default()
            },
        ] {
            let server = server_tls_custom_incoming(&config).unwrap().unwrap();
            assert_eq!(server.alpn_protocols, vec![b"h2".to_vec()]);
        }
        let missing_ca = TlsConfig {
            enabled: true,
            ca_file: "/missing/cadence-ca.pem".into(),
            ..Default::default()
        };
        assert!(server_tls_custom_incoming(&missing_ca).is_err());
    }

    #[tokio::test]
    async fn mutual_tls_admits_only_clients_signed_by_the_configured_ca() {
        install_tls_crypto_provider();
        let dir = tempfile::tempdir().unwrap();
        let ca = new_ca();
        let other = new_ca();
        let server_identity = issue(&ca, &["localhost", "127.0.0.1"]);
        let client_identity = issue(&ca, &["client"]);
        let rogue_identity = issue(&other, &["client"]);
        let ca_file = write(dir.path(), "ca.pem", &ca.cert.pem());
        let server = server_tls(&server_config(dir.path(), &ca_file, &server_identity, true))
            .unwrap()
            .unwrap();
        let addr = serve_reflection(Some(server)).await;

        let client = |identity: Option<&(String, String)>| {
            let (cert_file, key_file) = identity.map_or((String::new(), String::new()), |id| {
                (
                    write(dir.path(), "client-cert.pem", &id.0),
                    write(dir.path(), "client-key.pem", &id.1),
                )
            });
            client_tls(&TlsConfig {
                enabled: true,
                cert_file,
                key_file,
                ca_file: ca_file.clone(),
                enable_host_verification: true,
                ..Default::default()
            })
            .unwrap()
            .unwrap()
        };

        assert_eq!(
            call(addr, Some(&client(Some(&client_identity)))).await,
            Ok(())
        );
        assert!(call(addr, Some(&client(None))).await.is_err());
        assert!(call(addr, Some(&client(Some(&rogue_identity))))
            .await
            .is_err());
    }

    #[tokio::test]
    async fn server_ca_and_client_auth_matrix_matches_go_startup_and_handshakes() {
        install_tls_crypto_provider();
        let dir = tempfile::tempdir().unwrap();
        let ca = new_ca();
        let server_identity = issue(&ca, &["localhost", "127.0.0.1"]);
        let ca_file = write(dir.path(), "matrix-ca.pem", &ca.cert.pem());
        let empty_ca_file = write(dir.path(), "matrix-empty-ca.pem", "not PEM");

        for ca_file in [None, Some(empty_ca_file.as_str()), Some(ca_file.as_str())] {
            let config = TlsConfig {
                enabled: true,
                cert_file: write(dir.path(), "matrix-server-cert.pem", &server_identity.0),
                key_file: write(dir.path(), "matrix-server-key.pem", &server_identity.1),
                ca_file: ca_file.unwrap_or_default().to_owned(),
                ..Default::default()
            };
            let server = server_tls(&config).unwrap().unwrap();
            let addr = serve_reflection(Some(server)).await;
            let peer = client_tls(&TlsConfig {
                enabled: true,
                ..Default::default()
            })
            .unwrap()
            .unwrap();
            assert_eq!(call(addr, Some(&peer)).await, Ok(()));
        }

        // Without an explicit CA Go uses system roots; with an invalid PEM CA
        // Go keeps an empty pool. Both start successfully and reject a client
        // that presents no certificate. The valid-CA case (accept, reject
        // missing and rogue clients) is covered by the test above.
        for ca_file in [None, Some(empty_ca_file.as_str())] {
            let config = TlsConfig {
                enabled: true,
                cert_file: write(dir.path(), "auth-server-cert.pem", &server_identity.0),
                key_file: write(dir.path(), "auth-server-key.pem", &server_identity.1),
                ca_file: ca_file.unwrap_or_default().to_owned(),
                require_client_auth: true,
                ..Default::default()
            };
            if ca_file.is_some() {
                assert!(server_tls_custom_incoming(&config).unwrap().is_some());
                continue;
            }
            let server = server_tls(&config).unwrap().unwrap();
            let addr = serve_reflection(Some(server)).await;
            let peer = client_tls(&TlsConfig {
                enabled: true,
                ..Default::default()
            })
            .unwrap()
            .unwrap();
            assert!(call(addr, Some(&peer)).await.is_err());
        }
    }

    #[tokio::test]
    async fn host_verification_checks_chain_and_server_name() {
        install_tls_crypto_provider();
        let dir = tempfile::tempdir().unwrap();
        let ca = new_ca();
        let other = new_ca();
        let server_identity = issue(&ca, &["localhost", "127.0.0.1"]);
        let ca_file = write(dir.path(), "ca.pem", &ca.cert.pem());
        let other_ca_file = write(dir.path(), "other-ca.pem", &other.cert.pem());
        let server = server_tls(&server_config(
            dir.path(),
            &ca_file,
            &server_identity,
            false,
        ))
        .unwrap()
        .unwrap();
        let addr = serve_reflection(Some(server)).await;

        let verified = |ca_file: &str, server_name: &str, enable_host_verification: bool| {
            client_tls(&TlsConfig {
                enabled: true,
                ca_file: ca_file.to_owned(),
                server_name: server_name.to_owned(),
                enable_host_verification,
                ..Default::default()
            })
            .unwrap()
            .unwrap()
        };

        // The IP address is covered by the certificate's SAN.
        assert_eq!(
            call(addr, Some(&verified(&ca_file, "", true))).await,
            Ok(())
        );
        assert_eq!(
            call(addr, Some(&verified(&ca_file, "localhost", true))).await,
            Ok(())
        );
        assert!(call(addr, Some(&verified(&ca_file, "wrong.example", true)))
            .await
            .is_err());
        assert!(call(addr, Some(&verified(&other_ca_file, "", true)))
            .await
            .is_err());
        // Go's InsecureSkipVerify accepts the same server with an untrusted CA and wrong name.
        assert_eq!(
            call(
                addr,
                Some(&verified(&other_ca_file, "wrong.example", false))
            )
            .await,
            Ok(())
        );
    }

    #[test]
    fn lone_client_cert_file_is_not_loaded_like_go() {
        // Go ToTLSConfig loads the client pair only when certFile and keyFile are both set,
        // so a lone certFile is never read and the config builds.
        let one_sided = TlsConfig {
            enabled: true,
            cert_file: "/does/not/exist-cert.pem".into(),
            ..Default::default()
        };
        assert!(client_tls(&one_sided).unwrap().is_some());
    }

    #[test]
    fn missing_ca_file_fails_tls_load_like_go() {
        // Go ToTLSConfig returns the os.ReadFile error for the CA list on both sides,
        // which aborts startup. The identity is valid so only the CA read can fail.
        let dir = tempfile::tempdir().unwrap();
        let ca = new_ca();
        let (cert, key) = issue(&ca, &["localhost"]);
        let missing_ca = TlsConfig {
            enabled: true,
            cert_file: write(dir.path(), "cert.pem", &cert),
            key_file: write(dir.path(), "key.pem", &key),
            ca_file: "/does/not/exist-ca.pem".into(),
            ..Default::default()
        };
        assert!(server_tls(&missing_ca).is_err());
        assert!(client_tls(&missing_ca).is_err());
    }

    #[tokio::test]
    async fn ca_files_and_ca_file_are_both_trusted() {
        // Go appends caFile to caFiles and builds one pool from both entries.
        install_tls_crypto_provider();
        let dir = tempfile::tempdir().unwrap();
        let listed = new_ca();
        let single = new_ca();
        let listed_identity = issue(&listed, &["localhost", "127.0.0.1"]);
        let single_identity = issue(&single, &["localhost", "127.0.0.1"]);
        let listed_file = write(dir.path(), "listed-ca.pem", &listed.cert.pem());
        let single_file = write(dir.path(), "single-ca.pem", &single.cert.pem());
        let listed_server = server_tls(&server_config(
            dir.path(),
            &listed_file,
            &listed_identity,
            false,
        ))
        .unwrap()
        .unwrap();
        let single_server = server_tls(&server_config(
            dir.path(),
            &single_file,
            &single_identity,
            false,
        ))
        .unwrap()
        .unwrap();
        let listed_addr = serve_reflection(Some(listed_server)).await;
        let single_addr = serve_reflection(Some(single_server)).await;

        let client = client_tls(&TlsConfig {
            enabled: true,
            ca_files: vec![listed_file],
            ca_file: single_file,
            enable_host_verification: true,
            ..Default::default()
        })
        .unwrap()
        .unwrap();
        assert_eq!(call(listed_addr, Some(&client)).await, Ok(()));
        assert_eq!(call(single_addr, Some(&client)).await, Ok(()));
    }
}
