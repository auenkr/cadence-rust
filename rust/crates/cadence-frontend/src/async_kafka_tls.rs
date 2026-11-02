//! Go `config.TLS.ToTLSConfig` translated to a rustls client config for Kafka.

use crate::async_kafka::TlsConfig;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{ring as ring_provider, CryptoProvider};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::{ClientConfig, DigitallySignedStruct, Error, RootCertStore, SignatureScheme};
use std::sync::Arc;

/// Go `InsecureSkipVerify: !EnableHostVerification`.
#[derive(Debug)]
struct SkipVerify(Arc<CryptoProvider>);

impl ServerCertVerifier for SkipVerify {
    fn verify_server_cert(
        &self,
        _: &CertificateDer<'_>,
        _: &[CertificateDer<'_>],
        _: &ServerName<'_>,
        _: &[u8],
        _: UnixTime,
    ) -> Result<ServerCertVerified, Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

/// `None` when TLS is disabled (Go returns a nil tls.Config).
pub fn client_config(tls: &TlsConfig) -> anyhow::Result<Option<Arc<ClientConfig>>> {
    if !tls.enabled {
        return Ok(None);
    }
    let provider = Arc::new(ring_provider::default_provider());
    let mut ca_files = tls.ca_files.clone();
    if !tls.ca_file.is_empty() {
        ca_files.push(tls.ca_file.clone());
    }
    let mut roots = RootCertStore::empty();
    if ca_files.is_empty() {
        let native = rustls_native_certs::load_native_certs();
        for cert in native.certs {
            let _ = roots.add(cert);
        }
    } else {
        for file in &ca_files {
            let pem = std::fs::read(file)?;
            for cert in rustls_pemfile::certs(&mut pem.as_slice()) {
                roots.add(cert?)?;
            }
        }
    }
    let builder = ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()?;
    let builder = if tls.enable_host_verification {
        builder.with_root_certificates(roots)
    } else {
        builder
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(SkipVerify(provider)))
    };
    let config = if !tls.cert_file.is_empty() && !tls.key_file.is_empty() {
        let certs = rustls_pemfile::certs(&mut std::fs::read(&tls.cert_file)?.as_slice())
            .collect::<Result<Vec<_>, _>>()?;
        let key: PrivateKeyDer<'static> =
            rustls_pemfile::private_key(&mut std::fs::read(&tls.key_file)?.as_slice())?
                .ok_or_else(|| anyhow::anyhow!("no private key in {}", tls.key_file))?;
        builder.with_client_auth_cert(certs, key)?
    } else {
        builder.with_no_client_auth()
    };
    Ok(Some(Arc::new(config)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_tls_yields_none_and_enabled_builds_config() {
        assert!(client_config(&TlsConfig::default()).unwrap().is_none());
        let enabled = TlsConfig {
            enabled: true,
            ..Default::default()
        };
        assert!(client_config(&enabled).unwrap().is_some());
    }

    #[test]
    fn missing_ca_file_is_an_error() {
        let bad = TlsConfig {
            enabled: true,
            ca_file: "/nonexistent/ca.pem".into(),
            ..Default::default()
        };
        assert!(client_config(&bad).is_err());
    }
}
