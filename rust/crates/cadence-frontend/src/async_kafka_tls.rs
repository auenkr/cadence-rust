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

/// Go `tls.Config.ServerName`: certificate names are verified against the
/// configured name instead of the dialed broker host.
#[derive(Debug)]
struct ServerNameOverride {
    inner: Arc<dyn ServerCertVerifier>,
    name: ServerName<'static>,
}

impl ServerCertVerifier for ServerNameOverride {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        _: &ServerName<'_>,
        ocsp: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, Error> {
        self.inner
            .verify_server_cert(end_entity, intermediates, &self.name, ocsp, now)
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        self.inner.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        self.inner.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.inner.supported_verify_schemes()
    }
}

/// Wraps `inner` so verification uses `server_name` when it is non-empty.
fn with_server_name(
    inner: Arc<dyn ServerCertVerifier>,
    server_name: &str,
) -> anyhow::Result<Arc<dyn ServerCertVerifier>> {
    if server_name.is_empty() {
        return Ok(inner);
    }
    let name = ServerName::try_from(server_name.to_string())
        .map_err(|e| anyhow::anyhow!("invalid tls serverName {server_name}: {e}"))?;
    Ok(Arc::new(ServerNameOverride { inner, name }))
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
        if tls.server_name.is_empty() {
            builder.with_root_certificates(roots)
        } else {
            let webpki = rustls::client::WebPkiServerVerifier::builder_with_provider(
                Arc::new(roots),
                provider,
            )
            .build()?;
            builder
                .dangerous()
                .with_custom_certificate_verifier(with_server_name(webpki, &tls.server_name)?)
        }
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
    fn server_name_override_is_applied_to_verification() {
        #[derive(Debug)]
        struct Capture(std::sync::Mutex<Option<String>>);
        impl ServerCertVerifier for Capture {
            fn verify_server_cert(
                &self,
                _: &CertificateDer<'_>,
                _: &[CertificateDer<'_>],
                n: &ServerName<'_>,
                _: &[u8],
                _: UnixTime,
            ) -> Result<ServerCertVerified, Error> {
                *self.0.lock().unwrap() = Some(n.to_str().to_string());
                Ok(ServerCertVerified::assertion())
            }
            fn verify_tls12_signature(
                &self,
                _: &[u8],
                _: &CertificateDer<'_>,
                _: &DigitallySignedStruct,
            ) -> Result<HandshakeSignatureValid, Error> {
                Ok(HandshakeSignatureValid::assertion())
            }
            fn verify_tls13_signature(
                &self,
                _: &[u8],
                _: &CertificateDer<'_>,
                _: &DigitallySignedStruct,
            ) -> Result<HandshakeSignatureValid, Error> {
                Ok(HandshakeSignatureValid::assertion())
            }
            fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
                vec![]
            }
        }
        let capture = Arc::new(Capture(Default::default()));
        let v = with_server_name(capture.clone(), "kafka.internal").unwrap();
        let cert = CertificateDer::from(vec![0u8]);
        let dialed = ServerName::try_from("10.0.0.5").unwrap();
        v.verify_server_cert(&cert, &[], &dialed, &[], UnixTime::now())
            .unwrap();
        assert_eq!(
            capture.0.lock().unwrap().as_deref(),
            Some("kafka.internal")
        );
        // Empty name leaves the dialed host untouched.
        let v = with_server_name(capture.clone(), "").unwrap();
        v.verify_server_cert(&cert, &[], &dialed, &[], UnixTime::now())
            .unwrap();
        assert_eq!(capture.0.lock().unwrap().as_deref(), Some("10.0.0.5"));
        let with_name = TlsConfig {
            enabled: true,
            enable_host_verification: true,
            server_name: "kafka.internal".into(),
            ..Default::default()
        };
        assert!(client_config(&with_name).unwrap().is_some());
    }

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
