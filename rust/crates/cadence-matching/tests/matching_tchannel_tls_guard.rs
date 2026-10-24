//! Go has no TLS on the TChannel listener. Go's `rpc.tls` secures only the gRPC
//! inbound (`grpc.InboundCredentials`) and dials only gRPC outbounds; the HTTP
//! listener has its own `rpc.http.tls`, and the TChannel transport in
//! `go/common/rpc/factory.go` is built with `tchannel.ListenAddr` and no
//! credentials. The Rust TChannel listener must therefore never be wrapped in
//! TLS, and must accept plaintext regardless of `rpc.tls`.
//!
//! Source guard in the style of `matching_dyncfg_defaults_tests.rs`: the Rust
//! TChannel serving code must not reference TLS, and the launcher must bind the
//! Matching TChannel port with a plain `TcpListener`.

const TCHANNEL_SRC: &str = include_str!("../src/tchannel.rs");
const LAUNCHER_SRC: &str = include_str!("../src/launcher.rs");

#[test]
fn tchannel_serving_code_has_no_tls_path() {
    let lower = TCHANNEL_SRC.to_ascii_lowercase();
    assert!(
        !lower.contains("tls") && !lower.contains("rustls"),
        "tchannel.rs must not reference TLS; Go's TChannel listener is plaintext"
    );
}

#[test]
fn matching_tchannel_listener_is_plaintext_tcp() {
    assert!(
        LAUNCHER_SRC.contains("tokio::net::TcpListener::bind(matching_tchannel_addr)"),
        "Matching TChannel must bind a plain TCP listener, as Go's ListenAddr does"
    );
    assert!(
        !LAUNCHER_SRC.contains("tchannel::serve_matching_tls")
            && !LAUNCHER_SRC.contains("tchannel::serve_tls"),
        "no TLS-wrapped TChannel serve entry point may exist"
    );
}
