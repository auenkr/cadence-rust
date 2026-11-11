//! Outbound TLS for History-to-History gRPC forwarding.
//!
//! Go's History client factory dials peers with the history service's `rpc.tls`
//! client config; plaintext is kept when TLS is not configured.
use std::sync::Mutex;

use cadence_service_core::grpc_tls::ClientTls;

static HISTORY_CLIENT_TLS: Mutex<Option<ClientTls>> = Mutex::new(None);

/// Installs the client TLS config built from `services.history.rpc.tls`.
pub fn set_history_client_tls(tls: Option<ClientTls>) {
    *HISTORY_CLIENT_TLS.lock().unwrap() = tls;
}

/// The installed config (`None` means plaintext).
pub fn history_client_tls() -> Option<ClientTls> {
    HISTORY_CLIENT_TLS.lock().unwrap().clone()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_to_plaintext_and_can_be_cleared() {
        set_history_client_tls(None);
        assert!(history_client_tls().is_none());
    }
}
