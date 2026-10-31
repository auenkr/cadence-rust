//! Generated outbound gRPC client for the original Go History service.
use cadence_proto::history::history_api_client::HistoryApiClient;
use cadence_service_core::{config::TlsConfig, grpc_tls::{self, ClientTls}};
use std::time::Duration;
use tonic::transport::Channel;

#[derive(Clone)]
pub struct HistoryClient {
    channel: Channel,
    client: HistoryApiClient<Channel>,
    tls: Option<ClientTls>,
}

impl HistoryClient {
    pub fn from_channel(channel: Channel, max_message_size: usize) -> Self {
        let client = HistoryApiClient::new(channel.clone())
            .max_decoding_message_size(max_message_size)
            .max_encoding_message_size(max_message_size);
        Self { channel, client, tls: None }
    }

    pub fn client(&self) -> HistoryApiClient<Channel> { self.client.clone() }
    pub fn channel(&self) -> Channel { self.channel.clone() }

    /// Dial a discovered History peer using the same TLS config loader used by
    /// Matching's native History client (mTLS, CA roots and server-name rules).
    pub fn for_peer(peer: &str, tls: Option<&TlsConfig>, max_message_size: usize) -> anyhow::Result<Self> {
        let tls = tls.map(grpc_tls::client_tls).transpose()?;
        let channel = grpc_tls::endpoint_channel(peer, tls.as_ref())?;
        let mut client = Self::from_channel(channel, max_message_size);
        client.tls = tls;
        Ok(client)
    }

    pub fn tls(&self) -> Option<&ClientTls> { self.tls.as_ref() }
}

/// Build a lazy gRPC channel using the configured Go `services.history.rpc`
/// address. Connection establishment happens on the first RPC, matching the
/// Go client factory's lazy dialing behavior.
pub fn connect(host: &str, port: u16) -> anyhow::Result<tonic::transport::Channel> {
    let endpoint = format!("http://{host}:{port}")
        .parse::<tonic::transport::Endpoint>()?
        .connect_timeout(Duration::from_secs(10));
    Ok(endpoint.connect_lazy())
}

pub fn generated(channel: Channel, max_message_size: usize) -> HistoryApiClient<Channel> {
    HistoryApiClient::new(channel)
        .max_decoding_message_size(max_message_size)
        .max_encoding_message_size(max_message_size)
}
