//! Cassandra driver session setup shared by Matching's persistence adapters.
//!
//! The driver is asynchronous. Consumers must await its APIs from async
//! service paths; they must not call a nested runtime or synchronously wait on
//! driver futures from Tokio workers.

use scylla::client::{session::Session, session_builder::SessionBuilder};

#[derive(Debug, Clone)]
pub struct CassandraSessionConfig {
    pub hosts: Vec<String>,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub keyspace: String,
}

impl CassandraSessionConfig {
    pub fn from_parts(
        hosts: impl IntoIterator<Item = String>,
        port: i32,
        user: impl Into<String>,
        password: impl Into<String>,
        keyspace: impl Into<String>,
    ) -> anyhow::Result<Self> {
        let hosts: Vec<String> = hosts.into_iter().filter(|host| !host.trim().is_empty()).collect();
        if hosts.is_empty() {
            anyhow::bail!("Cassandra hosts must not be empty");
        }
        let port = if port == 0 { 9042 } else { port };
        let port = u16::try_from(port)
            .map_err(|_| anyhow::anyhow!("Cassandra port must be between 1 and 65535"))?;
        let keyspace = keyspace.into();
        if keyspace.is_empty() {
            anyhow::bail!("Cassandra keyspace must not be empty");
        }
        Ok(Self { hosts, port, user: user.into(), password: password.into(), keyspace })
    }

    /// Connect to all configured contact points and select Cadence's existing
    /// keyspace. This never creates a keyspace or applies schema migrations.
    pub async fn connect(&self) -> anyhow::Result<Session> {
        let nodes: Vec<String> = self.hosts.iter().map(|host| {
            let host = host.trim();
            if host.starts_with('[') {
                format!("{host}:{}", self.port)
            } else if host.parse::<std::net::IpAddr>().is_ok_and(|ip| ip.is_ipv6()) {
                format!("[{host}]:{}", self.port)
            } else {
                format!("{host}:{}", self.port)
            }
        }).collect();
        let mut builder = SessionBuilder::new()
            .known_nodes(nodes)
            .use_keyspace(self.keyspace.clone(), true);
        if !self.user.is_empty() {
            builder = builder.user(self.user.clone(), self.password.clone());
        }
        Ok(builder.build().await?)
    }
}
