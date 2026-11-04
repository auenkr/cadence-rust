//! `failover::DomainManager` over the domain persistence
//! (Go `domain.CleanPendingActiveState`, common/domain/failover_watcher.go).
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use cadence_persistence::matching_domain_metadata::DomainMetadataPersistence;

use crate::failover::coordinator::{
    UPDATE_DOMAIN_MAX_RETRY, UPDATE_DOMAIN_RETRY_COEFFICIENT, UPDATE_DOMAIN_RETRY_INITIAL_INTERVAL,
};
use crate::failover::DomainManager;

pub struct PersistenceDomainManager {
    persistence: Arc<dyn DomainMetadataPersistence>,
}

impl PersistenceDomainManager {
    pub fn new(persistence: Arc<dyn DomainMetadataPersistence>) -> Self {
        Self { persistence }
    }

    fn clean_blocking(&self, domain_id: &str, failover_version: i64) -> Result<bool, String> {
        // must read the metadata version first: it acts as the lock on the domain table.
        let version = self
            .persistence
            .metadata_version()
            .map_err(|e| format!("getting metadata: {e}"))?;
        let mut domain = self
            .persistence
            .get_domain(None, Some(domain_id))
            .map_err(|e| format!("getting domain: {e}"))?;
        if !(domain.is_global_domain
            && domain.failover_end_time_nanos.is_some()
            && failover_version == domain.failover_version)
        {
            return Ok(false);
        }
        domain.failover_end_time_nanos = None;
        domain.notification_version = version;
        // backoff.ThrottleRetry: every update error is retryable (isUpdateDomainRetryable),
        // exponential policy 50ms x2, max attempts 2.
        let mut delay = UPDATE_DOMAIN_RETRY_INITIAL_INTERVAL;
        let mut attempt = 1usize;
        loop {
            match self.persistence.update_domain(&domain, version) {
                Ok(()) => return Ok(true),
                Err(e) => {
                    if attempt >= UPDATE_DOMAIN_MAX_RETRY {
                        return Err(e);
                    }
                    std::thread::sleep(delay);
                    delay = Duration::from_secs_f64(delay.as_secs_f64() * UPDATE_DOMAIN_RETRY_COEFFICIENT);
                    attempt += 1;
                }
            }
        }
    }
}

#[async_trait]
impl DomainManager for PersistenceDomainManager {
    async fn clean_pending_active_state(
        &self,
        domain_id: &str,
        failover_version: i64,
    ) -> Result<bool, String> {
        let this = Self { persistence: self.persistence.clone() };
        let id = domain_id.to_string();
        tokio::task::spawn_blocking(move || this.clean_blocking(&id, failover_version))
            .await
            .map_err(|e| format!("join: {e}"))?
    }
}
