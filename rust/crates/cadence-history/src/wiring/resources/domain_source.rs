//! `DomainMetadataSource` for the History domain cache over the configured
//! default store (SQLite, MySQL, PostgreSQL or Cassandra), reusing the same
//! persistence factory as Matching/Frontend
//! (`matching_task_store::open_configured_default_store`).
use std::sync::Arc;
use std::time::Duration;

use cadence_service_core::config::PersistenceConfig;
use cadence_matching::matching_domain_cache::{DomainMetadataPage, DomainMetadataSource, DomainMetadataSourceError};
use cadence_persistence::matching_domain_metadata::MatchingDomainMetadata;
use cadence_matching::matching_domain_uuid::DomainIdParse;
use cadence_matching::matching_task_store::open_configured_default_store;

/// Opens the default datastore and returns its domain metadata source.
/// `active_cluster_name` is the current cluster; blocking, call from
/// `spawn_blocking`.
pub fn open_domain_metadata_source(
    persistence: &PersistenceConfig,
    active_cluster_name: &str,
) -> anyhow::Result<Arc<dyn DomainMetadataSource>> {
    let stores = open_configured_default_store(
        persistence,
        active_cluster_name,
        Arc::new(|| false),
        Arc::new(|| "thriftrw".to_string()),
        false,
    )?;
    stores
        .domain_metadata_source
        .ok_or_else(|| anyhow::anyhow!("configured default store has no domain metadata source"))
}

/// Go's domain cache reads through the rate-limited metadata manager
/// (`ratelimited.NewDomainManager`): every read is charged to the History
/// persistence limiter and a rejection is `ServiceBusyError`, which the cache
/// treats as transient.
pub struct RateLimitedDomainSource {
    inner: Arc<dyn DomainMetadataSource>,
    limiter: Arc<cadence_persistence::rate_limited::PersistenceLimiter>,
}

impl RateLimitedDomainSource {
    pub fn wrap(
        inner: Arc<dyn DomainMetadataSource>,
        limiter: Option<Arc<cadence_persistence::rate_limited::PersistenceLimiter>>,
    ) -> Arc<dyn DomainMetadataSource> {
        match limiter {
            Some(limiter) => Arc::new(Self { inner, limiter }),
            None => inner,
        }
    }

    fn check(&self) -> Result<(), DomainMetadataSourceError> {
        if self.limiter.allow() {
            Ok(())
        } else {
            Err(DomainMetadataSourceError::transient(
                cadence_persistence::rate_limited::PERSISTENCE_LIMIT_EXCEEDED,
            ))
        }
    }
}

impl DomainMetadataSource for RateLimitedDomainSource {
    fn metadata_version(&self, timeout: Duration) -> Result<i64, DomainMetadataSourceError> {
        self.check()?;
        self.inner.metadata_version(timeout)
    }
    fn list_domains(
        &self,
        page_size: usize,
        next_page_token: Option<&[u8]>,
        timeout: Duration,
    ) -> Result<DomainMetadataPage, DomainMetadataSourceError> {
        self.check()?;
        self.inner.list_domains(page_size, next_page_token, timeout)
    }
    fn get_domain(
        &self,
        name: Option<&str>,
        id: Option<&str>,
        timeout: Duration,
    ) -> Result<MatchingDomainMetadata, DomainMetadataSourceError> {
        self.check()?;
        self.inner.get_domain(name, id, timeout)
    }
    fn get_domain_by_raw_id(
        &self,
        id: &[u8],
        timeout: Duration,
        mode: DomainIdParse,
    ) -> Result<MatchingDomainMetadata, DomainMetadataSourceError> {
        self.check()?;
        self.inner.get_domain_by_raw_id(id, timeout, mode)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Default)]
    struct Counting(AtomicUsize);
    impl DomainMetadataSource for Counting {
        fn metadata_version(&self, _: Duration) -> Result<i64, DomainMetadataSourceError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(7)
        }
        fn list_domains(&self, _: usize, _: Option<&[u8]>, _: Duration) -> Result<DomainMetadataPage, DomainMetadataSourceError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(DomainMetadataPage { domains: Vec::new(), next_page_token: None })
        }
        fn get_domain(&self, _: Option<&str>, _: Option<&str>, _: Duration) -> Result<MatchingDomainMetadata, DomainMetadataSourceError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Err("not found".into())
        }
    }

    #[test]
    fn rejects_before_inner_call_with_transient_service_busy() {
        let inner = Arc::new(Counting::default());
        let limiter = cadence_persistence::rate_limited::PersistenceLimiter::build(
            Arc::new(|| 1),
            Arc::new(|| 0),
            Arc::new(|| Ok(1)),
        );
        let wrapped = RateLimitedDomainSource::wrap(inner.clone(), limiter);
        let t = Duration::from_secs(1);
        let mut busy = 0;
        for _ in 0..50 {
            if let Err(e) = wrapped.metadata_version(t) {
                assert_eq!(e.message, "Persistence Max QPS Reached.");
                assert!(e.retryable);
                busy += 1;
            }
        }
        assert!(busy > 0);
        assert_eq!(inner.0.load(Ordering::SeqCst) + busy, 50);
    }

    #[test]
    fn disabled_limiter_returns_inner() {
        let inner = Arc::new(Counting::default());
        let wrapped = RateLimitedDomainSource::wrap(inner.clone(), None);
        for _ in 0..20 {
            assert!(wrapped.metadata_version(Duration::from_secs(1)).is_ok());
        }
        assert_eq!(inner.0.load(Ordering::SeqCst), 20);
    }
}
