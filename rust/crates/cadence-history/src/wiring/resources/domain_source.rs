//! `DomainMetadataSource` for the History domain cache over the configured
//! default store (SQLite, MySQL, PostgreSQL or Cassandra), reusing the same
//! persistence factory as Matching/Frontend
//! (`matching_task_store::open_configured_default_store`).
use std::sync::Arc;

use cadence_service_core::config::PersistenceConfig;
use cadence_matching::matching_domain_cache::DomainMetadataSource;
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
