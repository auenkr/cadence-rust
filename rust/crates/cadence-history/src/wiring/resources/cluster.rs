//! `cluster.Metadata` backed by the shared `cadence_common::cluster::Metadata`.
use std::sync::Arc;

use cadence_common::cluster::Metadata;

use crate::engine::deps::{ClusterMetadata as EngineClusterMetadata, HResult, HistoryError};
use crate::task::deps::ClusterMetadata as TaskClusterMetadata;

#[derive(Clone)]
pub struct ClusterMetadataImpl {
    inner: Arc<Metadata>,
}

impl ClusterMetadataImpl {
    pub fn new(metadata: Metadata) -> Self {
        Self { inner: Arc::new(metadata) }
    }
    pub fn metadata(&self) -> &Metadata {
        &self.inner
    }
    /// Names of all enabled remote clusters (`GetRemoteClusterInfo` keys).
    pub fn remote_cluster_names(&self) -> Vec<String> {
        self.inner.remote_clusters().map(|(n, _)| n.clone()).collect()
    }
}

impl EngineClusterMetadata for ClusterMetadataImpl {
    fn get_current_cluster_name(&self) -> String {
        self.inner.current_cluster_name.clone()
    }
    fn cluster_name_for_failover_version(&self, version: i64) -> HResult<String> {
        self.inner
            .cluster_name_for_failover_version(version)
            .map(str::to_string)
            .map_err(HistoryError::Other)
    }
}

impl TaskClusterMetadata for ClusterMetadataImpl {
    fn get_current_cluster_name(&self) -> String {
        self.inner.current_cluster_name.clone()
    }
}
