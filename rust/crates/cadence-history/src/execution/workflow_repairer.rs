//! Checksum corruption repair from Go `execution/workflow_repairer.go`.
use std::{future::Future, time::{Duration, Instant}};
use super::checksum::{Checksum, MutableStateChecksumPayload};
use crate::mutable_state::MutableStateBuilder;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CorruptionType { None, ChecksumMismatch }
impl std::fmt::Display for CorruptionType {
    fn fmt(&self, out: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { out.write_str(match self { Self::None => "None", Self::ChecksumMismatch => "ChecksumMismatch" }) }
}
#[derive(Debug, thiserror::Error)]
pub enum RepairError {
    #[error("rebuilt mutable state checksum does not match original - checksum or history may be corrupted")]
    ChecksumMismatchAfterRebuild,
    #[error("missing version histories")] MissingVersionHistories,
    #[error("unexpected history events during corruption repair persistence")]
    UnexpectedHistoryEvents,
    #[error("workflow corruption repair timed out")] Timeout,
    #[error("workflow corruption repair canceled")] Canceled,
    #[error(transparent)] Other(#[from] anyhow::Error),
}
/// Persistence must use UpdateWorkflowModeIgnoreCurrent via the shard so range
/// ID and encoding are supplied. `persist_repaired_state` closes a passive
/// mutation, sets ExecutionStats.HistorySize, and rejects any new history.
#[allow(async_fn_in_trait)]
pub trait RepairContext {
    fn domain_name(&self, state: &MutableStateBuilder) -> Option<String>;
    fn verify_probability(&self, domain: &str) -> i32;
    fn random_percent(&mut self) -> i32;
    fn auto_repair(&self, domain: &str) -> bool;
    fn repair_timeout(&self, domain: &str) -> Duration;
    fn require_checksum_match(&self, domain: &str) -> bool;
    fn checksum_mismatch_detected(&mut self, state: &MutableStateBuilder);
    fn repair_attempted(&mut self);
    fn rebuild_checksum_result(&mut self, matches: bool);
    fn repair_finished(&mut self, elapsed: Duration, result: &Result<(), RepairError>);
    async fn rebuild_local_history(&mut self, state: &MutableStateBuilder) -> Result<(MutableStateBuilder, i64), RepairError>;
    async fn persist_repaired_state(&mut self, state: MutableStateBuilder, history_size: i64) -> Result<(), RepairError>;
}

pub fn checksum_matches(a: &Checksum, b: &Checksum) -> bool { !a.value.is_empty() && !b.value.is_empty() && a.value == b.value }

pub struct WorkflowRepairer<C> { pub context: C }
impl<C: RepairContext> WorkflowRepairer<C> {
    /// `caller_canceled` resolves only on explicit cancellation/shard shutdown,
    /// never on the caller's deadline. Repair receives an independent timeout.
    pub async fn verify_and_repair_workflow_if_needed(&mut self, state: &MutableStateBuilder, persisted: &Checksum, caller_canceled: impl Future<Output = ()>) -> Result<bool, RepairError> {
        if persisted.value.is_empty() { return Ok(false); }
        let Some(domain) = self.context.domain_name(state) else { return Ok(false); };
        if self.context.random_percent() >= self.context.verify_probability(&domain) { return Ok(false); }
        if MutableStateChecksumPayload::from_mutable_state(state).verify(persisted).is_ok() { return Ok(false); }
        self.context.checksum_mismatch_detected(state);
        if !self.context.auto_repair(&domain) { return Ok(false); }
        let start = Instant::now();
        let timeout = self.context.repair_timeout(&domain);
        self.context.repair_attempted();
        let result = tokio::select! {
            _ = caller_canceled => Err(RepairError::Canceled),
            result = tokio::time::timeout(timeout, self.repair_via_rebuild(state, persisted, &domain)) => result.unwrap_or(Err(RepairError::Timeout)),
        };
        self.context.repair_finished(start.elapsed(), &result);
        result.map(|()| true)
    }
    async fn repair_via_rebuild(&mut self, state: &MutableStateBuilder, persisted: &Checksum, domain: &str) -> Result<(), RepairError> {
        if state.version_histories.is_none() { return Err(RepairError::MissingVersionHistories); }
        let (mut rebuilt, history_size) = self.context.rebuild_local_history(state).await?;
        // Sticky state is not in history but is included in checksum. Preserve
        // it for comparison, along with the non-checksummed client metadata.
        for field in ["sticky_task_list", "sticky_schedule_to_start_timeout", "client_library_version", "client_feature_version", "client_impl"] {
            if let Some(value) = state.execution_info.get(field) { rebuilt.execution_info[field] = value.clone(); }
        }
        let matches = checksum_matches(&MutableStateChecksumPayload::from_mutable_state(&rebuilt).generate(), persisted);
        self.context.rebuild_checksum_result(matches);
        if !matches {
            if self.context.require_checksum_match(domain) { return Err(RepairError::ChecksumMismatchAfterRebuild); }
            rebuilt.execution_info["sticky_task_list"] = "".into();
            rebuilt.execution_info["sticky_schedule_to_start_timeout"] = 0.into();
        }
        // The original DB value may itself be corrupt. It remains the compare
        // condition; the replayed next event ID is the value written to DB.
        rebuilt.next_event_id_in_db = state.next_event_id;
        self.context.persist_repaired_state(rebuilt, history_size).await
    }
}
