//! Go `mutableStateBuilder.Load` checksum invalidation (`shouldInvalidateChecksum`) and
//! `workflowRepairerImpl.VerifyAndRepairWorkflowIfNeeded` detection half
//! (`verifyChecksumAndAnalyze`, `logChecksumMismatchDetected`).
use std::sync::Arc;

use super::engine_cache::EngineCore;
use super::engine_state::EngineMutableState;
use crate::execution::checksum::{Checksum, MutableStateChecksumPayload};
use crate::task::deps::{MetricsClient, Tag};

/// Result of the load-time checksum handling.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum ChecksumLoadOutcome {
    /// No (or empty) checksum persisted.
    NoChecksum,
    /// Go `shouldInvalidateChecksum`: state last updated before `MutableStateChecksumInvalidateBefore`.
    Invalidated,
    /// Go `rand.Intn(100) >= MutableStateChecksumVerifyProbability`.
    SkippedBySampling,
    Verified,
    Mismatch,
}

/// Decodes the JSON checksum blob written by `SerializeChecksum(JSON)`:
/// `{"Version":1,"Flavor":1,"Value":"<base64>"}`.
pub(super) fn parse_checksum_json(data: &[u8]) -> Option<Checksum> {
    let v: serde_json::Value = serde_json::from_slice(data).ok()?;
    let value = decode_base64(v.get("Value")?.as_str()?)?;
    Some(Checksum {
        version: v.get("Version").and_then(|x| x.as_i64()).unwrap_or(0) as i32,
        flavor: v.get("Flavor").and_then(|x| x.as_i64()).unwrap_or(0) as i32,
        value,
    })
}

fn decode_base64(s: &str) -> Option<Vec<u8>> {
    let mut out = Vec::new();
    let (mut acc, mut bits) = (0u32, 0u32);
    for c in s.bytes().take_while(|c| *c != b'=') {
        let d = match c {
            b'A'..=b'Z' => c - b'A',
            b'a'..=b'z' => c - b'a' + 26,
            b'0'..=b'9' => c - b'0' + 52,
            b'+' => 62,
            b'/' => 63,
            _ => return None,
        } as u32;
        acc = (acc << 6) | d;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((acc >> bits) as u8);
            acc &= (1 << bits) - 1;
        }
    }
    Some(out)
}

/// Pure decision logic. `roll` is `rand.Intn(100)`.
pub(super) fn evaluate_checksum(
    persisted: Option<&Checksum>,
    last_updated_nanos: i64,
    invalidate_before_secs: i64,
    verify_probability: i64,
    roll: i64,
    payload: &MutableStateChecksumPayload,
) -> ChecksumLoadOutcome {
    let Some(persisted) = persisted.filter(|c| !c.value.is_empty()) else {
        return ChecksumLoadOutcome::NoChecksum;
    };
    // Go Load: `shouldInvalidateChecksum` -> `LastUpdatedTimestamp.Before(invalidateBefore)`.
    if invalidate_before_secs > 0 && last_updated_nanos < invalidate_before_secs.saturating_mul(1_000_000_000) {
        return ChecksumLoadOutcome::Invalidated;
    }
    if roll >= verify_probability {
        return ChecksumLoadOutcome::SkippedBySampling;
    }
    match payload.verify(persisted) {
        Ok(()) => ChecksumLoadOutcome::Verified,
        Err(_) => ChecksumLoadOutcome::Mismatch,
    }
}

fn tag(key: &'static str, value: &str) -> Tag {
    Tag { key, value: value.to_string() }
}

impl EngineMutableState {
    /// Go `Load` invalidation + `VerifyAndRepairWorkflowIfNeeded` (detection, metrics, logs).
    /// With auto-repair disabled Go logs and continues; repair-by-rebuild is not part of the
    /// engine path (the generic `execution::workflow_repairer` is not wired), so with
    /// auto-repair enabled this only warns.
    pub(super) fn persisted_checksum(&self) -> Option<Checksum> {
        self.snapshot
            .checksum
            .as_ref()
            .filter(|b| b.encoding.eq_ignore_ascii_case("json"))
            .and_then(|b| parse_checksum_json(&b.data))
    }

    pub(super) fn verify_loaded_checksum(&self, core: &EngineCore) -> ChecksumLoadOutcome {
        let persisted = self.persisted_checksum();
        let (payload, last_updated, ids, next_event_id) = {
            let g = self.inner.lock().unwrap();
            (
                g.checksum_payload(),
                g.last_updated_nanos,
                (g.pc_exec.domain_id.clone(), g.pc_exec.workflow_id.clone(), g.pc_exec.run_id.clone()),
                g.pc_exec.next_event_id,
            )
        };
        let roll = rand::Rng::gen_range(&mut rand::thread_rng(), 0i64..100i64);
        let outcome = evaluate_checksum(
            persisted.as_ref(),
            last_updated,
            self.params.checksum_invalidate_before_secs,
            self.params.checksum_verify_probability,
            roll,
            &payload,
        );
        let metrics: &Arc<dyn MetricsClient> = &core.metrics;
        match outcome {
            ChecksumLoadOutcome::Invalidated => {
                metrics.inc_counter("WorkflowContextScope", "MutableStateChecksumInvalidated");
            }
            ChecksumLoadOutcome::Mismatch => {
                let cluster = core.current_cluster.clone();
                metrics.inc_counter("WorkflowContextScope", "MutableStateChecksumMismatch");
                metrics
                    .scope(
                        "WorkflowCorruptionRepairScope",
                        vec![tag("corruption_type", "ChecksumMismatch"), tag("source_cluster", &cluster)],
                    )
                    .inc_counter("MutableStateCorruptionDetected");
                tracing::error!(
                    domain_id = %ids.0, workflow_id = %ids.1, run_id = %ids.2, next_event_id,
                    cluster = %cluster, corruption_type = "ChecksumMismatch",
                    "mutable state checksum mismatch detected"
                );
                // Auto-repair enabled: the caller (`EngineExecutionContext::load_state`) runs
                // `repair_workflow` and reloads. Disabled: log and continue (Go parity).
            }
            _ => {}
        }
        outcome
    }
}

/// Go `newMutableStateChecksumPayload` over a rebuilt (typed) state.
pub(super) fn typed_checksum_payload(t: &crate::execution::mutable_state_builder::TypedMutableState) -> MutableStateChecksumPayload {
    let e = &t.execution_info;
    let mut timers: Vec<i64> = t.pending_timer_info_ids.values().map(|x| x.started_id).collect();
    timers.sort_unstable();
    MutableStateChecksumPayload {
        cancel_requested: e.cancel_requested,
        state: e.state as i16,
        last_first_event_id: e.last_first_event_id,
        next_event_id: e.next_event_id,
        last_processed_event_id: e.last_processed_event,
        signal_count: i64::from(e.signal_count),
        decision_attempt: e.decision_attempt as i32,
        decision_version: e.decision_version,
        decision_scheduled_id: e.decision_schedule_id,
        decision_started_id: e.decision_started_id,
        pending_timer_started_ids: timers,
        pending_activity_scheduled_ids: t.pending_activity_info_ids.keys().copied().collect(),
        pending_signal_initiated_ids: t.pending_signal_info_ids.keys().copied().collect(),
        pending_req_cancel_initiated_ids: t.pending_request_cancel_info_ids.keys().copied().collect(),
        pending_child_initiated_ids: t.pending_child_execution_info_ids.keys().copied().collect(),
        sticky_task_list_name: e.sticky_task_list.clone(),
        version_histories: t.version_histories.clone(),
    }
}

/// Go `checksumMatches`.
pub(super) fn checksum_matches(a: &Checksum, b: &Checksum) -> bool {
    !a.value.is_empty() && !b.value.is_empty() && a.value == b.value
}

impl EngineMutableState {
    /// Go `workflowRepairerImpl.repairWorkflow` (checksum-mismatch branch): metrics, logs and an
    /// independent `CorruptionRepairTimeout` around [`Self::repair_via_rebuild`].
    pub(super) async fn repair_workflow(&self, core: &Arc<EngineCore>, persisted: &Checksum) -> crate::engine::deps::HResult<()> {
        let cluster = core.current_cluster.clone();
        let scope = core.metrics.scope("WorkflowCorruptionRepairScope", vec![tag("corruption_type", "ChecksumMismatch")]);
        let (domain_id, workflow_id, run_id) = {
            let g = self.inner.lock().unwrap();
            (g.pc_exec.domain_id.clone(), g.pc_exec.workflow_id.clone(), g.pc_exec.run_id.clone())
        };
        let start = std::time::Instant::now();
        scope.tagged(vec![tag("source_cluster", &cluster)]).inc_counter("WorkflowRepairAttempted");
        tracing::info!(
            domain_id = %domain_id, workflow_id = %workflow_id, run_id = %run_id, cluster = %cluster,
            corruption_type = "ChecksumMismatch", "Attempting to repair corrupted workflow"
        );
        let result = match tokio::time::timeout(self.params.corruption_repair_timeout, self.repair_via_rebuild(core, persisted)).await {
            Ok(r) => r,
            Err(_) => {
                scope.inc_counter("WorkflowRepairTimeout");
                Err(crate::engine::deps::HistoryError::internal("workflow corruption repair timed out"))
            }
        };
        scope.exponential_histogram("WorkflowRepairDuration", start.elapsed());
        match &result {
            Ok(()) => {
                scope.inc_counter("WorkflowRepairSuccess");
                tracing::info!(domain_id = %domain_id, workflow_id = %workflow_id, run_id = %run_id, corruption_type = "ChecksumMismatch", "Workflow repair succeeded");
            }
            Err(e) => {
                scope.inc_counter("WorkflowRepairFailure");
                tracing::error!(domain_id = %domain_id, workflow_id = %workflow_id, run_id = %run_id, corruption_type = "ChecksumMismatch", error = %e, "Workflow repair failed");
            }
        }
        result
    }

    /// Go `repairViaRebuild` + `persistRepairedState`.
    #[allow(clippy::result_large_err)]
    async fn repair_via_rebuild(&self, core: &Arc<EngineCore>, persisted: &Checksum) -> crate::engine::deps::HResult<()> {
        use crate::engine::deps::HistoryError;
        use crate::execution::workflow::TransactionPolicy;
        use crate::ndc::NdcMutableState;
        use cadence_persistence::history_contract as pc;
        use cadence_persistence::version_history::{VersionHistoriesExt, VersionHistoryExt};

        let (info, version_histories) = {
            let g = self.inner.lock().unwrap();
            (g.pc_exec.clone(), g.version_histories.clone())
        };
        let histories = version_histories.ok_or_else(|| HistoryError::internal("missing version histories"))?;
        let current = histories.current_history().map_err(|e| HistoryError::internal(e.to_string()))?;
        let branch_token = current.branch_token.clone();
        let last = current.last_item().map_err(|e| HistoryError::internal(e.to_string()))?;
        let (last_event_id, last_version) = (last.event_id, last.version);

        let now = core.clock.now();
        let rebuild_core = core.clone();
        let (domain_id, workflow_id, run_id) = (info.domain_id.clone(), info.workflow_id.clone(), info.run_id.clone());
        let rebuild_branch = branch_token.clone();
        let rebuilt = tokio::task::spawn_blocking(move || {
            let rebuilder = super::ndc_adapters::new_state_rebuilder(&rebuild_core);
            let branch = rebuild_branch.clone();
            rebuilder.rebuild_owned(
                now, &domain_id, &workflow_id, &run_id, &rebuild_branch, last_event_id, last_version,
                &run_id, &move || Ok(branch.clone()), "",
            )
        })
        .await
        .map_err(|e| HistoryError::internal(format!("state rebuild task failed: {e}")))?;
        let (mut rebuilt, history_size) = rebuilt.map_err(|e| HistoryError::internal(e.to_string()))?;

        // Sticky task list is in the checksum but not in history: preserve for comparison.
        // Client metadata is not checksummed but kept for continuity.
        {
            let t = rebuilt.typed_mut();
            t.execution_info.sticky_task_list = info.sticky_task_list.clone();
            t.execution_info.sticky_schedule_to_start_timeout_nanos = info.sticky_schedule_to_start_timeout_nanos;
            t.execution_info.client_library_version = info.client_library_version.clone();
            t.execution_info.client_feature_version = info.client_feature_version.clone();
            t.execution_info.client_impl = info.client_impl.clone();
            t.history_size = history_size;
        }
        let rebuilt_checksum = typed_checksum_payload(rebuilt.typed()).generate();
        if checksum_matches(&rebuilt_checksum, persisted) {
            core.metrics.inc_counter("WorkflowCorruptionRepairScope", "MutableStateRebuildChecksumMatch");
        } else {
            core.metrics.inc_counter("WorkflowCorruptionRepairScope", "MutableStateRebuildChecksumMismatch");
            if self.params.require_checksum_match_after_rebuild_repair {
                return Err(HistoryError::internal(
                    "rebuilt mutable state checksum does not match original - checksum or history may be corrupted",
                ));
            }
            let t = rebuilt.typed_mut();
            t.execution_info.sticky_task_list = String::new();
            t.execution_info.sticky_schedule_to_start_timeout_nanos = 0;
        }

        // The DB value (possibly corrupt) is the conditional-write condition.
        let original_next_event_id = info.next_event_id;
        let rebuilt_next_event_id = rebuilt.typed().execution_info.next_event_id;
        if original_next_event_id != rebuilt_next_event_id {
            tracing::warn!(
                domain_id = %info.domain_id, workflow_id = %info.workflow_id, run_id = %info.run_id,
                original_next_event_id, rebuilt_next_event_id,
                "NextEventID corruption detected - original DB value differs from rebuilt value"
            );
        }
        rebuilt.set_update_condition(original_next_event_id);

        // persistRepairedState: passive mutation, no new events allowed.
        core.domains.get_domain_name(&info.domain_id)?;
        let (mut mutation, events) = rebuilt
            .close_transaction_as_mutation(now, TransactionPolicy::Passive)
            .map_err(|e| HistoryError::internal(e.to_string()))?;
        mutation.execution_info.history_size = history_size;
        if !events.is_empty() {
            return Err(HistoryError::internal("unexpected history events during corruption repair persistence"));
        }
        let shard = &core.shard;
        let request = pc::UpdateWorkflowExecutionRequest {
            shard_id: shard.shard_id(),
            range_id: shard.range_id(),
            mode: pc::UpdateWorkflowMode::IgnoreCurrent,
            update_workflow_mutation: mutation,
            new_workflow_snapshot: None,
            workflow_request_mode: pc::WorkflowRequestMode::New,
            current_timestamp_unix_nanos: now,
        };
        core.executions.update_workflow_execution(request).await.map_err(crate::wiring::persistence::engine_ndc::to_history_error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sum(v: &[u8]) -> Checksum {
        Checksum { version: 1, flavor: 1, value: v.to_vec() }
    }

    #[test]
    fn parses_go_json_shape() {
        let c = parse_checksum_json(br#"{"Version":1,"Flavor":1,"Value":"3q2+7w=="}"#).unwrap();
        assert_eq!(c, sum(&[0xde, 0xad, 0xbe, 0xef]));
        assert!(parse_checksum_json(b"not json").is_none());
    }

    #[test]
    fn evaluate_follows_go_order() {
        let p = MutableStateChecksumPayload::default();
        let good = p.generate();
        let bad = sum(&[1, 2, 3, 4]);
        let ev = |c: Option<&Checksum>, upd, inv, prob, roll| evaluate_checksum(c, upd, inv, prob, roll, &p);
        assert_eq!(ev(None, 0, 0, 100, 0), ChecksumLoadOutcome::NoChecksum);
        assert_eq!(ev(Some(&sum(&[])), 0, 0, 100, 0), ChecksumLoadOutcome::NoChecksum);
        // invalidateBefore: updated before the cutoff -> invalidated, even for a bad checksum
        assert_eq!(ev(Some(&bad), 5_000_000_000, 10, 100, 0), ChecksumLoadOutcome::Invalidated);
        assert_eq!(ev(Some(&bad), 15_000_000_000, 10, 100, 0), ChecksumLoadOutcome::mismatch_for_test());
        // sampling: roll >= probability skips
        assert_eq!(ev(Some(&bad), 0, 0, 50, 50), ChecksumLoadOutcome::SkippedBySampling);
        assert_eq!(ev(Some(&bad), 0, 0, 0, 0), ChecksumLoadOutcome::SkippedBySampling);
        assert_eq!(ev(Some(&good), 0, 0, 100, 99), ChecksumLoadOutcome::Verified);
        assert_eq!(ev(Some(&bad), 0, 0, 100, 99), ChecksumLoadOutcome::Mismatch);
    }

    impl ChecksumLoadOutcome {
        fn mismatch_for_test() -> Self {
            ChecksumLoadOutcome::Mismatch
        }
    }
}
