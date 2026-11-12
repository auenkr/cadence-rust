//! Go `mutableStateBuilder.Load` checksum invalidation (`shouldInvalidateChecksum`) and
//! `workflowRepairerImpl.VerifyAndRepairWorkflowIfNeeded` detection half
//! (`verifyChecksumAndAnalyze`, `logChecksumMismatchDetected`).
use std::sync::Arc;

use super::engine_cache::EngineCore;
use super::engine_state::EngineMutableState;
use crate::execution::checksum::{Checksum, MutableStateChecksumPayload};
use crate::task::deps::{MetricsClient, MetricsScope, Tag};

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
    pub(super) fn verify_loaded_checksum(&self, core: &EngineCore) -> ChecksumLoadOutcome {
        let persisted = self
            .snapshot
            .checksum
            .as_ref()
            .filter(|b| b.encoding.eq_ignore_ascii_case("json"))
            .and_then(|b| parse_checksum_json(&b.data));
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
                metrics.inc_counter("WorkflowContext", "MutableStateChecksumInvalidated");
            }
            ChecksumLoadOutcome::Mismatch => {
                let cluster = core.current_cluster.clone();
                metrics.inc_counter("WorkflowContext", "MutableStateChecksumMismatch");
                metrics
                    .scope(
                        "WorkflowCorruptionRepair",
                        vec![tag("corruption_type", "ChecksumMismatch"), tag("source_cluster", &cluster)],
                    )
                    .inc_counter("MutableStateCorruptionDetected");
                tracing::error!(
                    domain_id = %ids.0, workflow_id = %ids.1, run_id = %ids.2, next_event_id,
                    cluster = %cluster, corruption_type = "ChecksumMismatch",
                    "mutable state checksum mismatch detected"
                );
                if self.params.enable_corruption_auto_repair {
                    tracing::warn!(
                        workflow_id = %ids.1, run_id = %ids.2,
                        "corruption auto repair is enabled but repair-by-rebuild is not available on the engine path"
                    );
                }
            }
            _ => {}
        }
        outcome
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
