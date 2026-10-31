//! Worker-specific utility contracts shared by system workers.

use std::future::Future;
use std::time::Duration;

/// Cadence's internal system domain. Go uses this domain for most system workflows.
pub const SYSTEM_DOMAIN: &str = "cadence-system";

/// Go `StartWorkflowWithRetry`'s warm-up delay and exponential retry bounds.
pub const WORKFLOW_START_RETRY_INITIAL: Duration = Duration::from_secs(1);
pub const WORKFLOW_START_RETRY_MAXIMUM: Duration = Duration::from_secs(60);

/// Wait for dependent services to warm up, then retry workflow startup forever
/// with Go's 1s, 2s, 4s ... capped-at-60s exponential schedule. The Go helper
/// retries every error and runs with a background context, so cancellation is
/// intentionally owned by the surrounding worker process.
pub async fn start_workflow_with_retry<F, Fut, E>(
    startup_delay: Duration,
    mut start_workflow: F,
) where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<(), E>>,
{
    tokio::time::sleep(startup_delay).await;
    let mut retry_attempt = 0u32;
    loop {
        if start_workflow().await.is_ok() {
            return;
        }
        tokio::time::sleep(workflow_start_retry_delay(retry_attempt)).await;
        retry_attempt = retry_attempt.saturating_add(1);
    }
}

pub fn workflow_start_retry_delay(retry_attempt: u32) -> Duration {
    let multiplier = 1u32.checked_shl(retry_attempt).unwrap_or(u32::MAX);
    WORKFLOW_START_RETRY_INITIAL
        .saturating_mul(multiplier)
        .min(WORKFLOW_START_RETRY_MAXIMUM)
}

/// Go stores JSON encoded system-workflow payloads in Cadence `Payload.data`.
/// This helper keeps serialization centralized so each port can mirror the SDK
/// converter and add byte-for-byte Go fixtures as it is implemented.
pub fn encode_json<T: serde::Serialize>(value: &T) -> anyhow::Result<Vec<u8>> {
    Ok(serde_json::to_vec(value)?)
}

pub fn decode_json<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> anyhow::Result<T> {
    Ok(serde_json::from_slice(bytes)?)
}

#[cfg(test)]
mod tests {
    #[test]
    fn system_workflow_payloads_use_compact_json() {
        let bytes = super::encode_json(&serde_json::json!({"DomainID":"domain-id"})).unwrap();
        assert_eq!(bytes, br#"{"DomainID":"domain-id"}"#);
        let value: serde_json::Value = super::decode_json(&bytes).unwrap();
        assert_eq!(value["DomainID"], "domain-id");
    }

    #[test]
    fn workflow_start_retry_schedule_matches_go_policy() {
        assert_eq!(
            super::workflow_start_retry_delay(0),
            std::time::Duration::from_secs(1)
        );
        assert_eq!(
            super::workflow_start_retry_delay(1),
            std::time::Duration::from_secs(2)
        );
        assert_eq!(
            super::workflow_start_retry_delay(5),
            std::time::Duration::from_secs(32)
        );
        assert_eq!(
            super::workflow_start_retry_delay(6),
            std::time::Duration::from_secs(60)
        );
        assert_eq!(
            super::workflow_start_retry_delay(100),
            std::time::Duration::from_secs(60)
        );
    }
}
