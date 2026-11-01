//! Port of Go `batcher.BatchWorkflow` (v1) and `BatchWorkflowV2` on the SDK
//! runtime.

use std::future::poll_fn;
use std::pin::Pin;
use std::task::Poll;
use std::time::Duration;

use serde::Deserialize;

use super::*;
use crate::sdk::core::{
    ActivityError, ActivityOptions, RetryPolicy, WorkflowContext, WorkflowError, WorkflowResult,
};
use crate::sdk::converter;
use crate::sdk::registry::Registry;

/// Go `TuneSignal`; zero values leave the parameter unchanged.
#[derive(Clone, Debug, Default, Deserialize, serde::Serialize, PartialEq, Eq)]
#[serde(default)]
pub struct TuneSignal {
    #[serde(rename = "RPS")]
    pub rps: i32,
    #[serde(rename = "Concurrency")]
    pub concurrency: i32,
}

/// Go `batchActivityOptions` with the per-run heartbeat timeout and maximum
/// attempts applied.
pub fn activity_options(params: &BatchParams, wait_for_cancellation: bool) -> ActivityOptions {
    ActivityOptions {
        schedule_to_start: Duration::from_secs(ACTIVITY_SCHEDULE_TO_START_TIMEOUT_SECONDS),
        start_to_close: Duration::from_secs(ACTIVITY_START_TO_CLOSE_TIMEOUT_SECONDS),
        heartbeat: Duration::from_nanos(params.activity_heartbeat_timeout_nanos.max(0) as u64),
        retry_policy: Some(RetryPolicy {
            initial_interval: Duration::from_secs(ACTIVITY_RETRY_INITIAL_INTERVAL_SECONDS),
            backoff_coefficient: ACTIVITY_RETRY_BACKOFF_COEFFICIENT_MILLI as f64 / 1000.0,
            maximum_interval: Duration::from_secs(ACTIVITY_RETRY_MAXIMUM_INTERVAL_SECONDS),
            maximum_attempts: params.max_activity_retries,
            expiration_interval: Duration::from_secs(ACTIVITY_START_TO_CLOSE_TIMEOUT_SECONDS),
            non_retriable_error_reasons: vec![NON_RETRIABLE_ERROR_REASON.to_owned()],
        }),
        wait_for_cancellation,
        ..Default::default()
    }
}

fn parse_params(input: &[u8]) -> Result<BatchParams, WorkflowError> {
    let params: BatchParams =
        converter::decode(input).map_err(|e| WorkflowError::generic(e.to_string()))?;
    validate_and_default(params)
        .map(|v| v.0)
        .map_err(WorkflowError::generic)
}

fn encode_result(result: &HeartBeatDetails) -> WorkflowResult {
    converter::encode(result)
        .map(Some)
        .map_err(|e| WorkflowError::generic(e.to_string()))
}

fn decode_result(data: Option<Vec<u8>>) -> Result<HeartBeatDetails, WorkflowError> {
    converter::decode(&data.unwrap_or_default()).map_err(|e| WorkflowError::generic(e.to_string()))
}

/// Deprecated v1 workflow: one activity processes every page.
pub async fn batch_workflow(ctx: WorkflowContext, input: Vec<u8>) -> WorkflowResult {
    let params = parse_params(&input)?;
    let options = activity_options(&params, false);
    let result = ctx
        .execute_activity(ACTIVITY_TYPE, &options, &params)
        .await?;
    encode_result(&decode_result(result)?)
}

enum Selected {
    Activity(Result<Option<Vec<u8>>, ActivityError>),
    Tune,
}

/// V2 workflow: tune signals cancel the running activity and restart it with
/// updated RPS/concurrency, carrying progress through the cancel details.
pub async fn batch_workflow_v2(ctx: WorkflowContext, input: Vec<u8>) -> WorkflowResult {
    let mut params = parse_params(&input)?;
    let tune = ctx.signal_channel(TUNE_SIGNAL);
    loop {
        let options = activity_options(&params, true);
        let mut future = ctx.execute_activity(V2_ACTIVITY_TYPE, &options, &params);
        let selected = poll_fn(|cx| {
            if let Poll::Ready(r) = Pin::new(&mut future).poll(cx) {
                return Poll::Ready(Selected::Activity(r));
            }
            if let Some(signal) = tune.receive_async::<TuneSignal>() {
                if signal.rps > 0 {
                    params.rps = signal.rps;
                }
                if signal.concurrency > 0 {
                    params.concurrency = signal.concurrency;
                }
                return Poll::Ready(Selected::Tune);
            }
            Poll::Pending
        })
        .await;
        match selected {
            Selected::Activity(result) => {
                future.cancel();
                return encode_result(&decode_result(result?)?);
            }
            Selected::Tune => {
                future.cancel();
                let (result, details) = future.get_detailed().await;
                match result {
                    // Finished before the cancellation was delivered.
                    Ok(data) => return encode_result(&decode_result(data)?),
                    Err(ActivityError::Canceled) => {
                        if let Some(Ok(progress)) =
                            details.map(|d| converter::decode::<HeartBeatDetails>(&d))
                        {
                            params.progress = Some(progress);
                        }
                    }
                    Err(other) => return Err(other.into()),
                }
            }
        }
    }
}

pub fn register_sdk(registry: &mut Registry) {
    registry.register_workflow(V1_WORKFLOW_TYPE, batch_workflow);
    registry.register_workflow(V2_WORKFLOW_TYPE, batch_workflow_v2);
}
