//! Byte-preserving native engine poll core. The handler must supply checked
//! startup, quota, domain-cache and ownership integration (see memory note).
use super::{response_config, NativeMatchingEngine};
use crate::matching_dispatch_native::{NativeFuture, NativePollLifecycle, NativePollObserver};
use crate::matching_registry_native::NativeQueueHandle;
use cadence_common::go_string::GoString;
use cadence_persistence::matching_native_task_store::TaskListKeyBytes;
use cadence_proto::native::{api, matching};
use std::{sync::Arc, time::Duration};
use tokio::sync::Notify;
use tonic::Status;

/// Registration must remove its cancellation entry on drop, including when the
/// RPC future is dropped. Keys and domain names at this boundary are raw bytes.
pub struct NativePollRegistration {
    pub cancel: Arc<Notify>,
    pub guard: Box<dyn Send>,
}

/// Handler/domain/config integration. Handler startup, quota, domain lookup
/// guard and entry ownership must already have succeeded before calling the
/// engine. These methods supply the remaining live domain/config dependencies.
pub trait NativePollContext: Send + Sync {
    fn domain_name(&self, id: &GoString) -> Result<GoString, Status>;
    fn domain_active(&self, id: &GoString) -> Result<bool, Status>;
    fn check_queue(&self, key: &TaskListKeyBytes, queue: &NativeQueueHandle) -> Result<(), Status>;
    fn register_poll(
        &self,
        domain: &GoString,
        list: &GoString,
        task_type: i32,
        poller: &GoString,
    ) -> NativePollRegistration;
    /// Subscribe before resolving the domain, as in the handler. Resolves on failover.
    fn failover(&self, domain: &GoString) -> NativeFuture<()>;
    fn expiration(&self, domain: &GoString, list: &GoString, task_type: i32) -> Duration;
    fn override_rate(&self, domain: &GoString, list: &GoString, task_type: i32) -> f64;
    fn auto_config_enabled(&self, domain: &GoString, list: &GoString, task_type: i32) -> bool;
    fn observer(
        &self,
        _domain: &GoString,
        _list: &GoString,
        _task_type: i32,
    ) -> Option<Arc<dyn NativePollObserver>> {
        None
    }
    fn observer_with_context(
        &self, domain: &GoString, list: &GoString, task_type: i32,
        _task_list_kind: i32, _isolation_group: &GoString,
    ) -> Option<Arc<dyn NativePollObserver>> {
        self.observer(domain, list, task_type)
    }
}

fn name(list: &GoString, decision: bool) -> Result<(), Status> {
    cadence_common::task_list::TaskListNameBytes::parse(list.as_bytes())
        .map(|_| ())
        .map_err(|error| {
            let mut message = Vec::new();
            if decision {
                message.extend_from_slice(b"couldn't create new decision tasklist ");
            }
            message.extend_from_slice(error.as_bytes());
            crate::matching_native_status::raw_status(
                tonic::Code::Internal,
                &GoString::from(message),
                tonic::codegen::Bytes::new(),
            )
        })
}
fn domain_id(id: &GoString) -> Result<uuid::Uuid, Status> {
    id.as_utf8()
        .ok()
        .and_then(|s| uuid::Uuid::parse_str(s).ok())
        .ok_or_else(|| Status::invalid_argument("DomainUUID is not valid."))
}
fn expiration(
    context: &dyn NativePollContext,
    domain: &GoString,
    list: &GoString,
    kind: i32,
    deadline: tokio::time::Instant,
) -> Duration {
    context.expiration(domain, list, kind).min(
        deadline
            .saturating_duration_since(tokio::time::Instant::now())
            .saturating_sub(Duration::from_secs(1)),
    )
}
fn rate(
    context: &dyn NativePollContext,
    domain: &GoString,
    list: &GoString,
    kind: i32,
    worker: Option<f64>,
    queue: &NativeQueueHandle,
) -> f64 {
    let override_rate = context.override_rate(domain, list, kind);
    let selected = if override_rate > 0.0 {
        Some(override_rate)
    } else {
        worker
    };
    match selected.filter(|v| *v >= 0.0) {
        Some(v) => {
            queue.limiter.report_limit(v);
            v
        }
        None => crate::matching_limiter::TASK_DISPATCH_RPS,
    }
}
fn hints(queue: &NativeQueueHandle) -> Option<matching::LoadBalancerHints> {
    queue
        .load_balancer_hints(crate::grpc_matching::matching_now())
        .map(
            |(backlog_count, rate_per_second)| matching::LoadBalancerHints {
                backlog_count,
                rate_per_second,
            },
        )
}
fn hint(
    context: &dyn NativePollContext,
    domain: &GoString,
    list: &GoString,
    kind: i32,
    started: std::time::Instant,
) -> api::AutoConfigHint {
    api::AutoConfigHint {
        enable_auto_config: context.auto_config_enabled(domain, list, kind),
        poller_wait_time_in_ms: i64::try_from(started.elapsed().as_millis()).unwrap_or(i64::MAX),
    }
}

async fn query_state_until_cancelled<T>(
    deadline: tokio::time::Instant,
    cancel: &tokio::sync::Notify,
    lookup: impl std::future::Future<Output = Result<T, Status>>,
) -> Result<T, Status> {
    tokio::select! {
        biased;
        _ = cancel.notified() => Err(Status::cancelled("Matching poll cancelled.")),
        result = tokio::time::timeout_at(deadline, lookup) => {
            result.unwrap_or_else(|_| Err(Status::deadline_exceeded("Matching poll deadline exceeded.")))
        }
    }
}

impl NativeMatchingEngine {
    pub async fn poll_for_decision_task(
        &self,
        request: matching::PollForDecisionTaskRequest,
        deadline: Option<tokio::time::Instant>,
        context: &dyn NativePollContext,
    ) -> Result<matching::PollForDecisionTaskResponse, Status> {
        let deadline =
            deadline.ok_or_else(|| Status::invalid_argument("Context timeout is not set."))?;
        let mut payload = request.request.unwrap_or_default();
        let list = payload
            .task_list
            .get_or_insert_with(Default::default)
            .clone();
        name(&list.name, true)?;
        let failover = context.failover(&request.domain_id);
        payload.domain = context.domain_name(&request.domain_id)?;
        crate::grpc_matching::check_poll_context(Some(deadline))?;
        let registration =
            context.register_poll(&request.domain_id, &list.name, 1, &request.poller_id);
        let domain = domain_id(&request.domain_id)?;
        let key = TaskListKeyBytes {
            domain_id: domain,
            name: list.name.clone(),
            task_type: 0,
        };
        let queue = self
            .registry
            .get(
                key.clone(),
                crate::matching_handler_native::persistence_kind(list.kind),
                crate::grpc_matching::matching_now(),
            )
            .await?;
        context.check_queue(&key, &queue)?;
        let started = std::time::Instant::now();
        let expiration = expiration(context, &request.domain_id, &list.name, 0, deadline);
        if queue.stop_if_isolation_changed() {
            return Ok(matching::PollForDecisionTaskResponse {
                auto_config_hint: Some(hint(context, &request.domain_id, &list.name, 0, started)),
                ..Default::default()
            });
        }
        let lifecycle = NativePollLifecycle {
            manager: queue.pollers.clone(),
            id: request.poller_id,
            cancel: Some(registration.cancel.clone()),
            info: cadence_common::poller_bytes::Info {
                identity: payload.identity.clone(),
                rate_per_second: rate(context, &request.domain_id, &list.name, 0, None, &queue),
                isolation_group: request.isolation_group.clone(),
            },
            forwarded: !request.forwarded_from.is_empty(),
            observer: context.observer_with_context(&request.domain_id, &list.name, 0, i32::from(crate::matching_handler_native::persistence_kind(list.kind)), &request.isolation_group),
        };
        let active = context.domain_active(&request.domain_id)?;
        let _standby_wait = (!active).then(|| lifecycle.begin());
        let query_lifecycle = lifecycle.clone();
        let mut response = tokio::select! {
            biased;
            query = queue.queries.next() => {
                let _wait = query_lifecycle.begin();
                if let Some(observer) = &query_lifecycle.observer { observer.poll_success(true); observer.poll_match(false, false); }
                if !query.shape.is_valid() { return Err(Status::unknown(crate::matching_panic::NIL_DEREFERENCE_MESSAGE)); }
                let execution = query.request.workflow_execution.clone().unwrap_or_default();
                // Go finishes the synchronous query handoff before looking up
                // mutable state. If this poll is cancelled during that lookup,
                // it reports the lookup error to the query caller; it does not
                // put the already-consumed query back on the task list.
                let state = query_state_until_cancelled(
                    deadline,
                    &registration.cancel,
                    queue.dispatcher.query_mutable_state_with_deadline(&execution, Some(deadline)),
                ).await;
                match state {
                    Ok(state) => crate::matching_query_native::query_response(query, &state),
                    Err(error) => { let _ = queue.queries.complete(&query.task_id, crate::matching_query_native::QueryOutcome::Internal(error)); Default::default() }
                }
            }
            result = async {
                if active { queue.dispatcher.poll_decision_with_poller(payload, expiration, lifecycle, Some(deadline)).await }
                else { queue.dispatcher.forward_query_poll(payload, expiration, lifecycle).await }
            } => result?,
            _ = failover => { self.registry.release_blocked_pollers_for_domain(domain); Default::default() },
            _ = registration.cancel.notified() => Default::default(),
        };
        response.load_balancer_hints = hints(&queue);
        response.partition_config = response_config(&queue);
        response.auto_config_hint = Some(hint(context, &request.domain_id, &list.name, 0, started));
        Ok(response)
    }

    pub async fn poll_for_activity_task(
        &self,
        request: matching::PollForActivityTaskRequest,
        deadline: Option<tokio::time::Instant>,
        context: &dyn NativePollContext,
    ) -> Result<matching::PollForActivityTaskResponse, Status> {
        let deadline =
            deadline.ok_or_else(|| Status::invalid_argument("Context timeout is not set."))?;
        let Some(mut payload) = request.request else {
            crate::grpc_matching::check_poll_context(Some(deadline))?;
            return Err(Status::unknown(
                crate::matching_panic::NIL_DEREFERENCE_MESSAGE,
            ));
        };
        let list = payload
            .task_list
            .get_or_insert_with(Default::default)
            .clone();
        name(&list.name, false)?;
        let failover = context.failover(&request.domain_id);
        payload.domain = context.domain_name(&request.domain_id)?;
        crate::grpc_matching::check_poll_context(Some(deadline))?;
        let registration =
            context.register_poll(&request.domain_id, &list.name, 2, &request.poller_id);
        let domain = domain_id(&request.domain_id)?;
        let key = TaskListKeyBytes {
            domain_id: domain,
            name: list.name.clone(),
            task_type: 1,
        };
        let queue = self
            .registry
            .get(
                key.clone(),
                crate::matching_handler_native::persistence_kind(list.kind),
                crate::grpc_matching::matching_now(),
            )
            .await?;
        context.check_queue(&key, &queue)?;
        let started = std::time::Instant::now();
        let expiration = expiration(context, &request.domain_id, &list.name, 1, deadline);
        if queue.stop_if_isolation_changed() {
            return Ok(matching::PollForActivityTaskResponse {
                auto_config_hint: Some(hint(context, &request.domain_id, &list.name, 0, started)),
                ..Default::default()
            });
        }
        let worker_rate = payload
            .task_list_metadata
            .as_ref()
            .and_then(|m| m.max_tasks_per_second);
        let lifecycle = NativePollLifecycle {
            manager: queue.pollers.clone(),
            id: request.poller_id,
            cancel: Some(registration.cancel.clone()),
            info: cadence_common::poller_bytes::Info {
                identity: payload.identity.clone(),
                rate_per_second: rate(
                    context,
                    &request.domain_id,
                    &list.name,
                    1,
                    worker_rate,
                    &queue,
                ),
                isolation_group: request.isolation_group.clone(),
            },
            forwarded: !request.forwarded_from.is_empty(),
            observer: context.observer_with_context(&request.domain_id, &list.name, 1, i32::from(crate::matching_handler_native::persistence_kind(list.kind)), &request.isolation_group),
        };
        let mut response = tokio::select! {
            result = async {
                if context.domain_active(&request.domain_id)? { queue.dispatcher.poll_activity_with_poller(payload, expiration, lifecycle, Some(deadline)).await }
                else { let _wait = lifecycle.begin(); tokio::time::sleep(expiration).await; Ok(Default::default()) }
            } => result?,
            _ = async { if queue.queue.task_list_kind() == 0 { failover.await } else { std::future::pending().await } } => { self.registry.release_blocked_pollers_for_domain(domain); Default::default() },
            _ = registration.cancel.notified() => Default::default(),
        };
        response.load_balancer_hints = hints(&queue);
        response.partition_config = response_config(&queue);
        let task_type = if response.task_token.is_empty() { 0 } else { 1 };
        response.auto_config_hint = Some(hint(
            context,
            &request.domain_id,
            &list.name,
            task_type,
            started,
        ));
        Ok(response)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn committed_query_lookup_cancellation_is_terminal_error() {
        let cancel = std::sync::Arc::new(tokio::sync::Notify::new());
        let task_cancel = cancel.clone();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let lookup = async move {
            let _ = started_tx.send(());
            std::future::pending::<Result<(), Status>>().await
        };
        let lookup_task = tokio::spawn(async move {
            query_state_until_cancelled(
                tokio::time::Instant::now() + std::time::Duration::from_secs(5),
                &task_cancel,
                lookup,
            )
            .await
        });
        started_rx.await.unwrap();
        // The poll has committed the query and entered its state lookup.
        // Cancellation ends that lookup and yields the error delivered to the
        // query waiter by the decision-poll branch.
        cancel.notify_one();
        let error = lookup_task.await.unwrap().unwrap_err();
        assert_eq!(error.code(), tonic::Code::Cancelled);
    }

    #[test]
    fn invalid_partition_error_keeps_raw_name_and_decision_prefix() {
        let list = GoString::from(b"/__cadence_sys/\xff/0".as_slice());
        for (decision, prefix) in [
            (false, b"invalid partitioned task list name ".as_slice()),
            (
                true,
                b"couldn't create new decision tasklist invalid partitioned task list name "
                    .as_slice(),
            ),
        ] {
            let error = name(&list, decision).unwrap_err();
            let raw = error
                .metadata()
                .get_bin(crate::matching_native_status::RAW_MESSAGE_MARKER)
                .unwrap()
                .to_bytes()
                .unwrap();
            let mut expected = prefix.to_vec();
            expected.extend_from_slice(list.as_bytes());
            assert_eq!(raw.as_ref(), expected.as_slice());
        }
        assert!(name(&GoString::from(b"\xff".as_slice()), true).is_ok());
    }

    #[test]
    fn invalid_domain_bytes_are_not_lossily_parsed() {
        assert!(domain_id(&GoString::from(b"\xff".as_slice())).is_err());
        assert_eq!(
            domain_id(&GoString::from("00000000-0000-0000-0000-000000000001")).unwrap(),
            uuid::Uuid::from_u128(1)
        );
    }
}
