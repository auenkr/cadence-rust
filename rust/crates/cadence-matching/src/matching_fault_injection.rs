//! Go random fake-error injectors that apply to Matching.
//!
//! * `system.persistenceErrorInjectionRate` wraps the TaskManager created by
//!   `common/persistence/client/factory.go:195-199` (read once, when the
//!   manager is built; no errors during the first 30s,
//!   `persistence/wrappers/errorinjectors/utils.go`).
//! * `matching.errorInjectionRate` wraps the Matching client built by
//!   `client/clientfactory.go:183-185` (read once; no startup grace,
//!   `common/errors/fake_errors.go:GenerateFakeError`). Matching uses that
//!   client for partition forwarding.
//!
//! Both pick uniformly from {ServiceBusy, InternalService, Timeout,
//! Unhandled}, forward Timeout/Unhandled calls 50% of the time and always
//! return the fake error afterwards.

use cadence_persistence::{
    matching_blob::TaskListData,
    matching_task_store::{
        AllocatedTask, BackendErrorKind, MatchingTaskStore, MatchingTaskStoreError,
        Result as TaskResult, TaskListKey, TaskListRecord,
    },
};
use rand::Rng;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};
use tonic::Status;
use uuid::Uuid;

const STARTUP_GRACE: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FakeError {
    ServiceBusy,
    InternalService,
    Timeout,
    Unhandled,
}

const FAKE_ERRORS: [FakeError; 4] = [
    FakeError::ServiceBusy,
    FakeError::InternalService,
    FakeError::Timeout,
    FakeError::Unhandled,
];

impl FakeError {
    fn persistence_message(self) -> &'static str {
        match self {
            Self::ServiceBusy => "Fake Service Busy Error.",
            Self::InternalService => "Fake Internal Service Error.",
            Self::Timeout => "Fake Persistence Timeout Error.",
            Self::Unhandled => "fake unhandled error",
        }
    }

    fn to_task_store_error(self) -> MatchingTaskStoreError {
        let kind = match self {
            Self::ServiceBusy => BackendErrorKind::Throttled,
            Self::InternalService => BackendErrorKind::Internal,
            Self::Timeout => BackendErrorKind::Timeout,
            Self::Unhandled => BackendErrorKind::Permanent,
        };
        MatchingTaskStoreError::Backend {
            kind,
            message: self.persistence_message().into(),
        }
    }

    fn to_status(self) -> Status {
        match self {
            Self::ServiceBusy => Status::resource_exhausted("Fake Service Busy Error."),
            Self::InternalService => Status::internal("Fake Internal Service Error."),
            Self::Timeout => Status::deadline_exceeded("context deadline exceeded"),
            Self::Unhandled => Status::unknown("fake unhandled error"),
        }
    }
}

/// Go `GenerateFakeError`: `roll` is `rand.Float64()`, `pick` is the index
/// draw (`rand.Intn(len(fakeErrors))`, here `0..4`).
pub fn generate_fake_error(rate: f64, roll: f64, pick: usize) -> Option<FakeError> {
    (roll < rate).then(|| FAKE_ERRORS[pick % FAKE_ERRORS.len()])
}

/// Go `ShouldForwardCall`: `coin` is `rand.Intn(2) == 0`.
pub fn should_forward_call(fake: Option<FakeError>, coin: bool) -> bool {
    match fake {
        None => true,
        Some(FakeError::Timeout | FakeError::Unhandled) => coin,
        Some(_) => false,
    }
}

fn draw(rate: f64) -> (Option<FakeError>, bool) {
    let mut rng = rand::thread_rng();
    let fake = generate_fake_error(rate, rng.gen::<f64>(), rng.gen_range(0..FAKE_ERRORS.len()));
    let forward = should_forward_call(fake, rng.gen_bool(0.5));
    (fake, forward)
}

type ErrorSink = Arc<dyn Fn(&str) + Send + Sync>;

/// `InjectingTaskStore` mirrors Go's `errorinjectors.NewTaskManager`.
pub struct InjectingTaskStore {
    store: Arc<dyn MatchingTaskStore>,
    rate: f64,
    started: Instant,
    sink: ErrorSink,
}

pub fn inject_task_store_errors(
    store: Arc<dyn MatchingTaskStore>,
    rate: f64,
) -> Arc<dyn MatchingTaskStore> {
    if rate == 0.0 {
        return store;
    }
    Arc::new(InjectingTaskStore {
        store,
        rate,
        started: Instant::now(),
        sink: Arc::new(|line| eprintln!("ERROR {line}")),
    })
}

impl InjectingTaskStore {
    #[cfg(test)]
    fn with_start(
        store: Arc<dyn MatchingTaskStore>,
        rate: f64,
        started: Instant,
        sink: ErrorSink,
    ) -> Self {
        Self { store, rate, started, sink }
    }

    fn run<T>(
        &self,
        operation: &'static str,
        call: impl FnOnce() -> TaskResult<T>,
    ) -> TaskResult<T> {
        // Go `generateFakeError`: nothing is injected during the first 30s.
        let (fake, forward) = if self.started.elapsed() < STARTUP_GRACE {
            (None, true)
        } else {
            draw(self.rate)
        };
        let result = if forward { Some(call()) } else { None };
        let Some(fake) = fake else {
            return result.expect("forwarded call when no fake error");
        };
        let store_error = match &result {
            Some(Err(error)) => error.to_string(),
            _ => "<nil>".into(),
        };
        (self.sink)(&format!(
            "Injected fake persistence error store-operation={operation} error={:?} bool={forward} store-error={store_error}",
            fake.persistence_message()
        ));
        Err(fake.to_task_store_error())
    }
}

impl MatchingTaskStore for InjectingTaskStore {
    fn get_task_list(&self, key: &TaskListKey) -> TaskResult<TaskListRecord> {
        self.run("get-task-list", || self.store.get_task_list(key))
    }
    fn lease(
        &self,
        key: &TaskListKey,
        kind: i16,
        previous_range: i64,
        now_nanos: i64,
    ) -> TaskResult<TaskListRecord> {
        self.run("lease-task-list", || {
            self.store.lease(key, kind, previous_range, now_nanos)
        })
    }
    fn update_task_list(
        &self,
        key: &TaskListKey,
        range: i64,
        data: TaskListData,
        now_nanos: i64,
    ) -> TaskResult<()> {
        self.run("update-task-list", || {
            self.store.update_task_list(key, range, data, now_nanos)
        })
    }
    fn create_tasks_with_domains(
        &self,
        key: &TaskListKey,
        range: i64,
        now_nanos: i64,
        tasks: &[(Uuid, &AllocatedTask)],
    ) -> TaskResult<()> {
        self.run("create-tasks", || {
            self.store
                .create_tasks_with_domains(key, range, now_nanos, tasks)
        })
    }
    fn get_tasks(
        &self,
        key: &TaskListKey,
        read_level: i64,
        max_read_level: i64,
        batch_size: i64,
    ) -> TaskResult<Vec<AllocatedTask>> {
        self.run("get-tasks", || {
            self.store
                .get_tasks(key, read_level, max_read_level, batch_size)
        })
    }
    fn task_list_size(&self, key: &TaskListKey, ack_level: i64) -> TaskResult<i64> {
        self.run("get-task-list-size", || {
            self.store.task_list_size(key, ack_level)
        })
    }
    fn complete_task(&self, key: &TaskListKey, task_id: i64) -> TaskResult<()> {
        self.run("complete-task", || self.store.complete_task(key, task_id))
    }
    fn complete_tasks_less_than(
        &self,
        key: &TaskListKey,
        inclusive_task_id: i64,
        limit: i64,
    ) -> TaskResult<usize> {
        self.run("complete-tasks-less-than", || {
            self.store
                .complete_tasks_less_than(key, inclusive_task_id, limit)
        })
    }
    fn delete_task_list(&self, key: &TaskListKey, range: i64) -> TaskResult<()> {
        self.run("delete-task-list", || self.store.delete_task_list(key, range))
    }
}

static CLIENT_ERROR_RATE: OnceLock<f64> = OnceLock::new();

/// Record `matching.errorInjectionRate`; Go reads it once while building the
/// client, so later dynamic-config changes do not apply.
pub fn install_client_error_rate(rate: f64) {
    let _ = CLIENT_ERROR_RATE.set(rate);
}

/// Wrap one outbound Matching client call (partition forwarding) with Go's
/// `errorinjectors.matchingClient` behavior.
pub async fn inject_client_call<T, F>(operation: &'static str, call: F) -> Result<T, Status>
where
    F: std::future::Future<Output = Result<T, Status>>,
{
    let rate = CLIENT_ERROR_RATE.get().copied().unwrap_or(0.0);
    if rate == 0.0 {
        return call.await;
    }
    let (fake, forward) = draw(rate);
    inject_client_with(operation, fake, forward, call).await
}

async fn inject_client_with<T, F>(
    operation: &'static str,
    fake: Option<FakeError>,
    forward: bool,
    call: F,
) -> Result<T, Status>
where
    F: std::future::Future<Output = Result<T, Status>>,
{
    let result = if forward { Some(call.await) } else { None };
    let Some(fake) = fake else {
        return result.expect("forwarded call when no fake error");
    };
    let client_error = match &result {
        Some(Err(error)) => error.to_string(),
        _ => "<nil>".into(),
    };
    eprintln!(
        "ERROR Injected fake matching client error client-operation={operation} error={:?} bool={forward} client-error={client_error}",
        fake.persistence_message()
    );
    Err(fake.to_status())
}

#[cfg(test)]
mod tests {
    use super::*;
    use cadence_persistence::matching_task_store::TaskListKey;
    use std::sync::Mutex;

    #[test]
    fn fake_error_selection_and_forwarding_follow_go() {
        assert_eq!(generate_fake_error(0.5, 0.49, 2), Some(FakeError::Timeout));
        assert_eq!(generate_fake_error(0.5, 0.5, 2), None);
        assert_eq!(generate_fake_error(0.0, 0.0, 0), None);
        assert_eq!(generate_fake_error(1.0, 0.99, 0), Some(FakeError::ServiceBusy));
        assert!(should_forward_call(None, false));
        assert!(!should_forward_call(Some(FakeError::ServiceBusy), true));
        assert!(!should_forward_call(Some(FakeError::InternalService), true));
        assert!(should_forward_call(Some(FakeError::Timeout), true));
        assert!(!should_forward_call(Some(FakeError::Unhandled), false));
    }

    #[test]
    fn persistence_errors_map_to_retry_classes() {
        assert!(FakeError::ServiceBusy.to_task_store_error().is_transient());
        assert!(FakeError::InternalService.to_task_store_error().is_transient());
        assert!(FakeError::Timeout.to_task_store_error().is_transient());
        assert!(!FakeError::Unhandled.to_task_store_error().is_transient());
    }

    struct Counting(Mutex<usize>);
    impl MatchingTaskStore for Counting {
        fn get_task_list(&self, _: &TaskListKey) -> TaskResult<TaskListRecord> {
            unreachable!()
        }
        fn lease(&self, _: &TaskListKey, _: i16, _: i64, _: i64) -> TaskResult<TaskListRecord> {
            unreachable!()
        }
        fn update_task_list(&self, _: &TaskListKey, _: i64, _: TaskListData, _: i64) -> TaskResult<()> {
            unreachable!()
        }
        fn create_tasks_with_domains(&self, _: &TaskListKey, _: i64, _: i64, _: &[(Uuid, &AllocatedTask)]) -> TaskResult<()> {
            unreachable!()
        }
        fn get_tasks(&self, _: &TaskListKey, _: i64, _: i64, _: i64) -> TaskResult<Vec<AllocatedTask>> {
            unreachable!()
        }
        fn task_list_size(&self, _: &TaskListKey, _: i64) -> TaskResult<i64> {
            unreachable!()
        }
        fn complete_task(&self, _: &TaskListKey, _: i64) -> TaskResult<()> {
            *self.0.lock().unwrap() += 1;
            Ok(())
        }
        fn complete_tasks_less_than(&self, _: &TaskListKey, _: i64, _: i64) -> TaskResult<usize> {
            unreachable!()
        }
        fn delete_task_list(&self, _: &TaskListKey, _: i64) -> TaskResult<()> {
            unreachable!()
        }
    }

    fn key() -> TaskListKey {
        TaskListKey {
            domain_id: Uuid::nil(),
            name: "tl".into(),
            task_type: 0,
        }
    }

    #[test]
    fn task_store_injection_has_startup_grace_zero_rate_passthrough_and_always_errors() {
        let inner = Arc::new(Counting(Mutex::new(0)));
        // Zero rate returns the store untouched.
        let passthrough = inject_task_store_errors(inner.clone(), 0.0);
        passthrough.complete_task(&key(), 1).unwrap();
        assert_eq!(*inner.0.lock().unwrap(), 1);

        // Inside the 30s grace nothing is injected even at rate 1.
        let lines = Arc::new(Mutex::new(Vec::<String>::new()));
        let sink_lines = lines.clone();
        let sink: ErrorSink = Arc::new(move |line| sink_lines.lock().unwrap().push(line.into()));
        let grace = InjectingTaskStore::with_start(inner.clone(), 1.0, Instant::now(), sink.clone());
        grace.complete_task(&key(), 1).unwrap();
        assert_eq!(*inner.0.lock().unwrap(), 2);
        assert!(lines.lock().unwrap().is_empty());

        // After the grace, rate 1 always returns a fake error and logs it.
        let past = Instant::now() - Duration::from_secs(31);
        let injecting = InjectingTaskStore::with_start(inner.clone(), 1.0, past, sink);
        for _ in 0..20 {
            assert!(injecting.complete_task(&key(), 1).is_err());
        }
        let lines = lines.lock().unwrap();
        assert_eq!(lines.len(), 20);
        assert!(lines[0].contains("Injected fake persistence error store-operation=complete-task"));
    }

    #[tokio::test]
    async fn client_injection_skips_forwarding_for_busy_and_always_returns_fake() {
        let called = Arc::new(Mutex::new(0));
        let c = called.clone();
        let result: Result<(), Status> = inject_client_with(
            "matching-add-decision-task",
            Some(FakeError::ServiceBusy),
            false,
            async move {
                *c.lock().unwrap() += 1;
                Ok(())
            },
        )
        .await;
        assert_eq!(result.unwrap_err().code(), tonic::Code::ResourceExhausted);
        assert_eq!(*called.lock().unwrap(), 0);

        let c = called.clone();
        let result: Result<(), Status> = inject_client_with(
            "matching-add-decision-task",
            Some(FakeError::Timeout),
            true,
            async move {
                *c.lock().unwrap() += 1;
                Ok(())
            },
        )
        .await;
        assert_eq!(result.unwrap_err().code(), tonic::Code::DeadlineExceeded);
        assert_eq!(*called.lock().unwrap(), 1);

        let ok: Result<u8, Status> = inject_client_with("op", None, true, async { Ok(7) }).await;
        assert_eq!(ok.unwrap(), 7);
        // Rate unset => plain pass-through.
        let ok: Result<u8, Status> = inject_client_call("op", async { Ok(9) }).await;
        assert_eq!(ok.unwrap(), 9);
    }
}
