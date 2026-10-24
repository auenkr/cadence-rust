//! Parity tests for Go `service/matching` cases that were PARTIAL in
//! `memory/matching-go-test-map-20261023.md`. Public API only; the per-row
//! table lives in `memory/evidence/matching-partial-upgrade-a-20261024/notes.md`.

use cadence_api::admin::DynamicConfigValue;
use cadence_api::enums::EncodingType;
use cadence_api::types::DataBlob;
use cadence_matching::matching_config as cfg;
use cadence_matching::matching_handler::{EngineLifecycle, Handler};
use cadence_service_core::admin::DynamicConfigOverlay;
use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

fn set(overlay: &mut DynamicConfigOverlay, key: &str, value: Value) {
    overlay.update(
        false,
        key,
        vec![DynamicConfigValue {
            value: Some(DataBlob::new(
                EncodingType::JSON,
                serde_json::to_vec(&value).unwrap(),
            )),
            filters: vec![],
        }],
    );
}

// Go config_test.go TestNewConfig (line 45): the same keys and values are set, then
// read back. Rust has no single Config struct, so every matching reader that exists is
// checked with the Go key name and the Go expected value.
#[test]
fn go_new_config_readers_return_go_test_values() {
    let mut overlay = DynamicConfigOverlay::default();
    set(&mut overlay, "matching.rps", json!(3));
    set(&mut overlay, "matching.workerrps", json!(4));
    set(&mut overlay, "matching.domainrps", json!(5));
    set(&mut overlay, "matching.domainworkerrps", json!(6));
    set(&mut overlay, "matching.enableSyncMatch", json!(true));
    set(&mut overlay, "matching.maxTasklistIdleTime", json!("10ns"));
    set(&mut overlay, "matching.semaphoreIdleTime", json!("44ns"));
    set(&mut overlay, "matching.activityTaskSyncMatchWaitTime", json!("24ns"));

    assert_eq!(cfg::user_rps(overlay.property_values("matching.rps")), 3);
    assert_eq!(cfg::worker_rps(overlay.property_values("matching.workerrps")), 4);
    assert_eq!(
        cfg::domain_user_rps(overlay.property_values("matching.domainrps"), "domain"),
        5
    );
    assert_eq!(
        cfg::domain_worker_rps(overlay.property_values("matching.domainworkerrps"), "domain"),
        6
    );
    assert!(cfg::enable_sync_match(
        overlay.property_values("matching.enableSyncMatch"),
        "domain",
        "tasklist",
        0
    ));
    assert_eq!(
        cfg::max_tasklist_idle_time(
            overlay.property_values("matching.maxTasklistIdleTime"),
            "domain",
            "tasklist",
            0
        ),
        10
    );
    assert_eq!(
        cfg::semaphore_idle_time(overlay.property_values("matching.semaphoreIdleTime"), "domain"),
        44
    );
    assert_eq!(
        cfg::activity_task_sync_match_wait_time(
            overlay.property_values("matching.activityTaskSyncMatchWaitTime"),
            "domain"
        ),
        24
    );
}

#[test]
#[ignore = "parity gap: Rust has no NewConfig equivalent. Go asserts all 66 Config fields (RangeSize, HostName, RPCConfig, TaskDispatchRPS, AllIsolationGroups, ...) against one key table; Rust reads each key inside its own module, so no single table covers them"]
fn go_new_config_every_field_has_a_rust_reader() {
    panic!("parity gap: no Rust Config struct to compare against");
}

// Go handler/context_test.go TestHandleErrKnowErrors (line 48) and TestHandleErrUncategorizedError.
#[test]
#[ignore = "parity gap: Rust has no generic handleErr. Go returns each of 14 typed errors (and their %w-wrapped forms) unchanged; Rust converts each RPC's errors to tonic::Status at its own call site, so identity pass-through cannot hold"]
fn go_handle_err_passes_typed_errors_through_unchanged() {
    panic!("parity gap: no generic handleErr in Rust");
}

// Go handler/handler_test.go TestStart (line 129) and TestStop (line 140): the engine's Start is called once,
// and Stop after Start calls the engine's Stop once, with goleak verifying no goroutines remain.
struct CountingEngine {
    starts: Arc<AtomicUsize>,
    stops: Arc<AtomicUsize>,
}

impl EngineLifecycle for CountingEngine {
    fn start(&self) {
        self.starts.fetch_add(1, Ordering::SeqCst);
    }
    fn stop(&self) {
        self.stops.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn go_handler_start_calls_engine_start_once_and_stop_calls_engine_stop_once() {
    let starts = Arc::new(AtomicUsize::new(0));
    let stops = Arc::new(AtomicUsize::new(0));
    let handler = Handler::new(CountingEngine {
        starts: starts.clone(),
        stops: stops.clone(),
    });
    assert!(handler.start());
    assert!(!handler.start());
    assert_eq!(starts.load(Ordering::SeqCst), 1);
    assert!(handler.stop());
    assert!(!handler.stop());
    assert_eq!(stops.load(Ordering::SeqCst), 1);
}
