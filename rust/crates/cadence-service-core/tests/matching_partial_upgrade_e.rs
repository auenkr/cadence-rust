//! Go parity for `go/common/dynamicconfig/file_based_oracle_test.go`
//! (`TestFileBasedOraclePollReload`). Rust's file-based dynamic config is a
//! one-shot `DynamicConfigOverlay::from_file` snapshot; Go's client re-reads the
//! file on its poll interval.
use cadence_service_core::admin::DynamicConfigOverlay;

#[test]
#[ignore = "parity gap: Go FileBasedClient re-reads the YAML on every poll when the file changes (file_based_oracle_test.go TestFileBasedOraclePollReload, 7s deadline); Rust DynamicConfigOverlay::from_file is a one-shot snapshot with no poller, so an existing overlay never observes a rewritten file"]
fn file_based_oracle_poll_reload_observes_rewritten_file() {
    let path = std::env::temp_dir().join(format!(
        "cadence-dynamic-config-poll-{}.yaml",
        std::process::id()
    ));
    std::fs::write(&path, "testGetIntPropertyKey:\n- value: 1\n").unwrap();
    let overlay = DynamicConfigOverlay::from_file(&path).unwrap();
    std::fs::write(&path, "testGetIntPropertyKey:\n- value: 2\n").unwrap();
    // Go's client keeps the same instance and must surface value 2 after a poll.
    let value: i64 = serde_json::from_slice(
        &overlay.property_values("testGetIntPropertyKey").snapshot_values()[0]
            .value
            .as_ref()
            .unwrap()
            .data,
    )
    .unwrap();
    std::fs::remove_file(&path).unwrap();
    assert_eq!(value, 2);
}
