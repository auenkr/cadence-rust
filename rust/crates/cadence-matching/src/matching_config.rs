//! Go `service/matching/config` readers for keys that had no Rust reader,
//! with the defaults from `common/dynamicconfig/dynamicproperties/constants.go`.
//!
//! Go copies `matching.enableSyncMatch`, `matching.maxTasklistIdleTime` and
//! `matching.activityTaskSyncMatchWaitTime` into `newTaskListConfig`
//! (task_list_manager.go:1040-1062) but nothing in `service/matching` ever
//! calls the resulting `TaskListConfig` fields, so these readers exist for
//! config parity and gate no behavior; wiring them would change Go logic.
use cadence_persistence::dynamic_config::{self, PropertyValueSource};

/// Go `dynamicproperties.UnlimitedRPS` (`math.MaxInt32`).
pub const UNLIMITED_RPS: i64 = i32::MAX as i64;
/// Go `MatchingUserRPS` default.
pub const DEFAULT_USER_RPS: i64 = 1200;
/// Go `MatchingDomainUserRPS` default (0 falls back to `matching.rps`).
pub const DEFAULT_DOMAIN_USER_RPS: i64 = 0;
/// Go `MaxTasklistIdleTime` and `MatchingSemaphoreIdleTime` default (5m).
pub const DEFAULT_IDLE_TIME_NANOS: i64 = 5 * 60 * 1_000_000_000;

/// Go `MatchingActivityTaskSyncMatchWaitTime` default (50ms).
pub const DEFAULT_ACTIVITY_SYNC_MATCH_WAIT_NANOS: i64 = 50_000_000;

/// Go `MatchingUserRPS` (`matching.rps`).
pub fn user_rps(values: impl PropertyValueSource) -> i64 {
    dynamic_config::integer(values, None, DEFAULT_USER_RPS)
}

/// Go `MatchingWorkerRPS` (`matching.workerrps`).
pub fn worker_rps(values: impl PropertyValueSource) -> i64 {
    dynamic_config::integer(values, None, UNLIMITED_RPS)
}

/// Go `MatchingDomainUserRPS` (`matching.domainrps`).
pub fn domain_user_rps(values: impl PropertyValueSource, domain: &str) -> i64 {
    dynamic_config::integer(values, Some(domain), DEFAULT_DOMAIN_USER_RPS)
}

/// Go `MatchingDomainWorkerRPS` (`matching.domainworkerrps`).
pub fn domain_worker_rps(values: impl PropertyValueSource, domain: &str) -> i64 {
    dynamic_config::integer(values, Some(domain), UNLIMITED_RPS)
}

/// Go `MatchingEnableSyncMatch` (`matching.enableSyncMatch`), not consumed by Go.
pub fn enable_sync_match(
    values: impl PropertyValueSource,
    domain: &str,
    task_list: &str,
    task_type: i32,
) -> bool {
    dynamic_config::task_list_boolean(values, Some(domain), task_list, task_type, true)
}

/// Go `MaxTasklistIdleTime` (`matching.maxTasklistIdleTime`), not consumed by Go.
pub fn max_tasklist_idle_time(
    values: impl PropertyValueSource,
    domain: &str,
    task_list: &str,
    task_type: i32,
) -> i64 {
    dynamic_config::task_list_duration(
        values,
        Some(domain),
        task_list,
        task_type,
        DEFAULT_IDLE_TIME_NANOS,
    )
}

/// Go `MatchingActivityTaskSyncMatchWaitTime`
/// (`matching.activityTaskSyncMatchWaitTime`, default 50ms), not consumed by Go.
pub fn activity_task_sync_match_wait_time(values: impl PropertyValueSource, domain: &str) -> i64 {
    dynamic_config::duration(values, Some(domain), DEFAULT_ACTIVITY_SYNC_MATCH_WAIT_NANOS)
}

/// Go `MatchingSemaphoreIdleTime` (`matching.semaphoreIdleTime`).
pub fn semaphore_idle_time(values: impl PropertyValueSource, domain: &str) -> i64 {
    dynamic_config::duration(values, Some(domain), DEFAULT_IDLE_TIME_NANOS)
}

/// Go `engine.go` task-list type defaulting: only an activity request maps to
/// the activity persistence type, everything else (including unset/invalid)
/// is a decision task list. Returns the persistence type (0 decision, 1 activity)
/// for a public API task-list type (1 decision, 2 activity).
pub fn persistence_task_list_type(api_type: i32) -> i32 {
    if api_type == 2 {
        1
    } else {
        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cadence_api::admin::DynamicConfigValue;
    use cadence_persistence::dynamic_config::{RuntimePropertyProvider, RuntimePropertyType};
    use serde_json::json;
    use std::{
        collections::BTreeMap,
        sync::{Arc, Mutex},
    };

    type RuntimeCall = (
        String,
        RuntimePropertyType,
        serde_json::Value,
        BTreeMap<String, serde_json::Value>,
    );

    #[derive(Debug, Default)]
    struct FakeRuntime(
        std::sync::RwLock<BTreeMap<(String, String, String), serde_json::Value>>,
        Mutex<Vec<RuntimeCall>>,
    );

    impl RuntimePropertyProvider for FakeRuntime {
        fn evaluate(
            &self,
            key: &str,
            property_type: RuntimePropertyType,
            default: serde_json::Value,
            filters: &BTreeMap<String, serde_json::Value>,
        ) -> Result<serde_json::Value, String> {
            self.1.lock().unwrap().push((
                key.to_owned(),
                property_type,
                default.clone(),
                filters.clone(),
            ));
            if key == "matching.runtimeFailure" {
                return Err("provider unavailable".into());
            }
            let domain = filters
                .get("domainName")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let task_list = filters
                .get("taskListName")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let task_type = filters
                .get("taskType")
                .and_then(|v| v.as_i64())
                .unwrap_or(-1)
                .to_string();
            Ok(self
                .0
                .read()
                .unwrap()
                .get(&(
                    key.to_owned(),
                    domain.to_owned(),
                    format!("{task_list}:{task_type}"),
                ))
                .cloned()
                .unwrap_or(default))
        }
    }

    fn value(v: serde_json::Value) -> Vec<DynamicConfigValue> {
        vec![DynamicConfigValue {
            value: Some(cadence_api::types::DataBlob::new(
                cadence_api::enums::EncodingType::JSON,
                serde_json::to_vec(&v).unwrap(),
            )),
            filters: Vec::new(),
        }]
    }

    #[test]
    fn go_defaults_when_unset() {
        assert_eq!(user_rps(&[]), 1200);
        assert_eq!(worker_rps(&[]), i32::MAX as i64);
        assert_eq!(domain_user_rps(&[], "d"), 0);
        assert_eq!(domain_worker_rps(&[], "d"), i32::MAX as i64);
        assert!(enable_sync_match(&[], "d", "tl", 0));
        assert_eq!(max_tasklist_idle_time(&[], "d", "tl", 1), 300_000_000_000);
        assert_eq!(semaphore_idle_time(&[], "d"), 300_000_000_000);
    }

    #[test]
    fn activity_sync_match_wait_defaults_to_go_50ms() {
        assert_eq!(activity_task_sync_match_wait_time(&[], "d"), 50_000_000);
    }
    #[test]
    fn configured_values_override_defaults() {
        assert_eq!(user_rps(&value(json!(5))), 5);
        assert_eq!(worker_rps(&value(json!(7))), 7);
        assert!(!enable_sync_match(&value(json!(false)), "d", "tl", 0));
        assert_eq!(
            semaphore_idle_time(&value(json!("30s")), "d"),
            30_000_000_000
        );
        assert_eq!(
            max_tasklist_idle_time(&value(json!("2m")), "d", "tl", 0),
            120_000_000_000
        );
    }

    #[test]
    fn wrong_type_falls_back_to_default() {
        assert_eq!(user_rps(&value(json!("abc"))), 1200);
        assert_eq!(semaphore_idle_time(&value(json!(12)), "d"), 300_000_000_000);
    }

    #[test]
    fn non_activity_task_list_type_maps_to_decision() {
        assert_eq!(persistence_task_list_type(1), 0);
        assert_eq!(persistence_task_list_type(2), 1);
        assert_eq!(persistence_task_list_type(0), 0);
        assert_eq!(persistence_task_list_type(9), 0);
        assert_eq!(persistence_task_list_type(-1), 0);
    }

    #[test]
    fn openfeature_runtime_reads_keep_identity_filters_defaults_and_live_values() {
        let fake = Arc::new(FakeRuntime::default());
        fake.0.write().unwrap().insert(
            ("matching.domainrps".into(), "alpha".into(), ":-1".into()),
            json!(31),
        );
        fake.0.write().unwrap().insert(
            ("matching.domainrps".into(), "beta".into(), ":-1".into()),
            json!(47),
        );
        fake.0.write().unwrap().insert(
            (
                "matching.enableSyncMatch".into(),
                "alpha".into(),
                "queue:0".into(),
            ),
            json!(false),
        );
        fake.0.write().unwrap().insert(
            (
                "matching.enableSyncMatch".into(),
                "alpha".into(),
                "queue:1".into(),
            ),
            json!(true),
        );
        let mut overlay = crate::admin::DynamicConfigOverlay::default();
        overlay.set_runtime_provider(Some(fake.clone()));

        assert_eq!(
            domain_user_rps(overlay.property_values("matching.domainrps"), "alpha"),
            31
        );
        assert_eq!(
            domain_user_rps(overlay.property_values("matching.domainrps"), "beta"),
            47
        );
        fake.0.write().unwrap().insert(
            ("matching.domainrps".into(), "alpha".into(), ":-1".into()),
            json!(88),
        );
        assert_eq!(
            domain_user_rps(overlay.property_values("matching.domainrps"), "alpha"),
            88
        );
        assert!(!enable_sync_match(
            overlay.property_values("matching.enableSyncMatch"),
            "alpha",
            "queue",
            0
        ));
        assert!(enable_sync_match(
            overlay.property_values("matching.enableSyncMatch"),
            "alpha",
            "queue",
            1
        ));
        assert_eq!(
            dynamic_config::duration(
                overlay.property_values("matching.durationDefault"),
                Some("alpha"),
                90_000_000_000,
            ),
            90_000_000_000,
        );
        assert_eq!(
            dynamic_config::integer(overlay.property_values("matching.runtimeFailure"), None, 19),
            19
        );
        fake.0.write().unwrap().insert(
            ("matching.domainrps".into(), "wrong".into(), ":-1".into()),
            json!("not-an-int"),
        );
        assert_eq!(
            domain_user_rps(overlay.property_values("matching.domainrps"), "wrong"),
            0
        );

        let calls = fake.1.lock().unwrap();
        assert!(calls
            .iter()
            .any(
                |(key, property_type, default, filters)| key == "matching.domainrps"
                    && *property_type == RuntimePropertyType::Integer
                    && default == &json!(0)
                    && filters.get("domainName") == Some(&json!("alpha"))
            ));
        assert!(calls
            .iter()
            .any(
                |(key, property_type, default, filters)| key == "matching.enableSyncMatch"
                    && *property_type == RuntimePropertyType::Boolean
                    && default == &json!(true)
                    && filters.get("taskListName") == Some(&json!("queue"))
                    && filters.get("taskType") == Some(&json!(0))
            ));
        assert!(calls.iter().any(|(key, property_type, default, filters)| {
            key == "matching.durationDefault"
                && *property_type == RuntimePropertyType::Duration
                && default == &json!(90_000_000_000i64)
                && filters.get("domainName") == Some(&json!("alpha"))
        }));
    }
}
