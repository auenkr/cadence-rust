//! Go `matchingEngineImpl.emitInfoOrDebugLog` (`handler/engine.go:1588-1598`).
//!
//! Go logs at Info only when `system.enableDebugMode` (read once at service
//! start, default false) and `matching.enableTaskInfoLogByDomainID` (filter
//! `domainID`, default false, read per call) are both true; otherwise the same
//! event is logged at Debug. Rust has no debug log level, so those events are
//! dropped.

use crate::admin::DynamicConfigOverlay;
use std::sync::{Arc, Mutex, OnceLock};

type Sink = Arc<dyn Fn(&str) + Send + Sync>;

pub struct TaskInfoLog {
    overlay: Arc<Mutex<DynamicConfigOverlay>>,
    debug_mode: bool,
    sink: Sink,
}

static TASK_INFO_LOG: OnceLock<TaskInfoLog> = OnceLock::new();

impl TaskInfoLog {
    pub fn new(overlay: Arc<Mutex<DynamicConfigOverlay>>, debug_mode: bool) -> Self {
        Self {
            overlay,
            debug_mode,
            sink: Arc::new(|line| eprintln!("INFO {line}")),
        }
    }

    fn enabled(&self, domain_id: &str) -> bool {
        if !self.debug_mode {
            return false;
        }
        let overlay = self.overlay.lock().expect("admin dynamic config lock");
        cadence_persistence::dynamic_config::boolean_domain_id(
            overlay.property_values("matching.enableTaskInfoLogByDomainID"),
            domain_id,
            false,
        )
    }

    pub fn emit(&self, domain_id: &str, message: &str, fields: &[(&str, String)]) {
        if !self.enabled(domain_id) {
            return;
        }
        let mut line = message.to_owned();
        for (key, value) in fields {
            line.push_str(&format!(" {key}={value}"));
        }
        (self.sink)(&line);
    }
}

/// Install the process logger once at startup; later calls are ignored.
pub fn install(log: TaskInfoLog) {
    let _ = TASK_INFO_LOG.set(log);
}

/// `emitInfoOrDebugLog`: no-op until `install` has run.
pub fn emit(domain_id: &str, message: &str, fields: &[(&str, String)]) {
    if let Some(log) = TASK_INFO_LOG.get() {
        log.emit(domain_id, message, fields);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cadence_api::{
        admin::{DynamicConfigFilter, DynamicConfigValue},
        enums::EncodingType,
        types::DataBlob,
    };

    fn overlay_with(entries: Vec<DynamicConfigValue>) -> Arc<Mutex<DynamicConfigOverlay>> {
        let mut overlay = DynamicConfigOverlay::default();
        overlay.update(false, "matching.enableTaskInfoLogByDomainID", entries);
        Arc::new(Mutex::new(overlay))
    }

    fn blob(value: serde_json::Value) -> Option<DataBlob> {
        Some(DataBlob::new(
            EncodingType::JSON,
            serde_json::to_vec(&value).unwrap(),
        ))
    }

    fn log(debug: bool, entries: Vec<DynamicConfigValue>) -> (TaskInfoLog, Arc<Mutex<Vec<String>>>) {
        let lines = Arc::new(Mutex::new(Vec::new()));
        let sink_lines = lines.clone();
        let mut log = TaskInfoLog::new(overlay_with(entries), debug);
        log.sink = Arc::new(move |line| sink_lines.lock().unwrap().push(line.into()));
        (log, lines)
    }

    fn per_domain(domain_id: &str) -> Vec<DynamicConfigValue> {
        vec![DynamicConfigValue {
            value: blob(serde_json::json!(true)),
            filters: vec![DynamicConfigFilter {
                name: "domainID".into(),
                value: blob(serde_json::json!(domain_id)),
            }],
        }]
    }

    #[test]
    fn info_requires_debug_mode_and_domain_id_flag_otherwise_dropped() {
        let fields = [("wf-id", "w".to_owned()), ("wf-schedule-id", "5".to_owned())];
        let (log, lines) = log(true, per_domain("d1"));
        log.emit("d1", "Received AddDecisionTask", &fields);
        log.emit("other", "Received AddDecisionTask", &fields);
        let lines = lines.lock().unwrap();
        assert_eq!(
            lines.as_slice(),
            ["Received AddDecisionTask wf-id=w wf-schedule-id=5"]
        );
        drop(lines);

        let (log, lines) = self::log(false, per_domain("d1"));
        log.emit("d1", "Received AddDecisionTask", &fields);
        assert!(lines.lock().unwrap().is_empty());

        let (log, lines) = self::log(true, vec![]);
        log.emit("d1", "Received AddDecisionTask", &fields);
        assert!(lines.lock().unwrap().is_empty());
    }
}
