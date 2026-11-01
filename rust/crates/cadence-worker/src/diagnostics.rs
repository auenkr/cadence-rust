//! Diagnostics workflow wire names and invariant analyzer boundary.
pub const TASK_LIST: &str = "diagnostics-wf-tasklist";
pub const WORKFLOW_TYPE: &str = "diagnostics-workflow";
pub const STARTER_WORKFLOW_TYPE: &str = "diagnostics-starter-workflow";
pub const IDENTIFY_ISSUES_ACTIVITY: &str = "identifyIssues";
pub const ROOT_CAUSE_ISSUES_ACTIVITY: &str = "rootCauseIssues";
pub const EMIT_USAGE_LOGS_ACTIVITY: &str = "emitUsageLogs";
pub const QUERY_REPORT: &str = "query-diagnostics-report";

// ---- Constants from go/service/worker/diagnostics (activities.go, parent_workflow.go) ----
pub const LINK_TO_TIMEOUTS_RUNBOOK: &str =
    "https://cadenceworkflow.io/docs/workflow-troubleshooting/timeouts/";
pub const LINK_TO_FAILURES_RUNBOOK: &str =
    "https://cadenceworkflow.io/docs/workflow-troubleshooting/activity-failures/";
pub const LINK_TO_RETRIES_RUNBOOK: &str =
    "https://cadenceworkflow.io/docs/workflow-troubleshooting/retries";
pub const LINK_TO_TIMEOUT_RISKS_RUNBOOK: &str =
    "https://cadenceworkflow.io/docs/workflow-troubleshooting/timeout-risks/";
pub const LINK_TO_ANTIPATTERNS_RUNBOOK: &str =
    "https://cadenceworkflow.io/docs/workflow-troubleshooting/antipatterns/";
pub const WF_DIAGNOSTICS_APP_NAME: &str = "workflow-diagnostics";
pub const MAX_PAGE_SIZE: i32 = 1000;
pub const MAX_ISSUES_PER_INVARIANT: usize = 10;

pub const ISSUE_TYPE_TIMEOUTS: &str = "Timeout";
pub const ISSUE_TYPE_FAILURES: &str = "Failure";
pub const ISSUE_TYPE_RETRY: &str = "Retry";
pub const ISSUE_TYPE_TIMEOUT_RISKS: &str = "TimeoutRisk";
pub const ISSUE_TYPE_ANTIPATTERNS: &str = "Antipatterns";

/// Root cause strings from `invariant/interface.go`.
pub mod root_cause {
    pub const MISSING_POLLERS: &str = "There are no pollers for the tasklist";
    pub const POLLERS_STATUS: &str = "There are pollers for the tasklist but the task was not picked up. Check historical backlog metrics";
    pub const SCHEDULE_TO_CLOSE_TIMEOUT_POLLERS_STATUS: &str = "There are pollers for the tasklist. The task may not have been picked up or ran too long. Check historical backlog metrics and execution logs";
    pub const NO_HEARTBEAT_TIMEOUT_NO_RETRY_POLICY: &str =
        "Heartbeat timeout and retry policy are not configured";
    pub const HEARTBEATING_NOT_ENABLED_WITH_RETRY_POLICY: &str =
        "Heartbeat timeout not enabled for activity but there is a retry policy configured";
    pub const HEARTBEATING_ENABLED_WITHOUT_RETRY_POLICY: &str =
        "Heartbeat timeout enabled for activity but there is no retry policy configured";
    pub const HEARTBEATING_ENABLED_MISSING_HEARTBEAT: &str =
        "Heartbeat timeout enabled for activity but timed out due to missing heartbeat";
    pub const SERVICE_SIDE_ISSUE: &str = "There is an issue in the worker service that is causing a failure. Check identity for service logs";
    pub const SERVICE_SIDE_PANIC: &str =
        "There is a panic in the activity/workflow that is causing a failure";
    pub const SERVICE_SIDE_CUSTOM_ERROR: &str = "Customised error returned by the activity/workflow";
    pub const BLOB_SIZE_LIMIT: &str =
        "Workflow has exceeded the blob size limits configured for the domain";
    pub const HISTORY_SIZE_EXCEEDS_LIMIT: &str =
        "Workflow history has exceeded the size or event count limit configured for the domain";
}

/// `DiagnosticsStarterWorkflowInput` (Go field names on the wire).
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct DiagnosticsStarterWorkflowInput {
    pub domain: String,
    pub identity: String,
    #[serde(rename = "WorkflowID")]
    pub workflow_id: String,
    #[serde(rename = "RunID")]
    pub run_id: String,
}

/// `DiagnosticsWorkflowInput`.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct DiagnosticsWorkflowInput {
    pub domain: String,
    #[serde(rename = "WorkflowID")]
    pub workflow_id: String,
    #[serde(rename = "RunID")]
    pub run_id: String,
}

/// Which sections of `DiagnosticsWorkflowResult` are non-nil.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ResultSections {
    pub timeouts: bool,
    pub failures: bool,
    pub retries: bool,
    pub timeout_risks: bool,
    pub antipatterns: bool,
}

/// Port of `getIssueType` (parent_workflow.go): each present section prepends "-<Type>".
pub fn get_issue_type(r: ResultSections) -> String {
    let mut s = String::new();
    for (present, name) in [
        (r.timeouts, ISSUE_TYPE_TIMEOUTS),
        (r.failures, ISSUE_TYPE_FAILURES),
        (r.retries, ISSUE_TYPE_RETRY),
        (r.timeout_risks, ISSUE_TYPE_TIMEOUT_RISKS),
        (r.antipatterns, ISSUE_TYPE_ANTIPATTERNS),
    ] {
        if present {
            s = format!("{s}-{name}");
        }
    }
    s
}

/// Port of `capNumberOfIssues` (activities.go).
pub fn cap_number_of_issues<T>(mut issues: Vec<T>) -> Vec<T> {
    issues.truncate(MAX_ISSUES_PER_INVARIANT);
    issues
}

/// `analytics.WfDiagnosticsUsageData`; times are Unix milliseconds (UTC).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WfDiagnosticsUsageData {
    pub domain: String,
    pub workflow_id: String,
    pub run_id: String,
    pub identity: String,
    pub issue_type: String,
    pub diagnostics_workflow_id: String,
    pub diagnostics_run_id: String,
    pub environment: String,
    pub diagnostics_start_time_ms: i64,
    pub diagnostics_end_time_ms: i64,
    pub satisfaction_feedback: bool,
}

/// Payload bytes of the Pinot message built by `emitter.EmitUsageData`
/// (Go's `json.Marshal` of a map sorts keys; a BTreeMap does the same).
pub fn usage_payload(d: &WfDiagnosticsUsageData) -> Vec<u8> {
    use serde_json::json;
    let mut m = std::collections::BTreeMap::new();
    m.insert("domain", json!(d.domain));
    m.insert("workflowID", json!(d.workflow_id));
    m.insert("runID", json!(d.run_id));
    m.insert("identity", json!(d.identity));
    m.insert("satisfaction_feedback", json!(d.satisfaction_feedback));
    m.insert("issue_type", json!(d.issue_type));
    m.insert("diagnostics_workflowID", json!(d.diagnostics_workflow_id));
    m.insert("diagnostics_workflow_runID", json!(d.diagnostics_run_id));
    m.insert("environment", json!(d.environment));
    m.insert("diagnostics_start_time", json!(d.diagnostics_start_time_ms));
    m.insert("diagnostics_end_time", json!(d.diagnostics_end_time_ms));
    serde_json::to_vec(&m).expect("map of json values serializes")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn issue_type_matches_go_ordering() {
        assert_eq!(get_issue_type(ResultSections::default()), "");
        let all = ResultSections {
            timeouts: true,
            failures: true,
            retries: true,
            timeout_risks: true,
            antipatterns: true,
        };
        assert_eq!(
            get_issue_type(all),
            "-Timeout-Failure-Retry-TimeoutRisk-Antipatterns"
        );
        let some = ResultSections {
            failures: true,
            antipatterns: true,
            ..Default::default()
        };
        assert_eq!(get_issue_type(some), "-Failure-Antipatterns");
    }

    #[test]
    fn caps_issues_at_ten() {
        assert_eq!(cap_number_of_issues((0..25).collect::<Vec<_>>()).len(), 10);
        assert_eq!(cap_number_of_issues(vec![1, 2, 3]), vec![1, 2, 3]);
        assert!(cap_number_of_issues(Vec::<i32>::new()).is_empty());
    }

    #[test]
    fn usage_payload_has_sorted_go_keys() {
        let d = WfDiagnosticsUsageData {
            domain: "d".into(),
            workflow_id: "w".into(),
            run_id: "r".into(),
            identity: "i".into(),
            issue_type: "-Failure".into(),
            diagnostics_workflow_id: "dw".into(),
            diagnostics_run_id: "dr".into(),
            environment: "c1".into(),
            diagnostics_start_time_ms: 10,
            diagnostics_end_time_ms: 20,
            satisfaction_feedback: false,
        };
        let s = String::from_utf8(usage_payload(&d)).unwrap();
        assert_eq!(
            s,
            r#"{"diagnostics_end_time":20,"diagnostics_start_time":10,"diagnostics_workflowID":"dw","diagnostics_workflow_runID":"dr","domain":"d","environment":"c1","identity":"i","issue_type":"-Failure","runID":"r","satisfaction_feedback":false,"workflowID":"w"}"#
        );
    }

    #[test]
    fn starter_input_wire_names() {
        let i = DiagnosticsStarterWorkflowInput {
            domain: "d".into(),
            identity: "x".into(),
            workflow_id: "w".into(),
            run_id: "r".into(),
        };
        assert_eq!(
            serde_json::to_string(&i).unwrap(),
            r#"{"Domain":"d","Identity":"x","WorkflowID":"w","RunID":"r"}"#
        );
    }
}
