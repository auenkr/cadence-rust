//! Default search attribute registry ported from Go common/definition/indexedKeys.go.
use crate::enums::IndexedValueType as T;
pub fn default_indexed_keys() -> std::collections::HashMap<String, T> {
    [
        ("BinaryChecksums".to_string(), T::Keyword),
        ("CadenceChangeVersion".to_string(), T::Keyword),
        ("CadenceScheduleBackfillID".to_string(), T::Keyword),
        ("CadenceScheduleCron".to_string(), T::Keyword),
        ("CadenceScheduleID".to_string(), T::Keyword),
        ("CadenceScheduleIsBackfill".to_string(), T::Bool),
        ("CadenceScheduleState".to_string(), T::Keyword),
        ("CadenceScheduleTime".to_string(), T::Datetime),
        ("CadenceScheduleWorkflowType".to_string(), T::Keyword),
        ("CloseStatus".to_string(), T::Int),
        ("CloseTime".to_string(), T::Int),
        ("ClusterAttributeName".to_string(), T::Keyword),
        ("ClusterAttributeScope".to_string(), T::Keyword),
        ("CronSchedule".to_string(), T::Keyword),
        ("CustomBoolField".to_string(), T::Bool),
        ("CustomDatetimeField".to_string(), T::Datetime),
        ("CustomDomain".to_string(), T::String),
        ("CustomDoubleField".to_string(), T::Double),
        ("CustomIntField".to_string(), T::Int),
        ("CustomKeywordField".to_string(), T::Keyword),
        ("CustomStringField".to_string(), T::String),
        ("DomainID".to_string(), T::Keyword),
        ("ExecutionStatus".to_string(), T::Int),
        ("ExecutionTime".to_string(), T::Int),
        ("HistoryLength".to_string(), T::Int),
        ("IsCron".to_string(), T::Bool),
        ("NumClusters".to_string(), T::Int),
        ("Operator".to_string(), T::String),
        ("RunID".to_string(), T::Keyword),
        ("ScheduledExecutionTime".to_string(), T::Int),
        ("StartTime".to_string(), T::Int),
        ("TaskList".to_string(), T::Keyword),
        ("UpdateTime".to_string(), T::Int),
        ("WorkflowID".to_string(), T::Keyword),
        ("WorkflowType".to_string(), T::Keyword),
    ]
    .into_iter()
    .collect()
}

/// Port of Go `definition.IsSystemIndexedKey`: keys reserved by the system
/// that `AddSearchAttribute` must reject.
pub fn is_system_indexed_key(key: &str) -> bool {
    matches!(
        key,
        "DomainID"
            | "WorkflowID"
            | "RunID"
            | "WorkflowType"
            | "StartTime"
            | "ExecutionTime"
            | "CloseTime"
            | "CloseStatus"
            | "HistoryLength"
            | "TaskList"
            | "IsCron"
            | "NumClusters"
            | "UpdateTime"
            | "CronSchedule"
            | "ExecutionStatus"
            | "ScheduledExecutionTime"
            | "ClusterAttributeScope"
            | "ClusterAttributeName"
            | "CadenceScheduleID"
            | "CadenceScheduleTime"
            | "CadenceScheduleIsBackfill"
            | "CadenceScheduleState"
            | "CadenceScheduleCron"
            | "CadenceScheduleWorkflowType"
            | "CadenceScheduleBackfillID"
    )
}

#[cfg(test)]
mod tests {
    #[test]
    fn system_keys_match_go() {
        assert!(super::is_system_indexed_key("DomainID"));
        assert!(super::is_system_indexed_key("CadenceScheduleID"));
        assert!(!super::is_system_indexed_key("CustomKeywordField"));
        assert!(!super::is_system_indexed_key("MyField"));
    }
}
