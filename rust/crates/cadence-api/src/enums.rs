//! Cadence Thrift enum types ported from `go/common/types/shared.go`.
//!
//! Numeric values match the Go `iota` order so Thrift/binary payloads stay
//! compatible. String forms match Go `String()` output.

use serde::{Deserialize, Deserializer, Serialize, Serializer};

macro_rules! cadence_enum {
    ($name:ident { $($variant:ident = $value:expr => $s:expr,)* }) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        #[repr(i32)]
        #[non_exhaustive]
        pub enum $name {
            $($variant = $value,)*
        }
        impl Serialize for $name {
            fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                s.serialize_i32(*self as i32)
            }
        }
        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
                let v = i32::deserialize(d)?;
                Self::from_i32(v).ok_or_else(|| serde::de::Error::custom(format!(
                    "unknown {} value {v}",
                    stringify!($name),
                )))
            }
        }
        impl $name {
            #[must_use] pub fn value(self) -> i32 { self as i32 }
            #[must_use] pub fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => $s,)*
                }
            }
            pub fn values() -> &'static [$name] {
                static V: &[$name] = &[$($name::$variant,)*];
                V
            }
            #[must_use] pub fn from_i32(v: i32) -> Option<Self> {
                match v {
                    $($value => Some(Self::$variant),)*
                    _ => None,
                }
            }
            pub fn parse(s: &str) -> Option<Self> {
                let u = s.to_ascii_uppercase();
                match u.as_str() {
                    $(x if x == $s.to_ascii_uppercase().as_str() => Some(Self::$variant),)*
                    _ => s.parse::<i32>().ok().and_then(Self::from_i32),
                }
            }
        }
        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(self.as_str())
            }
        }
        impl Default for $name {
            fn default() -> Self { Self::values()[0] }
        }
    };
}

cadence_enum!(FailureCategory {
    Poll = 0 => "Poll",
    Standard = 1 => "Standard",
    Fatal = 2 => "Fatal",
});

cadence_enum!(ArchivalStatus {
    Disabled = 0 => "DISABLED",
    Enabled = 1 => "ENABLED",
});

cadence_enum!(CancelExternalWorkflowExecutionFailedCause {
    UnknownExternalWorkflowExecution = 0 => "UNKNOWN_EXTERNAL_WORKFLOW_EXECUTION",
    WorkflowAlreadyCompleted = 1 => "WORKFLOW_ALREADY_COMPLETED",
});

cadence_enum!(ChildWorkflowExecutionFailedCause {
    WorkflowAlreadyRunning = 0 => "WORKFLOW_ALREADY_RUNNING",
});

cadence_enum!(ContinueAsNewInitiator {
    Decider = 0 => "Decider",
    RetryPolicy = 1 => "RetryPolicy",
    CronSchedule = 2 => "CronSchedule",
});

cadence_enum!(DecisionTaskFailedCause {
    UnhandledDecision = 0 => "UNHANDLED_DECISION",
    BadScheduleActivityAttributes = 1 => "BAD_SCHEDULE_ACTIVITY_ATTRIBUTES",
    BadRequestCancelActivityAttributes = 2 => "BAD_REQUEST_CANCEL_ACTIVITY_ATTRIBUTES",
    BadStartTimerAttributes = 3 => "BAD_START_TIMER_ATTRIBUTES",
    BadCancelTimerAttributes = 4 => "BAD_CANCEL_TIMER_ATTRIBUTES",
    BadRecordMarkerAttributes = 5 => "BAD_RECORD_MARKER_ATTRIBUTES",
    BadCompleteWorkflowExecutionAttributes = 6 => "BAD_COMPLETE_WORKFLOW_EXECUTION_ATTRIBUTES",
    BadFailWorkflowExecutionAttributes = 7 => "BAD_FAIL_WORKFLOW_EXECUTION_ATTRIBUTES",
    BadCancelWorkflowExecutionAttributes = 8 => "BAD_CANCEL_WORKFLOW_EXECUTION_ATTRIBUTES",
    BadRequestCancelExternalWorkflowExecutionAttributes = 9 => "BAD_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_ATTRIBUTES",
    BadContinueAsNewAttributes = 10 => "BAD_CONTINUE_AS_NEW_ATTRIBUTES",
    StartTimerDuplicateID = 11 => "START_TIMER_DUPLICATE_I_D",
    ResetStickyTasklist = 12 => "RESET_STICKY_TASKLIST",
    WorkflowWorkerUnhandledFailure = 13 => "WORKFLOW_WORKER_UNHANDLED_FAILURE",
    BadSignalWorkflowExecutionAttributes = 14 => "BAD_SIGNAL_WORKFLOW_EXECUTION_ATTRIBUTES",
    BadStartChildExecutionAttributes = 15 => "BAD_START_CHILD_EXECUTION_ATTRIBUTES",
    ForceCloseDecision = 16 => "FORCE_CLOSE_DECISION",
    FailoverCloseDecision = 17 => "FAILOVER_CLOSE_DECISION",
    BadSignalInputSize = 18 => "BAD_SIGNAL_INPUT_SIZE",
    ResetWorkflow = 19 => "RESET_WORKFLOW",
    BadBinary = 20 => "BAD_BINARY",
    ScheduleActivityDuplicateID = 21 => "SCHEDULE_ACTIVITY_DUPLICATE_I_D",
    BadSearchAttributes = 22 => "BAD_SEARCH_ATTRIBUTES",
    BadAcquireSemaphoreAttributes = 23 => "BAD_ACQUIRE_SEMAPHORE_ATTRIBUTES",
    BadReleaseSemaphoreAttributes = 24 => "BAD_RELEASE_SEMAPHORE_ATTRIBUTES",
});

cadence_enum!(DecisionTaskTimedOutCause {
    Timeout = 0 => "Timeout",
    Reset = 1 => "Reset",
});

cadence_enum!(DecisionType {
    ScheduleActivityTask = 0 => "ScheduleActivityTask",
    RequestCancelActivityTask = 1 => "RequestCancelActivityTask",
    StartTimer = 2 => "StartTimer",
    CompleteWorkflowExecution = 3 => "CompleteWorkflowExecution",
    FailWorkflowExecution = 4 => "FailWorkflowExecution",
    CancelTimer = 5 => "CancelTimer",
    CancelWorkflowExecution = 6 => "CancelWorkflowExecution",
    RequestCancelExternalWorkflowExecution = 7 => "RequestCancelExternalWorkflowExecution",
    RecordMarker = 8 => "RecordMarker",
    ContinueAsNewWorkflowExecution = 9 => "ContinueAsNewWorkflowExecution",
    StartChildWorkflowExecution = 10 => "StartChildWorkflowExecution",
    SignalExternalWorkflowExecution = 11 => "SignalExternalWorkflowExecution",
    UpsertWorkflowSearchAttributes = 12 => "UpsertWorkflowSearchAttributes",
    AcquireSemaphore = 13 => "AcquireSemaphore",
    ReleaseSemaphore = 14 => "ReleaseSemaphore",
});

// FailoverType uses explicit discriminants in Go (Invalid=0, Force=1, Graceful=2).
cadence_enum!(FailoverType { Invalid = 0 => "INVALID", Force = 1 => "FORCE", Graceful = 2 => "GRACEFUL", });

cadence_enum!(DomainStatus {
    Registered = 0 => "REGISTERED",
    Deprecated = 1 => "DEPRECATED",
    Deleted = 2 => "DELETED",
});

cadence_enum!(EncodingType {
    ThriftRW = 0 => "ThriftRW",
    JSON = 1 => "JSON",
});

cadence_enum!(EventType {
    WorkflowExecutionStarted = 0 => "WorkflowExecutionStarted",
    WorkflowExecutionCompleted = 1 => "WorkflowExecutionCompleted",
    WorkflowExecutionFailed = 2 => "WorkflowExecutionFailed",
    WorkflowExecutionTimedOut = 3 => "WorkflowExecutionTimedOut",
    DecisionTaskScheduled = 4 => "DecisionTaskScheduled",
    DecisionTaskStarted = 5 => "DecisionTaskStarted",
    DecisionTaskCompleted = 6 => "DecisionTaskCompleted",
    DecisionTaskTimedOut = 7 => "DecisionTaskTimedOut",
    DecisionTaskFailed = 8 => "DecisionTaskFailed",
    ActivityTaskScheduled = 9 => "ActivityTaskScheduled",
    ActivityTaskStarted = 10 => "ActivityTaskStarted",
    ActivityTaskCompleted = 11 => "ActivityTaskCompleted",
    ActivityTaskFailed = 12 => "ActivityTaskFailed",
    ActivityTaskTimedOut = 13 => "ActivityTaskTimedOut",
    ActivityTaskCancelRequested = 14 => "ActivityTaskCancelRequested",
    RequestCancelActivityTaskFailed = 15 => "RequestCancelActivityTaskFailed",
    ActivityTaskCanceled = 16 => "ActivityTaskCanceled",
    TimerStarted = 17 => "TimerStarted",
    TimerFired = 18 => "TimerFired",
    CancelTimerFailed = 19 => "CancelTimerFailed",
    TimerCanceled = 20 => "TimerCanceled",
    WorkflowExecutionCancelRequested = 21 => "WorkflowExecutionCancelRequested",
    WorkflowExecutionCanceled = 22 => "WorkflowExecutionCanceled",
    RequestCancelExternalWorkflowExecutionInitiated = 23 => "RequestCancelExternalWorkflowExecutionInitiated",
    RequestCancelExternalWorkflowExecutionFailed = 24 => "RequestCancelExternalWorkflowExecutionFailed",
    ExternalWorkflowExecutionCancelRequested = 25 => "ExternalWorkflowExecutionCancelRequested",
    MarkerRecorded = 26 => "MarkerRecorded",
    WorkflowExecutionSignaled = 27 => "WorkflowExecutionSignaled",
    WorkflowExecutionTerminated = 28 => "WorkflowExecutionTerminated",
    WorkflowExecutionContinuedAsNew = 29 => "WorkflowExecutionContinuedAsNew",
    StartChildWorkflowExecutionInitiated = 30 => "StartChildWorkflowExecutionInitiated",
    StartChildWorkflowExecutionFailed = 31 => "StartChildWorkflowExecutionFailed",
    ChildWorkflowExecutionStarted = 32 => "ChildWorkflowExecutionStarted",
    ChildWorkflowExecutionCompleted = 33 => "ChildWorkflowExecutionCompleted",
    ChildWorkflowExecutionFailed = 34 => "ChildWorkflowExecutionFailed",
    ChildWorkflowExecutionCanceled = 35 => "ChildWorkflowExecutionCanceled",
    ChildWorkflowExecutionTimedOut = 36 => "ChildWorkflowExecutionTimedOut",
    ChildWorkflowExecutionTerminated = 37 => "ChildWorkflowExecutionTerminated",
    SignalExternalWorkflowExecutionInitiated = 38 => "SignalExternalWorkflowExecutionInitiated",
    SignalExternalWorkflowExecutionFailed = 39 => "SignalExternalWorkflowExecutionFailed",
    ExternalWorkflowExecutionSignaled = 40 => "ExternalWorkflowExecutionSignaled",
    UpsertWorkflowSearchAttributes = 41 => "UpsertWorkflowSearchAttributes",
    SemaphoreAcquireInitiated = 42 => "SemaphoreAcquireInitiated",
    SemaphoreAcquired = 43 => "SemaphoreAcquired",
    SemaphoreReleased = 44 => "SemaphoreReleased",
});

cadence_enum!(HistoryEventFilterType {
    AllEvent = 0 => "ALL_EVENT",
    CloseEvent = 1 => "CLOSE_EVENT",
});

cadence_enum!(IndexedValueType {
    String = 0 => "STRING",
    Keyword = 1 => "KEYWORD",
    Int = 2 => "INT",
    Double = 3 => "DOUBLE",
    Bool = 4 => "BOOL",
    Datetime = 5 => "DATETIME",
});

cadence_enum!(ParentClosePolicy {
    Abandon = 0 => "ABANDON",
    RequestCancel = 1 => "REQUEST_CANCEL",
    Terminate = 2 => "TERMINATE",
});

cadence_enum!(PendingActivityState {
    Scheduled = 0 => "SCHEDULED",
    Started = 1 => "STARTED",
    CancelRequested = 2 => "CANCEL_REQUESTED",
});

cadence_enum!(PendingDecisionState {
    Scheduled = 0 => "SCHEDULED",
    Started = 1 => "STARTED",
});

cadence_enum!(QueryConsistencyLevel {
    Eventual = 0 => "EVENTUAL",
    Strong = 1 => "STRONG",
});

cadence_enum!(QueryRejectCondition {
    NotOpen = 0 => "NOT_OPEN",
    NotCompletedCleanly = 1 => "NOT_COMPLETED_CLEANLY",
});

cadence_enum!(QueryResultType {
    Answered = 0 => "ANSWERED",
    Failed = 1 => "FAILED",
});

cadence_enum!(QueryTaskCompletedType {
    Completed = 0 => "COMPLETED",
    Failed = 1 => "FAILED",
});

cadence_enum!(SignalExternalWorkflowExecutionFailedCause {
    UnknownExternalWorkflowExecution = 0 => "UNKNOWN_EXTERNAL_WORKFLOW_EXECUTION",
    WorkflowAlreadyCompleted = 1 => "WORKFLOW_ALREADY_COMPLETED",
});

cadence_enum!(TaskListKind {
    Normal = 0 => "NORMAL",
    Sticky = 1 => "STICKY",
    Ephemeral = 2 => "EPHEMERAL",
});

cadence_enum!(TaskListType {
    Decision = 0 => "Decision",
    Activity = 1 => "Activity",
});

cadence_enum!(TimeoutType {
    StartToClose = 0 => "START_TO_CLOSE",
    ScheduleToStart = 1 => "SCHEDULE_TO_START",
    ScheduleToClose = 2 => "SCHEDULE_TO_CLOSE",
    Heartbeat = 3 => "HEARTBEAT",
});

cadence_enum!(WorkflowExecutionCloseStatus {
    Completed = 0 => "COMPLETED",
    Failed = 1 => "FAILED",
    Canceled = 2 => "CANCELED",
    Terminated = 3 => "TERMINATED",
    ContinuedAsNew = 4 => "CONTINUED_AS_NEW",
    TimedOut = 5 => "TIMED_OUT",
});

cadence_enum!(WorkflowExecutionStatus {
    Pending = 0 => "PENDING",
    Started = 1 => "STARTED",
    Completed = 2 => "COMPLETED",
    Failed = 3 => "FAILED",
    Canceled = 4 => "CANCELED",
    Terminated = 5 => "TERMINATED",
    ContinuedAsNew = 6 => "CONTINUED_AS_NEW",
    TimedOut = 7 => "TIMED_OUT",
});

cadence_enum!(WorkflowIDReusePolicy {
    AllowDuplicateFailedOnly = 0 => "AllowDuplicateFailedOnly",
    AllowDuplicate = 1 => "AllowDuplicate",
    RejectDuplicate = 2 => "RejectDuplicate",
    TerminateIfRunning = 3 => "TerminateIfRunning",
});

cadence_enum!(CrossClusterTaskType {
    StartChildExecution = 0 => "StartChildExecution",
    CancelExecution = 1 => "CancelExecution",
    SignalExecution = 2 => "SignalExecution",
    RecordChildWorkflowExecutionComplete = 3 => "RecordChildWorkflowExecutionComplete",
    ApplyParentClosePolicy = 4 => "ApplyParentClosePolicy",
});

cadence_enum!(CrossClusterTaskFailedCause {
    DomainNotActive = 0 => "DOMAIN_NOT_ACTIVE",
    DomainNotExists = 1 => "DOMAIN_NOT_EXISTS",
    WorkflowAlreadyRunning = 2 => "WORKFLOW_ALREADY_RUNNING",
    WorkflowNotExists = 3 => "WORKFLOW_NOT_EXISTS",
    WorkflowAlreadyCompleted = 4 => "WORKFLOW_ALREADY_COMPLETED",
    Uncategorized = 5 => "UNCATEGORIZED",
});

cadence_enum!(GetTaskFailedCause {
    ServiceBusy = 0 => "SERVICE_BUSY",
    Timeout = 1 => "TIMEOUT",
    ShardOwnershipLost = 2 => "SHARD_OWNERSHIP_LOST",
    Uncategorized = 3 => "UNCATEGORIZED",
});

cadence_enum!(CronOverlapPolicy { Skipped = 0 => "SKIP", BufferOne = 1 => "BUFFERONE", });

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_type_values_match_go_iota_order() {
        assert_eq!(EventType::WorkflowExecutionStarted.value(), 0);
        assert_eq!(EventType::DecisionTaskScheduled.value(), 4);
        assert_eq!(EventType::values().len(), 45);
    }

    #[test]
    fn decision_type_values_match_go() {
        assert_eq!(DecisionType::values().len(), 15);
        assert_eq!(DecisionType::ScheduleActivityTask.value(), 0);
        assert_eq!(DecisionType::ReleaseSemaphore.value(), 14);
    }

    #[test]
    fn display_matches_go_string_output() {
        assert_eq!(ArchivalStatus::Disabled.to_string(), "DISABLED");
        assert_eq!(TimeoutType::StartToClose.to_string(), "START_TO_CLOSE");
        assert_eq!(
            EventType::WorkflowExecutionStarted.to_string(),
            "WorkflowExecutionStarted"
        );
        assert_eq!(
            WorkflowIDReusePolicy::AllowDuplicateFailedOnly.to_string(),
            "AllowDuplicateFailedOnly"
        );
    }

    #[test]
    fn enums_serialize_as_thrift_ints() {
        let v: serde_json::Value = serde_json::to_value(EventType::ActivityTaskScheduled).unwrap();
        assert_eq!(v, serde_json::Value::Number(9.into()));
        let back: EventType = serde_json::from_value(v).unwrap();
        assert_eq!(back, EventType::ActivityTaskScheduled);
    }

    #[test]
    fn parse_accepts_names_and_numbers() {
        assert_eq!(TaskListKind::parse("NORMAL"), Some(TaskListKind::Normal));
        assert_eq!(TaskListKind::parse("sticky"), Some(TaskListKind::Sticky));
        assert_eq!(EncodingType::parse("1"), Some(EncodingType::JSON));
        assert_eq!(DomainStatus::parse("NOPE"), None);
    }

    #[test]
    fn failover_type_explicit_discriminants() {
        assert_eq!(FailoverType::Invalid.value(), 0);
        assert_eq!(FailoverType::Force.value(), 1);
        assert_eq!(FailoverType::Graceful.value(), 2);
    }
}
