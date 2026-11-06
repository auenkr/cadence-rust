//! Port of `go/service/history/common/{errors,type}.go` and `go/service/history/lookup/lookup.go`.
use std::error::Error;

use cadence_api::errors::{LimitExceededError, ServiceBusyError, WorkflowExecutionAlreadyStartedError};
use cadence_persistence::history_contract::PersistenceError;
use cadence_persistence::history_sql::HistoryPersistenceError;

/// Go `IsOperationPossiblySuccessfulError`: returns true for errors where a persistence
/// write may have succeeded despite the error being returned (e.g. timeout, unknown
/// network error). Returns false for errors that definitively indicate the write
/// did not occur. `None` is Go's `nil` error.
pub fn is_operation_possibly_successful_error(err: Option<&(dyn Error + 'static)>) -> bool {
    let Some(err) = err else {
        return false;
    };
    if err.is::<WorkflowExecutionAlreadyStartedError>()
        || err.is::<ServiceBusyError>()
        || err.is::<LimitExceededError>()
    {
        return false;
    }
    if let Some(e) = err.downcast_ref::<PersistenceError>() {
        return !matches!(
            e,
            PersistenceError::WorkflowAlreadyStarted { .. }
                | PersistenceError::CurrentWorkflowConditionFailed(_)
                | PersistenceError::ConditionFailed(_)
                | PersistenceError::ShardOwnershipLost { .. }
        );
    }
    if let Some(e) = err.downcast_ref::<HistoryPersistenceError>() {
        return !matches!(
            e,
            HistoryPersistenceError::WorkflowAlreadyStarted(_)
                | HistoryPersistenceError::CurrentWorkflowConditionFailed(_)
                | HistoryPersistenceError::ConditionFailed(_)
                | HistoryPersistenceError::ShardOwnershipLost { .. }
        );
    }
    true
}

/// Go `lookup.HistoryServerByShardID` key: `string(rune(shardID))`.
pub use crate::shard_context::history_server_key_by_shard_id;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn possibly_successful() {
        assert!(!is_operation_possibly_successful_error(None));
        let busy = ServiceBusyError::default();
        assert!(!is_operation_possibly_successful_error(Some(&busy)));
        let lost = PersistenceError::ShardOwnershipLost { shard_id: 1, expected_range_id: 1, actual_range_id: 2 };
        assert!(!is_operation_possibly_successful_error(Some(&lost)));
        let timeout = PersistenceError::Timeout("t".into());
        assert!(is_operation_possibly_successful_error(Some(&timeout)));
        let other = std::fmt::Error;
        assert!(is_operation_possibly_successful_error(Some(&other)));
    }
}
