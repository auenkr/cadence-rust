//! Go `nosql.convertCommonErrors` + gocql `public/client.go` error classifiers
//! applied to scylla errors (shared by the history/visibility Cassandra stores).
use scylla::errors::{DbError, ExecutionError, RequestAttemptError};
use std::any::Any;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CassandraErrorKind {
    Timeout,
    Throttled,
    DbUnavailable,
    Internal,
}

/// Go `IsTimeoutError` (deadline / no response / connection closed / write timeout),
/// `IsThrottlingError` (code 0x1001 overloaded) and `IsDBUnavailableError`
/// (Unavailable with LWT / consistency message).
pub fn classify_execution_error(error: &ExecutionError) -> CassandraErrorKind {
    match error {
        ExecutionError::RequestTimeout(_) => CassandraErrorKind::Timeout,
        ExecutionError::LastAttemptError(attempt) => match attempt {
            RequestAttemptError::BrokenConnectionError(_)
            | RequestAttemptError::DbError(DbError::WriteTimeout { .. }, _) => {
                CassandraErrorKind::Timeout
            }
            RequestAttemptError::DbError(DbError::Overloaded, _) => CassandraErrorKind::Throttled,
            RequestAttemptError::DbError(DbError::Unavailable { .. }, message)
                if message.contains("Cannot perform LWT operation")
                    || message.to_lowercase().contains("cannot achieve consistency level") =>
            {
                CassandraErrorKind::DbUnavailable
            }
            _ => CassandraErrorKind::Internal,
        },
        _ => CassandraErrorKind::Internal,
    }
}

/// Classify any displayable driver error; only `ExecutionError` carries a category.
pub fn classify<E: Any>(error: &E) -> CassandraErrorKind {
    (error as &dyn Any)
        .downcast_ref::<ExecutionError>()
        .map_or(CassandraErrorKind::Internal, classify_execution_error)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn attempt(db: DbError, msg: &str) -> ExecutionError {
        ExecutionError::LastAttemptError(RequestAttemptError::DbError(db, msg.to_owned()))
    }

    #[test]
    fn categories() {
        assert_eq!(classify(&attempt(DbError::Overloaded, "x")), CassandraErrorKind::Throttled);
        assert_eq!(
            classify(&attempt(
                DbError::Unavailable {
                    consistency: scylla::statement::Consistency::Quorum,
                    required: 2,
                    alive: 1
                },
                "Cannot achieve consistency level QUORUM"
            )),
            CassandraErrorKind::DbUnavailable
        );
        assert_eq!(
            classify(&attempt(
                DbError::Unavailable {
                    consistency: scylla::statement::Consistency::Quorum,
                    required: 2,
                    alive: 1
                },
                "other"
            )),
            CassandraErrorKind::Internal
        );
        assert_eq!(classify(&"plain string"), CassandraErrorKind::Internal);
    }
}
