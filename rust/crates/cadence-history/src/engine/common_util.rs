//! Ports of the `go/common/util.go` helpers used by the history engine.

use super::deps::HistoryError;
use cadence_api::errors::BadRequestError;

/// `common.IsValidIDLength`. Metrics/logging are reported through `tracing`;
/// the caller increments its own counter when `warn` is returned true.
/// Returns `(valid, exceeded_warn_limit)`.
pub fn is_valid_id_length(id: &str, warn_limit: usize, error_limit: usize) -> (bool, bool) {
    (id.len() <= error_limit, id.len() > warn_limit)
}

/// `common.ValidateDomainUUID`
pub fn validate_domain_uuid(domain_uuid: &str) -> Result<(), HistoryError> {
    if domain_uuid.is_empty() {
        Err(BadRequestError::new("Missing domain UUID.").into())
    } else if uuid::Uuid::parse_str(domain_uuid).is_err() {
        Err(BadRequestError::new("Invalid domain UUID.").into())
    } else {
        Ok(())
    }
}
