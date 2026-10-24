//! Ports of Go `handler/context_test.go` (error mapping) and
//! `semaphore/identifier_test.go::TestParseRequestOwner`.
use super::semaphore_status;
use crate::matching_semaphore::ManagerError;
use cadence_common::semaphore::{parse_owner, Owner};
use tonic::Code;

// Go TestHandleErrKnowErrors: each known error keeps its category.
#[test]
fn matching_handler_semaphore_errors_keep_their_category() {
    let cases = [
        (ManagerError::BadRequest("x".into()), Code::InvalidArgument),
        (ManagerError::Disabled, Code::InvalidArgument),
        (ManagerError::NotFound("x".into()), Code::NotFound),
        (ManagerError::Deadline, Code::DeadlineExceeded),
        (ManagerError::NotReady, Code::ResourceExhausted),
        (
            ManagerError::ServiceBusy("x".into()),
            Code::ResourceExhausted,
        ),
    ];
    for (error, code) in cases {
        let label = format!("{error:?}");
        assert_eq!(semaphore_status(error).code(), code, "{label}");
    }
}

// Go TestHandleErrUncategorizedError: an unknown error becomes Internal.
#[test]
fn matching_handler_uncategorized_error_is_internal() {
    let status = semaphore_status(ManagerError::Internal("boom".into()));
    assert_eq!(status.code(), Code::Internal);
    assert_eq!(status.message(), "boom");
}

// Go TestParseRequestOwner.
#[test]
fn matching_semaphore_parse_request_owner_matches_go() {
    let valid = Owner {
        workflow_id: "wf-1".into(),
        run_id: "run-1".into(),
        hold_id: 7,
    };
    assert_eq!(parse_owner(&valid.encode()).unwrap(), valid);
    assert!(parse_owner("wf-1").is_err(), "no length prefix");
    assert!(parse_owner("").is_err(), "empty");
}
