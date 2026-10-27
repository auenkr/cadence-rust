//! Matching's native byte-valued string model. The public protobuf model stays
//! UTF-8; conversion back to it is explicitly fallible.

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeStringError {
    pub field: &'static str,
    pub bytes: Vec<u8>,
}

impl std::fmt::Display for NativeStringError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "invalid UTF-8 at protobuf string field {}", self.field)
    }
}

impl std::error::Error for NativeStringError {}

// Generated data types intentionally mirror upstream names, including aliases
// and deprecated fields. They contain no generated transport implementation.
#[allow(clippy::all, deprecated, unused_imports)]
mod generated {
    include!(concat!(env!("OUT_DIR"), "/matching_native.rs"));
}
pub use generated::*;

#[cfg(test)]
mod tests {
    use super::*;
    use cadence_common::go_string::GoString;

    #[test]
    fn native_matching_roundtrips_valid_public_protobuf_requests() {
        let original = crate::matching::AddDecisionTaskRequest {
            domain_id: "domain".into(),
            workflow_execution: Some(crate::api::WorkflowExecution {
                workflow_id: "workflow".into(), run_id: "run".into(),
            }),
            partition_config: [("key".to_owned(), "value".to_owned())].into(),
            ..Default::default()
        };
        let native: matching::AddDecisionTaskRequest = original.clone().into();
        let back: crate::matching::AddDecisionTaskRequest = native.try_into().unwrap();
        assert_eq!(back, original);
    }

    #[test]
    fn native_matching_preserves_invalid_string_map_entries_until_protobuf_boundary() {
        let mut native = matching::AddDecisionTaskRequest::default();
        native.partition_config.insert(GoString::from(vec![0xff]), GoString::from(vec![0xfe]));
        assert_eq!(native.partition_config.get([0xff_u8].as_slice()).unwrap().as_bytes(), &[0xfe]);
        let error = crate::matching::AddDecisionTaskRequest::try_from(native).unwrap_err();
        assert_eq!(error.bytes, vec![0xff]);
        assert!(error.field.ends_with("partition_config.key"));
    }

    #[test]
    fn native_matching_nested_identity_does_not_require_utf8() {
        let mut native = matching::PollForDecisionTaskRequest::default();
        native.request = Some(api::PollForDecisionTaskRequest {
            identity: GoString::from(vec![0xff, 0, 0xfe]),
            ..Default::default()
        });
        assert_eq!(native.request.as_ref().unwrap().identity.as_bytes(), &[0xff, 0, 0xfe]);
        let error = crate::matching::PollForDecisionTaskRequest::try_from(native).unwrap_err();
        assert_eq!(error.bytes, vec![0xff, 0, 0xfe]);
        assert!(error.field.ends_with("identity"));
    }
}
