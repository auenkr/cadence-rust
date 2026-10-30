//! Matching's native byte-valued string model. The public protobuf model stays
//! UTF-8; conversion back to it is explicitly fallible.

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeStringError {
    pub field: &'static str,
    pub bytes: Vec<u8>,
}

impl std::fmt::Display for NativeStringError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "invalid UTF-8 at protobuf string field {}",
            self.field
        )
    }
}

impl std::error::Error for NativeStringError {}

// Generated data types intentionally mirror upstream names, including aliases
// and deprecated fields. They contain no generated transport implementation.
#[allow(clippy::all, deprecated, unused_imports, unused_variables, unused_mut)]
mod generated {
    include!(concat!(env!("OUT_DIR"), "/matching_native.rs"));
}
pub use generated::*;

// Wire primitives used by the native Message implementations emitted by
// native_model.rs. Unlike prost::encoding::string, these preserve arbitrary
// Go string bytes. Kept here so generated code and focused codec tests share
// precisely the same implementation.
#[doc(hidden)]
pub mod wire {
    use cadence_common::go_string::GoString;
    use prost::{
        bytes::{Buf, BufMut},
        encoding::{self, DecodeContext, WireType},
        DecodeError,
    };

    /// Map encoding for message values that always writes the value field,
    /// matching Go's marshaler. Decoding stays with prost's `hash_map::merge`.
    pub mod message_map {
        use prost::{
            bytes::BufMut,
            encoding::{self, WireType},
        };
        use std::collections::HashMap;
        use std::hash::Hash;

        pub fn encode<K, V, B, KE, KL, VE, VL>(
            key_encode: KE,
            key_encoded_len: KL,
            val_encode: VE,
            val_encoded_len: VL,
            tag: u32,
            values: &HashMap<K, V>,
            buf: &mut B,
        ) where
            K: Eq + Hash,
            B: BufMut,
            KE: Fn(u32, &K, &mut B),
            KL: Fn(u32, &K) -> usize,
            VE: Fn(u32, &V, &mut B),
            VL: Fn(u32, &V) -> usize,
        {
            for (key, value) in values {
                let len = key_encoded_len(1, key) + val_encoded_len(2, value);
                encoding::encode_key(tag, WireType::LengthDelimited, buf);
                encoding::encode_varint(len as u64, buf);
                key_encode(1, key, buf);
                val_encode(2, value, buf);
            }
        }

        pub fn encoded_len<K, V, KL, VL>(
            key_encoded_len: KL,
            val_encoded_len: VL,
            tag: u32,
            values: &HashMap<K, V>,
        ) -> usize
        where
            K: Eq + Hash,
            KL: Fn(u32, &K) -> usize,
            VL: Fn(u32, &V) -> usize,
        {
            encoding::key_len(tag) * values.len()
                + values
                    .iter()
                    .map(|(key, value)| {
                        let len = key_encoded_len(1, key) + val_encoded_len(2, value);
                        encoding::encoded_len_varint(len as u64) + len
                    })
                    .sum::<usize>()
        }
    }

    pub fn encode_string(tag: u32, value: &GoString, buf: &mut impl BufMut) {
        encoding::encode_key(tag, WireType::LengthDelimited, buf);
        encoding::encode_varint(value.as_bytes().len() as u64, buf);
        buf.put_slice(value.as_bytes());
    }

    pub fn encoded_len_string(tag: u32, value: &GoString) -> usize {
        encoding::key_len(tag)
            + encoding::encoded_len_varint(value.as_bytes().len() as u64)
            + value.as_bytes().len()
    }

    pub fn merge_string(
        wire_type: WireType,
        value: &mut GoString,
        buf: &mut impl Buf,
        ctx: DecodeContext,
    ) -> Result<(), DecodeError> {
        let mut bytes = Vec::new();
        encoding::bytes::merge(wire_type, &mut bytes, buf, ctx)?;
        *value = GoString::from(bytes);
        Ok(())
    }

    pub mod string {
        pub use super::{
            encode_string as encode, encoded_len_string as encoded_len, merge_string as merge,
        };
    }

    // google.protobuf.StringValue is an embedded wrapper message. Using the
    // scalar string helper directly would lose its presence and field tag.
    pub mod string_value {
        use super::*;
        use prost::Message;

        #[derive(Debug, Default)]
        struct Wrapper(GoString);

        impl Message for Wrapper {
            fn encode_raw(&self, buf: &mut impl BufMut) {
                if !self.0.is_empty() {
                    encode_string(1, &self.0, buf);
                }
            }
            fn encoded_len(&self) -> usize {
                if self.0.is_empty() {
                    0
                } else {
                    encoded_len_string(1, &self.0)
                }
            }
            fn merge_field(
                &mut self,
                tag: u32,
                wire_type: WireType,
                buf: &mut impl Buf,
                ctx: DecodeContext,
            ) -> Result<(), DecodeError> {
                if tag == 1 {
                    merge_string(wire_type, &mut self.0, buf, ctx)
                } else {
                    encoding::skip_field(wire_type, tag, buf, ctx)
                }
            }
            fn clear(&mut self) {
                self.0 = GoString::default();
            }
        }

        pub fn encode(tag: u32, value: &GoString, buf: &mut impl BufMut) {
            encoding::message::encode(tag, &Wrapper(value.clone()), buf);
        }
        pub fn encoded_len(tag: u32, value: &GoString) -> usize {
            encoding::message::encoded_len(tag, &Wrapper(value.clone()))
        }
        pub fn merge(
            wire_type: WireType,
            value: &mut GoString,
            buf: &mut impl Buf,
            ctx: DecodeContext,
        ) -> Result<(), DecodeError> {
            let mut wrapper = Wrapper(value.clone());
            encoding::message::merge(wire_type, &mut wrapper, buf, ctx)?;
            *value = wrapper.0;
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cadence_common::go_string::GoString;
    use prost::Message;

    fn unhex(value: &str) -> Vec<u8> {
        (0..value.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&value[i..i + 2], 16).unwrap())
            .collect()
    }

    #[test]
    fn native_messages_decode_original_go_raw_scalar_nested_and_map_fields() {
        let fixture = std::env::var("MATCHING_GO_RAW_STRING_ORACLE")
            .ok()
            .map(|path| std::fs::read_to_string(path).unwrap())
            .unwrap_or_else(|| {
                include_str!(
                    "../../../../memory/evidence/matching-raw-protobuf-40578298264/go-oracle.json"
                )
                .to_owned()
            });
        let rows: serde_json::Value = serde_json::from_str(&fixture).unwrap();
        for row in rows.as_array().unwrap() {
            let raw = unhex(row["hex"].as_str().unwrap());
            for key in ["inputHex", "marshalHex"] {
                let case = &row["protobuf"]["domainId"];
                let message = matching::AddDecisionTaskRequest::decode(
                    unhex(case[key].as_str().unwrap()).as_slice(),
                )
                .unwrap();
                assert_eq!(message.domain_id.as_bytes(), raw);
                let encoded = message.encode_to_vec();
                assert_eq!(encoded.len(), message.encoded_len());
                assert_eq!(
                    matching::AddDecisionTaskRequest::decode(encoded.as_slice()).unwrap(),
                    message
                );

                let case = &row["protobuf"]["workflowId"];
                let message = matching::AddDecisionTaskRequest::decode(
                    unhex(case[key].as_str().unwrap()).as_slice(),
                )
                .unwrap();
                assert_eq!(
                    message
                        .workflow_execution
                        .as_ref()
                        .unwrap()
                        .workflow_id
                        .as_bytes(),
                    raw
                );
                assert_eq!(
                    matching::AddDecisionTaskRequest::decode(message.encode_to_vec().as_slice())
                        .unwrap(),
                    message
                );

                let case = &row["protobuf"]["partitionMap"];
                let message = matching::AddDecisionTaskRequest::decode(
                    unhex(case[key].as_str().unwrap()).as_slice(),
                )
                .unwrap();
                assert_eq!(message.partition_config.len(), 1);
                assert_eq!(
                    message
                        .partition_config
                        .get(raw.as_slice())
                        .unwrap()
                        .as_bytes(),
                    raw
                );
                assert_eq!(
                    matching::AddDecisionTaskRequest::decode(message.encode_to_vec().as_slice())
                        .unwrap(),
                    message
                );

                let case = &row["protobuf"]["pollIdentity"];
                let message = matching::PollForDecisionTaskRequest::decode(
                    unhex(case[key].as_str().unwrap()).as_slice(),
                )
                .unwrap();
                assert_eq!(message.request.as_ref().unwrap().identity.as_bytes(), raw);
                assert_eq!(
                    matching::PollForDecisionTaskRequest::decode(
                        message.encode_to_vec().as_slice()
                    )
                    .unwrap(),
                    message
                );
            }
        }
    }

    #[test]
    fn native_protobuf_presence_unknown_fields_and_duplicate_scalars() {
        let original = crate::matching::PollForDecisionTaskResponse {
            previous_started_event_id: Some(0),
            attempt: 7,
            workflow_type: Some(crate::api::WorkflowType {
                name: "type".into(),
            }),
            ..Default::default()
        };
        let native =
            matching::PollForDecisionTaskResponse::decode(original.encode_to_vec().as_slice())
                .unwrap();
        assert_eq!(native.previous_started_event_id, Some(0));
        assert_eq!(
            crate::matching::PollForDecisionTaskResponse::decode(native.encode_to_vec().as_slice())
                .unwrap(),
            original
        );
        let mut wire = unhex("0a01ff0a01fe");
        // Unknown length-delimited field and repeated scalar: last value wins.
        wire.extend_from_slice(&[0xa2, 0x06, 0x01, 0xff]);
        assert_eq!(
            matching::AddDecisionTaskRequest::decode(wire.as_slice())
                .unwrap()
                .domain_id
                .as_bytes(),
            &[0xfe]
        );
        for malformed in [
            vec![0x0a, 0x02, 0xff],
            vec![0x0d, 0, 0, 0, 0],
            vec![0x42, 0x02, 0x0a],
        ] {
            assert!(matching::AddDecisionTaskRequest::decode(malformed.as_slice()).is_err());
        }
    }

    #[test]
    fn native_protobuf_duplicate_map_keys_and_oneof_follow_prost_merge_rules() {
        let wire = unhex("42060a01ff12016142060a01ff12016242070a03efbfbd1200");
        let request = matching::AddDecisionTaskRequest::decode(wire.as_slice()).unwrap();
        assert_eq!(request.partition_config.len(), 2);
        assert_eq!(
            request
                .partition_config
                .get([0xff].as_slice())
                .unwrap()
                .as_bytes(),
            b"b"
        );
        assert_eq!(
            request
                .partition_config
                .get("�".as_bytes())
                .unwrap()
                .as_bytes(),
            b""
        );
        let event = crate::api::HistoryEvent {
            attributes: Some(
                crate::api::history_event::Attributes::DecisionTaskScheduledEventAttributes(
                    Default::default(),
                ),
            ),
            ..Default::default()
        };
        let native = api::HistoryEvent::decode(event.encode_to_vec().as_slice()).unwrap();
        assert_eq!(
            crate::api::HistoryEvent::decode(native.encode_to_vec().as_slice()).unwrap(),
            event
        );
    }

    #[test]
    fn native_matching_trait_covers_the_full_matching_rpc_surface() {
        assert_eq!(NATIVE_MATCHING_METHODS.len(), 13);
        for method in [
            "PollForDecisionTask",
            "AddActivityTask",
            "AcquireSemaphore",
            "RefreshTaskListPartitionConfig",
        ] {
            assert!(
                NATIVE_MATCHING_METHODS.contains(&method),
                "missing {method}"
            );
        }
    }

    #[test]
    fn native_matching_roundtrips_valid_public_protobuf_requests() {
        let original = crate::matching::AddDecisionTaskRequest {
            domain_id: "domain".into(),
            workflow_execution: Some(crate::api::WorkflowExecution {
                workflow_id: "workflow".into(),
                run_id: "run".into(),
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
        native
            .partition_config
            .insert(GoString::from(vec![0xff]), GoString::from(vec![0xfe]));
        assert_eq!(
            native
                .partition_config
                .get([0xff_u8].as_slice())
                .unwrap()
                .as_bytes(),
            &[0xfe]
        );
        let error = crate::matching::AddDecisionTaskRequest::try_from(native).unwrap_err();
        assert_eq!(error.bytes, vec![0xff]);
        assert!(error.field.ends_with("partition_config.key"));
    }

    #[test]
    fn native_matching_nested_identity_does_not_require_utf8() {
        let native = matching::PollForDecisionTaskRequest {
            request: Some(api::PollForDecisionTaskRequest {
                identity: GoString::from(vec![0xff, 0, 0xfe]),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(
            native.request.as_ref().unwrap().identity.as_bytes(),
            &[0xff, 0, 0xfe]
        );
        let error = crate::matching::PollForDecisionTaskRequest::try_from(native).unwrap_err();
        assert_eq!(error.bytes, vec![0xff, 0, 0xfe]);
        assert!(error.field.ends_with("identity"));
    }

    #[test]
    fn raw_string_wire_primitive_preserves_oracle_bytes() {
        use prost::encoding;
        for input_hex in ["0a01ff", "0a02c0af", "0a00"] {
            let input: Vec<u8> = input_hex
                .as_bytes()
                .as_chunks::<2>()
                .0
                .iter()
                .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
                .collect();
            let mut buf = &input[..];
            let (tag, wire_type) = encoding::decode_key(&mut buf).unwrap();
            assert_eq!(tag, 1);
            let mut value = GoString::default();
            wire::merge_string(
                wire_type,
                &mut value,
                &mut buf,
                encoding::DecodeContext::default(),
            )
            .unwrap();
            assert_eq!(value.as_bytes(), &input[2..]);
            let mut encoded = Vec::new();
            wire::encode_string(1, &value, &mut encoded);
            assert_eq!(encoded, input);
            assert_eq!(wire::encoded_len_string(1, &value), encoded.len());
        }
    }
}
