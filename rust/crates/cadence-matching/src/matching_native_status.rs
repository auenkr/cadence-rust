//! UNVERIFIED (no Cargo run yet): Go-exact gRPC error envelope for the
//! unary native Matching service.
//!
//! `tonic::Status::message` is a `String`, while Go Matching errors are
//! arbitrary byte strings (`GoString`). grpc-go (v1.83.1,
//! `internal/transport/http_util.go: encodeGrpcMessageUnchecked`) writes
//! `grpc-message` by decoding the Go string rune by rune: every byte of a
//! valid multi-byte rune and every byte outside `0x20..=0x7e` (and `%`) is
//! percent-encoded, and each invalid byte decodes as `utf8.RuneError`, whose
//! bytes `EF BF BD` are percent-encoded. So the Go wire never carries `%FF`
//! for an invalid byte; it carries `%EF%BF%BD` once per invalid byte. This
//! module reproduces that encoding from the original bytes, which tonic's
//! own `String` message + `ENCODING_SET` cannot do (different escape set,
//! lossy replacement per maximal sequence).
//!
//! Mechanism: `raw_status` builds a tonic `Status` carrying the original
//! message bytes in a private binary metadata marker. `RawStatusService`
//! wraps the generated `MatchingApiServer`; for responses carrying the
//! marker it removes the marker and rewrites the initial `grpc-message`
//! header. tonic 0.12 unary handlers return errors via `Status::into_http`
//! (trailers-only response: grpc-status / grpc-message / details are plain
//! HTTP headers), so this is sufficient for unary methods only. Streaming
//! errors travel in trailers and are NOT handled here.
//! `grpc-status` and `grpc-status-details-bin` are left untouched.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use base64::alphabet;
use base64::engine::{DecodePaddingMode, GeneralPurpose, GeneralPurposeConfig};
use base64::Engine;
use cadence_common::go_string::GoString;
use tonic::codegen::http::{self, HeaderValue};
use tonic::codegen::Service;
use tonic::metadata::{BinaryMetadataKey, BinaryMetadataValue};
use tonic::server::NamedService;
use tonic::{Code, Status};

/// Private marker header (binary metadata) holding the original message.
pub const RAW_MESSAGE_MARKER: &str = "x-cadence-raw-grpc-message-bin";

/// Build a `Status` whose wire `grpc-message` is the Go encoding of
/// `message`'s exact bytes once passed through `RawStatusService`.
/// `details` is forwarded untouched as `grpc-status-details-bin`.
pub fn raw_status(code: Code, message: &GoString, details: tonic::codegen::Bytes) -> Status {
    let mut status = Status::with_details(
        code,
        String::from_utf8_lossy(message.as_bytes()).into_owned(),
        details,
    );
    status.metadata_mut().insert_bin(
        BinaryMetadataKey::from_static(RAW_MESSAGE_MARKER),
        BinaryMetadataValue::from_bytes(message.as_bytes()),
    );
    status
}

// google.rpc.Status.message is a protobuf string on Go's wire. A bytes
// field has the same length-delimited wire encoding and retains its contents.
#[derive(Clone, PartialEq, prost::Message)]
struct RpcStatusBytes {
    #[prost(int32, tag = "1")]
    code: i32,
    #[prost(bytes = "vec", tag = "2")]
    message: Vec<u8>,
    #[prost(message, repeated, tag = "3")]
    details: Vec<prost_types::Any>,
}

/// Cadence typed error details with a byte-preserving google.rpc.Status
/// message. `detail` is the already encoded api.v1 error message.
pub fn raw_detailed_status(code: Code, message: &GoString, name: &str, detail: Vec<u8>) -> Status {
    use prost::Message;
    // grpc-go converts gogo/status into google.rpc.Status, then marshals it
    // with protobuf/v2. An invalid UTF-8 Status.message fails that marshal;
    // grpc-go still sends grpc-message, but omits grpc-status-details-bin.
    if message.as_utf8().is_err() {
        return raw_status(code, message, tonic::codegen::Bytes::new());
    }
    let envelope = RpcStatusBytes {
        code: code as i32,
        message: message.as_bytes().to_vec(),
        details: vec![prost_types::Any {
            type_url: format!("type.googleapis.com/uber.cadence.api.v1.{name}"),
            value: detail,
        }],
    };
    raw_status(code, message, envelope.encode_to_vec().into())
}

/// grpc-go `encodeGrpcMessage` over arbitrary bytes.
pub fn encode_go_grpc_message(msg: &[u8]) -> String {
    let mut out = String::with_capacity(msg.len());
    let push = |out: &mut String, b: u8| out.push_str(&format!("%{b:02X}"));
    let mut rest = msg;
    while !rest.is_empty() {
        match std::str::from_utf8(rest) {
            Ok(s) => {
                encode_valid(&mut out, s);
                break;
            }
            Err(e) => {
                let (valid, tail) = rest.split_at(e.valid_up_to());
                encode_valid(&mut out, std::str::from_utf8(valid).unwrap());
                // Go decodes one RuneError (size 1) per invalid byte.
                let bad = e.error_len().unwrap_or(tail.len());
                for _ in 0..bad {
                    for b in "\u{FFFD}".bytes() {
                        push(&mut out, b);
                    }
                }
                rest = &tail[bad..];
            }
        }
    }
    out
}

fn encode_valid(out: &mut String, s: &str) {
    for b in s.bytes() {
        if (b' '..=b'~').contains(&b) && b != b'%' {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
}

fn marker_engine() -> GeneralPurpose {
    GeneralPurpose::new(
        &alphabet::STANDARD,
        GeneralPurposeConfig::new().with_decode_padding_mode(DecodePaddingMode::Indifferent),
    )
}

/// Rewrite one response in place. Unmarked responses are untouched.
fn rewrite_headers<B>(response: &mut http::Response<B>) {
    let Some(marker) = response.headers_mut().remove(RAW_MESSAGE_MARKER) else {
        return;
    };
    let raw = marker_engine()
        .decode(marker.as_bytes())
        .expect("marker is written by raw_status as base64");
    let value = HeaderValue::from_str(&encode_go_grpc_message(&raw))
        .expect("percent-encoded message is visible ASCII");
    response.headers_mut().insert("grpc-message", value);
}

/// Wrapper for a unary tonic service (e.g. `MatchingApiServer<_>`).
#[derive(Clone, Debug)]
pub struct RawStatusService<S>(S);

impl<S> RawStatusService<S> {
    pub fn new(inner: S) -> Self {
        Self(inner)
    }
}

impl<S: NamedService> NamedService for RawStatusService<S> {
    const NAME: &'static str = S::NAME;
}

impl<S, ReqBody, ResBody> Service<http::Request<ReqBody>> for RawStatusService<S>
where
    S: Service<http::Request<ReqBody>, Response = http::Response<ResBody>>,
    S::Future: Send + 'static,
    S::Error: 'static,
    ResBody: 'static,
{
    type Response = http::Response<ResBody>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.0.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<ReqBody>) -> Self::Future {
        let fut = self.0.call(req);
        Box::pin(async move {
            let mut response = fut.await?;
            rewrite_headers(&mut response);
            Ok(response)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::body::BoxBody;
    use tonic::codegen::Bytes;
    use tower::ServiceExt;

    #[derive(Clone)]
    struct Fail(Option<Status>);

    impl NamedService for Fail {
        const NAME: &'static str = "uber.cadence.matching.v1.MatchingAPI";
    }

    impl Service<http::Request<()>> for Fail {
        type Response = http::Response<BoxBody>;
        type Error = std::convert::Infallible;
        type Future = std::future::Ready<Result<Self::Response, Self::Error>>;
        fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, _: http::Request<()>) -> Self::Future {
            let status = self.0.clone().unwrap();
            std::future::ready(Ok(status.into_http()))
        }
    }

    async fn run(status: Status) -> http::Response<BoxBody> {
        RawStatusService::new(Fail(Some(status)))
            .oneshot(http::Request::new(()))
            .await
            .unwrap()
    }

    async fn msg(bytes: &[u8]) -> http::Response<BoxBody> {
        run(raw_status(
            Code::NotFound,
            &GoString::from(bytes),
            Bytes::new(),
        ))
        .await
    }

    fn header(r: &http::Response<BoxBody>) -> &str {
        r.headers()["grpc-message"].to_str().unwrap()
    }

    #[tokio::test]
    async fn invalid_bytes_match_go_rune_error_per_byte() {
        assert_eq!(header(&msg(b"a\xffb").await), "a%EF%BF%BDb");
        assert_eq!(header(&msg(b"a\xfeb").await), "a%EF%BF%BDb");
        assert_eq!(header(&msg(b"\xe2\x82").await), "%EF%BF%BD%EF%BF%BD");
        assert_ne!(header(&msg(b"\xff").await), "%FF");
        // Valid U+FFFD input gives the same bytes as an invalid byte (Go).
        assert_eq!(header(&msg("\u{FFFD}".as_bytes()).await), "%EF%BF%BD");
    }

    #[tokio::test]
    async fn escapes_match_go() {
        assert_eq!(
            header(&msg(b"a\0b\nc%d e~\x7f").await),
            "a%00b%0Ac%25d e~%7F"
        );
        assert_eq!(header(&msg("é".as_bytes()).await), "%C3%A9");
        assert_eq!(header(&msg(b"plain \"?#<>`{}").await), "plain \"?#<>`{}");
    }

    #[tokio::test]
    async fn empty_message_and_status_details_preserved() {
        let raw_details = Bytes::from_static(b"\x0a\x03\xff\x00\x01");
        let r = run(raw_status(
            Code::InvalidArgument,
            &GoString::default(),
            raw_details.clone(),
        ))
        .await;
        assert_eq!(header(&r), "");
        assert_eq!(r.headers()["grpc-status"], "3");
        let want = base64::engine::general_purpose::STANDARD_NO_PAD.encode(&raw_details);
        assert_eq!(r.headers()["grpc-status-details-bin"], want.as_str());
        assert!(r.headers().get(RAW_MESSAGE_MARKER).is_none());
    }

    #[tokio::test]
    async fn marker_removed_and_unmarked_unchanged() {
        let r = msg(b"x\xff").await;
        assert!(r.headers().get(RAW_MESSAGE_MARKER).is_none());
        let plain = run(Status::not_found("a b%c")).await;
        let direct = Status::not_found("a b%c").into_http();
        assert_eq!(plain.headers(), direct.headers());
    }

    #[tokio::test]
    async fn typed_error_envelope_keeps_raw_message_and_detail_fields() {
        use prost::Message;
        let message = GoString::from("Client version not supported");
        // ClientVersionNotSupportedError.feature_version carries one raw byte.
        let detail = vec![0x0a, 0x01, 0xff];
        let status = raw_detailed_status(
            Code::FailedPrecondition,
            &message,
            "ClientVersionNotSupportedError",
            detail.clone(),
        );
        let envelope = RpcStatusBytes::decode(status.details()).unwrap();
        assert_eq!(envelope.code, Code::FailedPrecondition as i32);
        assert_eq!(envelope.message, message.as_bytes());
        assert_eq!(envelope.details[0].value, detail);
        assert_eq!(
            envelope.details[0].type_url,
            "type.googleapis.com/uber.cadence.api.v1.ClientVersionNotSupportedError"
        );
        let response = run(status).await;
        assert_eq!(header(&response), "Client version not supported");
        assert!(!response.headers().contains_key(RAW_MESSAGE_MARKER));
        let wire_details = marker_engine()
            .decode(response.headers()["grpc-status-details-bin"].as_bytes())
            .unwrap();
        let decoded = RpcStatusBytes::decode(wire_details.as_slice()).unwrap();
        assert_eq!(decoded, envelope);
    }

    #[tokio::test]
    async fn invalid_status_message_omits_details_like_grpc_go() {
        let message = GoString::from(&b"worker error: \xe2\x82"[..]);
        let status = raw_detailed_status(
            Code::InvalidArgument,
            &message,
            "QueryFailedError",
            Vec::new(),
        );
        assert!(status.details().is_empty());
        let response = run(status).await;
        assert_eq!(header(&response), "worker error: %EF%BF%BD%EF%BF%BD");
        assert!(!response.headers().contains_key("grpc-status-details-bin"));
        assert!(!response.headers().contains_key(RAW_MESSAGE_MARKER));
    }

    #[test]
    fn typed_query_envelopes_match_pinned_go_oracle_when_supplied() {
        use prost::Message;
        let Ok(path) = std::env::var("MATCHING_GO_RAW_STRING_ORACLE") else {
            return;
        };
        let rows: serde_json::Value =
            serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        let hex = |bytes: &[u8]| {
            bytes
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()
        };
        for row in rows.as_array().unwrap() {
            let raw_hex = row["hex"].as_str().unwrap().as_bytes();
            let raw: Vec<u8> = raw_hex
                .as_chunks::<2>()
                .0
                .iter()
                .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
                .collect();
            let raw = GoString::from(raw);
            let query = crate::matching_query_native::NativeQueryError::QueryFailed {
                message: raw.clone(),
            }
            .into_status();
            assert_eq!(
                hex(query.details()),
                row["grpcQueryError"]["transportDetailsHex"]
                    .as_str()
                    .unwrap()
            );
            let version =
                crate::matching_query_native::NativeQueryError::ClientVersionNotSupported {
                    feature_version: raw.clone(),
                    client_impl: raw,
                    supported_versions: GoString::from(">=1.5.0"),
                }
                .into_status();
            assert_eq!(
                hex(version.details()),
                row["grpcVersionError"]["transportDetailsHex"]
                    .as_str()
                    .unwrap()
            );
            assert_eq!(
                RpcStatusBytes::decode(version.details()).unwrap().code,
                Code::FailedPrecondition as i32
            );
        }
    }

    #[test]
    fn named_service_forwarded() {
        assert_eq!(
            <RawStatusService<Fail> as NamedService>::NAME,
            "uber.cadence.matching.v1.MatchingAPI"
        );
    }
}
