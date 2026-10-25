# Matching TChannel TLS parity

Result: Go does NOT serve TLS on the TChannel inbound; no Rust change needed.

Proof (go/common/rpc/factory.go):
- L63-65: `tchannel.NewChannelTransport(ServiceName, ListenAddr)` - no TLS option.
- L69: `tchannel.NewTransport(ServiceName)` - no TLS option.
- L75: `ch.NewInbound()` plain inbound.
- L91-92: `p.InboundTLS` is applied only to the gRPC inbound (`grpc.InboundCredentials`).
- L115-116: HTTP inbound has its own TLS/Mode.
yarpc tchannel has no inbound TLS support. Rust: gRPC TLS (grpc_tls) and HTTP TLS (launcher.rs matching_http_tls) already mirror these; TChannel plaintext matches Go.
No code changed, so no CI run required.
