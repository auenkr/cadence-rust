# Frontend cross-review round 2 (Opus), 2026-11-04 — read-only, master 3ace896eb

| ID | Codex | Opus round 2 | Evidence |
|---|---|---|---|
| O11 | PARTIAL | ACCEPT | Thrift decoder divides by 1e6 (`cadence-thrift/src/convert.rs:2278-2279`), so the production formatter gets ms. Remaining defects: sub-ms precision loss and the nanos/ms mismatch on the remote gRPC model. Settle a single unit first. |
| O14 | PARTIAL | ACCEPT | Reset points and parent domain ID are lost. The selection-policy omission on proto matches Go and is not a regression. |
| O15 | DISAGREE | ACCEPT (withdraw) | Go `helpers.go:150` does an unchecked `int32(...)`. Rust `cadence-proto/src/convert/mod.rs:817` `d.seconds as i32` wraps the same way. |
| O31 | PARTIAL | ACCEPT | `cadence-service-core/src/handlers.rs:607-613` copies the archival status and URI straight from the request, with no registration policy. The gRPC path is still affected; Thrift is not. |
| O32 | DISAGREE | ACCEPT | `handlers.rs:616` calls `domain_replication`. The isolated helper is dead or maintenance-only code, not a deployed defect. |
| O34 | DISAGREE | ACCEPT | `cadence-frontend/src/handler_workflow.rs:1093` runs `validate_request` before the archive branch. |
| C10 | new | AGREE | `cadence-matching/src/grpc_admin.rs:1065-1068` sets all four boundaries to None. |
| C11 | new | AGREE | Thrift `RemoveTask` uses a raw `opt_i32` (`cadence-thrift/src/convert.rs:63+`). `cadence-service-core/src/admin.rs:1980` forwards it unmapped into the History proto `task_type`. |
| C12 | new | AGREE (not re-traced line by line) | The Codex citations look consistent. The field-40 omission is plausible because the API model has no such field. |
| C13 | new | AGREE | `data_blob_out` hard-codes `encoding_type: 2` (`grpc_admin.rs:~53`). |
| C14 | new | AGREE | `IsolationGroupState` is Invalid=0/Healthy=1/Drained=2 (`cadence-api/src/enums.rs:304`). `grpc_admin.rs:~417/472` adds `+ 1`, and the setters near 438/497 subtract 1. |
| C15 | new | AGREE | `grpc_admin.rs:847-850` sets `encoding_type: None`. |
