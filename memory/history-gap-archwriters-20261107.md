# History gap: archiver write side (branch `history-gap-archwriters`, 2026-11-07)

- `cadence-common/src/archiver/writer.rs`: Go filestore `historyArchiver.Archive` / `visibilityArchiver.Archive` (same file names,
  JSON formats, base64 memo, close status text, mkdir/write modes from `fileMode`/`dirMode` parsed like `ParseUint(s,0,32)`).
  History takes batches via the `HistoryBatchSource` seam (Go `HistoryIterator` over HistoryV2Manager; not yet implemented).
- `assembly/archiver_client.rs`: inline path writes `file://` visibility fully; history needs a `HistoryBatchSource` (None by default
  -> explicit error, task retried). s3/gs schemes still explicit errors (TODO s3store/gcloud writers).
- `OriginalTaskList`/`Kind` added to typed Activity/DecisionTask; filled in `TaskInfoAdapter` (queue_exec.rs `typed_task`).
- Remaining: HistoryBatchSource over history store, s3store/gstorage writers, ArchivalSignaler, dynamic-config rates.

## Evidence
(see CI below)
- cadence-history: https://github.com/auenkr/cadence-rust/actions/runs/42342025121 (head daf4393, code identical to final for history) success
- cadence-common: https://github.com/auenkr/cadence-rust/actions/runs/42342655926 head 3d592a3b86d4a2aa3875e7b186fae96681edf5d1 success (first run 42342395858 failed on a wrong test mode "9"; fixed)
