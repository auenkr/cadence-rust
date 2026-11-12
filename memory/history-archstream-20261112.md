# history-archstream (2026-11-12)

Closes the remaining gaps of `history-archgaps-20261112.md` (s3store / gcloud history archivers).

## Changes
- `HistoryBatchSource` (`cadence-common/src/archiver/writer.rs`) gained `read_page(request, first_event_id)` (Go `readHistory` = `ReadFullPageV2EventsByBatch`, 250 events per page) and `archive_incomplete_history()`. The default `read_page` slices `read_all`; `read_all` stays for the filestore path.
- `HistoryIterator` (`cloud_archiver/write.rs`) now follows Go `readHistoryBatches` line by line: it pages through the source from `NextEventID`, returns mid-page once the target is reached, does the look-ahead read, treats EntityNotExists with firstEventID != 1 as finished, and a finished state encodes `NextEventID: 0` like Go. The whole history is no longer read up front for s3/gcs.
- `HistoryNodeBatchSource::read_page` (History) pages `ReadHistoryBranch` with a token from `first_event_id` until 250 events or the token ends.
- Mutated-history check moved to the archivers, per blob with `is_last` like Go (`IsHistoryMutated`); `ArchiveIncompleteHistory` comes from the source. s3: plain `ErrHistoryMutated` (non-retryable, substituted); gcs: returned as is (transient).
- Blob size: `estimate_size` / blob bytes use `go_json_bytes`: compact JSON with Go HTML-safe string escaping, and Go `omitempty` for `eventId`/`version`/`taskId` zero values. Limitation: other nested omitempty differences between Rust serde types and Go structs are not normalised.
- Inline archival from History: `inline_archive_options` passes only the metrics client, no ProgressManager and no NonRetriableError, as Go `archiveHistoryInline` (only the AllowArchivingIncompleteHistory option is passed). Unit-tested.

## Tests
paged iteration call sequence `[1,2,3,4]` and state encoding, first-page EntityNotExists, Go JSON size/escaping/omitempty, per-blob mutated check with and without incomplete-history, inline options have no progress manager / non-retriable error.

## Evidence
- rust-ci (clippy + test) on code head 3c-pushed commit: https://github.com/auenkr/cadence-rust/actions/runs/43180611187 (clippy, format, test steps success)
- gate mysql/grpc/true: https://github.com/auenkr/cadence-rust/actions/runs/43182140040
- gate postgres/tchannel/false: https://github.com/auenkr/cadence-rust/actions/runs/43182145355
(final results appended below)
