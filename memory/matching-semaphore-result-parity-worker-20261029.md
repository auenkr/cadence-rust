# Semaphore result parity worker — 2026-10-29

Branch `t3/semaphore-result-parity` cherry-picks the six source commits from
`origin/t3/semaphore-startup-cancel` source `a86d7a915a1f64d4fac9c5b260b016b95c9c8d6a`;
the startup error follow-up is `127a9f802`, and result handling follow-up is
`5abd83ef03910cc0a826f82f877198f0588e2d88`.

Removed the extra metadata read; Go startup scans only. Scan failures now carry
raw bucket bytes and their source classification; the service test covers a live
caller, waiter release and retry. The result follow-up passes a caller lifetime
to manager and persistence interfaces, waits for the operation's result, and
keeps reservation/cache updates for applied writes. Its tests cover an applied
write released after cancellation, an existing hold confirmed with an already
cancelled context, and pre-write/waiting cancellation. These changes are not a
complete cancellation parity fix: existing production persistence adapters use
the default context methods and do not actively cancel pending I/O. A hung store
operation can therefore outlive the caller. A controlled confirmation with
cancellation occurring during the read, and an injected context-aware store
returning a cancellation error after entering I/O, still need coverage. Audit
classification of wrapped typed errors and transport details as well. Do not
merge this as a parity-complete fix without resolving these gaps.

Focused CI dispatched: https://github.com/auenkr/cadence-rust/actions/runs/40911583315
at source head `5abd83ef03910cc0a826f82f877198f0588e2d88`, package
`cadence-matching`. One observation showed `in_progress`, no conclusion, with
checkout running and Clippy/format/test pending. No pass or failure is known.
No local Cargo, Go or runtime validation was performed. Fresh workspace and
mixed-cluster gates and Matching acceptance remain outstanding.
