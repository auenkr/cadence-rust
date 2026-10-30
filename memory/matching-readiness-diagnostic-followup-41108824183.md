# Matching readiness stall: bounded diagnostic follow-up

This source-only follow-up is based on the candidate ancestry containing
`2c3bc4a9ab50a824a89b96bac33e40309ef587ef` and the failed PostgreSQL run
41108824183. Original artifacts remain at
`memory/evidence/matching-postgres-41108824183` and
`/mnt/storage/service/ci-evidence/matching-postgres-41108824183.AY5Gf8/`.
The initial classification and limitations are recorded in
`matching-readiness-stall-41108824183.md` (commit `e9d562956`).

## Diagnostic changes

`tests/matching-process-evidence.cjs` now records best-effort per-thread
`/proc/<pid>/task/<tid>/syscall` and kernel `stack` content alongside the
existing 500 ms CPU sample, state, and wait channel. Each field is clipped
independently (512 and 2048 characters); collection remains capped at 64
threads. The two port-filtered socket snapshots now use `ss -p` to request
process/fd attribution for the listener and connections. Existing command
timeouts and output caps remain in force. Collection does not read process
environment or command lines. Procfs permissions and kernel configuration can
make syscall/stack fields unavailable; `ss -p` attribution can also be
incomplete.

The manual production-validation workflow has a `matching_gdb_diagnostic`
boolean defaulting to false. Only when selected together with `lane=postgres`
does that job install GDB, set `kernel.yama.ptrace_scope=0`, and set the
existing `CADENCE_MATCHING_PROC_EVIDENCE_GDB=1` observer flag. The existing
observer invokes batch GDB with a 20-second timeout and bounded output, taking
up to 12 frames per thread. Attaching GDB briefly stops the Matching process
while threads are attached, so this can affect the diagnostic interval and
must be treated as a deliberate perturbation. The default run and every other
lane do not install GDB or change ptrace policy. RPC criteria, retries,
timeouts, acceptance scenarios, and service behavior were not changed.

## Review status

This is a diagnostic-only source patch. No local Cargo, Go, Node syntax,
formatting, test, or runtime command was run. Review is limited to source
inspection and `git diff --check`; no CI run is dispatched by this follow-up.
