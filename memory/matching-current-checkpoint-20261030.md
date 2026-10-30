# Matching checkpoint — 2026-10-30

Matching remains incomplete. Do not begin History. No completion or parity claim is supported yet.

## Validation and diagnosis

- Latest validated workspace: exact source SHA `6c73d9aa447171d4eeda37d71dd0a955d0996a9b`, CI run [41059165266](https://github.com/auenkr/cadence-rust/actions/runs/41059165266): clippy and workspace tests passed (0). Global formatting reported 1; changed Matching files reported 0.
- PostgreSQL mixed gate [41094318642](https://github.com/auenkr/cadence-rust/actions/runs/41094318642) failed before SDK/recovery validation. TCP connect was 1 ms, while gRPC remained `CONNECTING` and the LB pick timed out. The unique runtime cause is unproven; the capped trace cannot establish an engine stall.
- Do not poll or dispatch around the current background CI owner. It owns run `41100032910`, `matching-grpc-transport-evidence-4a5da9fb`.

## Candidate and evidence status

- Consolidated candidate `be7016ef2d04dcb00fda946ddb67e3fbf15bae09` (`origin/t3/matching-acceptance-shutdown-consolidated`) is unvalidated. It retains reviewed acceptance `584f3156`, timestamp changes `d0ef6bf3`, and shutdown change `e3802467`. Shutdown source review found no issues.
- Process evidence `93deac8e` was reviewed as bounded, but immediate startup failures could consume the capture. The integration worker is fixing elapsed-time qualification and final capture, and enabling the flag; workflow check `d7c80954c` is being combined.
- Four workers are the target; use Luna for straightforward tasks. Keep all validation on CI.

## Next sequence

1. Reconcile the main CI owner's evidence after it is available; do not poll or dispatch while it owns run `41100032910`.
2. Freeze the source, then run a fresh, clean-target workspace gate with the package input unset.
3. Run the PostgreSQL mixed gate serially against the immutable source. Record the server SHA256, nested process exit codes, SDK results, and failure/restart/recovery evidence.
4. Continue the Matching parity audit. Matching remains incomplete until the service and mixed Go/Rust acceptance evidence are complete.
