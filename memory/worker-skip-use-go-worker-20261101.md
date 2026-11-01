# Decision: skip the Rust Worker service, keep using the Go Worker (2026-11-01, user)

The user decided NOT to port the Worker service to Rust. The cluster will run the original Go Worker (go/service/worker) alongside the Rust Matching and Rust Frontend (and History when done).

**Why:** user directive; the Go Worker is ~19k lines of system workflows and is not worth re-porting.
**This overrides** the "everything in Rust / no Go required" and "Order: Matching -> History -> Frontend -> Worker" lines in AGENTS.md for the Worker only. AGENTS.md itself was not edited.
**How to apply:**
- Do not start or continue Rust Worker work. The `worker-core` branch (head 068974dd3 at decision time, partial scanner/indexer/scheduler ports) is kept as unmerged reference only; the Worker subagent was cancelled.
- Frontend validation gates run the Go Worker against Rust Frontend + Rust Matching + Go History. The Go Worker must work unmodified against the Rust Frontend (it uses the Frontend client and system domains), so Frontend parity for the APIs the Go Worker calls is required.
- Remaining scope: finish Rust Frontend (integration branch frontend-domain-admin), then mixed-cluster gates with the Go Worker running.
