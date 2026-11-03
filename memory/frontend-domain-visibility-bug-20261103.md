# Frontend "Domain <uuid> does not exist" gate failure (2026-11-03)

Root cause: not a Rust Frontend bug. `frontend-mixed-gate.yml` built cadence-server from input `rust_ref`, default `fe-integ` (433 commits behind master, stale), not the dispatched ref. Failing runs 41653791350 / 41651925095 tested that stale code (commit.txt: rust_ref=fe-integ head=115f5b4b5). The Frontend restart was never the cause (SDK steps already ran before it).

Fix: rust_ref defaults to the dispatched ref (same fix landed on master 1fdfb9e51; master version taken in merge). Diagnostic eprintln lines removed.

Evidence (Rust built from the branch, domains persisted at notification versions 4,5,6):
- mysql kafka=false 41677791616 (head 588376c81) success; final 41681720569 (head aa33bb69a) success, binary sha256 e1d578ecdb2548bccca0573b7fa8f51f8747d2b133485d75a8449a62c44eb803
- mysql kafka=true two_history 41679608418 success (aa33bb69a)
- postgres two_cluster 41679631225 success (aa33bb69a)
- cassandra 41679650574 success (aa33bb69a)
- rust-ci 41679582657 success (aa33bb69a)
Master run 41681897553 was cancelled (concurrency); not needed, bug does not reproduce on real master code.
