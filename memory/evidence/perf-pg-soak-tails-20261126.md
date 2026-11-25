# PostgreSQL c16 tail soak (2026-11-26)

## Predeclared margin (written before any run was dispatched or read)

Rust is declared free of a material PostgreSQL tail penalty iff, for the mean over runs of per-run p99 (whole run, and also the last-third window),
**Rust p99 <= Go p99 * 1.05**, judged on the upper bound of the 95% run-cluster bootstrap CI of the paired relative delta (Rust-Go)/Go
(upper bound <= +5% = pass; point estimate <= +5% but CI upper > +5% = inconclusive; point estimate > +5% = fail).
Done separately for (a) 4 unpaced runs and (b) 4 paced runs (target_rate=70). Rust-control (A/A, same tree) deltas give the noise floor.
