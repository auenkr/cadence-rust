# PostgreSQL activity owner supplemental review (2026-10-30)

## Scope and pins

Read-only source review of supplemental commit
`f009e8d70763e0a30a0b933646f25131d180709d` against coherent Matching candidate
`5ee69b5572688c2dba2245c7b411c7182c5b9b16`. The candidate branch has a later
documentation-only head `62b5b5baa`; no implementation was integrated or
changed by this review. No local Cargo, Go, formatting, test, or runtime
validation was run.

## Findings

The supplemental fix addresses a real assertion gap in the earlier crash
probe: one common nonempty owner for a single activity task-list key does not
show that a restarted process rejoined the owner set. `f009e8d` checks 64
deterministic activity task-list names through both direct Rust Matching
endpoints, requires nonempty equal owners for every pair, and requires the
restarted endpoint address to own at least one sampled partition. It retains
the Go Frontend `DescribeTaskList` request and writes the extended evidence
only after the restart assertions pass. The assertion is appropriately limited
to sampled partition ownership; it is not full Ringpop membership evidence.

No concrete correctness defect was found in the added assertion by source
inspection. Its result remains unverified until the PostgreSQL production gate
executes this path. In particular, the source assertion does not substitute
for observing the durable activity row and positive range before SIGKILL, the
actual killed signal, exact activity history counts, SDK result, and queue
drain in the gate artifact.

The supplemental source depends on earlier code commits and should not be
integrated as a standalone cherry-pick. Required code commits in order are:

1. `3a1da7b06bdbec4ddaa5858f003f4f986870540c` — add PostgreSQL durable
   activity owner crash-recovery probe and Go SDK seed/recovery workflows.
2. `f07ef0a37fa391cc1492d4e2563df9ef95e50068` — forward decision/activity
   task type through the observer, Cassandra helper, SQL paths, and self-check.
3. `d63c12d68d57e49cd81f182c610b27eedef131b7` — tighten activity persistence,
   recovery history, and cleanup assertions.
4. `f009e8d70763e0a30a0b933646f25131d180709d` — require restarted member
   ownership across the sampled activity partitions.

`01bf00dd04d72f131e9d1a527d83e2904783bf4b` is review-status documentation,
not a required code dependency; do not blindly copy its older review narrative.
The tested candidate already has independent changes in
`tests/matching-service-compat.cjs` for startup/readiness diagnostics and
cleanup. A read-only `git merge-tree` of candidate and supplemental tip
reported no textual merge conflicts, but that is only a merge preview and no
integration was performed. Review the combined harness before accepting the
chain, since both lines edit this file.

## CI checkpoint

Inspected GitHub Actions run
<https://github.com/auenkr/cadence-rust/actions/runs/41055162424> once. It is
for exact candidate SHA `5ee69b5572688c2dba2245c7b411c7182c5b9b16` on
`t3/matching-postgres-readiness-trace-reconciled`; status was `in_progress`,
with Clippy and formatting successful and the workspace `Test` step still
running. No run conclusion, workspace test total, or final artifact SHA is
available at this checkpoint. Per instruction, no follow-up poll and no
PostgreSQL production gate were dispatched while this workspace run remained
active. Resume from this run's terminal state before dispatching the gate on
an immutable ref to the exact candidate SHA.
