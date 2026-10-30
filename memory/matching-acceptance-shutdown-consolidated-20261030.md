# Matching acceptance + shutdown trace consolidated candidate

Candidate: `t3/matching-acceptance-shutdown-consolidated`.

Source base: reviewed acceptance commit `584f31560803516db67414a1e4e230e583bd46a`, descendant of `3479f19342db200a79802d49333dd7c4bea18d09` (which is based on `d0ef6bf3a4fb8056b22a0b10d56ccd16e8f495c`). Shutdown trace merged from `e3802467c0ccc0e7b012e207164d3e212c653e70`; merge is clean and preserves both parent histories. No shutdown source/workflow/memory files conflicted.

Review audit: `584f315` retains the host-address correction (`directClients.map(node => node.tchannel)`), bounded 30-second owner convergence, and `crossMemberTopology`. Comparing the consolidated tree against `3479f193` shows only these additional acceptance-source changes: bounded convergence and topology evidence; the host-address correction is retained. No other acceptance behavior changes were introduced by consolidation.

Shutdown trace change is opt-in source diagnostics plus enabling `CADENCE_MATCHING_SHUTDOWN_TRACE=1` in the PostgreSQL validation workflow. No runtime behavior changes intended.

Status: **UNVALIDATED**. No local Cargo, Go, test, runtime, formatting, or syntax checks; no CI dispatched. Main 35 retains active gate 41100032910 and CI ownership. Process snapshot `c9c470` is pending and must be added by its owner before any acceptance conclusion. This candidate does not replace or modify the integration worker's branch. Do not apply `3479f193` a second time; it is already in the reviewed ancestry.

Exact consolidated source tree SHA: `c9135e39c082ba47f57311d7527f183a38c6f088`. Validation status applies to this tree.
