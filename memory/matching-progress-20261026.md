# Matching continuation checkpoint

User reduced the worker limit to **two Luna workers**. Keep all Cargo, Go,
and formatter commands on Actions. Mixed gates remain serial. Matching is
incomplete; do not start the next service.

Master source `a039f209902eac0bb5899ec36d9582eabbb8c87b` includes the typed
SQL RunID error and the active Refresh error regression. Refresh focused run
40222171242 passed on `cfc7eab79a5a8ab24fe56f2b027e22af86f13be8`.
Master integration run 40222658215 is pending at this checkpoint.

Unmerged combined candidate: `matching-final-candidate`,
`41739ea94d56b00de5eacbb729d513f4abeb6528`; fresh workspace run
https://github.com/auenkr/cadence-rust/actions/runs/40222413373 is pending.
It contains the standalone SQL fatal lifecycle and real-binary regression.

Focused fatal run https://github.com/auenkr/cadence-rust/actions/runs/40222090786
failed on `ff91ef4bde65fe86972831f02177e7a6bd862102`: Clippy passed, preceding
tests passed, but the real child did not exit within 20 seconds after Add
returned an error. A failed Add alone does not prove the SQL writer ran.
Luna repair task `00ad1ebf-1ed6-4a1a-8012-c50603f42e30` is diagnosing the
request path and fatal signal. Preserve exit-2/no-success assertions; do not
hide the defect with a larger timeout. Candidate adds child kill/wait cleanup.

Next: inspect repair and focused CI, assemble corrected candidate, run fresh
workspace Clippy then tests, and run the exact-SHA serial mixed acceptance
plan in `matching-final-gate-plan-20261025.md`, recording binary SHA256s.
No current fatal-path passing test or final Matching acceptance is claimed.
