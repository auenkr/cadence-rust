# Frontend master sync — 2026-11-02

Integrated completed gate extras (`b92bd41b62e2ebad3c36156ca56e88b2c6b95568`) and PostgreSQL visibility/runtime fixes (`3c4ae6ae6`) into master, ending at merge `32bfc14e3`. Dashboard conflict resolved preserving both evidence sets. Frontend remains partial; the combined source has not yet completed fresh CI.

- MySQL mixed run https://github.com/auenkr/cadence-rust/actions/runs/41427827989 passed 51/51 on source `5644052674ad0294bb10662a8fe8c67446193e3b`; binary SHA256 `e6cd5037ffb45cf4897c81a5d40d005207e12b6e9f149de7b4dc67efe803db85`.
- Clean workspace run https://github.com/auenkr/cadence-rust/actions/runs/41424261789 passed on `d08287ffecebbd7b4f09ddeeadfaa54a2f464702` (before later evidence-only commits).
- PostgreSQL mixed run https://github.com/auenkr/cadence-rust/actions/runs/41418956694 passed 34/34 on `b5b9e0cae8bf30cba3934c408ec88ecf742b72e5`; binary SHA256 `7a77873938cd1f8fa850ab02a97b1e2e270ce3695da262a77b8457a134ac95f9`. This proves Frontend restart, not Matching restart.
- PostgreSQL branch clean workspace run https://github.com/auenkr/cadence-rust/actions/runs/41422156747 passed on `4df07d90ad30a7ffb6311e820a1aa0760d6e2d9b`.

Merge checks: Python harness parsed with ast; staged diff whitespace check passed. No local Cargo, Go or runtime gate commands executed. Next: combined clean CI, Cassandra mixed lane, and scheduler inbound context propagation. Broader parity gaps remain in the Nov 2 audit.

## Combined verification and Cassandra failure

Fresh uncached workspace CI https://github.com/auenkr/cadence-rust/actions/runs/41429248975 passed on `344c83cf42961ce4bcb6f5ab94c4649cc289c986`.

Cassandra lane run https://github.com/auenkr/cadence-rust/actions/runs/41429904560 failed on `6ce90c7993a3554d4e32037662898580cb0b1bd8` (both gate and resolved Rust source). Binary SHA256 `c2911ed24bc479aad7bde16a8b24854af45d499d00a1a7208fb4d94f0e59b8e3`. Go History and Rust Matching listening checks passed; Frontend listening checks failed because Frontend exited with nested Tokio runtime panic after opening configured stores. No SDK acceptance reached. Artifact reviewed at `/tmp/cadence-fe-cassandra-41429904560`; failure is a real exposed runtime gap, not acceptance.

Scheduler context code `66fd2b282be8749bfc261715c8bc4146476b6920` passed package CI https://github.com/auenkr/cadence-rust/actions/runs/41430427854; formatting patch downloaded, follow-on YARPC routing CI41431354388 pending. These branches remain unmerged.
