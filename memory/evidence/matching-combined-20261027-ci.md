# Matching combined candidate CI — 2026-10-27

Provisional source: `91c481a612eac48f786f208f4c2f4193864ed966`, branch
`t3/matching-combined-20261027`.

Run: https://github.com/auenkr/cadence-rust/actions/runs/40409014196

Dispatched `rust-ci.yml` with `clean_target=true` and no package filter, requiring
Clippy followed by workspace tests in a fresh runner target. Result is pending.
This candidate combines the reviewed panic quoting change, Cassandra cache UUID
normalization and adaptive SDK timeout budgets (180-second SDK context and
240-second JS child limit). The independent candidate source review is pending.

The deterministic History TChannel fixture/oracle repair is not integrated;
its prior focused run 40408688639 failed Go byte comparison due to generated
fixture nondeterminism, before Cargo ran. That failed source is not on this
candidate. Its correction requires separate review and validation before any
final acceptance pin includes it. No mixed gate has been dispatched for this
candidate. This is not a Matching completion record.

No Cargo, Go or rustfmt command was run on the development host. CI artifacts,
exact head identity, terminal conclusion and fresh-target test exits still need
to be reviewed. Evidence changes stay on this separate branch to preserve the
candidate run's source identity.
