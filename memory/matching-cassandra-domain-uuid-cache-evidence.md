# Matching Cassandra domain UUID cache normalization

## Change

Recovered commit `f02250a679f0f3007b2fbb5c784044ff3c2c0ee8` from the existing
`t3/cassandra-domain-cache-uuid-normalization` branch and cherry-picked it as
`ca11f306ceffaf37630e291672a1da41a525bbda` on
`t3/matching-cassandra-uuid-cache`.

The change canonicalizes domain IDs only for the Cassandra metadata source,
using the pinned gocql `ParseUUID` rules. It applies on page insertion, cache
lookup, and cache-miss lookup. SQL source IDs retain identity normalization.
The resolver remains cache-only for telemetry/quota context, and invalid
Cassandra spellings produce no cached domain name. RPC UUID validation and SQL
panic behavior are untouched.

Source review compared `gocql@v0.0.0-20211015133455-b225f9b53fa1/uuid.go:64-87`
with `gocql_uuid_string` in
`rust/crates/cadence-matching/src/matching_domain_cache.rs`. Both accept 32
ASCII hex digits, skip hyphens only at even decoded-digit offsets, accept
uppercase, reject malformed/extra hex digits, and require exactly 32 decoded
digits. Regression coverage includes uppercase and unhyphenated lookup of a
cached record plus accepted/rejected spellings. `git diff --check` passed.

## CI evidence

- Prior related successful CI: [run 40407463949](https://github.com/auenkr/cadence-rust/actions/runs/40407463949), head `5eb45b37e8a0fcbc1acd967b16b2a5164b7093f5`, conclusion `success`. The implementation commit is an ancestor of that tested head. Clippy, formatting, and tests succeeded. This run was not the current branch head.
- Fresh focused dispatch: [run 40408405589](https://github.com/auenkr/cadence-rust/actions/runs/40408405589), workflow `rust-ci.yml`, package `cadence-matching`, branch `t3/matching-cassandra-uuid-cache`, head `ca11f306ceffaf37630e291672a1da41a525bbda`. At evidence capture the run was `in_progress`; no result is claimed yet.
- No local Cargo, Go, rustfmt, or heavy validation was run. No mixed-cluster gate was dispatched.
