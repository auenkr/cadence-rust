# matching-hash32-short-20261024: Go farm.Hash32 for inputs of 0–4 bytes

## Problem

`cadence_common::membership::hash32` (used by the Matching hashring view
identifier, `matching_membership.rs`, Go `emitHashIdentifier`) delegated to the
upstream `farmhash` 1.1.5 crate's `hash32`. For 0–4 byte inputs that crate
reads each byte as unsigned (`farmhashmk_shared.rs` `mk_hash32_len_0_to_4`:
`let v = s[i] as u8;`). Go `dgryski/go-farm` `hash32Len0to4` reads each byte as
`int8` (`farmhashcc.go:35`: `v := int8(s[i])`, then `b = b*c1 + uint32(v)`), so
any byte ≥ 0x80 yields a different value. Inputs of 5+ bytes are unaffected by
this path.

## Diff

`rust/crates/cadence-common/src/membership.rs` only:

- `hash32` now: if `len > 4` → `farmhash::hash32(bytes)` (unchanged behaviour);
  otherwise → `fingerprint32(bytes)`.
- `fingerprint32`'s 0–4 path is exactly Go's `hash32Len0to4(s, 0)`:
  `b=0`, `c=9`, per byte `b = b*0xcc9e2d51 + int8(byte)`, `c ^= b`, then
  `fmix(mur(b, mur(len, c)))`. Go's `Fingerprint32` and `Hash32` are the same
  function for all lengths (`fp_generic.go:12` returns `Hash32(s)`). No code
  outside `hash32` changed; `fingerprint32` and its callers are untouched.
- Test `go_hash32_matches_go_farm_vectors` added (13 vectors).

## Oracle vectors

Source: `memory/evidence/matching-hash32-short-20261024/hash32_vectors_go.go.txt`
(a standalone Go program importing `github.com/dgryski/go-farm`
v0.0.0-20240924180020-3414d57e47da from the local module cache).
Run: `GOFLAGS=-mod=mod GOPROXY=off GOSUMDB=off /mnt/storage/service/bin/work-safe go run .`
(go1.27.0, exit 0). `Hash32` and `Fingerprint32` columns were identical for every input.

| input (hex)      | len | Go Hash32  |
|------------------|-----|------------|
| (empty)          | 0   | 3696677242 |
| 00               | 1   | 3232311124 |
| 7f               | 1   | 3259183209 |
| 80               | 1   | 801663339  |
| ff               | 1   | 495582926  |
| 61 62            | 2   | 1098068221 |
| ef bf bd         | 3   | 4143110158 |
| 80 ff ef bf      | 4   | 740356336  |
| ef bf bd 01      | 4   | 110792607  |
| 80 00 ff 7f      | 4   | 847840402  |
| "hello"          | 5   | 2039911270 |
| 80 ff ef bf bd   | 5   | 2633776259 |
| "hello world"    | 11  | 430397466  |

The 0–4 rows of this table are the same values the existing
`go_fingerprint32_matches_signed_short_byte_vectors` test pins, which is an
independent cross-check that `fingerprint32` already matches Go on this path.
The 5- and 11-byte rows exercise the `farmhash::hash32` delegation; `430397466`
is also the value pinned by `farmhash_32_uses_go_compatible_fingerprint`
(`matching_mysql.rs`).

### Hand check (sign effect on one byte)

Input `[0x80]`, `len=1`, seed 0:
- Go: `int8(0x80) = -128`, so `b = 0*c1 + (-128) = 0xFFFFFF80`.
- Unsigned (upstream crate): `b = 128 = 0x00000080`.
Different `b` feeds `c ^= b` and `mur(b, …)`, so the final hash differs. The
signed result 801663339 is pinned in the test.

## Verification

- No local cargo (per task). Verification is GitHub Actions `rust-ci.yml`
  with `package=cadence-common` (`cargo clippy -p cadence-common --all-targets
  -- -D warnings` and `cargo test -p cadence-common`).
- Run URLs and conclusions: see the section below.

## Scope notes

- `cadence-persistence` (`matching_mysql.rs:571`, `matching_postgres.rs:343`)
  calls `farmhash::hash32` directly on `domainID + "_" + tasklist`, which is
  always longer than 4 bytes. Its results do not change. It was not modified.
- Not changed: `fingerprint32`, Ring, Ringpop checksums, shard-routing code.

## CI runs

- (filled in below)
