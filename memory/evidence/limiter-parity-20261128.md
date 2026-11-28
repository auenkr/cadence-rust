# Limiter parity (Go x/time/rate) — 2026-11-28

Branch: `limiter-parity`

## Findings
- `cadence-common/src/rate_limiter.rs` is a line port of x/time/rate `reserveN`/`advance`/`CancelAt` plus Cadence's `clock.ratelimiter` wrapper (monotonic `latestNow`, `SetLimit/SetBurst` at `latestNow`, deny-reservation cancel). Already covered by Go-generated fixture `tests/fixtures/clock-rate.json` (`clock_rate_original_go_operations`).
- `dynamic_rate_limiter.rs` matches Go `quotas.DynamicRateLimiter`: 5s TTL (`now > last+ttl-1`, CAS), burst `max(ceil(rps),1)`, burst 0 when rps==0.
- **Divergences found by code reading: none.** No source fixes made.

## Tests added (fake clock, table-driven, in dynamic_rate_limiter.rs)
History default 20 rps/burst 20 allow table; Wait spacing (50ms steps) after burst; Wait-longer-than-deadline consumes nothing; fractional tokens (2.5 rps); dynamic rps decrease/increase/zero→restore after TTL; `limit_and_burst` table.

## CI
- Run: https://github.com/auenkr/cadence-rust/actions/runs/46108552667 (`rust-ci.yml`, package=cadence-common)
- Head SHA at dispatch: d6df99b23ee971dee526cbea203cccc2a23ae66f
- Conclusion: **success** (clippy-test: success, fmt: success). New tests passed; no expectation corrections needed.
