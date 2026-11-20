# fe-async-restart-flake (branch fe-async-restart-flake)

Failing run: https://github.com/auenkr/cadence-rust/actions/runs/44819724322 (frontend-mixed-gate, MySQL).
Steps failed: sdk-async-frontend-restart-no-lost-messages, sdk-async-driver-exit. Earlier similar: memory/fe-async-restart-loss-20261114.md.

## Root cause (from run artifacts)
- Kafka message was NOT lost. Go worker log shows all async messages processed; no consumer nack/DLQ lines.
- Lost id `...-fe-restart-22` started, then history logged `Activity timed out` START_TO_CLOSE at 21:32:35
  (EchoWorkflow activity StartToClose = 1 min, no retry policy) -> workflow failed -> harness waitResult saw failure.
- Harness does `kill` (SIGKILL) of the Rust Frontend while activity polls are in flight: matching handed the activity task to a poller
  whose Frontend connection died, so the task was recorded Started but never delivered. Go Frontend has the same outcome under SIGKILL
  (no graceful drain possible), so this is a harness race, not a Rust producer ack/durability bug.

## Fix (harness only, no Rust logic change)
tests/frontend-gate/main.go EchoWorkflow: activity StartToClose 10s + RetryPolicy (1s, max 5 attempts), the standard SDK remedy.

## Validation
(see run URLs appended below)
