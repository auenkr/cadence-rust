# Failed production MySQL gate run (2026-10-20T184311Z)

Exit code `1`. The failure is a harness defect, not a service failure: the
services, the SDK workflows, the SIGKILL restart, the graceful drain and the
SDK recovery all completed, and the run reached the garbage collection gate.

## Failure

```
ReferenceError: restartWatermark is not defined
    at run (tests/matching-service-compat.cjs:482:24)
```

The restart flow declared its watermark inside the restart branch while the
production gate read it from the enclosing scope. The branch was not
executed by the probe, so the defect only appeared when the gate ran.

## Harness fix

The restart flow now keeps one function scoped record
(`restartState.taskList`, `restartState.watermark`), and the probe gained a
static check that reports undefined identifiers without a cluster:

```
python3 tests/run-matching-probe-static-checks.py
```

It parses every probe with `node --check` and runs ESLint's `no-undef` rule
with the repository's web workspace installation. The check reproduces the
defect above: the broken probe is reported as
`'restartWatermark' is not defined`.

## What the run already proved

- The shared database holds Go's schema only, with no private Rust table
  (`results.json`, `goSchemaOnly`).
- Matching's task garbage collector removed every acknowledged row of the
  completed workflow's queues, while the task of the never polled control
  list survived (`db-observations-manual.json`).
- The persisted acknowledgement of a queue can lag the in-memory
  acknowledgement, because it is written by the acknowledgement timer and
  not by the completion. The later audit of the Go source
  (`memory/matching-mysql-ack-persistence-audit-20261020.md`) showed that Go
  behaves the same way, so this is not a parity gap.
