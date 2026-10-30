# Matching shutdown trace PostgreSQL validation candidate

Candidate base: `d0ef6bf3a4fb8056b22a0b10d56ccd16e8f495c` (includes the timestamped RPC callback probe and its assertions). Integrated source commit: `0d59ec138fadf73378b097940c44d5f438dcc62a`.

The candidate adds opt-in, timestamped Matching shutdown stage logs and sets `CADENCE_MATCHING_SHUTDOWN_TRACE: "1"` at the job level in `.github/workflows/matching-production-validation.yml`, beside the existing `CADENCE_MATCHING_LIST_PARTITIONS_TRACE: "1"`. The workflow job environment is inherited by `tests/run-go-service-baseline.py`; its production Matching probe environment is built from `{**os.environ, ...}` and passed as `env=probe_environment` to `subprocess.run`. That command launches `tests/matching-service-compat.cjs` with Node, so the Rust Matching process and Node probe inherit the setting. The Python path also preserves the list-partitions trace setting.

The shutdown source diff changes diagnostic output only when the new variable is present. It adds fixed stage names and Unix nanosecond timestamps around existing shutdown actions; drain duration is sampled through the existing live provider and logged. It does not alter cancellation, ordering, timeouts, workflow assertions, or the existing probe timestamps. No formatting or source correction was needed by inspection.

Status: integrated candidate only; unvalidated. No local formatter, Cargo, Go, tests, runtime, or CI was run. Do not treat this as a runtime fix. Main Luna 35 retains diagnosis and CI ownership; do not absorb the Sonnet 9c670 recovery patch until its result is known.

Exact candidate source SHA: `9771da271f97586a3ce7230e292a9c92f155ae8c`. CI should validate that exact SHA on the PostgreSQL production-validation lane and capture the Rust binary SHA256 before any runtime conclusion.
