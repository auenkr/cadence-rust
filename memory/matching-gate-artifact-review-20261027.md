# Matching exact-pin artifact review aid — 2026-10-27

Use after each of the ten serial gate runs in
[`matching-gate-readiness-20261027.md`](matching-gate-readiness-20261027.md).
This is a local Python standard-library extraction step; it does not dispatch,
wait for, or decide acceptance of a gate.

For each downloaded artifact directory, run from the repository root:

```sh
python3 tests/collect-matching-gate-evidence.py <artifact-dir> \
  --expected-sha <frozen-final-sha> \
  --run-url <full-github-actions-run-url> \
  --lane <lane-name> \
  --output <empty-review-dir>
```

The collector validates the artifact's source pin, Rust binary SHA256 record,
build/gate/runtime exits, and (for a successful gate) mixed topology and an
observed probe result row. `collection.json` also preserves the observed JSON
result rows, inventories named SDK and recovery evidence files as present or
missing, and records the database cleanup file's contents and hash. A failed
gate returns 1 after collecting evidence; missing or inconsistent required
evidence returns 2. Review each lane's actual scenario/recovery output and
cleanup record before dispatching the next lane. Do not infer execution from
`command.txt` or requested flags. File presence is only an evidence pointer;
it does not establish that an SDK/recovery assertion passed.

The output deliberately keeps `serviceAcceptance: false`. In particular, this
small report is not the Matching parity audit, does not validate every lane's
lane-specific assertions, and does not establish service completion. Record the
run URL, exact head SHA, conclusion, Rust binary hash, and manual findings in
memory only after reviewing each artifact. If the source changes during the
sequence, restart from the clean workspace gate at the new frozen pin.
