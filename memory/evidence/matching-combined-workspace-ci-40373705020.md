# Combined-source fresh workspace CI 40373705020

Run: https://github.com/auenkr/cadence-rust/actions/runs/40373705020

Tested source: `10692b2d5a5b912da051a80ca15c30f8236484dd` (`t3/final-acceptance-ci`). Conclusion: success.
Downloaded rust-ci-logs confirms fresh target `/home/runner/work/_temp/cargo-target-final`, Clippy/test/fmt/Matching runtime format exits 0. Workspace-wide formatting exit 1 remains advisory debt.

This source includes the approved real delayed-runtime shutdown regression with unwind release guard and live adaptive routing assertions. It predates the queue-scoped cache flag fixture correction merged at `eba0b437d`; that correction requires new exact-pin validation. No mixed gate or production binary identity is proved by this workspace run.

Original artifact SHA256:

- `commit.txt`: `7c70b9cc993b9223823b30abfd02c3e58e277116f71190ceb116fb59b39c8efb`
- `target-dir.txt`: `f2e8e5be63d5f88f972b21dd7011c9862114aa2a59e1526e8c0671550a6af630`
- `clippy.exit`: `9a271f2a916b0b6ee6cecb2426f0b3206ef074578be55d9bc94f6f3fe3ab86aa`
- `test.exit`: `9a271f2a916b0b6ee6cecb2426f0b3206ef074578be55d9bc94f6f3fe3ab86aa`
- `matching-runtime-format.exit`: `9a271f2a916b0b6ee6cecb2426f0b3206ef074578be55d9bc94f6f3fe3ab86aa`
- `clippy.log`: `578c5cd1fb7eb02d8a6941620ea3752257109e4e16de80389a2d458630d41928`
- `test.log`: `998ad3b2a2391cbf4b24e12f07aae31e3dd82d3c441cd276066a4d5258ce4b76`
