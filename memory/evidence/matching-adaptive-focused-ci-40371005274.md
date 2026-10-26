# Corrected adaptive probe focused CI

Run: https://github.com/auenkr/cadence-rust/actions/runs/40371005274

Source: `ee5b080f8306bd9dd65030aa7ddfe513d54e8b23`. Actions conclusion: success.
Downloaded rust-ci-logs confirms clippy.exit=0, test.exit=0, fmt.exit=0 and matching-runtime-format.exit=0. workspace-format.exit=1 remains advisory formatting debt.
Target was the ordinary cached rust/target, not the fresh final target. This validates focused Rust checks only; it does not execute the adaptive mixed scenario or prove routing, SDK completions, or service acceptance.

Artifact SHA256:

- `commit.txt`: `772976f598fc5536a13cb7e3390664bcda9cb49805bca619f2b65b1850b1076c`
- `clippy.exit`: `9a271f2a916b0b6ee6cecb2426f0b3206ef074578be55d9bc94f6f3fe3ab86aa`
- `test.exit`: `9a271f2a916b0b6ee6cecb2426f0b3206ef074578be55d9bc94f6f3fe3ab86aa`
- `matching-runtime-format.exit`: `9a271f2a916b0b6ee6cecb2426f0b3206ef074578be55d9bc94f6f3fe3ab86aa`
- `test.log`: `e9a8e9519fb72768bfe6ad8469c320d952f85a890100c9bcde58046b528ff749`
