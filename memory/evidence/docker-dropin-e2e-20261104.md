# Docker drop-in e2e with cadence-samples (2026-11-04)

Branch `t3/docker-dropin-e2e`, workflow `.github/workflows/docker-dropin-e2e.yml`, head 4cc3b-see `git log` (workflow commit "ci: temporary push trigger for dropin e2e").
Run: https://github.com/auenkr/cadence-rust/actions/runs/41761352302 -> **success** (single run, no fixes needed, no Rust code changed).

- Images: Go `sha256:ec8d6e0b348c...fb1914` (165,767,720 B); Rust `sha256:98fb8dedfd07...a5711` (204,373,891 B). Rust binary SHA256 `84878674bb435f323fa2f0dfed9bbd9614e09de899a2a1ff3cac85e5279e9d62`.
- Compose: tests/docker-drop-in/docker-compose.split.yml (upstream go/docker mysql layout split one service per container, because History/Worker are still Go: the all-in-one upstream container cannot be a Rust image). Workflow asserts the `docker compose config` diff Go vs Rust is exactly 2 lines, both `image:` (frontend, matching).
- cadence-samples cc365fe999740e61b1bca45816fedd5b6adaa876 (Go SDK go.uber.org/cadence v1.4.0-rc.3), real workers + triggers: helloworld, timer, childworkflow, signalcounter (signal asserted in history), query (__stack_trace), cancelactivity (cancel asserted), cron (rolls over). All asserted via CLI list; samples.exit 0.
- Restart: sleep + timer + helloworld started, `docker compose restart frontend matching` (Rust containers) mid-run; sleep and timer complete afterwards and a new childworkflow completes.
- Artifacts (3 day retention) contain compose-go/rust.yml, compose.diff, images.txt, logs, per-sample logs.
- Note: the workflow still has a TEMPORARY push trigger (needed because workflow_dispatch 404s until the file is on master); remove after merge.
