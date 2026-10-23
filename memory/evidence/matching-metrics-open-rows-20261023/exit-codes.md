# Exit codes (open-rows pass, 2026-10-24)

Command: CARGO_TARGET_DIR=/mnt/storage/service/cargo-target-shared /mnt/storage/service/bin/cargo-safe, cwd rust/

| step | exit |
|---|---|
| test-matching | 0 |
| test-core | 0 |
| clippy-core | 0 |
| clippy-matching | 0 |

Logs: /tmp/mo-*.log (not copied). Note: first run exited 101 on uninitialised go/idls submodule; fixed by 'git submodule update --init go/idls' (no tracked change). Chain script ends with echo, so the aggregate mo.exit is not a pass signal; per-step files are.
