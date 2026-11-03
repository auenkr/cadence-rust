# Frontend workflow residuals (2026-11-04)

Branch `t3/frontend-workflow-residuals`, code head `8c6e48bde7457ca5c3191025b0010aedd8af9930` (plus this evidence commit).

## Implemented
1. sendRawWorkflowHistory: `GetWorkflowExecutionHistory` returns ThriftRW raw blobs when dynamic config `frontend.sendRawWorkflowHistory` is on and the client supports it (`supports_raw_history_query`: uber-go/cli >=1.6.0, uber-java >=1.3.0). Handler sets private `__rawHistory`; `history_reader` returns `raw_blobs`.
2. DeleteDomain: BadRequest "Domain still have workflow execution history." (thrift + REST) when a deprecated domain still has executions.
3. Thrift AdminService `ResendReplicationTasks` MethodDef added (was missing from the table).
4. e2e test: admin RefreshWorkflowTasks delegates to History. Plus delete-domain-with-executions e2e; `admin_and_visibility_routes` now expects 400.
5. Gate steps (`tests/frontend-gate/api.go`): raw history supported/unsupported client, delete-domain deprecated (with/without executions), failover-domain. `run-frontend-gate.py` enables `frontend.sendRawWorkflowHistory` for Rust.

## Evidence (all on GitHub Actions)
| Check | Run | Result |
|---|---|---|
| rust-ci (clippy + tests) | https://github.com/auenkr/cadence-rust/actions/runs/41713548777 | success |
| mixed gate mysql kafka two_history | https://github.com/auenkr/cadence-rust/actions/runs/41719496074 (head 8c6e48b, rust sha256 fc4178f4d7e0682989bed83834f1a1d98ea9d221fafc6378540b2964df99f368) | success |
| mixed gate postgres two_cluster | https://github.com/auenkr/cadence-rust/actions/runs/41721586163 (sha256 91e1b081bb243143a59ec33dee532e270bc867ca26d4681e247a95c511038986) | success |
| mixed gate cassandra | https://github.com/auenkr/cadence-rust/actions/runs/41723040080 (sha256 456a6550d73a91e6aa53b24f72b59a8f34d2e2095aae684d5fa39ceabb16893c) | **failure** |
| mixed gate cassandra (retry) | https://github.com/auenkr/cadence-rust/actions/runs/41725331932 (sha256 0d42c5c97f409985c719b9816642447350781b72bd9f7055bfcfdeb0637405e5) | **failure** |

Earlier runs: 41715100222 failed on my first version of `api-delete-domain-with-executions` (domain cache not yet loaded after RegisterDomain; fixed with a start retry). 41717342780 (mysql) failed only on the ratelimiter steps below, passed on re-run.

## Cassandra failure (not caused by this change)
Both cassandra runs: `api-*` steps (including all new ones) report `failures=0`; only `global-ratelimiter-global-shadow-local-serves-requests` and `...-enforces-service-busy` fail ("Failed to create workflow"). The same two steps fail intermittently on branch `t3/frontend-admin-coverage`, which does not contain this change (runs 41723108226, 41720780482, 41715051316), and also failed once on mysql here (41717342780) before passing. This is a pre-existing flake/bug in the global-ratelimiter `global-shadow-local` mode, not investigated here.
