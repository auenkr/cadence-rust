# Frontend sol-review P2/P3 fixes (#6-#18), branch frontend-solfix-p2

Commits (see `git log origin/master..frontend-solfix-p2`):
- 40156c227: #6 (gRPC activity-poll timestamps ms), #7 (describe consistency level), #8 (task-list describe fields), #9 (cluster info versions), #17 (reset/restart run id validation)
- 29b747bbf: #11 (async cancellable poll wait, parent deadline), #12 (dynamic poll-delay/version-check), #13 (zero-rps burst)
- 28f7c6319: #18 (go-version prerelease/malformed parsing; real, ported)
- 0464aa2fe: #14 (caller/transport metric tags)
- 5648ac9a0: #16 (visibility list sampler: `system.enableVisibilitySampling` default false, `frontend.visibilityListMaxQPS`, per-domain bucket, ServiceBusy "Persistence Max QPS Reached for List Operations."), #10 (task-local inbound deadline; outbound TTL = min(budget, caller remaining); removed 60s cap in matching tchannel.rs)
- 49c7dc64b: rustfmt + CallerMetadata test-literal fix (rustfmt run directly on changed files, not cargo)

Not done / limitations:
- #15 (Admin request/error metric scopes): real in Go (admin/handler.go startRequestProfile/error) but Rust admin dispatch has no metrics sink; wiring deferred, not implemented.
- #8 partition_config: Thrift IDL / API model have no field, so only task_list and other carriable fields were ported.
- #10 and #16 have a unit test each (clamp test; list sampler test); #15 none.

CI (head 49c7dc64b), rust-ci all success:
- cadence-frontend https://github.com/auenkr/cadence-rust/actions/runs/43595719164
- cadence-service-core https://github.com/auenkr/cadence-rust/actions/runs/43595722247
- cadence-matching https://github.com/auenkr/cadence-rust/actions/runs/43595725141
- cadence-proto https://github.com/auenkr/cadence-rust/actions/runs/43595727682
- Earlier runs on 5648ac9a0 failed on rustfmt + one test literal; fixed in 49c7dc64b.
Mixed gate (mysql): https://github.com/auenkr/cadence-rust/actions/runs/43596181056 (result below)
