# fix-fe-domain-es (2026-11-04)
Fixed: O30 (failover_notification_version = metadata notification version, durable update/failover), O31 (gRPC + helper register apply archival policy via shared `apply_archival_registration_policy`), O33 (`exists_for_register` propagates non-NotFound errors), O35 (search_after int/datetime/bool conversion + clamp), O36 (lowercase sort error), O37 (skip empty domain filter), O38 (burst = max(1,int(limit))), C15 part 2 (reapply decodes by blob encoding, errors propagate; e2e test updated to JSON blob), activity-poll Thrift FromThrift now converts ns->ms (confirmed: Go matching ns was re-multiplied by 1e6 in ToThrift).
Not done: O33 string match on "does not exist" for persistence NotFound (trait returns String).
Unit tests added in es_query, global_ratelimit, admin, domains.
CI: https://github.com/auenkr/cadence-rust/actions/runs/41753637102 success
Gate: https://github.com/auenkr/cadence-rust/actions/runs/41754785063 success (es=true, two_cluster=true)
