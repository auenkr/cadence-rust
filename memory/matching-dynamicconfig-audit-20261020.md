# Matching dynamic configuration port audit

Go startup in `go/common/dynamicconfig/dynamicconfigfx/fx.go` selects modern
`dynamicconfig.client` whenever it is nonempty. An empty value selects legacy
`dynamicConfigClient` and logs a warning. Relative file paths resolve from the
process root (`--root`, default current working directory), not from the config
file's parent.

Go filebased client (`go/common/dynamicconfig/file_based_client.go`) requires
an existing path and synchronously loads its first snapshot. Poll intervals
below five seconds, including zero, are changed to five seconds. A ticker
reloads when file mtime is newer than the last poll time. Each poll records its
wall-clock time even on failure. Parse/conversion failures log and retain the
last valid snapshot. Values are stored atomically only after a complete parse
and recursive map-key conversion succeeds.

The YAML value shape is `propertyName: [{value: ..., constraints: {...}}]`.
Value maps are recursively converted to string-keyed maps; a non-string nested
value-map key errors. Constraint maps aren't converted. Filtered entries match
when their constraints form a subset of request filters; the first matching
constrained entry wins, otherwise the last unfiltered entry is the fallback.
Typed getters return the property default and an error for type mismatch or
invalid duration. Filebased `UpdateValue` validates the key/value, writes one
unfiltered entry back to the file and updates the current snapshot. Restore and
List are unsupported. The Fx wrapper falls back to NOP on filebased client
construction errors. The Matching Rust port will preserve that fallback for
bad/missing file configuration, while modern `configstore`, `openfeature`, and
unknown providers will return explicit unsupported-provider errors as directed
by the service migration gate.

Rust now parses Go's modern `dynamicconfig.client` and legacy
`dynamicConfigClient` fields, resolves relative paths against `--root` (default
cwd), loads a file snapshot before constructing Matching, and owns its watcher
through `MatchingRuntime`. The watcher uses the Go five-second minimum and
configured interval, swaps only a fully parsed new snapshot, and retains the
last good snapshot on reload errors. Invalid file configuration warns and uses
the Go Fx NOP fallback. Modern `configstore`, `openfeature`, and unknown
providers fail explicitly as scoped unsupported-provider limitations.
`DynamicConfigOverlay::from_file` parses ordered JSON values and constraints;
the existing admin overlay API remains separate. Focused parser coverage is
added, but no build or test command has yet run due to the active no-Cargo lane.
Fixtures cover the repository's actual Go `testConfig.yaml`, plus Go-shaped
missing/null values and constraints and nested integer/boolean map keys (which
Go's recursive conversion rejects). Runtime property readers currently return
their configured fallback on typed decode errors instead of surfacing the
file-client's typed error; that API-level discrepancy remains part of the
Matching parity audit. Filebased `UpdateValue`, `RestoreValue`, and `ListValue`
are not exposed through the Matching service's existing overlay handlers and
remain unsupported in this scoped port.

Matching persistence factories previously trimmed `persistence.defaultStore`
before lookup. Go uses the configured name as an exact map key, so both task
store and domain-cache factories now preserve it verbatim; regression tests
cover a padded name failing to match an unpadded configured datastore.

The production restart gate's first post-codec run completed both normal
SIGKILL/restart and graceful-drain Go SDK recovery workflows, then reached an
isolated-poller assertion that needs `system.enableTasklistIsolation` and
`system.allIsolationGroups`. A test-only attempt to set them through Matching's
HTTP endpoint failed because Matching-only mode serves the health router on
that endpoint; the harness change was reverted. The real config-file port is
needed to validate the behavior without adding a frontend or weakening the
assertion.
