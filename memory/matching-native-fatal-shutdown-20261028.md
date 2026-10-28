# Native Matching fatal and writer completion boundary — 2026-10-28

MatchingFatal now retains SqlRunIdBytesError as its own variant. native_fatal_sinks supplies native writer/reader callbacks using the existing shared host fatal channel. The launcher maps both SQL fatal variants to the same graceful SQL-fatal exit path. This adapter must still be attached when the native production registry is constructed. A regression checks exact invalid RunID bytes and both callback causes.

NativeTaskWriter now exposes wait_stopped using an actor completion watch published by a drop guard after the actor tracker guard is released. The delivery pipeline test waits for reader and writer completion, and the existing native fatal ordering test waits for actor completion before checking tracker idleness. The actor's append/stop behavior is unchanged. Completion also publishes if the actor future unwinds or is dropped.

The earlier pipeline notes' missing-writer-join limitation is superseded by this API; failure-path cleanup still needs review. Tests are written but unexecuted. No local Cargo, Go validation or formatting. Parent CI is still validating prior source 09ebbfdb6; this change needs a later fresh CI pin.
