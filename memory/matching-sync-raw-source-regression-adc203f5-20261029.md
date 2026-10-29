# Matching sync activity raw source regression

Based on d9ac938ba, added a native engine test exercising a forwarded activity sync match against a loopback History gRPC service. The source cache holds the uppercase UUID as its exact metadata key; the test checks that History receives that spelling, that the task-list queue belongs to the target domain, that the poll returns the activity and token, and that neither source nor target has persisted task rows. This covers successful delivery rather than only forwarding or unmatched persistence.

No Cargo, rustfmt, Go, runtime, or CI validation was run as directed. The parent must run the matching Actions gate and review any formatting or compile failures; this note makes no pass claim. This isolated regression is not mixed-cluster or SDK parity evidence.
