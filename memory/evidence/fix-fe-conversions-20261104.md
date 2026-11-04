# Frontend conversion fixes (branch t3/fix-fe-conversions)

Fixes: C1/O10 gRPC FirstRunAt ns; C2 decision-task timestamps (model is ms, gRPC now uses ms->Timestamp);
O12 PendingActivityInfo 7 fields (Thrift+gRPC); O14 parentDomainId/autoResetPoints (+ActiveClusterSelectionPolicy);
C7 selection policy mapped on Start/SignalWithStart (Thrift 200/220, gRPC); C8 AutoConfigHint/TotalHistoryBytes on polls
(incl. empty decision poll hint); C9 per-domain frontend.historyMaxPageSize for decision poll history.

Deferred: O11 (Describe uses ms `info`): production Describe feeds ms through Thrift decode, so formatter is consistent; swapping is unsafe.
Residual: decision-poll model is ms so sub-ms precision of scheduled/started time is lost. Suspected latent bug: activity-poll Thrift
to_thrift multiplies raw-ns values by 1e6 (out of scope).

## CI evidence
(pending)
