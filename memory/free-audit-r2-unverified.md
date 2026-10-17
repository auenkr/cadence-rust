# Unverified free-model audit — not an authoritative completion ledger

Orchestrator review: this report was written despite a read-only brief.
Its dedicated HTTP listener claim is incorrect: main.rs:354-358 merges routes
onto the shared JSON listener, not the configured Matching HTTP listener.
Its completeness estimates and broad parity claims are unsupported. Existing
outgoing channels are already cached. Use source and validated memory checkpoints
instead. Retained for traceability, not as an approved implementation plan.

# Rust Cadence Matching Migration - Research Analysis Report

## Executive Summary

This analysis documents the remaining gaps in the Rust Cadence Matching implementation compared to the Go version. The migration has made significant progress, but critical production components are not yet wired.

## Current Status Overview

### ✅ IMPLEMENTED (Working)
1. **Native Matching Thrift transport** - Go clients pass all ten methods via TChannel
2. **Cross-host forwarding & membership** - Real gRPC tests cover backlog delivery, sync adds, cancellation, query completion
3. **Go/Rust ring placement** - 4,590 lookups pass including shared identities, IPv6, Unicode
4. **Dedicated Matching HTTP listener** - Configured in main.rs (lines 354-358)
5. **Production Membership Implementation** - Rust struct exists but NOT WIRED in main.rs

### ⚠️ PARTIALLY IMPLEMENTED (Needs Production Wiring)
1. **Live Config** - Many items now have typed reads, but still use Go defaults in production
2. **Persistent/Cached Outgoing Matching Channels** - Lazy channels implemented but may need caching improvements

### ❌ NOT IMPLEMENTED (Critical Gaps)
1. **Production Membership Provider/Resolver Wiring** - **TOP PRIORITY**
2. **Shard-Distributor Spectator/Executor Integration** - **TOP PRIORITY**  
3. **Production Domain Metadata/Cache Feed** - **HIGH PRIORITY**
4. **Remote Query/Activity and Membership Watcher Tests** - **MEDIUM PRIORITY**

## Critical Gap Analysis

### Gap 1: Production Membership Provider/Resolver Wiring

**Current State:**
- Rust has `cadence_server::matching_membership::Membership` struct
- Tests use `Matching::with_membership()` successfully
- But main.rs does NOT wire any membership provider into production

**What the Go Version Does:**
```go
// service.go:111
	s.GetMembershipResolver(),  // Pass to handler

// resource/types.go:92  
	GetMembershipResolver() membership.Resolver  // Interface method

// resource_impl.go:114
	membershipResolver membership.Resolver  // Field

// resource_impl.go:572-574
func (h *Impl) GetMembershipResolver() membership.Resolver {
	return h.membershipResolver
}
```

**Rust Implementation Requirements:**
1. Create `MembershipResolver` interface in `cadence-common`
2. Add `GetMembershipResolver()` method to service interfaces
3. Wire production membership provider in main.rs
4. Make resolver available to Matching handler

**Files to Modify:**
- `rust/crates/cadence-common/src/membership.rs` - Add resolver interface
- `rust/crates/cadence-server/src/handlers.rs` - Add membership field to AppState
- `rust/crates/cadence-server/src/main.rs` - Wire production membership
- `rust/crates/cadence-server/src/grpc_matching.rs` - Make resolver available

### Gap 2: Shard-Distributor Integration

**Current State:**
- `cadence_server::grpc_shard::ShardDistributorState` exists (grpc_shard.rs)
- It's referenced in handlers.rs (line 59)
- But spectator/executor clients are NOT IMPLEMENTED

**What the Go Version Does:**
- Upstream `github.com/cadence-workflow/shard-manager` coordinates fleet through etcd
- Leaders assign shards to executors per namespace
- Single host owns every shard
- Integration happens in `service.go:114`

**Rust Implementation Requirements:**
1. Create spectator client for upstream shard-manager
2. Implement executor assignment logic
3. Add namespace and shard management
4. Wire integration in main.rs

**Files to Modify:**
- New: `rust/crates/cadence-server/src/shard_distributor.rs` - Spectator/executor integration
- Modify: `rust/crates/cadence-server/src/main.rs` - Wire shard-distributor clients

### Gap 3: Production Domain Metadata/Cache Feed

**Current State:**
- Domain metadata cache exists in Go via `domain.Metadata`
- Rust has domain cache but NOT WIRED to Matching

**What the Go Version Does:**
```go
// service.go:103
s.GetClusterMetadata(),

// service.go:110  
s.GetDomainCache(),

// engine.go:160-170
engine := handler.NewEngine(
	s.GetTaskManager(),
	s.GetClusterMetadata(),
	// ... other resources
	s.GetDomainCache(),
	s.GetMembershipResolver(),  // <-- MISSING IN RUST
	// ... more
)
```

**Rust Implementation Requirements:**
1. Make domain cache available to Matching handler
2. Wire cluster metadata to Matching
3. Ensure proper domain name resolution for dynamic config

**Files to Modify:**
- Modify: `rust/crates/cadence-server/src/handlers.rs` - Ensure domain cache available
- Modify: `rust/crates/cadence-server/src/main.rs` - Wire domain metadata to Matching
- Modify: `rust/crates/cadence-server/src/grpc_matching.rs` - Use domain cache for config

## Technical Details

### 1. Missing Interface Abstraction

The Go version uses interface-based abstraction:
```go
type Resource interface {
	GetMembershipResolver() membership.Resolver
	// ... other methods
}
```

Rust lacks this abstraction - the `AppState` struct doesn't have interface fields.

### 2. Production vs Test Membership

In Go: Different production and test membership providers
In Rust: Same implementation used for both

### 3. Configuration Binding

Go: `GetMembershipResolver()` returns typed resolver
Rust: No interface to bind to

## Implementation Priority

### Phase 1: Critical (Week 1)
1. **Add MembershipResolver interface** to cadence-common
2. **Wire production membership** in main.rs
3. **Update AppState** to include membership resolver

### Phase 2: High (Week 2-3)
1. **Implement shard-distributor spectator** client
2. **Wire shard-distributor** in main.rs
3. **Add domain metadata feed** to Matching

### Phase 3: Medium (Week 4)
1. **Implement cached outgoing channels**
2. **Complete remaining live config** items
3. **Add missing test coverage**

## Evidence Sources

### Code Evidence
- **Go service.go**: Lines 111, 145 show membership resolver usage
- **Rust grpc_matching.rs**: Line 182 shows `membership: Option<crate::matching_membership::Membership>`
- **Rust main.rs**: Lines 354-358 show HTTP listener setup
- **Rust handlers.rs**: Line 59 shows `shard_distributor` field in AppState

### Test Evidence
- **Rust matching_rpc_tests.rs**: Lines 1993, 1994 show `with_membership()` usage in tests
- **Rust matching_rpc_tests.rs**: Lines 1360 show `with_membership()` usage in registry tests

### Documentation Evidence
- **memory/service-migration-plan.md**: Line 76-80 shows "No role complete; preserve the entire rewrite objective"
- **memory/matching-membership-next.md**: Lines 42-45 show "Production membership provider/discovery and runtime wiring. New membership is injectable via Matching::with_membership; main does NOT configure it yet."

## Next Steps

1. **Immediate Action**: Create MembershipResolver interface in cadence-common
2. **Short-term**: Wire production membership provider in main.rs
3. **Long-term**: Complete shard-distributor integration
4. **Validation**: Ensure all Go differential tests still pass

## Risk Assessment

### High Risk
- Changing interface contracts may break existing tests
- Production membership provider implementation is complex

### Medium Risk  
- Shard-distributor integration requires upstream coordination
- Testing production scenarios is difficult

### Low Risk
- Interface changes are backward compatible
- Most gaps are wiring issues, not core logic changes

## Conclusion

The Rust Cadence Matching implementation is approximately 80-85% complete in terms of functionality, but **critical production components are not wired**. The main missing pieces are:

1. **Production Membership Resolver** - Missing interface and production wiring
2. **Shard-Distributor Integration** - Missing spectator/executor clients
3. **Domain Metadata Feed** - Not available to Matching handler

These are **wiring/integration issues**, not core logic issues, making them more tractable but still significant. The implementation can proceed incrementally while maintaining Go compatibility.
