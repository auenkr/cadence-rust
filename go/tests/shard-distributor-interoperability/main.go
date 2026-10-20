// Command shard-distributor-interoperability runs the pinned Go shard-manager
// handlers over their generated YARPC gRPC wrappers for Rust interop tests.
// It is deliberately a small deterministic fixture, not a replacement server.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	"github.com/cadence-workflow/shard-manager/common/clock"
	"github.com/cadence-workflow/shard-manager/common/log"
	"github.com/cadence-workflow/shard-manager/common/metrics"
	"github.com/cadence-workflow/shard-manager/common/types"
	"github.com/cadence-workflow/shard-manager/service/sharddistributor/config"
	"github.com/cadence-workflow/shard-manager/service/sharddistributor/handler"
	"github.com/cadence-workflow/shard-manager/service/sharddistributor/store"
	grpcwrapper "github.com/cadence-workflow/shard-manager/service/sharddistributor/wrappers/grpc"
	"go.uber.org/yarpc"
	grpctransport "go.uber.org/yarpc/transport/grpc"
)

// fixtureStore overrides the operations exercised by the real Go handlers;
// embedding preserves the pinned Store interface without fake RPC handlers.
type fixtureStore struct {
	store.Store
	mu          sync.Mutex
	owners      map[string]*store.ShardOwner
	heartbeats  map[string]store.HeartbeatState
	assignments map[string]*store.AssignedState
	subscribers map[chan map[*store.ShardOwner][]string]struct{}
}

func newFixtureStore() *fixtureStore {
	return &fixtureStore{owners: map[string]*store.ShardOwner{}, heartbeats: map[string]store.HeartbeatState{}, assignments: map[string]*store.AssignedState{}, subscribers: map[chan map[*store.ShardOwner][]string]struct{}{}}
}
func key(namespace, id string) string { return namespace + "\x00" + id }
func (s *fixtureStore) GetShardOwner(_ context.Context, namespace, shard string) (*store.ShardOwner, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	owner := s.owners[key(namespace, shard)]
	if owner == nil {
		return nil, store.ErrShardNotFound
	}
	copy := *owner
	return &copy, nil
}
func (s *fixtureStore) GetHeartbeat(_ context.Context, namespace, executor string) (*store.HeartbeatState, *store.AssignedState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	h, ok := s.heartbeats[key(namespace, executor)]
	if !ok {
		return nil, nil, store.ErrExecutorNotFound
	}
	assigned := s.assignments[key(namespace, executor)]
	return &h, assigned, nil
}
func (s *fixtureStore) RecordHeartbeat(_ context.Context, namespace, executor string, state store.HeartbeatState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.heartbeats[key(namespace, executor)] = state
	return nil
}
func (s *fixtureStore) SubscribeToAssignmentChanges(ctx context.Context, _ string) (<-chan map[*store.ShardOwner][]string, func(), error) {
	ch := make(chan map[*store.ShardOwner][]string, 8)
	s.mu.Lock()
	s.subscribers[ch] = struct{}{}
	s.mu.Unlock()
	var once sync.Once
	cancel := func() { once.Do(func() { s.mu.Lock(); delete(s.subscribers, ch); close(ch); s.mu.Unlock() }) }
	go func() { <-ctx.Done(); cancel() }()
	return ch, cancel, nil
}
func (s *fixtureStore) setAssignment(namespace, executor string, shardIDs []string, metadata map[string]string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for executorKey, previous := range s.assignments {
		if len(executorKey) <= len(namespace)+1 || executorKey[:len(namespace)+1] != namespace+"\x00" {
			continue
		}
		for shard := range previous.AssignedShards {
			delete(s.owners, key(namespace, shard))
		}
		previous.AssignedShards = map[string]*types.ShardAssignment{}
	}
	assigned := &store.AssignedState{AssignedShards: map[string]*types.ShardAssignment{}, LastUpdated: time.Now().UTC()}
	for _, shard := range shardIDs {
		assigned.AssignedShards[shard] = &types.ShardAssignment{Status: types.AssignmentStatusREADY}
		s.owners[key(namespace, shard)] = &store.ShardOwner{ExecutorID: executor, Metadata: clone(metadata)}
	}
	s.assignments[key(namespace, executor)] = assigned
	owners := map[*store.ShardOwner][]string{{ExecutorID: executor, Metadata: clone(metadata)}: append([]string(nil), shardIDs...)}
	for ch := range s.subscribers {
		select {
		case ch <- owners:
		default:
		}
	}
}
func clone(in map[string]string) map[string]string {
	out := map[string]string{}
	for k, v := range in {
		out[k] = v
	}
	return out
}

// stdin accepts one JSON command per line; the Rust test controls ownership
// transitions without adding a fake implementation of the Go RPC surface.
type command struct {
	Op        string            `json:"op"`
	Namespace string            `json:"namespace"`
	Executor  string            `json:"executor"`
	Shards    []string          `json:"shards"`
	Metadata  map[string]string `json:"metadata"`
}

func main() {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	must(err)
	fixture := newFixtureStore()
	timeSource := clock.NewRealTimeSource()
	cfg := config.ShardDistribution{Namespaces: []config.Namespace{{Name: "matching", Type: "fixed"}}}
	api := handler.NewHandler(log.NewNoop(), timeSource, cfg, &config.Config{}, fixture)
	api.Start()
	executor := handler.NewExecutorHandler(log.NewNoop(), fixture, timeSource, cfg, metrics.NoopClient)
	transport := grpctransport.NewTransport()
	dispatcher := yarpc.NewDispatcher(yarpc.Config{Name: "shard-manager", Inbounds: yarpc.Inbounds{transport.NewInbound(listener)}})
	grpcwrapper.NewGRPCHandler(api).Register(dispatcher)
	grpcwrapper.NewExecutorGRPCExecutor(executor).Register(dispatcher)
	must(dispatcher.Start())
	defer dispatcher.Stop()
	defer api.Stop()
	must(json.NewEncoder(os.Stdout).Encode(map[string]string{"address": listener.Addr().String(), "namespace": "matching"}))
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		var cmd command
		if err := json.Unmarshal(scanner.Bytes(), &cmd); err != nil {
			respond(err.Error())
			continue
		}
		switch cmd.Op {
		case "assign", "reassign":
			fixture.setAssignment(cmd.Namespace, cmd.Executor, cmd.Shards, cmd.Metadata)
			respond("ok")
		case "lose":
			fixture.setAssignment(cmd.Namespace, "", nil, nil)
			respond("ok")
		case "stats":
			respond(fixture.snapshot(cmd.Namespace))
		case "stop":
			respond("stopping")
			return
		default:
			respond(fmt.Sprintf("unknown operation %q", cmd.Op))
		}
	}
	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, err)
	}
}
func respond(value any) { _ = json.NewEncoder(os.Stdout).Encode(map[string]any{"result": value}) }
func must(err error) {
	if err != nil {
		panic(err)
	}
}

func (s *fixtureStore) snapshot(namespace string) map[string]store.HeartbeatState {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := map[string]store.HeartbeatState{}
	prefix := namespace + "\x00"
	for k, state := range s.heartbeats {
		if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			result[k[len(prefix):]] = state
		}
	}
	return result
}
