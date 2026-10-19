package history

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"testing"
	"time"

	"go.uber.org/mock/gomock"

	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/clock"
	cadencelog "github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/membership"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/service"
)

type historyPeerOracle struct {
	Hosts        []oracleHost           `json:"hosts"`
	Routes       map[string]string      `json:"routes"`
	ShardKeys    map[string]string      `json:"shardKeys"`
	AllGRPC      []string               `json:"allGrpc"`
	AllTChannel  []string               `json:"allTchannel"`
	AddressGRPC  string                 `json:"addressGrpc"`
	AddressTChan string                 `json:"addressTchannel"`
	AfterRemoval map[string]string      `json:"afterRemoval"`
	Remaining    []string               `json:"remaining"`
	Errors       map[string]oracleError `json:"errors"`
}

type oracleHost struct {
	Address  string `json:"address"`
	Identity string `json:"identity"`
	GRPC     string `json:"grpc"`
	TChannel string `json:"tchannel"`
}

type oracleError struct {
	Text      string `json:"text"`
	Transient bool   `json:"serviceTransient"`
}

func TestCadenceMatchingHistoryPeerPlacementOracle(t *testing.T) {
	snapshot := []membership.HostInfo{
		membership.NewDetailedHostInfo("10.8.0.1:7934", "history-a", membership.PortMap{membership.PortGRPC: 7834, membership.PortTchannel: 7934}),
		membership.NewDetailedHostInfo("10.8.0.2:7934", "history-b", membership.PortMap{membership.PortGRPC: 7835, membership.PortTchannel: 7935}),
		membership.NewDetailedHostInfo("10.8.0.3:7934", "history-c", membership.PortMap{membership.PortGRPC: 7836, membership.PortTchannel: 7936}),
	}
	ctrl := gomock.NewController(t)
	provider := membership.NewMockPeerProvider(ctrl)
	provider.EXPECT().GetMembers(service.History).DoAndReturn(func(string) ([]membership.HostInfo, error) {
		return append([]membership.HostInfo(nil), snapshot...), nil
	}).AnyTimes()
	provider.EXPECT().Subscribe(service.History, gomock.Any()).Return(nil).AnyTimes()
	provider.EXPECT().WhoAmI().Return(snapshot[0], nil).AnyTimes()
	provider.EXPECT().SelfEvict().Return(nil).AnyTimes()
	provider.EXPECT().Start(gomock.Any()).Return(nil).AnyTimes()
	provider.EXPECT().Stop(gomock.Any()).Return(nil).AnyTimes()

	// Use Cadence's real membership.Ring and ringpop-compatible hash placement.
	// The resolver mock below injects this live ring into the production
	// history.peerResolver so shard conversion and transport selection are both
	// exercised by the actual Go implementation.
	ts := clock.NewMockedTimeSourceAt(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	ring := membership.NewHashring(service.History, provider, ts, cadencelog.NewNoop(), metrics.NoopScope)
	if err := ring.Refresh(); err != nil {
		t.Fatal(err)
	}
	resolver := membership.NewMockResolver(ctrl)
	resolver.EXPECT().Lookup(service.History, gomock.Any()).DoAndReturn(func(_ string, key string) (membership.HostInfo, error) {
		return ring.Lookup(key)
	}).AnyTimes()
	resolver.EXPECT().Members(service.History).DoAndReturn(func(string) ([]membership.HostInfo, error) {
		return ring.Members(), nil
	}).AnyTimes()
	resolver.EXPECT().LookupByAddress(service.History, gomock.Any()).DoAndReturn(func(_ string, address string) (membership.HostInfo, error) {
		for _, host := range ring.Members() {
			if host.GetAddress() == address {
				return host, nil
			}
		}
		return membership.HostInfo{}, fmt.Errorf("host %q not found", address)
	}).AnyTimes()

	grpc := NewPeerResolver(32, resolver, membership.PortGRPC)
	tchannel := NewPeerResolver(32, resolver, membership.PortTchannel)
	oracle := historyPeerOracle{
		Routes: map[string]string{}, ShardKeys: map[string]string{},
		AfterRemoval: map[string]string{}, Errors: map[string]oracleError{},
	}
	for _, host := range snapshot {
		grpcAddress, _ := host.GetNamedAddress(membership.PortGRPC)
		tchannelAddress, _ := host.GetNamedAddress(membership.PortTchannel)
		oracle.Hosts = append(oracle.Hosts, oracleHost{host.GetAddress(), host.Identity(), grpcAddress, tchannelAddress})
	}
	for _, workflow := range []string{"workflow-1", "workflow-42", "emoji-🙂", "", "workflow/with/slash", "café"} {
		key := common.WorkflowIDToHistoryShard(workflow, 32)
		route, err := grpc.FromWorkflowID(workflow)
		if err != nil {
			t.Fatal(err)
		}
		oracle.ShardKeys["workflow:"+workflow] = string(rune(key))
		oracle.Routes["workflow:"+workflow] = route
	}
	for _, domain := range []string{"domain-1", "domain-42", "tenant-β", "", "domain/a"} {
		key := common.DomainIDToHistoryShard(domain, 32)
		route, err := grpc.FromDomainID(domain)
		if err != nil {
			t.Fatal(err)
		}
		oracle.ShardKeys["domain:"+domain] = string(rune(key))
		oracle.Routes["domain:"+domain] = route
	}
	for _, shard := range []int{0, 1, 31, 32, 65535, 0x110000} {
		key := string(rune(shard))
		route, err := grpc.FromShardID(shard)
		if err != nil {
			t.Fatal(err)
		}
		oracle.ShardKeys[fmt.Sprintf("shard:%d", shard)] = key
		oracle.Routes[fmt.Sprintf("shard:%d", shard)] = route
	}
	covered := map[string]bool{}
	for _, route := range oracle.Routes {
		covered[route] = true
	}
	if len(covered) != len(snapshot) {
		t.Fatalf("oracle keys did not exercise all %d History owners: %v", len(snapshot), covered)
	}
	for _, named := range []struct {
		key, address string
		client       PeerResolver
	}{
		{"allGrpc", "", grpc}, {"allTchannel", "", tchannel},
	} {
		peers, err := named.client.GetAllPeers()
		if err != nil {
			t.Fatal(err)
		}
		sort.Strings(peers)
		if named.key == "allGrpc" {
			oracle.AllGRPC = peers
		} else {
			oracle.AllTChannel = peers
		}
	}
	oracle.AddressGRPC, _ = grpc.FromHostAddress(snapshot[1].GetAddress())
	oracle.AddressTChan, _ = tchannel.FromHostAddress(snapshot[2].GetAddress())

	// Refresh after removing one peer and ensure lookups/all-peer views use the
	// refreshed actual ring rather than stale cached address metadata.
	snapshot = snapshot[:2]
	ts.Advance(2 * time.Second)
	if err := ring.Refresh(); err != nil {
		t.Fatal(err)
	}
	for _, shard := range []int{0, 1, 31, 32, 65535, 0x110000} {
		route, err := grpc.FromShardID(shard)
		if err != nil {
			t.Fatal(err)
		}
		oracle.AfterRemoval[fmt.Sprintf("shard:%d", shard)] = route
		if route == "10.8.0.3:7836" {
			t.Fatalf("removed History peer still owns shard %d: %s", shard, route)
		}
	}
	for _, host := range ring.Members() {
		address, err := host.GetNamedAddress(membership.PortGRPC)
		if err != nil {
			t.Fatal(err)
		}
		oracle.Remaining = append(oracle.Remaining, address)
	}
	sort.Strings(oracle.Remaining)

	// Exercise Go's service-transient error conversion for missing transport
	// metadata and resolver failures.
	missingPort := NewPeerResolver(32, resolver, "missing")
	_, missingErr := missingPort.FromShardID(1)
	oracle.Errors["missingNamedPort"] = oracleError{missingErr.Error(), common.IsServiceTransientError(missingErr)}
	failResolver := membership.NewMockResolver(ctrl)
	failResolver.EXPECT().Lookup(service.History, gomock.Any()).Return(membership.HostInfo{}, errors.New("injected lookup failure")).AnyTimes()
	_, lookupErr := NewPeerResolver(32, failResolver, membership.PortGRPC).FromShardID(1)
	oracle.Errors["lookupFailure"] = oracleError{lookupErr.Error(), common.IsServiceTransientError(lookupErr)}

	raw, err := json.Marshal(oracle)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("CADENCE_MATCHING_HISTORY_PEER_ORACLE=%s\n", raw)
	if path := os.Getenv("CADENCE_MATCHING_HISTORY_PEER_ORACLE_OUTPUT"); path != "" {
		if err := os.WriteFile(path, append(raw, '\n'), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}
