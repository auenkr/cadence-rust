package handler

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"go.uber.org/mock/gomock"

	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/clock"
	cadencelog "github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/membership"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/service"
	"github.com/uber/cadence/common/types"
	"github.com/uber/cadence/service/matching/config"
)

type ownerOracleHost struct { Address string `json:"address"`; Identity string `json:"identity"`; GRPC string `json:"grpc_address"` }
type ownerOraclePartition struct { Key string `json:"key"`; Owner string `json:"owner"` }
type ownerOracleResponse struct { Activity []ownerOraclePartition `json:"activity"`; Decision []ownerOraclePartition `json:"decision"` }
type ownerOracleScenario struct { Name string `json:"name"`; Hosts []ownerOracleHost `json:"hosts"`; Root ownerOracleResponse `json:"root"`; Child ownerOracleResponse `json:"child"`; Empty ownerOracleResponse `json:"empty"` }
type ownerOracle struct { Scenarios []ownerOracleScenario `json:"scenarios"` }

func TestCadenceMatchingPartitionOwnerOrderOracle(t *testing.T) {
	ctrl := gomock.NewController(t)
	provider := membership.NewMockPeerProvider(ctrl)
	var members []membership.HostInfo
	provider.EXPECT().GetMembers(service.Matching).DoAndReturn(func(string) ([]membership.HostInfo, error) { return append([]membership.HostInfo(nil), members...), nil }).AnyTimes()
	provider.EXPECT().Subscribe(service.Matching, gomock.Any()).Return(nil).AnyTimes()
	ts := clock.NewMockedTimeSourceAt(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	ring := membership.NewHashring(service.Matching, provider, ts, cadencelog.NewNoop(), metrics.NoopScope)
	resolver, err := membership.NewResolver(nil, metrics.NewNoopMetricsClient(), cadencelog.NewNoop(), map[string]membership.SingleProvider{service.Matching: ring})
	if err != nil { t.Fatal(err) }
	domainCache := cache.NewMockDomainCache(ctrl)
	domainCache.EXPECT().GetDomainID("domain").Return("domain-id", nil).AnyTimes()
	e := &matchingEngineImpl{ membershipResolver: resolver, domainCache: domainCache, config: &config.Config{NumTasklistWritePartitions: func(string, string, int) int { return 3 }} }
	makeHosts := func(names []string) []membership.HostInfo {
		out := make([]membership.HostInfo, 0, len(names))
		for _, name := range names { out = append(out, membership.NewDetailedHostInfo(name+":7935", name, membership.PortMap{membership.PortGRPC: 7835})) }
		return out
	}
	resp := func(name string) ownerOracleResponse {
		r, err := e.ListTaskListPartitions(nil, &types.MatchingListTaskListPartitionsRequest{Domain: "domain", TaskList: &types.TaskList{ Name: name }})
		if err != nil { t.Fatal(err) }
		convert := func(in []*types.TaskListPartitionMetadata) []ownerOraclePartition { out := make([]ownerOraclePartition, 0, len(in)); for _, p := range in { out = append(out, ownerOraclePartition{p.GetKey(), p.GetOwnerHostName()}) }; return out }
		return ownerOracleResponse{convert(r.ActivityTaskListPartitions), convert(r.DecisionTaskListPartitions)}
	}
	var oracle ownerOracle
	for _, scenario := range []struct{name string; hosts []membership.HostInfo}{
		{"baseline", makeHosts([]string{"one", "two", "three"})},
		{"add", makeHosts([]string{"one", "two", "three", "four"})},
		{"remove", makeHosts([]string{"one", "three"})},
		{"identity-replacement", []membership.HostInfo{membership.NewDetailedHostInfo("one:7935", "one-v2", nil), membership.NewDetailedHostInfo("three:7935", "three", nil)}},
		{"address-replacement", []membership.HostInfo{membership.NewDetailedHostInfo("one-new:7935", "one-v2", nil), membership.NewDetailedHostInfo("three:7935", "three", nil)}},
		{"empty", nil},
	} {
		members = scenario.hosts
		ts.Advance(2 * time.Second) // production hashring suppresses refreshes within one second
		if err := ring.Refresh(); err != nil { t.Fatal(err) }
		entry := ownerOracleScenario{Name: scenario.name}
		for _, host := range members { entry.Hosts = append(entry.Hosts, ownerOracleHost{host.GetAddress(), host.Identity(), ""}) }
		entry.Root = resp("queue")
		entry.Child = resp("/__cadence_sys/queue/9")
		entry.Empty = resp("")
		oracle.Scenarios = append(oracle.Scenarios, entry)
	}
	file := os.Getenv("CADENCE_MATCHING_OWNER_ORACLE_OUTPUT")
	if file == "" { t.Fatal("CADENCE_MATCHING_OWNER_ORACLE_OUTPUT is required") }
	b, err := json.MarshalIndent(oracle, "", "  "); if err != nil { t.Fatal(err) }
	if err := os.WriteFile(file, append(b, '\n'), 0o644); err != nil { t.Fatal(err) }
}
