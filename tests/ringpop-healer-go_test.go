package swim

import (
	"encoding/json"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/stretchr/testify/require"
	"github.com/uber/ringpop-go/discovery/statichosts"
	"github.com/uber/ringpop-go/events"
)

type healerOracleChange struct {
	Address           string `json:"address"`
	Status            string `json:"status"`
	Incarnation       int64  `json:"incarnation"`
	Source            string `json:"source"`
	SourceIncarnation int64  `json:"sourceIncarnation"`
}

func healerChanges(changes []Change) []healerOracleChange {
	result := make([]healerOracleChange, 0, len(changes))
	for _, change := range changes {
		result = append(result, healerOracleChange{change.Address, change.Status, change.Incarnation, change.Source, change.SourceIncarnation})
	}
	return result
}

func TestGenerateRingpopHealerGoTrace(t *testing.T) {
	trace := map[string]interface{}{}
	for _, scenario := range []struct {
		name   string
		local  []Change
		remote []Change
	}{
		{"localFaultyWins", []Change{{Address: "shared", Status: Faulty, Incarnation: 20}}, []Change{{Address: "shared", Status: Alive, Incarnation: 10}}},
		{"remoteFaultyWins", []Change{{Address: "shared", Status: Alive, Incarnation: 10}}, []Change{{Address: "shared", Status: Faulty, Incarnation: 20}}},
		{"noConflict", []Change{{Address: "a", Status: Alive, Incarnation: 1}}, []Change{{Address: "b", Status: Alive, Incarnation: 2}}},
	} {
		forLocal, forRemote := nodesThatNeedToReincarnate(scenario.local, scenario.remote)
		trace[scenario.name] = map[string]interface{}{"forLocal": healerChanges(forLocal), "forRemote": healerChanges(forRemote)}
	}

	// Exercise the actual Go healer against a discovery provider containing an
	// unreachable target; it must report no successful target and one attempt.
	unknown := newChannelNode(t)
	defer unknown.Destroy()
	unknown.node.discoverProvider = statichosts.New("127.0.0.1:1")
	attempts := 0
	unknown.node.AddListener(on(AttemptHealEvent{}, func(events.Event) { attempts++ }))
	targets, err := unknown.node.healer.Heal()
	require.NoError(t, err)
	require.Empty(t, targets)
	require.Equal(t, 1, attempts)
	trace["unknownHost"] = map[string]interface{}{"attempts": attempts, "successfulTargets": len(targets)}

	unknown.node.memberlist.SetLocalStatus(Faulty)
	unknown.node.discoverProvider = statichosts.New(unknown.node.Address())
	attempts = 0
	_, err = unknown.node.healer.Heal()
	require.NoError(t, err)
	require.Equal(t, 1, attempts)
	trace["localHostFaultyCandidate"] = attempts

	unknown.node.discoverProvider = statichosts.New("invalid duplicate", "other", "invalid duplicate")
	attempts = 0
	_, err = unknown.node.healer.Heal()
	require.NoError(t, err)
	require.Equal(t, 2, attempts, "Go del removes all duplicate instances after the first failure")
	remaining := []string{"invalid duplicate", "other", "invalid duplicate"}
	remaining = del(remaining, "invalid duplicate")
	trace["delDuplicateRemaining"] = remaining
	trace["duplicateTargetAttempts"] = attempts

	// The exact retry cap is observable at the real healer boundary.
	failures := make([]string, 20)
	for i := range failures {
		failures[i] = "invalid host" + itoa(i)
	}
	unknown.node.discoverProvider = statichosts.New(failures...)
	attempts = 0
	_, err = unknown.node.healer.Heal()
	require.NoError(t, err)
	require.Equal(t, 10, attempts)
	trace["failureCap"] = attempts

	// A discovery error does not replace the previous host count; Probability
	// then raises that count to at least CountReachableMembers (local included).
	unknown.node.healer.previousHostListSize = 7
	unknown.node.discoverProvider = &BrokenDiscoverProvider{}
	_, err = unknown.node.healer.Heal()
	require.Error(t, err)
	trace["failedDiscoveryPreviousHostCount"] = unknown.node.healer.previousHostListSize
	trace["maxReverseFullSyncJobs"] = unknown.node.maxReverseFullSyncJobs
	trace["partitionHealPeriodNanos"] = int64(unknown.node.healer.period)
	reverseNode := newChannelNode(t)
	defer reverseNode.Destroy()
	maxJobs := reverseNode.node.maxReverseFullSyncJobs
	block := make(chan struct{})
	quit := make(chan struct{})
	var started int64
	reverseNode.node.AddListener(on(StartReverseFullSyncEvent{}, func(events.Event) {
		atomic.AddInt64(&started, 1)
		<-block
	}))
	reverseNode.node.AddListener(on(OmitReverseFullSyncEvent{}, func(events.Event) {
		go func() {
			for i := 0; i < maxJobs; i++ {
				block <- struct{}{}
			}
			close(quit)
		}()
	}))
	for i := 0; i < maxJobs+1; i++ {
		reverseNode.node.disseminator.tryStartReverseFullSync("127.0.0.1:1", time.Second)
	}
	select {
	case <-quit:
	case <-time.After(time.Second):
		t.Fatal("reverse full-sync cap did not omit the excess job")
	}
	require.Equal(t, int64(maxJobs), atomic.LoadInt64(&started))
	deadline := time.Now().Add(2 * time.Second)
	for len(reverseNode.node.disseminator.reverseFullSyncJobs) != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	require.Zero(t, len(reverseNode.node.disseminator.reverseFullSyncJobs))
	trace["reverseFullSyncJobsStarted"] = atomic.LoadInt64(&started)
	probabilityNode := newChannelNode(t)
	defer probabilityNode.Destroy()
	probabilityNode.node.healer.previousHostListSize = 0
	trace["probabilityLocalReachable"] = probabilityNode.node.healer.Probability()
	probabilityNode.node.healer.previousHostListSize = 8
	trace["probabilityDoesNotShrink"] = probabilityNode.node.healer.Probability()

	// Exercise the real receiver full-sync decision in a no-remote-member node.
	fullSyncNode := newChannelNode(t)
	defer fullSyncNode.Destroy()
	fullSyncNode.node.memberlist.MakeAlive(fullSyncNode.node.Address(), 1)
	fullSyncNode.node.disseminator.ClearChanges()
	full, fullSync := fullSyncNode.node.disseminator.IssueAsReceiver("peer", 1, fullSyncNode.node.memberlist.Checksum()+1)
	trace["noPendingChangesFullSync"] = map[string]interface{}{"fullSync": fullSync, "membersReturned": len(full)}

	// A live two-partition exchange exercises the Go JOIN/PING repair path and
	// checks converged membership, rather than setting a final membership view.
	a, b := newPartition(t, 1), newPartition(t, 1)
	defer destroyNodes(a...)
	defer destroyNodes(b...)
	remote, err := AttemptHeal(a[0].node, b[0].node.Address())
	require.NoError(t, err)
	require.Len(t, remote, 1)
	waitForPartitionHeal(t, 100, a, b)
	membersA, membersB := a[0].node.memberlist.GetMembers(), b[0].node.memberlist.GetMembers()
	allAlive := len(membersA) == 2 && len(membersB) == 2
	for _, member := range append(membersA, membersB...) {
		allAlive = allAlive && member.Status == Alive
	}
	require.True(t, allAlive)
	trace["twoPartitionRepair"] = map[string]interface{}{"membersA": len(membersA), "membersB": len(membersB), "allAlive": allAlive}

	// Stop's unbuffered quit send waits for an in-flight Heal pass. Hold a real
	// JOIN handler after it receives the request, prove Stop is still waiting,
	// then release it and observe bounded completion.
	stopSource, stopTarget := newChannelNode(t), newChannelNode(t)
	defer destroyNodes(stopSource, stopTarget)
	entered, release := make(chan struct{}), make(chan struct{})
	stopTarget.node.AddListener(on(JoinReceiveEvent{}, func(events.Event) {
		select {
		case <-entered:
		default:
			close(entered)
		}
		<-release
	}))
	stopSource.node.discoverProvider = statichosts.New(stopTarget.node.Address())
	stopSource.node.healer.period = time.Millisecond
	stopSource.node.clock = clock.New()
	stopSource.node.healer.Start()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("healer did not enter its JOIN attempt")
	}
	stopDone := make(chan struct{})
	go func() { stopSource.node.healer.Stop(); close(stopDone) }()
	select {
	case <-stopDone:
		t.Fatal("Go healer Stop returned while Heal was still in progress")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	select {
	case <-stopDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Go healer Stop did not finish after the active attempt returned")
	}
	trace["stopWaitsForActiveHeal"] = true

	data, err := json.MarshalIndent(trace, "", "  ")
	require.NoError(t, err)
	if output := os.Getenv("CADENCE_RINGPOP_HEALER_OUTPUT"); output != "" {
		require.NoError(t, os.WriteFile(output, append(data, '\n'), 0600))
	}
	t.Log("CADENCE_RINGPOP_HEALER_ORACLE=" + string(data))
}

// Avoid importing strconv solely for port formatting in the test fixture.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var digits [20]byte
	i := len(digits)
	for n > 0 {
		i--
		digits[i] = byte('0' + n%10)
		n /= 10
	}
	return string(digits[i:])
}
