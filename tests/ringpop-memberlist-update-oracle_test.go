package swim

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/uber/ringpop-go/logging"
	"github.com/uber/ringpop-go/util"
)

type updateSnapshot struct {
	Name           string    `json:"name"`
	NowUnixSeconds int64     `json:"nowUnixSeconds"`
	NowNanoseconds int       `json:"nowNanoseconds"`
	InitialLocal   Member    `json:"initialLocal"`
	InitialMembers []Member  `json:"initialMembers"`
	Input          []Change  `json:"input"`
	Applied  []Change `json:"applied"`
	Members  []Member `json:"members"`
	Checksum uint32   `json:"checksum"`
	Local    Member   `json:"local"`
}

func TestGenerateRingpopMemberlistUpdateFixtures(t *testing.T) {
	out := os.Getenv("CADENCE_RINGPOP_UPDATE_OUTPUT")
	if out == "" {
		t.Fatal("CADENCE_RINGPOP_UPDATE_OUTPUT is required")
	}
	now := time.Unix(1700000000, 123000000)
	cases := []struct {
		name   string
		local  Member
		inputs []Change
	}{
		{"remote_insert_update", Member{Address: "local", Status: Alive, Incarnation: 100}, []Change{
			{Source: "sender", SourceIncarnation: 7, Address: "peer", Incarnation: 2, Status: Alive, Labels: map[string]string{"zone": "west"}, Timestamp: util.Timestamp(time.Unix(11, 0))},
			{Source: "sender", SourceIncarnation: 7, Address: "peer", Incarnation: 3, Status: Suspect, Labels: map[string]string{"zone": "east"}, Timestamp: util.Timestamp(time.Unix(12, 0))},
		}},
		{"unknown_tombstone_rejected", Member{Address: "local", Status: Alive, Incarnation: 100}, []Change{{Source: "sender", SourceIncarnation: 7, Address: "unknown-tomb", Incarnation: 9, Status: Tombstone, Timestamp: util.Timestamp(time.Unix(21, 0))}}},
		{"local_stale_equal_newer_refutation", Member{Address: "local", Status: Alive, Incarnation: 100, Labels: LabelMap{"zone": "west"}}, []Change{
			{Source: "sender", SourceIncarnation: 7, Address: "local", Incarnation: 99, Status: Suspect, Timestamp: util.Timestamp(time.Unix(31, 0))},
			{Source: "sender", SourceIncarnation: 7, Address: "local", Incarnation: 100, Status: Faulty, Timestamp: util.Timestamp(time.Unix(32, 0))},
			{Source: "sender", SourceIncarnation: 7, Address: "local", Incarnation: 101, Status: Suspect, Timestamp: util.Timestamp(time.Unix(33, 0))},
		}},
	}
	for _, tc := range cases {
		captureUpdate(t, tc.name, tc.local, nil, tc.inputs, now)
	}
	statuses := []string{Alive, Suspect, Faulty, Leave, Tombstone, "unknown"}
	for _, oldStatus := range statuses {
		for _, gossipStatus := range statuses {
			for _, delta := range []int64{-1, 0, 1} {
				name := oldStatus + "_to_" + gossipStatus + "_inc_" + map[int64]string{-1: "older", 0: "equal", 1: "newer"}[delta]
				old := &Member{Address: "peer", Status: oldStatus, Incarnation: 10}
				input := []Change{{Source: "sender", SourceIncarnation: 7, Address: "peer", Incarnation: 10 + delta, Status: gossipStatus, Timestamp: util.Timestamp(time.Unix(40, 0))}}
				captureUpdate(t, name, Member{Address: "local", Status: Alive, Incarnation: 100}, old, input, now)
			}
		}
	}
	inputs := make([]Change, 80)
	for i := range inputs {
		inputs[i] = Change{Source: "batch", SourceIncarnation: 4, Address: fmt.Sprintf("peer-%d", i), Incarnation: int64(i + 1), Status: Alive, Timestamp: util.Timestamp(time.Unix(int64(100+i), 0))}
	}
	captureUpdate(t, "batch_80_no_truncation", Member{Address: "local", Status: Alive, Incarnation: 100}, nil, inputs, now)

	data, err := json.MarshalIndent(map[string]interface{}{"source": "github.com/uber/ringpop-go v0.10.0 swim/memberlist.go Update", "cases": generatedUpdateCases}, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(out, append(data, '\n'), 0644); err != nil {
		t.Fatal(err)
	}
}

var generatedUpdateCases []updateSnapshot

func captureUpdate(t *testing.T, name string, local Member, old *Member, inputs []Change, now time.Time) {
	t.Helper()
	mockClock := clock.NewMock()
	mockClock.Add(now.Sub(time.Unix(0, 0)))
	n := &Node{address: local.Address, clock: mockClock, logger: logging.Logger("fixture"), maxReverseFullSyncJobs: 1}
	ml := &memberlist{node: n, local: &local, logger: logging.Logger("fixture")}
	ml.members.byAddress = map[string]*Member{local.Address: &local}
	ml.members.list = []*Member{&local}
	if old != nil {
		ml.members.byAddress[old.Address] = old
		ml.members.list = append(ml.members.list, old)
	}
	n.memberlist = ml
	n.disseminator = newDisseminator(n)
	n.stateTransitions = newStateTransitions(n, StateTimeouts{})
	initialLocal := *ml.local
	initialMembers := make([]Member, 0, len(ml.members.list))
	for _, member := range ml.members.list {
		initialMembers = append(initialMembers, *member)
	}
	copyInput := append([]Change(nil), inputs...)
	// Call the pinned Ringpop implementation directly. Update may normalize
	// incoming changes in place; preserving this slice captures its exact input.
	applied := ml.Update(copyInput)
	members := ml.GetMembers()
	sort.Slice(members, func(i, j int) bool { return members[i].Address < members[j].Address })
	generatedUpdateCases = append(generatedUpdateCases, updateSnapshot{Name: name, NowUnixSeconds: now.Unix(), NowNanoseconds: now.Nanosecond(), InitialLocal: initialLocal, InitialMembers: initialMembers, Input: inputs, Applied: applied, Members: members, Checksum: ml.Checksum(), Local: *ml.local})
}
