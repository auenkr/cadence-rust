package swim

import (
	"encoding/json"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/uber/ringpop-go/logging"
)

type joinAddFixture struct {
	Name          string                      `json:"name"`
	NowMillis     int64                       `json:"nowMillis"`
	Local         Member                      `json:"local"`
	Members       []Member                    `json:"members"`
	Input         []Change                    `json:"input"`
	FinalMembers  []Member                    `json:"finalMembers"`
	Checksum      uint32                      `json:"checksum"`
	InitialQueued map[string]queuedJoinChange `json:"initialQueued"`
	Queued        map[string]queuedJoinChange `json:"queued"`
	MaxP          int                         `json:"maxP"`
}

type queuedJoinChange struct {
	Change     *Change `json:"change"`
	Propagated int     `json:"propagated"`
}

func TestGenerateRingpopJoinAddJoinListFixtures(t *testing.T) {
	path := os.Getenv("CADENCE_RINGPOP_JOIN_ADDJOINLIST_OUTPUT")
	if path == "" {
		t.Fatal("CADENCE_RINGPOP_JOIN_ADDJOINLIST_OUTPUT is required")
	}
	fixtures := make([]joinAddFixture, 0, 4)
	nowMillis := int64(1700000000123)
	for _, incoming := range []struct {
		status      string
		incarnation int64
	}{
		{Suspect, 100}, {Suspect, 101}, {Faulty, 100}, {Faulty, 101},
	} {
		name := incoming.status + map[int64]string{100: "_equal", 101: "_newer"}[incoming.incarnation]
		local := Member{Address: "local", Status: Alive, Incarnation: 100, Labels: LabelMap{"serviceName": "matching", "zone": "west"}}
		members := []Member{local}
		for i := 0; i < 10; i++ {
			members = append(members, Member{Address: "peer-" + string(rune('0'+i)), Status: Alive, Incarnation: 1, Labels: LabelMap{"serviceName": "history"}})
		}
		mockClock := clock.NewMock()
		mockClock.Add(time.UnixMilli(nowMillis).Sub(time.Unix(0, 0)))
		n := &Node{address: "local", clock: mockClock, logger: logging.Logger("join-fixture"), maxReverseFullSyncJobs: 1}
		local = members[0]
		ml := &memberlist{node: n, local: &members[0], logger: logging.Logger("join-fixture")}
		ml.members.byAddress = make(map[string]*Member, len(members))
		ml.members.list = make([]*Member, 0, len(members))
		for i := range members {
			member := &members[i]
			ml.members.byAddress[member.Address] = member
			ml.members.list = append(ml.members.list, member)
		}
		n.memberlist = ml
		n.disseminator = newDisseminator(n)
		n.stateTransitions = newStateTransitions(n, StateTimeouts{})
		for _, address := range []string{"local", "peer-0", "untouched"} {
			n.disseminator.RecordChange(Change{Address: address, Status: Alive, Incarnation: 1})
		}
		n.disseminator.changes["peer-0"].p = 4
		n.disseminator.changes["untouched"].p = 6
		initialQueued := captureJoinQueue(n.disseminator)
		inputs := []Change{
			{Source: "seed", SourceIncarnation: 8, Address: "local", Incarnation: incoming.incarnation, Status: incoming.status, Labels: LabelMap{"serviceName": "remote"}},
			{Source: "seed", SourceIncarnation: 8, Address: "peer-0", Incarnation: 2, Status: Suspect, Labels: LabelMap{"serviceName": "history"}},
		}
		initialMembers := append([]Member(nil), members...)
		ml.AddJoinList(inputs)
		finalMembers := ml.GetMembers()
		sort.Slice(finalMembers, func(i, j int) bool { return finalMembers[i].Address < finalMembers[j].Address })
		queued := captureJoinQueue(n.disseminator)
		fixtures = append(fixtures, joinAddFixture{Name: name, NowMillis: nowMillis, Local: local, Members: initialMembers, Input: inputs, FinalMembers: finalMembers, Checksum: ml.Checksum(), InitialQueued: initialQueued, Queued: queued, MaxP: n.disseminator.maxP})
	}
	data, err := json.MarshalIndent(map[string]interface{}{"source": "github.com/uber/ringpop-go v0.10.0 swim/memberlist.go AddJoinList + Update + Node.handleChanges", "cases": fixtures}, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(data, '\n'), 0644); err != nil {
		t.Fatal(err)
	}
}

func captureJoinQueue(d *disseminator) map[string]queuedJoinChange {
	queued := make(map[string]queuedJoinChange)
	for address, pending := range d.changes {
		queued[address] = queuedJoinChange{Change: &pending.Change, Propagated: pending.p}
	}
	return queued
}
