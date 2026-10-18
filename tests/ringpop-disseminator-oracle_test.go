package swim

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/uber/ringpop-go/util"

	"github.com/uber/ringpop-go/logging"
)

type disseminationSnapshot struct {
	Changes  []Change       `json:"changes"`
	Pending  int            `json:"pending"`
	Counters map[string]int `json:"counters"`
}

func snapshot(d *disseminator) disseminationSnapshot {
	s := disseminationSnapshot{Changes: []Change{}, Pending: d.ChangesCount(), Counters: map[string]int{}}
	for address, change := range d.changes {
		s.Changes = append(s.Changes, change.Change.validateOutgoing())
		s.Counters[address] = change.p
	}
	sort.Slice(s.Changes, func(i, j int) bool { return s.Changes[i].Address < s.Changes[j].Address })
	return s
}

func fixtureDisseminator(local Member, members ...Member) *disseminator {
	n := &Node{address: local.Address, logger: logging.Logger("fixture")}
	ml := &memberlist{node: n, local: &local, logger: logging.Logger("fixture")}
	ml.members.byAddress = map[string]*Member{}
	for i := range members {
		m := members[i]
		if _, exists := ml.members.byAddress[m.Address]; exists {
			continue
		}
		ml.members.list = append(ml.members.list, &m)
		ml.members.byAddress[m.Address] = &m
	}
	if _, ok := ml.members.byAddress[local.Address]; !ok {
		ml.members.list = append(ml.members.list, ml.local)
		ml.members.byAddress[local.Address] = ml.local
	}
	n.memberlist = ml
	d := newDisseminator(n)
	d.maxP = 3
	return d
}

func TestGenerateRingpopDisseminatorFixtures(t *testing.T) {
	out := os.Getenv("CADENCE_RINGPOP_DISSEMINATOR_OUTPUT")
	if out == "" {
		t.Fatal("CADENCE_RINGPOP_DISSEMINATOR_OUTPUT is required")
	}
	result := map[string]interface{}{"source": "github.com/uber/ringpop-go v0.10.0 swim/disseminator.go"}

	// RecordChange replaces the queued entry by address and resets its p count.
	d := fixtureDisseminator(Member{Address: "local", Status: Alive, Incarnation: 41})
	d.RecordChange(Change{Source: "s", SourceIncarnation: 7, Address: "peer", Incarnation: 2, Status: Alive, Timestamp: util.Timestamp(time.Unix(11, 0))})
	d.changes["peer"].p = 2
	d.RecordChange(Change{Source: "s2", SourceIncarnation: 8, Address: "peer", Incarnation: 3, Status: Suspect, Timestamp: util.Timestamp(time.Unix(12, 0))})
	result["recordChange"] = snapshot(d)

	// Sender snapshots count only when their callback is invoked. The callback
	// indexes by address, so a replacement between issue and callback is bumped.
	d = fixtureDisseminator(Member{Address: "local", Status: Alive, Incarnation: 41})
	d.RecordChange(Change{Source: "old", Address: "peer", Incarnation: 1, Status: Alive})
	sent, callback := d.IssueAsSender()
	d.RecordChange(Change{Source: "new", Address: "peer", Incarnation: 2, Status: Suspect, Timestamp: util.Timestamp(time.Unix(19, 0))})
	callback()
	replaced := snapshot(d)
	d.RecordChange(Change{Source: "peer", Address: "failed", Incarnation: 3, Status: Alive})
	failedSent, _ := d.IssueAsSender() // failed delivery: deliberately do not invoke callback
	failedAfter := snapshot(d)
	result["senderCallbackReplacement"] = map[string]interface{}{"issued": sent, "afterCallback": replaced}
	result["failedCallbackNotInvoked"] = map[string]interface{}{"issued": failedSent, "afterFailedDelivery": failedAfter}

	// Repeated successful sender callbacks remove a change at maxP.
	d = fixtureDisseminator(Member{Address: "local", Status: Alive, Incarnation: 41})
	d.RecordChange(Change{Source: "local", SourceIncarnation: 41, Address: "repeat", Incarnation: 4, Status: Alive})
	repeats := []disseminationSnapshot{}
	for i := 0; i < 4; i++ {
		cs, cb := d.IssueAsSender()
		cb()
		s := snapshot(d)
		s.Changes = cs
		repeats = append(repeats, s)
	}
	result["repeatedSuccessfulSends"] = repeats

	// Receiver suppresses matching source+incarnation only. Checksum mismatch
	// with no remaining deltas returns the complete membership with local source.
	d = fixtureDisseminator(Member{Address: "local", Status: Alive, Incarnation: 41, Labels: LabelMap{"zone": "west"}},
		Member{Address: "local", Status: Alive, Incarnation: 41, Labels: LabelMap{"zone": "west"}},
		Member{Address: "tomb", Status: Tombstone, Incarnation: 6},
		Member{Address: "other", Status: Alive, Incarnation: 8})
	d.RecordChange(Change{Source: "sender", SourceIncarnation: 9, Address: "drop", Incarnation: 1, Status: Alive})
	d.RecordChange(Change{Source: "sender", SourceIncarnation: 10, Address: "keep-incarnation", Incarnation: 2, Status: Alive})
	d.RecordChange(Change{Source: "other", SourceIncarnation: 9, Address: "keep-source", Incarnation: 3, Status: Alive})
	filtered, _ := d.IssueAsReceiver("sender", 9, 0)
	sort.Slice(filtered, func(i, j int) bool { return filtered[i].Address < filtered[j].Address })
	result["receiverSourceIncarnationFilter"] = map[string]interface{}{"changes": filtered, "state": snapshot(d)}
	d.ClearChanges()
	full, fullSync := d.IssueAsReceiver("sender", 9, 123)
	sort.Slice(full, func(i, j int) bool { return full[i].Address < full[j].Address })
	result["checksumMismatchFullSync"] = map[string]interface{}{"fullSync": fullSync, "changes": full, "state": snapshot(d)}

	// Exercise the original AdjustMaxPropagations against real memberlist counts.
	maxP := map[string]int{}
	for _, count := range []int{0, 1, 8, 9, 10, 98, 99, 100, 999} {
		local := Member{Address: "local", Status: Alive}
		members := []Member{local}
		for i := 0; i < count; i++ {
			members = append(members, Member{Address: fmtAddress(i), Status: Alive})
		}
		d = fixtureDisseminator(local, members...)
		d.AdjustMaxPropagations()
		maxP[fmt.Sprint(count)] = d.maxP
	}
	result["maxPByPingableCount"] = maxP

	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	data = append(data, '\n')
	if err = os.WriteFile(out, data, 0644); err != nil {
		t.Fatal(err)
	}
}

func fmtAddress(i int) string { return "peer-" + strconv.Itoa(i) }
