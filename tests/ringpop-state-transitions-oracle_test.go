package swim

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/uber/ringpop-go/events"
)

type transitionEventRecorder struct{ statuses []string }

func (r *transitionEventRecorder) HandleEvent(event events.Event) {
	if status, ok := event.(MakeNodeStatusEvent); ok {
		r.statuses = append(r.statuses, status.Status)
	}
}

func TestGenerateCadenceStateTransitionOracle(t *testing.T) {
	path := os.Getenv("CADENCE_RINGPOP_TRANSITIONS_ORACLE")
	if path == "" {
		t.Fatal("CADENCE_RINGPOP_TRANSITIONS_ORACLE required")
	}
	mock := clock.NewMock()
	n := NewNode("test", "local", nil, &Options{Clock: mock, StateTimeouts: StateTimeouts{Faulty: 10 * time.Second}})
	defer n.Destroy()
	n.memberlist.MakeAlive("local", 1)
	n.memberlist.MakeAlive("peer", 7)
	localBefore := len(n.stateTransitions.timers)
	n.stateTransitions.ScheduleSuspectToFaulty(Change{Address: "local", Incarnation: 1})
	localIgnored := len(n.stateTransitions.timers) == localBefore
	n.stateTransitions.ScheduleSuspectToFaulty(Change{Address: "peer", Incarnation: 7})
	first := n.stateTransitions.timer("peer")
	firstScheduled := first != nil
	n.stateTransitions.ScheduleSuspectToFaulty(Change{Address: "peer", Incarnation: 8})
	sameStateIgnored := first == n.stateTransitions.timer("peer")
	retainedState := n.stateTransitions.timers["peer"].state
	n.memberlist.members.Lock()
	n.memberlist.members.byAddress["peer"].Incarnation = 8
	n.memberlist.members.Unlock()
	mock.Add(5 * time.Second)
	member, _ := n.memberlist.Member("peer")
	statusAfterFire := member.Status
	firedRetained := n.stateTransitions.timer("peer") != nil
	n.memberlist.MakeAlive("same-inc", 20)
	n.stateTransitions.ScheduleSuspectToFaulty(Change{Address: "same-inc", Incarnation: 20})
	mock.Add(5 * time.Second)
	sameInc, _ := n.memberlist.Member("same-inc")
	sameIncFireStatus := sameInc.Status
	n.stateTransitions.ScheduleFaultyToTombstone(Change{Address: "peer", Incarnation: 8})
	replacementScheduled := n.stateTransitions.timers["peer"].state == Faulty
	cancelRemoved := false
	n.stateTransitions.Cancel(Change{Address: "peer"})
	cancelRemoved = n.stateTransitions.timer("peer") == nil
	n.stateTransitions.ScheduleTombstoneToEvict(Change{Address: "pending", Incarnation: 10})
	n.stateTransitions.Disable()
	disableCleared := len(n.stateTransitions.timers) == 0
	n.stateTransitions.Enable()
	reenabled := n.stateTransitions.enabled
	n.memberlist.MakeAlive("canceled", 30)
	n.stateTransitions.ScheduleSuspectToFaulty(Change{Address: "canceled", Incarnation: 30})
	n.stateTransitions.Cancel(Change{Address: "canceled"})
	mock.Add(5 * time.Second)
	canceledPeer, _ := n.memberlist.Member("canceled")
	cancelPreventedFire := canceledPeer.Status != Faulty
	n.memberlist.MakeAlive("disabled", 40)
	n.stateTransitions.ScheduleSuspectToFaulty(Change{Address: "disabled", Incarnation: 40})
	n.stateTransitions.Disable()
	mock.Add(5 * time.Second)
	disabledPeer, _ := n.memberlist.Member("disabled")
	disablePreventedFire := disabledPeer.Status != Faulty
	negativeClock := clock.NewMock()
	n2 := NewNode("test", "local2", nil, &Options{Clock: negativeClock, StateTimeouts: StateTimeouts{Suspect: -1}})
	defer n2.Destroy()
	n2.memberlist.MakeAlive("negative", 4)
	n2.stateTransitions.ScheduleSuspectToFaulty(Change{Address: "negative", Incarnation: 4})
	negativeClock.Add(0)
	negative, _ := n2.memberlist.Member("negative")
	negativeFired := 0
	if negative.Status == Faulty {
		negativeFired = 1
	}
	orderClock := clock.NewMock()
	n3 := NewNode("test", "local3", nil, &Options{Clock: orderClock, StateTimeouts: StateTimeouts{Faulty: 10 * time.Second}})
	defer n3.Destroy()
	n3.memberlist.MakeAlive("suspect-first", 1)
	n3.memberlist.MakeFaulty("faulty-second", 2)
	recorder := &transitionEventRecorder{}
	n3.AddListener(recorder)
	n3.stateTransitions.ScheduleSuspectToFaulty(Change{Address: "suspect-first", Incarnation: 1})
	n3.stateTransitions.ScheduleFaultyToTombstone(Change{Address: "faulty-second", Incarnation: 2})
	// Ten seconds fires deadlines at five and ten seconds. It does not reach
	// the new faulty-to-tombstone timer scheduled by the first callback at t=5.
	orderClock.Add(10 * time.Second)
	deadlineOrder := []string{}
	for _, status := range recorder.statuses {
		if status == Faulty {
			deadlineOrder = append(deadlineOrder, "suspect-first")
		}
		if status == Tombstone {
			deadlineOrder = append(deadlineOrder, "faulty-second")
		}
	}
	trace := map[string]interface{}{
		"defaults":     []int64{int64(defaultOptions().StateTimeouts.Suspect), int64(defaultOptions().StateTimeouts.Faulty), int64(defaultOptions().StateTimeouts.Tombstone)},
		"localIgnored": localIgnored, "firstScheduled": firstScheduled, "sameStateIgnored": sameStateIgnored,
		"goStatusAfterFire": statusAfterFire, "sameIncFireStatus": sameIncFireStatus,
		"firedAction":   map[string]string{Suspect: "MakeFaulty"}[retainedState],
		"firedRetained": firedRetained, "replacementScheduled": replacementScheduled,
		"cancelRemoved": cancelRemoved, "disableCleared": disableCleared,
		"cancelPreventedFire": cancelPreventedFire, "disablePreventedFire": disablePreventedFire,
		"reenabled": reenabled, "negativeFired": negativeFired,
		"deadlineOrder": deadlineOrder,
	}
	data, err := json.Marshal(trace)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}
}
