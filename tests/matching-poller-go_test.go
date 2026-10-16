package poller

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/uber/cadence/common/clock"
)

func TestRustMatchingPollerFixtures(t *testing.T) {
	steps := []map[string]interface{}{
		{"op": "snapshot"},
		{"op": "start", "id": "1", "identity": "worker", "rate": 10.0, "group": "a"},
		{"op": "advance", "nanos": 10},
		{"op": "start", "id": "2", "identity": "worker", "rate": 20.0, "group": "b"},
		{"op": "snapshot"},
		{"op": "end", "id": "1"},
		{"op": "snapshot"},
		{"op": "cancel", "id": "2"},
		{"op": "snapshot"},
		{"op": "end", "id": "2"},
		{"op": "end", "id": "missing"},
		{"op": "snapshot"},
		{"op": "advance", "nanos": 300000000000},
		{"op": "snapshot"},
		{"op": "advance", "nanos": 1},
		{"op": "snapshot"},
		{"op": "start", "id": "empty", "identity": "", "rate": 0.0, "group": "c"},
		{"op": "snapshot"},
		{"op": "end", "id": "empty"},
		{"op": "start", "id": "", "identity": "orphan", "rate": 5.0, "group": "c"},
		{"op": "snapshot"},
		{"op": "bulk", "count": 5001},
		{"op": "snapshot"},
	}
	clock := clock.NewMockedTimeSourceAt(time.Unix(0, 1000000000))
	m := NewPollerManager(nil, clock)
	snapshots := []map[string]interface{}{}
	cancelled := 0
	for _, step := range steps {
		str := func(key string) string { value, _ := step[key].(string); return value }
		switch str("op") {
		case "start":
			m.StartPoll(str("id"), func() { cancelled++ }, &Info{Identity: str("identity"), RatePerSecond: step["rate"].(float64), IsolationGroup: str("group")})
		case "end":
			m.EndPoll(str("id"))
		case "cancel":
			if !m.CancelPoll(str("id")) {
				t.Fatal("expected existing poll")
			}
		case "advance":
			clock.Advance(time.Duration(step["nanos"].(int)))
		case "bulk":
			for i := 0; i < step["count"].(int); i++ {
				m.StartPoll("", context.CancelFunc(func() {}), &Info{Identity: fmt.Sprintf("bulk-%d", i), RatePerSecond: float64(i), IsolationGroup: "bulk"})
			}
		case "snapshot":
			count := m.GetCount()
			info := m.ListInfo()
			first, last := "", ""
			if len(info) > 0 {
				first = info[0].Identity
				last = info[len(info)-1].Identity
			}
			newest := int64(0)
			rate := 0.0
			if len(info) > 0 {
				newest = info[0].GetLastAccessTime()
				rate = info[0].RatePerSecond
			}
			after := clock.Now().Add(time.Nanosecond)
			snapshots = append(snapshots, map[string]interface{}{"count_before": count, "count_after": m.GetCount(), "listed": len(info), "first": first, "last": last, "newest": newest, "rate": rate, "cancelled": cancelled, "has_after": m.HasPollerAfter(after), "groups": m.GetCountByIsolationGroup(after), "a": m.HasPollerFromIsolationGroupAfter("a", after), "b": m.HasPollerFromIsolationGroupAfter("b", after), "c": m.HasPollerFromIsolationGroupAfter("c", after)})
		}
	}
	data, err := json.MarshalIndent(map[string]interface{}{"steps": steps, "snapshots": snapshots}, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(os.Getenv("CADENCE_POLLER_FIXTURES"), append(data, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
}
