// Overlaid into the upstream scheduler package by run-scheduler-compat.py.
// Calls the real, unexported Go functions; no Go implementation is copied.
package scheduler

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/robfig/cron/v3"
	"github.com/uber/cadence/common/types"
)

func TestRustSchedulerBoundaryFixtures(t *testing.T) {
	var cases []struct {
		Name, Cron, After, Until, Start, End string
		Next                                 *string
		Fires                                []string
	}
	data, err := os.ReadFile(os.Getenv("CADENCE_SCHEDULER_FIXTURES"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &cases); err != nil {
		t.Fatal(err)
	}
	parse := func(s string) time.Time {
		if s == "" {
			return time.Time{}
		}
		v, err := time.Parse(time.RFC3339Nano, s)
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			sched, err := cron.ParseStandard(c.Cron)
			if err != nil {
				t.Fatal(err)
			}
			spec := types.ScheduleSpec{StartTime: parse(c.Start), EndTime: parse(c.End)}
			next := computeNextRunTime(sched, parse(c.After), spec)
			want := time.Time{}
			if c.Next != nil {
				want = parse(*c.Next)
			}
			if !next.Equal(want) {
				t.Fatalf("next got %s want %s", next, want)
			}
			got := computeMissedFireTimes(sched, parse(c.After), parse(c.Until), spec)
			if got.truncated || len(got.times) != len(c.Fires) {
				t.Fatalf("fires got %v want %v", got, c.Fires)
			}
			for i, f := range c.Fires {
				if !got.times[i].Equal(parse(f)) {
					t.Fatalf("fire %d got %s want %s", i, got.times[i], f)
				}
			}
		})
	}
}
