// Overlaid into the upstream scheduler package by run-scheduler-compat.py.
// Calls the real, unexported Go functions; no Go implementation is copied.
package scheduler

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/robfig/cron/v3"
	"github.com/uber-go/tally"
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

func TestRustSchedulerCatchUpFixtures(t *testing.T) {
	var cases []struct {
		Name    string
		Policy  int
		Minutes int
		Rounds  []struct {
			Attempted, Skipped, Watermark int
			Pending                       bool
			Override                      int
		}
	}
	data, err := os.ReadFile(os.Getenv("CADENCE_SCHEDULER_CATCHUP_FIXTURES"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &cases); err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 1, 15, 10, 0, 0, 0, time.UTC)
	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			sched := mustParseCron(t, "* * * * *")
			input := &SchedulerWorkflowInput{Spec: types.ScheduleSpec{CronExpression: "* * * * *"}, Policies: types.SchedulePolicies{CatchUpPolicy: types.ScheduleCatchUpPolicySkip}}
			state := &SchedulerWorkflowState{CreateTime: base, UnpauseCatchUpPolicy: types.ScheduleCatchUpPolicy(c.Policy)}
			for i, want := range c.Rounds {
				budget := 100
				pending := processMissedRunsAt(nil, testLogger, tally.NewTestScope("", nil), sched, input, state, catchUpWatermark(state), base.Add(time.Duration(c.Minutes)*time.Minute), &budget)
				if 100-budget != want.Attempted || state.SkippedRuns != int64(want.Skipped) || pending != want.Pending || int(state.UnpauseCatchUpPolicy) != want.Override || !state.LastProcessedTime.Equal(base.Add(time.Duration(want.Watermark)*time.Minute)) {
					t.Fatalf("round %d attempted=%d skipped=%d pending=%v override=%v watermark=%v; want %+v", i, 100-budget, state.SkippedRuns, pending, state.UnpauseCatchUpPolicy, state.LastProcessedTime, want)
				}
			}
		})
	}
}
