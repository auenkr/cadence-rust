// Overlaid into the upstream scheduler package by run-scheduler-compat.py.
// Calls the real, unexported Go functions; no Go implementation is copied.
package scheduler

import (
	"context"
	"encoding/json"
	"github.com/uber/cadence/client/frontend"
	"go.uber.org/mock/gomock"
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

func TestRustSchedulerBackfillFixtures(t *testing.T) {
	var cases []struct {
		Name    string
		Signals []struct {
			ID         string
			Start, End int
		}
		Accepted      []bool
		Queued, Total int
		Rounds        []struct{ Attempted, Pending, Completed, Start int }
	}
	data, err := os.ReadFile(os.Getenv("CADENCE_SCHEDULER_BACKFILL_FIXTURES"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &cases); err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 1, 15, 10, 0, 0, 0, time.UTC)
	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			state := &SchedulerWorkflowState{Paused: true}
			input := &SchedulerWorkflowInput{Spec: types.ScheduleSpec{CronExpression: "* * * * *"}}
			scope := tally.NewTestScope("", nil)
			for i, sig := range c.Signals {
				accepted := handleBackfill(testLogger, scope, BackfillSignal{BackfillID: sig.ID, StartTime: base.Add(time.Duration(sig.Start) * time.Minute), EndTime: base.Add(time.Duration(sig.End) * time.Minute)}, state)
				if accepted != c.Accepted[i] {
					t.Fatalf("signal %d accepted=%v want=%v", i, accepted, c.Accepted[i])
				}
			}
			sched := mustParseCron(t, "* * * * *")
			budget := 100
			processBackfills(nil, testLogger, scope, sched, input, state, &budget)
			if budget != 100 || len(state.PendingBackfills) != c.Queued || int(state.PendingBackfills[0].RunsTotal) != c.Total || state.PendingBackfills[0].RunsCompleted != 0 {
				t.Fatalf("paused queue=%+v budget=%d", state.PendingBackfills, budget)
			}
			state.Paused = false
			for i, want := range c.Rounds {
				budget := 100
				processBackfills(nil, testLogger, scope, sched, input, state, &budget)
				if 100-budget != want.Attempted || len(state.PendingBackfills) != want.Pending {
					t.Fatalf("round %d budget=%d queue=%+v", i, budget, state.PendingBackfills)
				}
				if want.Pending > 0 && (int(state.PendingBackfills[0].RunsCompleted) != want.Completed || !state.PendingBackfills[0].StartTime.Equal(base.Add(time.Duration(want.Start)*time.Minute))) {
					t.Fatalf("round %d progress=%+v want=%+v", i, state.PendingBackfills[0], want)
				}
			}
		})
	}
}

func TestRustSchedulerIdentityLimitsFixtures(t *testing.T) {
	var fixture struct {
		IDs []struct {
			Schedule    string
			Nanos       int64
			Trigger, ID string
		}
		Limits []struct {
			User        int32
			Buffer      int
			Concurrency int32
		}
	}
	data, err := os.ReadFile(os.Getenv("CADENCE_SCHEDULER_IDENTITY_FIXTURES"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	for _, c := range fixture.IDs {
		if got := generateRequestID(c.Schedule, c.Nanos, TriggerSource(c.Trigger)); got != c.ID {
			t.Fatalf("id got %s want %s", got, c.ID)
		}
	}
	for _, c := range fixture.Limits {
		buffer, _ := effectiveBufferLimit(c.User)
		if buffer != c.Buffer || effectiveConcurrencyLimit(c.User) != c.Concurrency {
			t.Fatalf("limits user=%d buffer=%d concurrency=%d", c.User, buffer, effectiveConcurrencyLimit(c.User))
		}
	}
}

func TestRustSchedulerOverlapFixtures(t *testing.T) {
	var cases []struct {
		Policy  int
		Message string
	}
	data, err := os.ReadFile(os.Getenv("CADENCE_SCHEDULER_OVERLAP_FIXTURES"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &cases); err != nil {
		t.Fatal(err)
	}
	for _, c := range cases {
		t.Run(c.Message, func(t *testing.T) {
			mock := frontend.NewMockClient(gomock.NewController(t))
			execution := &types.WorkflowExecution{WorkflowID: "workflow", RunID: "run"}
			var ok bool
			var err error
			if c.Policy == int(types.ScheduleOverlapPolicyCancelPrevious) {
				mock.EXPECT().RequestCancelWorkflowExecution(gomock.Any(), gomock.Eq(&types.RequestCancelWorkflowExecutionRequest{Domain: "d", WorkflowExecution: execution, Cause: c.Message})).Return(nil)
				ok, err = cancelWorkflow(context.Background(), mock, "d", &RunningWorkflowInfo{WorkflowID: "workflow", RunID: "run"})
			} else {
				mock.EXPECT().TerminateWorkflowExecution(gomock.Any(), gomock.Eq(&types.TerminateWorkflowExecutionRequest{Domain: "d", WorkflowExecution: execution, Reason: c.Message})).Return(nil)
				ok, err = terminateWorkflow(context.Background(), mock, "d", &RunningWorkflowInfo{WorkflowID: "workflow", RunID: "run"})
			}
			if !ok || err != nil {
				t.Fatalf("ok=%v err=%v", ok, err)
			}
		})
	}
}
