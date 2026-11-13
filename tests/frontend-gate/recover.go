// History failure/restart/recovery acceptance (FE_MODE=recover). The Go SDK keeps in-flight timers, activities,
// activity timeouts, signals and a started-but-unanswered decision across a harness-driven failure, then every
// workflow must still complete after the recovery. Rounds (FE_RECOVER_ROUNDS, comma separated):
//   killall      SIGKILL every Rust History host, restart them (two_history: both hosts)
//   dbrestart    docker restart of the persistence container while History keeps running
//   killprimary  SIGKILL only the first History host; the second one takes over its shards, then it is restarted
// Handshake per round: driver writes <round>-request in FE_MARK_DIR, harness acts, writes <round>-done.
// Env: FE_DOMAIN, FE_MARK_DIR, FE_RECOVER_ROUNDS.
package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.uber.org/cadence"
	"go.uber.org/cadence/.gen/go/cadence/workflowserviceclient"
	"go.uber.org/cadence/.gen/go/shared"
	"go.uber.org/cadence/client"
	"go.uber.org/cadence/worker"
	"go.uber.org/cadence/workflow"
)

var recoverDir string

func RecoverSleepActivity(ctx context.Context, tag string, secs int) (string, error) {
	_ = os.WriteFile(filepath.Join(recoverDir, "act-started-"+tag+"-sleep"), []byte("x"), 0o644)
	select {
	case <-time.After(time.Duration(secs) * time.Second):
		return "act:" + tag, nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

func RecoverStuckActivity(ctx context.Context, tag string) (string, error) {
	_ = os.WriteFile(filepath.Join(recoverDir, "act-started-"+tag+"-stuck"), []byte("x"), 0o644)
	select {
	case <-time.After(90 * time.Second):
		return "stuck-finished", nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

func RecoverTimerWorkflow(ctx workflow.Context, tag string) (string, error) {
	if err := workflow.Sleep(ctx, 30*time.Second); err != nil {
		return "", err
	}
	return "timer:" + tag, nil
}

func RecoverActivityWorkflow(ctx workflow.Context, tag string) (string, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		ScheduleToStartTimeout: 2 * time.Minute, StartToCloseTimeout: 40 * time.Second,
		RetryPolicy: &cadence.RetryPolicy{InitialInterval: time.Second, BackoffCoefficient: 1, MaximumAttempts: 5, ExpirationInterval: 5 * time.Minute},
	})
	var out string
	err := workflow.ExecuteActivity(ctx, RecoverSleepActivity, tag, 20).Get(ctx, &out)
	return out, err
}

// RecoverTimeoutWorkflow: the activity start-to-close timer (persisted History timer task) must fire after recovery.
func RecoverTimeoutWorkflow(ctx workflow.Context, tag string) (string, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{ScheduleToStartTimeout: 2 * time.Minute, StartToCloseTimeout: 15 * time.Second})
	var out string
	err := workflow.ExecuteActivity(ctx, RecoverStuckActivity, tag).Get(ctx, &out)
	if err != nil && cadence.IsTimeoutError(err) {
		return "timeout:" + tag, nil
	}
	return out, err
}

func RecoverSignalWorkflow(ctx workflow.Context, tag string) (string, error) {
	ch := workflow.GetSignalChannel(ctx, "go")
	var a, b string
	ch.Receive(ctx, &a)
	ch.Receive(ctx, &b)
	return "signal:" + tag + ":" + a + "," + b, nil
}

func recoverMain(rpc workflowserviceclient.Interface, domain, marks string) {
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Minute)
	defer cancel()
	recoverDir = marks
	tl := "fe-gate-recover-" + uuid.NewString()
	w := worker.New(rpc, domain, tl, worker.Options{DisableStickyExecution: true})
	w.RegisterWorkflow(RecoverTimerWorkflow)
	w.RegisterWorkflow(RecoverActivityWorkflow)
	w.RegisterWorkflow(RecoverTimeoutWorkflow)
	w.RegisterWorkflow(RecoverSignalWorkflow)
	w.RegisterWorkflow(EchoWorkflow)
	w.RegisterActivity(EchoActivity)
	w.RegisterActivity(RecoverSleepActivity)
	w.RegisterActivity(RecoverStuckActivity)
	if !step("recover-worker-start", w.Start()) {
		return
	}
	defer w.Stop()
	c := client.NewClient(rpc, domain, nil)
	for _, round := range strings.Split(os.Getenv("FE_RECOVER_ROUNDS"), ",") {
		if round = strings.TrimSpace(round); round != "" {
			recoverRound(ctx, rpc, c, domain, tl, marks, round)
		}
	}
	fmt.Printf("RESULT failures=%d\n", failures)
}

func recoverRound(ctx context.Context, rpc workflowserviceclient.Interface, c client.Client, domain, tl, marks, round string) {
	tag := round + "-" + uuid.NewString()[:8]
	opts := func(id string) client.StartWorkflowOptions {
		return client.StartWorkflowOptions{ID: id, TaskList: tl, ExecutionStartToCloseTimeout: 10 * time.Minute, DecisionTaskStartToCloseTimeout: 10 * time.Second}
	}
	type item struct {
		name, want string
		run        client.WorkflowRun
	}
	var items []item
	startOne := func(name string, fn interface{}, want string) {
		id := "recover-" + name + "-" + tag
		run, err := c.ExecuteWorkflow(ctx, opts(id), fn, tag)
		if step("recover-"+round+"-start-"+name, err) {
			items = append(items, item{name, want, run})
		}
	}
	startOne("timer", RecoverTimerWorkflow, "timer:"+tag)
	startOne("activity", RecoverActivityWorkflow, "act:"+tag)
	startOne("activity-timeout", RecoverTimeoutWorkflow, "timeout:"+tag)
	startOne("signal", RecoverSignalWorkflow, "signal:"+tag+":one,two")

	// started-but-unanswered decision on a task list without SDK worker
	rawTL := tl + "-raw-" + round
	rawID := "recover-raw-" + tag
	identity := "recover-gate"
	_, err := rpc.StartWorkflowExecution(ctx, &shared.StartWorkflowExecutionRequest{Domain: &domain, WorkflowId: &rawID,
		WorkflowType: &shared.WorkflowType{Name: ptr("raw-wf")}, TaskList: &shared.TaskList{Name: &rawTL},
		ExecutionStartToCloseTimeoutSeconds: ptr(int32(600)), TaskStartToCloseTimeoutSeconds: ptr(int32(10)), RequestId: ptr(uuid.NewString())})
	rawOK := step("recover-"+round+"-raw-start", err)
	if rawOK {
		var polled bool
		for i := 0; i < 5 && !polled; i++ {
			pc, pcancel := context.WithTimeout(ctx, 70*time.Second)
			r, e := rpc.PollForDecisionTask(pc, &shared.PollForDecisionTaskRequest{Domain: &domain, TaskList: &shared.TaskList{Name: &rawTL}, Identity: &identity})
			pcancel()
			polled = e == nil && len(r.TaskToken) > 0
		}
		rawOK = step("recover-"+round+"-raw-poll-decision", errIf(!polled, "no decision task"))
	}

	// both activities must be running before the failure
	step("recover-"+round+"-activities-running", scenPoll(ctx, 90*time.Second, func() error {
		for _, n := range []string{"act-started-" + tag + "-sleep", "act-started-" + tag + "-stuck"} {
			if _, e := os.Stat(filepath.Join(marks, n)); e != nil {
				return e
			}
		}
		return nil
	}))
	for _, it := range items {
		if it.name == "signal" {
			step("recover-"+round+"-signal-one", retry(func() error { return c.SignalWorkflow(ctx, "recover-signal-"+tag, it.run.GetRunID(), "go", "one") }))
		}
	}

	_ = os.WriteFile(filepath.Join(marks, round+"-request"), []byte("x"), 0o644)
	// a workflow started while the failure is in progress must also complete
	var wg sync.WaitGroup
	var duringErr error
	wg.Add(1)
	go func() {
		defer wg.Done()
		id := "recover-during-" + tag
		dctx, dcancel := context.WithTimeout(ctx, 6*time.Minute)
		defer dcancel()
		duringErr = retryFor(dctx, func() error {
			run, e := c.ExecuteWorkflow(ctx, opts(id), EchoWorkflow, id)
			if e != nil {
				if strings.Contains(e.Error(), "already started") {
					run = c.GetWorkflow(ctx, id, "")
				} else {
					return e
				}
			}
			var out string
			if e = run.Get(ctx, &out); e != nil {
				return e
			}
			if out != id {
				return fmt.Errorf("result %q", out)
			}
			return nil
		})
	}()
	step("recover-"+round+"-handshake", waitFile(filepath.Join(marks, round+"-done"), 12*time.Minute))
	wg.Wait()
	step("recover-"+round+"-workflow-started-during-failure", duringErr)

	for _, it := range items {
		if it.name == "signal" {
			step("recover-"+round+"-signal-two", retry(func() error { return c.SignalWorkflow(ctx, "recover-signal-"+tag, it.run.GetRunID(), "go", "two") }))
		}
	}
	for _, it := range items {
		var out string
		gctx, gcancel := context.WithTimeout(ctx, 6*time.Minute)
		err := it.run.Get(gctx, &out)
		gcancel()
		if err == nil && out != it.want {
			err = fmt.Errorf("result %q want %q", out, it.want)
		}
		step("recover-"+round+"-complete-"+it.name, err)
	}
	if rawOK {
		// the in-flight decision must time out after recovery (persisted timer task), then a new one completes the run
		step("recover-"+round+"-raw-decision-timed-out", scenPoll(ctx, 4*time.Minute, func() error {
			ev, e := scenHistory(ctx, rpc, domain, rawID, "")
			if e != nil {
				return e
			}
			for _, x := range ev {
				if x.GetEventType() == shared.EventTypeDecisionTaskTimedOut {
					return nil
				}
			}
			return fmt.Errorf("no DecisionTaskTimedOut in %d events", len(ev))
		}))
		var next *shared.PollForDecisionTaskResponse
		step("recover-"+round+"-raw-poll-new-decision", scenPoll(ctx, 4*time.Minute, func() error {
			pc, pcancel := context.WithTimeout(ctx, 70*time.Second)
			defer pcancel()
			r, e := rpc.PollForDecisionTask(pc, &shared.PollForDecisionTaskRequest{Domain: &domain, TaskList: &shared.TaskList{Name: &rawTL}, Identity: &identity})
			if e != nil {
				return e
			}
			if len(r.TaskToken) == 0 {
				return fmt.Errorf("empty poll")
			}
			next = r
			return nil
		}))
		if next != nil {
			_, e := rpc.RespondDecisionTaskCompleted(ctx, &shared.RespondDecisionTaskCompletedRequest{TaskToken: next.TaskToken, Identity: &identity,
				Decisions: []*shared.Decision{{DecisionType: shared.DecisionTypeCompleteWorkflowExecution.Ptr(),
					CompleteWorkflowExecutionDecisionAttributes: &shared.CompleteWorkflowExecutionDecisionAttributes{Result: []byte(`"raw-done"`)}}}})
			step("recover-"+round+"-raw-respond-completed", e)
			step("recover-"+round+"-raw-closed-completed", scenPoll(ctx, time.Minute, func() error {
				ev, e := scenHistory(ctx, rpc, domain, rawID, "")
				if e != nil {
					return e
				}
				if n := len(ev); n == 0 || ev[n-1].GetEventType() != shared.EventTypeWorkflowExecutionCompleted {
					return fmt.Errorf("workflow not completed")
				}
				return nil
			}))
		}
	}
}
