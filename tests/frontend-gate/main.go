// Frontend mixed-gate SDK driver: the existing Go SDK talks to the Rust Frontend over TChannel.
// Steps are reported as "STEP <name> ok|FAIL ..." so the harness can summarize each Rust gap.
// Env: FE_TCHANNEL (host:port), FE_DOMAIN (already registered), FE_MARK_DIR (restart handshake dir).
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	"go.uber.org/cadence/.gen/go/cadence/workflowserviceclient"
	"go.uber.org/cadence/.gen/go/shared"
	"go.uber.org/cadence/client"
	"go.uber.org/cadence/worker"
	"go.uber.org/cadence/workflow"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/transport/tchannel"
)

func EchoActivity(_ context.Context, in string) (string, error) { return in, nil }
func EchoWorkflow(ctx workflow.Context, in string) (string, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{ScheduleToStartTimeout: time.Minute, StartToCloseTimeout: time.Minute})
	var out string
	err := workflow.ExecuteActivity(ctx, EchoActivity, in).Get(ctx, &out)
	return out, err
}

// SignalQueryWorkflow waits for "wake" signals, exposes its last value through query "state", completes on "done".
func SignalQueryWorkflow(ctx workflow.Context) (string, error) {
	state := "init"
	if err := workflow.SetQueryHandler(ctx, "state", func() (string, error) { return state, nil }); err != nil {
		return "", err
	}
	wake := workflow.GetSignalChannel(ctx, "wake")
	done := workflow.GetSignalChannel(ctx, "done")
	for {
		sel := workflow.NewSelector(ctx)
		finished := false
		sel.AddReceive(wake, func(c workflow.Channel, _ bool) { c.Receive(ctx, &state) })
		sel.AddReceive(done, func(c workflow.Channel, _ bool) { var s string; c.Receive(ctx, &s); finished = true })
		sel.Select(ctx)
		if finished {
			return "final:" + state, nil
		}
	}
}

func ChildWorkflow(ctx workflow.Context) (string, error) {
	ctx = workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{ExecutionStartToCloseTimeout: time.Minute})
	var out string
	err := workflow.ExecuteChildWorkflow(ctx, EchoWorkflow, "child").Get(ctx, &out)
	return out, err
}

func ScheduledWorkflow(ctx workflow.Context) (string, error) { return "scheduled", nil }

var failures int

func step(name string, err error) bool {
	if err != nil {
		failures++
		fmt.Printf("STEP %s FAIL %v\n", name, err)
		return false
	}
	fmt.Printf("STEP %s ok\n", name)
	return true
}

func waitFile(path string, d time.Duration) error {
	end := time.Now().Add(d)
	for time.Now().Before(end) {
		if _, err := os.Stat(path); err == nil {
			return nil
		}
		time.Sleep(time.Second)
	}
	return fmt.Errorf("timeout waiting for %s", path)
}

func main() {
	peer, domain, marks := os.Getenv("FE_TCHANNEL"), os.Getenv("FE_DOMAIN"), os.Getenv("FE_MARK_DIR")
	tr, err := tchannel.NewTransport(tchannel.ServiceName("frontend-gate"))
	if err != nil {
		panic(err)
	}
	d := yarpc.NewDispatcher(yarpc.Config{Name: "frontend-gate", Outbounds: yarpc.Outbounds{"cadence-frontend": {Unary: tr.NewSingleOutbound(peer)}}})
	if err := d.Start(); err != nil {
		panic(err)
	}
	defer d.Stop()
	rpc := workflowserviceclient.New(d.ClientConfig("cadence-frontend"))
	if os.Getenv("FE_MODE") == "es" {
		esMain(rpc, domain)
		os.Exit(map[bool]int{true: 1, false: 0}[failures > 0])
	}
	if os.Getenv("FE_MODE") == "async" {
		asyncMain(rpc, domain, os.Getenv("FE_DOMAIN_NOASYNC"), marks)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	tl := "fe-gate-" + uuid.NewString()

	_, err = rpc.DescribeDomain(ctx, &shared.DescribeDomainRequest{Name: &domain})
	step("describe-domain", err)

	w := worker.New(rpc, domain, tl, worker.Options{DisableStickyExecution: true})
	w.RegisterWorkflow(EchoWorkflow)
	w.RegisterWorkflow(ChildWorkflow)
	w.RegisterWorkflow(ScheduledWorkflow)
	w.RegisterActivity(EchoActivity)
	w.RegisterWorkflow(SignalQueryWorkflow)
	if !step("worker-start", w.Start()) {
		os.Exit(2)
	}
	defer w.Stop()
	c := client.NewClient(rpc, domain, nil)
	opts := func(id string) client.StartWorkflowOptions {
		return client.StartWorkflowOptions{ID: id, TaskList: tl, ExecutionStartToCloseTimeout: 5 * time.Minute, DecisionTaskStartToCloseTimeout: 10 * time.Second}
	}

	// 1. start + complete
	run, err := c.ExecuteWorkflow(ctx, opts(tl+"-echo"), EchoWorkflow, "hello")
	if step("start-echo", err) {
		var out string
		err = run.Get(ctx, &out)
		if err == nil && out != "hello" {
			err = fmt.Errorf("result %q", out)
		}
		step("complete-echo", err)
		desc, derr := c.DescribeWorkflowExecution(ctx, run.GetID(), run.GetRunID())
		if derr == nil && desc.WorkflowExecutionInfo.GetCloseStatus() != shared.WorkflowExecutionCloseStatusCompleted {
			derr = fmt.Errorf("close status %v", desc.WorkflowExecutionInfo.GetCloseStatus())
		}
		step("describe-closed", derr)
	}

	child, err := c.ExecuteWorkflow(ctx, opts(tl+"-child"), ChildWorkflow)
	if step("start-child", err) {
		var out string
		err = child.Get(ctx, &out)
		if err == nil && out != "child" { err = fmt.Errorf("child result %q", out) }
		step("complete-child", err)
	}
	_, err = rpc.DescribeTaskList(ctx, &shared.DescribeTaskListRequest{Domain: &domain, TaskList: &shared.TaskList{Name: &tl}, TaskListType: ptr(shared.TaskListTypeDecision)})
	step("describe-task-list", err)
	_, err = rpc.ListTaskListPartitions(ctx, &shared.ListTaskListPartitionsRequest{Domain: &domain, TaskList: &shared.TaskList{Name: &tl}})
	step("list-task-list-partitions", err)
	_, err = rpc.GetTaskListsByDomain(ctx, &shared.GetTaskListsByDomainRequest{DomainName: &domain})
	step("get-task-lists-by-domain", err)
	for _, action := range []string{"cancel", "terminate"} {
		r, e := c.ExecuteWorkflow(ctx, opts(tl+"-"+action), SignalQueryWorkflow)
		if !step("start-"+action, e) { continue }
		execution := &shared.WorkflowExecution{WorkflowId: ptr(r.GetID()), RunId: ptr(r.GetRunID())}
		if action == "cancel" {
			step("refresh-workflow-tasks", rpc.RefreshWorkflowTasks(ctx, &shared.RefreshWorkflowTasksRequest{Domain: &domain, Execution: execution}))
			_, e = rpc.ResetStickyTaskList(ctx, &shared.ResetStickyTaskListRequest{Domain: &domain, Execution: execution})
			step("reset-sticky-task-list", e)
			e = rpc.RequestCancelWorkflowExecution(ctx, &shared.RequestCancelWorkflowExecutionRequest{Domain: &domain, WorkflowExecution: execution, RequestId: ptr(uuid.NewString())})
		} else {
			e = rpc.TerminateWorkflowExecution(ctx, &shared.TerminateWorkflowExecutionRequest{Domain: &domain, WorkflowExecution: execution, Reason: ptr("mixed gate")})
		}
		step(action+"-workflow-execution", e)
	}
	// A dedicated worker remains alive while the real Go scheduler dispatches both backfilled runs.
	schedule := tl+"-schedule"
	cli := func(args ...string) error {
		base := []string{"--address", os.Getenv("FE_GRPC"), "--transport", "grpc", "--domain", domain, "schedule"}
		out, e := exec.CommandContext(ctx, os.Getenv("FE_CLI"), append(base, args...)...).CombinedOutput()
		if e != nil { return fmt.Errorf("%v: %s", e, out) }
		fmt.Printf("SDK schedule %s\n", out)
		return nil
	}
	if step("sched-create-grpc", cli("create", "--sid", schedule, "--ce", "0 0 1 1 *", "--wt", "main.ScheduledWorkflow", "--tl", tl, "--workflow_id_prefix", schedule)) {
		step("sched-backfill-grpc", cli("backfill", "--sid", schedule, "--st", "2024-01-01T00:00:00Z", "--endt", "2025-01-01T00:00:00Z"))
		backfillCtx, stopBackfill := context.WithTimeout(ctx, 3*time.Minute)
		e := retryFor(backfillCtx, func() error {
			resp, e := c.ListClosedWorkflow(backfillCtx, &shared.ListClosedWorkflowExecutionsRequest{Domain: &domain, MaximumPageSize: ptr(int32(100)), StartTimeFilter: &shared.StartTimeFilter{EarliestTime: ptr(int64(1)), LatestTime: ptr(time.Now().Add(time.Hour).UnixNano())}})
			if e != nil { return e }
			count := 0
			for _, x := range resp.Executions {
				if strings.HasPrefix(x.Execution.GetWorkflowId(), schedule) {
					desc, e := c.DescribeWorkflowExecution(backfillCtx, x.Execution.GetWorkflowId(), x.Execution.GetRunId())
					if e != nil { return e }
					if desc.WorkflowExecutionInfo.GetCloseStatus() != shared.WorkflowExecutionCloseStatusCompleted { return fmt.Errorf("backfill run not completed: %v", desc) }
					count++
				}
			}
			if count < 2 { return fmt.Errorf("only %d completed backfill runs, want 2", count) }
			return nil
		})
		stopBackfill()
		step("sched-backfill-completed", e)
		step("sched-describe-after-backfill-grpc", cli("describe", "--sid", schedule))
		step("sched-delete-grpc", cli("delete", "--sid", schedule))
	}

	// 2. signal + query + describe(open) + list
	sq, err := c.ExecuteWorkflow(ctx, opts(tl+"-sq"), SignalQueryWorkflow)
	if step("start-signal-query", err) {
		step("signal", c.SignalWorkflow(ctx, sq.GetID(), sq.GetRunID(), "wake", "one"))
		var st string
		var qerr error
		for i := 0; i < 20; i++ {
			qr, e := c.QueryWorkflow(ctx, sq.GetID(), sq.GetRunID(), "state")
			qerr = e
			if qerr == nil && qr == nil {
				qerr = errors.New("QueryWorkflow returned nil result and nil error")
			} else if qerr == nil {
				qerr = qr.Get(&st)
				if qerr == nil && st == "one" {
					break
				}
			}
			time.Sleep(time.Second)
		}
		if qerr == nil && st != "one" {
			qerr = fmt.Errorf("query state %q", st)
		}
		step("query", qerr)
		desc, derr := c.DescribeWorkflowExecution(ctx, sq.GetID(), sq.GetRunID())
		if derr == nil && desc.WorkflowExecutionInfo.CloseStatus != nil {
			derr = errors.New("open workflow reports closed")
		}
		step("describe-open", derr)
		var listErr error
		found := false
		for i := 0; i < 30 && !found; i++ {
			resp, e := c.ListOpenWorkflow(ctx, &shared.ListOpenWorkflowExecutionsRequest{Domain: &domain, MaximumPageSize: ptr(int32(100)), StartTimeFilter: &shared.StartTimeFilter{EarliestTime: ptr(int64(1)), LatestTime: ptr(time.Now().Add(time.Hour).UnixNano())}})
			listErr = e
			if e == nil {
				for _, x := range resp.Executions {
					if x.Execution.GetWorkflowId() == sq.GetID() {
						found = true
					}
				}
			}
			if !found {
				time.Sleep(time.Second)
			}
		}
		if listErr == nil && !found {
			listErr = errors.New("open execution not listed within 30s")
		}
		step("list-open", listErr)
		if marks != "" && os.Getenv("FE_CHURN") == "1" {
			churnPhase(ctx, c, opts, marks)
		}
		// 3. restart handshake: harness kills/restarts Rust Frontend while this workflow is open
		if marks != "" {
			_ = os.WriteFile(filepath.Join(marks, "restart-request"), []byte("x"), 0o644)
			step("restart-handshake", waitFile(filepath.Join(marks, "restart-done"), 5*time.Minute))
		}
		step("signal-after-restart", retry(func() error { return c.SignalWorkflow(ctx, sq.GetID(), sq.GetRunID(), "wake", "two") }))
		step("signal-done", retry(func() error { return c.SignalWorkflow(ctx, sq.GetID(), sq.GetRunID(), "done", "") }))
		var out string
		err = sq.Get(ctx, &out)
		if err == nil && out != "final:two" {
			err = fmt.Errorf("result %q", out)
		}
		step("complete-after-restart", err)
	}
	// 4. list closed
	var closedErr error
	found := false
	for i := 0; i < 30 && !found; i++ {
		resp, e := c.ListClosedWorkflow(ctx, &shared.ListClosedWorkflowExecutionsRequest{Domain: &domain, MaximumPageSize: ptr(int32(100)), StartTimeFilter: &shared.StartTimeFilter{EarliestTime: ptr(int64(1)), LatestTime: ptr(time.Now().Add(time.Hour).UnixNano())}})
		closedErr = e
		if e == nil && len(resp.Executions) >= 2 {
			found = true
		}
		if !found {
			time.Sleep(time.Second)
		}
	}
	if closedErr == nil && !found {
		closedErr = errors.New("closed executions not listed within 30s")
	}
	step("list-closed", closedErr)
	fmt.Printf("RESULT failures=%d\n", failures)
	if failures > 0 {
		os.Exit(1)
	}
}

func ptr[T any](v T) *T { return &v }

func retry(f func() error) error {
	var err error
	for i := 0; i < 60; i++ {
		if err = f(); err == nil {
			return nil
		}
		time.Sleep(time.Second)
	}
	return err
}

func retryFor(ctx context.Context, f func() error) error {
	for {
		err := f()
		if err == nil { return nil }
		select {
		case <-ctx.Done(): return fmt.Errorf("%v: %w", err, ctx.Err())
		case <-time.After(2*time.Second):
		}
	}
}

// churnPhase keeps starting and completing workflows (spread over all shards) while the harness
// kills and restarts the second Go History host.
func churnPhase(ctx context.Context, c client.Client, opts func(string) client.StartWorkflowOptions, marks string) {
	_ = os.WriteFile(filepath.Join(marks, "history-churn-request"), []byte("x"), 0o644)
	stop := make(chan struct{})
	errs := make(chan error, 8)
	counts := make(chan int, 8)
	for w := 0; w < 4; w++ {
		go func(w int) {
			n := 0
			for {
				select {
				case <-stop:
					counts <- n
					errs <- nil
					return
				default:
				}
				id := fmt.Sprintf("churn-%d-%s", w, uuid.NewString())
				var e error
				for try := 0; try < 30; try++ {
					var run client.WorkflowRun
					run, e = c.ExecuteWorkflow(ctx, opts(id), EchoWorkflow, id)
					if e == nil {
						var out string
						if e = run.Get(ctx, &out); e == nil && out != id {
							e = fmt.Errorf("result %q", out)
						}
					}
					if e == nil {
						break
					}
					time.Sleep(time.Second)
				}
				if e != nil {
					counts <- n
					errs <- e
					return
				}
				n++
			}
		}(w)
	}
	step("history-churn-handshake", waitFile(filepath.Join(marks, "history-churn-done"), 8*time.Minute))
	close(stop)
	var first error
	total := 0
	for w := 0; w < 4; w++ {
		total += <-counts
		if e := <-errs; e != nil && first == nil {
			first = e
		}
	}
	if first == nil && total < 8 {
		first = fmt.Errorf("only %d workflows completed during churn", total)
	}
	step("history-churn-workflows", first)
	fmt.Printf("SDK churn completed %d workflows\n", total)
}
