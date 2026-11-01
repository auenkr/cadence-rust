// Frontend mixed-gate SDK driver: the existing Go SDK talks to the Rust Frontend over TChannel.
// Steps are reported as "STEP <name> ok|FAIL ..." so the harness can summarize each Rust gap.
// Env: FE_TCHANNEL (host:port), FE_DOMAIN (already registered), FE_MARK_DIR (restart handshake dir).
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
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
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	tl := "fe-gate-" + uuid.NewString()

	_, err = rpc.DescribeDomain(ctx, &shared.DescribeDomainRequest{Name: &domain})
	step("describe-domain", err)

	w := worker.New(rpc, domain, tl, worker.Options{DisableStickyExecution: true})
	w.RegisterWorkflow(EchoWorkflow)
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
