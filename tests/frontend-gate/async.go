// Async workflow acceptance (FE_MODE=async): StartWorkflowAsync / SignalWithStartWorkflowAsync through the
// Rust Frontend's Kafka producer, executed by the Go async consumer in the Go Worker service.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.uber.org/cadence/.gen/go/cadence/workflowserviceclient"
	"go.uber.org/cadence/.gen/go/shared"
	"go.uber.org/cadence/client"
	"go.uber.org/cadence/worker"
)

func asyncMain(rpc workflowserviceclient.Interface, domain, noAsyncDomain, marks string) {
	ctx, cancel := context.WithTimeout(context.Background(), 14*time.Minute)
	defer cancel()
	tl := "fe-gate-async-" + uuid.NewString()
	w := worker.New(rpc, domain, tl, worker.Options{DisableStickyExecution: true})
	w.RegisterWorkflow(EchoWorkflow)
	w.RegisterWorkflow(SignalQueryWorkflow)
	w.RegisterActivity(EchoActivity)
	if !step("async-worker-start", w.Start()) {
		os.Exit(2)
	}
	defer w.Stop()
	c := client.NewClient(rpc, domain, nil)
	opts := func(id string) client.StartWorkflowOptions {
		return client.StartWorkflowOptions{ID: id, TaskList: tl, ExecutionStartToCloseTimeout: 5 * time.Minute, DecisionTaskStartToCloseTimeout: 10 * time.Second}
	}
	// The consumer lags: the Go worker refreshes consumers every minute and Kafka rebalances, so poll long.
	waitResult := func(id string, want string) error {
		deadline := time.Now().Add(5 * time.Minute)
		var last error
		for time.Now().Before(deadline) {
			actx, acancel := context.WithTimeout(ctx, 40*time.Second)
			var out string
			last = c.GetWorkflow(actx, id, "").Get(actx, &out)
			acancel()
			if last == nil {
				if out != want {
					return fmt.Errorf("result %q want %q", out, want)
				}
				return nil
			}
			time.Sleep(2 * time.Second)
		}
		return fmt.Errorf("workflow %s did not complete: %v", id, last)
	}

	// 1. async start executes
	id := tl + "-start"
	if _, err := c.StartWorkflowAsync(ctx, opts(id), EchoWorkflow, "async-hello"); step("async-start-accepted", err) {
		step("async-start-executes", waitResult(id, "async-hello"))
	}

	// 2. async signal-with-start executes
	sid := tl + "-sws"
	if _, err := c.SignalWithStartWorkflowAsync(ctx, sid, "wake", "one", opts(sid), SignalQueryWorkflow); step("async-signal-with-start-accepted", err) {
		var qerr error
		for i := 0; i < 150; i++ {
			qr, e := c.QueryWorkflow(ctx, sid, "", "state")
			qerr = e
			if e == nil {
				var st string
				if qerr = qr.Get(&st); qerr == nil && st == "one" {
					break
				} else if qerr == nil {
					qerr = fmt.Errorf("state %q", st)
				}
			}
			time.Sleep(2 * time.Second)
		}
		step("async-signal-with-start-signal-delivered", qerr)
		step("async-signal-with-start-done-signal", retry(func() error { return c.SignalWorkflow(ctx, sid, "", "done", "") }))
		step("async-signal-with-start-executes", waitResult(sid, "final:one"))
	}

	// 3. invalid workflow request: validation error, not queued
	vdomain := domain
	_, err := rpc.StartWorkflowExecutionAsync(ctx, &shared.StartWorkflowExecutionAsyncRequest{Request: &shared.StartWorkflowExecutionRequest{
		Domain: &vdomain, WorkflowId: ptr(tl + "-invalid"), TaskList: &shared.TaskList{Name: &tl},
		ExecutionStartToCloseTimeoutSeconds: ptr(int32(60)), TaskStartToCloseTimeoutSeconds: ptr(int32(10)), RequestId: ptr(uuid.NewString()),
	}})
	var bad *shared.BadRequestError
	if err == nil {
		err = errors.New("missing workflow type was accepted")
	} else if !errors.As(err, &bad) {
		err = fmt.Errorf("want BadRequestError, got %T: %v", err, err)
	} else {
		err = nil
	}
	step("async-invalid-workflow-bad-request", err)

	// 4. domain without async config: BadRequest
	ndomain := noAsyncDomain
	wt := "main.EchoWorkflow"
	_, err = rpc.StartWorkflowExecutionAsync(ctx, &shared.StartWorkflowExecutionAsyncRequest{Request: &shared.StartWorkflowExecutionRequest{
		Domain: &ndomain, WorkflowId: ptr(tl + "-disabled"), WorkflowType: &shared.WorkflowType{Name: &wt}, TaskList: &shared.TaskList{Name: &tl},
		ExecutionStartToCloseTimeoutSeconds: ptr(int32(60)), TaskStartToCloseTimeoutSeconds: ptr(int32(10)), RequestId: ptr(uuid.NewString()),
	}})
	bad = nil
	if err == nil {
		err = errors.New("disabled domain accepted an async start")
	} else if !errors.As(err, &bad) || !strings.Contains(bad.Message, "not enabled") {
		err = fmt.Errorf("want BadRequestError 'not enabled', got %T: %v", err, err)
	} else {
		err = nil
	}
	step("async-disabled-domain-bad-request", err)

	// 5. broker down: must be an error, never success; 6. recovery after restart
	if marks != "" {
		_ = os.WriteFile(filepath.Join(marks, "broker-down-request"), []byte("x"), 0o644)
		step("broker-down-handshake", waitFile(filepath.Join(marks, "broker-down-done"), 3*time.Minute))
		downID := tl + "-brokerdown"
		dctx, dcancel := context.WithTimeout(ctx, 100*time.Second)
		_, derr := c.StartWorkflowAsync(dctx, opts(downID), EchoWorkflow, "down")
		dcancel()
		if derr == nil {
			derr = errors.New("async start returned success while the broker was down")
		} else {
			derr = nil
		}
		step("async-broker-down-returns-error", derr)
		_ = os.WriteFile(filepath.Join(marks, "broker-up-request"), []byte("x"), 0o644)
		step("broker-up-handshake", waitFile(filepath.Join(marks, "broker-up-done"), 5*time.Minute))
		upID := tl + "-recovered"
		rerr := retryFor(ctx, func() error {
			rctx, rcancel := context.WithTimeout(ctx, 60*time.Second)
			defer rcancel()
			_, e := c.StartWorkflowAsync(rctx, opts(upID), EchoWorkflow, "recovered")
			return e
		})
		if step("async-start-after-broker-restart-accepted", rerr) {
			step("async-start-after-broker-restart-executes", waitResult(upID, "recovered"))
		}
		_, e := c.DescribeWorkflowExecution(ctx, downID, "")
		var nf *shared.EntityNotExistsError
		if e == nil {
			e = errors.New("workflow whose async start errored was executed")
		} else if errors.As(e, &nf) {
			e = nil
		}
		step("async-failed-start-not-executed", e)
	}
	fmt.Printf("RESULT failures=%d\n", failures)
	if failures > 0 {
		os.Exit(1)
	}
}
