// History replication gate SDK driver: runs a workload (signals, activity, timer, child workflow, query)
// against ONE cluster's Frontend (TChannel). The Python harness calls it per phase:
//   REPL_CMD=phase1  start workflow A (runs to completion) and B (parks waiting for "finish")
//   REPL_CMD=phase2  after failover: signal B "finish" and wait for it, start+complete workflow C
// Env: FE_TCHANNEL, FE_DOMAIN, REPL_TL, REPL_WID_B, REPL_RID_B (phase2).
// Output: "WF <name> <wid> <rid>" lines and "STEP <name> ok|FAIL <err>" lines.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"
	"go.uber.org/cadence/.gen/go/cadence/workflowserviceclient"
	"go.uber.org/cadence/client"
	"go.uber.org/cadence/worker"
	"go.uber.org/cadence/workflow"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/transport/tchannel"
)

func EchoActivity(_ context.Context, in string) (string, error) { return in, nil }

func ReplChild(ctx workflow.Context, in string) (string, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{ScheduleToStartTimeout: time.Minute, StartToCloseTimeout: time.Minute})
	var out string
	err := workflow.ExecuteActivity(ctx, EchoActivity, "child-"+in).Get(ctx, &out)
	return out, err
}

// ReplWorkflow: signal s1 -> activity -> timer -> child workflow -> (park if parkForFinish) -> signal finish -> timer -> done.
func ReplWorkflow(ctx workflow.Context, parkForFinish bool) (string, error) {
	stage := "init"
	if err := workflow.SetQueryHandler(ctx, "stage", func() (string, error) { return stage, nil }); err != nil {
		return "", err
	}
	var s1 string
	workflow.GetSignalChannel(ctx, "s1").Receive(ctx, &s1)
	stage = "s1"
	actCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{ScheduleToStartTimeout: time.Minute, StartToCloseTimeout: time.Minute})
	var act string
	if err := workflow.ExecuteActivity(actCtx, EchoActivity, s1).Get(ctx, &act); err != nil {
		return "", err
	}
	if err := workflow.NewTimer(ctx, 2*time.Second).Get(ctx, nil); err != nil {
		return "", err
	}
	stage = "timer"
	childCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{ExecutionStartToCloseTimeout: time.Minute})
	var child string
	if err := workflow.ExecuteChildWorkflow(childCtx, ReplChild, s1).Get(ctx, &child); err != nil {
		return "", err
	}
	stage = "child-done"
	if parkForFinish {
		stage = "waiting-finish"
	}
	var fin string
	workflow.GetSignalChannel(ctx, "finish").Receive(ctx, &fin)
	if err := workflow.NewTimer(ctx, 2*time.Second).Get(ctx, nil); err != nil {
		return "", err
	}
	stage = "done"
	return act + "|" + child + "|" + fin, nil
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

func main() {
	peer, domain, tl, cmd := os.Getenv("FE_TCHANNEL"), os.Getenv("FE_DOMAIN"), os.Getenv("REPL_TL"), os.Getenv("REPL_CMD")
	tr, err := tchannel.NewTransport(tchannel.ServiceName("history-repl-driver"))
	if err != nil {
		panic(err)
	}
	d := yarpc.NewDispatcher(yarpc.Config{Name: "history-repl-driver", Outbounds: yarpc.Outbounds{"cadence-frontend": {Unary: tr.NewSingleOutbound(peer)}}})
	if err := d.Start(); err != nil {
		panic(err)
	}
	defer d.Stop()
	rpc := workflowserviceclient.New(d.ClientConfig("cadence-frontend"))
	w := worker.New(rpc, domain, tl, worker.Options{DisableStickyExecution: true})
	w.RegisterWorkflow(ReplWorkflow)
	w.RegisterWorkflow(ReplChild)
	w.RegisterActivity(EchoActivity)
	if !step("worker-start", w.Start()) {
		os.Exit(2)
	}
	defer w.Stop()
	c := client.NewClient(rpc, domain, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	opts := func(id string) client.StartWorkflowOptions {
		return client.StartWorkflowOptions{ID: id, TaskList: tl, ExecutionStartToCloseTimeout: 10 * time.Minute, DecisionTaskStartToCloseTimeout: 10 * time.Second}
	}
	retry := func(f func() error) error {
		var err error
		for i := 0; i < 20; i++ {
			if err = f(); err == nil {
				return nil
			}
			time.Sleep(3 * time.Second)
		}
		return err
	}
	runFull := func(name string, parkFlag bool) {
		id := "repl-" + name + "-" + uuid.NewString()[:8]
		run, err := c.ExecuteWorkflow(ctx, opts(id), ReplWorkflow, parkFlag)
		if !step(name+"-start", err) {
			return
		}
		fmt.Printf("WF %s %s %s\n", name, run.GetID(), run.GetRunID())
		step(name+"-signal-s1", retry(func() error { return c.SignalWorkflow(ctx, run.GetID(), run.GetRunID(), "s1", "one") }))
		if parkFlag {
			ok := false
			last := "no query result"
			for i := 0; i < 90 && !ok; i++ {
				resp, qerr := c.QueryWorkflow(ctx, run.GetID(), run.GetRunID(), "stage")
				var st string
				if qerr != nil {
					last = "query error: " + qerr.Error()
				} else if gerr := resp.Get(&st); gerr != nil {
					last = "decode error: " + gerr.Error()
				} else if st != "waiting-finish" {
					last = "stage=" + st
				} else {
					ok = true
				}
				if !ok {
					time.Sleep(2 * time.Second)
				}
			}
			var perr error
			if !ok {
				perr = fmt.Errorf("never reached waiting-finish: %s", last)
			}
			step(name+"-parked-waiting-finish", perr)
			return
		}
		step(name+"-signal-finish", retry(func() error { return c.SignalWorkflow(ctx, run.GetID(), run.GetRunID(), "finish", "fin") }))
		var out string
		step(name+"-completed", run.Get(ctx, &out))
	}
	switch cmd {
	case "phase1":
		runFull("A", false)
		runFull("B", true)
	case "phase2":
		wid, rid := os.Getenv("REPL_WID_B"), os.Getenv("REPL_RID_B")
		step("B-signal-finish-after-failover", retry(func() error { return c.SignalWorkflow(ctx, wid, rid, "finish", "fin") }))
		var out string
		step("B-completed-after-failover", c.GetWorkflow(ctx, wid, rid).Get(ctx, &out))
		runFull("C", false)
	default:
		step("unknown-cmd", fmt.Errorf("REPL_CMD=%q", cmd))
	}
	if failures > 0 {
		os.Exit(1)
	}
}
