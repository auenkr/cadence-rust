// History swap driver: Go SDK against the Rust Frontend over TChannel. A worker runs for the whole test; the Python
// harness (tests/gates/run-history-swap.py) swaps the History implementation between commands. Commands arrive as files
// SWAP_CTL/<seq>-<cmd> (cmd = "start:B:4" | "sig:B:s1:0:4" | "await:B:wait-s2:0:4" | "result:B:0:4" | "dump:tag" | "quit"),
// results are written to SWAP_CTL/<seq>-<cmd>.done ("ok" or "ERR ...").
// Workflow: timer -> activity -> child -> wait s1 -> timer -> activity -> wait s2 -> complete.
// Env: FE_TCHANNEL, FE_DOMAIN, SWAP_CTL.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
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

func ChildWorkflow(ctx workflow.Context, in string) (string, error) {
	if err := workflow.Sleep(ctx, time.Second); err != nil {
		return "", err
	}
	ao := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{ScheduleToStartTimeout: time.Minute, StartToCloseTimeout: time.Minute})
	var out string
	err := workflow.ExecuteActivity(ao, EchoActivity, in).Get(ctx, &out)
	return out, err
}

func SwapWorkflow(ctx workflow.Context, id string) (string, error) {
	stage := "t1"
	if err := workflow.SetQueryHandler(ctx, "stage", func() (string, error) { return stage, nil }); err != nil {
		return "", err
	}
	ao := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{ScheduleToStartTimeout: time.Minute, StartToCloseTimeout: time.Minute})
	act := func() error { var o string; return workflow.ExecuteActivity(ao, EchoActivity, id).Get(ctx, &o) }
	if err := workflow.Sleep(ctx, 3*time.Second); err != nil {
		return "", err
	}
	if err := act(); err != nil {
		return "", err
	}
	stage = "child"
	cctx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{WorkflowID: id + "-child", ExecutionStartToCloseTimeout: 10 * time.Minute, TaskStartToCloseTimeout: 10 * time.Second})
	var cout string
	if err := workflow.ExecuteChildWorkflow(cctx, ChildWorkflow, id).Get(ctx, &cout); err != nil {
		return "", err
	}
	if cout != id {
		return "", fmt.Errorf("child result %q", cout)
	}
	var s string
	stage = "wait-s1"
	workflow.GetSignalChannel(ctx, "s1").Receive(ctx, &s)
	stage = "t2"
	if err := workflow.Sleep(ctx, 2*time.Second); err != nil {
		return "", err
	}
	if err := act(); err != nil {
		return "", err
	}
	stage = "wait-s2"
	workflow.GetSignalChannel(ctx, "s2").Receive(ctx, &s)
	stage = "done"
	return "done:" + id, nil
}

func retry(f func() error) error {
	var err error
	for end := time.Now().Add(180 * time.Second); time.Now().Before(end); time.Sleep(2 * time.Second) {
		if err = f(); err == nil {
			return nil
		}
	}
	return err
}

func main() {
	peer, domain, ctl := os.Getenv("FE_TCHANNEL"), os.Getenv("FE_DOMAIN"), os.Getenv("SWAP_CTL")
	tr, err := tchannel.NewTransport(tchannel.ServiceName("history-swap"))
	if err != nil {
		panic(err)
	}
	d := yarpc.NewDispatcher(yarpc.Config{Name: "history-swap", Outbounds: yarpc.Outbounds{"cadence-frontend": {Unary: tr.NewSingleOutbound(peer)}}})
	if err := d.Start(); err != nil {
		panic(err)
	}
	defer d.Stop()
	rpc := workflowserviceclient.New(d.ClientConfig("cadence-frontend"))
	tl := "swap-" + uuid.NewString()
	w := worker.New(rpc, domain, tl, worker.Options{DisableStickyExecution: true})
	w.RegisterWorkflow(SwapWorkflow)
	w.RegisterWorkflow(ChildWorkflow)
	w.RegisterActivity(EchoActivity)
	if err := w.Start(); err != nil {
		fmt.Println("worker start failed:", err)
		os.Exit(2)
	}
	defer w.Stop()
	c := client.NewClient(rpc, domain, nil)
	ctx := context.Background()
	wid := func(b string, i int) string { return fmt.Sprintf("%s-%s-%d", tl, b, i) }
	var all []string

	exec := func(cmd string) error {
		p := strings.Split(cmd, ":")
		num := func(i int) int { n, _ := strconv.Atoi(p[i]); return n }
		switch p[0] {
		case "start":
			for i := 0; i < num(2); i++ {
				id := wid(p[1], i)
				if err := retry(func() error {
					_, e := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: id, TaskList: tl, ExecutionStartToCloseTimeout: 20 * time.Minute, DecisionTaskStartToCloseTimeout: 10 * time.Second}, SwapWorkflow, id)
					return e
				}); err != nil {
					return fmt.Errorf("start %s: %v", id, err)
				}
				all = append(all, id, id+"-child")
			}
			return nil
		case "sig":
			for i := num(3); i < num(4); i++ {
				id := wid(p[1], i)
				if err := retry(func() error { return c.SignalWorkflow(ctx, id, "", p[2], "x") }); err != nil {
					return fmt.Errorf("signal %s %s: %v", id, p[2], err)
				}
			}
			return nil
		case "await":
			for i := num(3); i < num(4); i++ {
				id := wid(p[1], i)
				var last string
				if err := retry(func() error {
					resp, e := c.QueryWorkflow(ctx, id, "", "stage")
					if e != nil {
						return e
					}
					var s string
					if e = resp.Get(&s); e != nil {
						return e
					}
					last = s
					if s != p[2] {
						return fmt.Errorf("stage %q", s)
					}
					return nil
				}); err != nil {
					return fmt.Errorf("await %s %s (last %q): %v", id, p[2], last, err)
				}
			}
			return nil
		case "result":
			for i := num(2); i < num(3); i++ {
				id := wid(p[1], i)
				var out string
				if err := retry(func() error {
					gctx, cancel := context.WithTimeout(ctx, 60*time.Second)
					defer cancel()
					return c.GetWorkflow(gctx, id, "").Get(gctx, &out)
				}); err != nil {
					return fmt.Errorf("result %s: %v", id, err)
				}
				if out != "done:"+id {
					return fmt.Errorf("result %s: got %q", id, out)
				}
			}
			return nil
		case "dump":
			ids := append([]string(nil), all...)
			sort.Strings(ids)
			dump := map[string]json.RawMessage{}
			for _, id := range ids {
				var events []*shared.HistoryEvent
				if err := retry(func() error {
					events = nil
					it := c.GetWorkflowHistory(ctx, id, "", false, shared.HistoryEventFilterTypeAllEvent)
					for it.HasNext() {
						ev, e := it.Next()
						if e != nil {
							return e
						}
						events = append(events, ev)
					}
					return nil
				}); err != nil {
					return fmt.Errorf("dump %s: %v", id, err)
				}
				b, _ := json.Marshal(events)
				dump[id] = b
			}
			b, _ := json.MarshalIndent(dump, "", " ")
			return os.WriteFile(filepath.Join(ctl, "dump-"+p[1]+".json"), b, 0o644)
		}
		return fmt.Errorf("unknown command %q", cmd)
	}

	fmt.Println("READY")
	done := map[string]bool{}
	for {
		ents, _ := filepath.Glob(filepath.Join(ctl, "*"))
		sort.Strings(ents)
		for _, f := range ents {
			base := filepath.Base(f)
			if strings.HasSuffix(base, ".done") || strings.HasPrefix(base, "dump-") || done[base] {
				continue
			}
			done[base] = true
			cmd := base[strings.Index(base, "-")+1:]
			if cmd == "quit" {
				return
			}
			res := "ok"
			if err := exec(cmd); err != nil {
				res = "ERR " + err.Error()
			}
			fmt.Println("CMD", cmd, res)
			os.WriteFile(f+".done", []byte(res), 0o644)
		}
		time.Sleep(500 * time.Millisecond)
	}
}
