// Recovery-timing SDK driver: starts one echo workflow every -interval against a Frontend that may be
// down/restarting, and logs one JSON line per workflow (first start attempt, completion, ok) with
// wall-clock nanoseconds so the harness can compute recovery times against its own kill/start events.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/google/uuid"
	"go.uber.org/cadence"
	"go.uber.org/cadence/.gen/go/cadence/workflowserviceclient"
	"go.uber.org/cadence/.gen/go/shared"
	"go.uber.org/cadence/client"
	"go.uber.org/cadence/worker"
	"go.uber.org/cadence/workflow"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/transport/tchannel"
)

func echo(_ context.Context, payload string) (string, error) { return payload, nil }
func flow(ctx workflow.Context, payload string) (string, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{ScheduleToStartTimeout: 20 * time.Second, ScheduleToCloseTimeout: 40 * time.Second, StartToCloseTimeout: 10 * time.Second,
		RetryPolicy: &cadence.RetryPolicy{InitialInterval: time.Second, BackoffCoefficient: 1, MaximumAttempts: 5}})
	var result string
	err := workflow.ExecuteActivity(ctx, echo, payload).Get(ctx, &result)
	return result, err
}

type rec struct {
	Seq        int    `json:"seq"`
	WorkflowID string `json:"wf"`
	StartNs    int64  `json:"start_ns"`
	EndNs      int64  `json:"end_ns"`
	OK         bool   `json:"ok"`
	Attempts   int    `json:"attempts"`
	Err        string `json:"err,omitempty"`
}

func main() {
	peer := flag.String("endpoint", "127.0.0.1:7933", "Frontend TChannel endpoint")
	domain := flag.String("domain", "recovery", "domain (registered by the driver)")
	interval := flag.Duration("interval", 100*time.Millisecond, "workflow start interval")
	deadline := flag.Duration("workflow-deadline", 150*time.Second, "give up on one workflow after this long")
	out := flag.String("output", "samples.jsonl", "JSONL output")
	stopFile := flag.String("stop-file", "", "exit gracefully (after in-flight workflows finish) when this file appears")
	readyFile := flag.String("ready-file", "", "written once domain registered and worker started")
	lostDir := flag.String("lost-dir", "", "directory to dump event history of lost workflows")
	flag.Parse()

	tr, err := tchannel.NewTransport(tchannel.ServiceName("cadence-recovery"))
	must(err)
	d := yarpc.NewDispatcher(yarpc.Config{Name: "cadence-recovery", Outbounds: yarpc.Outbounds{"cadence-frontend": {Unary: tr.NewSingleOutbound(*peer)}}})
	must(d.Start())
	defer d.Stop()
	rpc := workflowserviceclient.New(d.ClientConfig("cadence-frontend"))

	// Register the domain, retrying until the Frontend is reachable.
	retention := int32(1)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		name := *domain
		err := rpc.RegisterDomain(ctx, &shared.RegisterDomainRequest{Name: &name, WorkflowExecutionRetentionPeriodInDays: &retention})
		cancel()
		if err == nil {
			break
		}
		if _, ok := err.(*shared.DomainAlreadyExistsError); ok {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	tl := "recovery-" + uuid.NewString()
	w := worker.New(rpc, *domain, tl, worker.Options{DisableStickyExecution: true})
	w.RegisterWorkflow(flow)
	w.RegisterActivity(echo)
	for {
		if err := w.Start(); err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	defer w.Stop()
	c := client.NewClient(rpc, *domain, nil)
	if *readyFile != "" {
		_ = os.WriteFile(*readyFile, []byte(time.Now().UTC().Format(time.RFC3339Nano)+"\n"), 0o644)
	}

	f, err := os.Create(*out)
	must(err)
	var mu sync.Mutex
	enc := json.NewEncoder(f)
	emit := func(r rec) {
		mu.Lock()
		defer mu.Unlock()
		_ = enc.Encode(r)
		_ = f.Sync()
	}
	var wg sync.WaitGroup
	var inflight int64
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	tick := time.NewTicker(*interval)
	defer tick.Stop()
loop:
	for seq := 0; ; seq++ {
		select {
		case <-sig:
			break loop
		case <-tick.C:
		}
		if *stopFile != "" {
			if _, err := os.Stat(*stopFile); err == nil {
				break loop
			}
		}
		if atomic.LoadInt64(&inflight) > 400 {
			continue
		}
		atomic.AddInt64(&inflight, 1)
		wg.Add(1)
		go func(seq int) {
			defer wg.Done()
			defer atomic.AddInt64(&inflight, -1)
			id := uuid.NewString()
			payload := fmt.Sprintf("p-%d", seq)
			r := rec{Seq: seq, WorkflowID: id, StartNs: time.Now().UnixNano()}
			ctx, cancel := context.WithTimeout(context.Background(), *deadline)
			defer cancel()
			var exec client.WorkflowRun
			var e error
			for {
				r.Attempts++
				sctx, scancel := context.WithTimeout(ctx, 5*time.Second)
				exec, e = c.ExecuteWorkflow(sctx, client.StartWorkflowOptions{ID: id, TaskList: tl, ExecutionStartToCloseTimeout: 90 * time.Second, DecisionTaskStartToCloseTimeout: 10 * time.Second}, flow, payload)
				scancel()
				if e == nil {
					break
				}
				if _, ok := e.(*shared.WorkflowExecutionAlreadyStartedError); ok {
					// An earlier attempt reached history before the reply was lost: attach to it.
					exec = c.GetWorkflow(ctx, id, "")
					e = nil
					break
				}
				if ctx.Err() != nil {
					break
				}
				time.Sleep(50 * time.Millisecond)
			}
			var v string
			if e == nil {
				e = exec.Get(ctx, &v)
			}
			if e == nil && v != payload {
				e = fmt.Errorf("output %q != input %q", v, payload)
			}
			r.EndNs = time.Now().UnixNano()
			r.OK = e == nil
			if e != nil {
				r.Err = e.Error()
			}
			emit(r)
			if e != nil && *lostDir != "" {
				dumpHistory(rpc, *domain, id, *lostDir)
			}
		}(seq)
	}
	wg.Wait()
	_ = f.Close()
}

// dumpHistory writes the full event history (with timestamps) of a lost workflow to dir/<id>.json.
func dumpHistory(rpc workflowserviceclient.Interface, domain, id, dir string) {
	_ = os.MkdirAll(dir, 0o755)
	var events []*shared.HistoryEvent
	var token []byte
	for i := 0; i < 20; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		resp, err := rpc.GetWorkflowExecutionHistory(ctx, &shared.GetWorkflowExecutionHistoryRequest{
			Domain: &domain, Execution: &shared.WorkflowExecution{WorkflowId: &id}, NextPageToken: token,
		})
		cancel()
		if err != nil {
			events = append(events, &shared.HistoryEvent{}) // marker; error below
			_ = os.WriteFile(dir+"/"+id+".err", []byte(err.Error()), 0o644)
			break
		}
		if resp.History != nil {
			events = append(events, resp.History.Events...)
		}
		token = resp.NextPageToken
		if len(token) == 0 {
			break
		}
	}
	b, _ := json.MarshalIndent(events, "", " ")
	_ = os.WriteFile(dir+"/"+id+".json", b, 0o644)
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
