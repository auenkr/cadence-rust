// History soak load generator (timers, activities, signals, queries, child workflows, continue-as-new): Go SDK against the Rust Frontend over TChannel.
// Steady start/timer/activity/signal/query/complete workflows at SOAK_RATE per second for SOAK_DURATION.
// Env: FE_TCHANNEL, FE_DOMAIN, SOAK_DURATION (e.g. 2h5m), SOAK_RATE (default 8), SOAK_DRAIN (default 10m).
// Prints "STATS {json}" every 10s and "FINAL {json}"; exits 1 unless every started workflow completed.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
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

type SoakIn struct {
	ID    string
	Gen   int
	State string
	Done  bool
}

func ChildWorkflow(ctx workflow.Context, in string) (string, error) {
	if err := workflow.Sleep(ctx, time.Second); err != nil {
		return "", err
	}
	ao := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{ScheduleToStartTimeout: time.Minute, StartToCloseTimeout: time.Minute})
	var out string
	err := workflow.ExecuteActivity(ao, EchoActivity, in).Get(ctx, &out)
	return out, err
}

// SoakWorkflow gen 0: timer, activity, child workflow, wait for "wake", then continue-as-new.
// gen 1: serve the state query and wait for "done". Signals/queries are sent without a run ID so they follow the chain.
func SoakWorkflow(ctx workflow.Context, in SoakIn) (string, error) {
	state := in.State
	if state == "" {
		state = "init"
	}
	if err := workflow.SetQueryHandler(ctx, "state", func() (string, error) { return state, nil }); err != nil {
		return "", err
	}
	wake, done := workflow.GetSignalChannel(ctx, "wake"), workflow.GetSignalChannel(ctx, "done")
	fin := in.Done
	if in.Gen == 0 {
		if err := workflow.Sleep(ctx, 2*time.Second); err != nil {
			return "", err
		}
		ao := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{ScheduleToStartTimeout: time.Minute, StartToCloseTimeout: time.Minute})
		var out string
		if err := workflow.ExecuteActivity(ao, EchoActivity, in.ID).Get(ctx, &out); err != nil {
			return "", err
		}
		cctx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{ExecutionStartToCloseTimeout: 5 * time.Minute, TaskStartToCloseTimeout: 10 * time.Second})
		var cout string
		if err := workflow.ExecuteChildWorkflow(cctx, ChildWorkflow, in.ID).Get(ctx, &cout); err != nil {
			return "", err
		}
		if cout != in.ID {
			return "", fmt.Errorf("child result %q", cout)
		}
		woke := false
		for !woke {
			sel := workflow.NewSelector(ctx)
			sel.AddReceive(wake, func(c workflow.Channel, _ bool) { c.Receive(ctx, &state); woke = true })
			sel.AddReceive(done, func(c workflow.Channel, _ bool) { var s string; c.Receive(ctx, &s); fin = true })
			sel.Select(ctx)
		}
		var s string
		for done.ReceiveAsync(&s) {
			fin = true
		}
		return "", workflow.NewContinueAsNewError(ctx, SoakWorkflow, SoakIn{ID: in.ID, Gen: 1, State: state, Done: fin})
	}
	for !fin {
		sel := workflow.NewSelector(ctx)
		sel.AddReceive(wake, func(c workflow.Channel, _ bool) { c.Receive(ctx, &state) })
		sel.AddReceive(done, func(c workflow.Channel, _ bool) { var s string; c.Receive(ctx, &s); fin = true })
		sel.Select(ctx)
	}
	return "final:" + state + ":" + in.ID, nil
}

var started, completed, failed, retries, signals, queries, inflight int64
var latNs, latCount, latMax, queryInit, queryInitWfs int64

// Latency samples (ms) for percentiles: end-to-end, start call, query call (successful attempt only).
var dumped, dumpedOK int64
var latMu sync.Mutex
var lat = map[string][]int64{"e2e": nil, "start": nil, "query": nil}

func record(kind string, ms int64) {
	latMu.Lock()
	lat[kind] = append(lat[kind], ms)
	latMu.Unlock()
}

func pcts(kind string) map[string]int64 {
	latMu.Lock()
	v := append([]int64(nil), lat[kind]...)
	latMu.Unlock()
	out := map[string]int64{}
	if len(v) == 0 {
		return out
	}
	sort.Slice(v, func(i, j int) bool { return v[i] < v[j] })
	for _, q := range []struct {
		n string
		p float64
	}{{"p50", .5}, {"p95", .95}, {"p99", .99}} {
		out[kind+"_"+q.n+"_ms"] = v[int(float64(len(v)-1)*q.p)]
	}
	return out
}

// retry retries transient errors (Frontend restart window) up to 90s and counts them.
func retry(f func() error) error {
	var err error
	for end := time.Now().Add(90 * time.Second); time.Now().Before(end); time.Sleep(time.Second) {
		if err = f(); err == nil {
			return nil
		}
		atomic.AddInt64(&retries, 1)
	}
	return err
}

func snapshot() map[string]int64 {
	n := atomic.LoadInt64(&latCount)
	avg := int64(0)
	if n > 0 {
		avg = atomic.LoadInt64(&latNs) / n / 1e6
	}
	m := map[string]int64{"ts": time.Now().Unix(), "started": atomic.LoadInt64(&started), "completed": atomic.LoadInt64(&completed),
		"failed": atomic.LoadInt64(&failed), "retried_calls": atomic.LoadInt64(&retries), "signals": atomic.LoadInt64(&signals),
		"queries": atomic.LoadInt64(&queries), "inflight": atomic.LoadInt64(&inflight), "e2e_avg_ms": avg, "e2e_max_ms": atomic.LoadInt64(&latMax),
		"query_init_attempts": atomic.LoadInt64(&queryInit), "query_init_workflows": atomic.LoadInt64(&queryInitWfs)}
	for _, k := range []string{"e2e", "start", "query"} {
		for a, b := range pcts(k) {
			m[a] = b
		}
	}
	return m
}

func main() {
	peer, domain := os.Getenv("FE_TCHANNEL"), os.Getenv("FE_DOMAIN")
	dur, _ := time.ParseDuration(os.Getenv("SOAK_DURATION"))
	if dur == 0 {
		dur = 2 * time.Minute
	}
	drain, _ := time.ParseDuration(os.Getenv("SOAK_DRAIN"))
	if drain == 0 {
		drain = 10 * time.Minute
	}
	rate, _ := strconv.ParseFloat(os.Getenv("SOAK_RATE"), 64)
	if rate <= 0 {
		rate = 8
	}
	tr, err := tchannel.NewTransport(tchannel.ServiceName("frontend-soak"))
	if err != nil {
		panic(err)
	}
	d := yarpc.NewDispatcher(yarpc.Config{Name: "frontend-soak", Outbounds: yarpc.Outbounds{"cadence-frontend": {Unary: tr.NewSingleOutbound(peer)}}})
	if err := d.Start(); err != nil {
		panic(err)
	}
	defer d.Stop()
	rpc := workflowserviceclient.New(d.ClientConfig("cadence-frontend"))
	tl := "fe-soak-" + uuid.NewString()
	w := worker.New(rpc, domain, tl, worker.Options{DisableStickyExecution: true})
	w.RegisterWorkflow(SoakWorkflow)
	w.RegisterWorkflow(ChildWorkflow)
	w.RegisterActivity(EchoActivity)
	if err := w.Start(); err != nil {
		fmt.Println("worker start failed:", err)
		os.Exit(2)
	}
	defer w.Stop()
	c := client.NewClient(rpc, domain, nil)
	ctx := context.Background()

	go func() {
		for range time.Tick(10 * time.Second) {
			b, _ := json.Marshal(snapshot())
			fmt.Println("STATS", string(b))
		}
	}()

	var wg sync.WaitGroup
	one := func(id string) {
		defer wg.Done()
		defer atomic.AddInt64(&inflight, -1)
		t0 := time.Now()
		marks := []time.Time{t0}
		fail := func(stage string, err error) {
			atomic.AddInt64(&failed, 1)
			fmt.Printf("FAIL %s %s %v\n", id, stage, err)
		}
		err := retry(func() error {
			var e error
			ts := time.Now()
			defer func() {
				if e == nil {
					record("start", time.Since(ts).Milliseconds())
				}
			}()
			_, e = c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: id, TaskList: tl, ExecutionStartToCloseTimeout: 10 * time.Minute, DecisionTaskStartToCloseTimeout: 10 * time.Second}, SoakWorkflow, SoakIn{ID: id})
			return e
		})
		if err != nil {
			fail("start", err)
			return
		}
		atomic.AddInt64(&started, 1)
		marks = append(marks, time.Now())
		time.Sleep(time.Duration(1+rand.Intn(4)) * time.Second)
		if err = retry(func() error { return c.SignalWorkflow(ctx, id, "", "wake", "w-"+id) }); err != nil {
			fail("signal", err)
			return
		}
		atomic.AddInt64(&signals, 1)
		marks = append(marks, time.Now())
		sawInit := false
		if err = retry(func() error {
			qs := time.Now()
			resp, e := c.QueryWorkflow(ctx, id, "", "state")
			if e != nil {
				return e
			}
			var s string
			if e = resp.Get(&s); e != nil {
				return e
			}
			if s != "w-"+id {
				if s == "init" {
					atomic.AddInt64(&queryInit, 1)
					if !sawInit {
						sawInit = true
						atomic.AddInt64(&queryInitWfs, 1)
					}
				}
				return fmt.Errorf("query state %q", s)
			}
			record("query", time.Since(qs).Milliseconds())
			return nil
		}); err != nil {
			fail("query", err)
			if atomic.AddInt64(&dumped, 1) <= 4 {
				dctx, dcancel := context.WithTimeout(ctx, 20*time.Second)
				it := c.GetWorkflowHistory(dctx, id, "", false, 0)
				for it.HasNext() {
					ev, e := it.Next()
					if e != nil {
						fmt.Printf("HISTDUMP %s err %v\n", id, e)
						break
					}
					fmt.Printf("HISTDUMP %s #%d %v ts=%d\n", id, ev.GetEventId(), ev.GetEventType(), ev.GetTimestamp())
				}
				dcancel()
			}
			return
		}
		atomic.AddInt64(&queries, 1)
		marks = append(marks, time.Now())
		if err = retry(func() error { return c.SignalWorkflow(ctx, id, "", "done", "x") }); err != nil {
			fail("signal-done", err)
			return
		}
		var out string
		if err = retry(func() error {
			gctx, cancel := context.WithTimeout(ctx, 60*time.Second)
			defer cancel()
			return c.GetWorkflow(gctx, id, "").Get(gctx, &out)
		}); err != nil {
			fail("result", err)
			return
		}
		if out != "final:w-"+id+":"+id {
			fail("result-value", fmt.Errorf("got %q", out))
			return
		}
		marks = append(marks, time.Now())
		ms := time.Since(t0).Milliseconds()
		atomic.AddInt64(&completed, 1)
		if ms > 12000 && atomic.AddInt64(&dumpedOK, 1) <= 3 {
			dctx, dcancel := context.WithTimeout(ctx, 20*time.Second)
			it := c.GetWorkflowHistory(dctx, id, "", false, 0)
			for it.HasNext() {
				ev, e := it.Next()
				if e != nil {
					break
				}
				fmt.Printf("HISTDUMP %s #%d %v ts=%d\n", id, ev.GetEventId(), ev.GetEventType(), ev.GetTimestamp())
			}
			dcancel()
			for i := 1; i < len(marks); i++ {
				fmt.Printf("STAGE %s %d %dms\n", id, i, marks[i].Sub(marks[i-1]).Milliseconds())
			}
			dumpChain(ctx, c, id)
		}
		atomic.AddInt64(&latNs, ms*1e6)
		atomic.AddInt64(&latCount, 1)
		record("e2e", ms)
		for {
			m := atomic.LoadInt64(&latMax)
			if ms <= m || atomic.CompareAndSwapInt64(&latMax, m, ms) {
				break
			}
		}
	}

	tick := time.NewTicker(time.Duration(float64(time.Second) / rate))
	end := time.Now().Add(dur)
	n := 0
	for time.Now().Before(end) {
		<-tick.C
		n++
		atomic.AddInt64(&inflight, 1)
		wg.Add(1)
		go one(fmt.Sprintf("%s-%07d", tl, n))
	}
	tick.Stop()
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(drain):
		fmt.Println("DRAIN TIMEOUT")
	}
	b, _ := json.Marshal(snapshot())
	fmt.Println("FINAL", string(b))
	s := snapshot()
	if s["started"] != s["completed"] || s["failed"] != 0 || s["started"] != int64(n) {
		os.Exit(1)
	}
}

// dumpChain prints the history of the first run (gen 0) of id and of its child workflows.
func dumpChain(ctx context.Context, c client.Client, id string) {
	dctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	prev := ""
	it := c.GetWorkflowHistory(dctx, id, "", false, 0)
	if it.HasNext() {
		if ev, e := it.Next(); e == nil && ev.WorkflowExecutionStartedEventAttributes != nil {
			prev = ev.WorkflowExecutionStartedEventAttributes.GetContinuedExecutionRunId()
		}
	}
	if prev == "" {
		return
	}
	children := []string{}
	it = c.GetWorkflowHistory(dctx, id, prev, false, 0)
	for it.HasNext() {
		ev, e := it.Next()
		if e != nil {
			break
		}
		fmt.Printf("HISTDUMP %s-g0 #%d %v ts=%d\n", id, ev.GetEventId(), ev.GetEventType(), ev.GetTimestamp())
		if a := ev.StartChildWorkflowExecutionInitiatedEventAttributes; a != nil && a.WorkflowId != nil {
			children = append(children, *a.WorkflowId)
		}
	}
	for _, ch := range children {
		it = c.GetWorkflowHistory(dctx, ch, "", false, 0)
		for it.HasNext() {
			ev, e := it.Next()
			if e != nil {
				break
			}
			fmt.Printf("HISTDUMP %s-child #%d %v ts=%d\n", id, ev.GetEventId(), ev.GetEventType(), ev.GetTimestamp())
		}
	}
}
