// Frontend soak load generator: Go SDK against the Rust Frontend over TChannel.
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

func SoakWorkflow(ctx workflow.Context, in string) (string, error) {
	state := "init"
	if err := workflow.SetQueryHandler(ctx, "state", func() (string, error) { return state, nil }); err != nil {
		return "", err
	}
	if err := workflow.Sleep(ctx, 2*time.Second); err != nil {
		return "", err
	}
	ao := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{ScheduleToStartTimeout: time.Minute, StartToCloseTimeout: time.Minute})
	var out string
	if err := workflow.ExecuteActivity(ao, EchoActivity, in).Get(ctx, &out); err != nil {
		return "", err
	}
	wake, done := workflow.GetSignalChannel(ctx, "wake"), workflow.GetSignalChannel(ctx, "done")
	for {
		sel := workflow.NewSelector(ctx)
		fin := false
		sel.AddReceive(wake, func(c workflow.Channel, _ bool) { c.Receive(ctx, &state) })
		sel.AddReceive(done, func(c workflow.Channel, _ bool) { var s string; c.Receive(ctx, &s); fin = true })
		sel.Select(ctx)
		if fin {
			return "final:" + state + ":" + out, nil
		}
	}
}

var started, completed, failed, retries, signals, queries, inflight int64
var latNs, latCount, latMax, queryInit, queryInitWfs int64

// Latency samples (ms) for percentiles: end-to-end, start call, query call (successful attempt only).
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
		fail := func(stage string, err error) {
			atomic.AddInt64(&failed, 1)
			fmt.Printf("FAIL %s %s %v\n", id, stage, err)
		}
		var run client.WorkflowRun
		err := retry(func() error {
			var e error
			ts := time.Now()
			defer func() {
				if e == nil {
					record("start", time.Since(ts).Milliseconds())
				}
			}()
			run, e = c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: id, TaskList: tl, ExecutionStartToCloseTimeout: 10 * time.Minute, DecisionTaskStartToCloseTimeout: 10 * time.Second}, SoakWorkflow, id)
			return e
		})
		if err != nil {
			fail("start", err)
			return
		}
		atomic.AddInt64(&started, 1)
		tStarted := time.Since(t0).Milliseconds()
		sleepFor := time.Duration(1+rand.Intn(4)) * time.Second
		time.Sleep(sleepFor)
		tSig0 := time.Now()
		if err = retry(func() error { return c.SignalWorkflow(ctx, id, run.GetRunID(), "wake", "w-"+id) }); err != nil {
			fail("signal", err)
			return
		}
		atomic.AddInt64(&signals, 1)
		tSigMs := time.Since(tSig0).Milliseconds()
		tQ0 := time.Now()
		qAttempts := 0
		sawInit := false
		if err = retry(func() error {
			qs := time.Now()
			qAttempts++
			resp, e := c.QueryWorkflow(ctx, id, run.GetRunID(), "state")
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
			return
		}
		atomic.AddInt64(&queries, 1)
		if err = retry(func() error { return c.SignalWorkflow(ctx, id, run.GetRunID(), "done", "x") }); err != nil {
			fail("signal-done", err)
			return
		}
		tQMs := time.Since(tQ0).Milliseconds()
		tR0 := time.Now()
		var out string
		if err = retry(func() error {
			gctx, cancel := context.WithTimeout(ctx, 60*time.Second)
			defer cancel()
			return c.GetWorkflow(gctx, id, run.GetRunID()).Get(gctx, &out)
		}); err != nil {
			fail("result", err)
			return
		}
		if out != "final:w-"+id+":"+id {
			fail("result-value", fmt.Errorf("got %q", out))
			return
		}
		ms := time.Since(t0).Milliseconds()
		fmt.Printf("STAGE %s e2e=%d start=%d presleep=%d signal=%d query=%d qattempts=%d result_after_done=%d\n", id, ms, tStarted, sleepFor.Milliseconds(), tSigMs, tQMs, qAttempts, time.Since(tR0).Milliseconds())
		atomic.AddInt64(&completed, 1)
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
