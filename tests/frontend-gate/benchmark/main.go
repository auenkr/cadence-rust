// SDK workload shared by Go and Rust Frontend+Matching benchmark deployments.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
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

func echo(_ context.Context, payload string) (string, error) { return payload, nil }
func flow(ctx workflow.Context, payload string) (string, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{ScheduleToStartTimeout: time.Minute, StartToCloseTimeout: time.Minute})
	var result string
	err := workflow.ExecuteActivity(ctx, echo, payload).Get(ctx, &result)
	return result, err
}

type sample struct {
	Seq       int     `json:"seq"`
	StartUnix int64   `json:"start_unix_nano"`
	LatencyMs float64 `json:"latency_ms"`
	WorkflowID string `json:"workflow_id"`
	RunID     string  `json:"run_id"`
}

// historyGaps fetches a workflow history and returns named gaps (ms) between consecutive milestone events.
func historyGaps(c client.Client, id, runID string) map[string]float64 {
	it := c.GetWorkflowHistory(context.Background(), id, runID, false, shared.HistoryEventFilterTypeAllEvent)
	type ev struct {
		t    shared.EventType
		ts   int64
	}
	var evs []ev
	for it.HasNext() {
		e, err := it.Next()
		if err != nil {
			return map[string]float64{"error": -1}
		}
		evs = append(evs, ev{e.GetEventType(), e.GetTimestamp()})
	}
	nth := func(t shared.EventType, n int) (int64, bool) {
		for _, e := range evs {
			if e.t == t {
				if n == 0 {
					return e.ts, true
				}
				n--
			}
		}
		return 0, false
	}
	out := map[string]float64{}
	gap := func(name string, t1 shared.EventType, n1 int, t2 shared.EventType, n2 int) {
		a, ok1 := nth(t1, n1)
		b, ok2 := nth(t2, n2)
		if ok1 && ok2 {
			out[name] = float64(b-a) / 1e6
		}
	}
	S, DS, DST, DC := shared.EventTypeWorkflowExecutionStarted, shared.EventTypeDecisionTaskScheduled, shared.EventTypeDecisionTaskStarted, shared.EventTypeDecisionTaskCompleted
	AS, AST, AC, WC := shared.EventTypeActivityTaskScheduled, shared.EventTypeActivityTaskStarted, shared.EventTypeActivityTaskCompleted, shared.EventTypeWorkflowExecutionCompleted
	gap("1_start_to_dec1_scheduled", S, 0, DS, 0)
	gap("2_dec1_scheduled_to_started", DS, 0, DST, 0)
	gap("3_dec1_started_to_completed", DST, 0, DC, 0)
	gap("4_dec1_completed_to_activity_scheduled", DC, 0, AS, 0)
	gap("5_activity_scheduled_to_started", AS, 0, AST, 0)
	gap("6_activity_started_to_completed", AST, 0, AC, 0)
	gap("7_activity_completed_to_dec2_scheduled", AC, 0, DS, 1)
	gap("8_dec2_scheduled_to_started", DS, 1, DST, 1)
	gap("9_dec2_started_to_completed", DST, 1, DC, 1)
	gap("10_dec2_completed_to_workflow_completed", DC, 1, WC, 0)
	gap("11_total_history_span", S, 0, WC, 0)
	return out
}

func main() {
	peer := flag.String("endpoint", "127.0.0.1:7933", "Frontend TChannel endpoint")
	domain := flag.String("domain", "benchmark", "pre-registered domain")
	label := flag.String("label", "", "deployment label")
	n := flag.Int("requests", 1000, "measured workflows")
	warmup := flag.Int("warmup", 100, "unmeasured workflows")
	concurrency := flag.Int("concurrency", 16, "closed-loop workflow concurrency")
	bytes := flag.Int("payload-bytes", 1024, "activity input/output bytes")
	sticky := flag.Bool("sticky", false, "enable worker sticky execution")
	output := flag.String("output", "benchmark.json", "JSON result path")
	measurementStartFile := flag.String("measurement-start-file", "", "write when warmup is complete and the collector may start its baseline")
	measurementEndFile := flag.String("measurement-end-file", "", "write after the last measured workflow (also written on failure)")
	collectorReadyFile := flag.String("collector-ready-file", "", "wait for the collector to create this after taking its baseline")
	collectorReadyTimeout := flag.Duration("collector-ready-timeout", 2*time.Minute, "maximum wait for collector readiness")
	flag.Parse()
	if *n < 1 || *warmup < 0 || *concurrency < 1 || *bytes < 0 || *label == "" {
		panic("invalid benchmark parameters")
	}
	if (*measurementStartFile == "") != (*measurementEndFile == "") {
		panic("measurement start and end marker files must be provided together")
	}
	if *collectorReadyFile != "" && *measurementStartFile == "" {
		panic("collector-ready-file requires measurement marker files")
	}
	if *collectorReadyTimeout <= 0 {
		panic("collector-ready-timeout must be positive")
	}
	// The collector waits for the start marker. Always release it on early exit,
	// including warmup/worker failures, so its process cannot wait forever.
	measurementStarted := false
	measuredStartUTC, measuredEndUTC := "", ""
	defer func() {
		if *measurementStartFile == "" {
			return
		}
		if !measurementStarted {
			_ = writeMarker(*measurementStartFile)
		}
		if measuredEndUTC == "" {
			measuredEndUTC = time.Now().UTC().Format(time.RFC3339Nano)
		}
		_ = writeMarker(*measurementEndFile)
	}()
	for _, path := range []string{*measurementStartFile, *measurementEndFile, *collectorReadyFile} {
		if path != "" {
			if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
				panic(err)
			}
		}
	}
	tr, err := tchannel.NewTransport(tchannel.ServiceName("cadence-benchmark"))
	if err != nil {
		panic(err)
	}
	d := yarpc.NewDispatcher(yarpc.Config{Name: "cadence-benchmark", Outbounds: yarpc.Outbounds{"cadence-frontend": {Unary: tr.NewSingleOutbound(*peer)}}})
	if err = d.Start(); err != nil {
		panic(err)
	}
	defer d.Stop()
	rpc := workflowserviceclient.New(d.ClientConfig("cadence-frontend"))
	tl := "benchmark-" + uuid.NewString()
	w := worker.New(rpc, *domain, tl, worker.Options{DisableStickyExecution: !*sticky})
	w.RegisterWorkflow(flow)
	w.RegisterActivity(echo)
	if err = w.Start(); err != nil {
		panic(err)
	}
	defer w.Stop()
	c := client.NewClient(rpc, *domain, nil)
	payload := strings.Repeat("x", *bytes)
	run := func(count int) ([]float64, []string, float64, []sample) {
		var mu sync.Mutex
		var wg sync.WaitGroup
		latencies := []float64{}
		failures := []string{}
		samples := []sample{}
		jobs := make(chan int)
		start := time.Now()
		for i := 0; i < *concurrency; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for range jobs {
					begin := time.Now()
					ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
					wfID, runID := uuid.NewString(), ""
					r, e := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: wfID, TaskList: tl, ExecutionStartToCloseTimeout: time.Minute, DecisionTaskStartToCloseTimeout: 10 * time.Second}, flow, payload)
					var value string
					if e == nil {
						runID = r.GetRunID()
						e = r.Get(ctx, &value)
					}
					if e == nil && value != payload {
						e = fmt.Errorf("activity output differs from input")
					}
					cancel()
					mu.Lock()
					if e != nil {
						failures = append(failures, e.Error())
					} else {
						ms := time.Since(begin).Seconds() * 1000
						latencies = append(latencies, ms)
						samples = append(samples, sample{len(samples), begin.UnixNano(), ms, wfID, runID})
					}
					mu.Unlock()
				}
			}()
		}
		for i := 0; i < count; i++ {
			jobs <- i
		}
		close(jobs)
		wg.Wait()
		return latencies, failures, time.Since(start).Seconds(), samples
	}
	_, warmupErrors, _, _ := run(*warmup)
	if len(warmupErrors) != 0 {
		panic(fmt.Sprintf("warmup failed: %s", warmupErrors[0]))
	}
	if *measurementStartFile != "" {
		if err = writeMarker(*measurementStartFile); err != nil {
			panic(err)
		}
		measurementStarted = true
		if *collectorReadyFile != "" {
			// Collector contract: after observing measurement-start-file, take the
			// baseline snapshot, then create collector-ready-file. We start timing
			// only after that signal, so warmup and baseline setup are excluded.
			deadline := time.Now().Add(*collectorReadyTimeout)
			for {
				if _, err = os.Stat(*collectorReadyFile); err == nil {
					break
				}
				if !os.IsNotExist(err) {
					panic(err)
				}
				if time.Now().After(deadline) {
					panic(fmt.Sprintf("timeout waiting for collector readiness file %s", *collectorReadyFile))
				}
				time.Sleep(100 * time.Millisecond)
			}
		}
	}
	measuredStartUTC = time.Now().UTC().Format(time.RFC3339Nano)
	latencies, failures, seconds, samples := run(*n)
	measuredEndUTC = time.Now().UTC().Format(time.RFC3339Nano)
	if *measurementEndFile != "" {
		if err = writeMarker(*measurementEndFile); err != nil {
			panic(err)
		}
	}
	// Raw per-workflow samples in completion order (before sorting).
	if rawF, rerr := os.Create(strings.TrimSuffix(*output, ".json") + "-samples.json"); rerr == nil {
		_ = json.NewEncoder(rawF).Encode(samples)
		_ = rawF.Close()
	}
	// History gap analysis for the slowest 5% and 20 random fast workflows.
	if len(samples) > 0 {
		order := append([]sample(nil), samples...)
		sort.Slice(order, func(i, j int) bool { return order[i].LatencyMs > order[j].LatencyMs })
		nSlow := (len(order) + 19) / 20
		pick := map[string][]sample{"slow": order[:nSlow]}
		fast := append([]sample(nil), order[nSlow:]...)
		rand.Shuffle(len(fast), func(i, j int) { fast[i], fast[j] = fast[j], fast[i] })
		if len(fast) > 20 {
			fast = fast[:20]
		}
		pick["fast"] = fast
		type rec struct {
			sample
			Gaps map[string]float64 `json:"gaps_ms"`
		}
		tail := map[string][]rec{}
		for k, list := range pick {
			for _, sm := range list {
				tail[k] = append(tail[k], rec{sm, historyGaps(c, sm.WorkflowID, sm.RunID)})
			}
		}
		if hf, herr := os.Create(strings.TrimSuffix(*output, ".json") + "-tail-history.json"); herr == nil {
			_ = json.NewEncoder(hf).Encode(tail)
			_ = hf.Close()
		}
	}
	sort.Float64s(latencies)
	percentile := func(p float64) interface{} {
		if len(latencies) == 0 {
			return nil
		}
		return latencies[int(float64(len(latencies)-1)*p)]
	}
	result := map[string]interface{}{"label": *label, "endpoint": *peer, "requests": *n, "warmup": *warmup, "concurrency": *concurrency, "payload_bytes": *bytes, "successful": len(latencies), "failed": len(failures), "elapsed_seconds": seconds, "measured_start_utc": measuredStartUTC, "measured_end_utc": measuredEndUTC, "successful_workflows_per_second": float64(len(latencies)) / seconds, "latency_ms": map[string]interface{}{"p50": percentile(.50), "p95": percentile(.95), "p99": percentile(.99)}, "errors": failures, "successful_latency_samples_ms": latencies, "transport": "tchannel", "sticky_execution": *sticky}
	f, err := os.Create(*output)
	if err != nil {
		panic(err)
	}
	if err = json.NewEncoder(f).Encode(result); err != nil {
		panic(err)
	}
	if err = f.Close(); err != nil {
		panic(err)
	}
	if len(failures) != 0 {
		os.Exit(1)
	}
}

func writeMarker(path string) error {
	if path == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, []byte(time.Now().UTC().Format(time.RFC3339Nano)), 0o644)
}
