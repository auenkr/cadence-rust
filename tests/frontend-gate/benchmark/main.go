// SDK workload shared by Go and Rust Frontend+Matching benchmark deployments.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.uber.org/cadence/.gen/go/cadence/workflowserviceclient"
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

func main() {
	peer := flag.String("endpoint", "127.0.0.1:7933", "Frontend TChannel endpoint")
	domain := flag.String("domain", "benchmark", "pre-registered domain")
	label := flag.String("label", "", "deployment label")
	n := flag.Int("requests", 1000, "measured workflows")
	warmup := flag.Int("warmup", 100, "unmeasured workflows")
	concurrency := flag.Int("concurrency", 16, "closed-loop workflow concurrency")
	bytes := flag.Int("payload-bytes", 1024, "activity input/output bytes")
	output := flag.String("output", "benchmark.json", "JSON result path")
	measurementStartFile := flag.String("measurement-start-file", "", "write when warmup is complete and the collector may start its baseline")
	measurementEndFile := flag.String("measurement-end-file", "", "write after the last measured workflow (also written on failure)")
	collectorReadyFile := flag.String("collector-ready-file", "", "wait for the collector to create this after taking its baseline")
	collectorReadyTimeout := flag.Duration("collector-ready-timeout", 2*time.Minute, "maximum wait for collector readiness")
	flag.Parse()
	if *n < 1 || *warmup < 0 || *concurrency < 1 || *bytes < 0 || *label == "" { panic("invalid benchmark parameters") }
	if (*measurementStartFile == "") != (*measurementEndFile == "") { panic("measurement start and end marker files must be provided together") }
	if *collectorReadyFile != "" && *measurementStartFile == "" { panic("collector-ready-file requires measurement marker files") }
	if *collectorReadyTimeout <= 0 { panic("collector-ready-timeout must be positive") }
	// The collector waits for the start marker. Always release it on early exit,
	// including warmup/worker failures, so its process cannot wait forever.
	measurementStarted := false
	measuredStartUTC, measuredEndUTC := "", ""
	defer func() {
		if *measurementStartFile == "" { return }
		if !measurementStarted { _ = writeMarker(*measurementStartFile) }
		if measuredEndUTC == "" { measuredEndUTC = time.Now().UTC().Format(time.RFC3339Nano) }
		_ = writeMarker(*measurementEndFile)
	}()
	for _, path := range []string{*measurementStartFile, *measurementEndFile, *collectorReadyFile} {
		if path != "" {
			if err := os.Remove(path); err != nil && !os.IsNotExist(err) { panic(err) }
		}
	}
	tr, err := tchannel.NewTransport(tchannel.ServiceName("cadence-benchmark"))
	if err != nil { panic(err) }
	d := yarpc.NewDispatcher(yarpc.Config{Name: "cadence-benchmark", Outbounds: yarpc.Outbounds{"cadence-frontend": {Unary: tr.NewSingleOutbound(*peer)}}})
	if err = d.Start(); err != nil { panic(err) }
	defer d.Stop()
	rpc := workflowserviceclient.New(d.ClientConfig("cadence-frontend"))
	tl := "benchmark-" + uuid.NewString()
	w := worker.New(rpc, *domain, tl, worker.Options{DisableStickyExecution: true})
	w.RegisterWorkflow(flow)
	w.RegisterActivity(echo)
	if err = w.Start(); err != nil { panic(err) }
	defer w.Stop()
	c := client.NewClient(rpc, *domain, nil)
	payload := strings.Repeat("x", *bytes)
	run := func(count int) ([]float64, []string, float64) {
		var mu sync.Mutex
		var wg sync.WaitGroup
		latencies := []float64{}
		failures := []string{}
		jobs := make(chan int)
		start := time.Now()
		for i := 0; i < *concurrency; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for range jobs {
					begin := time.Now()
					ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
					r, e := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: uuid.NewString(), TaskList: tl, ExecutionStartToCloseTimeout: time.Minute, DecisionTaskStartToCloseTimeout: 10*time.Second}, flow, payload)
					var value string
					if e == nil { e = r.Get(ctx, &value) }
					if e == nil && value != payload { e = fmt.Errorf("activity output differs from input") }
					cancel()
					mu.Lock()
					if e != nil { failures = append(failures, e.Error()) } else { latencies = append(latencies, time.Since(begin).Seconds()*1000) }
					mu.Unlock()
				}
			}()
		}
		for i := 0; i < count; i++ { jobs <- i }
		close(jobs)
		wg.Wait()
		return latencies, failures, time.Since(start).Seconds()
	}
	_, warmupErrors, _ := run(*warmup)
	if len(warmupErrors) != 0 { panic(fmt.Sprintf("warmup failed: %s", warmupErrors[0])) }
	if *measurementStartFile != "" {
		if err = writeMarker(*measurementStartFile); err != nil { panic(err) }
		measurementStarted = true
		if *collectorReadyFile != "" {
			// Collector contract: after observing measurement-start-file, take the
			// baseline snapshot, then create collector-ready-file. We start timing
			// only after that signal, so warmup and baseline setup are excluded.
			deadline := time.Now().Add(*collectorReadyTimeout)
			for {
				if _, err = os.Stat(*collectorReadyFile); err == nil { break }
				if !os.IsNotExist(err) { panic(err) }
				if time.Now().After(deadline) { panic(fmt.Sprintf("timeout waiting for collector readiness file %s", *collectorReadyFile)) }
				time.Sleep(100 * time.Millisecond)
			}
		}
	}
	measuredStartUTC = time.Now().UTC().Format(time.RFC3339Nano)
	latencies, failures, seconds := run(*n)
	measuredEndUTC = time.Now().UTC().Format(time.RFC3339Nano)
	if *measurementEndFile != "" {
		if err = writeMarker(*measurementEndFile); err != nil { panic(err) }
	}
	sort.Float64s(latencies)
	percentile := func(p float64) interface{} {
		if len(latencies) == 0 { return nil }
		return latencies[int(float64(len(latencies)-1)*p)]
	}
	result := map[string]interface{}{"label": *label, "endpoint": *peer, "requests": *n, "warmup": *warmup, "concurrency": *concurrency, "payload_bytes": *bytes, "successful": len(latencies), "failed": len(failures), "elapsed_seconds": seconds, "measured_start_utc": measuredStartUTC, "measured_end_utc": measuredEndUTC, "successful_workflows_per_second": float64(len(latencies))/seconds, "latency_ms": map[string]interface{}{"p50": percentile(.50), "p95": percentile(.95), "p99": percentile(.99)}, "errors": failures, "successful_latency_samples_ms": latencies, "transport": "tchannel", "sticky_execution": false}
	f, err := os.Create(*output)
	if err != nil { panic(err) }
	if err = json.NewEncoder(f).Encode(result); err != nil { panic(err) }
	if err = f.Close(); err != nil { panic(err) }
	if len(failures) != 0 { os.Exit(1) }
}

func writeMarker(path string) error {
	if path == "" { return nil }
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil { return err }
	return os.WriteFile(path, []byte(time.Now().UTC().Format(time.RFC3339Nano)), 0o644)
}
