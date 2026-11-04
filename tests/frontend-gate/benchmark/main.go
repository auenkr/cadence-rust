// SDK workload shared by Go and Rust Frontend+Matching benchmark deployments.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
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
	flag.Parse()
	if *n < 1 || *warmup < 0 || *concurrency < 1 || *bytes < 0 || *label == "" { panic("invalid benchmark parameters") }
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
	latencies, failures, seconds := run(*n)
	sort.Float64s(latencies)
	percentile := func(p float64) interface{} {
		if len(latencies) == 0 { return nil }
		return latencies[int(float64(len(latencies)-1)*p)]
	}
	result := map[string]interface{}{"label": *label, "endpoint": *peer, "requests": *n, "warmup": *warmup, "concurrency": *concurrency, "payload_bytes": *bytes, "successful": len(latencies), "failed": len(failures), "elapsed_seconds": seconds, "successful_workflows_per_second": float64(len(latencies))/seconds, "latency_ms": map[string]interface{}{"p50": percentile(.50), "p95": percentile(.95), "p99": percentile(.99)}, "errors": failures, "successful_latency_samples_ms": latencies, "transport": "tchannel", "sticky_execution": false}
	f, err := os.Create(*output)
	if err != nil { panic(err) }
	if err = json.NewEncoder(f).Encode(result); err != nil { panic(err) }
	if err = f.Close(); err != nil { panic(err) }
	if len(failures) != 0 { os.Exit(1) }
}
