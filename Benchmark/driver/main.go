// The exact same SDK binary and workflow run against every deployment.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"strings"
	"sync"
	"time"

	"go.uber.org/cadence/.gen/go/cadence/workflowserviceclient"
	"go.uber.org/cadence/.gen/go/shared"
	"go.uber.org/cadence/client"
	"go.uber.org/cadence/worker"
	"go.uber.org/cadence/workflow"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/transport/tchannel"
)

type Input struct {
	Payload        string
	ActivityMillis int
}

func activity(ctx context.Context, in Input) (string, error) {
	select {
	case <-time.After(time.Duration(in.ActivityMillis) * time.Millisecond):
		return in.Payload, nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}
func oneActivity(ctx workflow.Context, in Input) (string, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{ScheduleToStartTimeout: time.Minute, StartToCloseTimeout: time.Minute})
	var result string
	err := workflow.ExecuteActivity(ctx, activity, in).Get(ctx, &result)
	return result, err
}

type Sample struct {
	ExpectedSHA256      string    `json:"expected_result_sha256"`
	ObservedSHA256      string    `json:"observed_result_sha256"`
	ActivityCompletions []int64   `json:"activity_completion_unix_nano"`
	ID                  string    `json:"workflow_id"`
	RunID               string    `json:"run_id"`
	Phase               string    `json:"phase"`
	Scheduled           time.Time `json:"scheduled"`
	Started             time.Time `json:"started"`
	Completed           time.Time `json:"completed"`
	StartMS             float64   `json:"start_rpc_ms"`
	CompletionMS        float64   `json:"completion_ms"`
	LagMS               float64   `json:"generator_lag_ms"`
	ActivitySTS         []float64 `json:"activity_schedule_to_start_ms"`
	DecisionSTS         []float64 `json:"decision_schedule_to_start_ms"`
	ActivityMS          []float64 `json:"activity_ms"`
	StartedOK           bool      `json:"start_ok"`
	Correct             bool      `json:"correct"`
	Error               string    `json:"error,omitempty"`
	Dropped             bool      `json:"generator_dropped"`
}

func history(ctx context.Context, rpc workflowserviceclient.Interface, domain string, s *Sample) error {
	req := &shared.GetWorkflowExecutionHistoryRequest{Domain: &domain, Execution: &shared.WorkflowExecution{WorkflowId: &s.ID, RunId: &s.RunID}}
	scheduled := map[int64]int64{}
	started := map[int64]int64{}
	activities := 0
	decisions := 0
	seen := map[string]bool{}
	for {
		h, err := rpc.GetWorkflowExecutionHistory(ctx, req)
		if err != nil {
			return err
		}
		if h.History == nil {
			return fmt.Errorf("missing history")
		}
		for _, e := range h.History.Events {
			switch e.GetEventType() {
			case shared.EventTypeActivityTaskScheduled, shared.EventTypeDecisionTaskScheduled:
				scheduled[e.GetEventId()] = e.GetTimestamp()
			case shared.EventTypeActivityTaskStarted:
				id := e.GetActivityTaskStartedEventAttributes().GetScheduledEventId()
				t, ok := scheduled[id]
				if !ok {
					return fmt.Errorf("missing activity schedule")
				}
				s.ActivitySTS = append(s.ActivitySTS, float64(e.GetTimestamp()-t)/1e6)
				started[e.GetEventId()] = e.GetTimestamp()
				activities++
			case shared.EventTypeDecisionTaskStarted:
				id := e.GetDecisionTaskStartedEventAttributes().GetScheduledEventId()
				t, ok := scheduled[id]
				if !ok {
					return fmt.Errorf("missing decision schedule")
				}
				s.DecisionSTS = append(s.DecisionSTS, float64(e.GetTimestamp()-t)/1e6)
				decisions++
			case shared.EventTypeActivityTaskCompleted:
				s.ActivityCompletions = append(s.ActivityCompletions, e.GetTimestamp())
				id := e.GetActivityTaskCompletedEventAttributes().GetStartedEventId()
				t, ok := started[id]
				if !ok {
					return fmt.Errorf("missing activity start")
				}
				s.ActivityMS = append(s.ActivityMS, float64(e.GetTimestamp()-t)/1e6)
			case shared.EventTypeActivityTaskTimedOut, shared.EventTypeActivityTaskFailed, shared.EventTypeDecisionTaskTimedOut, shared.EventTypeDecisionTaskFailed:
				return fmt.Errorf("unexpected task failure: %s", e.GetEventType())
			}
		}
		if len(h.NextPageToken) == 0 {
			break
		}
		key := string(h.NextPageToken)
		if seen[key] {
			return fmt.Errorf("repeated history token")
		}
		seen[key] = true
		req.NextPageToken = h.NextPageToken
	}
	if activities != 1 || len(s.ActivityMS) != 1 || decisions < 2 {
		return fmt.Errorf("task anomaly: activities=%d completions=%d decisions=%d", activities, len(s.ActivityMS), decisions)
	}
	return nil
}
func main() {
	mode := flag.String("mode", "generate", "generate or worker")
	endpoint := flag.String("endpoint", "127.0.0.1:7933", "TChannel frontend")
	domain := flag.String("domain", "benchmark", "domain")
	tl := flag.String("task-list", "benchmark", "task list")
	rate := flag.Float64("rate", 5, "fixed starts per second")
	duration := flag.Duration("duration", 30*time.Minute, "measurement arrival window")
	warmup := flag.Duration("warmup", 10*time.Minute, "warmup arrival window")
	max := flag.Int("max-inflight", 10000, "hard safety bound; overflow fails run without slowing arrivals")
	capacity := flag.Int("worker-capacity", 128, "activity and decision execution capacity")
	payload := flag.Int("payload-bytes", 1024, "bytes")
	activityMS := flag.Int("activity-ms", 10, "controlled activity duration")
	output := flag.String("output", "samples.jsonl", "per-ID durable ledger")
	prefix := flag.String("prefix", "benchmark", "unique run prefix")
	startFile := flag.String("measurement-start-file", "", "collector marker")
	endFile := flag.String("measurement-end-file", "", "collector marker")
	readyFile := flag.String("collector-ready-file", "", "collector baseline handshake")
	flag.Parse()
	if *rate <= 0 || math.IsNaN(*rate) || math.IsInf(*rate, 0) || *rate > 100000 || *duration <= 0 || *warmup < 0 || *max < 1 || *payload < 0 || *activityMS < 0 || *capacity < 1 {
		panic("invalid parameters")
	}
	tr, err := tchannel.NewTransport(tchannel.ServiceName("cadence-openloop"))
	must(err)
	d := yarpc.NewDispatcher(yarpc.Config{Name: "cadence-openloop", Outbounds: yarpc.Outbounds{"cadence-frontend": {Unary: tr.NewSingleOutbound(*endpoint)}}})
	must(d.Start())
	defer d.Stop()
	rpc := workflowserviceclient.New(d.ClientConfig("cadence-frontend"))
	if *mode == "worker" {
		w := worker.New(rpc, *domain, *tl, worker.Options{DisableStickyExecution: true, MaxConcurrentActivityExecutionSize: *capacity, MaxConcurrentDecisionTaskExecutionSize: *capacity})
		w.RegisterWorkflow(oneActivity)
		w.RegisterActivity(activity)
		must(w.Start())
		defer w.Stop()
		fmt.Println("worker ready")
		select {}
	}
	if *mode != "generate" {
		panic("unknown mode")
	}
	f, err := os.Create(*output)
	must(err)
	defer f.Close()
	encoder := json.NewEncoder(f)
	c := client.NewClient(rpc, *domain, nil)
	in := Input{strings.Repeat("x", *payload), *activityMS}
	var mu sync.Mutex
	var wg sync.WaitGroup
	slots := make(chan struct{}, *max)
	failed := false
	emit := func(s Sample) {
		mu.Lock()
		defer mu.Unlock()
		must(encoder.Encode(s))
		if !s.Correct {
			failed = true
		}
	}
	phase := func(name string, window time.Duration) {
		begin := time.Now()
		count := int(math.Ceil(window.Seconds() * *rate))
		for i := 0; i < count; i++ {
			scheduled := begin.Add(time.Duration(float64(i) / *rate * float64(time.Second)))
			if delay := time.Until(scheduled); delay > 0 {
				time.Sleep(delay)
			}
			s := Sample{ID: fmt.Sprintf("%s-%s-%09d", *prefix, name, i), Phase: name, Scheduled: scheduled, LagMS: float64(time.Since(scheduled)) / 1e6}
			select {
			case slots <- struct{}{}:
			default:
				s.Dropped = true
				s.Error = "inflight safety bound exceeded"
				emit(s)
				continue
			}
			wg.Add(1)
			go func(s Sample) {
				defer wg.Done()
				defer func() { <-slots }()
				s.ExpectedSHA256 = fmt.Sprintf("%x", sha256.Sum256([]byte(in.Payload)))
				s.Started = time.Now()
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
				defer cancel()
				r, e := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: s.ID, TaskList: *tl, ExecutionStartToCloseTimeout: time.Minute, DecisionTaskStartToCloseTimeout: 10 * time.Second}, oneActivity, in)
				s.StartMS = float64(time.Since(s.Started)) / 1e6
				if e == nil {
					s.StartedOK = true
					s.RunID = r.GetRunID()
					var value string
					e = r.Get(ctx, &value)
					s.ObservedSHA256 = fmt.Sprintf("%x", sha256.Sum256([]byte(value)))
					s.Completed = time.Now()
					s.CompletionMS = float64(s.Completed.Sub(s.Scheduled)) / 1e6
					if e == nil && value != in.Payload {
						e = fmt.Errorf("wrong result")
					}
					if e == nil {
						e = history(ctx, rpc, *domain, &s)
					}
				}
				s.Correct = e == nil
				if e != nil {
					s.Error = e.Error()
				}
				emit(s)
			}(s)
		}
		if delay := time.Until(begin.Add(window)); delay > 0 {
			time.Sleep(delay)
		}
		wg.Wait()
		must(f.Sync())
	}
	phase("warmup", *warmup)
	if *startFile != "" {
		must(os.WriteFile(*startFile, []byte(time.Now().UTC().Format(time.RFC3339Nano)), 0644))
	}
	if *readyFile != "" {
		deadline := time.Now().Add(time.Minute)
		for {
			if _, err := os.Stat(*readyFile); err == nil {
				break
			}
			if time.Now().After(deadline) {
				panic("collector readiness timeout")
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	phase("measure", *duration)
	if *endFile != "" {
		must(os.WriteFile(*endFile, []byte(time.Now().UTC().Format(time.RFC3339Nano)), 0644))
	}
	if failed {
		os.Exit(1)
	}
}
func must(err error) {
	if err != nil {
		panic(err)
	}
}
