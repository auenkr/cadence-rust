package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.uber.org/cadence"
	"go.uber.org/cadence/.gen/go/cadence/workflowserviceclient"
	"go.uber.org/cadence/.gen/go/shared"
	"go.uber.org/cadence/client"
	"go.uber.org/cadence/worker"
	"go.uber.org/cadence/workflow"
)

type childHistoryRef struct {
	workflowID string
	runID      string
}

// fetchHistoryDiagnostic pages one execution and returns references to children
// that actually started. Child histories are fetched separately and never
// recursively expanded.
func fetchHistoryDiagnostic(ctx context.Context, rpc workflowserviceclient.Interface, domain, workflowID, runID string, findChildren bool) (string, []childHistoryRef) {
	if runID == "" {
		description, err := rpc.DescribeWorkflowExecution(ctx, &shared.DescribeWorkflowExecutionRequest{
			Domain: &domain, Execution: &shared.WorkflowExecution{WorkflowId: &workflowID},
		})
		if err != nil {
			return fmt.Sprintf("history diagnostic could not resolve run ID for workflow %s: %v", workflowID, err), nil
		}
		runID = description.WorkflowExecutionInfo.GetExecution().GetRunId()
	}
	request := &shared.GetWorkflowExecutionHistoryRequest{Domain: &domain,
		Execution: &shared.WorkflowExecution{WorkflowId: &workflowID, RunId: &runID}}
	counts := make(map[string]int)
	total, pages := 0, 0
	seenTokens := make(map[string]bool)
	var children []childHistoryRef
	for {
		response, err := rpc.GetWorkflowExecutionHistory(ctx, request)
		if err != nil {
			return fmt.Sprintf("history diagnostic failed for workflow %s/%s after %d events/%d pages: %v", workflowID, runID, total, pages, err), children
		}
		pages++
		if response.History != nil {
			for _, event := range response.History.Events {
				counts[event.GetEventType().String()]++
				total++
				encoded, encodeErr := json.Marshal(event)
				if encodeErr != nil {
					fmt.Printf("history event workflow=%s/%s eventID=%d encodeError=%v\n", workflowID, runID, event.GetEventId(), encodeErr)
				} else {
					fmt.Printf("history event workflow=%s/%s event=%s\n", workflowID, runID, encoded)
				}
				if findChildren && event.GetChildWorkflowExecutionStartedEventAttributes() != nil {
					attrs := event.GetChildWorkflowExecutionStartedEventAttributes()
					children = append(children, childHistoryRef{workflowID: attrs.GetWorkflowExecution().GetWorkflowId(), runID: attrs.GetWorkflowExecution().GetRunId()})
				}
			}
		}
		if len(response.NextPageToken) == 0 {
			return fmt.Sprintf("history workflow=%s/%s events=%d pages=%d eventTypes=%v", workflowID, runID, total, pages, counts), children
		}
		token := string(response.NextPageToken)
		if seenTokens[token] {
			return fmt.Sprintf("history diagnostic repeated page token for workflow %s/%s after %d events/%d pages eventTypes=%v", workflowID, runID, total, pages, counts), children
		}
		seenTokens[token] = true
		request.NextPageToken = response.NextPageToken
	}
}

// retryHistoryDiagnostic deliberately uses one fresh, five-second budget for
// the parent and a small number of already-started child histories. The suite's
// context is commonly expired by the time run.Get reports its deadline.
func retryHistoryDiagnostic(rpc workflowserviceclient.Interface, domain, workflowID, runID string) string {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	parent, children := fetchHistoryDiagnostic(ctx, rpc, domain, workflowID, runID, true)
	const maxChildHistories = 2
	limit := len(children)
	if limit > maxChildHistories {
		limit = maxChildHistories
	}
	diagnostics := []string{parent}
	for _, child := range children[:limit] {
		if child.workflowID == "" || child.runID == "" {
			diagnostics = append(diagnostics, fmt.Sprintf("child history skipped: incomplete execution reference workflow=%q run=%q", child.workflowID, child.runID))
			continue
		}
		detail, _ := fetchHistoryDiagnostic(ctx, rpc, domain, child.workflowID, child.runID, false)
		diagnostics = append(diagnostics, detail)
	}
	if ctx.Err() != nil {
		diagnostics = append(diagnostics, fmt.Sprintf("child history diagnostic budget exhausted after %s: %v", 5*time.Second, ctx.Err()))
	}
	if len(children) > limit {
		diagnostics = append(diagnostics, fmt.Sprintf("child history diagnostics truncated: fetched=%d started=%d limit=%d", limit, len(children), maxChildHistories))
	}
	return strings.Join(diagnostics, "; ")
}

func mixedRunGetError(rpc workflowserviceclient.Interface, domain, scenario string, run interface {
	GetID() string
	GetRunID() string
}, err error) error {
	workflowID, runID := run.GetID(), run.GetRunID()
	diagnostic := retryHistoryDiagnostic(rpc, domain, workflowID, runID)
	return fmt.Errorf("%s workflow %s/%s failed: %w; %s", scenario, workflowID, runID, err, diagnostic)
}

func remaining(ctx context.Context) time.Duration {
	deadline, ok := ctx.Deadline()
	if !ok {
		return 0
	}
	if time.Until(deadline) < 0 {
		return 0
	}
	return time.Until(deadline).Round(time.Millisecond)
}

// The domain is provisioned by the mixed-service harness. Every worker poll and
// task append crosses original Go Frontend/History and Rust Matching RPCs.
func runMixedMatchingSDK(ctx context.Context, rpc workflowserviceclient.Interface, domain string) {
	if os.Getenv("CADENCE_MIXED_ACTIVITY_CRASH_SEED") == "1" {
		workflowTaskList := os.Getenv("CADENCE_MIXED_ACTIVITY_CRASH_WORKFLOW_TASK_LIST")
		activityTaskList := os.Getenv("CADENCE_MIXED_ACTIVITY_CRASH_ACTIVITY_TASK_LIST")
		workflowID := os.Getenv("CADENCE_MIXED_ACTIVITY_CRASH_WORKFLOW_ID")
		w := worker.New(rpc, domain, workflowTaskList, worker.Options{DisableStickyExecution: true})
		w.RegisterWorkflow(ActivityCrashRecoveryWorkflow)
		if err := w.Start(); err != nil {
			panic(err)
		}
		defer w.Stop()
		run, err := client.NewClient(rpc, domain, nil).ExecuteWorkflow(ctx, client.StartWorkflowOptions{
			ID: workflowID, TaskList: workflowTaskList, ExecutionStartToCloseTimeout: 5 * time.Minute,
			DecisionTaskStartToCloseTimeout: time.Minute,
		}, ActivityCrashRecoveryWorkflow, activityTaskList, "activity-crash-recovered")
		if err != nil {
			panic(err)
		}
		fmt.Printf("activity-crash seed started workflowID=%s runID=%s workflowTaskList=%s activityTaskList=%s\n",
			run.GetID(), run.GetRunID(), workflowTaskList, activityTaskList)
		var result string
		if err := run.Get(ctx, &result); err != nil {
			panic(mixedRunGetError(rpc, domain, "activity-crash-seed", run, err))
		}
		panic("activity crash seed unexpectedly completed before the recovery worker started")
	}
	if workflowID := os.Getenv("CADENCE_MIXED_ACTIVITY_CRASH_RECOVERY_WORKFLOW_ID"); workflowID != "" {
		workflowTaskList := os.Getenv("CADENCE_MIXED_ACTIVITY_CRASH_WORKFLOW_TASK_LIST")
		activityTaskList := os.Getenv("CADENCE_MIXED_ACTIVITY_CRASH_ACTIVITY_TASK_LIST")
		workflowWorker := worker.New(rpc, domain, workflowTaskList, worker.Options{DisableStickyExecution: true})
		workflowWorker.RegisterWorkflow(ActivityCrashRecoveryWorkflow)
		activityWorker := worker.New(rpc, domain, activityTaskList, worker.Options{DisableStickyExecution: true})
		activityWorker.RegisterActivity(EchoActivity)
		if err := workflowWorker.Start(); err != nil {
			panic(err)
		}
		defer workflowWorker.Stop()
		if err := activityWorker.Start(); err != nil {
			panic(err)
		}
		defer activityWorker.Stop()
		var result string
		run := client.NewClient(rpc, domain, nil).GetWorkflow(ctx, workflowID, "")
		if err := run.Get(ctx, &result); err != nil {
			panic(mixedRunGetError(rpc, domain, "activity-crash-recovery", run, err))
		}
		require(result == "activity-crash-recovered", "activity crash recovery SDK result changed")
		fmt.Printf("Existing Go SDK recovered queued activity after Rust Matching owner SIGKILL; workflowID=%s result=%s\n", workflowID, result)
		return
	}
	if workflowID := os.Getenv("CADENCE_MIXED_RESTART_WORKFLOW_ID"); workflowID != "" {
		taskList := os.Getenv("CADENCE_MIXED_RESTART_TASK_LIST")
		w := worker.New(rpc, domain, taskList, worker.Options{DisableStickyExecution: true})
		w.RegisterWorkflow(EchoWorkflow)
		w.RegisterActivity(EchoActivity)
		if err := w.Start(); err != nil {
			panic(err)
		}
		defer w.Stop()
		run := client.NewClient(rpc, domain, nil).GetWorkflow(ctx, workflowID, "")
		var result string
		if err := run.Get(ctx, &result); err != nil {
			panic(err)
		}
		require(result == "restart-proof", "restart SDK workflow result changed")
		fmt.Println("Existing Go SDK resumed and completed the pre-restart workflow/activity")
		return
	}
	taskList := os.Getenv("CADENCE_MIXED_MATCHING_TASK_LIST")
	if taskList == "" {
		taskList = "mixed-sdk-" + uuid.NewString()
	}
	w := worker.New(rpc, domain, taskList, worker.Options{DisableStickyExecution: true})
	w.RegisterWorkflow(EchoWorkflow)
	w.RegisterActivity(EchoActivity)
	w.RegisterActivity(RetryOnceActivity)
	w.RegisterWorkflow(RetryWorkflow)
	w.RegisterWorkflow(ParentWorkflow)
	w.RegisterWorkflow(ChildWorkflow)
	w.RegisterWorkflow(WorkflowRunRetry)
	w.RegisterWorkflow(WorkflowTaskRetryWorkflow)
	w.RegisterWorkflow(ActivityTimeoutWorkflow)
	w.RegisterWorkflow(SignalWorkflow)
	w.RegisterWorkflow(CancelWorkflow)
	w.RegisterActivity(CancelSleepActivity)
	if err := w.Start(); err != nil {
		panic(err)
	}
	defer w.Stop()
	c := client.NewClient(rpc, domain, nil)
	if os.Getenv("CADENCE_MIXED_MATCHING_RETRY_ONLY") == "1" {
		started := time.Now()
		fmt.Printf("scenario start name=workflow-task-retry-only workflowID=%s taskList=%s suiteBudgetRemaining=%s\n", taskList+"-workflow-task-retry-only", taskList, remaining(ctx))
		run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: taskList + "-workflow-task-retry-only", TaskList: taskList, ExecutionStartToCloseTimeout: time.Minute, DecisionTaskStartToCloseTimeout: 10 * time.Second}, WorkflowTaskRetryWorkflow)
		if err != nil {
			fmt.Printf("scenario end name=workflow-task-retry-only workflowID=%s outcome=start-error duration=%s suiteBudgetRemaining=%s error=%v\n", taskList+"-workflow-task-retry-only", time.Since(started), remaining(ctx), err)
			panic(fmt.Errorf("workflow-task-retry-only start failed for %s: %w", taskList+"-workflow-task-retry-only", err))
		}
		var result string
		if err := run.Get(ctx, &result); err != nil {
			fmt.Printf("scenario end name=workflow-task-retry-only workflowID=%s runID=%s outcome=error duration=%s suiteBudgetRemaining=%s\n", run.GetID(), run.GetRunID(), time.Since(started), remaining(ctx))
			panic(mixedRunGetError(rpc, domain, "workflow-task-retry-only", run, err))
		}
		require(result == "workflow-task-recovered", "mixed Matching retry-only result changed")
		fmt.Println(retryHistoryDiagnostic(rpc, domain, run.GetID(), run.GetRunID()))
		fmt.Printf("scenario end name=workflow-task-retry-only workflowID=%s runID=%s duration=%s suiteBudgetRemaining=%s\n", run.GetID(), run.GetRunID(), time.Since(started), remaining(ctx))
		return
	}
	if loadJSON := os.Getenv("CADENCE_MIXED_MATCHING_LOAD_IDS"); loadJSON != "" {
		var ids []string
		if err := json.Unmarshal([]byte(loadJSON), &ids); err != nil {
			panic(err)
		}
		for _, id := range ids {
			started := time.Now()
			fmt.Printf("scenario start name=adaptive-load workflowID=%s suiteBudgetRemaining=%s\n", id, remaining(ctx))
			var value string
			run := c.GetWorkflow(ctx, id, "")
			if err := run.Get(ctx, &value); err != nil {
				fmt.Printf("scenario end name=adaptive-load workflowID=%s runID=%s outcome=error duration=%s suiteBudgetRemaining=%s\n", run.GetID(), run.GetRunID(), time.Since(started), remaining(ctx))
				panic(mixedRunGetError(rpc, domain, "adaptive-load", run, err))
			}
			require(value == "adaptive-load", "adaptive scaler load result changed")
			fmt.Printf("scenario end name=adaptive-load workflowID=%s runID=%s duration=%s suiteBudgetRemaining=%s\n", run.GetID(), run.GetRunID(), time.Since(started), remaining(ctx))
		}
		fmt.Printf("Completed %d adaptive scaler load workflows on %s\n", len(ids), taskList)
	}
	run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: taskList, TaskList: taskList, ExecutionStartToCloseTimeout: time.Minute, DecisionTaskStartToCloseTimeout: 10 * time.Second}, EchoWorkflow, "sdk-through-rust-matching")
	if err != nil {
		panic(err)
	}
	var result string
	if err := run.Get(ctx, &result); err != nil {
		panic(err)
	}
	require(result == "sdk-through-rust-matching", "mixed Matching SDK result changed")
	for _, scenario := range []struct {
		name     string
		workflow interface{}
		want     string
	}{
		{"activity-retry", RetryWorkflow, "retried"},
		{"child", ParentWorkflow, "child-result"},
		{"workflow-run-retry", WorkflowRunRetry, "workflow-retried"},
		{"workflow-task-retry", WorkflowTaskRetryWorkflow, "workflow-task-recovered"},
		{"activity-timeout", ActivityTimeoutWorkflow, "activity-timed-out"},
	} {
		started := time.Now()
		workflowID := taskList + "-" + scenario.name
		fmt.Printf("scenario start name=%s workflowID=%s suiteBudgetRemaining=%s\n", scenario.name, workflowID, remaining(ctx))
		run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: workflowID, TaskList: taskList, ExecutionStartToCloseTimeout: time.Minute, DecisionTaskStartToCloseTimeout: 10 * time.Second, RetryPolicy: &workflow.RetryPolicy{InitialInterval: time.Second, MaximumAttempts: 3}}, scenario.workflow)
		if err != nil {
			fmt.Printf("scenario end name=%s workflowID=%s outcome=start-error duration=%s suiteBudgetRemaining=%s error=%v\n", scenario.name, workflowID, time.Since(started), remaining(ctx), err)
			panic(err)
		}
		var result string
		if err := run.Get(ctx, &result); err != nil {
			fmt.Printf("scenario end name=%s workflowID=%s runID=%s outcome=error duration=%s suiteBudgetRemaining=%s\n", scenario.name, run.GetID(), run.GetRunID(), time.Since(started), remaining(ctx))
			panic(mixedRunGetError(rpc, domain, scenario.name, run, err))
		}
		require(result == scenario.want, "mixed Matching scenario result changed: "+scenario.name)
		fmt.Printf("scenario end name=%s workflowID=%s runID=%s duration=%s suiteBudgetRemaining=%s\n", scenario.name, run.GetID(), run.GetRunID(), time.Since(started), remaining(ctx))
	}
	signal, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: taskList + "-signal", TaskList: taskList, ExecutionStartToCloseTimeout: time.Minute, DecisionTaskStartToCloseTimeout: 10 * time.Second}, SignalWorkflow)
	if err != nil {
		panic(err)
	}
	if err := c.SignalWorkflow(ctx, signal.GetID(), signal.GetRunID(), "wake", "mixed-signal"); err != nil {
		panic(err)
	}
	var signaled string
	if err := signal.Get(ctx, &signaled); err != nil {
		panic(err)
	}
	require(signaled == "mixed-signal", "mixed Matching signal result changed")
	// Cancellation: request cancel while the workflow has a pending activity; Go ends the run Canceled.
	cancelRun, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: taskList + "-cancel", TaskList: taskList, ExecutionStartToCloseTimeout: time.Minute, DecisionTaskStartToCloseTimeout: 10 * time.Second}, CancelWorkflow)
	if err != nil {
		panic(err)
	}
	time.Sleep(2 * time.Second)
	if err := c.CancelWorkflow(ctx, cancelRun.GetID(), cancelRun.GetRunID()); err != nil {
		panic(err)
	}
	err = cancelRun.Get(ctx, nil)
	var canceled *cadence.CanceledError
	require(errors.As(err, &canceled), fmt.Sprintf("cancel workflow did not end Canceled: %v", err))
	desc, err := c.DescribeWorkflowExecution(ctx, cancelRun.GetID(), cancelRun.GetRunID())
	if err != nil {
		panic(err)
	}
	require(desc.WorkflowExecutionInfo.GetCloseStatus().String() == "CANCELED", "cancel workflow close status is not CANCELED")
	fmt.Println("Existing Go SDK workflow cancellation ended Canceled through Rust Matching")
	fmt.Println("Existing Go SDK echo, activity retry, child, workflow retry, workflow-task retry, activity timeout and signal passed through Go Frontend/History and Rust Matching")
}
