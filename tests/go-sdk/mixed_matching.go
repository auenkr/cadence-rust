package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"
	"go.uber.org/cadence"
	"go.uber.org/cadence/.gen/go/cadence/workflowserviceclient"
	"go.uber.org/cadence/client"
	"go.uber.org/cadence/worker"
	"go.uber.org/cadence/workflow"
)

// The domain is provisioned by the mixed-service harness. Every worker poll and
// task append crosses original Go Frontend/History and Rust Matching RPCs.
func runMixedMatchingSDK(ctx context.Context, rpc workflowserviceclient.Interface, domain string) {
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
	taskList := "mixed-sdk-" + uuid.NewString()
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
	if loadJSON := os.Getenv("CADENCE_MIXED_MATCHING_LOAD_IDS"); loadJSON != "" {
		var ids []string
		if err := json.Unmarshal([]byte(loadJSON), &ids); err != nil {
			panic(err)
		}
		for _, id := range ids {
			var value string
			if err := c.GetWorkflow(ctx, id, "").Get(ctx, &value); err != nil {
				panic(err)
			}
			require(value == "adaptive-load", "adaptive scaler load result changed")
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
		run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: taskList + "-" + scenario.name, TaskList: taskList, ExecutionStartToCloseTimeout: time.Minute, DecisionTaskStartToCloseTimeout: 10 * time.Second, RetryPolicy: &workflow.RetryPolicy{InitialInterval: time.Second, MaximumAttempts: 3}}, scenario.workflow)
		if err != nil {
			panic(err)
		}
		var result string
		if err := run.Get(ctx, &result); err != nil {
			panic(fmt.Errorf("%s: %w", scenario.name, err))
		}
		require(result == scenario.want, "mixed Matching scenario result changed: "+scenario.name)
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
