package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"
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
	if err := w.Start(); err != nil {
		panic(err)
	}
	defer w.Stop()
	c := client.NewClient(rpc, domain, nil)
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
	fmt.Println("Existing Go SDK echo, activity retry, child, workflow retry, workflow-task retry, activity timeout and signal passed through Go Frontend/History and Rust Matching")
}
