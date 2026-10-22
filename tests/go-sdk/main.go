// Compatibility driver uses the existing Cadence Go SDK over native TChannel.
// Default mode targets standalone Rust; mixed mode targets Go Frontend with Rust Matching.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"go.uber.org/cadence"
	"go.uber.org/cadence/.gen/go/cadence/workflowserviceclient"
	"go.uber.org/cadence/.gen/go/shared"
	"go.uber.org/cadence/activity"
	"go.uber.org/cadence/client"
	"go.uber.org/cadence/worker"
	"go.uber.org/cadence/workflow"
	"go.uber.org/thriftrw/protocol"
	"go.uber.org/thriftrw/wire"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/api/transport"
	"go.uber.org/yarpc/transport/tchannel"
)

func EchoActivity(_ context.Context, input string) (string, error) { return input, nil }
func SignalWorkflow(ctx workflow.Context) (string, error) {
	var value string
	workflow.GetSignalChannel(ctx, "wake").Receive(ctx, &value)
	return value, nil
}
func CancelSleepActivity(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(30 * time.Second):
		return nil
	}
}
func CancelWorkflow(ctx workflow.Context) (string, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{ScheduleToStartTimeout: time.Minute, StartToCloseTimeout: time.Minute})
	err := workflow.ExecuteActivity(ctx, CancelSleepActivity).Get(ctx, nil)
	if err != nil {
		return "", err
	}
	return "not-canceled", nil
}
func ChildWorkflow(workflow.Context) (string, error) { return "child-result", nil }
func ParentWorkflow(ctx workflow.Context) (string, error) {
	ctx = workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{ExecutionStartToCloseTimeout: time.Minute, TaskStartToCloseTimeout: 10 * time.Second})
	var result string
	err := workflow.ExecuteChildWorkflow(ctx, ChildWorkflow).Get(ctx, &result)
	return result, err
}
func RetryOnceActivity(ctx context.Context) error {
	if activity.GetInfo(ctx).Attempt == 0 {
		return fmt.Errorf("fail first attempt")
	}
	return nil
}
func RetryWorkflow(ctx workflow.Context) (string, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		ScheduleToStartTimeout: time.Minute,
		StartToCloseTimeout:    time.Minute,
		RetryPolicy:            &workflow.RetryPolicy{InitialInterval: time.Second, BackoffCoefficient: 1, MaximumAttempts: 3},
	})
	if err := workflow.ExecuteActivity(ctx, RetryOnceActivity).Get(ctx, nil); err != nil {
		return "", err
	}
	return "retried", nil
}

// Test-only worker fault injection: these panics exercise workflow-task retry,
// not workflow-run retry. Production workflow behavior is not changed.
var injectedWorkflowTaskFailures int32

func WorkflowTaskRetryWorkflow(workflow.Context) (string, error) {
	if atomic.AddInt32(&injectedWorkflowTaskFailures, 1) <= 2 {
		panic("injected workflow task failure")
	}
	return "workflow-task-recovered", nil
}

func ActivityTimeoutWorkflow(ctx workflow.Context) (string, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskList: "unpolled-activity-timeouts", ScheduleToStartTimeout: time.Second,
		StartToCloseTimeout: 30 * time.Second,
	})
	err := workflow.ExecuteActivity(ctx, EchoActivity, "timeout").Get(ctx, nil)
	if !cadence.IsTimeoutError(err) {
		return "", fmt.Errorf("expected activity timeout, got %v", err)
	}
	return "activity-timed-out", nil
}
func WorkflowRunRetry(ctx workflow.Context) (string, error) {
	if workflow.GetInfo(ctx).Attempt == 0 {
		return "", cadence.NewCustomError("workflow-first-attempt", "retry-details")
	}
	return "workflow-retried", nil
}
func EchoWorkflow(ctx workflow.Context, input string) (string, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{ScheduleToStartTimeout: time.Minute, StartToCloseTimeout: time.Minute})
	var result string
	err := workflow.ExecuteActivity(ctx, EchoActivity, input).Get(ctx, &result)
	return result, err
}
func ptr[T any](v T) *T { return &v }
func require(ok bool, message string) {
	if !ok {
		panic(message)
	}
}
func main() {
	peer := os.Getenv("CADENCE_TCHANNEL_PEER")
	if peer == "" {
		peer = "127.0.0.1:7933"
	}
	// Wait for the native listener when launched alongside a server restart.
	readyDeadline := time.Now().Add(30 * time.Second)
	for {
		connection, dialErr := net.DialTimeout("tcp", peer, time.Second)
		if dialErr == nil {
			connection.Close()
			break
		}
		if time.Now().After(readyDeadline) {
			panic(fmt.Errorf("Rust TChannel endpoint not ready: %w", dialErr))
		}
		time.Sleep(100 * time.Millisecond)
	}
	tr, err := tchannel.NewTransport(tchannel.ServiceName("cadence-rust-go-sdk-compat"))
	if err != nil {
		panic(err)
	}
	dispatcher := yarpc.NewDispatcher(yarpc.Config{Name: "cadence-rust-go-sdk-compat", Outbounds: yarpc.Outbounds{"cadence-frontend": {Unary: tr.NewSingleOutbound(peer)}}})
	if err = dispatcher.Start(); err != nil {
		panic(err)
	}
	defer dispatcher.Stop()
	rpc := workflowserviceclient.New(dispatcher.ClientConfig("cadence-frontend"))
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if domain := os.Getenv("CADENCE_MIXED_MATCHING_DOMAIN"); domain != "" {
		runMixedMatchingSDK(ctx, rpc, domain)
		return
	}
	// The Go server registers a separate, case-sensitive Meta::health Thrift API.
	healthResponse, err := dispatcher.ClientConfig("cadence-frontend").GetUnaryOutbound().Call(ctx, &transport.Request{Caller: "cadence-rust-go-sdk-compat", Service: "cadence-frontend", Procedure: "Meta::health", Encoding: "thrift", Body: bytes.NewReader([]byte{0})})
	if err != nil {
		panic(err)
	}
	healthBytes, err := io.ReadAll(healthResponse.Body)
	healthResponse.Body.Close()
	if err != nil {
		panic(err)
	}
	healthValue, err := protocol.Binary.Decode(bytes.NewReader(healthBytes), wire.TStruct)
	if err != nil {
		panic(err)
	}
	var healthy bool
	var healthMessage string
	for _, result := range healthValue.GetStruct().Fields {
		if result.ID == 0 {
			for _, field := range result.Value.GetStruct().Fields {
				if field.ID == 1 {
					healthy = field.Value.GetBool()
				}
				if field.ID == 2 {
					healthMessage = field.Value.GetString()
				}
			}
		}
	}
	require(healthy && healthMessage == "OK", "Go Meta::health wire/status mismatch")
	id := uuid.NewString()
	domain := "rust-tchannel-sdk-" + id
	taskList := "rust-tchannel-sdk-" + id
	dc := client.NewDomainClient(rpc, nil)
	err = dc.Register(ctx, &shared.RegisterDomainRequest{Name: ptr(domain), Description: ptr("Go SDK via Rust native TChannel"), WorkflowExecutionRetentionPeriodInDays: ptr(int32(3))})
	if err != nil {
		panic(err)
	}
	desc, err := rpc.DescribeDomain(ctx, &shared.DescribeDomainRequest{Name: ptr(domain)})
	if err != nil {
		panic(err)
	}
	require(desc.ReplicationConfiguration.GetActiveClusterName() != "", "default active cluster missing")
	require(len(desc.ReplicationConfiguration.Clusters) == 1, "default cluster list missing")
	updated, err := rpc.UpdateDomain(ctx, &shared.UpdateDomainRequest{Name: ptr(domain), Configuration: &shared.DomainConfiguration{EmitMetric: ptr(false)}})
	if err != nil {
		panic(err)
	}
	require(!updated.Configuration.GetEmitMetric(), "explicit false emitMetric was lost")
	for key, value := range map[string]string{"keep": "old", "next": "value"} {
		_, err = rpc.UpdateDomain(ctx, &shared.UpdateDomainRequest{Name: ptr(domain), UpdatedInfo: &shared.UpdateDomainInfo{Data: map[string]string{key: value}}})
		if err != nil {
			panic(err)
		}
	}
	_, err = rpc.UpdateDomain(ctx, &shared.UpdateDomainRequest{Name: ptr(domain), UpdatedInfo: &shared.UpdateDomainInfo{Description: ptr("must-rollback")}, Configuration: &shared.DomainConfiguration{WorkflowExecutionRetentionPeriodInDays: ptr(int32(0))}})
	require(err != nil, "zero retention update was accepted")
	desc, err = rpc.DescribeDomain(ctx, &shared.DescribeDomainRequest{Name: ptr(domain)})
	if err != nil {
		panic(err)
	}
	require(desc.DomainInfo.GetDescription() == "Go SDK via Rust native TChannel", "invalid update partially changed description")
	require(desc.DomainInfo.Data["keep"] == "old" && desc.DomainInfo.Data["next"] == "value", "domain data update replaced existing keys")
	require(!desc.Configuration.GetEmitMetric(), "omitted emitMetric reset explicit false")
	// One pending long poll must not block another call on the shared peer.
	pollCtx, pollCancel := context.WithTimeout(ctx, 2*time.Second)
	pollDone := make(chan error, 1)
	go func() {
		_, err := rpc.PollForDecisionTask(pollCtx, &shared.PollForDecisionTaskRequest{Domain: ptr(domain), TaskList: &shared.TaskList{Name: ptr("empty-task-list")}, Identity: ptr("concurrent-poll")})
		pollDone <- err
	}()
	time.Sleep(50 * time.Millisecond)
	fastCtx, fastCancel := context.WithTimeout(ctx, time.Second)
	attrs, err := rpc.GetSearchAttributes(fastCtx)
	fastCancel()
	pollCancel()
	if err != nil {
		panic(err)
	}
	require(attrs.Keys["CloseStatus"] == shared.IndexedValueTypeInt, "CloseStatus registry mismatch")
	<-pollDone

	w := worker.New(rpc, domain, taskList, worker.Options{DisableStickyExecution: true})
	w.RegisterWorkflow(EchoWorkflow)
	w.RegisterActivity(EchoActivity)
	w.RegisterActivity(RetryOnceActivity)
	w.RegisterWorkflow(RetryWorkflow)
	w.RegisterWorkflow(SignalWorkflow)
	w.RegisterWorkflow(ChildWorkflow)
	w.RegisterWorkflow(ParentWorkflow)
	w.RegisterWorkflow(WorkflowRunRetry)
	w.RegisterWorkflow(ActivityTimeoutWorkflow)
	w.RegisterWorkflow(WorkflowTaskRetryWorkflow)
	if err = w.Start(); err != nil {
		panic(err)
	}
	defer w.Stop()
	c := client.NewClient(rpc, domain, nil)
	// Forces request AND response fragmentation with the SDK's CRC32 checksums.
	input := strings.Repeat("opaque-payload-", 110000)
	run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: "echo-" + id, TaskList: taskList, ExecutionStartToCloseTimeout: 2 * time.Minute, DecisionTaskStartToCloseTimeout: time.Minute}, EchoWorkflow, input)
	if err != nil {
		panic(err)
	}
	var result string
	if err = run.Get(ctx, &result); err != nil {
		panic(err)
	}
	require(result == input, "SDK workflow result changed")
	retryRun, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: "retry-" + id, TaskList: taskList, ExecutionStartToCloseTimeout: 2 * time.Minute, DecisionTaskStartToCloseTimeout: time.Minute}, RetryWorkflow)
	if err != nil {
		panic(err)
	}
	var retryResult string
	if err = retryRun.Get(ctx, &retryResult); err != nil {
		panic(err)
	}
	require(retryResult == "retried", "SDK activity retry did not complete")
	retryHistory, err := rpc.GetWorkflowExecutionHistory(ctx, &shared.GetWorkflowExecutionHistoryRequest{Domain: ptr(domain), Execution: &shared.WorkflowExecution{WorkflowId: ptr(retryRun.GetID()), RunId: ptr(retryRun.GetRunID())}})
	if err != nil {
		panic(err)
	}
	var scheduled, started, failed int
	for _, event := range retryHistory.History.Events {
		switch event.GetEventType() {
		case shared.EventTypeActivityTaskScheduled:
			scheduled++
		case shared.EventTypeActivityTaskStarted:
			started++
		case shared.EventTypeActivityTaskFailed:
			failed++
		}
	}
	require(scheduled == 1 && started == 1 && failed == 0, fmt.Sprintf("retry history mismatch: scheduled=%d started=%d failed=%d", scheduled, started, failed))
	execution := &shared.WorkflowExecution{WorkflowId: ptr(run.GetID()), RunId: ptr(run.GetRunID())}
	wf, err := rpc.DescribeWorkflowExecution(ctx, &shared.DescribeWorkflowExecutionRequest{Domain: ptr(domain), Execution: execution})
	if err != nil {
		panic(err)
	}
	require(wf.WorkflowExecutionInfo.GetCloseStatus() == shared.WorkflowExecutionCloseStatusCompleted, "workflow not completed")
	require(time.Unix(0, wf.WorkflowExecutionInfo.GetStartTime()).After(time.Now().Add(-time.Minute)), "Thrift startTime must be Unix nanoseconds")
	require(wf.WorkflowExecutionInfo.GetTaskList() == taskList, "Thrift task list metadata missing")
	listed, err := rpc.ListWorkflowExecutions(ctx, &shared.ListWorkflowExecutionsRequest{Domain: ptr(domain), PageSize: ptr(int32(100)), Query: ptr("CloseStatus = 0")})
	if err != nil {
		panic(err)
	}
	require(len(listed.Executions) == 2, "SDK visibility query mismatch")
	closed, err := rpc.ListClosedWorkflowExecutions(ctx, &shared.ListClosedWorkflowExecutionsRequest{Domain: ptr(domain), MaximumPageSize: ptr(int32(100)), StartTimeFilter: &shared.StartTimeFilter{EarliestTime: ptr(int64(0)), LatestTime: ptr(time.Now().UnixNano())}})
	if err != nil {
		panic(err)
	}
	require(len(closed.Executions) == 2, "SDK legacy closed list mismatch")
	count, err := rpc.CountWorkflowExecutions(ctx, &shared.CountWorkflowExecutionsRequest{Domain: ptr(domain), Query: ptr("CloseStatus = 0")})
	if err != nil {
		panic(err)
	}
	require(count.GetCount() == 2, "SDK count mismatch")
	domains, err := rpc.ListDomains(ctx, &shared.ListDomainsRequest{PageSize: ptr(int32(1))})
	if err != nil {
		panic(err)
	}
	require(len(domains.Domains) == 1, "SDK domain page size ignored")
	if len(domains.NextPageToken) > 0 {
		_, err = rpc.ListDomains(ctx, &shared.ListDomainsRequest{PageSize: ptr(int32(1)), NextPageToken: domains.NextPageToken})
		if err != nil {
			panic(err)
		}
	}
	signalRun, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: "signal-" + id, TaskList: taskList, ExecutionStartToCloseTimeout: time.Minute, DecisionTaskStartToCloseTimeout: 10 * time.Second}, SignalWorkflow)
	if err != nil {
		panic(err)
	}
	// Wait until the workflow is idle, so a signal must enqueue a fresh task.
	for {
		info, err := rpc.DescribeWorkflowExecution(ctx, &shared.DescribeWorkflowExecutionRequest{Domain: ptr(domain), Execution: &shared.WorkflowExecution{WorkflowId: ptr(signalRun.GetID()), RunId: ptr(signalRun.GetRunID())}})
		if err != nil {
			panic(err)
		}
		if info.PendingDecision == nil && info.WorkflowExecutionInfo.GetHistoryLength() >= 4 {
			break
		}
		select {
		case <-ctx.Done():
			panic(ctx.Err())
		case <-time.After(50 * time.Millisecond):
		}
	}
	if err = c.SignalWorkflow(ctx, signalRun.GetID(), signalRun.GetRunID(), "wake", "awake"); err != nil {
		panic(err)
	}
	var signalResult string
	if err = signalRun.Get(ctx, &signalResult); err != nil {
		panic(err)
	}
	require(signalResult == "awake", "SDK signal did not wake idle workflow")
	parentRun, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: "parent-" + id, TaskList: taskList, ExecutionStartToCloseTimeout: time.Minute, DecisionTaskStartToCloseTimeout: 10 * time.Second}, ParentWorkflow)
	if err != nil {
		panic(err)
	}
	var childResult string
	if err = parentRun.Get(ctx, &childResult); err != nil {
		panic(err)
	}
	require(childResult == "child-result", "SDK parent/child workflow failed")
	// No worker polls this task list: the server must expire the workflow itself.
	workflowRetryRun, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID: "workflow-retry-" + id, TaskList: taskList,
		ExecutionStartToCloseTimeout: time.Minute, DecisionTaskStartToCloseTimeout: 10 * time.Second,
		RetryPolicy: &workflow.RetryPolicy{InitialInterval: time.Second, BackoffCoefficient: 2, MaximumAttempts: 2},
	}, WorkflowRunRetry)
	if err != nil {
		panic(err)
	}
	var workflowRetryResult string
	if err = workflowRetryRun.Get(ctx, &workflowRetryResult); err != nil {
		panic(err)
	}
	require(workflowRetryResult == "workflow-retried", "SDK workflow retry did not succeed")
	firstRetryHistory, err := rpc.GetWorkflowExecutionHistory(ctx, &shared.GetWorkflowExecutionHistoryRequest{
		Domain: ptr(domain), Execution: &shared.WorkflowExecution{WorkflowId: ptr(workflowRetryRun.GetID()), RunId: ptr(workflowRetryRun.GetRunID())},
	})
	if err != nil {
		panic(err)
	}
	firstRetryTerminal := firstRetryHistory.History.Events[len(firstRetryHistory.History.Events)-1]
	require(firstRetryTerminal.GetEventType() == shared.EventTypeWorkflowExecutionContinuedAsNew, "SDK failed run did not continue as new")
	require(firstRetryTerminal.WorkflowExecutionContinuedAsNewEventAttributes.GetInitiator() == shared.ContinueAsNewInitiatorRetryPolicy, "SDK workflow retry initiator missing")
	cronRun, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID: "cron-" + id, TaskList: taskList, CronSchedule: "@every 1s",
		ExecutionStartToCloseTimeout: time.Minute, DecisionTaskStartToCloseTimeout: 10 * time.Second,
	}, ChildWorkflow)
	if err != nil {
		panic(err)
	}
	var cronSuccessor string
	for tries := 0; tries < 50; tries++ {
		history, readErr := rpc.GetWorkflowExecutionHistory(ctx, &shared.GetWorkflowExecutionHistoryRequest{
			Domain: ptr(domain), Execution: &shared.WorkflowExecution{WorkflowId: ptr(cronRun.GetID()), RunId: ptr(cronRun.GetRunID())},
		})
		if readErr != nil {
			panic(readErr)
		}
		terminal := history.History.Events[len(history.History.Events)-1]
		if terminal.GetEventType() == shared.EventTypeWorkflowExecutionContinuedAsNew {
			require(terminal.WorkflowExecutionContinuedAsNewEventAttributes.GetInitiator() == shared.ContinueAsNewInitiatorCronSchedule, "SDK cron initiator missing")
			cronSuccessor = terminal.WorkflowExecutionContinuedAsNewEventAttributes.GetNewExecutionRunId()
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	require(cronSuccessor != "", "SDK successful cron run did not recur")
	cronHistory, err := rpc.GetWorkflowExecutionHistory(ctx, &shared.GetWorkflowExecutionHistoryRequest{
		Domain: ptr(domain), Execution: &shared.WorkflowExecution{WorkflowId: ptr(cronRun.GetID()), RunId: ptr(cronSuccessor)},
	})
	if err != nil {
		panic(err)
	}
	var cronPreviousResult string
	if err = json.Unmarshal(cronHistory.History.Events[0].WorkflowExecutionStartedEventAttributes.LastCompletionResult, &cronPreviousResult); err != nil {
		panic(err)
	}
	require(cronPreviousResult == "child-result", "SDK cron last completion result missing")
	if err = c.TerminateWorkflow(ctx, cronRun.GetID(), "", "compatibility check complete", nil); err != nil {
		panic(err)
	}
	activityTimeoutRun, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID: "activity-timeout-" + id, TaskList: taskList,
		ExecutionStartToCloseTimeout: time.Minute, DecisionTaskStartToCloseTimeout: 10 * time.Second,
	}, ActivityTimeoutWorkflow)
	if err != nil {
		panic(err)
	}
	var activityTimeoutResult string
	if err = activityTimeoutRun.Get(ctx, &activityTimeoutResult); err != nil {
		panic(err)
	}
	require(activityTimeoutResult == "activity-timed-out", "SDK activity timeout did not reach workflow")
	activityTimeoutHistory, err := rpc.GetWorkflowExecutionHistory(ctx, &shared.GetWorkflowExecutionHistoryRequest{
		Domain: ptr(domain), Execution: &shared.WorkflowExecution{WorkflowId: ptr(activityTimeoutRun.GetID()), RunId: ptr(activityTimeoutRun.GetRunID())},
	})
	if err != nil {
		panic(err)
	}
	foundActivityTimeout := false
	for _, event := range activityTimeoutHistory.History.Events {
		if event.GetEventType() == shared.EventTypeActivityTaskTimedOut {
			foundActivityTimeout = true
			require(event.ActivityTaskTimedOutEventAttributes.GetTimeoutType() == shared.TimeoutTypeScheduleToStart, "SDK activity timeout type mismatch")
		}
	}
	require(foundActivityTimeout, "SDK typed activity timeout history missing")
	taskRetryRun, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID: "task-retry-" + id, TaskList: taskList,
		ExecutionStartToCloseTimeout: time.Minute, DecisionTaskStartToCloseTimeout: time.Second,
	}, WorkflowTaskRetryWorkflow)
	if err != nil {
		panic(err)
	}
	var taskRetryResult string
	if err = taskRetryRun.Get(ctx, &taskRetryResult); err != nil {
		panic(err)
	}
	require(taskRetryResult == "workflow-task-recovered", "SDK workflow task retry did not recover")
	taskRetryHistory, err := rpc.GetWorkflowExecutionHistory(ctx, &shared.GetWorkflowExecutionHistoryRequest{
		Domain: ptr(domain), Execution: &shared.WorkflowExecution{WorkflowId: ptr(taskRetryRun.GetID()), RunId: ptr(taskRetryRun.GetRunID())},
	})
	if err != nil {
		panic(err)
	}
	var taskFailures, taskSchedules, taskStarts int
	for _, event := range taskRetryHistory.History.Events {
		switch event.GetEventType() {
		case shared.EventTypeDecisionTaskFailed:
			taskFailures++
		case shared.EventTypeDecisionTaskScheduled:
			taskSchedules++
		case shared.EventTypeDecisionTaskStarted:
			taskStarts++
		}
	}
	require(taskFailures == 1 && taskSchedules == 2 && taskStarts == 2,
		fmt.Sprintf("SDK transient retry history mismatch: failures=%d scheduled=%d started=%d", taskFailures, taskSchedules, taskStarts))

	taskTimeoutList := "task-timeout-" + id
	taskTimeoutRun, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID: "task-timeout-" + id, TaskList: taskTimeoutList,
		ExecutionStartToCloseTimeout: time.Minute, DecisionTaskStartToCloseTimeout: time.Second,
	}, ChildWorkflow)
	if err != nil {
		panic(err)
	}
	abandonedTask, err := rpc.PollForDecisionTask(ctx, &shared.PollForDecisionTaskRequest{
		Domain: ptr(domain), TaskList: &shared.TaskList{Name: ptr(taskTimeoutList)}, Identity: ptr("abandoned-worker"),
	})
	if err != nil {
		panic(err)
	}
	require(len(abandonedTask.TaskToken) > 0, "SDK expected workflow task to abandon")
	foundTaskTimeout := false
	for tries := 0; tries < 30; tries++ {
		history, readErr := rpc.GetWorkflowExecutionHistory(ctx, &shared.GetWorkflowExecutionHistoryRequest{
			Domain: ptr(domain), Execution: &shared.WorkflowExecution{WorkflowId: ptr(taskTimeoutRun.GetID()), RunId: ptr(taskTimeoutRun.GetRunID())},
		})
		if readErr != nil {
			panic(readErr)
		}
		for _, event := range history.History.Events {
			if event.GetEventType() == shared.EventTypeDecisionTaskTimedOut {
				foundTaskTimeout = true
			}
		}
		if foundTaskTimeout {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	require(foundTaskTimeout, "Rust did not expire abandoned workflow task")
	_, staleTaskErr := rpc.RespondDecisionTaskCompleted(ctx, &shared.RespondDecisionTaskCompletedRequest{TaskToken: abandonedTask.TaskToken})
	_, staleTaskMissing := staleTaskErr.(*shared.EntityNotExistsError)
	require(staleTaskMissing, fmt.Sprintf("stale task must return EntityNotExistsError, got %T %v", staleTaskErr, staleTaskErr))
	timeoutWorker := worker.New(rpc, domain, taskTimeoutList, worker.Options{DisableStickyExecution: true})
	timeoutWorker.RegisterWorkflow(ChildWorkflow)
	if err = timeoutWorker.Start(); err != nil {
		panic(err)
	}
	defer timeoutWorker.Stop()
	var taskTimeoutResult string
	if err = taskTimeoutRun.Get(ctx, &taskTimeoutResult); err != nil {
		panic(err)
	}
	require(taskTimeoutResult == "child-result", "SDK workflow task timeout retry did not recover")
	inlineList := "inline-" + id
	inlineRun, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID: "inline-" + id, TaskList: inlineList,
		ExecutionStartToCloseTimeout: time.Minute, DecisionTaskStartToCloseTimeout: 10 * time.Second,
	}, ChildWorkflow)
	if err != nil {
		panic(err)
	}
	inlineFirst, err := rpc.PollForDecisionTask(ctx, &shared.PollForDecisionTaskRequest{
		Domain: ptr(domain), TaskList: &shared.TaskList{Name: ptr(inlineList)}, Identity: ptr("inline-worker"),
	})
	if err != nil {
		panic(err)
	}
	inlineResponse, err := rpc.RespondDecisionTaskCompleted(ctx, &shared.RespondDecisionTaskCompletedRequest{
		TaskToken: inlineFirst.TaskToken, Identity: ptr("inline-worker"),
		ReturnNewDecisionTask: ptr(true), ForceCreateNewDecisionTask: ptr(true),
	})
	if err != nil {
		panic(err)
	}
	require(inlineResponse.DecisionTask != nil && len(inlineResponse.DecisionTask.TaskToken) > 0, "SDK inline workflow task missing")
	inlineTask := inlineResponse.DecisionTask
	require(inlineTask.GetPreviousStartedEventId() == inlineFirst.GetStartedEventId(), "SDK inline previous-started event mismatch")
	require(inlineTask.History.Events[0].GetEventId() == inlineFirst.GetStartedEventId()+1, "SDK inline history must be incremental")
	_, err = rpc.RespondDecisionTaskCompleted(ctx, &shared.RespondDecisionTaskCompletedRequest{
		TaskToken: inlineTask.TaskToken, Identity: ptr("inline-worker"), Decisions: []*shared.Decision{{
			DecisionType: ptr(shared.DecisionTypeCompleteWorkflowExecution),
			CompleteWorkflowExecutionDecisionAttributes: &shared.CompleteWorkflowExecutionDecisionAttributes{Result: []byte("\"inline-result\"")},
		}},
	})
	if err != nil {
		panic(err)
	}
	var inlineResult string
	if err = inlineRun.Get(ctx, &inlineResult); err != nil {
		panic(err)
	}
	require(inlineResult == "inline-result", "SDK inline task token could not complete workflow")
	stickyList := "sticky-" + id
	stickyWorker := worker.New(rpc, domain, stickyList, worker.Options{StickyScheduleToStartTimeout: time.Second})
	stickyWorker.RegisterWorkflow(EchoWorkflow)
	stickyWorker.RegisterWorkflow(SignalWorkflow)
	stickyWorker.RegisterActivity(EchoActivity)
	if err = stickyWorker.Start(); err != nil {
		panic(err)
	}
	defer stickyWorker.Stop()
	stickyRun, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID: "sticky-echo-" + id, TaskList: stickyList,
		ExecutionStartToCloseTimeout: time.Minute, DecisionTaskStartToCloseTimeout: 10 * time.Second,
	}, EchoWorkflow, "sticky-result")
	if err != nil {
		panic(err)
	}
	var stickyResult string
	if err = stickyRun.Get(ctx, &stickyResult); err != nil {
		panic(err)
	}
	require(stickyResult == "sticky-result", "SDK default sticky worker did not complete activity workflow")
	stickyHistory, err := rpc.GetWorkflowExecutionHistory(ctx, &shared.GetWorkflowExecutionHistoryRequest{
		Domain: ptr(domain), Execution: &shared.WorkflowExecution{WorkflowId: ptr(stickyRun.GetID()), RunId: ptr(stickyRun.GetRunID())},
	})
	if err != nil {
		panic(err)
	}
	stickyScheduled := false
	for _, event := range stickyHistory.History.Events {
		if event.GetEventType() == shared.EventTypeDecisionTaskScheduled && event.DecisionTaskScheduledEventAttributes.TaskList.GetName() != stickyList {
			stickyScheduled = true
		}
	}
	require(stickyScheduled, "SDK sticky owner queue was never used")
	stickySignalRun, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID: "sticky-signal-" + id, TaskList: stickyList,
		ExecutionStartToCloseTimeout: time.Minute, DecisionTaskStartToCloseTimeout: 10 * time.Second,
	}, SignalWorkflow)
	if err != nil {
		panic(err)
	}
	stickyIdle := false
	for tries := 0; tries < 50; tries++ {
		info, readErr := rpc.DescribeWorkflowExecution(ctx, &shared.DescribeWorkflowExecutionRequest{
			Domain: ptr(domain), Execution: &shared.WorkflowExecution{WorkflowId: ptr(stickySignalRun.GetID()), RunId: ptr(stickySignalRun.GetRunID())},
		})
		if readErr != nil {
			panic(readErr)
		}
		if info.PendingDecision == nil {
			stickyIdle = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	require(stickyIdle, "SDK sticky signal workflow never became idle")
	if err = c.SignalWorkflow(ctx, stickySignalRun.GetID(), stickySignalRun.GetRunID(), "wake", "sticky-awake"); err != nil {
		panic(err)
	}
	var stickySignalResult string
	if err = stickySignalRun.Get(ctx, &stickySignalResult); err != nil {
		panic(err)
	}
	require(stickySignalResult == "sticky-awake", "SDK cached sticky workflow did not receive signal")

	// An absent cached owner must time out to the normal queue; ResetSticky
	// must clear ownership without changing workflow history.
	for _, resetOwner := range []bool{false, true} {
		fallbackList := fmt.Sprintf("sticky-fallback-%t-%s", resetOwner, id)
		fallbackRun, startErr := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
			ID: fallbackList, TaskList: fallbackList,
			ExecutionStartToCloseTimeout: time.Minute, DecisionTaskStartToCloseTimeout: 10 * time.Second,
		}, ChildWorkflow)
		if startErr != nil {
			panic(startErr)
		}
		initial, pollErr := rpc.PollForDecisionTask(ctx, &shared.PollForDecisionTaskRequest{
			Domain: ptr(domain), TaskList: &shared.TaskList{Name: ptr(fallbackList)}, Identity: ptr("lost-sticky-worker"),
		})
		if pollErr != nil {
			panic(pollErr)
		}
		_, completeErr := rpc.RespondDecisionTaskCompleted(ctx, &shared.RespondDecisionTaskCompletedRequest{
			TaskToken: initial.TaskToken, StickyAttributes: &shared.StickyExecutionAttributes{
				WorkerTaskList: &shared.TaskList{Name: ptr("absent-owner-" + fallbackList)}, ScheduleToStartTimeoutSeconds: ptr(int32(1)),
			},
		})
		if completeErr != nil {
			panic(completeErr)
		}
		if resetOwner {
			_, resetErr := rpc.ResetStickyTaskList(ctx, &shared.ResetStickyTaskListRequest{
				Domain: ptr(domain), Execution: &shared.WorkflowExecution{WorkflowId: ptr(fallbackRun.GetID()), RunId: ptr(fallbackRun.GetRunID())},
			})
			if resetErr != nil {
				panic(resetErr)
			}
		}
		if err = c.SignalWorkflow(ctx, fallbackRun.GetID(), fallbackRun.GetRunID(), "wake", "fallback"); err != nil {
			panic(err)
		}
		fallbackTask, pollErr := rpc.PollForDecisionTask(ctx, &shared.PollForDecisionTaskRequest{
			Domain: ptr(domain), TaskList: &shared.TaskList{Name: ptr(fallbackList)}, Identity: ptr("replacement-worker"),
		})
		if pollErr != nil {
			panic(pollErr)
		}
		require(len(fallbackTask.TaskToken) > 0 && fallbackTask.History.Events[0].GetEventId() == 1, "SDK sticky fallback did not supply full replay history")
		foundStickyTimeout := false
		for _, event := range fallbackTask.History.Events {
			if event.GetEventType() == shared.EventTypeDecisionTaskTimedOut && event.DecisionTaskTimedOutEventAttributes.GetTimeoutType() == shared.TimeoutTypeScheduleToStart {
				foundStickyTimeout = true
			}
		}
		require(foundStickyTimeout != resetOwner, "SDK sticky timeout/reset behavior mismatch")
		_, completeErr = rpc.RespondDecisionTaskCompleted(ctx, &shared.RespondDecisionTaskCompletedRequest{
			TaskToken: fallbackTask.TaskToken, Decisions: []*shared.Decision{{DecisionType: ptr(shared.DecisionTypeCompleteWorkflowExecution),
				CompleteWorkflowExecutionDecisionAttributes: &shared.CompleteWorkflowExecutionDecisionAttributes{Result: []byte("\"fallback-result\"")}}},
		})
		if completeErr != nil {
			panic(completeErr)
		}
		var fallbackResult string
		if err = fallbackRun.Get(ctx, &fallbackResult); err != nil {
			panic(err)
		}
		require(fallbackResult == "fallback-result", "SDK fallback task could not complete")
	}
	timeoutRun, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: "timeout-" + id, TaskList: "unpolled-" + id, ExecutionStartToCloseTimeout: 2 * time.Second, DecisionTaskStartToCloseTimeout: time.Second}, EchoWorkflow, "timeout")
	if err != nil {
		panic(err)
	}
	err = timeoutRun.Get(ctx, nil)
	require(cadence.IsTimeoutError(err), fmt.Sprintf("SDK expected workflow timeout error, got %v", err))
	timeoutHistory, err := rpc.GetWorkflowExecutionHistory(ctx, &shared.GetWorkflowExecutionHistoryRequest{Domain: ptr(domain), Execution: &shared.WorkflowExecution{WorkflowId: ptr(timeoutRun.GetID()), RunId: ptr(timeoutRun.GetRunID())}})
	if err != nil {
		panic(err)
	}
	terminal := timeoutHistory.History.Events[len(timeoutHistory.History.Events)-1]
	require(terminal.GetEventType() == shared.EventTypeWorkflowExecutionTimedOut && terminal.WorkflowExecutionTimedOutEventAttributes.GetTimeoutType() == shared.TimeoutTypeStartToClose, "SDK timeout history mismatch")
	report := map[string]interface{}{"result": "passed", "transport": "tchannel", "peer": peer, "domain": domain, "workflowId": run.GetID(), "runId": run.GetRunID(), "payloadBytes": len(input), "historyEvents": wf.WorkflowExecutionInfo.GetHistoryLength(), "retryWorkflowId": retryRun.GetID(), "retryHistoryEvents": len(retryHistory.History.Events), "timeoutWorkflowId": timeoutRun.GetID(), "signalWorkflowId": signalRun.GetID(), "parentWorkflowId": parentRun.GetID(), "workflowRetryWorkflowId": workflowRetryRun.GetID(), "cronWorkflowId": cronRun.GetID(), "activityTimeoutWorkflowId": activityTimeoutRun.GetID(), "workflowTaskRetryId": taskRetryRun.GetID(), "workflowTaskTimeoutId": taskTimeoutRun.GetID(), "inlineWorkflowId": inlineRun.GetID(), "stickyWorkflowId": stickyRun.GetID(), "stickySignalWorkflowId": stickySignalRun.GetID()}
	b, _ := json.Marshal(report)
	fmt.Println(string(b))
}
