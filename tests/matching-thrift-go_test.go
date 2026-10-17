package thrift_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/uber/cadence/.gen/go/cadence/workflowserviceclient"
	"github.com/uber/cadence/.gen/go/health/metaclient"
	m "github.com/uber/cadence/.gen/go/matching"
	"github.com/uber/cadence/.gen/go/matching/matchingserviceclient"
	s "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/api/transport"
	yhttp "go.uber.org/yarpc/transport/http"
	"go.uber.org/yarpc/transport/tchannel"
)

func TestRustMatchingNativeThrift(t *testing.T) {
	if os.Getenv("CADENCE_MATCHING_TCHANNEL") == "" {
		t.Skip("requires native Rust Matching")
	}
	for _, mode := range []string{"tchannel", "http"} {
		t.Run(mode, func(t *testing.T) {
			tr, err := tchannel.NewTransport(tchannel.ServiceName("matching-thrift-compat"))
			if err != nil {
				t.Fatal(err)
			}
			var outbound transport.UnaryOutbound = tr.NewSingleOutbound(os.Getenv("CADENCE_MATCHING_TCHANNEL"))
			if mode == "http" {
				outbound = yhttp.NewTransport().NewSingleOutbound(os.Getenv("CADENCE_MATCHING_HTTP"))
			}
			dispatcher := yarpc.NewDispatcher(yarpc.Config{Name: "matching-thrift-compat", Outbounds: yarpc.Outbounds{
				"cadence-matching": {Unary: outbound}, "cadence-frontend": {Unary: tr.NewSingleOutbound(os.Getenv("CADENCE_FRONTEND_TCHANNEL"))},
			}})
			if err := dispatcher.Start(); err != nil {
				t.Fatal(err)
			}
			defer dispatcher.Stop()
			matching := matchingserviceclient.New(dispatcher.ClientConfig("cadence-matching"))
			frontend := workflowserviceclient.New(dispatcher.ClientConfig("cadence-frontend"))
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			health, err := metaclient.New(dispatcher.ClientConfig("cadence-matching")).Health(ctx)
			if err != nil || !health.GetOk() {
				t.Fatalf("health: %v %v", health, err)
			}
			domain := "thrift-" + uuid.NewString()
			if err := frontend.RegisterDomain(ctx, &s.RegisterDomainRequest{Name: common.StringPtr(domain), WorkflowExecutionRetentionPeriodInDays: common.Int32Ptr(3)}); err != nil {
				t.Fatal(err)
			}
			described, err := frontend.DescribeDomain(ctx, &s.DescribeDomainRequest{Name: common.StringPtr(domain)})
			if err != nil {
				t.Fatal(err)
			}
			domainID := described.DomainInfo.GetUUID()
			list := &s.TaskList{Name: common.StringPtr("tasks-" + uuid.NewString()), Kind: s.TaskListKindNormal.Ptr()}
			// These original generated clients check declared exception ids.
			_, err = matching.DescribeTaskList(ctx, &m.DescribeTaskListRequest{DomainUUID: common.StringPtr(domainID)})
			var bad *s.BadRequestError
			if !errors.As(err, &bad) {
				t.Fatalf("bad request type: %T %v", err, err)
			}
			sticky := &s.TaskList{Name: common.StringPtr("sticky"), Kind: s.TaskListKindSticky.Ptr()}
			err = matching.AddDecisionTask(ctx, &m.AddDecisionTaskRequest{DomainUUID: common.StringPtr(domainID), TaskList: sticky, Execution: &s.WorkflowExecution{WorkflowId: common.StringPtr("wf"), RunId: common.StringPtr(uuid.NewString())}, ScheduleId: common.Int64Ptr(2)})
			var unavailable *s.StickyWorkerUnavailableError
			if !errors.As(err, &unavailable) {
				t.Fatalf("sticky error: %T %v", err, err)
			}
			workflowID := uuid.NewString()
			run, err := frontend.StartWorkflowExecution(ctx, &s.StartWorkflowExecutionRequest{Domain: common.StringPtr(domain), WorkflowId: common.StringPtr(workflowID), WorkflowType: &s.WorkflowType{Name: common.StringPtr("thrift-test")}, TaskList: list, ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(60), TaskStartToCloseTimeoutSeconds: common.Int32Ptr(10), RequestId: common.StringPtr(uuid.NewString())})
			if err != nil {
				t.Fatal(err)
			}
			execution := &s.WorkflowExecution{WorkflowId: common.StringPtr(workflowID), RunId: run.RunId}

			err = matching.AddDecisionTask(ctx, &m.AddDecisionTaskRequest{DomainUUID: common.StringPtr(domainID), Execution: execution, TaskList: list, ScheduleId: common.Int64Ptr(2), ScheduleToStartTimeoutSeconds: common.Int32Ptr(30)})
			if err != nil {
				t.Fatal(err)
			}
			decision, err := matching.PollForDecisionTask(ctx, &m.PollForDecisionTaskRequest{DomainUUID: common.StringPtr(domainID), PollerID: common.StringPtr(uuid.NewString()), PollRequest: &s.PollForDecisionTaskRequest{TaskList: list, Identity: common.StringPtr("thrift-worker")}})
			if err != nil || len(decision.TaskToken) == 0 || decision.GetStartedEventId() == 0 || decision.WorkflowExecution.GetRunId() != run.GetRunId() {
				t.Fatalf("decision delivery: %v %v", decision, err)
			}
			_, err = frontend.RespondDecisionTaskCompleted(ctx, &s.RespondDecisionTaskCompletedRequest{TaskToken: decision.TaskToken, Identity: common.StringPtr("thrift-worker"), Decisions: []*s.Decision{{DecisionType: s.DecisionTypeScheduleActivityTask.Ptr(), ScheduleActivityTaskDecisionAttributes: &s.ScheduleActivityTaskDecisionAttributes{ActivityId: common.StringPtr("activity"), ActivityType: &s.ActivityType{Name: common.StringPtr("echo")}, TaskList: list, Input: []byte("input"), ScheduleToCloseTimeoutSeconds: common.Int32Ptr(30), ScheduleToStartTimeoutSeconds: common.Int32Ptr(30), StartToCloseTimeoutSeconds: common.Int32Ptr(30)}}}})
			if err != nil {
				t.Fatal(err)
			}
			history, err := frontend.GetWorkflowExecutionHistory(ctx, &s.GetWorkflowExecutionHistoryRequest{Domain: common.StringPtr(domain), Execution: execution})
			if err != nil {
				t.Fatal(err)
			}
			var scheduleID int64
			for _, event := range history.History.Events {
				if event.ActivityTaskScheduledEventAttributes != nil {
					scheduleID = event.GetEventId()
				}
			}
			if scheduleID == 0 {
				t.Fatal("missing activity schedule")
			}
			err = matching.AddActivityTask(ctx, &m.AddActivityTaskRequest{DomainUUID: common.StringPtr(domainID), SourceDomainUUID: common.StringPtr(domainID), Execution: execution, TaskList: list, ScheduleId: common.Int64Ptr(scheduleID)})
			if err != nil {
				t.Fatal(err)
			}
			activity, err := matching.PollForActivityTask(ctx, &m.PollForActivityTaskRequest{DomainUUID: common.StringPtr(domainID), PollerID: common.StringPtr(uuid.NewString()), PollRequest: &s.PollForActivityTaskRequest{TaskList: list, Identity: common.StringPtr("activity-worker"), TaskListMetadata: &s.TaskListMetadata{MaxTasksPerSecond: common.Float64Ptr(50)}}})
			if err != nil || string(activity.Input) != "input" || activity.GetActivityId() != "activity" {
				t.Fatalf("activity delivery: %v %v", activity, err)
			}
			lists, err := matching.GetTaskListsByDomain(ctx, &s.GetTaskListsByDomainRequest{DomainName: common.StringPtr(domain)})
			if err != nil || lists.DecisionTaskListMap[list.GetName()] == nil || lists.ActivityTaskListMap[list.GetName()] == nil {
				t.Fatalf("loaded managers: %v %v", lists, err)
			}
			partitions, err := matching.ListTaskListPartitions(ctx, &m.ListTaskListPartitionsRequest{Domain: common.StringPtr(domain), TaskList: list})
			if err != nil || len(partitions.DecisionTaskListPartitions) != 1 || partitions.DecisionTaskListPartitions[0].GetKey() != list.GetName() {
				t.Fatalf("partitions: %v %v", partitions, err)
			}
			queryList := &s.TaskList{Name: common.StringPtr("queries-" + uuid.NewString()), Kind: s.TaskListKindNormal.Ptr()}
			pollID := uuid.NewString()
			pollDone := make(chan *m.PollForDecisionTaskResponse, 1)
			pollErrors := make(chan error, 1)
			go func() {
				v, e := matching.PollForDecisionTask(ctx, &m.PollForDecisionTaskRequest{DomainUUID: common.StringPtr(domainID), PollerID: common.StringPtr(pollID), PollRequest: &s.PollForDecisionTaskRequest{TaskList: queryList, Identity: common.StringPtr("query-worker")}})
				if e != nil {
					pollErrors <- e
				} else {
					pollDone <- v
				}
			}()
			waitForPoller(t, ctx, matching, domainID, queryList)
			result := make(chan *s.QueryWorkflowResponse, 1)
			queryErrors := make(chan error, 1)
			go func() {
				v, e := matching.QueryWorkflow(ctx, &m.QueryWorkflowRequest{DomainUUID: common.StringPtr(domainID), TaskList: queryList, QueryRequest: &s.QueryWorkflowRequest{Domain: common.StringPtr(domain), Execution: execution, Query: &s.WorkflowQuery{QueryType: common.StringPtr("ping")}}})
				if e != nil {
					queryErrors <- e
				} else {
					result <- v
				}
			}()
			var query *m.PollForDecisionTaskResponse
			select {
			case query = <-pollDone:
			case e := <-pollErrors:
				t.Fatal(e)
			case e := <-queryErrors:
				t.Fatal(e)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if query.Query.GetQueryType() != "ping" {
				t.Fatal("query payload changed")
			}
			var token struct {
				TaskID   string `json:"TaskID"`
				TaskList string `json:"TaskList"`
			}
			if err := json.Unmarshal(query.TaskToken, &token); err != nil {
				t.Fatal(err)
			}
			if token.TaskList != queryList.GetName() {
				t.Fatal("query completion partition changed")
			}
			err = matching.RespondQueryTaskCompleted(ctx, &m.RespondQueryTaskCompletedRequest{DomainUUID: common.StringPtr(domainID), TaskList: queryList, TaskID: common.StringPtr(token.TaskID), CompletedRequest: &s.RespondQueryTaskCompletedRequest{CompletedType: s.QueryTaskCompletedTypeCompleted.Ptr(), QueryResult: []byte("pong")}})
			if err != nil {
				t.Fatal(err)
			}
			select {
			case v := <-result:
				if string(v.QueryResult) != "pong" {
					t.Fatal("query result changed")
				}
			case e := <-queryErrors:
				t.Fatal(e)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			cancelList := &s.TaskList{Name: common.StringPtr("cancel-" + uuid.NewString())}
			cancelID := uuid.NewString()
			cancelDone := make(chan error, 1)
			go func() {
				_, e := matching.PollForActivityTask(ctx, &m.PollForActivityTaskRequest{DomainUUID: common.StringPtr(domainID), PollerID: common.StringPtr(cancelID), PollRequest: &s.PollForActivityTaskRequest{TaskList: cancelList, Identity: common.StringPtr("cancel-worker")}})
				cancelDone <- e
			}()
			for {
				info, e := matching.DescribeTaskList(ctx, &m.DescribeTaskListRequest{DomainUUID: common.StringPtr(domainID), DescRequest: &s.DescribeTaskListRequest{TaskList: cancelList, TaskListType: s.TaskListTypeActivity.Ptr()}})
				if e != nil {
					t.Fatal(e)
				}
				if len(info.Pollers) > 0 {
					break
				}
				select {
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				case <-time.After(5 * time.Millisecond):
				}
			}
			if err := matching.CancelOutstandingPoll(ctx, &m.CancelOutstandingPollRequest{DomainUUID: common.StringPtr(domainID), TaskList: cancelList, TaskListType: common.Int32Ptr(1), PollerID: common.StringPtr(cancelID)}); err != nil {
				t.Fatal(err)
			}
			select {
			case <-cancelDone:
			case <-time.After(time.Second):
				t.Fatal("cancel did not release poll")
			}
			// Go's empty-poll tailroom is one second ahead of the transport TTL.
			short, stop := context.WithTimeout(ctx, 1200*time.Millisecond)
			defer stop()
			empty, err := matching.PollForDecisionTask(short, &m.PollForDecisionTaskRequest{DomainUUID: common.StringPtr(domainID), PollerID: common.StringPtr(uuid.NewString()), PollRequest: &s.PollForDecisionTaskRequest{TaskList: &s.TaskList{Name: common.StringPtr("empty-" + uuid.NewString())}}})
			if err != nil || len(empty.TaskToken) != 0 {
				t.Fatalf("empty poll deadline: %v %v", empty, err)
			}
			fmt.Println("Verified all ten native Matching Thrift methods via", mode)
		})
	}
}
func waitForPoller(t *testing.T, ctx context.Context, matching matchingserviceclient.Interface, domain string, list *s.TaskList) {
	t.Helper()
	for {
		info, e := matching.DescribeTaskList(ctx, &m.DescribeTaskListRequest{DomainUUID: common.StringPtr(domain), DescRequest: &s.DescribeTaskListRequest{TaskList: list, TaskListType: s.TaskListTypeDecision.Ptr()}})
		if e != nil {
			t.Fatal(e)
		}
		if len(info.Pollers) > 0 {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(5 * time.Millisecond):
		}
	}
}
