package thrift_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "github.com/ncruces/go-sqlite3/driver"
	_ "github.com/ncruces/go-sqlite3/embed"
	"github.com/uber/cadence/.gen/go/cadence/workflowserviceclient"
	"github.com/uber/cadence/.gen/go/health/metaclient"
	m "github.com/uber/cadence/.gen/go/matching"
	"github.com/uber/cadence/.gen/go/matching/matchingserviceclient"
	s "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/config"
	"github.com/uber/cadence/common/log"
	p "github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/persistence/serialization"
	sqlstore "github.com/uber/cadence/common/persistence/sql"
	_ "github.com/uber/cadence/common/persistence/sql/sqlplugin/sqlite"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/api/transport"
	yhttp "go.uber.org/yarpc/transport/http"
	"go.uber.org/yarpc/transport/tchannel"
)

func TestRustMatchingNativeThrift(t *testing.T) {
	if os.Getenv("CADENCE_MATCHING_TCHANNEL") == "" {
		t.Skip("requires native Rust Matching")
	}
	modes := []string{"tchannel", "http", "http-trailing-slash"}
	if os.Getenv("CADENCE_MATCHING_TCHANNEL_ONLY") == "1" {
		modes = []string{"tchannel"}
	}
	if os.Getenv("CADENCE_MATCHING_TLS_HTTP") != "" {
		modes = append(modes, "http-tls")
	}
	for _, mode := range modes {
		t.Run(mode, func(t *testing.T) {
			tr, err := tchannel.NewTransport(tchannel.ServiceName("matching-thrift-compat"))
			if err != nil {
				t.Fatal(err)
			}
			var outbound transport.UnaryOutbound = tr.NewSingleOutbound(os.Getenv("CADENCE_MATCHING_TCHANNEL"))
			if mode != "tchannel" {
				endpoint := os.Getenv("CADENCE_MATCHING_HTTP")
				if mode == "http-tls" {
					endpoint = os.Getenv("CADENCE_MATCHING_TLS_HTTP")
					root := x509.NewCertPool()
					certificate, err := os.ReadFile(os.Getenv("CADENCE_MATCHING_TLS_CERT"))
					if err != nil || !root.AppendCertsFromPEM(certificate) {
						t.Fatalf("load test TLS root: %v", err)
					}
					outbound = yhttp.NewTransport().NewSingleOutbound(endpoint, yhttp.OutboundTLSConfiguration(&tls.Config{RootCAs: root, ServerName: "localhost"}))
				} else {
					if mode == "http-trailing-slash" {
						endpoint += "/"
					}
					outbound = yhttp.NewTransport().NewSingleOutbound(endpoint)
				}
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
			// The Go handler/engine oracle records both absent and empty task-list
			// shapes as successful DescribeTaskList calls with no pollers.
			for _, request := range []*m.DescribeTaskListRequest{
				{DomainUUID: common.StringPtr(domainID)},
				{DomainUUID: common.StringPtr(domainID), DescRequest: &s.DescribeTaskListRequest{TaskList: &s.TaskList{}}},
			} {
				described, describeErr := matching.DescribeTaskList(ctx, request)
				pollerCount := -1
				if described != nil {
					pollerCount = len(described.GetPollers())
				}
				if describeErr != nil || pollerCount != 0 {
					t.Fatalf("empty DescribeTaskList shape: response=%v pollers=%d error=%T %v", described, pollerCount, describeErr, describeErr)
				}
			}
			// NewIdentifier rejects malformed reserved partition names before the
			// manager/domain lookup; the original Go oracle preserves this typed error.
			_, err = matching.DescribeTaskList(ctx, &m.DescribeTaskListRequest{
				DomainUUID: common.StringPtr(domainID),
				DescRequest: &s.DescribeTaskListRequest{TaskList: &s.TaskList{Name: common.StringPtr("/__cadence_sys//0")}},
			})
			var internal *s.InternalServiceError
			if !errors.As(err, &internal) || internal.GetMessage() != "invalid partitioned task list name /__cadence_sys//0" {
				t.Fatalf("malformed reserved task-list error: %T %v", err, err)
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
			activityDecision := func(id, input string) *s.Decision {
				return &s.Decision{DecisionType: s.DecisionTypeScheduleActivityTask.Ptr(), ScheduleActivityTaskDecisionAttributes: &s.ScheduleActivityTaskDecisionAttributes{
					ActivityId: common.StringPtr(id), ActivityType: &s.ActivityType{Name: common.StringPtr("echo")}, TaskList: list,
					Input: []byte(input), ScheduleToCloseTimeoutSeconds: common.Int32Ptr(30),
					ScheduleToStartTimeoutSeconds: common.Int32Ptr(30), StartToCloseTimeoutSeconds: common.Int32Ptr(30),
				}}
			}
			_, err = frontend.RespondDecisionTaskCompleted(ctx, &s.RespondDecisionTaskCompletedRequest{TaskToken: decision.TaskToken, Identity: common.StringPtr("thrift-worker"), Decisions: []*s.Decision{activityDecision("activity", "input"), activityDecision("go-queued-activity", "go-input")}})
			if err != nil {
				t.Fatal(err)
			}
			history, err := frontend.GetWorkflowExecutionHistory(ctx, &s.GetWorkflowExecutionHistoryRequest{Domain: common.StringPtr(domain), Execution: execution})
			if err != nil {
				t.Fatal(err)
			}
			scheduleIDs := make(map[string]int64)
			for _, event := range history.History.Events {
				if event.ActivityTaskScheduledEventAttributes != nil {
					scheduleIDs[event.ActivityTaskScheduledEventAttributes.GetActivityId()] = event.GetEventId()
				}
			}
			if scheduleIDs["activity"] == 0 || scheduleIDs["go-queued-activity"] == 0 {
				t.Fatalf("missing activity schedules: %v", scheduleIDs)
			}
			// Go owns the first lease and inserts task ID 2 before Rust opens this
			// activity queue. Rust then leases the next ID block and its local write
			// advances the bounded reader past both rows.
			if path := os.Getenv("CADENCE_MATCHING_SQLITE_DB"); path != "" {
				insertGoQueuedActivityTask(t, path, domainID, list.GetName(), workflowID, run.GetRunId(), scheduleIDs["go-queued-activity"])
			}
			err = matching.AddActivityTask(ctx, &m.AddActivityTaskRequest{DomainUUID: common.StringPtr(domainID), SourceDomainUUID: common.StringPtr(domainID), Execution: execution, TaskList: list, ScheduleId: common.Int64Ptr(scheduleIDs["activity"])})
			if err != nil {
				t.Fatal(err)
			}
			if path := os.Getenv("CADENCE_MATCHING_SQLITE_DB"); path != "" {
				assertRustQueuedActivityTaskReadable(t, path, domainID, list.GetName(), workflowID, run.GetRunId(), scheduleIDs["activity"])
			}
			if os.Getenv("CADENCE_MATCHING_SQLITE_DB") != "" {
				goActivity, err := matching.PollForActivityTask(ctx, &m.PollForActivityTaskRequest{DomainUUID: common.StringPtr(domainID), PollerID: common.StringPtr(uuid.NewString()), PollRequest: &s.PollForActivityTaskRequest{TaskList: list, Identity: common.StringPtr("go-activity-worker"), TaskListMetadata: &s.TaskListMetadata{MaxTasksPerSecond: common.Float64Ptr(50)}}})
				if err != nil || string(goActivity.Input) != "go-input" || goActivity.GetActivityId() != "go-queued-activity" {
					t.Fatalf("Go-enqueued activity delivery: %v %v", goActivity, err)
				}
			}
			activity, err := matching.PollForActivityTask(ctx, &m.PollForActivityTaskRequest{DomainUUID: common.StringPtr(domainID), PollerID: common.StringPtr(uuid.NewString()), PollRequest: &s.PollForActivityTaskRequest{TaskList: list, Identity: common.StringPtr("activity-worker"), TaskListMetadata: &s.TaskListMetadata{MaxTasksPerSecond: common.Float64Ptr(50)}}})
			if err != nil || string(activity.Input) != "input" || activity.GetActivityId() != "activity" {
				t.Fatalf("Rust-enqueued activity delivery: %v %v", activity, err)
			}
			if path := os.Getenv("CADENCE_MATCHING_SQLITE_DB"); path != "" {
				assertGoQueuedActivityTaskConsumed(t, path, domainID, list.GetName())
			}
			if verifyPath := os.Getenv("CADENCE_MATCHING_VERIFY_TASK_LIST"); verifyPath != "" {
				if err := os.WriteFile(verifyPath, []byte(domainID+"\n"+list.GetName()+"\n"), 0600); err != nil {
					t.Fatalf("write configured task-list verification input: %v", err)
				}
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

func insertGoQueuedActivityTask(t *testing.T, path, domainID, taskList, workflowID, runID string, scheduleID int64) {
	t.Helper()
	dc := p.NewDefaultDynamicConfiguration()
	parser, err := serialization.NewParser(dc)
	if err != nil {
		t.Fatal(err)
	}
	factory := sqlstore.NewFactory(config.SQL{PluginName: "sqlite", DatabaseName: path, NumShards: 1, MaxConns: 1, MaxIdleConns: 1, MaxConnLifetime: time.Hour}, "cluster0", log.NewNoop(), parser, dc)
	store, err := factory.NewTaskStore()
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	now := time.Now()
	metadata, err := store.LeaseTaskList(ctx, &p.LeaseTaskListRequest{
		DomainID: domainID, TaskList: taskList, TaskType: 1,
		CurrentTimeStamp: now,
	})
	if err != nil {
		t.Fatalf("Go TaskStore could not lease activity task list before Rust: %v", err)
	}
	if metadata.TaskListInfo.RangeID != 1 {
		t.Fatalf("expected Go to acquire the first activity-list lease, got range %d", metadata.TaskListInfo.RangeID)
	}
	_, err = store.CreateTasks(ctx, &p.CreateTasksRequest{
		TaskListInfo:     metadata.TaskListInfo,
		CurrentTimeStamp: now,
		Tasks: []*p.CreateTaskInfo{{TaskID: 2, Data: &p.TaskInfo{
			DomainID: domainID, WorkflowID: workflowID, RunID: runID, ScheduleID: scheduleID,
			ScheduleToStartTimeoutSeconds: 30,
		}}},
	})
	if err != nil {
		t.Fatalf("Go TaskStore could not enqueue activity task for Rust Matching: %v", err)
	}
	rows, err := store.GetTasks(ctx, &p.GetTasksRequest{DomainID: domainID, TaskList: taskList, TaskType: 1, ReadLevel: -1, BatchSize: 100})
	if err != nil {
		t.Fatalf("Go TaskStore could not verify seeded task ID 2: %v", err)
	}
	foundTaskID2 := false
	for _, row := range rows.Tasks {
		foundTaskID2 = foundTaskID2 || row.TaskID == 2
	}
	if !foundTaskID2 {
		t.Fatal("Go TaskStore did not persist seeded activity task ID 2")
	}
}

func assertRustQueuedActivityTaskReadable(t *testing.T, path, domainID, taskList, workflowID, runID string, scheduleID int64) {
	t.Helper()
	dc := p.NewDefaultDynamicConfiguration()
	parser, err := serialization.NewParser(dc)
	if err != nil {
		t.Fatal(err)
	}
	factory := sqlstore.NewFactory(config.SQL{PluginName: "sqlite", DatabaseName: path, NumShards: 1, MaxConns: 1, MaxIdleConns: 1, MaxConnLifetime: time.Hour}, "cluster0", log.NewNoop(), parser, dc)
	store, err := factory.NewTaskStore()
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	metadata, err := store.GetTaskList(ctx, &p.GetTaskListRequest{DomainID: domainID, TaskList: taskList, TaskType: 1})
	if err != nil || metadata.TaskListInfo == nil || metadata.TaskListInfo.RangeID != 2 {
		t.Fatalf("Go TaskStore could not read Rust's second activity-list lease: metadata=%v err=%v", metadata, err)
	}
	rows, err := store.GetTasks(ctx, &p.GetTasksRequest{DomainID: domainID, TaskList: taskList, TaskType: 1, ReadLevel: -1, BatchSize: 100})
	if err != nil {
		t.Fatalf("Go TaskStore could not read Rust-created activity task: %v", err)
	}
	for _, row := range rows.Tasks {
		if row.TaskID == 2 {
			continue
		}
		if row.WorkflowID == workflowID && row.RunID == runID && row.ScheduleID == scheduleID {
			if row.DomainID != domainID {
				t.Fatalf("Rust activity task has unexpected domain %q", row.DomainID)
			}
			return
		}
	}
	t.Fatalf("Go TaskStore did not find Rust-created activity task for workflow %q schedule %d; rows=%+v", workflowID, scheduleID, rows.Tasks)
}

func assertGoQueuedActivityTaskConsumed(t *testing.T, path, domainID, taskList string) {
	t.Helper()
	dc := p.NewDefaultDynamicConfiguration()
	parser, err := serialization.NewParser(dc)
	if err != nil {
		t.Fatal(err)
	}
	factory := sqlstore.NewFactory(config.SQL{PluginName: "sqlite", DatabaseName: path, NumShards: 1, MaxConns: 1, MaxIdleConns: 1, MaxConnLifetime: time.Hour}, "cluster0", log.NewNoop(), parser, dc)
	store, err := factory.NewTaskStore()
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	metadata, err := store.GetTaskList(context.Background(), &p.GetTaskListRequest{DomainID: domainID, TaskList: taskList, TaskType: 1})
	if err != nil || metadata.TaskListInfo.RangeID != 2 {
		t.Fatalf("Rust did not take the second activity-list lease before readback: metadata=%v err=%v", metadata, err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		tasks, err := store.GetTasks(context.Background(), &p.GetTasksRequest{DomainID: domainID, TaskList: taskList, TaskType: 1, ReadLevel: -1, BatchSize: 100})
		if err != nil {
			t.Fatalf("Go TaskStore could not inspect Rust's post-poll queue: %v", err)
		}
		goTaskRemains := false
		for _, task := range tasks.Tasks {
			goTaskRemains = goTaskRemains || task.TaskID == 2
		}
		if !goTaskRemains {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("Go-enqueued task ID 2 remains queued after Rust Matching delivered the distinct activity")
		}
		time.Sleep(25 * time.Millisecond)
	}
}

// TestRustMatchingDomainCacheFailoverProbe runs against the production Rust
// Matching process. The Python runner changes the Go SQL domain row while this
// worker poll is blocked; an ordinary metadata update must leave it blocked,
// while a global non-active-active failover must release it with an empty poll.
func TestRustMatchingDomainCacheFailoverProbe(t *testing.T) {
	address := os.Getenv("CADENCE_MATCHING_TCHANNEL")
	if address == "" {
		t.Skip("requires native Rust Matching")
	}
	tr, err := tchannel.NewTransport(tchannel.ServiceName("matching-domain-cache-probe"))
	if err != nil {
		t.Fatal(err)
	}
	dispatcher := yarpc.NewDispatcher(yarpc.Config{Name: "matching-domain-cache-probe", Outbounds: yarpc.Outbounds{
		"cadence-matching": {Unary: tr.NewSingleOutbound(address)},
	}})
	if err := dispatcher.Start(); err != nil {
		t.Fatal(err)
	}
	defer dispatcher.Stop()
	matching := matchingserviceclient.New(dispatcher.ClientConfig("cadence-matching"))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		response, err := matching.PollForDecisionTask(ctx, &m.PollForDecisionTaskRequest{
			DomainUUID: common.StringPtr("00000000-0000-0000-0000-000000000077"),
			PollerID:   common.StringPtr(uuid.NewString()),
			PollRequest: &s.PollForDecisionTaskRequest{
				TaskList: &s.TaskList{Name: common.StringPtr("cache-failover-probe"), Kind: s.TaskListKindNormal.Ptr()},
				Identity: common.StringPtr("domain-cache-probe"),
			},
		})
		if err == nil && len(response.GetTaskToken()) != 0 {
			err = fmt.Errorf("unexpected task delivered by failover probe: %x", response.GetTaskToken())
		}
		result <- err
	}()
	if ready := os.Getenv("CADENCE_MATCHING_FAILOVER_READY"); ready != "" {
		if err := os.WriteFile(ready, []byte("poll-started"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	failoverWritten := os.Getenv("CADENCE_MATCHING_FAILOVER_WRITTEN")
	if failoverWritten == "" {
		t.Fatal("CADENCE_MATCHING_FAILOVER_WRITTEN is required")
	}
	for {
		if _, err := os.Stat(failoverWritten); err == nil {
			break
		}
		select {
		case err := <-result:
			t.Fatalf("poll returned before Go failover update: %v", err)
		case <-ctx.Done():
			t.Fatalf("Go failover update did not run: %v", ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(18 * time.Second):
		t.Fatal("poll was not released by Go failover update")
	}
}

// Used by the production binary drain oracle: one generated-Go Thrift call
// must succeed after SIGTERM while matching.shutdownDrainDuration is active.
func TestRustMatchingDrainHealth(t *testing.T) {
	addr := os.Getenv("CADENCE_MATCHING_TCHANNEL")
	if addr == "" {
		t.Fatal("CADENCE_MATCHING_TCHANNEL is required")
	}
	tr, err := tchannel.NewTransport(tchannel.ServiceName("matching-drain-compat"))
	if err != nil {
		t.Fatal(err)
	}
	dispatcher := yarpc.NewDispatcher(yarpc.Config{Name: "matching-drain-compat", Outbounds: yarpc.Outbounds{
		"cadence-matching": {Unary: tr.NewSingleOutbound(addr)},
	}})
	if err := dispatcher.Start(); err != nil {
		t.Fatal(err)
	}
	defer dispatcher.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	health, err := metaclient.New(dispatcher.ClientConfig("cadence-matching")).Health(ctx)
	if err != nil || !health.GetOk() {
		t.Fatalf("Matching Meta::health during shutdown drain: %v %v", health, err)
	}
}

func TestRustMatchingTLS(t *testing.T) {
	endpoint := os.Getenv("CADENCE_MATCHING_TLS_HTTP")
	if endpoint == "" {
		t.Skip("requires TLS Matching listener")
	}
	tr := yhttp.NewTransport()
	root := x509.NewCertPool()
	certificate, err := os.ReadFile(os.Getenv("CADENCE_MATCHING_TLS_CERT"))
	if err != nil || !root.AppendCertsFromPEM(certificate) {
		t.Fatalf("load test TLS root: %v", err)
	}
	outbound := tr.NewSingleOutbound(endpoint, yhttp.OutboundTLSConfiguration(&tls.Config{RootCAs: root, ServerName: "localhost"}))
	dispatcher := yarpc.NewDispatcher(yarpc.Config{Name: "matching-tls-compat", Outbounds: yarpc.Outbounds{
		"cadence-matching": {Unary: outbound},
	}})
	if err := dispatcher.Start(); err != nil {
		t.Fatal(err)
	}
	defer dispatcher.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	health, err := metaclient.New(dispatcher.ClientConfig("cadence-matching")).Health(ctx)
	if err != nil || !health.GetOk() {
		t.Fatalf("TLS Meta::health: %v %v", health, err)
	}
	if plain := os.Getenv("CADENCE_MATCHING_PLAINTEXT_HTTP"); plain != "" {
		plainDispatcher := yarpc.NewDispatcher(yarpc.Config{Name: "matching-plaintext-compat", Outbounds: yarpc.Outbounds{
			"cadence-matching": {Unary: yhttp.NewTransport().NewSingleOutbound(plain)},
		}})
		if err := plainDispatcher.Start(); err != nil {
			t.Fatal(err)
		}
		defer plainDispatcher.Stop()
		health, err := metaclient.New(plainDispatcher.ClientConfig("cadence-matching")).Health(ctx)
		if err != nil || !health.GetOk() {
			t.Fatalf("plaintext Meta::health: %v %v", health, err)
		}
	}
	if os.Getenv("CADENCE_MATCHING_EXPECT_PLAINTEXT_REJECT") != "" {
		plain := "http://127.0.0.1:" + os.Getenv("CADENCE_MATCHING_PORT") + "/matching"
		plainDispatcher := yarpc.NewDispatcher(yarpc.Config{Name: "matching-plaintext-rejected", Outbounds: yarpc.Outbounds{
			"cadence-matching": {Unary: yhttp.NewTransport().NewSingleOutbound(plain)},
		}})
		if err := plainDispatcher.Start(); err != nil {
			t.Fatal(err)
		}
		defer plainDispatcher.Stop()
		if _, err := metaclient.New(plainDispatcher.ClientConfig("cadence-matching")).Health(ctx); err == nil {
			t.Fatal("enforced listener accepted plaintext YARPC call")
		}
	}
}

func TestRustMatchingPlainHTTP(t *testing.T) {
	endpoint := os.Getenv("CADENCE_MATCHING_PLAINTEXT_HTTP")
	if endpoint == "" {
		t.Skip("requires plaintext Matching listener")
	}
	dispatcher := yarpc.NewDispatcher(yarpc.Config{Name: "matching-plaintext-compat", Outbounds: yarpc.Outbounds{
		"cadence-matching": {Unary: yhttp.NewTransport().NewSingleOutbound(endpoint)},
	}})
	if err := dispatcher.Start(); err != nil {
		t.Fatal(err)
	}
	defer dispatcher.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	health, err := metaclient.New(dispatcher.ClientConfig("cadence-matching")).Health(ctx)
	if err != nil || !health.GetOk() {
		t.Fatalf("plaintext Meta::health: %v %v", health, err)
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
