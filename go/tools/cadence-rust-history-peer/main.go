// Command cadence-rust-history-peer is a generated-Go HistoryService
// endpoint for exercising the Rust Matching TChannel client in mixed-cluster
// tests. It intentionally embeds the server interface and implements only
// the five History methods used by Matching.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"os"
	"time"

	history "github.com/uber/cadence/.gen/go/history"
	"github.com/uber/cadence/.gen/go/history/historyserviceserver"
	"github.com/uber/cadence/.gen/go/shared"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/transport/tchannel"
)

type peer struct {
	historyserviceserver.Interface
	mode string
}

func (p *peer) observe(ctx context.Context) {
	c := yarpc.CallFromContext(ctx)
	_ = json.NewEncoder(os.Stdout).Encode(map[string]string{
		"caller": c.Caller(), "service": c.Service(), "transport": c.Transport(),
		"procedure": c.Procedure(), "encoding": string(c.Encoding()),
	})
}

func (p *peer) RefreshWorkflowTasks(ctx context.Context, request *history.RefreshWorkflowTasksRequest) error {
	p.observe(ctx)
	if request.GetDomainUIID() != "domain-id" || request.Request == nil || request.Request.GetDomain() != "domain" || request.Request.Execution == nil || request.Request.Execution.GetWorkflowId() != "workflow-id" || request.Request.Execution.GetRunId() != "run-id" {
		panic("RefreshWorkflowTasks request fields did not survive Rust Thrift mapping")
	}
	if p.mode == "exception" || p.mode == "redirect" {
		return &history.ShardOwnershipLostError{Owner: stringPtr("history-b:7935"), Message: stringPtr("wrong owner")}
	}
	return nil
}

func (p *peer) DescribeWorkflowExecution(ctx context.Context, request *history.DescribeWorkflowExecutionRequest) (*shared.DescribeWorkflowExecutionResponse, error) {
	p.observe(ctx)
	if request.GetDomainUUID() != "domain-id" || request.Request == nil || request.Request.GetDomain() != "domain" || request.Request.Execution == nil || request.Request.Execution.GetWorkflowId() != "workflow-id" || request.Request.Execution.GetRunId() != "run-id" {
		panic("DescribeWorkflowExecution request fields did not survive Rust Thrift mapping")
	}
	if p.mode == "exception" || p.mode == "redirect" {
		return nil, &history.ShardOwnershipLostError{Owner: stringPtr("history-b:7935"), Message: stringPtr("wrong owner")}
	}
	return &shared.DescribeWorkflowExecutionResponse{PendingDecision: &shared.PendingDecisionInfo{Attempt: int64Ptr(3), ScheduleID: int64Ptr(41)}}, nil
}

func (p *peer) GetMutableState(ctx context.Context, request *history.GetMutableStateRequest) (*history.GetMutableStateResponse, error) {
	p.observe(ctx)
	if request.GetDomainUUID() != "domain-id" || request.Execution == nil || request.Execution.GetWorkflowId() != "workflow-id" || request.Execution.GetRunId() != "run-id" || request.GetExpectedNextEventId() != 9 || string(request.CurrentBranchToken) != "input-branch" {
		panic("GetMutableState request fields did not survive Rust Thrift mapping")
	}
	if p.mode == "typed" {
		return nil, &shared.EntityNotExistsError{Message: "mutable missing", ActiveClusters: []string{}}
	}
	return &history.GetMutableStateResponse{NextEventId: int64Ptr(17), ClientLibraryVersion: stringPtr("go-peer"), CurrentBranchToken: []byte("branch")}, nil
}

func (p *peer) RecordDecisionTaskStarted(ctx context.Context, request *history.RecordDecisionTaskStartedRequest) (*history.RecordDecisionTaskStartedResponse, error) {
	p.observe(ctx)
	if request.GetDomainUUID() != "domain-id" || request.WorkflowExecution == nil || request.WorkflowExecution.GetWorkflowId() != "workflow-id" || request.WorkflowExecution.GetRunId() != "run-id" || request.GetScheduleId() != 51 || request.GetTaskId() != 52 || request.GetRequestId() != "decision-request" {
		panic("RecordDecisionTaskStarted request fields did not survive Rust Thrift mapping")
	}
	if p.mode == "typed" {
		return nil, &history.EventAlreadyStartedError{Message: "decision already started"}
	}
	return &history.RecordDecisionTaskStartedResponse{ScheduledEventId: int64Ptr(21), StartedEventId: int64Ptr(22), NextEventId: int64Ptr(23), Attempt: int64Ptr(2), BranchToken: []byte("decision")}, nil
}

func (p *peer) RecordActivityTaskStarted(ctx context.Context, request *history.RecordActivityTaskStartedRequest) (*history.RecordActivityTaskStartedResponse, error) {
	p.observe(ctx)
	if request.GetDomainUUID() != "domain-id" || request.WorkflowExecution == nil || request.WorkflowExecution.GetWorkflowId() != "workflow-id" || request.WorkflowExecution.GetRunId() != "run-id" || request.GetScheduleId() != 61 || request.GetTaskId() != 62 || request.GetRequestId() != "activity-request" {
		panic("RecordActivityTaskStarted request fields did not survive Rust Thrift mapping")
	}
	if p.mode == "typed" {
		return nil, &shared.EntityNotExistsError{Message: "activity missing", ActiveClusters: []string{}}
	}
	return &history.RecordActivityTaskStartedResponse{Attempt: int64Ptr(4), HeartbeatDetails: []byte("beat"), WorkflowDomain: stringPtr("go-domain")}, nil
}

func stringPtr(v string) *string { return &v }
func int64Ptr(v int64) *int64    { return &v }

func main() {
	address := flag.String("address", "127.0.0.1:7935", "TChannel listen address")
	mode := flag.String("mode", "success", "success, exception, or redirect")
	flag.Parse()
	listener, err := net.Listen("tcp", *address)
	if err != nil {
		panic(err)
	}
	if err := listener.Close(); err != nil {
		panic(err)
	}
	ch, err := tchannel.NewChannelTransport(tchannel.ServiceName("cadence-history"), tchannel.ListenAddr(*address))
	if err != nil {
		panic(err)
	}
	dispatcher := yarpc.NewDispatcher(yarpc.Config{
		Name:     "cadence-history",
		Inbounds: yarpc.Inbounds{ch.NewInbound()},
	})
	dispatcher.Register(historyserviceserver.New(&peer{mode: *mode}))
	if err := dispatcher.Start(); err != nil {
		panic(err)
	}
	fmt.Printf("READY %s\n", *address)
	for {
		time.Sleep(time.Hour)
	}
}
