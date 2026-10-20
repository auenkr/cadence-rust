// Minimal generated-YARPC Matching peer used by the Rust forwarding oracle.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	api "github.com/uber/cadence-idl/go/proto/api/v1"
	matching "github.com/uber/cadence/.gen/proto/matching/v1"
	"go.uber.org/yarpc"
	grpc "go.uber.org/yarpc/transport/grpc"
)

type peer struct {
	matching.MatchingAPIYARPCServer
}

var observationLock sync.Mutex

func observe(ctx context.Context, method string, fields map[string]string) string {
	call := yarpc.CallFromContext(ctx)
	deadline, hasDeadline := ctx.Deadline()
	encoding := string(call.Encoding())
	result := fmt.Sprintf("%s|%s|%t|%s", call.Service(), call.Caller(), hasDeadline, encoding)
	if path := os.Getenv("CADENCE_MATCHING_GO_PEER_LOG"); path != "" {
		observationLock.Lock()
		defer observationLock.Unlock()
		file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
		if err == nil {
			_ = json.NewEncoder(file).Encode(map[string]interface{}{
				"method":      method,
				"rpcService":  call.Service(),
				"rpcCaller":   call.Caller(),
				"rpcEncoding": encoding,
				"deadline":    hasDeadline,
				"fields":      fields,
			})
			_ = file.Close()
		}
	}
	for key, value := range map[string]string{
		"observed-service":      call.Service(),
		"observed-caller":       call.Caller(),
		"observed-encoding":     string(call.Encoding()),
		"observed-rpc-service":  call.Service(),
		"observed-rpc-caller":   call.Caller(),
		"observed-rpc-encoding": encoding,
	} {
		_ = call.WriteResponseHeader(key, value)
	}
	if hasDeadline {
		_ = call.WriteResponseHeader("observed-deadline", deadline.UTC().Format(time.RFC3339Nano))
	}
	return result
}

func (peer) PollForDecisionTask(ctx context.Context, request *matching.PollForDecisionTaskRequest) (*matching.PollForDecisionTaskResponse, error) {
	fields := map[string]string{"domainId": request.DomainId, "pollerId": request.PollerId, "taskList": request.Request.GetTaskList().GetName(), "forwardedFrom": request.ForwardedFrom}
	observed := observe(ctx, "pollDecision", fields)
	return &matching.PollForDecisionTaskResponse{
		TaskToken: []byte("go-yarpc-decision|" + observed), StartedEventId: 17,
		WorkflowExecution: &api.WorkflowExecution{WorkflowId: "go-peer-workflow", RunId: "go-peer-run"},
	}, nil
}

func (peer) PollForActivityTask(ctx context.Context, request *matching.PollForActivityTaskRequest) (*matching.PollForActivityTaskResponse, error) {
	fields := map[string]string{"domainId": request.DomainId, "pollerId": request.PollerId, "taskList": request.Request.GetTaskList().GetName(), "forwardedFrom": request.ForwardedFrom}
	observed := observe(ctx, "pollActivity", fields)
	return &matching.PollForActivityTaskResponse{
		TaskToken: []byte("go-yarpc-activity|" + observed), ActivityId: "go-peer-activity",
		WorkflowExecution: &api.WorkflowExecution{WorkflowId: "go-peer-workflow", RunId: "go-peer-run"},
	}, nil
}

func (peer) AddDecisionTask(ctx context.Context, request *matching.AddDecisionTaskRequest) (*matching.AddDecisionTaskResponse, error) {
	observe(ctx, "addDecision", map[string]string{"domainId": request.DomainId, "workflowId": request.WorkflowExecution.GetWorkflowId(), "taskList": request.TaskList.GetName(), "forwardedFrom": request.ForwardedFrom})
	return &matching.AddDecisionTaskResponse{PartitionConfig: &matching.TaskListPartitionConfig{Version: 19}}, nil
}

func (peer) AddActivityTask(ctx context.Context, request *matching.AddActivityTaskRequest) (*matching.AddActivityTaskResponse, error) {
	observe(ctx, "addActivity", map[string]string{"domainId": request.DomainId, "workflowId": request.WorkflowExecution.GetWorkflowId(), "taskList": request.TaskList.GetName(), "forwardedFrom": request.ForwardedFrom})
	return &matching.AddActivityTaskResponse{PartitionConfig: &matching.TaskListPartitionConfig{Version: 23}}, nil
}

func (peer) QueryWorkflow(ctx context.Context, request *matching.QueryWorkflowRequest) (*matching.QueryWorkflowResponse, error) {
	observe(ctx, "query", map[string]string{"domainId": request.DomainId, "taskList": request.TaskList.GetName(), "forwardedFrom": request.ForwardedFrom})
	return &matching.QueryWorkflowResponse{QueryResult: &api.Payload{Data: []byte("go-yarpc-query")}}, nil
}

func main() {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	transport := grpc.NewTransport()
	dispatcher := yarpc.NewDispatcher(yarpc.Config{
		Name:     "cadence-matching",
		Inbounds: yarpc.Inbounds{transport.NewInbound(listener)},
	})
	dispatcher.Register(matching.BuildMatchingAPIYARPCProcedures(peer{}))
	if err := dispatcher.Start(); err != nil {
		panic(err)
	}
	defer dispatcher.Stop()
	if err := json.NewEncoder(os.Stdout).Encode(map[string]string{"address": listener.Addr().String()}); err != nil {
		panic(err)
	}
	select {}
}
