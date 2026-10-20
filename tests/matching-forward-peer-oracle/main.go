// Minimal generated-YARPC Matching peer used by the Rust forwarding oracle.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"time"

	api "github.com/uber/cadence-idl/go/proto/api/v1"
	matching "github.com/uber/cadence/.gen/proto/matching/v1"
	"go.uber.org/yarpc"
	grpc "go.uber.org/yarpc/transport/grpc"
)

type peer struct {
	matching.MatchingAPIYARPCServer
}

func observe(ctx context.Context) string {
	call := yarpc.CallFromContext(ctx)
	deadline, hasDeadline := ctx.Deadline()
	result := fmt.Sprintf("%s|%s|%s|%s|%t", call.Service(), call.Caller(), call.Header("rpc-service"), call.Header("rpc-caller"), hasDeadline)
	result += "|" + call.Header("rpc-encoding")
	for key, value := range map[string]string{
		"observed-service":      call.Service(),
		"observed-caller":       call.Caller(),
		"observed-encoding":     string(call.Encoding()),
		"observed-rpc-service":  call.Header("rpc-service"),
		"observed-rpc-caller":   call.Header("rpc-caller"),
		"observed-rpc-encoding": call.Header("rpc-encoding"),
	} {
		_ = call.WriteResponseHeader(key, value)
	}
	if hasDeadline {
		_ = call.WriteResponseHeader("observed-deadline", deadline.UTC().Format(time.RFC3339Nano))
	}
	return result
}

func (peer) PollForDecisionTask(ctx context.Context, _ *matching.PollForDecisionTaskRequest) (*matching.PollForDecisionTaskResponse, error) {
	observed := observe(ctx)
	return &matching.PollForDecisionTaskResponse{
		TaskToken: []byte("go-yarpc-decision|" + observed), StartedEventId: 17,
		WorkflowExecution: &api.WorkflowExecution{WorkflowId: "go-peer-workflow", RunId: "go-peer-run"},
	}, nil
}

func (peer) PollForActivityTask(ctx context.Context, _ *matching.PollForActivityTaskRequest) (*matching.PollForActivityTaskResponse, error) {
	observed := observe(ctx)
	return &matching.PollForActivityTaskResponse{
		TaskToken: []byte("go-yarpc-activity|" + observed), ActivityId: "go-peer-activity",
		WorkflowExecution: &api.WorkflowExecution{WorkflowId: "go-peer-workflow", RunId: "go-peer-run"},
	}, nil
}

func (peer) AddDecisionTask(ctx context.Context, _ *matching.AddDecisionTaskRequest) (*matching.AddDecisionTaskResponse, error) {
	observe(ctx)
	return &matching.AddDecisionTaskResponse{PartitionConfig: &matching.TaskListPartitionConfig{Version: 19}}, nil
}

func (peer) AddActivityTask(ctx context.Context, _ *matching.AddActivityTaskRequest) (*matching.AddActivityTaskResponse, error) {
	observe(ctx)
	return &matching.AddActivityTaskResponse{PartitionConfig: &matching.TaskListPartitionConfig{Version: 23}}, nil
}

func (peer) QueryWorkflow(ctx context.Context, _ *matching.QueryWorkflowRequest) (*matching.QueryWorkflowResponse, error) {
	observe(ctx)
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
