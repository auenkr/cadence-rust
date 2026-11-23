package main

import (
	"time"
	"context"
	"fmt"

	"github.com/uber/cadence/.gen/go/cadence/workflowserviceclient"
	"github.com/uber/cadence/.gen/go/shared"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/transport/http"
)

func main() {
	h := http.NewTransport()
	outbound := h.NewSingleOutbound("http://127.0.0.1:8080")
	dispatcher := yarpc.NewDispatcher(yarpc.Config{
		Name:      "client",
		Outbounds: yarpc.Outbounds{"cadence-frontend": {Unary: outbound}},
	})
	if err := dispatcher.Start(); err != nil { panic(err) }
	defer dispatcher.Stop()

	c := workflowserviceclient.New(dispatcher.ClientConfig("cadence-frontend"))
	reg := &shared.RegisterDomainRequest{
		Name:                                strptr("samples-domain"),
		Description:                         strptr("d"),
		OwnerEmail:                          strptr("o@e.com"),
		WorkflowExecutionRetentionPeriodInDays: int32ptr(7),
		ActiveClusterName:                    strptr("active"),
		Clusters: []*shared.ClusterReplicationConfiguration{{ClusterName: strptr("active")}},
		EmitMetric:                          boolptr(true),
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	if err := c.RegisterDomain(ctx, reg); err != nil {
		fmt.Println("Register err:", err)
	} else {
		fmt.Println("Registered domain")
	}
	start := &shared.StartWorkflowExecutionRequest{
		Domain:                              strptr("samples-domain"),
		WorkflowId:                          strptr("test-workflow"),
		WorkflowType:                        &shared.WorkflowType{Name: strptr("HelloWorkflow")},
		TaskList:                            &shared.TaskList{Name: strptr("tl")},
		Identity:                            strptr("test-client"),
		RequestId:                           strptr("4D1E4058-6FCF-4BA8-BF16-8FA8B02F9651"),
		ExecutionStartToCloseTimeoutSeconds: int32ptr(10),
		TaskStartToCloseTimeoutSeconds:      int32ptr(5),
	}
	resp, err := c.StartWorkflowExecution(ctx, start)
	if err != nil {
		fmt.Println("Start err:", err)
		return
	}
	fmt.Println("Start run id:", *resp.RunId)
	// Signal
	sig := &shared.SignalWorkflowExecutionRequest{
		Domain:          strptr("samples-domain"),
		WorkflowExecution: &shared.WorkflowExecution{WorkflowId: strptr("test-workflow"), RunId: resp.RunId},
		SignalName:      strptr("sig"),
		Input:           []byte("payload"),
		Identity:        strptr("cli"),
		RequestId:       strptr("4D1E4058-6FCF-4BA8-BF16-8FA8B02F9652"),
	}
	if err := c.SignalWorkflowExecution(ctx, sig); err != nil {
		fmt.Println("Signal err:", err)
	} else {
		fmt.Println("Signaled")
	}
}

func strptr(s string) *string      { return &s }
func int32ptr(v int32) *int32      { return &v }
func boolptr(v bool) *bool         { return &v }
