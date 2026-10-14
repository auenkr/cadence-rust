package main

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/cadence/client"
	hello "rust-cadence-version-workflow"

	"go.uber.org/cadence/.gen/go/cadence/workflowserviceclient"
	"go.uber.org/cadence/.gen/go/shared"
	cadenceclient "go.uber.org/cadence/client"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/transport/http"
)

const (
	domain   = "samples-domain"
	taskList = "rust-cadence-task-list"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	h := http.NewTransport()
	outbound := h.NewSingleOutbound("http://127.0.0.1:8080")
	dispatcher := yarpc.NewDispatcher(yarpc.Config{
		Name: "rust-cadence-starter",
		Outbounds: yarpc.Outbounds{
			"cadence-frontend": {Unary: outbound},
		},
	})
	if err := dispatcher.Start(); err != nil {
		panic(err)
	}
	defer dispatcher.Stop()

	rpc := workflowserviceclient.New(dispatcher.ClientConfig("cadence-frontend"))

	dc := cadenceclient.NewDomainClient(rpc, &client.Options{})
	err := dc.Register(ctx, &shared.RegisterDomainRequest{
		Name:                                   strptr(domain),
		Description:                            strptr("samples domain for rust cadence"),
		OwnerEmail:                             strptr("owner@example.com"),
		WorkflowExecutionRetentionPeriodInDays: int32ptr(7),
		ActiveClusterName:                      strptr("active"),
		Clusters: []*shared.ClusterReplicationConfiguration{
			{ClusterName: strptr("active")},
		},
		EmitMetric: boolptr(true),
	})
	if err != nil {
		fmt.Println("Register (may already exist):", err)
	}

	c := cadenceclient.NewClient(rpc, domain, &client.Options{})
	opts := client.StartWorkflowOptions{
		ID:                              "rust-cadence-hello-active",
		TaskList:                        taskList,
		ExecutionStartToCloseTimeout:    time.Minute,
		DecisionTaskStartToCloseTimeout: time.Minute,
	}
	run, err := c.StartWorkflow(ctx, opts, hello.HelloWorkflow)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Started workflow %s (run %s); it remains open until it receives the 'finish' signal.\n", run.ID, run.RunID)
}

func strptr(v string) *string { return &v }
func int32ptr(v int32) *int32 { return &v }
func boolptr(v bool) *bool    { return &v }
