package main

import (
	"context"
	"time"

	"go.uber.org/cadence/client"
	hello "rust-cadence-version-workflow"

	"go.uber.org/cadence/.gen/go/cadence/workflowserviceclient"
	"go.uber.org/cadence/.gen/go/shared"
	"go.uber.org/cadence/worker"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/transport/http"
)

const (
	domain   = "samples-domain"
	taskList = "rust-cadence-task-list"
)

func strptr(v string) *string { return &v }
func int32ptr(v int32) *int32 { return &v }
func boolptr(v bool) *bool    { return &v }

func main() {
	h := http.NewTransport()
	outbound := h.NewSingleOutbound("http://127.0.0.1:8080")
	dispatcher := yarpc.NewDispatcher(yarpc.Config{
		Name: "rust-cadence-worker",
		Outbounds: yarpc.Outbounds{
			"cadence-frontend": {Unary: outbound},
		},
	})
	if err := dispatcher.Start(); err != nil {
		panic(err)
	}
	defer dispatcher.Stop()

	rpcClient := workflowserviceclient.New(dispatcher.ClientConfig("cadence-frontend"))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	dc := client.NewDomainClient(rpcClient, &client.Options{})
	_ = dc.Register(ctx, &shared.RegisterDomainRequest{
		Name:                                   strptr(domain),
		Description:                            strptr("samples domain for rust cadence"),
		OwnerEmail:                             strptr("owner@example.com"),
		WorkflowExecutionRetentionPeriodInDays: int32ptr(7),
		EmitMetric:                             boolptr(true),
	})

	w := worker.New(rpcClient, domain, taskList, worker.Options{})
	w.RegisterWorkflow(hello.HelloWorkflow)
	w.RegisterActivity(hello.HelloActivity)

	if err := w.Start(); err != nil {
		panic(err)
	}
	select {}
}
