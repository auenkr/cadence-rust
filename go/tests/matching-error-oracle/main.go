// Verify YARPC's pinned Go client decodes typed ownership details emitted by
// a production Rust Matching gRPC endpoint.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"go.uber.org/yarpc"
	ygrpc "go.uber.org/yarpc/transport/grpc"

	matchingv1 "github.com/uber/cadence/.gen/proto/matching/v1"
	"github.com/uber/cadence/client/wrappers/grpc"
	cadenceerrors "github.com/uber/cadence/common/errors"
	"github.com/uber/cadence/common/types"
)

func required(name string) string {
	value := os.Getenv(name)
	if value == "" {
		panic("missing " + name)
	}
	return value
}

func main() {
	address := required("CADENCE_MATCHING_ERROR_TARGET")
	domainID := required("CADENCE_MATCHING_ERROR_DOMAIN_ID")
	domain := required("CADENCE_MATCHING_ERROR_DOMAIN")
	taskList := required("CADENCE_MATCHING_ERROR_TASK_LIST")
	wantOwner := required("CADENCE_MATCHING_ERROR_EXPECTED_OWNER")
	wantSelf := required("CADENCE_MATCHING_ERROR_EXPECTED_SELF")

	transport := ygrpc.NewTransport()
	dispatcher := yarpc.NewDispatcher(yarpc.Config{
		Name: "cadence-matching-error-oracle",
		Outbounds: yarpc.Outbounds{
			"cadence-matching": {Unary: transport.NewSingleOutbound(address)},
		},
	})
	if err := dispatcher.Start(); err != nil {
		panic(err)
	}
	defer dispatcher.Stop()

	client := grpc.NewMatchingClient(matchingv1.NewMatchingAPIYARPCClient(dispatcher.ClientConfig("cadence-matching")))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := client.PollForDecisionTask(ctx, &types.MatchingPollForDecisionTaskRequest{
		DomainUUID: domainID,
		PollerID:   "go-typed-ownership-oracle",
		PollRequest: &types.PollForDecisionTaskRequest{
			Domain: domain, TaskList: &types.TaskList{Name: taskList}, Identity: "go-typed-ownership-oracle",
		},
	})
	var ownership *cadenceerrors.TaskListNotOwnedByHostError
	if !errors.As(err, &ownership) {
		panic(fmt.Sprintf("Go Matching client did not decode Rust ownership error to *TaskListNotOwnedByHostError: %T %v", err, err))
	}
	if ownership.OwnedByIdentity != wantOwner || ownership.MyIdentity != wantSelf || ownership.TasklistName != taskList {
		panic(fmt.Sprintf("decoded typed ownership fields mismatch: got owner=%q self=%q taskList=%q", ownership.OwnedByIdentity, ownership.MyIdentity, ownership.TasklistName))
	}
	fmt.Printf("typed ownership error decoded: owner=%s self=%s taskList=%s\n", ownership.OwnedByIdentity, ownership.MyIdentity, ownership.TasklistName)
}
