package ringpopprovider

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/uber/cadence/.gen/go/health/metaclient"
	m "github.com/uber/cadence/.gen/go/matching"
	"github.com/uber/cadence/.gen/go/matching/matchingserviceclient"
	s "github.com/uber/cadence/.gen/go/shared"
	tchannelgo "github.com/uber/tchannel-go"
	channeljson "github.com/uber/tchannel-go/json"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/transport/tchannel"
)

func TestRustCombinedMatchingRingpopTChannel(t *testing.T) {
	addr := os.Getenv("CADENCE_COMBINED_TCHANNEL")
	if addr == "" {
		t.Fatal("CADENCE_COMBINED_TCHANNEL is required")
	}
	tr, err := tchannel.NewTransport(tchannel.ServiceName("combined-tchannel-compat"))
	if err != nil {
		t.Fatal(err)
	}
	dispatcher := yarpc.NewDispatcher(yarpc.Config{Name: "combined-tchannel-compat", Outbounds: yarpc.Outbounds{
		"cadence-matching": {Unary: tr.NewSingleOutbound(addr)},
	}})
	if err := dispatcher.Start(); err != nil {
		t.Fatal(err)
	}
	defer dispatcher.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	health, err := metaclient.New(dispatcher.ClientConfig("cadence-matching")).Health(ctx)
	if err != nil || !health.GetOk() {
		t.Fatalf("Matching Meta::health: %v %v", health, err)
	}
	_, err = matchingserviceclient.New(dispatcher.ClientConfig("cadence-matching")).DescribeTaskList(
		ctx, &m.DescribeTaskListRequest{},
	)
	var badRequest *s.BadRequestError
	if !errors.As(err, &badRequest) {
		t.Fatalf("MatchingService::DescribeTaskList was not dispatched: %T %v", err, err)
	}
	channel, err := tchannelgo.NewChannel("combined-ringpop", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer channel.Close()
	jsonClient := channeljson.NewClient(channel, "ringpop",
		&channeljson.ClientOptions{HostPort: addr})
	var headers interface{}
	jsonCtx, jsonCancel := channeljson.NewContext(10 * time.Second)
	defer jsonCancel()
	if err := jsonClient.Call(jsonCtx, "/protocol/header-probe", map[string]string{}, &headers); err != nil {
		t.Fatalf("Ringpop JSON /protocol/header-probe on shared listener: %v", err)
	}
}
