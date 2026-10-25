package ringpopprovider

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	tcg "github.com/uber/tchannel-go"

	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/membership"
	ringpopconfig "github.com/uber/cadence/common/peerprovider/ringpopprovider/config"
)

// TestRustMatchingHistoryRingStub joins the Rust Matching ringpop ring as a real
// Go cadence-history member (serviceName label + grpc port label) so the Rust
// Matching History resolver has a History host, as in a real cluster. The
// advertised grpc port is the Rust server's History gRPC listener.
func TestRustMatchingHistoryRingStub(t *testing.T) {
	bootstrap := os.Getenv("CADENCE_RINGPOP_BOOTSTRAP")
	grpcPort, err := strconv.Atoi(os.Getenv("CADENCE_HISTORY_GRPC_PORT"))
	if bootstrap == "" || err != nil {
		t.Fatal("CADENCE_RINGPOP_BOOTSTRAP and CADENCE_HISTORY_GRPC_PORT are required")
	}
	readyFile, releaseFile := os.Getenv("RINGPOP_STUB_READY_FILE"), os.Getenv("RINGPOP_STUB_RELEASE_FILE")
	if readyFile == "" || releaseFile == "" {
		t.Fatal("RINGPOP_STUB_READY_FILE and RINGPOP_STUB_RELEASE_FILE are required")
	}
	ch, err := tcg.NewChannel("cadence-history", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ch.Close()
	if err := ch.ListenAndServe("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	provider, err := New("cadence-history", &ringpopconfig.Config{
		Name:             "cadence",
		BootstrapMode:    ringpopconfig.BootstrapModeHosts,
		BootstrapHosts:   []string{bootstrap},
		MaxJoinDuration:  30 * time.Second,
		BroadcastAddress: "127.0.0.1",
	}, ch, membership.PortMap{membership.PortGRPC: uint16(grpcPort), membership.PortTchannel: uint16(grpcPort)}, log.NewNoop())
	if err != nil {
		t.Fatal(err)
	}
	if err := provider.Start(context.Background()); err != nil {
		t.Fatalf("join Rust Matching ring: %v", err)
	}
	defer provider.Stop(context.Background())
	if err := os.WriteFile(readyFile, []byte("ready\n"), 0600); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(150 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(releaseFile); err == nil {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("release file never appeared")
}
