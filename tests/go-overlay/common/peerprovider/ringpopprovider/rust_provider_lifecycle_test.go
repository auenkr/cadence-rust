package ringpopprovider

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/uber/cadence/common/log/testlogger"
	"github.com/uber/cadence/common/membership"
	ringpopconfig "github.com/uber/cadence/common/peerprovider/ringpopprovider/config"
	tchannel "github.com/uber/tchannel-go"
)

// This test pairs the production Rust Matching provider with Cadence's pinned
// Go Ringpop provider. It validates shared-listener JOIN, role/port labels,
// Rust self-eviction, and a same-address Rust process restart/recovery.
func TestRustMatchingRingpopProviderGoPeerLifecycle(t *testing.T) {
	probe := os.Getenv("RUST_MATCHING_RINGPOP_PROBE")
	if probe == "" {
		t.Skip("set RUST_MATCHING_RINGPOP_PROBE via the integration runner")
	}

	channel, err := tchannel.NewChannel("cadence-history", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := channel.ListenAndServe("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	goAddress := channel.PeerInfo().HostPort
	rustAddress := reserveRustAddress(t)
	port, err := net.SplitHostPort(goAddress)
	if err != nil {
		t.Fatal(err)
	}
	var goPort uint16
	if _, err := fmt.Sscan(port, &goPort); err != nil {
		t.Fatal(err)
	}
	goProvider, err := New("cadence-history", &ringpopconfig.Config{
		Name:            "cadence",
		BootstrapMode:   ringpopconfig.BootstrapModeHosts,
		BootstrapHosts:  []string{rustAddress},
		MaxJoinDuration: 15 * time.Second,
	}, channel, membership.PortMap{
		membership.PortGRPC:     7836,
		membership.PortTchannel: goPort,
	}, testlogger.New(t))
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	startDone := make(chan error, 1)
	go func() { startDone <- goProvider.Start(ctx) }()

	// Keep the reservation made in the provider's seed list. Start a Rust node
	// there only after the Go listener is ready to answer Ringpop JOIN calls.
	command := exec.CommandContext(ctx, probe, rustAddress, goAddress)
	var output strings.Builder
	command.Stdout = &output
	command.Stderr = &output
	if err := command.Start(); err != nil {
		cancel()
		t.Fatal(err)
	}
	defer func() {
		if command.ProcessState == nil {
			_ = command.Process.Kill()
			_ = command.Wait()
		}
	}()
	defer func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopCancel()
		if err := goProvider.Stop(stopCtx); err != nil {
			t.Errorf("stopping Go Ringpop provider: %v", err)
		}
		_ = channel.Close()
	}()

	select {
	case err := <-startDone:
		if err != nil {
			t.Fatalf("starting pinned Go peer: %v\n%s", err, output.String())
		}
	case <-ctx.Done():
		t.Fatalf("Go peer did not bootstrap: %v\n%s", ctx.Err(), output.String())
	}

	waitMembers(t, ctx, goProvider, 1, "Rust Matching peer did not join Go Ringpop")
	initial, err := goProvider.GetMembers("cadence-matching")
	if err != nil || len(initial) != 1 {
		t.Fatalf("Rust Matching membership: %#v, %v", initial, err)
	}
	tchannelAddress, err := initial[0].GetNamedAddress(membership.PortTchannel)
	if err != nil || tchannelAddress != rustAddress {
		t.Fatalf("Rust tchannel label = %q, %v; want %q", tchannelAddress, err, rustAddress)
	}
	grpcAddress, err := initial[0].GetNamedAddress(membership.PortGRPC)
	if err != nil || grpcAddress != "127.0.0.1:7234" {
		t.Fatalf("Rust grpc label = %q, %v; want 127.0.0.1:7234", grpcAddress, err)
	}

	waitMembers(t, ctx, goProvider, 0, "Go peer did not observe Rust self-eviction")
	waitMembers(t, ctx, goProvider, 1, "same-address Rust restart did not recover membership")

	if err := command.Wait(); err != nil {
		t.Fatalf("Rust provider lifecycle probe failed: %v\n%s", err, output.String())
	}
	if !strings.Contains(output.String(), "rust-peer-recovered") {
		t.Fatalf("Rust lifecycle probe did not report recovery:\n%s", output.String())
	}
}

func reserveRustAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}

func waitMembers(t *testing.T, ctx context.Context, provider *Provider, count int, message string) {
	t.Helper()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		members, err := provider.GetMembers("cadence-matching")
		if err != nil {
			t.Fatalf("reading Go peer membership: %v", err)
		}
		if len(members) == count {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("%s: got %d members, want %d", message, len(members), count)
		case <-ticker.C:
		}
	}
}
