package ringpopprovider

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/uber/cadence/common/log/testlogger"
	"github.com/uber/cadence/common/membership"
	ringpopconfig "github.com/uber/cadence/common/peerprovider/ringpopprovider/config"
	"github.com/uber/ringpop-go/swim"
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
	_, port, err := net.SplitHostPort(goAddress)
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
	defer func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopCancel()
		if err := goProvider.Stop(stopCtx); err != nil {
			t.Errorf("stopping Go Ringpop provider: %v", err)
		}
		channel.Close()
	}()
	startDone := make(chan error, 1)
	go func() { startDone <- goProvider.Start(ctx) }()

	// Keep the reservation made in the provider's seed list. Start a Rust node
	// there only after the Go listener is ready to answer Ringpop JOIN calls.
	command := exec.CommandContext(ctx, probe, rustAddress, goAddress)
	stdin, err := command.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	defer stdin.Close()
	output := &lockedBuffer{}
	command.Stdout = output
	command.Stderr = output
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
	firstIncarnation := peerIncarnation(t, goProvider, rustAddress)
	t.Logf("initial Rust member accepted: address=%s incarnation=%d tchannel=%s grpc=%s", rustAddress, firstIncarnation, tchannelAddress, grpcAddress)
	if _, err := io.WriteString(stdin, "evict\n"); err != nil {
		t.Fatalf("allowing Rust self-eviction: %v", err)
	}

	waitMembers(t, ctx, goProvider, 0, "Go peer did not observe Rust self-eviction")
	t.Log("Go peer confirmed Rust self-eviction before restart")
	if _, err := io.WriteString(stdin, "restart\n"); err != nil {
		t.Fatalf("allowing Rust restart: %v", err)
	}
	waitMembers(t, ctx, goProvider, 1, "same-address Rust restart did not recover membership", func() string {
		return fmt.Sprintf("Rust probe output:\n%s\nGo reachable members: %s", output.String(), reachableMembers(goProvider))
	})
	waitForIncarnationGreater(t, ctx, goProvider, rustAddress, firstIncarnation, output)
	restartedIncarnation := peerIncarnation(t, goProvider, rustAddress)
	recovered, err := goProvider.GetMembers("cadence-matching")
	if err != nil || len(recovered) != 1 {
		t.Fatalf("restarted Rust Matching membership: %#v, %v", recovered, err)
	}
	recoveredGRPC, err := recovered[0].GetNamedAddress(membership.PortGRPC)
	if err != nil || recoveredGRPC != "127.0.0.1:7234" {
		t.Fatalf("restarted Rust grpc label = %q, %v; want 127.0.0.1:7234", recoveredGRPC, err)
	}
	recoveredTChannel, err := recovered[0].GetNamedAddress(membership.PortTchannel)
	if err != nil || recoveredTChannel != rustAddress {
		t.Fatalf("restarted Rust tchannel label = %q, %v; want %q", recoveredTChannel, err, rustAddress)
	}
	t.Logf("restarted Rust member accepted: address=%s incarnation=%d previous=%d tchannel=%s grpc=%s", rustAddress, restartedIncarnation, firstIncarnation, recoveredTChannel, recoveredGRPC)
	if _, err := io.WriteString(stdin, "stop\n"); err != nil {
		t.Fatalf("allowing final Rust provider stop: %v", err)
	}

	if err := command.Wait(); err != nil {
		t.Fatalf("Rust provider lifecycle probe failed: %v\n%s", err, output.String())
	}
	if !strings.Contains(output.String(), "rust-peer-recovered") {
		t.Fatalf("Rust lifecycle probe did not report recovery:\n%s", output.String())
	}
	t.Logf("Rust probe phases:\n%s", output.String())
}

func waitForIncarnationGreater(t *testing.T, ctx context.Context, provider *Provider, address string, previous int64, output *lockedBuffer) {
	t.Helper()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		if incarnation := peerIncarnation(t, provider, address); incarnation > previous {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("restarted Rust incarnation did not exceed %d\nRust probe output:\n%s\nGo reachable members: %s", previous, output.String(), reachableMembers(provider))
		case <-ticker.C:
		}
	}
}

type lockedBuffer struct {
	mu sync.Mutex
	bytes.Buffer
}

func (b *lockedBuffer) Write(value []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.Buffer.Write(value)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.Buffer.String()
}

func peerIncarnation(t *testing.T, provider *Provider, address string) int64 {
	t.Helper()
	var incarnation int64
	_, err := provider.ringpop.GetReachableMembers(func(member swim.Member) bool {
		if member.Address == address {
			incarnation = member.Incarnation
			return true
		}
		return false
	})
	if err != nil {
		t.Fatalf("reading Rust member incarnation: %v", err)
	}
	if incarnation == 0 {
		t.Fatalf("Rust address %s was not in Go's reachable member set", address)
	}
	return incarnation
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

func waitMembers(t *testing.T, ctx context.Context, provider *Provider, count int, message string, diagnostics ...func() string) {
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
			detail := ""
			if len(diagnostics) > 0 {
				detail = "\n" + diagnostics[0]()
			}
			t.Fatalf("%s: got %d members, want %d%s", message, len(members), count, detail)
		case <-ticker.C:
		}
	}
}

func reachableMembers(provider *Provider) string {
	var members []string
	_, err := provider.ringpop.GetReachableMembers(func(member swim.Member) bool {
		members = append(members, fmt.Sprintf("%s status=%s incarnation=%d labels=%v", member.Address, member.Status, member.Incarnation, member.Labels))
		return false
	})
	if err != nil {
		return fmt.Sprintf("error: %v", err)
	}
	return strings.Join(members, "; ")
}
