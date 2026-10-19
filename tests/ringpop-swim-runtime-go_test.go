package swim

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"testing"
	"time"

	metrics "github.com/rcrowley/go-metrics"
	"github.com/uber/ringpop-go/discovery/statichosts"
	"github.com/uber/tchannel-go"
	tchanneljson "github.com/uber/tchannel-go/json"
	"golang.org/x/net/context"
)

type fixedRuntimeIter struct{ member Member }

func (i fixedRuntimeIter) Next() (*Member, bool) {
	member := i.member
	return &member, true
}

type swimRuntimeOracle struct {
	Iterator                []string `json:"iterator"`
	DirectSuccess           string   `json:"directSuccess"`
	NoHelpers               string   `json:"noHelpers"`
	SparseHelpers           string   `json:"sparseHelpers"`
	AllErrors               string   `json:"allErrors"`
	Cascade                 []string `json:"cascade"`
	Cancelled               []string `json:"cancelled"`
	Stopped                 []string `json:"stopped"`
	CadenceInitialInRange   bool     `json:"cadenceInitialInRange"`
	CadenceBehindFloorNanos int64    `json:"cadenceBehindFloorNanos"`
	CadenceRateNanos        int64    `json:"cadenceRateNanos"`
	CadenceSampleSize       int      `json:"cadenceSampleSize"`
}

func TestGoSwimProbeAndTransitionOracle(t *testing.T) {
	oracle := swimRuntimeOracle{}
	iteratorNode := newChannelNode(t)
	defer iteratorNode.Destroy()
	addRuntimeMember(iteratorNode.node, "127.0.0.1:9910", Alive)
	for i := 0; i < 2; i++ {
		member, ok := iteratorNode.node.memberiter.Next()
		if !ok || member.Address != "127.0.0.1:9910" {
			t.Fatalf("member iterator selected %+v, ok=%v", member, ok)
		}
		oracle.Iterator = append(oracle.Iterator, member.Address)
	}

	requester := newChannelNode(t)
	defer requester.Destroy()
	target := newChannelNode(t)
	defer target.Destroy()
	if _, err := target.node.Bootstrap(&BootstrapOptions{DiscoverProvider: statichosts.New()}); err != nil {
		t.Fatal(err)
	}
	addRuntimeMember(requester.node, target.node.Address(), Alive)
	targetMember, _ := requester.node.memberlist.Member(target.node.Address())
	requester.node.memberiter = fixedRuntimeIter{Member{Address: target.node.Address(), Incarnation: targetMember.Incarnation, Status: Alive}}
	requester.node.pingNextMember()
	if member, _ := requester.node.memberlist.Member(target.node.Address()); member == nil || member.Status != Alive {
		t.Fatalf("successful direct probe status = %+v", member)
	}
	oracle.DirectSuccess = "alive"

	noHelpers := newChannelNode(t)
	defer noHelpers.Destroy()
	deadTarget := closedAddress(t)
	addRuntimeMember(noHelpers.node, deadTarget, Alive)
	noHelpers.node.memberiter = fixedRuntimeIter{Member{Address: deadTarget, Incarnation: 1, Status: Alive}}
	noHelpers.node.pingNextMember()
	if member, _ := noHelpers.node.memberlist.Member(deadTarget); member == nil || member.Status != Suspect {
		t.Fatalf("empty helper pool must fall through to suspect: %+v", member)
	}
	oracle.NoHelpers = "suspect"

	sparse := newChannelNode(t)
	defer sparse.Destroy()
	addresses := closedAddresses(t, 2)
	sparseTarget, helper := addresses[0], addresses[1]
	addRuntimeMember(sparse.node, sparseTarget, Alive)
	addRuntimeMember(sparse.node, helper, Alive)
	sparse.node.memberiter = fixedRuntimeIter{Member{Address: sparseTarget, Incarnation: 1, Status: Alive}}
	sparse.node.pingNextMember()
	if member, _ := sparse.node.memberlist.Member(sparseTarget); member == nil || member.Status != Suspect {
		t.Fatalf("sparse helper pool must fall through to suspect: %+v", member)
	}
	oracle.SparseHelpers = "suspect"

	allErrors := newChannelNode(t)
	defer allErrors.Destroy()
	addresses = closedAddresses(t, 4)
	allTarget := addresses[0]
	addRuntimeMember(allErrors.node, allTarget, Alive)
	for _, helper := range addresses[1:] {
		addRuntimeMember(allErrors.node, helper, Alive)
	}
	allErrors.node.memberiter = fixedRuntimeIter{Member{Address: allTarget, Incarnation: 1, Status: Alive}}
	allErrors.node.pingNextMember()
	if member, _ := allErrors.node.memberlist.Member(allTarget); member == nil || member.Status != Alive {
		t.Fatalf("configured-size all-error guard should remain inconclusive: %+v", member)
	}
	oracle.AllErrors = "alive"

	transitions := newChannelNode(t)
	defer transitions.Destroy()
	transitions.node.stateTransitions.timeouts = StateTimeouts{
		Suspect: 5 * time.Nanosecond, Faulty: 10 * time.Nanosecond, Tombstone: 5 * time.Nanosecond,
	}
	address := "127.0.0.1:9911"
	addRuntimeMember(transitions.node, address, Suspect)
	oracle.Cascade = append(oracle.Cascade, memberStatus(transitions.node, address))
	transitions.clock.Add(5 * time.Nanosecond)
	oracle.Cascade = append(oracle.Cascade, memberStatus(transitions.node, address))
	transitions.clock.Add(9 * time.Nanosecond)
	oracle.Cascade = append(oracle.Cascade, memberStatus(transitions.node, address))
	transitions.clock.Add(1 * time.Nanosecond)
	oracle.Cascade = append(oracle.Cascade, memberStatus(transitions.node, address))
	transitions.clock.Add(5 * time.Nanosecond)
	if _, ok := transitions.node.memberlist.Member(address); ok {
		oracle.Cascade = append(oracle.Cascade, "present")
	} else {
		oracle.Cascade = append(oracle.Cascade, "evicted")
	}

	cancelled := newChannelNode(t)
	defer cancelled.Destroy()
	cancelAddress := "127.0.0.1:9912"
	addRuntimeMember(cancelled.node, cancelAddress, Suspect)
	member, _ := cancelled.node.memberlist.Member(cancelAddress)
	addRuntimeMemberAt(cancelled.node, cancelAddress, member.Incarnation+1, Alive)
	cancelled.clock.Add(time.Second)
	oracle.Cancelled = []string{memberStatus(cancelled.node, cancelAddress)}

	stopped := newChannelNode(t)
	defer stopped.Destroy()
	stopAddress := "127.0.0.1:9913"
	addRuntimeMember(stopped.node, stopAddress, Suspect)
	stopped.node.Stop()
	stopped.clock.Add(time.Second)
	oracle.Stopped = []string{memberStatus(stopped.node, stopAddress)}
	stopped.node.Start()
	stopped.clock.Add(time.Second)
	oracle.Stopped = append(oracle.Stopped, memberStatus(stopped.node, stopAddress))

	cadenceNode := newChannelNode(t)
	defer cadenceNode.Destroy()
	cadence := cadenceNode.node.gossip
	cadence.minProtocolPeriod = time.Nanosecond
	cadence.protocol.timing = metrics.NewHistogram(metrics.NewUniformSample(10))
	for _, sample := range []int64{10, 20, 30, 40} {
		cadence.protocol.timing.Update(sample)
	}
	cadence.AdjustProtocolRate()
	oracle.CadenceRateNanos = int64(cadence.ProtocolRate())
	for sample := int64(0); sample < 12; sample++ {
		cadence.protocol.timing.Update(sample)
	}
	oracle.CadenceSampleSize = cadence.protocol.timing.Sample().Size()
	cadence.protocol.numPeriods = 1
	cadence.protocol.lastPeriod = time.Now().Add(-time.Second)
	oracle.CadenceBehindFloorNanos = int64(cadence.ComputeProtocolDelay())
	cadence.protocol.numPeriods = 0
	initialDelay := cadence.ComputeProtocolDelay()
	oracle.CadenceInitialInRange = initialDelay >= 0 && initialDelay <= cadence.minProtocolPeriod

	raw, err := json.Marshal(oracle)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("CADENCE_SWIM_RUNTIME_ORACLE=%s\n", raw)
}

func TestRustSwimRuntimeAgainstGo(t *testing.T) {
	bin := os.Getenv("RINGPOP_SWIM_RUNTIME_PROBE")
	if bin == "" {
		t.Fatal("RINGPOP_SWIM_RUNTIME_PROBE required")
	}
	channel, err := tchannel.NewChannel("test", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer channel.Close()
	if err = channel.ListenAndServe("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	goAddress := channel.PeerInfo().HostPort
	target := NewNode("test", goAddress, channel.GetSubChannel("ringpop"), nil)
	defer target.Destroy()
	if _, err = target.Bootstrap(&BootstrapOptions{DiscoverProvider: statichosts.New()}); err != nil {
		t.Fatal(err)
	}

	rustAddress := "127.0.0.2:7933"
	cmd := exec.Command(bin, goAddress, rustAddress)
	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output
	if err = cmd.Start(); err != nil {
		t.Fatalf("start Rust SWIM runtime: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		member, ok := target.memberlist.Member(rustAddress)
		if ok && member != nil && member.Status == Alive {
			break
		}
		if time.Now().After(deadline) {
			_ = cmd.Process.Kill()
			_, _ = cmd.Process.Wait()
			t.Fatalf("Go target did not observe Rust as ALIVE while its probe was running: %+v", member)
		}
		time.Sleep(5 * time.Millisecond)
	}
	rawWait := make(chan error, 1)
	go func() { rawWait <- cmd.Wait() }()
	select {
	case err = <-rawWait:
		if err != nil {
			t.Fatalf("Rust SWIM runtime: %v: %s", err, output.String())
		}
	case <-time.After(3 * time.Second):
		_ = cmd.Process.Kill()
		t.Fatalf("Rust SWIM runtime did not stop: %s", output.String())
	}
	raw := output.Bytes()
	var got struct {
		Ready     bool     `json:"ready"`
		Lifecycle string   `json:"lifecycle"`
		Members   []Member `json:"members"`
	}
	if err = json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("Rust runtime output %q: %v", raw, err)
	}
	if !got.Ready || got.Lifecycle != "Stopped" {
		t.Fatalf("Rust runtime lifecycle did not preserve readiness across Stop: %+v", got)
	}
	if member, ok := target.memberlist.Member(rustAddress); !ok || member == nil {
		t.Fatalf("Go target did not retain Rust's gossiped membership: %+v", member)
	}
	statusDeadline := time.Now().Add(time.Second)
	for {
		member, ok := target.memberlist.Member(rustAddress)
		if ok && member != nil && member.Status == Suspect {
			break
		}
		if time.Now().After(statusDeadline) {
			t.Fatalf("Go target post-exit member status = %+v, want %s", member, Suspect)
		}
		time.Sleep(5 * time.Millisecond)
	}
	foundTarget := false
	for _, member := range got.Members {
		foundTarget = foundTarget || member.Address == goAddress
	}
	if !foundTarget {
		t.Fatal("Rust runtime did not merge the Go target membership")
	}
}

func TestGoStopWaitsForInflightProtocol(t *testing.T) {
	blockedChannel, err := tchannel.NewChannel("test", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer blockedChannel.Close()
	entered := make(chan struct{})
	release := make(chan struct{})
	if err := tchanneljson.Register(blockedChannel.GetSubChannel("test"), tchanneljson.Handlers{
		"/protocol/ping": func(_ tchanneljson.Context, _ *ping) (*ping, error) {
			close(entered)
			<-release
			return &ping{App: "test"}, nil
		},
	}, func(_ context.Context, err error) { t.Errorf("blocked Go ping handler: %v", err) }); err != nil {
		t.Fatal(err)
	}
	if err := blockedChannel.ListenAndServe("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	goAddress := blockedChannel.PeerInfo().HostPort

	requester := newChannelNode(t)
	defer requester.Destroy()
	addRuntimeMember(requester.node, goAddress, Alive)
	member, _ := requester.node.memberlist.Member(goAddress)
	requester.node.memberiter = fixedRuntimeIter{member: Member{
		Address: member.Address, Incarnation: member.Incarnation, Status: member.Status,
	}}
	requester.node.gossip.Start()
	// newChannelNode uses a mock clock; advance the initial gossip cadence so
	// the first protocol tick actually makes the blocking request below.
	requester.clock.Add(time.Minute)
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("Go gossip did not enter its in-flight protocol call")
	}

	stopped := make(chan struct{})
	go func() {
		requester.node.gossip.Stop()
		close(stopped)
	}()
	select {
	case <-stopped:
		t.Fatal("Go gossip Stop returned before the in-flight protocol call")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("Go gossip Stop did not return after the protocol call completed")
	}
	requester.node.Destroy()
}

func addRuntimeMember(node *Node, address, status string) {
	addRuntimeMemberAt(node, address, 1, status)
}

func addRuntimeMemberAt(node *Node, address string, incarnation int64, status string) {
	local := node.memberlist.LocalMember()
	node.memberlist.Update([]Change{{
		Source: local.Address, SourceIncarnation: local.Incarnation,
		Address: address, Incarnation: incarnation, Status: status,
	}})
}

func memberStatus(node *Node, address string) string {
	member, ok := node.memberlist.Member(address)
	if !ok || member == nil {
		return "absent"
	}
	return member.Status
}

func closedAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err = listener.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}

func closedAddresses(t *testing.T, count int) []string {
	t.Helper()
	listeners := make([]net.Listener, 0, count)
	addresses := make([]string, 0, count)
	for i := 0; i < count; i++ {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners = append(listeners, listener)
		addresses = append(addresses, listener.Addr().String())
	}
	for _, listener := range listeners {
		if err := listener.Close(); err != nil {
			t.Fatal(err)
		}
	}
	return addresses
}
