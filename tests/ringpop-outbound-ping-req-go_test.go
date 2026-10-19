package swim

import (
	"encoding/json"
	"net"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/uber/ringpop-go/discovery/statichosts"
	"github.com/uber/tchannel-go"
)

// A real Rust requester joins a pinned Go helper, then issues PING-REQ through
// it to a separate pinned Go target. This exercises selection, wire fields,
// response merge, and the sender's error-only dissemination callback.
func TestRustOutboundPingRequestAgainstGo(t *testing.T) {
	bin := os.Getenv("RINGPOP_OUTBOUND_PING_REQ_PROBE")
	if bin == "" {
		t.Fatal("RINGPOP_OUTBOUND_PING_REQ_PROBE required")
	}
	targetChannel, err := tchannel.NewChannel("cadence", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer targetChannel.Close()
	if err = targetChannel.ListenAndServe("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	targetAddress := targetChannel.PeerInfo().HostPort
	target := NewNode("cadence", targetAddress, targetChannel.GetSubChannel("ringpop"), nil)
	defer target.Destroy()
	if _, err = target.Bootstrap(&BootstrapOptions{DiscoverProvider: statichosts.New(targetAddress)}); err != nil {
		t.Fatal(err)
	}

	helperChannel, err := tchannel.NewChannel("cadence", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer helperChannel.Close()
	if err = helperChannel.ListenAndServe("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	helperAddress := helperChannel.PeerInfo().HostPort
	helper := NewNode("cadence", helperAddress, helperChannel.GetSubChannel("ringpop"), nil)
	defer helper.Destroy()
	if _, err = helper.Bootstrap(&BootstrapOptions{DiscoverProvider: statichosts.New(targetAddress)}); err != nil {
		t.Fatal(err)
	}

	requesterAddress := "127.0.0.2:7933"
	cmd := exec.Command(bin, helperAddress, requesterAddress, targetAddress)
	raw, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Rust indirect PING: %v: %s", err, raw)
	}
	var got struct {
		Reached       bool           `json:"reached"`
		Errors        int            `json:"errors"`
		Members       []Member       `json:"members"`
		Dissemination map[string]int `json:"dissemination"`
	}
	if err = json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("Rust output %q: %v", raw, err)
	}
	if !got.Reached || got.Errors != 0 {
		t.Fatalf("indirect probe result: %+v", got)
	}
	if _, ok := target.memberlist.Member(requesterAddress); !ok {
		t.Fatal("Go target did not receive requester membership through helper")
	}
	if _, ok := memberByAddress(got.Members, targetAddress); !ok {
		t.Fatal("Rust requester did not merge target changes from PING-REQ response")
	}
	if got.Dissemination[requesterAddress] != 0 {
		t.Fatalf("successful PING-REQ bumped sender counters: %v", got.Dissemination)
	}
}

// Pin the unusual Go callback contract directly: a decoded response (whether
// pingStatus is true or false) does not bump IssueAsSender, while an RPC error
// does. The Rust sender mirrors this oracle.
func TestGoPingRequestSenderCounterOracle(t *testing.T) {
	nodes := genChannelNodes(t, 3)
	defer destroyNodes(nodes...)
	bootstrapNodes(t, nodes...)
	waitForConvergence(t, 100, nodes...)
	sender, helper, target := nodes[0], nodes[1], nodes[2]
	localAddress := sender.node.Address()
	sender.node.disseminator.RecordChange(Change{
		Address: localAddress, Incarnation: sender.node.Incarnation(), Status: Alive,
	})
	baseline := sender.node.disseminator.changes[localAddress].p

	deadTarget, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	deadTargetAddress := deadTarget.Addr().String()
	_ = deadTarget.Close()
	for _, targetAddress := range []string{target.node.Address(), deadTargetAddress} {
		response, err := newPingRequestSender(sender.node, helper.node.Address(), targetAddress, time.Second).SendPingRequest()
		if err != nil {
			t.Fatalf("decoded PING-REQ response for %s: %v", targetAddress, err)
		}
		if targetAddress == target.node.Address() && !response.Ok {
			t.Fatalf("expected helper to reach target: %+v", response)
		}
		if targetAddress != target.node.Address() && response.Ok {
			t.Fatalf("expected false pingStatus for closed target: %+v", response)
		}
		if got := sender.node.disseminator.changes[localAddress].p; got != baseline {
			t.Fatalf("successful PING-REQ response bumped sender counter: got %d want %d", got, baseline)
		}
	}
	dead, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	deadAddress := dead.Addr().String()
	_ = dead.Close()
	if _, err = newPingRequestSender(sender.node, deadAddress, target.node.Address(), time.Second).SendPingRequest(); err == nil {
		t.Fatal("expected helper RPC failure")
	}
	if got := sender.node.disseminator.changes[localAddress].p; got != baseline+1 {
		t.Fatalf("failed PING-REQ callback counter = %d, want %d", got, baseline+1)
	}
}

func memberByAddress(members []Member, address string) (Member, bool) {
	for _, member := range members {
		if member.Address == address {
			return member, true
		}
	}
	return Member{}, false
}
