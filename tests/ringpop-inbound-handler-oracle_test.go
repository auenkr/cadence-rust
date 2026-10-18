package swim

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/uber/ringpop-go/discovery/statichosts"
)

// Run inside pinned ringpop-go v0.10.0/swim using a Go overlay. This invokes
// the original unexported handlers; it is not a reimplementation of them.
func TestGenerateRingpopInboundHandlerFixtures(t *testing.T) {
	n := newChannelNode(t)
	defer n.node.Destroy()
	defer n.channel.Close()
	fixture := map[string]interface{}{}

	join, err := handleJoin(n.node, &joinRequest{App: n.node.app, Source: "127.0.0.2:7933", Incarnation: 200})
	if err != nil {
		t.Fatal(err)
	}
	fixture["join"] = join
	_, err = handleJoin(n.node, &joinRequest{App: "wrong", Source: "127.0.0.2:7933"})
	fixture["join_wrong_app"] = err.Error()
	_, err = handleJoin(n.node, &joinRequest{App: n.node.app, Source: n.node.Address()})
	fixture["join_self"] = err.Error()

	_, err = handlePing(n.node, &ping{App: n.node.app, Source: "127.0.0.2:7933"})
	fixture["ping_before_ready"] = err.Error()

	_, err = n.node.Bootstrap(&BootstrapOptions{DiscoverProvider: statichosts.New(n.node.Address())})
	if err != nil {
		t.Fatal(err)
	}
	pong, err := handlePing(n.node, &ping{App: n.node.app, Source: "127.0.0.2:7933", SourceIncarnation: 200, Checksum: n.node.memberlist.Checksum()})
	if err != nil {
		t.Fatal(err)
	}
	fixture["ping"] = pong
	wrong, err := handlePing(n.node, &ping{App: "wrong", Source: "127.0.0.2:7933"})
	if err == nil || wrong != nil {
		t.Fatalf("expected wrong-app error, got %#v, %v", wrong, err)
	}
	fixture["ping_wrong_app"] = err.Error()

	path := os.Getenv("CADENCE_RINGPOP_INBOUND_OUTPUT")
	encoded, err := json.MarshalIndent(fixture, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, encoded, 0600); err != nil {
		t.Fatal(err)
	}
}
