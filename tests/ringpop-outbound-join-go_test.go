package swim

import (
	"encoding/json"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/uber/ringpop-go/discovery/statichosts"
	"github.com/uber/tchannel-go"
)

// Rust makes one real JSON TChannel JOIN to a bootstrapped pinned Go v0.10.0 node.
func TestRustOutboundJoinAgainstGo(t *testing.T) {
	bin := os.Getenv("RINGPOP_JOIN_PROBE")
	if bin == "" {
		t.Fatal("RINGPOP_JOIN_PROBE required")
	}
	ch, err := tchannel.NewChannel("ringpop-join-oracle", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ch.Close()
	if err = ch.ListenAndServe("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	seed := ch.PeerInfo().HostPort
	goNode := NewNode("cadence", seed, ch.GetSubChannel("ringpop"), nil)
	defer goNode.Destroy()
	if _, err = goNode.Bootstrap(&BootstrapOptions{DiscoverProvider: statichosts.New(seed)}); err != nil {
		t.Fatal(err)
	}

	rustAddress := "127.0.0.2:7933"
	cmd := exec.Command(bin, seed, rustAddress)
	raw, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Rust sender: %v: %s", err, raw)
	}
	var got struct {
		Response struct {
			App         string   `json:"app"`
			Coordinator string   `json:"coordinator"`
			Membership  []Change `json:"membership"`
			Checksum    uint32   `json:"membershipChecksum"`
		} `json:"response"`
		Members  []Member `json:"members"`
		Checksum uint32   `json:"checksum"`
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("Rust output %q: %v", raw, err)
	}
	if got.Response.App != "cadence" || got.Response.Coordinator != seed || len(got.Response.Membership) != 1 || got.Response.Membership[0].Address != seed {
		t.Fatalf("unexpected original Go join response: %+v", got.Response)
	}
	if got.Response.Checksum != goNode.memberlist.Checksum() {
		t.Fatalf("join checksum %d != original Go memberlist checksum %d", got.Response.Checksum, goNode.memberlist.Checksum())
	}
	if len(got.Members) != 2 {
		t.Fatalf("Rust did not apply complete join membership: %+v", got.Members)
	}
	oracleChannel, err := tchannel.NewChannel("ringpop-join-state-oracle", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer oracleChannel.Close()
	oracle := NewNode("cadence", rustAddress, oracleChannel.GetSubChannel("ringpop"), nil)
	defer oracle.Destroy()
	oracle.memberlist.local.Incarnation = 100
	oracle.memberlist.AddJoinList(got.Response.Membership)
	if got.Checksum != oracle.memberlist.Checksum() {
		t.Fatalf("Rust accepted checksum %d != original Go AddJoinList checksum %d", got.Checksum, oracle.memberlist.Checksum())
	}
	goMembers := oracle.memberlist.GetMembers()
	addresses := map[string]bool{}
	for _, member := range got.Members {
		addresses[member.Address] = true
	}
	if len(goMembers) != len(got.Members) || !addresses[goMembers[0].Address] || !addresses[goMembers[1].Address] {
		t.Fatalf("Rust accepted members differ from original Go AddJoinList: Rust=%+v Go=%+v", got.Members, goMembers)
	}
	for _, tc := range []struct{ local, app, message string }{
		{rustAddress, "other-app", "A node tried joining a different app cluster"},
		{seed, "cadence", "A node tried joining a cluster by attempting to join itself"},
	} {
		failed := exec.Command(bin, seed, tc.local, tc.app)
		output, err := failed.CombinedOutput()
		if err == nil || !strings.Contains(string(output), tc.message) {
			t.Fatalf("expected original Go join rejection %q, err=%v output=%s", tc.message, err, output)
		}
	}
}
