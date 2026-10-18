package swim

import (
	"encoding/json"
	"os"
	"os/exec"
	"testing"

	"github.com/uber/ringpop-go/discovery/statichosts"
	"github.com/uber/tchannel-go"
)

// Rust sends a real direct JSON TChannel PING to the pinned Go v0.10.0 peer.
func TestRustOutboundPingAgainstGo(t *testing.T) {
	bin := os.Getenv("RINGPOP_PING_PROBE")
	if bin == "" {
		t.Fatal("RINGPOP_PING_PROBE required")
	}
	ch, err := tchannel.NewChannel("ringpop-ping-oracle", nil)
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

	local := "127.0.0.2:7933"
	cmd := exec.Command(bin, seed, local)
	raw, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Rust PING: %v: %s", err, raw)
	}
	var got struct {
		Response ping           `json:"response"`
		Members  []Member       `json:"members"`
		Checksum uint32         `json:"checksum"`
		Queued   map[string]int `json:"queued"`
		MaxP     int            `json:"maxP"`
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("Rust output %q: %v", raw, err)
	}
	if got.Response.Source != seed || got.Response.App != "" || got.Response.Checksum != goNode.memberlist.Checksum() {
		t.Fatalf("unexpected Go PING response or checksum: response=%+v GoChecksum=%d", got.Response, goNode.memberlist.Checksum())
	}
	if got.Checksum != goNode.memberlist.Checksum() || len(got.Members) != 2 {
		t.Fatalf("Rust did not apply the complete Go response: checksum=%d Go=%d members=%+v", got.Checksum, goNode.memberlist.Checksum(), got.Members)
	}
	requestChange, ok := goNode.disseminator.ChangesByAddress(local)
	if !ok || requestChange.Source != "" || requestChange.SourceIncarnation != 0 || requestChange.Incarnation != 100 || requestChange.Status != Alive {
		t.Fatalf("Go accepted unexpected request change metadata: %+v, present=%v", requestChange, ok)
	}
	responseAddresses := map[string]bool{}
	for _, change := range got.Response.Changes {
		responseAddresses[change.Address] = true
	}
	if len(responseAddresses) != 2 || !responseAddresses[seed] || !responseAddresses[local] {
		t.Fatalf("unexpected changes returned by Go PING handler: %+v", got.Response.Changes)
	}
	// Go's receiver increments response changes immediately. Since its
	// filter compares change source metadata (empty for this initial local
	// delta), the full response includes both nodes. Rust applies the Go change
	// and ignores its own equal-incarnation response change.
	if got.Queued[local] != 1 {
		t.Fatalf("Rust sender callback did not bump local change once: %v", got.Queued)
	}
	if got.Queued[seed] != 0 || goNode.disseminator.changes[seed].p != 1 || goNode.disseminator.changes[local].p != 1 {
		t.Fatalf("PING sender/receiver counters differ: Rust=%v Go seed p=%d local p=%d", got.Queued, goNode.disseminator.changes[seed].p, goNode.disseminator.changes[local].p)
	}
	if got.MaxP != goNode.disseminator.maxP {
		t.Fatalf("maxP differs: Rust=%d Go=%d", got.MaxP, goNode.disseminator.maxP)
	}
}
