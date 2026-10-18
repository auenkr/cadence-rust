package swim

import (
	"encoding/json"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/uber-common/bark"
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

// Exercise the original, unmodified JoinCluster loop with a complete failed
// group and an already-expired signed deadline. The no-op delayer keeps this
// source comparison bounded; timing is asserted only by the safe inequality.
func TestOriginalJoinClusterChecksDeadlineAfterGroup(t *testing.T) {
	ch, err := tchannel.NewChannel("ringpop-join-cluster-oracle", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ch.Close()
	if err = ch.ListenAndServe("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	seed := ch.PeerInfo().HostPort
	peer := NewNode("cadence", seed, ch.GetSubChannel("ringpop"), nil)
	defer peer.Destroy()
	if _, err = peer.Bootstrap(&BootstrapOptions{DiscoverProvider: statichosts.New(seed)}); err != nil {
		t.Fatal(err)
	}
	badChannel, err := tchannel.NewChannel("ringpop-join-cluster-bad-peer", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer badChannel.Close()
	if err = badChannel.ListenAndServe("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	badSeed := badChannel.PeerInfo().HostPort
	badPeer := NewNode("other-app", badSeed, badChannel.GetSubChannel("ringpop"), nil)
	defer badPeer.Destroy()
	if _, err = badPeer.Bootstrap(&BootstrapOptions{DiscoverProvider: statichosts.New(badSeed)}); err != nil {
		t.Fatal(err)
	}
	localChannel, err := tchannel.NewChannel("ringpop-join-cluster-client", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer localChannel.Close()
	local := NewNode("cadence", "127.0.0.1:7934", localChannel.GetSubChannel("ringpop"), nil)
	local.discoverProvider = statichosts.New(seed, badSeed)
	sender, err := newJoinSender(local, &joinOpts{
		timeout: time.Second, size: 2, maxJoinDuration: -1,
		parallelismFactor: 1, delayer: &nullDelayer{},
	})
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	joined, err := sender.JoinCluster()
	if err == nil || !strings.Contains(err.Error(), "join duration of") || len(joined) != 1 {
		t.Fatalf("expected original Go deadline error after failed group; joined=%v err=%v", joined, err)
	}
	if time.Since(start) <= 0 || sender.numTries != 1 {
		t.Fatalf("deadline/group ordering mismatch: elapsed=%v attempts=%d", time.Since(start), sender.numTries)
	}
}

type callbackJoinDelayer struct{ after func() }

func (d *callbackJoinDelayer) delay() time.Duration {
	d.after()
	return 0
}

// The original Go destroyed branch discards successes accumulated by the
// preceding group, unlike the deadline branch above.
func TestOriginalJoinClusterDestroyedDiscardsPartialSuccess(t *testing.T) {
	ch, err := tchannel.NewChannel("ringpop-join-cluster-destroyed-good", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ch.Close()
	if err = ch.ListenAndServe("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	goodSeed := ch.PeerInfo().HostPort
	good := NewNode("cadence", goodSeed, ch.GetSubChannel("ringpop"), nil)
	defer good.Destroy()
	if _, err = good.Bootstrap(&BootstrapOptions{DiscoverProvider: statichosts.New(goodSeed)}); err != nil {
		t.Fatal(err)
	}
	badChannel, err := tchannel.NewChannel("ringpop-join-cluster-destroyed-bad", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer badChannel.Close()
	if err = badChannel.ListenAndServe("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	badSeed := badChannel.PeerInfo().HostPort
	bad := NewNode("other-app", badSeed, badChannel.GetSubChannel("ringpop"), nil)
	defer bad.Destroy()
	if _, err = bad.Bootstrap(&BootstrapOptions{DiscoverProvider: statichosts.New(badSeed)}); err != nil {
		t.Fatal(err)
	}
	localChannel, err := tchannel.NewChannel("ringpop-join-cluster-destroyed-client", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer localChannel.Close()
	local := NewNode("cadence", "127.0.0.1:7934", localChannel.GetSubChannel("ringpop"), nil)
	local.discoverProvider = statichosts.New(goodSeed, badSeed)
	sender, err := newJoinSender(local, &joinOpts{
		timeout: time.Second, size: 2, maxJoinDuration: time.Second,
		parallelismFactor: 1, delayer: &callbackJoinDelayer{after: local.Destroy},
	})
	if err != nil {
		t.Fatal(err)
	}
	joined, err := sender.JoinCluster()
	if err == nil || err.Error() != "node destroyed while attempting to join cluster" || joined != nil {
		t.Fatalf("destroyed branch must discard prior group successes; joined=%v err=%v", joined, err)
	}
}

// This uses ringpop-go's real internal delay implementation with only its
// randomizer and sleeper replaced, matching the Rust deterministic seam.
func TestOriginalJoinDelayerWindowsAndOptionEdges(t *testing.T) {
	var sleeps []time.Duration
	var bounds []int
	d := &exponentialDelayer{
		logger:       bark.NewNopLogger(),
		initialDelay: 100 * time.Millisecond,
		maxDelay:     250 * time.Millisecond,
		randomizer:   func(bound int) int { bounds = append(bounds, bound); return 9 },
		sleeper:      func(delay time.Duration) { sleeps = append(sleeps, delay) },
	}
	want := []time.Duration{9 * time.Millisecond, 109 * time.Millisecond, 209 * time.Millisecond, 250 * time.Millisecond, 250 * time.Millisecond}
	for i, expected := range want {
		if got := d.delay(); got != expected {
			t.Fatalf("delay %d = %v, want %v", i, got, expected)
		}
	}
	if !d.maxDelayReached || len(bounds) != 3 || len(sleeps) != len(want) {
		t.Fatalf("bounds=%v sleeps=%v maxReached=%v", bounds, sleeps, d.maxDelayReached)
	}
	zero := &exponentialDelayer{logger: bark.NewNopLogger(), initialDelay: 0, maxDelay: 0, randomizer: func(int) int { t.Fatal("zero cap must not jitter"); return 0 }, sleeper: func(time.Duration) {}}
	if got := zero.delay(); got != 0 || !zero.maxDelayReached {
		t.Fatalf("zero option: delay=%v maxReached=%v", got, zero.maxDelayReached)
	}
	for _, tc := range []struct {
		name         string
		initial, max time.Duration
	}{
		{"negative initial", -time.Millisecond, time.Second},
		{"negative cap", time.Millisecond, -time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("expected Go Intn negative-bound panic")
				}
			}()
			d, _ := newExponentialDelayer("test", &delayOpts{initial: tc.initial, max: tc.max, randomizer: func(bound int) int {
				if bound <= 0 {
					panic("invalid argument to Intn")
				}
				return 0
			}, sleeper: func(time.Duration) {}})
			d.delay()
		})
	}
}

func TestOriginalGoDurationStringFractionalFixtures(t *testing.T) {
	for _, tc := range []struct {
		nanos int64
		want  string
	}{
		{1_001_234, "1.001234ms"}, {1_000_001, "1.000001ms"},
		{1_234, "1.234µs"}, {1_001, "1.001µs"},
		{1_000_000_001, "1.000000001s"}, {-1_234, "-1.234µs"},
	} {
		if got := time.Duration(tc.nanos).String(); got != tc.want {
			t.Errorf("Duration(%d).String() = %q, want %q", tc.nanos, got, tc.want)
		}
	}
}

// Rust JoinGroup speaks to two independent pinned Go Ringpop v0.10 peers in
// one concurrent group call and merges both Go membership replies.
func TestRustOutboundJoinGroupAgainstGo(t *testing.T) {
	bin := os.Getenv("RINGPOP_JOIN_PROBE")
	if bin == "" {
		t.Fatal("RINGPOP_JOIN_PROBE required")
	}
	var seeds []string
	var channels []*tchannel.Channel
	var nodes []*Node
	for i := 0; i < 2; i++ {
		ch, err := tchannel.NewChannel("ringpop-join-group-oracle", nil)
		if err != nil {
			t.Fatal(err)
		}
		channels = append(channels, ch)
		if err = ch.ListenAndServe("127.0.0.1:0"); err != nil {
			t.Fatal(err)
		}
		seed := ch.PeerInfo().HostPort
		seeds = append(seeds, seed)
		goNode := NewNode("cadence", seed, ch.GetSubChannel("ringpop"), nil)
		nodes = append(nodes, goNode)
		if _, err = goNode.Bootstrap(&BootstrapOptions{DiscoverProvider: statichosts.New(seed)}); err != nil {
			t.Fatal(err)
		}
	}
	defer func() {
		for _, node := range nodes {
			node.Destroy()
		}
		for _, ch := range channels {
			ch.Close()
		}
	}()

	local := "127.0.0.2:7933"
	args := append([]string{"--group", local}, seeds...)
	raw, err := exec.Command(bin, args...).CombinedOutput()
	if err != nil {
		t.Fatalf("Rust JoinGroup: %v: %s", err, raw)
	}
	var got struct {
		Result struct {
			Attempt   int64    `json:"attempt"`
			Successes []string `json:"successes"`
			Failures  []string `json:"failures"`
		} `json:"result"`
		Members []Member `json:"members"`
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("Rust output %q: %v", raw, err)
	}
	if got.Result.Attempt != 1 || len(got.Result.Successes) != 2 || len(got.Result.Failures) != 0 {
		t.Fatalf("unexpected Go peer outcomes: %+v", got.Result)
	}
	addresses := map[string]bool{}
	for _, member := range got.Members {
		addresses[member.Address] = true
	}
	if len(got.Members) != 3 || !addresses[local] || !addresses[seeds[0]] || !addresses[seeds[1]] {
		t.Fatalf("Rust failed to merge both original-Go memberships: %+v", got.Members)
	}
}
