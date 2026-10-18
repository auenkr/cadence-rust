package swim

import (
	"encoding/json"
	"net"
	"os"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/uber/ringpop-go/discovery/statichosts"
	ringpoputil "github.com/uber/ringpop-go/util"
)

func TestGoCaptureHostMatrix(t *testing.T) {
	cases := []struct{ input, host string; valid bool }{
		{"host:", "host", true}, {":80", "", true}, {"[::1]:", "::1", true},
		{"[fe80::1%eth0]:80", "fe80::1%eth0", true}, {"[abc]:80", "abc", true},
		{"[[::1]:80", "", false}, {"host:abc", "host", true}, {"host:1:2", "", false},
		{"host:[]:80", "", false}, {"[::1]x:80", "", false}, {"[::1]:80:90", "", false},
		{"host:80]", "", false}, {"[]:80", "", true},
	}
	for _, tc := range cases {
		host, _, err := net.SplitHostPort(tc.input)
		if (err == nil) != tc.valid || host != tc.host || ringpoputil.CaptureHost(tc.input) != tc.host {
			t.Errorf("SplitHostPort/CaptureHost(%q) = %q, %v; want %q valid=%v", tc.input, host, err, tc.host, tc.valid)
		}
	}
}

type selectionFixture struct {
	Name                          string   `json:"name"`
	Local                         string   `json:"local"`
	Seeds                         []string `json:"seeds"`
	RequestedTimeoutNanos         int64    `json:"requestedTimeoutNanos"`
	RequestedMaxJoinDurationNanos int64    `json:"requestedMaxJoinDurationNanos"`
	RequestedSize                 int      `json:"requestedSize"`
	RequestedParallelism          int      `json:"requestedParallelism"`
	BootstrapHosts                []string `json:"bootstrapHosts"`
	Potential                     []string `json:"potential"`
	Preferred                     []string `json:"preferred"`
	NonPreferred                  []string `json:"nonPreferred"`
	Joined                        []string `json:"joined,omitempty"`
	Group                         []string `json:"group"`
	TimeoutNanos                  int64    `json:"timeoutNanos"`
	MaxJoinDurationNanos          int64    `json:"maxJoinDurationNanos"`
	Size                          int      `json:"size"`
	Parallelism                   int      `json:"parallelism"`
}

func TestGenerateRingpopBootstrapSelectionFixtures(t *testing.T) {
	output := os.Getenv("CADENCE_RINGPOP_BOOTSTRAP_SELECTION_OUTPUT")
	if output == "" {
		t.Fatal("CADENCE_RINGPOP_BOOTSTRAP_SELECTION_OUTPUT is required")
	}
	cases := []struct {
		name, local string
		hosts       []string
		opts        *joinOpts
		joined      []string
	}{
		{"empty-seeds-self-only", "127.0.0.1:3000", nil, nil, nil},
		{"normalization-self-and-exact-duplicate", "127.0.0.1:3000", []string{"127.0.0.1:3000", "127.0.0.1:3001", "127.0.0.1:3001", "127.0.0.2:4000"}, nil, nil},
		{"remote-host-preference", "192.0.2.1:1", []string{"192.0.2.1:2", "192.0.2.1:3", "192.0.2.2:1", "192.0.2.3:1"}, &joinOpts{size: 3, parallelismFactor: 1}, nil},
		{"joined-exclusion", "192.0.2.1:1", []string{"192.0.2.1:2", "192.0.2.2:1", "192.0.2.3:1"}, &joinOpts{size: 3, parallelismFactor: 1}, []string{"192.0.2.2:1"}},
		{"requested-size-capped-to-potential", "192.0.2.1:1", []string{"192.0.2.2:1", "192.0.2.3:1"}, &joinOpts{size: 9, parallelismFactor: 3}, nil},
		{"zero-options-default", "192.0.2.1:1", []string{"192.0.2.2:1", "192.0.2.3:1", "192.0.2.4:1", "192.0.2.5:1"}, &joinOpts{}, nil},
		{"negative-options-retained", "192.0.2.1:1", []string{"192.0.2.2:1", "192.0.2.3:1"}, &joinOpts{timeout: -2 * time.Second, maxJoinDuration: -7 * time.Second, size: -3, parallelismFactor: -4}, nil},
		{"explicit-nonzero-options", "192.0.2.1:1", []string{"192.0.2.2:1", "192.0.2.3:1", "192.0.2.4:1", "192.0.2.5:1", "192.0.2.6:1", "192.0.2.7:1"}, &joinOpts{timeout: 250 * time.Millisecond, maxJoinDuration: 4 * time.Second, size: 2, parallelismFactor: 3}, nil},
		{"ipv6-capture", "[2001:db8::1]:10", []string{"[2001:db8::1]:11", "[2001:db8::2]:12"}, &joinOpts{size: 1, parallelismFactor: 1}, nil},
		{"hostname-capture", "node-a.example:10", []string{"node-a.example:11", "node-b.example:12"}, &joinOpts{size: 1, parallelismFactor: 1}, nil},
	}
	fixtures := make([]selectionFixture, 0, len(cases)+1)
	for _, tc := range cases {
		node := NewNode("cadence", tc.local, nil, nil)
		node.discoverProvider = statichosts.New(tc.hosts...)
		j, err := newJoinSender(node, tc.opts)
		if err != nil {
			t.Fatalf("%s: newJoinSender: %v", tc.name, err)
		}
		j.Init(tc.joined)
		group := j.SelectGroup(tc.joined)
		f := snapshotSelection(tc.name, tc.local, tc.hosts, tc.opts, j, tc.joined, group)
		fixtures = append(fixtures, f)
		if tc.name == "empty-seeds-self-only" && (len(f.BootstrapHosts) != 1 || f.BootstrapHosts[0] != tc.local || len(f.Potential) != 0 || len(group) != 0) {
			t.Fatalf("self insertion/exclusion mismatch: %+v", f)
		}
		if tc.name == "normalization-self-and-exact-duplicate" && !reflect.DeepEqual(f.BootstrapHosts, []string{"127.0.0.1:3000", "127.0.0.1:3001", "127.0.0.1:3001", "127.0.0.2:4000"}) {
			t.Fatalf("seed normalization mismatch: %+v", f.BootstrapHosts)
		}
		if tc.name == "remote-host-preference" {
			// Selection's returned order is meaningful for preference: the two
			// remote candidates must precede the local-host fallback. Fixture
			// membership below is sorted to avoid asserting map-derived order.
			if len(group) != 3 || group[0] == "192.0.2.1:2" || group[0] == "192.0.2.1:3" || group[1] == "192.0.2.1:2" || group[1] == "192.0.2.1:3" {
				t.Fatalf("remote hosts were not selected first: %v", group)
			}
		}
		if tc.name == "joined-exclusion" && containsString(f.Potential, "192.0.2.2:1") {
			t.Fatalf("joined address remained potential: %v", f.Potential)
		}
		if tc.name == "requested-size-capped-to-potential" && (j.size != 2 || len(group) != 2) {
			t.Fatalf("effective size/potential cap mismatch: size=%d group=%v", j.size, group)
		}
		if tc.name == "zero-options-default" && (j.timeout != time.Second || j.maxJoinDuration != 120*time.Second || j.size != 3 || j.parallelismFactor != 2) {
			t.Fatalf("default option mismatch: %+v", j)
		}
		if tc.name == "negative-options-retained" && (j.timeout != -2*time.Second || j.maxJoinDuration != -7*time.Second || j.size != -3 || j.parallelismFactor != -4) {
			t.Fatalf("negative option mismatch: %+v", j)
		}
		if tc.name == "explicit-nonzero-options" && (j.timeout != 250*time.Millisecond || j.maxJoinDuration != 4*time.Second || j.size != 2 || j.parallelismFactor != 3 || len(group) != 6) {
			t.Fatalf("explicit option/cap mismatch: %+v group=%v", j, group)
		}
	}
	// Exhaust a round, then verify original SelectGroup starts a new round and
	// can select the same eligible set again. Ordering is deliberately ignored.
	node := NewNode("cadence", "192.0.2.1:1", nil, nil)
	node.discoverProvider = statichosts.New("192.0.2.2:1")
	j, err := newJoinSender(node, &joinOpts{size: 1, parallelismFactor: 1})
	if err != nil {
		t.Fatal(err)
	}
	first := sorted(j.SelectGroup(nil))
	second := sorted(j.SelectGroup(nil))
	if len(first) != 1 || !reflect.DeepEqual(first, second) {
		t.Fatalf("exhausted round did not reset to its eligible target: first=%v second=%v", first, second)
	}
	fixtures = append(fixtures, snapshotSelection("exhausted-round-reset", "192.0.2.1:1", []string{"192.0.2.2:1"}, &joinOpts{size: 1, parallelismFactor: 1}, j, nil, second))
	data, err := json.MarshalIndent(map[string]interface{}{"source": "github.com/uber/ringpop-go v0.10.0 swim/join_sender.go newJoinSender + SelectGroup", "cases": fixtures}, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(output, append(data, '\n'), 0644); err != nil {
		t.Fatal(err)
	}
}

func snapshotSelection(name, local string, seeds []string, requested *joinOpts, j *joinSender, joined, group []string) selectionFixture {
	if requested == nil {
		requested = &joinOpts{}
	}
	return selectionFixture{Name: name, Local: local, Seeds: seeds, RequestedTimeoutNanos: int64(requested.timeout), RequestedMaxJoinDurationNanos: int64(requested.maxJoinDuration), RequestedSize: requested.size, RequestedParallelism: requested.parallelismFactor, BootstrapHosts: sorted(flattenHosts(j.bootstrapHostsMap)), Potential: sorted(j.potentialNodes), Preferred: sorted(j.preferredNodes), NonPreferred: sorted(j.nonPreferredNodes), Joined: sorted(joined), Group: sorted(group), TimeoutNanos: int64(j.timeout), MaxJoinDurationNanos: int64(j.maxJoinDuration), Size: j.size, Parallelism: j.parallelismFactor}
}
func flattenHosts(m map[string][]string) []string {
	var out []string
	for _, hosts := range m {
		out = append(out, hosts...)
	}
	return out
}
func sorted(in []string) []string { out := append([]string(nil), in...); sort.Strings(out); return out }
func containsString(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}
