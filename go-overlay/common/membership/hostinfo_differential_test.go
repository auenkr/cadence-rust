package membership

import (
	"net"
	"strconv"
	"testing"
)

// TestHostPortAndPortLabelOracle supplies actual Go stdlib results for the
// matching Rust HostInfo/peer-label fixture in cadence-server.
func TestHostPortAndPortLabelOracle(t *testing.T) {
	cases := []struct {
		address, wantHost string
		valid             bool
	}{
		{"host:7935", "host", true},
		{":7935", "", true},
		{"host:", "host", true},
		{"[::1]:7935", "::1", true},
		{"[bad]:7935", "bad", true},
		{"[::1]x:7935", "", false},
		{"bad:host:7935", "", false},
		{"broken", "", false},
		{"host]:7935", "", false},
		{"[host]]:7935", "", false},
		{"[host:7935", "", false},
		{"[host]", "", false},
	}
	for _, tc := range cases {
		host, _, err := net.SplitHostPort(tc.address)
		if (err == nil) != tc.valid || (err == nil && host != tc.wantHost) {
			t.Errorf("SplitHostPort(%q) = (%q, %v), want (%q, valid=%v)", tc.address, host, err, tc.wantHost, tc.valid)
			continue
		}
		if tc.valid {
			got := net.JoinHostPort(host, "80")
			want := tc.wantHost + ":80"
			if tc.wantHost == "::1" {
				want = "[::1]:80"
			}
			if got != want {
				t.Errorf("JoinHostPort(%q, 80) = %q, want %q", host, got, want)
			}
		}
	}
	for _, tc := range []struct {
		label string
		want  uint64
		valid bool
	}{
		{"0", 0, true}, {"65535", 65535, true}, {"65536", 0, false},
		{"-1", 0, false}, {"+1", 0, false}, {"00080", 80, true}, {"", 0, false},
	} {
		got, err := strconv.ParseUint(tc.label, 10, 16)
		if (err == nil) != tc.valid || (err == nil && got != tc.want) {
			t.Errorf("ParseUint(%q, 10, 16) = (%d, %v), want (%d, valid=%v)", tc.label, got, err, tc.want, tc.valid)
		}
	}
}
