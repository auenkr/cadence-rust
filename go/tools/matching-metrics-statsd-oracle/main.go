// matching-metrics-statsd-oracle emits a semantic StatsD packet set from the
// pinned Cadence Tally reporter and cactus client for Rust parity checks.
package main

import (
	"fmt"
	"math"
	"net"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/cactus/go-statsd-client/statsd"
	"github.com/uber-go/tally"
	tallystatsd "github.com/uber-go/tally/statsd"
	cadencestatsd "github.com/uber/cadence/common/metrics/tally/statsd"
)

func main() {
	custom := len(os.Args) > 1 && os.Args[1] == "custom"
	listener, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	defer listener.Close()
	clientPrefix, scopePrefix, zone := "agent", "scope", "west"
	if custom {
		clientPrefix, scopePrefix, zone = "agent-canary", "matching-custom", "canary_v2"
	}
	client, err := statsd.NewClientWithConfig(&statsd.ClientConfig{
		Address: listener.LocalAddr().String(), Prefix: clientPrefix, UseBuffered: true,
		FlushInterval: 20 * time.Millisecond, FlushBytes: 512,
	})
	if err != nil {
		panic(err)
	}
	reporter := cadencestatsd.NewReporter(client, tallystatsd.Options{})
	root, _ := tally.NewRootScope(tally.ScopeOptions{
		Prefix: scopePrefix, Tags: map[string]string{"zone": zone, "cadence_service": "cadence-matching"}, Reporter: reporter,
	}, 100*time.Millisecond)
	defer client.Close()
	tags := map[string]string{"operation": "HistoryClientGetMutableState", "cadence_role": "history_client"}
	scope := root.Tagged(tags)
	scope.Counter("requests").Inc(2)
	scope.Counter("requests").Inc(3)
	scope.Gauge("queue").Update(4)
	scope.Gauge("queue").Update(9)
	scope.Timer("elapsed").Record(7 * time.Millisecond)
	buckets := tally.DurationBuckets{0}
	for i := 0; i <= 40; i++ {
		buckets = append(buckets, time.Duration(float64(time.Millisecond)*math.Pow(2, float64(i)/2)))
	}
	scope.Histogram("latency", buckets).RecordDuration(20 * time.Millisecond)
	scope.Histogram("latency", buckets).RecordDuration(21 * time.Millisecond)
	time.Sleep(140 * time.Millisecond) // cross one Tally reporting boundary
	root.Close()
	_ = client.Close() // flush the buffered UDP client at the explicit boundary
	_ = listener.SetReadDeadline(time.Now().Add(60 * time.Millisecond))
	var lines []string
	buf := make([]byte, 2048)
	for {
		n, _, readErr := listener.ReadFrom(buf)
		if readErr != nil {
			break
		}
		lines = append(lines, strings.Split(strings.TrimSpace(string(buf[:n])), "\n")...)
	}
	sort.Strings(lines)
	for _, line := range lines {
		fmt.Fprintln(os.Stdout, line)
	}
}
