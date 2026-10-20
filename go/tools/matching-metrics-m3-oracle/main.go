// matching-metrics-m3-oracle captures and semantically decodes actual
// Tally v3.5.8 M3 UDP MetricBatch packets for the Rust encoder tests.
package main

import (
	"encoding/json"
	"fmt"
	"math"
	"net"
	"os"
	"time"

	"github.com/uber-go/tally"
	"github.com/uber-go/tally/m3"
	m3thrift "github.com/uber-go/tally/m3/thrift/v2"
	"github.com/uber-go/tally/thirdparty/github.com/apache/thrift/lib/go/thrift"
)

func main() {
	left, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	defer left.Close()
	right, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	defer right.Close()
	config := m3.Configuration{
		HostPorts: []string{left.LocalAddr().String(), right.LocalAddr().String()},
		Service:   "cadence_matching", Env: "test", CommonTags: map[string]string{"zone": "west"},
		Queue: 1, PacketSize: 32768, HistogramBucketTagPrecision: 6,
	}
	reporter, err := config.NewReporter()
	if err != nil {
		panic(err)
	}
	root, _ := tally.NewRootScope(tally.ScopeOptions{
		Prefix: "cadence", Tags: map[string]string{"cadence_service": "cadence_matching"}, CachedReporter: reporter,
	}, time.Hour)
	tags := map[string]string{"cadence_role": "history_client", "operation": "HistoryClientGetMutableState"}
	scope := root.Tagged(tags)
	scope.Counter("cadence_client_requests").Inc(5)
	scope.Gauge("matching_queue_depth").Update(9)
	scope.Timer("cadence_client_latency").Record(7 * time.Millisecond)
	buckets := tally.DurationBuckets{0}
	for i := 0; i <= 40; i++ {
		buckets = append(buckets, time.Duration(float64(time.Millisecond)*math.Pow(2, float64(i)/2)))
	}
	scope.Histogram("cadence_client_latency_ns", buckets).RecordDuration(20 * time.Millisecond)
	scope.Histogram("cadence_client_latency_ns", buckets).RecordDuration(21 * time.Millisecond)
	if err := root.Close(); err != nil { // scope close reports and closes the M3 reporter
		panic(err)
	}
	for _, listener := range []net.PacketConn{left, right} {
		_ = listener.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		buf := make([]byte, 32768)
		for {
			n, _, readErr := listener.ReadFrom(buf)
			if readErr != nil {
				break
			}
			fmt.Fprintln(os.Stdout, decode(buf[:n]))
		}
	}
}

func decode(packet []byte) string {
	transport := thrift.NewTMemoryBufferLen(len(packet))
	if _, err := transport.Write(packet); err != nil {
		panic(err)
	}
	protocol := thrift.NewTBinaryProtocolFactoryDefault().GetProtocol(transport)
	name, _, _, err := protocol.ReadMessageBegin()
	if err != nil {
		panic(err)
	}
	if name != "emitMetricBatchV2" {
		panic("unexpected M3 method: " + name)
	}
	args := m3thrift.M3EmitMetricBatchV2Args{}
	if err := args.Read(protocol); err != nil {
		panic(err)
	}
	encoded, err := json.Marshal(args.Batch)
	if err != nil {
		panic(err)
	}
	return string(encoded)
}
