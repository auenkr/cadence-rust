// matching-metrics-prometheus-oracle emits the pinned Cadence Go/Tally reporter
// output for the synthetic event set used by the Rust sink's parity tests.
package main

import (
	"fmt"
	"net/http/httptest"
	"os"
	"time"

	prom "github.com/m3db/prometheus_client_golang/prometheus"
	"github.com/m3db/prometheus_client_golang/prometheus/promhttp"
	"github.com/uber-go/tally"
	tallyprom "github.com/uber-go/tally/prometheus"
	cadenceprom "github.com/uber/cadence/common/metrics/tally/prometheus"
)

func main() {
	timerType := ""
	if len(os.Args) > 1 {
		timerType = os.Args[1]
	}
	custom := len(os.Args) > 2 && os.Args[2] == "custom"
	registry := prom.NewRegistry()
	opts := tallyprom.Options{Registerer: registry}
	if custom {
		opts.DefaultHistogramBuckets = []float64{0.003, 0.01}
		opts.DefaultSummaryObjectives = map[float64]float64{0.5: 0.05, 0.9: 0.01}
	} else {
		for _, bucket := range cadenceprom.DefaultHistogramBuckets() {
			opts.DefaultHistogramBuckets = append(opts.DefaultHistogramBuckets, bucket.Upper)
		}
	}
	if timerType == "histogram" {
		opts.DefaultTimerType = tallyprom.HistogramTimerType
	}
	reporter := tallyprom.NewReporter(opts)
	// Match Cadence's Low1ms100s histogram. Tally converts duration buckets to
	// seconds when reporting to Prometheus.
	buckets := make(tally.DurationBuckets, 42)
	buckets[0] = 0
	for i := 0; i <= 40; i++ {
		value := float64(time.Millisecond) * powSqrt2(i)
		buckets[i+1] = time.Duration(value)
	}
	prefix := "cadence"
	rootTags := map[string]string{"zone": "north-west", "cadence_service": "cadence_matching"}
	if custom {
		prefix = "matching-custom"
		rootTags["deployment"] = "canary.v2"
	}
	scope, closer := tally.NewRootScope(tally.ScopeOptions{
		Prefix:    prefix,
		Tags:      rootTags,
		Separator: tallyprom.DefaultSeparator,
		SanitizeOptions: &tally.SanitizeOptions{
			NameCharacters:       tally.ValidCharacters{Ranges: tally.AlphanumericRange, Characters: []rune{'_'}},
			KeyCharacters:        tally.ValidCharacters{Ranges: tally.AlphanumericRange, Characters: []rune{'_'}},
			ValueCharacters:      tally.ValidCharacters{Ranges: tally.AlphanumericRange, Characters: []rune{'_'}},
			ReplacementCharacter: '_',
		},
		CachedReporter: reporter,
	}, time.Millisecond)
	tags := map[string]string{"operation": "HistoryClientGetMutableState", "cadence_role": "history_client"}
	timerScope := scope.Tagged(tags)
	timerScope.Counter("cadence_client_requests").Inc(1)
	timerScope.Timer("cadence_client_latency").Record(2 * time.Millisecond)
	timerScope.Histogram("cadence_client_latency_ns", buckets).RecordDuration(2 * time.Millisecond)
	time.Sleep(20 * time.Millisecond)
	_ = closer.Close()
	recorder := httptest.NewRecorder()
	promhttp.HandlerFor(registry, promhttp.HandlerOpts{}).ServeHTTP(recorder, httptest.NewRequest("GET", "/metrics", nil))
	fmt.Print(recorder.Body.String())
}

func powSqrt2(exponent int) float64 {
	result := 1.0
	for i := 0; i < exponent; i++ {
		result *= 1.4142135623730951
	}
	return result
}
