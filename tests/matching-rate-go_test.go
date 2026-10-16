package tasklist

import (
	"encoding/json"
	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/service/matching/config"
	"os"
	"testing"
	"time"
)

func TestRustMatchingRateReports(t *testing.T) {
	type step struct {
		Now               int64   `json:"now"`
		Report            float64 `json:"report"`
		Partitions        int     `json:"partitions"`
		Value             float64 `json:"value"`
		Limit             float64 `json:"limit"`
		Burst             int     `json:"burst"`
		LastUpdate        int64   `json:"last_update"`
		LastReceived      int64   `json:"last_received"`
		AppliedPartitions int     `json:"applied_partitions"`
		Reads             int     `json:"reads"`
	}
	type fixture struct {
		Name       string  `json:"name"`
		Initial    float64 `json:"initial"`
		TTL        int64   `json:"ttl"`
		MinBurst   int     `json:"min_burst"`
		Partitions int     `json:"partitions"`
		Steps      []step  `json:"steps"`
	}
	data, err := os.ReadFile(os.Getenv("CADENCE_MATCHING_RATE_INPUT"))
	if err != nil {
		t.Fatal(err)
	}
	var cases []fixture
	if err = json.Unmarshal(data, &cases); err != nil {
		t.Fatal(err)
	}
	for i := range cases {
		c := &cases[i]
		source := clock.NewMockedTimeSourceAt(time.Unix(0, 0))
		partitions, reads := c.Partitions, 0
		limiter := newTaskListLimiter(source, metrics.NoopScope, &config.TaskListConfig{
			TaskDispatchRPS: c.Initial, TaskDispatchRPSTTL: time.Duration(c.TTL),
			MinTaskThrottlingBurstSize: func() int { return c.MinBurst },
		}, func() int { reads++; return partitions })
		var previous int64
		for j := range c.Steps {
			s := &c.Steps[j]
			source.Advance(time.Duration(s.Now - previous))
			previous = s.Now
			partitions = s.Partitions
			limiter.ReportLimit(s.Report)
			s.Value = limiter.value.Load()
			s.Limit = float64(limiter.Limit())
			s.Burst = limiter.backing.Burst()
			s.LastUpdate = limiter.lastUpdate.Load().UnixNano()
			s.LastReceived = limiter.lastReceivedValue.Load().UnixNano()
			s.AppliedPartitions = limiter.partitions
			s.Reads = reads
		}
	}
	data, err = json.MarshalIndent(cases, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(os.Getenv("CADENCE_MATCHING_RATE_OUTPUT"), append(data, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
}
