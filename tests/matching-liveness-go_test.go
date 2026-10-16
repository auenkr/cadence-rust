package liveness

import (
	"encoding/json"
	"github.com/uber/cadence/common/clock"
	"os"
	"testing"
	"time"
)

func TestRustMatchingLiveness(t *testing.T) {
	type step struct {
		Delta int64 `json:"delta"`
		Mark  bool  `json:"mark,omitempty"`
		Alive bool  `json:"alive"`
	}
	type fixture struct {
		TTL   int64  `json:"ttl"`
		Steps []step `json:"steps"`
	}
	cases := []fixture{
		{TTL: 100, Steps: []step{{Delta: 0}, {Delta: 50}, {Delta: 50}, {Mark: true}, {Delta: 99}, {Delta: 1}, {Delta: -200}, {Mark: true}, {Delta: 1}}},
		{TTL: 0, Steps: []step{{Delta: 0}, {Delta: -1}, {Mark: true}}},
		{TTL: -10, Steps: []step{{Delta: 0}, {Delta: -11}, {Mark: true}}},
	}
	for i := range cases {
		c := &cases[i]
		source := clock.NewMockedTimeSourceAt(time.Unix(0, 1000000000))
		live := NewLiveness(source, time.Duration(c.TTL), func() {})
		for j := range c.Steps {
			s := &c.Steps[j]
			source.Advance(time.Duration(s.Delta))
			if s.Mark {
				live.MarkAlive()
			}
			s.Alive = live.IsAlive()
		}
	}
	data, err := json.MarshalIndent(cases, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(os.Getenv("CADENCE_LIVENESS_OUTPUT"), append(data, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
}
