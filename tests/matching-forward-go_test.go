package quotas

import (
	"testing"
	"time"

	"github.com/uber/cadence/common/clock"
)

// The Rust forwarder uses this exact dynamic limiter configuration.
func TestRustMatchingForwardRate(t *testing.T) {
	clock := clock.NewMockedTimeSource()
	rps := 2.0
	limiter := NewDynamicRateLimiterWithOpts(func() float64 { return rps }, DynamicRateLimiterOpts{
		TTL: 5 * time.Second, MinBurst: 1, TimeSource: clock,
	})
	check := func(want bool) {
		t.Helper()
		if got := limiter.Allow(); got != want {
			t.Fatalf("Allow() = %v, want %v", got, want)
		}
	}
	check(true)
	check(true)
	check(false)
	rps = 0
	clock.Advance(5*time.Second - time.Nanosecond)
	check(true)
	clock.Advance(time.Nanosecond)
	check(false)
	rps = 1
	clock.Advance(5*time.Second - time.Nanosecond)
	check(false)
	clock.Advance(time.Nanosecond)
	check(false)
	clock.Advance(time.Second)
	check(true)
}
