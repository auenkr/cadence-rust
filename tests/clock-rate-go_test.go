package clock

import (
	"context"
	"encoding/json"
	"golang.org/x/time/rate"
	"os"
	"strconv"
	"testing"
	"time"
)

type observedRateSource struct {
	MockedTimeSource
	delay time.Duration
}

func (s *observedRateSource) NewTimer(delay time.Duration) Timer {
	s.delay = delay
	return s.MockedTimeSource.NewTimer(delay)
}

type rateDeadlineContext struct {
	context.Context
	deadline time.Time
}

func (c rateDeadlineContext) Deadline() (time.Time, bool) { return c.deadline, true }

func TestRustClockRateOperations(t *testing.T) {
	type step struct {
		Deadline    *int64 `json:"deadline,omitempty"`
		WaitAdvance int64  `json:"wait_advance,omitempty"`
		WaitCancel  bool   `json:"wait_cancel,omitempty"`
		Interleave  bool   `json:"interleave,omitempty"`
		WaitError   string `json:"wait_error,omitempty"`
		WaitDelay   int64  `json:"wait_delay,omitempty"`
		Now         int64  `json:"now"`
		Op          string `json:"op"`
		ID          string `json:"id,omitempty"`
		Rate        string `json:"rate,omitempty"`
		Burst       int    `json:"burst,omitempty"`
		Used        bool   `json:"used,omitempty"`
		Allowed     bool   `json:"allowed"`
		Tokens      string `json:"tokens"`
		Limit       string `json:"limit"`
		ActualBurst int    `json:"actual_burst"`
		Latest      int64  `json:"latest"`
		LatestZero  bool   `json:"latest_zero"`
	}
	type fixture struct {
		Name  string `json:"name"`
		Rate  string `json:"rate"`
		Burst int    `json:"burst"`
		Steps []step `json:"steps"`
	}
	parse := func(value string) float64 {
		f, err := strconv.ParseFloat(value, 64)
		if err != nil {
			t.Fatal(err)
		}
		return f
	}
	data, err := os.ReadFile(os.Getenv("CADENCE_CLOCK_RATE_INPUT"))
	if err != nil {
		t.Fatal(err)
	}
	var cases []fixture
	if err = json.Unmarshal(data, &cases); err != nil {
		t.Fatal(err)
	}
	for i := range cases {
		c := &cases[i]
		source := NewMockedTimeSourceAt(time.Unix(0, 0))
		observed := &observedRateSource{MockedTimeSource: source}
		limiter := NewRateLimiterWithTimeSource(observed, rate.Limit(parse(c.Rate)), c.Burst).(*ratelimiter)
		reservations := map[string]Reservation{}
		var previous int64
		for j := range c.Steps {
			s := &c.Steps[j]
			source.Advance(time.Duration(s.Now - previous))
			previous = s.Now
			switch s.Op {
			case "allow":
				s.Allowed = limiter.Allow()
			case "reserve":
				reservations[s.ID] = limiter.Reserve()
				s.Allowed = reservations[s.ID].Allow()
			case "used":
				reservations[s.ID].Used(s.Used)
			case "check":
				s.Allowed = reservations[s.ID].Allow()
			case "limit":
				limiter.SetLimit(rate.Limit(parse(s.Rate)))
			case "burst":
				limiter.SetBurst(s.Burst)
			case "both":
				limiter.SetLimitAndBurst(rate.Limit(parse(s.Rate)), s.Burst)
			case "tokens":
				limiter.Tokens()
			case "wait":
				base, cancel := context.WithCancel(context.Background())
				var ctx context.Context = base
				if s.Deadline != nil {
					ctx = rateDeadlineContext{Context: base, deadline: time.Unix(0, *s.Deadline)}
				}
				observed.delay = 0
				finished := make(chan error, 1)
				go func() { finished <- limiter.Wait(ctx) }()
				if s.WaitAdvance > 0 || s.WaitCancel {
					registered := make(chan struct{})
					go func() { source.BlockUntil(1); close(registered) }()
					select {
					case <-registered:
					case <-time.After(time.Second):
						cancel()
						t.Fatal("Wait did not register timer")
					}
					source.Advance(time.Duration(s.WaitAdvance))
					previous += s.WaitAdvance
					if s.Interleave {
						limiter.Tokens()
					}
					if s.WaitCancel {
						cancel()
					}
				}
				var waitErr error
				select {
				case waitErr = <-finished:
				case <-time.After(time.Second):
					cancel()
					t.Fatal("Wait did not finish")
				}
				cancel()
				switch waitErr {
				case nil:
					s.WaitError = "ok"
				case errWaitZeroBurst:
					s.WaitError = "zero"
				case errWaitLongerThanDeadline:
					s.WaitError = "deadline"
				case context.Canceled:
					s.WaitError = "cancelled"
				default:
					t.Fatalf("unexpected Wait error %v", waitErr)
				}
				s.WaitDelay = int64(observed.delay)
			default:
				t.Fatalf("unknown op %s", s.Op)
			}
			// Observe without advancing the wrapper's latestNow; Tokens() here would
			// change the cancellation semantics being tested.
			s.Tokens = strconv.FormatFloat(limiter.limiter.TokensAt(limiter.latestNow), 'g', -1, 64)
			s.Limit = strconv.FormatFloat(float64(limiter.Limit()), 'g', -1, 64)
			s.ActualBurst = limiter.Burst()
			s.LatestZero = limiter.latestNow.IsZero()
			if !s.LatestZero {
				s.Latest = limiter.latestNow.UnixNano()
			}
		}
	}
	data, err = json.MarshalIndent(cases, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(os.Getenv("CADENCE_CLOCK_RATE_OUTPUT"), append(data, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
}
