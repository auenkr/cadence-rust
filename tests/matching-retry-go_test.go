package backoff

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/uber/cadence/common/types"
)

type rustMatchingRetryPolicy struct {
	retries int
	pauses  *int
}

func (p rustMatchingRetryPolicy) Expiration() time.Duration { return 0 }
func (p rustMatchingRetryPolicy) ComputeNextDelay(_ time.Duration, attempt int) time.Duration {
	if attempt >= p.retries {
		return -1
	}
	*p.pauses++
	return time.Nanosecond
}

func TestRustMatchingRetryOutcomes(t *testing.T) {
	type fixture struct {
		Errors  []string `json:"errors"`
		Retries int      `json:"retries"`
		Calls   int      `json:"calls"`
		Pauses  int      `json:"pauses"`
		Outcome string   `json:"outcome"`
	}
	cases := []fixture{
		{Errors: []string{"success"}, Retries: 0},
		{Errors: []string{"condition"}, Retries: 3},
		{Errors: []string{"not-found"}, Retries: 3},
		{Errors: []string{"first", "success"}, Retries: 1},
		{Errors: []string{"first", "second", "success"}, Retries: 2},
		{Errors: []string{"first", "second", "third"}, Retries: 0},
		{Errors: []string{"first", "second", "third"}, Retries: 1},
		{Errors: []string{"first", "second", "third"}, Retries: 2},
		{Errors: []string{"first", "condition"}, Retries: 3},
		{Errors: []string{"first", "second", "not-found"}, Retries: 3},
	}
	for i := range cases {
		c := &cases[i]
		retry := NewThrottleRetry(WithRetryPolicy(rustMatchingRetryPolicy{c.Retries, &c.Pauses}), WithRetryableError(func(err error) bool {
			_, ok := err.(*types.InternalServiceError)
			return ok
		}))
		err := retry.Do(context.Background(), func(context.Context) error {
			name := "success"
			if c.Calls < len(c.Errors) {
				name = c.Errors[c.Calls]
			}
			c.Calls++
			switch name {
			case "success":
				return nil
			case "condition", "not-found":
				return &types.EntityNotExistsError{Message: name}
			default:
				return &types.InternalServiceError{Message: name}
			}
		})
		if err == nil {
			c.Outcome = "success"
		} else {
			c.Outcome = err.Error()
		}
	}
	data, err := json.MarshalIndent(cases, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(os.Getenv("CADENCE_MATCHING_RETRY_OUTPUT"), append(data, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
}
