package tasklist

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/types"
	"go.uber.org/mock/gomock"
)

func TestRustMatchingGC(t *testing.T) {
	type fixture struct {
		Name           string    `json:"name"`
		Cursor         int64     `json:"cursor"`
		Ack            int64     `json:"ack"`
		Batch          int       `json:"batch"`
		Force          bool      `json:"force,omitempty"`
		Last           bool      `json:"last,omitempty"`
		Elapsed        int64     `json:"elapsed_ns,omitempty"`
		Busy           bool      `json:"busy,omitempty"`
		Results        []int     `json:"results"`
		Calls          [][]int64 `json:"calls"`
		ExpectedCursor int64     `json:"expected_cursor"`
		Attempted      bool      `json:"attempted"`
	}
	data, err := os.ReadFile(os.Getenv("CADENCE_GC_INPUT"))
	if err != nil {
		t.Fatal(err)
	}
	var cases []fixture
	if err = json.Unmarshal(data, &cases); err != nil {
		t.Fatal(err)
	}
	for i := range cases {
		c := &cases[i]
		id, err := NewIdentifier("domain", "queue", 0)
		if err != nil {
			t.Fatal(err)
		}
		manager, deps := setupMocksForTaskListManager(t, id, types.TaskListKindNormal)
		manager.config.MaxTaskDeleteBatchSize = func() int { return c.Batch }
		manager.config.MaxTimeBetweenTaskDeletes = time.Second
		gc := manager.taskGC
		gc.ackLevel = c.Cursor
		if c.Last {
			gc.lastDeleteTime = time.Now().Add(-time.Duration(c.Elapsed))
		}
		initial := gc.lastDeleteTime
		if c.Busy {
			gc.lock = 1
		}
		c.Calls = [][]int64{}
		position := 0
		deps.mockTaskManager.EXPECT().CompleteTasksLessThan(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, request *persistence.CompleteTasksLessThanRequest) (*persistence.CompleteTasksLessThanResponse, error) {
			c.Calls = append(c.Calls, []int64{request.TaskID, int64(request.Limit)})
			if position >= len(c.Results) {
				t.Fatalf("%s: unexpected delete", c.Name)
			}
			n := c.Results[position]
			position++
			if n < 0 {
				return nil, errors.New("injected deletion error")
			}
			return &persistence.CompleteTasksLessThanResponse{TasksCompleted: n}, nil
		}).AnyTimes()
		if c.Force {
			gc.RunNow(c.Ack)
		} else {
			gc.Run(c.Ack)
		}
		if position != len(c.Results) {
			t.Fatalf("%s: unused deletion responses", c.Name)
		}
		c.ExpectedCursor = gc.ackLevel
		c.Attempted = !gc.lastDeleteTime.Equal(initial)
	}
	data, err = json.MarshalIndent(cases, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(os.Getenv("CADENCE_GC_OUTPUT"), append(data, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
}
