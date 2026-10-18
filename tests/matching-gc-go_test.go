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
		batchReads := 0
		manager.config.MaxTaskDeleteBatchSize = func() int { batchReads++; return c.Batch }
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
		if want := map[bool]int{true: 0, false: 1}[c.Busy]; batchReads != want {
			t.Fatalf("%s: batch property reads=%d want=%d", c.Name, batchReads, want)
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

func TestRustMatchingGCSignedBounds(t *testing.T) {
	id, err := NewIdentifier("domain", "queue", 0)
	if err != nil {
		t.Fatal(err)
	}
	manager, deps := setupMocksForTaskListManager(t, id, types.TaskListKindNormal)
	manager.config.MaxTimeBetweenTaskDeletes = time.Hour

	manager.config.MaxTaskDeleteBatchSize = func() int { return 0 }
	zeroCalls := 0
	deps.mockTaskManager.EXPECT().CompleteTasksLessThan(gomock.Any(), gomock.Any()).DoAndReturn(func(context.Context, *persistence.CompleteTasksLessThanRequest) (*persistence.CompleteTasksLessThanResponse, error) {
		zeroCalls++
		return nil, errors.New("SQLite plugin missing limit parameter")
	}).Times(1)
	manager.taskGC.RunNow(1)
	if zeroCalls != 1 || manager.taskGC.ackLevel != 0 {
		t.Fatalf("zero batch: calls=%d ack=%d", zeroCalls, manager.taskGC.ackLevel)
	}

	manager.config.MaxTaskDeleteBatchSize = func() int { return -2 }
	negativeCalls := 0
	deps.mockTaskManager.EXPECT().CompleteTasksLessThan(gomock.Any(), gomock.Any()).DoAndReturn(func(context.Context, *persistence.CompleteTasksLessThanRequest) (*persistence.CompleteTasksLessThanResponse, error) {
		negativeCalls++
		if negativeCalls == 3 {
			return nil, errors.New("bounded oracle stop")
		}
		return &persistence.CompleteTasksLessThanResponse{TasksCompleted: 3}, nil
	}).Times(3)
	func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				t.Fatalf("unexpected panic: %v", recovered)
			}
		}()
		manager.taskGC.RunNow(1)
	}()
	if negativeCalls != 3 || manager.taskGC.ackLevel != 0 {
		t.Fatalf("negative batch bounded attempts: calls=%d ack=%d", negativeCalls, manager.taskGC.ackLevel)
	}
}

func TestRustMatchingGCLiveBatchReadFrequency(t *testing.T) {
	id, err := NewIdentifier("domain", "queue", 0)
	if err != nil {
		t.Fatal(err)
	}
	manager, deps := setupMocksForTaskListManager(t, id, types.TaskListKindNormal)
	configured, reads := 1, 0
	manager.config.MaxTaskDeleteBatchSize = func() int { reads++; return configured }
	var limits []int
	deps.mockTaskManager.EXPECT().CompleteTasksLessThan(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, request *persistence.CompleteTasksLessThanRequest) (*persistence.CompleteTasksLessThanResponse, error) {
		limits = append(limits, request.Limit)
		if len(limits) == 1 {
			configured = 2
			return &persistence.CompleteTasksLessThanResponse{TasksCompleted: 1}, nil
		}
		return &persistence.CompleteTasksLessThanResponse{TasksCompleted: 0}, nil
	}).Times(3)
	manager.taskGC.RunNow(1)
	manager.taskGC.RunNow(2)
	if reads != 2 {
		t.Fatalf("batch property sampled %d times for two attempts", reads)
	}
	if len(limits) != 3 || limits[0] != 1 || limits[1] != 1 || limits[2] != 2 {
		t.Fatalf("batch values across drain and live update: %v", limits)
	}
}
