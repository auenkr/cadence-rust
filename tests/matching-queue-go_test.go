package tasklist

import (
	"context"
	"encoding/json"
	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/dynamicconfig/dynamicproperties"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/types"
	"go.uber.org/mock/gomock"
	"os"
	"testing"
	"time"
)

func TestRustMatchingQueueReader(t *testing.T) {
	type fixture struct {
		Read         int64   `json:"read"`
		Max          int64   `json:"max"`
		Range        int     `json:"range"`
		Batch        int     `json:"batch"`
		IDs          []int64 `json:"ids"`
		ExpectedIDs  []int64 `json:"expected_ids"`
		ExpectedRead int64   `json:"expected_read"`
		Finished     bool    `json:"finished"`
	}
	data, err := os.ReadFile(os.Getenv("CADENCE_MATCHING_QUEUE_INPUT"))
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
		manager.config.ReadRangeSize = dynamicproperties.GetIntPropertyFn(c.Range)
		manager.config.GetTasksBatchSize = func() int { return c.Batch }
		deps.mockTaskManager.EXPECT().GetTasks(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, r *persistence.GetTasksRequest) (*persistence.GetTasksResponse, error) {
			rows := []*persistence.TaskInfo{}
			for _, id := range c.IDs {
				if id > r.ReadLevel && id <= *r.MaxReadLevel {
					rows = append(rows, &persistence.TaskInfo{TaskID: id})
					if len(rows) >= r.BatchSize {
						break
					}
				}
			}
			return &persistence.GetTasksResponse{Tasks: rows}, nil
		}).AnyTimes()
		tasks, read, finished, err := manager.taskReader.getTaskBatch(c.Read, c.Max)
		if err != nil {
			t.Fatal(err)
		}
		c.ExpectedIDs = []int64{}
		for _, task := range tasks {
			c.ExpectedIDs = append(c.ExpectedIDs, task.TaskID)
		}
		c.ExpectedRead = read
		c.Finished = finished
	}
	data, err = json.MarshalIndent(cases, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(os.Getenv("CADENCE_MATCHING_QUEUE_OUTPUT"), append(data, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
}
func TestRustMatchingQueueExpiry(t *testing.T) {
	type fixture struct {
		Now     int64 `json:"now"`
		Expiry  int64 `json:"expiry"`
		Expired bool  `json:"expired"`
	}
	data, err := os.ReadFile(os.Getenv("CADENCE_MATCHING_QUEUE_INPUT"))
	if err != nil {
		t.Fatal(err)
	}
	var cases []fixture
	if err = json.Unmarshal(data, &cases); err != nil {
		t.Fatal(err)
	}
	for i := range cases {
		c := &cases[i]
		reader := &taskReader{timeSource: clock.NewMockedTimeSourceAt(time.Unix(0, c.Now))}
		expiry := time.Unix(0, c.Expiry)
		if c.Expiry == (time.Time{}).UnixNano() {
			expiry = time.Time{}
		}
		c.Expired = reader.isTaskExpired(&persistence.TaskInfo{Expiry: expiry})
	}
	data, err = json.MarshalIndent(cases, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(os.Getenv("CADENCE_MATCHING_QUEUE_OUTPUT"), append(data, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
}

func TestRustMatchingQueueAdmission(t *testing.T) {
	type fixture struct {
		Consumed  int   `json:"consumed"`
		Read      int64 `json:"read"`
		Backlog   int64 `json:"backlog"`
		Completed bool  `json:"completed"`
	}
	data, err := os.ReadFile(os.Getenv("CADENCE_MATCHING_QUEUE_INPUT"))
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
		manager, _ := setupMocksForTaskListManager(t, id, types.TaskListKindNormal)
		reader := manager.taskReader
		buffer := make(chan *persistence.TaskInfo)
		reader.taskBuffers = map[string]chan *persistence.TaskInfo{"": buffer}
		reader.getIsolationGroupForTask = func(context.Context, *persistence.TaskInfo) (string, time.Duration) { return "", 0 }
		result := make(chan bool, 1)
		go func() {
			result <- reader.addTasksToBuffer([]*persistence.TaskInfo{{TaskID: 1}, {TaskID: 2}, {TaskID: 3}})
		}()
		for j := 0; j < c.Consumed; j++ {
			select {
			case <-buffer:
			case <-time.After(time.Second):
				t.Fatal("buffer admission stalled")
			}
		}
		deadline := time.Now().Add(time.Second)
		for reader.taskAckManager.GetReadLevel() < int64(c.Consumed+1) {
			if time.Now().After(deadline) {
				t.Fatal("blocked row was not registered")
			}
			time.Sleep(time.Millisecond)
		}
		reader.cancelFunc()
		select {
		case c.Completed = <-result:
		case <-time.After(time.Second):
			t.Fatal("cancelled reader did not exit")
		}
		c.Read = reader.taskAckManager.GetReadLevel()
		c.Backlog = reader.taskAckManager.GetBacklogCount()
	}
	data, err = json.MarshalIndent(cases, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(os.Getenv("CADENCE_MATCHING_QUEUE_OUTPUT"), append(data, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
}
