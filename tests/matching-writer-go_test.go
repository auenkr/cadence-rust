package tasklist

import (
	"encoding/json"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/types"
	"os"
	"testing"
)

func TestRustMatchingWriterBatch(t *testing.T) {
	type fixture struct {
		Maximum   int     `json:"maximum"`
		Queued    int     `json:"queued"`
		Initial   int     `json:"initial"`
		Batch     []int64 `json:"batch"`
		Remaining int     `json:"remaining"`
	}
	cases := []fixture{{Maximum: 100, Queued: 250, Initial: 1}, {Maximum: 0, Queued: 3, Initial: 1}, {Maximum: -1, Queued: 3, Initial: 1}, {Maximum: 1, Queued: 3, Initial: 1}, {Maximum: 100, Queued: 0, Initial: 1}, {Maximum: 2, Queued: 1, Initial: 1}, {Maximum: 2, Queued: 5, Initial: 2}}
	for i := range cases {
		c := &cases[i]
		id, err := NewIdentifier("domain", "queue", 0)
		if err != nil {
			t.Fatal(err)
		}
		manager, _ := setupMocksForTaskListManager(t, id, types.TaskListKindNormal)
		writer := manager.taskWriter
		writer.config.MaxTaskBatchSize = func() int { return c.Maximum }
		for j := 0; j < c.Queued; j++ {
			writer.appendCh <- &writeTaskRequest{taskInfo: &persistence.TaskInfo{ScheduleID: int64(c.Initial + j)}}
		}
		requests := []*writeTaskRequest{}
		for j := 0; j < c.Initial; j++ {
			requests = append(requests, &writeTaskRequest{taskInfo: &persistence.TaskInfo{ScheduleID: int64(j)}})
		}
		c.Batch = []int64{}
		for _, request := range writer.getWriteBatch(requests) {
			c.Batch = append(c.Batch, request.taskInfo.ScheduleID)
		}
		c.Remaining = len(writer.appendCh)
	}
	data, err := json.MarshalIndent(cases, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(os.Getenv("CADENCE_WRITER_OUTPUT"), append(data, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
}
