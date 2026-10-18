package tasklist

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/types"
)

// Runs the original package methods through a go test overlay. Channels gate
// each transition; only context deadlines use wall time (see memory note).
func TestRustMatchingWriterNextOracle(t *testing.T) {
	type trace struct {
		Capacity int `json:"capacity"`
		Maximum int `json:"maximum"`
		Batch []int64 `json:"batch"`
		Remaining int `json:"remaining"`
		EnqueueTimeout string `json:"enqueue_timeout"`
		ResponseTimeout string `json:"response_timeout"`
		CancelledAfterEnqueue string `json:"cancelled_after_enqueue"`
		Stopped string `json:"stopped"`
	}
	var out []trace
	for i, maximum := range []int{0, -1, 1, 4} {
		id, err := NewIdentifier("domain", "queue", 0); if err != nil { t.Fatal(err) }
		manager, _ := setupMocksForTaskListManager(t, id, types.TaskListKindNormal)
		configuredCapacity := []int{0, 1, 4, 8}[i]
		manager.config.OutstandingTaskAppendsThreshold = func() int { return configuredCapacity }
		writer := newTaskWriter(manager)
		constructedCapacity := cap(writer.appendCh)
		writer.config.MaxTaskBatchSize = func() int { return maximum }
		writer.appendCh = make(chan *writeTaskRequest, 8)
		for i := 0; i < 5; i++ { writer.appendCh <- &writeTaskRequest{taskInfo:&persistence.TaskInfo{ScheduleID:int64(i+1)}} }
		batch := writer.getWriteBatch([]*writeTaskRequest{{taskInfo:&persistence.TaskInfo{ScheduleID:0}}})
		row := trace{Capacity:constructedCapacity, Maximum:maximum, Remaining:len(writer.appendCh), Batch:[]int64{}}
		for _, req := range batch { row.Batch = append(row.Batch, req.taskInfo.ScheduleID) }
		out = append(out, row)
	}
	// Full buffered queue: deadline expires before enqueue and returns ServiceBusy.
	id, _ := NewIdentifier("domain", "queue", 0); manager, _ := setupMocksForTaskListManager(t, id, types.TaskListKindNormal)
	w := manager.taskWriter; w.config.AppendTaskTimeout = func() time.Duration { return 15*time.Millisecond }; w.appendCh = make(chan *writeTaskRequest, 1); w.appendCh <- &writeTaskRequest{}
	_, err := w.appendTask(context.Background(), &persistence.TaskInfo{})
	full := out[0]; full.EnqueueTimeout = err.Error(); out = append(out, full)
	// Receiver takes the request, then leaves response pending until the same deadline.
	w.appendCh = make(chan *writeTaskRequest, 1)
	done := make(chan error, 1); go func(){ _, e := w.appendTask(context.Background(), &persistence.TaskInfo{}); done <- e }()
	<-w.appendCh
	err = <-done
	response := out[0]; response.ResponseTimeout = err.Error(); out = append(out, response)
	// Cancellation after enqueue leaves the queued request in place.
	w.config.AppendTaskTimeout = func() time.Duration { return time.Second }; w.appendCh = make(chan *writeTaskRequest, 1)
	ctx, cancel := context.WithCancel(context.Background()); done = make(chan error, 1)
	go func(){ _, e := w.appendTask(ctx, &persistence.TaskInfo{}); done <- e }()
	<-w.appendCh; cancel(); err = <-done
	cancelled := out[0]; cancelled.CancelledAfterEnqueue = err.Error(); out = append(out, cancelled)
	// Stop wakes a caller waiting for a response; appendCh is deliberately not closed.
	w2 := *w; w2.stopCh = make(chan struct{}); w2.stopped = 0; w2.appendCh = make(chan *writeTaskRequest, 1)
	done = make(chan error, 1); go func(){ _, e := w2.appendTask(context.Background(), &persistence.TaskInfo{}); done <- e }(); <-w2.appendCh; w2.Stop(); err = <-done
	stopped := out[0]; stopped.Stopped = err.Error(); out = append(out, stopped)
	data, err := json.MarshalIndent(out, "", "  "); if err != nil { t.Fatal(err) }
	if err = os.WriteFile(os.Getenv("CADENCE_WRITER_NEXT_OUTPUT"), append(data, '\n'), 0600); err != nil { t.Fatal(err) }
}
