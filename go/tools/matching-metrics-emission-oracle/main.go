// Emits a deterministic snapshot from the production metrics client and tally test sink.
package main

import (
	"context"
	"encoding/json"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/uber-go/tally"
	"github.com/uber/cadence/common/config"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/persistence/wrappers/metered"
	"github.com/uber/cadence/common/types"
)

type row struct {
	Name  string            `json:"name"`
	Kind  string            `json:"kind"`
	Tags  map[string]string `json:"tags"`
	Count int64             `json:"count,omitempty"`
}

type fixtureTaskManager struct {
	persistence.TaskManager
	calls int
}

func (m *fixtureTaskManager) GetTasks(context.Context, *persistence.GetTasksRequest) (*persistence.GetTasksResponse, error) {
	m.calls++
	if m.calls == 1 {
		return &persistence.GetTasksResponse{}, nil
	}
	if m.calls == 2 {
		return &persistence.GetTasksResponse{Tasks: []*persistence.TaskInfo{{}}}, nil
	}
	return nil, &types.BadRequestError{Message: "metrics-oracle-failure"}
}

func main() {
	mode := "default"
	if len(os.Args) > 1 {
		mode = os.Args[1]
	}
	migration := metrics.MigrationConfig{}
	if strings.HasPrefix(mode, "both") {
		migration.Histogram.Default = "both"
	}
	if mode == "name-on" {
		migration.Histogram.Names = map[string]bool{"syncmatch_latency_per_tl_ns": true}
	}
	if mode == "name-off" {
		migration.Histogram.Default = "both"
		migration.Histogram.Names = map[string]bool{"syncmatch_latency_per_tl_ns": false}
	}
	root := tally.NewTestScope("", nil)
	client := metrics.NewClient(root, metrics.Matching, migration)
	if mode == "persistence" {
		manager := metered.NewTaskManager(&fixtureTaskManager{}, client, log.NewNoop(), &config.Persistence{})
		_, _ = manager.GetTasks(context.Background(), &persistence.GetTasksRequest{})
		_, _ = manager.GetTasks(context.Background(), &persistence.GetTasksRequest{})
		emit(root)
		return
	}
	ctx := client.Scope(metrics.MatchingTaskListMgrScope, metrics.DomainTag("domain-a"), metrics.TaskListTag("queue-a"), metrics.TaskListTypeTag("decision"))
	ctx.IncCounter(metrics.PollSuccessPerTaskListCounter)
	ctx.IncCounter(metrics.PollTimeoutPerTaskListCounter)
	ctx.IncCounter(metrics.ConditionFailedErrorPerTaskListCounter)
	ctx.IncCounter(metrics.SyncMatchForwardTaskThrottleErrorPerTasklist)
	ctx.UpdateGauge(metrics.PollerPerTaskListCounter, 2)
	ctx.RecordTimer(metrics.SyncMatchLocalPollLatencyPerTaskList, 5*time.Millisecond)
	ctx.ExponentialHistogram(metrics.SyncMatchLocalPollLatencyPerTaskListHistogram, 5*time.Millisecond)
	request := client.Scope(metrics.MatchingAddDecisionTaskScope, metrics.DomainTag("domain-a"), metrics.TaskListTag("queue/a"), metrics.CallerTag("history"), metrics.TransportTag("grpc"))
	request.RecordTimer(metrics.SyncMatchLatencyPerTaskList, 5*time.Millisecond)
	request.ExponentialHistogram(metrics.SyncMatchLatencyPerTaskListHistogram, 5*time.Millisecond)
	client.Scope(metrics.MatchingAddTaskScope, metrics.DomainTag("domain-a"), metrics.TaskListTag("queue/a"), metrics.TaskListTypeTag("decision_task"), metrics.MatchingHostTag("matching-test")).IncCounter(metrics.CadenceTasklistRequests)
	client.Scope(metrics.MatchingPollForDecisionTaskScope, metrics.DomainTag("domain-a"), metrics.TaskListTag("queue/a")).IncCounter(metrics.ForwardedPerTaskListCounter)
	persistence := client.Scope(metrics.PersistenceGetTasksScope, metrics.TaskCategoryTag("none"))
	persistence.IncCounter(metrics.PersistenceEmptyResponseCounter)
	persistence.RecordHistogramValue(metrics.PersistenceResponseRowSize, 3)
	persistence.RecordHistogramValue(metrics.PersistenceResponsePayloadSize, 4096)

	emit(root)
}

func emit(root tally.TestScope) {
	s := root.Snapshot()
	rows := []row{}
	for _, v := range s.Counters() {
		rows = append(rows, row{v.Name(), "counter", v.Tags(), v.Value()})
	}
	for _, v := range s.Gauges() {
		rows = append(rows, row{v.Name(), "gauge", v.Tags(), 0})
	}
	for _, v := range s.Timers() {
		rows = append(rows, row{v.Name(), "timer", v.Tags(), int64(len(v.Values()))})
	}
	for _, v := range s.Histograms() {
		rows = append(rows, row{v.Name(), "histogram", v.Tags(), int64(len(v.Durations()) + len(v.Values()))})
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Name != rows[j].Name {
			return rows[i].Name < rows[j].Name
		}
		return rows[i].Kind < rows[j].Kind
	})
	_ = json.NewEncoder(os.Stdout).Encode(rows)
}
