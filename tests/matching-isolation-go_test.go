package tasklist

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/isolationgroup"
	"github.com/uber/cadence/common/log/testlogger"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/types"
	"github.com/uber/cadence/service/matching/poller"
	"go.uber.org/mock/gomock"
)

func TestRustMatchingIsolationFixtures(t *testing.T) {
	type selection struct {
		Name            string   `json:"name"`
		Enabled         bool     `json:"enabled"`
		Sticky          bool     `json:"sticky"`
		Group           string   `json:"group"`
		Known           bool     `json:"known"`
		Drained         bool     `json:"drained"`
		StateError      bool     `json:"state_error"`
		Recent          bool     `json:"recent"`
		PartitionGroups []string `json:"partition_groups"`
		Duration        int64    `json:"duration"`
		Latency         int64    `json:"latency"`
		Expected        []any    `json:"expected"`
	}
	base := selection{Enabled: true, Group: "a", Known: true, Recent: true, PartitionGroups: []string{}, Duration: int64(time.Second)}
	var selections []selection
	for _, name := range []string{"isolated", "disabled", "sticky", "empty", "unknown", "drained", "state-error", "no-pollers", "wrong-partition", "missing-partition", "assigned", "zero-duration", "latency", "minimum-boundary", "below-boundary", "future-task", "negative-duration"} {
		c := base
		c.Name = name
		switch name {
		case "disabled":
			c.Enabled = false
		case "sticky":
			c.Sticky = true
		case "empty":
			c.Group = ""
		case "unknown":
			c.Known = false
		case "drained":
			c.Drained = true
		case "state-error":
			c.StateError = true
		case "no-pollers":
			c.Recent = false
		case "wrong-partition":
			c.PartitionGroups = []string{"b"}
		case "missing-partition":
			c.PartitionGroups = nil
		case "assigned":
			c.PartitionGroups = []string{"a", "b"}
		case "zero-duration":
			c.Duration = 0
			c.Latency = int64(time.Hour)
		case "latency":
			c.Latency = int64(300 * time.Millisecond)
		case "minimum-boundary":
			c.Latency = int64(950 * time.Millisecond)
		case "below-boundary":
			c.Latency = int64(950*time.Millisecond) - 1
		case "future-task":
			c.Latency = -int64(time.Second)
		case "negative-duration":
			c.Duration = -1
		}
		controller := gomock.NewController(t)
		cfg := defaultTestConfig()
		cfg.EnableTasklistIsolation = func(string) bool { return c.Enabled }
		cfg.AllIsolationGroups = func() []string {
			if c.Known {
				return []string{"a"}
			}
			return []string{"b"}
		}
		cfg.TaskIsolationDuration = func(string, string, int) time.Duration { return time.Duration(c.Duration) }
		cfg.TaskIsolationPollerWindow = func(string, string, int) time.Duration { return 2 * time.Second }
		source := clock.NewMockedTimeSource()
		manager := createTestTaskListManagerWithConfig(t, testlogger.New(t), controller, cfg, source)
		if c.Sticky {
			manager.taskListKind = types.TaskListKindSticky
		}
		if c.Recent {
			manager.pollers.StartPoll("poller", func() {}, &poller.Info{Identity: "worker", IsolationGroup: "a"})
		}
		state := isolationgroup.NewMockState(controller)
		var err error
		if c.StateError {
			err = errors.New("state unavailable")
		}
		state.EXPECT().IsDrained(gomock.Any(), gomock.Any(), gomock.Any()).Return(c.Drained, err).AnyTimes()
		manager.isolationState = state
		manager.partitionConfig = &types.TaskListPartitionConfig{ReadPartitions: map[int]*types.TaskListPartition{}}
		if c.PartitionGroups != nil {
			manager.partitionConfig.ReadPartitions[0] = &types.TaskListPartition{IsolationGroups: c.PartitionGroups}
		}
		group, duration := manager.getIsolationGroupForTask(context.Background(), &persistence.TaskInfo{CreatedTime: source.Now().Add(-time.Duration(c.Latency)), PartitionConfig: map[string]string{isolationgroup.GroupKey: c.Group}})
		c.Expected = []any{group, int64(duration)}
		selections = append(selections, c)
	}
	var timeouts []map[string]any
	for _, buffers := range []int{1, 5} {
		for _, rps := range []float64{1000, 0.1, 0} {
			for _, isolation := range []time.Duration{0, time.Second, 5 * time.Second} {
				controller := gomock.NewController(t)
				manager := createTestTaskListManagerWithConfig(t, testlogger.New(t), controller, defaultTestConfig(), clock.NewMockedTimeSource())
				manager.taskReader.taskBuffers = make(map[string]chan *persistence.TaskInfo)
				for i := 0; i < buffers; i++ {
					manager.taskReader.taskBuffers[string(rune('a'+i))] = nil
				}
				manager.taskReader.config.AsyncTaskDispatchTimeout = func() time.Duration { return 3 * time.Second }
				timeouts = append(timeouts, map[string]any{"buffers": buffers, "rps": rps, "isolation": int64(isolation), "expected": int64(manager.taskReader.getDispatchTimeout(rps, isolation))})
			}
		}
	}
	var rewrites []map[string]any
	for _, input := range []map[string]string{nil, {}, {"other": "retained"}, {"isolation-group": "a"}, {"isolation-group": "a", "wf-id": "workflow", "other": "dropped"}, {"isolation-group": "b", "original-isolation-group": "a", "wf-id": "workflow"}, {"isolation-group": "", "original-isolation-group": ""}} {
		for _, group := range []string{"", "a", "b"} {
			info := &persistence.TaskInfo{PartitionConfig: input}
			task := newInternalTask(info, nil, types.TaskSourceDbBacklog, "", false, group)
			rewrites = append(rewrites, map[string]any{"input": input, "group": group, "expected": task.Event.PartitionConfig})
		}
	}
	data, err := json.MarshalIndent(map[string]any{"selection": selections, "timeouts": timeouts, "rewrites": rewrites}, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(os.Getenv("CADENCE_ISOLATION_FIXTURES"), data, 0600); err != nil {
		t.Fatal(err)
	}
}
