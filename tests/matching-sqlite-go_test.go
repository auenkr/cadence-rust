package sql_test

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	_ "github.com/ncruces/go-sqlite3/driver"
	_ "github.com/ncruces/go-sqlite3/embed"
	"github.com/uber/cadence/common/config"
	"github.com/uber/cadence/common/dynamicconfig/dynamicproperties"
	"github.com/uber/cadence/common/log"
	p "github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/persistence/serialization"
	sqlstore "github.com/uber/cadence/common/persistence/sql"
	_ "github.com/uber/cadence/common/persistence/sql/sqlplugin/sqlite"
)

func TestRustMatchingSQLiteInterop(t *testing.T) {
	path := os.Getenv("CADENCE_MATCHING_SQLITE_DB")
	if path == "" {
		t.Fatal("database path required")
	}
	encoding := "thriftrw"
	dc := p.NewDefaultDynamicConfiguration()
	dc.SerializationEncoding = func(...dynamicproperties.FilterOption) string { return encoding }
	parser, err := serialization.NewParser(dc)
	if err != nil {
		t.Fatal(err)
	}
	factory := sqlstore.NewFactory(config.SQL{PluginName: "sqlite", DatabaseName: path, NumShards: 1, MaxConns: 1, MaxIdleConns: 1, MaxConnLifetime: time.Hour}, "cluster0", log.NewNoop(), parser, dc)
	store, err := factory.NewTaskStore()
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	domain := "00000000-0000-0000-0000-000000000001"
	name := "go-to-rust"
	run := "00000000-0000-0000-0000-000000000002"
	now := time.Unix(0, 1700000000123456789)
	source := "00000000-0000-0000-0000-000000000003"
	if os.Getenv("CADENCE_MATCHING_SQLITE_PHASE") == "seed" {
		cross, err := store.LeaseTaskList(ctx, &p.LeaseTaskListRequest{DomainID: domain, TaskList: "cross-domain", CurrentTimeStamp: now})
		if err != nil {
			t.Fatal(err)
		}
		_, err = store.CreateTasks(ctx, &p.CreateTasksRequest{TaskListInfo: cross.TaskListInfo, CurrentTimeStamp: now, Tasks: []*p.CreateTaskInfo{{TaskID: 1, Data: &p.TaskInfo{DomainID: source, WorkflowID: "cross-go", RunID: run}}}})
		if err != nil {
			t.Fatal(err)
		}
		lease, err := store.LeaseTaskList(ctx, &p.LeaseTaskListRequest{DomainID: domain, TaskList: name, TaskListKind: 1, CurrentTimeStamp: now})
		if err != nil {
			t.Fatal(err)
		}
		if lease.TaskListInfo.RangeID != 1 {
			t.Fatal(lease)
		}
		for i, format := range []string{"thriftrw", "thriftrw_snappy"} {
			encoding = format
			_, err = store.CreateTasks(ctx, &p.CreateTasksRequest{TaskListInfo: lease.TaskListInfo, CurrentTimeStamp: now, Tasks: []*p.CreateTaskInfo{{TaskID: int64(i + 1), Data: &p.TaskInfo{DomainID: domain, WorkflowID: "go-workflow", RunID: run, ScheduleID: 2, ScheduleToStartTimeoutSeconds: int32(i * 3), PartitionConfig: map[string]string{"zone": "a"}}}}})
			if err != nil {
				t.Fatal(err)
			}
		}
		info := lease.TaskListInfo
		info.AdaptivePartitionConfig = &p.TaskListPartitionConfig{Version: 3, ReadPartitions: map[int]*p.TaskListPartition{0: {IsolationGroups: []string{"a"}}}, WritePartitions: map[int]*p.TaskListPartition{0: {IsolationGroups: []string{"a"}}}}
		_, err = store.UpdateTaskList(ctx, &p.UpdateTaskListRequest{TaskListInfo: info, CurrentTimeStamp: now.Add(1000)})
		if err != nil {
			t.Fatal(err)
		}
	} else {
		for _, id := range []string{domain, source} {
			tasks, err := store.GetTasks(ctx, &p.GetTasksRequest{DomainID: id, TaskList: "cross-domain", ReadLevel: 0, BatchSize: 10})
			if err != nil {
				t.Fatal(err)
			}
			if id == domain && len(tasks.Tasks) != 0 {
				t.Fatal("source tasks leaked into target domain")
			}
			if id == source && (len(tasks.Tasks) != 2 || tasks.Tasks[1].WorkflowID != "cross-rust" || tasks.Tasks[1].DomainID != source) {
				t.Fatalf("cross-domain tasks: %+v", tasks.Tasks)
			}
		}
		info, err := store.GetTaskList(ctx, &p.GetTaskListRequest{DomainID: domain, TaskList: name})
		if err != nil {
			t.Fatal(err)
		}
		if info.TaskListInfo.RangeID != 2 || info.TaskListInfo.AckLevel != 1 || info.TaskListInfo.LastUpdated.UnixNano() != now.UnixNano()+2000 {
			t.Fatalf("bad Rust metadata: %+v", info.TaskListInfo)
		}
		config := info.TaskListInfo.AdaptivePartitionConfig
		if config == nil || config.Version != 4 || len(config.ReadPartitions) != 2 || len(config.WritePartitions) != 1 {
			t.Fatalf("bad Rust partition config: %+v", config)
		}
		native, err := store.GetTaskList(ctx, &p.GetTaskListRequest{DomainID: domain, TaskList: "native-queue-config"})
		if err != nil {
			t.Fatal(err)
		}
		nativeConfig := native.TaskListInfo.AdaptivePartitionConfig
		if nativeConfig == nil || nativeConfig.Version != 11 || len(nativeConfig.ReadPartitions) != 2 || len(nativeConfig.WritePartitions) != 1 ||
			len(nativeConfig.ReadPartitions[0].IsolationGroups) != 2 || nativeConfig.ReadPartitions[0].IsolationGroups[1] != "b" || nativeConfig.ReadPartitions[5] == nil || len(nativeConfig.ReadPartitions[5].IsolationGroups) != 0 ||
			native.TaskListInfo.AckLevel != 0 || native.TaskListInfo.RangeID != 1 {
			t.Fatalf("bad native Rust queue config: %+v", native.TaskListInfo)
		}
		tasks, err := store.GetTasks(ctx, &p.GetTasksRequest{DomainID: domain, TaskList: name, ReadLevel: 0, BatchSize: 10})
		if err != nil {
			t.Fatal(err)
		}
		if len(tasks.Tasks) != 3 || tasks.Tasks[2].WorkflowID != "rust-workflow" || tasks.Tasks[2].TaskID != 3 || tasks.Tasks[2].RunID != run {
			t.Fatalf("bad Rust tasks: %+v", tasks.Tasks)
		}
		// A rejected append from the previous owner must not leak inserted rows.
		stale := *info.TaskListInfo
		stale.RangeID = 1
		_, err = store.CreateTasks(ctx, &p.CreateTasksRequest{TaskListInfo: &stale, CurrentTimeStamp: now, Tasks: []*p.CreateTaskInfo{{TaskID: 4, Data: &p.TaskInfo{DomainID: domain, WorkflowID: "stale", RunID: run}}}})
		var condition *p.ConditionFailedError
		if !errors.As(err, &condition) {
			t.Fatalf("expected fencing error, got %v", err)
		}
		result, err := store.CompleteTasksLessThan(ctx, &p.CompleteTasksLessThanRequest{DomainID: domain, TaskListName: name, TaskID: 2, Limit: 1})
		if err != nil || result.TasksCompleted != 1 {
			t.Fatalf("bad deletion: %+v %v", result, err)
		}
	}
}
