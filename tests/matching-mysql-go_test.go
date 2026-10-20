package sql_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/uber/cadence/common/config"
	"github.com/uber/cadence/common/dynamicconfig/dynamicproperties"
	"github.com/uber/cadence/common/log"
	p "github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/persistence/serialization"
	sqlstore "github.com/uber/cadence/common/persistence/sql"
)

// TestRustMatchingMySQLInterop is injected into go/common/persistence/sql by
// the opt-in Python runner. Its Go TaskManager and DomainStore are the writer
// and reader oracle around the Rust adapter process.
func TestRustMatchingMySQLInterop(t *testing.T) {
	dc := p.NewDefaultDynamicConfiguration()
	encoding := "thriftrw"
	dc.SerializationEncoding = func(...dynamicproperties.FilterOption) string { return encoding }
	parser, err := serialization.NewParser(dc)
	if err != nil {
		t.Fatal(err)
	}
	factory := sqlstore.NewFactory(config.SQL{
		PluginName:      "mysql",
		User:            envOr("CADENCE_MYSQL_USER", "root"),
		Password:        envOr("CADENCE_MYSQL_PASSWORD", "cadence"),
		DatabaseName:    envOr("CADENCE_MYSQL_DATABASE", "cadence"),
		ConnectProtocol: "tcp",
		ConnectAddr:     envOr("CADENCE_MYSQL_ADDR", "127.0.0.1:33307"),
		NumShards:       1,
		MaxConns:        16,
		MaxIdleConns:    8,
		MaxConnLifetime: 5 * time.Minute,
	}, "active", log.NewNoop(), parser, dc)
	ctx := context.Background()

	phase := os.Getenv("CADENCE_MYSQL_PHASE")
	if phase == "seed" {
		seedGoDomainRows(t, ctx, factory, func(value string) { encoding = value })
		seedGoTaskRows(t, ctx, factory, func(value string) { encoding = value })
		return
	}
	if phase != "verify-rust" {
		t.Fatalf("unknown CADENCE_MYSQL_PHASE %q", phase)
	}
	verifyRustTaskRows(t, ctx, factory)
	verifyRustExpiryCollection(t, ctx, factory)
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func seedGoDomainRows(t *testing.T, ctx context.Context, factory *sqlstore.Factory, setEncoding func(string)) {
	t.Helper()
	store, err := factory.NewDomainStore()
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for index, domain := range []struct{ id, name string }{
		{"11111111-1111-4111-8111-111111111111", "go-domain-a"},
		{"22222222-2222-4222-8222-222222222222", "go-domain-b"},
	} {
		if index == 1 {
			setEncoding("thriftrw_snappy")
		}
		_, err := store.CreateDomain(ctx, &p.InternalCreateDomainRequest{
			Info:   &p.DomainInfo{ID: domain.id, Name: domain.name, Status: 1, Data: map[string]string{"writer": "go"}},
			Config: &p.InternalDomainConfig{Retention: 24 * time.Hour},
			ReplicationConfig: &p.InternalDomainReplicationConfig{
				ActiveClusterName: "active",
				Clusters:          []*p.ClusterReplicationConfig{{ClusterName: "active"}},
			},
			ConfigVersion:   1,
			FailoverVersion: 1,
		})
		if err != nil {
			t.Fatalf("Go DomainStore CreateDomain(%s): %v", domain.name, err)
		}
	}
	setEncoding("thriftrw")
	metadata, err := store.GetMetadata(ctx)
	if err != nil || metadata.NotificationVersion < 3 {
		t.Fatalf("Go metadata version=%+v err=%v", metadata, err)
	}
}

func seedGoTaskRows(t *testing.T, ctx context.Context, factory *sqlstore.Factory, setEncoding func(string)) {
	t.Helper()
	store, err := factory.NewTaskStore()
	if err != nil {
		t.Fatal(err)
	}
	manager := p.NewTaskManager(store)
	defer manager.Close()
	lease, err := manager.LeaseTaskList(ctx, &p.LeaseTaskListRequest{
		DomainID:     "11111111-1111-4111-8111-111111111111",
		TaskList:     "go-queue",
		TaskType:     1,
		TaskListKind: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if lease.TaskListInfo.RangeID != 1 {
		t.Fatalf("first Go lease range=%d, want 1", lease.TaskListInfo.RangeID)
	}
	_, err = manager.CreateTasks(ctx, &p.CreateTasksRequest{
		TaskListInfo: lease.TaskListInfo,
		Tasks:        []*p.CreateTaskInfo{{TaskID: 10, Data: &p.TaskInfo{DomainID: lease.TaskListInfo.DomainID, WorkflowID: "go-workflow-10", RunID: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", ScheduleID: 10}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	setEncoding("thriftrw_snappy")
	_, err = manager.CreateTasks(ctx, &p.CreateTasksRequest{
		TaskListInfo: lease.TaskListInfo,
		Tasks:        []*p.CreateTaskInfo{{TaskID: 20, Data: &p.TaskInfo{DomainID: lease.TaskListInfo.DomainID, WorkflowID: "go-workflow-20", RunID: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", ScheduleID: 20}}},
	})
	setEncoding("thriftrw")
	if err != nil {
		t.Fatal(err)
	}
}

func verifyRustTaskRows(t *testing.T, ctx context.Context, factory *sqlstore.Factory) {
	t.Helper()
	store, err := factory.NewTaskStore()
	if err != nil {
		t.Fatal(err)
	}
	manager := p.NewTaskManager(store)
	defer manager.Close()
	queue, err := manager.GetTaskList(ctx, &p.GetTaskListRequest{
		DomainID: "11111111-1111-4111-8111-111111111111", TaskList: "go-queue", TaskType: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if queue.TaskListInfo.RangeID != 2 || queue.TaskListInfo.AckLevel != 20 {
		t.Fatalf("Rust-updated queue metadata: %+v", queue.TaskListInfo)
	}
	tasks, err := manager.GetTasks(ctx, &p.GetTasksRequest{
		DomainID: "11111111-1111-4111-8111-111111111111", TaskList: "go-queue", TaskType: 1, BatchSize: 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks.Tasks) != 3 || tasks.Tasks[2].TaskID != 30 || tasks.Tasks[2].WorkflowID != "rust-workflow-30" {
		t.Fatalf("Go read of Rust task rows: %+v", tasks.Tasks)
	}
	concurrent, err := manager.GetTaskList(ctx, &p.GetTaskListRequest{
		DomainID: "11111111-1111-4111-8111-111111111111", TaskList: "concurrent-lease", TaskType: 1,
	})
	if err != nil || concurrent.TaskListInfo.RangeID != 5 {
		t.Fatalf("Go read concurrent Rust leases: list=%+v err=%v", concurrent, err)
	}
}

func verifyRustExpiryCollection(t *testing.T, ctx context.Context, factory *sqlstore.Factory) {
	t.Helper()
	store, err := factory.NewTaskStore()
	if err != nil {
		t.Fatal(err)
	}
	manager := p.NewTaskManager(store)
	defer manager.Close()
	expired, err := manager.GetTaskList(ctx, &p.GetTaskListRequest{
		DomainID: "11111111-1111-4111-8111-111111111111", TaskList: "gc-expired", TaskType: 1,
	})
	if err != nil || expired.TaskListInfo.Expiry.UnixNano() != 86_400_000_000_000 {
		t.Fatalf("Go read of Rust expiry: list=%+v err=%v", expired, err)
	}
	err = manager.DeleteTaskList(ctx, &p.DeleteTaskListRequest{
		DomainID:     "11111111-1111-4111-8111-111111111111",
		TaskListName: "gc-expired",
		TaskListType: 1,
		RangeID:      1,
	})
	if err != nil {
		t.Fatalf("Go TaskManager could not delete Rust expired queue: %v", err)
	}
	if _, err := manager.GetTaskList(ctx, &p.GetTaskListRequest{
		DomainID: "11111111-1111-4111-8111-111111111111", TaskList: "gc-expired", TaskType: 1,
	}); err == nil {
		t.Fatal("Go TaskManager still reads the Rust queue after its fenced expiry delete")
	}
}
