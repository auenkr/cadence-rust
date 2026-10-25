package tasklist

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
	sqloracle "github.com/uber/cadence/common/persistence/sql"
	"github.com/uber/cadence/common/persistence/sql/sqlplugin"
	"github.com/uber/cadence/service/matching/config"
)

const oracleDomainID = "00000000-0000-4000-8000-000000000001"

type oracleDB struct{ sqlplugin.DB }

func (oracleDB) SupportsTTL() bool            { return false }
func (oracleDB) GetTotalNumDBShards() int     { return 1 }
func (oracleDB) IsDupEntryError(error) bool   { return false }
func (oracleDB) IsNotFoundError(error) bool   { return false }
func (oracleDB) IsTimeoutError(error) bool    { return false }
func (oracleDB) IsThrottlingError(error) bool { return false }
func (oracleDB) BeginTx(context.Context, int) (sqlplugin.Tx, error) {
	return nil, errors.New("oracle reached SQL transaction boundary")
}

type oracleTaskManager struct {
	persistence.TaskManager
	store persistence.TaskStore
}

func (m oracleTaskManager) CreateTasks(ctx context.Context, req *persistence.CreateTasksRequest) (*persistence.CreateTasksResponse, error) {
	return m.store.CreateTasks(ctx, req)
}

func TestRawRunIDSQLWriterOracle(t *testing.T) {
	for _, tc := range []struct {
		name, runID string
		fatal       bool
	}{{"malformed", "not-a-uuid", true}, {"empty", "", false}, {"valid", "00000000-0000-4000-8000-000000000002", false}} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestRawRunIDSQLWriterOracleChild$")
			cmd.Env = append(os.Environ(), "CADENCE_WRITER_ORACLE_RUN_ID="+tc.runID)
			out, err := cmd.CombinedOutput()
			output := string(out)
			if !strings.Contains(output, "ORACLE_BEFORE_APPEND") {
				t.Fatalf("child did not reach append path: err=%v output=%s", err, output)
			}
			if tc.fatal {
				if err == nil || strings.Contains(output, "ORACLE_APPEND_ACK") {
					t.Fatalf("malformed SQL RunID did not terminate before append acknowledgement: err=%v output=%s", err, output)
				}
				for _, frame := range []string{"sql_task_store.go", "task_writer.go", "MustParseUUID"} {
					if !strings.Contains(output, frame) {
						t.Fatalf("panic output lacks real-path frame %q: %s", frame, output)
					}
				}
				return
			}
			if err != nil || !strings.Contains(output, "ORACLE_APPEND_ACK error=") || !strings.Contains(output, "oracle reached SQL transaction boundary") {
				t.Fatalf("empty/valid RunID did not return controlled persistence error: err=%v output=%s", err, output)
			}
		})
	}
}

func TestRawRunIDSQLWriterOracleChild(t *testing.T) {
	runID, ok := os.LookupEnv("CADENCE_WRITER_ORACLE_RUN_ID")
	if !ok {
		return
	}
	store, err := sqloracle.NewTaskStoreForWriterOracle(oracleDB{})
	if err != nil {
		t.Fatal(err)
	}
	db := newTaskListDB(oracleTaskManager{store: store}, oracleDomainID, "oracle", "oracle-task-list", 0, 0, log.NewNoop())
	cfg := &config.TaskListConfig{
		AppendTaskTimeout: func() time.Duration { return 30 * time.Second },
		MaxTaskBatchSize:  func() int { return 1 },
		RangeSize:         100,
	}
	w := &taskWriter{
		db: db, config: cfg, appendCh: make(chan *writeTaskRequest, 1),
		taskIDBlock: taskIDBlock{start: 1, end: 100}, stopCh: make(chan struct{}),
		fatalCh: make(chan struct{}), logger: log.NewNoop(), scope: metrics.NoopScope,
	}
	go w.taskWriterLoop()
	fmt.Println("ORACLE_BEFORE_APPEND")
	_, err = w.appendTask(context.Background(), &persistence.TaskInfo{
		DomainID: oracleDomainID, WorkflowID: "oracle-workflow", RunID: runID, ScheduleID: 1,
	})
	fmt.Printf("ORACLE_APPEND_ACK error=%v\n", err)
	w.Stop()
}
