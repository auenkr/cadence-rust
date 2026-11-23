package serialization

import (
	"encoding/hex"
	"fmt"
	"testing"
)

// This oracle prints deterministic sqlblobs payloads on the GitHub runner so
// the Rust persistence codec can be checked against the exact Go ThriftRW
// implementation without requiring a Go build on the development host.
func TestPrintSQLBlobsGoldenHex(t *testing.T) {
	historyTree := historyTreeInfoToThrift(historyTreeInfoTestData)
	fixtures := []struct {
		name string
		value thriftRWType
	}{
		{"ShardInfo", shardInfoToThrift(shardInfoTestData)},
		{"WorkflowExecutionInfo", workflowExecutionInfoToThrift(workflowExecutionInfoTestData)},
		{"ActivityInfo", activityInfoToThrift(activityInfoTestData)},
		{"ChildExecutionInfo", childExecutionInfoToThrift(childExecutionInfoTestData)},
		{"SignalInfo", signalInfoToThrift(signalInfoTestData)},
		{"RequestCancelInfo", requestCancelInfoToThrift(requestCancelInfoTestData)},
		{"TimerInfo", timerInfoToThrift(timerInfoTestData)},
		{"TransferTaskInfo", transferTaskInfoToThrift(transferTaskInfoTestData)},
		{"TimerTaskInfo", timerTaskInfoToThrift(timerTaskInfoTestData)},
		{"ReplicationTaskInfo", replicationTaskInfoToThrift(replicationTaskInfoTestData)},
		{"CrossClusterTaskInfo", crossClusterTaskInfoToThrift(transferTaskInfoTestData)},
		{"HistoryTreeInfo", historyTree},
		{"HistoryBranchRange", historyTree.Ancestors[0]},
	}
	for _, fixture := range fixtures {
		data, err := thriftRWEncode(fixture.value)
		if err != nil {
			t.Fatalf("encode %s: %v", fixture.name, err)
		}
		fmt.Printf("SQLBLOBS_GOLDEN %s %s\n", fixture.name, hex.EncodeToString(data))
	}
}
