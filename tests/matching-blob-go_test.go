package serialization

import (
	"encoding/hex"
	"encoding/json"
	"os"
	"testing"
	"time"
)

func TestRustMatchingBlobFixtures(t *testing.T) {
	type fixture struct {
		Kind   string `json:"kind"`
		Data   any    `json:"data"`
		Thrift string `json:"thrift_hex"`
		Snappy string `json:"snappy_hex"`
	}
	var fixtures []fixture
	tasks := []*TaskInfo{
		{ExpiryTimestamp: time.Unix(0, 0), CreatedTimestamp: time.Unix(0, 0)},
		{WorkflowID: "wf-世界", RunID: UUID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}, ScheduleID: 19, ExpiryTimestamp: time.Time{}, CreatedTimestamp: time.Unix(0, 1700000000123456789)},
		{WorkflowID: "empty", RunID: UUID{}, ScheduleID: -24, ExpiryTimestamp: time.Unix(0, -1), CreatedTimestamp: time.Unix(0, 1), PartitionConfig: map[string]string{}},
		{WorkflowID: "partition", PartitionConfig: map[string]string{"group": "zone-a"}},
	}
	for _, info := range tasks {
		plain, err := (&thriftEncoder{}).taskInfoToBlob(info)
		if err != nil {
			t.Fatal(err)
		}
		compressed, err := (&snappyThriftEncoder{}).taskInfoToBlob(info)
		if err != nil {
			t.Fatal(err)
		}
		var run any
		if info.RunID != nil {
			ids := []int{}
			for _, b := range info.RunID {
				ids = append(ids, int(b))
			}
			run = ids
		}
		data := map[string]any{"workflow_id": info.WorkflowID, "run_id": run, "schedule_id": info.ScheduleID, "expiry_nanos": info.ExpiryTimestamp.UnixNano(), "created_nanos": info.CreatedTimestamp.UnixNano(), "partition_config": info.PartitionConfig}
		fixtures = append(fixtures, fixture{"task", data, hex.EncodeToString(plain), hex.EncodeToString(compressed)})
	}
	configs := []*TaskListPartitionConfig{nil, {}, {Version: 7, NumReadPartitions: 1, NumWritePartitions: 1, ReadPartitions: map[int32]*TaskListPartition{}, WritePartitions: map[int32]*TaskListPartition{0: {}}}, {Version: -24, NumReadPartitions: 1, NumWritePartitions: 1, ReadPartitions: map[int32]*TaskListPartition{3: {IsolationGroups: []string{}}}, WritePartitions: map[int32]*TaskListPartition{3: {IsolationGroups: []string{"a", "世界"}}}}}
	for i, config := range configs {
		info := &TaskListInfo{Kind: int16(i), AckLevel: int64(i - 1), ExpiryTimestamp: time.Unix(0, int64(i)), LastUpdated: time.Unix(0, 1700000000123456789), AdaptivePartitionConfig: config}
		plain, err := (&thriftEncoder{}).taskListInfoToBlob(info)
		if err != nil {
			t.Fatal(err)
		}
		compressed, err := (&snappyThriftEncoder{}).taskListInfoToBlob(info)
		if err != nil {
			t.Fatal(err)
		}
		var adaptive any
		if config != nil {
			partitions := func(input map[int32]*TaskListPartition) any {
				if input == nil {
					return nil
				}
				out := map[int32]any{}
				for id, p := range input {
					out[id] = map[string]any{"isolation_groups": p.IsolationGroups}
				}
				return out
			}
			adaptive = map[string]any{"version": config.Version, "num_read_partitions": config.NumReadPartitions, "num_write_partitions": config.NumWritePartitions, "read_partitions": partitions(config.ReadPartitions), "write_partitions": partitions(config.WritePartitions)}
		}
		data := map[string]any{"kind": info.Kind, "ack_level": info.AckLevel, "expiry_nanos": info.ExpiryTimestamp.UnixNano(), "last_updated_nanos": info.LastUpdated.UnixNano(), "adaptive_partition_config": adaptive}
		fixtures = append(fixtures, fixture{"task_list", data, hex.EncodeToString(plain), hex.EncodeToString(compressed)})
	}
	data, err := json.MarshalIndent(fixtures, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(os.Getenv("CADENCE_MATCHING_BLOB_OUTPUT"), append(data, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
}
