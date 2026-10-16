package tasklist

import (
	"context"
	"encoding/json"
	apiv1 "github.com/uber/cadence-idl/go/proto/api/v1"
	matchingv1 "github.com/uber/cadence/.gen/proto/matching/v1"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/types"
	mapper "github.com/uber/cadence/common/types/mapper/proto"
	"go.uber.org/mock/gomock"
	"os"
	"strconv"
	"testing"
)

func TestRustMatchingIdentifiers(t *testing.T) {
	type fixture struct {
		Name      string            `json:"name"`
		Root      string            `json:"root"`
		Partition int               `json:"partition"`
		Error     string            `json:"error"`
		Parents   map[string]string `json:"parents"`
	}
	data, err := os.ReadFile(os.Getenv("CADENCE_MATCHING_INPUT"))
	if err != nil {
		t.Fatal(err)
	}
	var fixtures []fixture
	if err = json.Unmarshal(data, &fixtures); err != nil {
		t.Fatal(err)
	}
	for i := range fixtures {
		f := &fixtures[i]
		id, err := NewIdentifier("domain", f.Name, 0)
		if err != nil {
			f.Error = err.Error()
			continue
		}
		f.Root = id.GetRoot()
		f.Partition = id.Partition()
		f.Parents = map[string]string{}
		for _, degree := range []int{0, 1, 2, 3, 20, int(^uint(0) >> 1)} {
			f.Parents[strconv.Itoa(degree)] = id.Parent(degree)
		}
	}
	data, err = json.MarshalIndent(fixtures, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(os.Getenv("CADENCE_MATCHING_OUTPUT"), append(data, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
}

type rustPartitionConfig struct {
	Version int64            `json:"version"`
	Read    map[int][]string `json:"read_partitions"`
	Write   map[int][]string `json:"write_partitions"`
}

func (c rustPartitionConfig) internal() *types.TaskListPartitionConfig {
	read := map[int]*types.TaskListPartition{}
	write := map[int]*types.TaskListPartition{}
	for id, groups := range c.Read {
		read[id] = &types.TaskListPartition{IsolationGroups: groups}
	}
	for id, groups := range c.Write {
		write[id] = &types.TaskListPartition{IsolationGroups: groups}
	}
	return &types.TaskListPartitionConfig{Version: c.Version, ReadPartitions: read, WritePartitions: write}
}
func rustPartitionOutput(c *types.TaskListPartitionConfig) *rustPartitionConfig {
	if c == nil {
		return nil
	}
	out := &rustPartitionConfig{Version: c.Version, Read: map[int][]string{}, Write: map[int][]string{}}
	for id, p := range c.ReadPartitions {
		out.Read[id] = p.IsolationGroups
	}
	for id, p := range c.WritePartitions {
		out.Write[id] = p.IsolationGroups
	}
	return out
}
func TestRustMatchingPartitions(t *testing.T) {
	type result struct {
		Error  string               `json:"error"`
		Config *rustPartitionConfig `json:"config"`
		Writes int                  `json:"writes"`
	}
	type fixture struct {
		Updates []rustPartitionConfig `json:"updates"`
		Results []result              `json:"results"`
	}
	data, err := os.ReadFile(os.Getenv("CADENCE_MATCHING_INPUT"))
	if err != nil {
		t.Fatal(err)
	}
	var cases []fixture
	if err = json.Unmarshal(data, &cases); err != nil {
		t.Fatal(err)
	}
	for i := range cases {
		id, _ := NewIdentifier("domain", "tl", persistence.TaskListTypeDecision)
		manager, deps := setupMocksForTaskListManager(t, id, types.TaskListKindNormal)
		writes := 0
		deps.mockTaskManager.EXPECT().UpdateTaskList(gomock.Any(), gomock.Any()).DoAndReturn(func(context.Context, *persistence.UpdateTaskListRequest) (*persistence.UpdateTaskListResponse, error) {
			writes++
			return &persistence.UpdateTaskListResponse{}, nil
		}).AnyTimes()
		cases[i].Results = nil
		for _, update := range cases[i].Updates {
			_, _, err := manager.updatePartitionConfig(context.Background(), update.internal())
			message := ""
			if err != nil {
				message = err.Error()
			}
			cases[i].Results = append(cases[i].Results, result{Error: message, Config: rustPartitionOutput(manager.partitionConfig), Writes: writes})
		}
	}
	data, err = json.MarshalIndent(cases, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(os.Getenv("CADENCE_MATCHING_OUTPUT"), append(data, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
}

func TestRustMatchingPartitionWire(t *testing.T) {
	// Prost's default map encoding omits an empty value. Pinned gogo
	// decodes that as nil; explicit empty submessage presence is required.
	for _, test := range []struct {
		bytes   []byte
		present bool
	}{
		{[]byte{0x22, 0}, false},
		{[]byte{0x22, 2, 0x12, 0}, true},
	} {
		var api apiv1.TaskListPartitionConfig
		if err := api.Unmarshal(test.bytes); err != nil {
			t.Fatal(err)
		}
		if (api.ReadPartitions[0] != nil) != test.present {
			t.Fatalf("public partition presence: %+v", api.ReadPartitions)
		}
		var internal matchingv1.TaskListPartitionConfig
		if err := internal.Unmarshal(test.bytes); err != nil {
			t.Fatal(err)
		}
		if (internal.ReadPartitions[0] != nil) != test.present {
			t.Fatalf("internal partition presence: %+v", internal.ReadPartitions)
		}
	}

	type fixture struct {
		Count      int32              `json:"count"`
		Partitions map[int32][]string `json:"partitions"`
		Expected   map[int][]string   `json:"expected"`
	}
	data, err := os.ReadFile(os.Getenv("CADENCE_MATCHING_INPUT"))
	if err != nil {
		t.Fatal(err)
	}
	var cases []fixture
	if err = json.Unmarshal(data, &cases); err != nil {
		t.Fatal(err)
	}
	for i := range cases {
		input := map[int32]*apiv1.TaskListPartition{}
		for id, groups := range cases[i].Partitions {
			input[id] = &apiv1.TaskListPartition{IsolationGroups: groups}
		}
		result := mapper.ToAPITaskListPartitionsMap(cases[i].Count, input)
		cases[i].Expected = map[int][]string{}
		for id, p := range result {
			groups := p.IsolationGroups
			if groups == nil {
				groups = []string{}
			}
			cases[i].Expected[id] = groups
		}
	}
	data, err = json.MarshalIndent(cases, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(os.Getenv("CADENCE_MATCHING_OUTPUT"), append(data, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
}
