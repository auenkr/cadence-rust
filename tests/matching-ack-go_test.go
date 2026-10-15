package messaging

import (
	"encoding/json"
	"github.com/uber/cadence/common/log"
	"os"
	"testing"
)

func TestRustMatchingAck(t *testing.T) {
	type step struct {
		Op string `json:"op"`
		ID int64  `json:"id"`
	}
	type result struct {
		Error  string   `json:"error"`
		Levels [3]int64 `json:"levels"`
	}
	type fixture struct {
		Steps   []step   `json:"steps"`
		Results []result `json:"results"`
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
		manager := NewAckManager(log.NewNoop())
		cases[i].Results = nil
		for _, op := range cases[i].Steps {
			message := ""
			switch op.Op {
			case "read":
				if err := manager.ReadItem(op.ID); err != nil {
					message = err.Error()
				}
			case "ack":
				manager.AckItem(op.ID)
			case "setRead":
				manager.SetReadLevel(op.ID)
			case "setAck":
				manager.SetAckLevel(op.ID)
			default:
				t.Fatalf("unknown operation %s", op.Op)
			}
			cases[i].Results = append(cases[i].Results, result{Error: message, Levels: [3]int64{manager.GetReadLevel(), manager.GetAckLevel(), manager.GetBacklogCount()}})
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
