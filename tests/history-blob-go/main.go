// Produce fixtures using the original Go persistence serializer, without a server.
package main

import (
	"encoding/json"
	"github.com/uber/cadence/common/constants"
	p "github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/types"
	"io"
	"os"
	"path/filepath"
)

func main() {
	if len(os.Args) == 2 && os.Args[1] == "decode" {
		data, err := io.ReadAll(os.Stdin)
		if err != nil {
			panic(err)
		}
		events, err := p.NewPayloadSerializer().DeserializeBatchEvents(p.NewDataBlob(data, constants.EncodingTypeThriftRW))
		if err != nil {
			panic(err)
		}
		if err := json.NewEncoder(os.Stdout).Encode(events); err != nil {
			panic(err)
		}
		return
	}
	kind := types.EventTypeWorkflowExecutionSignaled
	timestamp := int64(1700000000123000000)
	events := []*types.HistoryEvent{{ID: 7, Timestamp: &timestamp, EventType: &kind, Version: -24, TaskID: 19, WorkflowExecutionSignaledEventAttributes: &types.WorkflowExecutionSignaledEventAttributes{SignalName: "wake", Input: []byte{0, 255, 42}, Identity: "oracle"}}}
	for name, batch := range map[string][]*types.HistoryEvent{"history_blob.bin": events, "history_blob_empty.bin": {}} {
		blob, err := p.NewPayloadSerializer().SerializeBatchEvents(batch, constants.EncodingTypeThriftRW)
		if err != nil {
			panic(err)
		}
		if err := os.WriteFile(filepath.Join(os.Args[1], name), blob.Data, 0644); err != nil {
			panic(err)
		}
	}
}
