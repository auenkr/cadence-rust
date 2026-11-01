// Decode Rust's nested async request payloads with the original Go consumer codec.
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common/codec"
)

func require(ok bool, message string) {
	if !ok {
		panic(message)
	}
}

func main() {
	require(len(os.Args) == 2, "expected fixture directory")
	encoder := codec.NewThriftRWEncoder()
	read := func(name string) []byte {
		payload, err := os.ReadFile(filepath.Join(os.Args[1], name))
		if err != nil {
			panic(err)
		}
		return payload
	}
	startPayload := read("start.bin")
	var start shared.StartWorkflowExecutionAsyncRequest
	if err := encoder.Decode(startPayload, &start); err != nil {
		panic(fmt.Errorf("Go rejected Rust async start payload: %w", err))
	}
	require(start.Request != nil, "missing start request")
	require(start.Request.GetDomain() == "shop", "wrong start domain")
	require(start.Request.GetWorkflowId() == "order-17", "wrong start workflow ID")
	require(start.Request.GetWorkflowType().GetName() == "Order", "wrong start workflow type")
	require(start.Request.GetTaskList().GetName() == "orders", "wrong start task list")
	require(bytes.Equal(start.Request.GetInput(), []byte{1, 2}), "lost start input")
	var unversioned shared.StartWorkflowExecutionAsyncRequest
	require(encoder.Decode(startPayload[1:], &unversioned) != nil, "Go unexpectedly accepted unversioned payload")

	var signal shared.SignalWithStartWorkflowExecutionAsyncRequest
	if err := encoder.Decode(read("signal-with-start.bin"), &signal); err != nil {
		panic(fmt.Errorf("Go rejected Rust async signal-with-start payload: %w", err))
	}
	require(signal.Request != nil, "missing signal-with-start request")
	require(signal.Request.GetDomain() == "shop", "wrong signal domain")
	require(signal.Request.GetWorkflowId() == "order-18", "wrong signal workflow ID")
	require(signal.Request.GetWorkflowType().GetName() == "Order", "wrong signal workflow type")
	require(signal.Request.GetTaskList().GetName() == "orders", "wrong signal task list")
	require(signal.Request.GetSignalName() == "refresh", "lost signal name")
	if err := json.NewEncoder(os.Stdout).Encode(map[string]bool{
		"go_decodes_rust_async_start": true,
		"go_decodes_rust_async_signal_with_start": true,
		"go_rejects_unversioned_payload": true,
	}); err != nil {
		panic(err)
	}
}
