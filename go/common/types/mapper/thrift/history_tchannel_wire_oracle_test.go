package thrift

// This opt-in fixture generator serializes the pinned Go mapper output with
// the same ThriftRW binary encoder used by the generated TChannel structs.
// Run with CADENCE_THRIFT_WIRE_FIXTURE_OUT=/path/to/history.json go test
// ./common/types/mapper/thrift -run TestGenerateHistoryTChannelWireOracle.
// The checked-in Rust fixtures must be copied from this output; hand-written
// byte arrays are useful schema checks but are not Go wire oracle evidence.

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"testing"

	"github.com/uber/cadence/.gen/go/history"
	"github.com/uber/cadence/common/types"
	"github.com/uber/cadence/common/types/testdata"
	thriftbinary "go.uber.org/thriftrw/protocol/binary"
	"go.uber.org/thriftrw/protocol/stream"
)

type historyWireFixture struct {
	Name   string `json:"name"`
	Type   string `json:"thrift_type"`
	Base64 string `json:"base64"`
}

type thriftWireEncoder interface {
	Encode(stream.Writer) error
}

func encodeHistoryWireFixture(t *testing.T, name string, value thriftWireEncoder) historyWireFixture {
	t.Helper()
	var buffer bytes.Buffer
	if err := value.Encode(thriftbinary.NewStreamWriter(&buffer)); err != nil {
		t.Fatalf("encode fixture %s: %v", name, err)
	}
	canonical, err := canonicalizeThriftBinary(buffer.Bytes())
	if err != nil {
		t.Fatalf("canonicalize fixture %s: %v", name, err)
	}
	return historyWireFixture{Name: name, Type: "thrift-binary-struct", Base64: base64.StdEncoding.EncodeToString(canonical)}
}

func TestGenerateHistoryTChannelWireOracle(t *testing.T) {
	output := os.Getenv("CADENCE_THRIFT_WIRE_FIXTURE_OUT")
	if output == "" {
		t.Skip("set CADENCE_THRIFT_WIRE_FIXTURE_OUT to generate pinned Go ThriftRW wire fixtures")
	}
	// testdata initializes timestamps from time.Now. Set the shared pointed-to
	// values before mapping so every generated field is stable without changing
	// the imported testdata or production mapper behavior.
	testdata.Timestamp = 1_700_000_000
	testdata.Timestamp1 = testdata.Timestamp + 1
	testdata.Timestamp2 = testdata.Timestamp + 2
	testdata.Timestamp3 = testdata.Timestamp + 3
	testdata.Timestamp4 = testdata.Timestamp + 4
	testdata.Timestamp5 = testdata.Timestamp + 5
	fixtures := []historyWireFixture{
		encodeHistoryWireFixture(t, "get_mutable_state_request", history.HistoryService_GetMutableState_Helper.Args(FromHistoryGetMutableStateRequest(&testdata.HistoryGetMutableStateRequest))),
		encodeHistoryWireFixture(t, "get_mutable_state_response", historySuccess(history.HistoryService_GetMutableState_Helper.WrapResponse(FromHistoryGetMutableStateResponse(&testdata.HistoryGetMutableStateResponse), nil))),
		encodeHistoryWireFixture(t, "refresh_workflow_tasks_request", history.HistoryService_RefreshWorkflowTasks_Helper.Args(FromHistoryRefreshWorkflowTasksRequest(&testdata.HistoryRefreshWorkflowTasksRequest))),
		encodeHistoryWireFixture(t, "refresh_workflow_tasks_response", historySuccess(history.HistoryService_RefreshWorkflowTasks_Helper.WrapResponse(nil))),
		encodeHistoryWireFixture(t, "describe_workflow_execution_request", history.HistoryService_DescribeWorkflowExecution_Helper.Args(FromHistoryDescribeWorkflowExecutionRequest(&testdata.HistoryDescribeWorkflowExecutionRequest))),
		encodeHistoryWireFixture(t, "describe_workflow_execution_response", historySuccess(history.HistoryService_DescribeWorkflowExecution_Helper.WrapResponse(FromDescribeWorkflowExecutionResponse(&testdata.DescribeWorkflowExecutionResponse), nil))),
		encodeHistoryWireFixture(t, "record_decision_task_started_request", history.HistoryService_RecordDecisionTaskStarted_Helper.Args(FromRecordDecisionTaskStartedRequest(&testdata.HistoryRecordDecisionTaskStartedRequest))),
		encodeHistoryWireFixture(t, "record_decision_task_started_response", historySuccess(history.HistoryService_RecordDecisionTaskStarted_Helper.WrapResponse(FromRecordDecisionTaskStartedResponse(&testdata.HistoryRecordDecisionTaskStartedResponse), nil))),
		encodeHistoryWireFixture(t, "record_activity_task_started_request", history.HistoryService_RecordActivityTaskStarted_Helper.Args(FromRecordActivityTaskStartedRequest(&testdata.HistoryRecordActivityTaskStartedRequest))),
		encodeHistoryWireFixture(t, "record_activity_task_started_response", historySuccess(history.HistoryService_RecordActivityTaskStarted_Helper.WrapResponse(FromRecordActivityTaskStartedResponse(&testdata.HistoryRecordActivityTaskStartedResponse), nil))),
		encodeHistoryWireFixture(t, "record_activity_empty_header", historySuccess(history.HistoryService_RecordActivityTaskStarted_Helper.WrapResponse(FromRecordActivityTaskStartedResponse(&types.RecordActivityTaskStartedResponse{
			ScheduledEvent: &types.HistoryEvent{ActivityTaskScheduledEventAttributes: &types.ActivityTaskScheduledEventAttributes{Header: &types.Header{Fields: map[string][]byte{}}}},
		}), nil))),
		encodeHistoryWireFixture(t, "record_activity_absent_header", historySuccess(history.HistoryService_RecordActivityTaskStarted_Helper.WrapResponse(FromRecordActivityTaskStartedResponse(&types.RecordActivityTaskStartedResponse{
			ScheduledEvent: &types.HistoryEvent{ActivityTaskScheduledEventAttributes: &types.ActivityTaskScheduledEventAttributes{}},
		}), nil))),
		encodeHistoryWireFixture(t, "record_activity_empty_input", historySuccess(history.HistoryService_RecordActivityTaskStarted_Helper.WrapResponse(FromRecordActivityTaskStartedResponse(&types.RecordActivityTaskStartedResponse{
			ScheduledEvent: &types.HistoryEvent{ActivityTaskScheduledEventAttributes: &types.ActivityTaskScheduledEventAttributes{Input: []byte{}}},
		}), nil))),
		encodeHistoryWireFixture(t, "record_activity_absent_input", historySuccess(history.HistoryService_RecordActivityTaskStarted_Helper.WrapResponse(FromRecordActivityTaskStartedResponse(&types.RecordActivityTaskStartedResponse{
			ScheduledEvent: &types.HistoryEvent{ActivityTaskScheduledEventAttributes: &types.ActivityTaskScheduledEventAttributes{}},
		}), nil))),
		encodeHistoryWireFixture(t, "record_decision_empty_queries", historySuccess(history.HistoryService_RecordDecisionTaskStarted_Helper.WrapResponse(FromRecordDecisionTaskStartedResponse(&types.RecordDecisionTaskStartedResponse{Queries: map[string]*types.WorkflowQuery{}}), nil))),
		encodeHistoryWireFixture(t, "record_decision_absent_queries", historySuccess(history.HistoryService_RecordDecisionTaskStarted_Helper.WrapResponse(FromRecordDecisionTaskStartedResponse(&types.RecordDecisionTaskStartedResponse{}), nil))),
		encodeHistoryWireFixture(t, "get_mutable_empty_version_histories", historySuccess(history.HistoryService_GetMutableState_Helper.WrapResponse(FromHistoryGetMutableStateResponse(&types.GetMutableStateResponse{VersionHistories: &types.VersionHistories{Histories: []*types.VersionHistory{}}}), nil))),
		encodeHistoryWireFixture(t, "get_mutable_absent_version_histories", historySuccess(history.HistoryService_GetMutableState_Helper.WrapResponse(FromHistoryGetMutableStateResponse(&types.GetMutableStateResponse{}), nil))),
		encodeHistoryWireFixture(t, "exception_entity_not_exists", &history.HistoryService_GetMutableState_Result{EntityNotExistError: FromEntityNotExistsError(&types.EntityNotExistsError{Message: "missing", CurrentCluster: "active", ActiveCluster: "active", ActiveClusters: []string{"active"}})}),
		encodeHistoryWireFixture(t, "exception_workflow_already_completed", &history.HistoryService_RecordActivityTaskStarted_Result{WorkflowExecutionAlreadyCompletedError: FromWorkflowExecutionAlreadyCompletedError(&types.WorkflowExecutionAlreadyCompletedError{Message: "closed"})}),
		encodeHistoryWireFixture(t, "exception_event_already_started", &history.HistoryService_RecordDecisionTaskStarted_Result{EventAlreadyStartedError: FromEventAlreadyStartedError(&types.EventAlreadyStartedError{Message: "duplicate"})}),
		encodeHistoryWireFixture(t, "exception_domain_not_active", &history.HistoryService_RefreshWorkflowTasks_Result{DomainNotActiveError: FromDomainNotActiveError(&types.DomainNotActiveError{Message: "inactive", DomainName: "domain", CurrentCluster: "standby", ActiveCluster: "active", ActiveClusters: []string{"active"}})}),
		encodeHistoryWireFixture(t, "exception_shard_ownership_lost", &history.HistoryService_DescribeWorkflowExecution_Result{ShardOwnershipLostError: &history.ShardOwnershipLostError{Owner: stringPtr("history-b:7935"), Message: stringPtr("wrong owner")}}),
		encodeHistoryWireFixture(t, "exception_service_busy", &history.HistoryService_RecordActivityTaskStarted_Result{ServiceBusyError: FromServiceBusyError(&types.ServiceBusyError{Message: "busy", Reason: "capacity"})}),
		encodeHistoryWireFixture(t, "exception_internal_service", &history.HistoryService_GetMutableState_Result{InternalServiceError: FromInternalServiceError(&types.InternalServiceError{Message: "internal"})}),
	}
	encoded, err := json.MarshalIndent(fixtures, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(output, append(encoded, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
}

// canonicalizeThriftBinary preserves the bytes emitted by the generated Go
// Thrift encoder except that map entries are ordered by their encoded key.
// Go map iteration order is intentionally unspecified, while Thrift maps are
// unordered and consumers must not rely on that order.
func canonicalizeThriftBinary(input []byte) ([]byte, error) {
	output, offset, err := canonicalizeThriftValue(input, 0, 12)
	if err != nil {
		return nil, err
	}
	if offset != len(input) {
		return nil, fmt.Errorf("trailing Thrift bytes: consumed %d of %d", offset, len(input))
	}
	return output, nil
}

func canonicalizeThriftValue(input []byte, offset int, kind byte) ([]byte, int, error) {
	start := offset
	read := func(size int) ([]byte, error) {
		if size < 0 || offset+size > len(input) {
			return nil, fmt.Errorf("truncated Thrift value at byte %d", offset)
		}
		part := input[offset : offset+size]
		offset += size
		return part, nil
	}
	readLength := func() (int, error) {
		encoded, err := read(4)
		if err != nil {
			return 0, err
		}
		n := int(int32(binary.BigEndian.Uint32(encoded)))
		if n < 0 {
			return 0, fmt.Errorf("negative Thrift length %d", n)
		}
		return n, nil
	}

	switch kind {
	case 2, 3:
		_, err := read(1)
		return append([]byte(nil), input[start:offset]...), offset, err
	case 4, 10:
		_, err := read(8)
		return append([]byte(nil), input[start:offset]...), offset, err
	case 6:
		_, err := read(2)
		return append([]byte(nil), input[start:offset]...), offset, err
	case 8:
		_, err := read(4)
		return append([]byte(nil), input[start:offset]...), offset, err
	case 11:
		n, err := readLength()
		if err != nil {
			return nil, offset, err
		}
		_, err = read(n)
		return append([]byte(nil), input[start:offset]...), offset, err
	case 12:
		var output bytes.Buffer
		for {
			tag, err := read(1)
			if err != nil {
				return nil, offset, err
			}
			output.Write(tag)
			if tag[0] == 0 { // STOP
				break
			}
			fieldID, err := read(2)
			if err != nil {
				return nil, offset, err
			}
			output.Write(fieldID)
			value, next, err := canonicalizeThriftValue(input, offset, tag[0])
			if err != nil {
				return nil, next, err
			}
			offset = next
			output.Write(value)
		}
		return output.Bytes(), offset, nil
	case 13:
		header, err := read(2)
		if err != nil {
			return nil, offset, err
		}
		count, err := readLength()
		if err != nil {
			return nil, offset, err
		}
		if count > len(input) {
			return nil, offset, fmt.Errorf("invalid Thrift map size %d", count)
		}
		type entry struct{ key, value []byte }
		entries := make([]entry, 0, count)
		for i := 0; i < count; i++ {
			key, next, err := canonicalizeThriftValue(input, offset, header[0])
			if err != nil {
				return nil, next, err
			}
			offset = next
			value, next, err := canonicalizeThriftValue(input, offset, header[1])
			if err != nil {
				return nil, next, err
			}
			offset = next
			entries = append(entries, entry{key: key, value: value})
		}
		sort.Slice(entries, func(i, j int) bool { return bytes.Compare(entries[i].key, entries[j].key) < 0 })
		var output bytes.Buffer
		output.Write(header)
		var encodedCount [4]byte
		binary.BigEndian.PutUint32(encodedCount[:], uint32(count))
		output.Write(encodedCount[:])
		for _, item := range entries {
			output.Write(item.key)
			output.Write(item.value)
		}
		return output.Bytes(), offset, nil
	case 14, 15:
		header, err := read(1)
		if err != nil {
			return nil, offset, err
		}
		count, err := readLength()
		if err != nil {
			return nil, offset, err
		}
		var output bytes.Buffer
		output.Write(header)
		var encodedCount [4]byte
		binary.BigEndian.PutUint32(encodedCount[:], uint32(count))
		output.Write(encodedCount[:])
		for i := 0; i < count; i++ {
			value, next, err := canonicalizeThriftValue(input, offset, header[0])
			if err != nil {
				return nil, next, err
			}
			offset = next
			output.Write(value)
		}
		return output.Bytes(), offset, nil
	default:
		return nil, offset, fmt.Errorf("unsupported Thrift type %d at byte %d", kind, offset)
	}
}

func stringPtr(value string) *string { return &value }

func historySuccess(value thriftWireEncoder, err error) thriftWireEncoder {
	if err != nil {
		panic(err)
	}
	return value
}
