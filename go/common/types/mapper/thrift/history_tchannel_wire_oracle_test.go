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
	"encoding/json"
	"os"
	"testing"

	"github.com/uber/cadence/.gen/go/history"
	"github.com/uber/cadence/common/types"
	"github.com/uber/cadence/common/types/testdata"
	"go.uber.org/thriftrw/protocol/binary"
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
	if err := value.Encode(binary.NewStreamWriter(&buffer)); err != nil {
		t.Fatalf("encode fixture %s: %v", name, err)
	}
	return historyWireFixture{Name: name, Type: "thrift-binary-struct", Base64: base64.StdEncoding.EncodeToString(buffer.Bytes())}
}

func TestGenerateHistoryTChannelWireOracle(t *testing.T) {
	output := os.Getenv("CADENCE_THRIFT_WIRE_FIXTURE_OUT")
	if output == "" {
		t.Skip("set CADENCE_THRIFT_WIRE_FIXTURE_OUT to generate pinned Go ThriftRW wire fixtures")
	}
	fixtures := []historyWireFixture{
		encodeHistoryWireFixture(t, "get_mutable_state_request", FromHistoryGetMutableStateRequest(&testdata.HistoryGetMutableStateRequest)),
		encodeHistoryWireFixture(t, "get_mutable_state_response", FromHistoryGetMutableStateResponse(&testdata.HistoryGetMutableStateResponse)),
		encodeHistoryWireFixture(t, "refresh_workflow_tasks_request", FromHistoryRefreshWorkflowTasksRequest(&testdata.HistoryRefreshWorkflowTasksRequest)),
		encodeHistoryWireFixture(t, "describe_workflow_execution_request", FromHistoryDescribeWorkflowExecutionRequest(&testdata.HistoryDescribeWorkflowExecutionRequest)),
		encodeHistoryWireFixture(t, "describe_workflow_execution_response", FromDescribeWorkflowExecutionResponse(&testdata.DescribeWorkflowExecutionResponse)),
		encodeHistoryWireFixture(t, "record_decision_task_started_request", FromRecordDecisionTaskStartedRequest(&testdata.HistoryRecordDecisionTaskStartedRequest)),
		encodeHistoryWireFixture(t, "record_decision_task_started_response", FromRecordDecisionTaskStartedResponse(&testdata.HistoryRecordDecisionTaskStartedResponse)),
		encodeHistoryWireFixture(t, "record_activity_task_started_request", FromRecordActivityTaskStartedRequest(&testdata.HistoryRecordActivityTaskStartedRequest)),
		encodeHistoryWireFixture(t, "record_activity_task_started_response", FromRecordActivityTaskStartedResponse(&testdata.HistoryRecordActivityTaskStartedResponse)),
		encodeHistoryWireFixture(t, "record_activity_empty_header", FromRecordActivityTaskStartedResponse(&types.RecordActivityTaskStartedResponse{
			ScheduledEvent: &types.HistoryEvent{ActivityTaskScheduledEventAttributes: &types.ActivityTaskScheduledEventAttributes{Header: &types.Header{Fields: map[string][]byte{}}}},
		})),
		encodeHistoryWireFixture(t, "record_activity_absent_header", FromRecordActivityTaskStartedResponse(&types.RecordActivityTaskStartedResponse{
			ScheduledEvent: &types.HistoryEvent{ActivityTaskScheduledEventAttributes: &types.ActivityTaskScheduledEventAttributes{}},
		})),
		encodeHistoryWireFixture(t, "record_activity_empty_input", FromRecordActivityTaskStartedResponse(&types.RecordActivityTaskStartedResponse{
			ScheduledEvent: &types.HistoryEvent{ActivityTaskScheduledEventAttributes: &types.ActivityTaskScheduledEventAttributes{Input: []byte{}}},
		})),
		encodeHistoryWireFixture(t, "record_activity_absent_input", FromRecordActivityTaskStartedResponse(&types.RecordActivityTaskStartedResponse{
			ScheduledEvent: &types.HistoryEvent{ActivityTaskScheduledEventAttributes: &types.ActivityTaskScheduledEventAttributes{}},
		})),
		encodeHistoryWireFixture(t, "record_decision_empty_queries", FromRecordDecisionTaskStartedResponse(&types.RecordDecisionTaskStartedResponse{Queries: map[string]*types.WorkflowQuery{}})),
		encodeHistoryWireFixture(t, "record_decision_absent_queries", FromRecordDecisionTaskStartedResponse(&types.RecordDecisionTaskStartedResponse{})),
		encodeHistoryWireFixture(t, "get_mutable_empty_version_histories", FromHistoryGetMutableStateResponse(&types.GetMutableStateResponse{VersionHistories: &types.VersionHistories{Histories: []*types.VersionHistory{}}})),
		encodeHistoryWireFixture(t, "get_mutable_absent_version_histories", FromHistoryGetMutableStateResponse(&types.GetMutableStateResponse{})),
		encodeHistoryWireFixture(t, "exception_entity_not_exists", FromEntityNotExistsError(&types.EntityNotExistsError{Message: "missing", CurrentCluster: "active", ActiveCluster: "active", ActiveClusters: []string{"active"}})),
		encodeHistoryWireFixture(t, "exception_workflow_already_completed", FromWorkflowExecutionAlreadyCompletedError(&types.WorkflowExecutionAlreadyCompletedError{Message: "closed"})),
		encodeHistoryWireFixture(t, "exception_event_already_started", FromEventAlreadyStartedError(&types.EventAlreadyStartedError{Message: "duplicate"})),
		encodeHistoryWireFixture(t, "exception_domain_not_active", FromDomainNotActiveError(&types.DomainNotActiveError{Message: "inactive", DomainName: "domain", CurrentCluster: "standby", ActiveCluster: "active", ActiveClusters: []string{"active"}})),
		encodeHistoryWireFixture(t, "exception_shard_ownership_lost", &history.ShardOwnershipLostError{Owner: stringPtr("history-b:7935"), Message: stringPtr("wrong owner")}),
		encodeHistoryWireFixture(t, "exception_service_busy", FromServiceBusyError(&types.ServiceBusyError{Message: "busy", Reason: "capacity"})),
		encodeHistoryWireFixture(t, "exception_internal_service", FromInternalServiceError(&types.InternalServiceError{Message: "internal"})),
	}
	encoded, err := json.MarshalIndent(fixtures, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(output, append(encoded, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
}

func stringPtr(value string) *string { return &value }
