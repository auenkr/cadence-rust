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
	// Match the reviewed fixture's fixed timestamps; testdata otherwise uses
	// time.Now at package initialization, making a fresh oracle incomparable.
	testdata.Timestamp1 = 1792476472
	testdata.Timestamp2 = testdata.Timestamp1 + 1
	testdata.Timestamp3 = testdata.Timestamp1 + 2
	testdata.Timestamp4 = testdata.Timestamp1 + 3
	testdata.Timestamp5 = testdata.Timestamp1 + 4
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

func stringPtr(value string) *string { return &value }

func historySuccess(value thriftWireEncoder, err error) thriftWireEncoder {
	if err != nil {
		panic(err)
	}
	return value
}
