// Command thrift-fixtures generates Thrift binary-protocol fixtures for the
// Rust port's wire-compatibility tests. The fixtures are encoded with the
// same library the Go server uses (thriftrw), so byte-level agreement with
// the Rust decoder/encoder proves wire compatibility.
//
// Usage: go run ./cmd/thrift-fixtures -out <dir>
package main

import (
	"bytes"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/uber/cadence/.gen/go/cadence"
	"github.com/uber/cadence/.gen/go/shared"
	"go.uber.org/thriftrw/protocol/binary"
	"go.uber.org/thriftrw/wire"
)

func strptr(s string) *string       { return &s }
func i32ptr(v int32) *int32         { return &v }
func i64ptr(v int64) *int64         { return &v }
func boolptr(v bool) *bool          { return &v }
func f64ptr(v float64) *float64     { return &v }

func enveloped(name string, typ wire.EnvelopeType, value wire.Value) []byte {
	var buf bytes.Buffer
	env := wire.Envelope{Name: name, Type: typ, SeqID: 42, Value: value}
	if err := binary.Default.EncodeEnveloped(env, &buf); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

func mustWire(v interface {
	ToWire() (wire.Value, error)
}) wire.Value {
	w, err := v.ToWire()
	if err != nil {
		panic(err)
	}
	return w
}

func writeFile(dir, name string, data []byte) {
	if err := os.WriteFile(filepath.Join(dir, name), data, 0644); err != nil {
		panic(err)
	}
	fmt.Printf("%s: %d bytes\n", name, len(data))
}

func main() {
	out := flag.String("out", "", "output directory for fixtures")
	flag.Parse()
	if *out == "" {
		panic("-out is required")
	}
	if err := os.MkdirAll(*out, 0755); err != nil {
		panic(err)
	}

	// 1. StartWorkflowExecution CALL args, richly populated.
	reuse := shared.WorkflowIdReusePolicyRejectDuplicate
	kind := shared.TaskListKindNormal
	enc := shared.EncodingTypeJSON
	archival := shared.ArchivalStatusEnabled
	startReq := &shared.StartWorkflowExecutionRequest{
		Domain:   strptr("samples-domain"),
		WorkflowId: strptr("workflowid123"),
		WorkflowType: &shared.WorkflowType{
			Name: strptr("workflow_type"),
		},
		TaskList: &shared.TaskList{
			Name: strptr("tasklist-name"),
			Kind: &kind,
		},
		Input:                              []byte{0x00, 0xff, 0x01, 0x02},
		ExecutionStartToCloseTimeoutSeconds: i32ptr(11),
		TaskStartToCloseTimeoutSeconds:      i32ptr(10),
		Identity:                           strptr("My custom identity"),
		RequestId:                          strptr("4D1E4058-6FCF-4BA8-BF16-8FA8B02F9651"),
		WorkflowIdReusePolicy:              &reuse,
		RetryPolicy: &shared.RetryPolicy{
			InitialIntervalInSeconds:    i32ptr(1),
			BackoffCoefficient:          f64ptr(2.0),
			MaximumIntervalInSeconds:    i32ptr(100),
			MaximumAttempts:             i32ptr(3),
			NonRetriableErrorReasons:    []string{"BadInput", "Fatal"},
			ExpirationIntervalInSeconds: i32ptr(60),
			JitterCoefficient:           f64ptr(0.1),
		},
		CronSchedule: strptr("0 6 * * *"),
		Memo: &shared.Memo{
			Fields: map[string][]byte{"key": []byte("value")},
		},
		SearchAttributes: &shared.SearchAttributes{
			IndexedFields: map[string][]byte{"CustomKeywordField": []byte("keyword")},
		},
		Header: &shared.Header{
			Fields: map[string][]byte{"h": []byte("v")},
		},
		DelayStartSeconds:  i32ptr(5),
		JitterStartSeconds: i32ptr(2),
	}
	startArgs := &cadence.WorkflowService_StartWorkflowExecution_Args{
		StartRequest: startReq,
	}
	writeFile(*out, "start_args.bin", enveloped(
		"StartWorkflowExecution", wire.Call, mustWire(startArgs),
	))

	// 2. StartWorkflowExecution REPLY with a response (for decoder + encoder checks).
	startResp := &shared.StartWorkflowExecutionResponse{
		RunId: strptr("4D1E4058-6FCF-4BA8-BF16-8FA8B02F9651"),
	}
	startResult := &cadence.WorkflowService_StartWorkflowExecution_Result{
		Success: startResp,
	}
	writeFile(*out, "start_reply.bin", enveloped(
		"StartWorkflowExecution", wire.Reply, mustWire(startResult),
	))

	// 3. StartWorkflowExecution REPLY carrying a declared exception.
	badResult := &cadence.WorkflowService_StartWorkflowExecution_Result{
		BadRequestError: &shared.BadRequestError{
			Message: "Domain not set on request.",
		},
	}
	writeFile(*out, "start_bad_request.bin", enveloped(
		"StartWorkflowExecution", wire.Reply, mustWire(badResult),
	))

	// 4. PollForDecisionTask REPLY with a rich history (decoder stress test).
	pollCategory := shared.FailureCategoryPoll
	_ = enc
	_ = archival
	pollResp := &shared.PollForDecisionTaskResponse{
		TaskToken: []byte(`{"domainId":"d","scheduleId":2}`),
		WorkflowExecution: &shared.WorkflowExecution{
			WorkflowId: strptr("wf-1"),
			RunId:      strptr("run-1"),
		},
		WorkflowType: &shared.WorkflowType{
			Name: strptr("wt"),
		},
		PreviousStartedEventId: i64ptr(0),
		StartedEventId:         i64ptr(3),
		Attempt:                i64ptr(1),
		BacklogCountHint:       i64ptr(2),
		History: &shared.History{
			Events: []*shared.HistoryEvent{
				{
					EventId:   i64ptr(1),
					Timestamp: i64ptr(1791960424201),
					EventType: func() *shared.EventType {
						v := shared.EventTypeWorkflowExecutionStarted
						return &v
					}(),
					Version: i64ptr(5),
					WorkflowExecutionStartedEventAttributes: &shared.WorkflowExecutionStartedEventAttributes{
						WorkflowType: &shared.WorkflowType{
							Name: strptr("wt"),
						},
						TaskList: &shared.TaskList{
							Name: strptr("tl"),
						},
						Input:                              []byte("input"),
						ExecutionStartToCloseTimeoutSeconds: i32ptr(60),
						TaskStartToCloseTimeoutSeconds:      i32ptr(10),
						Identity:                           strptr("sdk"),
						RequestId:                          strptr("req-1"),
						Attempt:                            i32ptr(2),
						CronSchedule:                       strptr("0 6 * * *"),
						RetryPolicy: &shared.RetryPolicy{
							InitialIntervalInSeconds: i32ptr(1),
							BackoffCoefficient:       f64ptr(2.0),
							MaximumAttempts:          i32ptr(5),
						},
						Memo: &shared.Memo{
							Fields: map[string][]byte{"key": []byte("value")},
						},
						SearchAttributes: &shared.SearchAttributes{
							IndexedFields: map[string][]byte{"CustomKeywordField": []byte("keyword")},
						},
						Header: &shared.Header{
							Fields: map[string][]byte{"h": []byte("v")},
						},
					},
				},
				{
					EventId:   i64ptr(2),
					EventType: func() *shared.EventType {
						v := shared.EventTypeDecisionTaskScheduled
						return &v
					}(),
					DecisionTaskScheduledEventAttributes: &shared.DecisionTaskScheduledEventAttributes{
						TaskList: &shared.TaskList{
							Name: strptr("tl"),
						},
						StartToCloseTimeoutSeconds: i32ptr(10),
						Attempt:                    i64ptr(0),
					},
				},
				{
					EventId:   i64ptr(3),
					EventType: func() *shared.EventType {
						v := shared.EventTypeActivityTaskScheduled
						return &v
					}(),
					ActivityTaskScheduledEventAttributes: &shared.ActivityTaskScheduledEventAttributes{
						ActivityId: strptr("a1"),
						ActivityType: &shared.ActivityType{
							Name: strptr("at"),
						},
						TaskList: &shared.TaskList{
							Name: strptr("tl"),
						},
						Input:                         []byte("in"),
						ScheduleToCloseTimeoutSeconds: i32ptr(30),
						DecisionTaskCompletedEventId:  i64ptr(4),
						RetryPolicy: &shared.RetryPolicy{
							InitialIntervalInSeconds: i32ptr(1),
							BackoffCoefficient:       f64ptr(1.5),
							MaximumAttempts:          i32ptr(2),
						},
					},
				},
				{
					EventId:   i64ptr(4),
					EventType: func() *shared.EventType {
						v := shared.EventTypeActivityTaskFailed
						return &v
					}(),
					ActivityTaskFailedEventAttributes: &shared.ActivityTaskFailedEventAttributes{
						Reason:         strptr("boom"),
						Details:        []byte("details"),
						FailureOptions: &shared.FailureOptions{FailureCategory: &pollCategory},
						ScheduledEventId: i64ptr(3),
						StartedEventId:   i64ptr(5),
						Identity:         strptr("worker"),
					},
				},
			},
		},
		WorkflowExecutionTaskList: &shared.TaskList{
			Name: strptr("tl"),
		},
	}
	pollResult := &cadence.WorkflowService_PollForDecisionTask_Result{
		Success: pollResp,
	}
	writeFile(*out, "poll_resp.bin", enveloped(
		"PollForDecisionTask", wire.Reply, mustWire(pollResult),
	))

	// 5. SignalWorkflowExecution CALL args.
	sigArgs := &cadence.WorkflowService_SignalWorkflowExecution_Args{
		SignalRequest: &shared.SignalWorkflowExecutionRequest{
			Domain: strptr("d"),
			WorkflowExecution: &shared.WorkflowExecution{
				WorkflowId: strptr("w"),
				RunId:      strptr("r"),
			},
			SignalName: strptr("sig"),
			Input:      []byte("payload"),
			Identity:   strptr("cli"),
			RequestId:  strptr("4D1E4058-6FCF-4BA8-BF16-8FA8B02F9651"),
		},
	}
	writeFile(*out, "signal_args.bin", enveloped(
		"SignalWorkflowExecution", wire.Call, mustWire(sigArgs),
	))
}
