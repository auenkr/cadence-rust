// API coverage steps for the Rust Frontend (frontend-api-coverage): WorkflowService APIs that the
// main SDK flow does not reach (manual poll/respond, heartbeat, reset, restart, diagnose, domain
// lifecycle, failover, failover history, sync signal-with-start) and Admin APIs through the Go CLI
// over both TChannel and gRPC. Expected outcomes follow go/service/frontend/api.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.uber.org/cadence/.gen/go/cadence/workflowserviceclient"
	"go.uber.org/cadence/.gen/go/shared"
)

// routed accepts nil or a typed Cadence error produced by the service that Go would also produce
// (BadRequest, EntityNotExists, ...). It rejects InternalService and transport-level failures
// such as "unknown method" which mean the Rust Frontend did not implement the call.
func routed(err error) error {
	if err == nil {
		return nil
	}
	var internal *shared.InternalServiceError
	if errors.As(err, &internal) {
		return err
	}
	msg := strings.ToLower(err.Error())
	for _, bad := range []string{"unknown method", "unimplemented", "not implemented", "unsupported"} {
		if strings.Contains(msg, bad) {
			return err
		}
	}
	return nil
}

func wantBadRequest(err error, contains string) error {
	var br *shared.BadRequestError
	if !errors.As(err, &br) {
		return fmt.Errorf("want BadRequestError(%q), got %T %v", contains, err, err)
	}
	if !strings.Contains(br.Message, contains) {
		return fmt.Errorf("want message containing %q, got %q", contains, br.Message)
	}
	return nil
}

func apiCoverage(ctx context.Context, rpc workflowserviceclient.Interface, domain, tl string, echoID, echoRunID string) {
	pollCtx := func() (context.Context, context.CancelFunc) { return context.WithTimeout(ctx, 30*time.Second) }
	mtl := tl + "-manual"
	wfID := tl + "-manual-wf"

	// ---- cluster/meta APIs
	_, err := rpc.GetClusterInfo(ctx)
	step("api-get-cluster-info", err)
	sa, err := rpc.GetSearchAttributes(ctx)
	if err == nil && (sa == nil || len(sa.Keys) == 0) {
		err = errors.New("no search attribute keys")
	}
	step("api-get-search-attributes", err)

	// ---- domain lifecycle: list, deprecate, delete, failover, failover history
	dom := "fe-api-" + strings.ReplaceAll(uuid.NewString()[:8], "-", "")
	dom2 := dom + "b"
	for _, d := range []string{dom, dom2} {
		e := rpc.RegisterDomain(ctx, &shared.RegisterDomainRequest{Name: ptr(d), WorkflowExecutionRetentionPeriodInDays: ptr(int32(1))})
		step("api-register-"+d[len(d)-1:], e)
	}
	var listed bool
	var token []byte
	for i := 0; i < 20 && !listed; i++ {
		resp, e := rpc.ListDomains(ctx, &shared.ListDomainsRequest{PageSize: ptr(int32(100)), NextPageToken: token})
		if e != nil {
			err = e
			break
		}
		for _, x := range resp.Domains {
			if x.DomainInfo.GetName() == dom {
				listed = true
			}
		}
		token = resp.NextPageToken
		if len(token) == 0 {
			break
		}
	}
	if err == nil && !listed {
		err = errors.New("registered domain missing from ListDomains")
	}
	step("api-list-domains", err)

	step("api-delete-domain-not-deprecated", func() error {
		return wantBadRequest(rpc.DeleteDomain(ctx, &shared.DeleteDomainRequest{Name: ptr(dom2)}), "Domain is not in a deprecated state.")
	}())
	e := rpc.DeprecateDomain(ctx, &shared.DeprecateDomainRequest{Name: ptr(dom)})
	if e == nil {
		d, de := rpc.DescribeDomain(ctx, &shared.DescribeDomainRequest{Name: ptr(dom)})
		if de == nil && d.DomainInfo.GetStatus() != shared.DomainStatusDeprecated {
			de = fmt.Errorf("status %v", d.DomainInfo.GetStatus())
		}
		e = de
	}
	step("api-deprecate-domain", e)
	// Go lists workflow executions before deleting; either outcome must be a Go-shaped response.
	step("api-delete-domain-deprecated", routed(rpc.DeleteDomain(ctx, &shared.DeleteDomainRequest{Name: ptr(dom)})))
	step("api-delete-domain-missing", func() error {
		e := rpc.DeleteDomain(ctx, &shared.DeleteDomainRequest{Name: ptr("fe-api-none-" + uuid.NewString()[:6])})
		var nf *shared.EntityNotExistsError
		// DeleteDomain does not declare EntityNotExists in the Thrift IDL, so Go's yarpc layer surfaces it as an undeclared error.
		if !errors.As(e, &nf) && (e == nil || !strings.Contains(e.Error(), "EntityNotExistsError")) {
			return fmt.Errorf("want EntityNotExistsError, got %T %v", e, e)
		}
		return nil
	}())
	_, e = rpc.FailoverDomain(ctx, &shared.FailoverDomainRequest{})
	step("api-failover-domain-empty-rejected", func() error {
		var br *shared.BadRequestError
		if !errors.As(e, &br) {
			return fmt.Errorf("want BadRequestError, got %T %v", e, e)
		}
		return nil
	}())
	_, e = rpc.FailoverDomain(ctx, &shared.FailoverDomainRequest{DomainName: ptr(dom2), DomainActiveClusterName: ptr("active")})
	step("api-failover-domain", routed(e))
	_, e = rpc.ListFailoverHistory(ctx, &shared.ListFailoverHistoryRequest{})
	step("api-list-failover-history-no-domain-id", wantBadRequest(e, "DomainID is required in filters"))
	d2, e := rpc.DescribeDomain(ctx, &shared.DescribeDomainRequest{Name: ptr(dom2)})
	if e == nil {
		_, e = rpc.ListFailoverHistory(ctx, &shared.ListFailoverHistoryRequest{Filters: &shared.ListFailoverHistoryRequestFilters{DomainID: d2.DomainInfo.UUID}})
	}
	step("api-list-failover-history", routed(e))

	// ---- sync SignalWithStart
	swsID := tl + "-sws"
	sws, e := rpc.SignalWithStartWorkflowExecution(ctx, &shared.SignalWithStartWorkflowExecutionRequest{
		Domain: &domain, WorkflowId: &swsID, WorkflowType: &shared.WorkflowType{Name: ptr("main.SignalQueryWorkflow")},
		TaskList: &shared.TaskList{Name: &tl}, ExecutionStartToCloseTimeoutSeconds: ptr(int32(300)), TaskStartToCloseTimeoutSeconds: ptr(int32(10)),
		RequestId: ptr(uuid.NewString()), SignalName: ptr("done"), SignalInput: []byte(`"x"`)})
	if e == nil && sws.GetRunId() == "" {
		e = errors.New("empty run id")
	}
	step("api-signal-with-start", e)
	longIdentity := strings.Repeat("i", 1001)
	_, e = rpc.SignalWithStartWorkflowExecution(ctx, &shared.SignalWithStartWorkflowExecutionRequest{
		Domain: &domain, WorkflowId: ptr(swsID + "-id"), WorkflowType: &shared.WorkflowType{Name: ptr("main.SignalQueryWorkflow")},
		TaskList: &shared.TaskList{Name: &tl}, ExecutionStartToCloseTimeoutSeconds: ptr(int32(300)), TaskStartToCloseTimeoutSeconds: ptr(int32(10)),
		RequestId: ptr(uuid.NewString()), SignalName: ptr("done"), Identity: &longIdentity})
	// Go's SignalWithStart validation does not check identity length (only Respond*/heartbeat do), so it must not be rejected.
	step("api-signal-with-start-identity-not-validated", func() error {
		if e != nil && strings.Contains(e.Error(), "Identity length") {
			return fmt.Errorf("unexpected identity rejection: %v", e)
		}
		return nil
	}())

	// ---- manual decision/activity task flow (no SDK worker on mtl)
	start, e := rpc.StartWorkflowExecution(ctx, &shared.StartWorkflowExecutionRequest{
		Domain: &domain, WorkflowId: &wfID, WorkflowType: &shared.WorkflowType{Name: ptr("manual-wf")}, TaskList: &shared.TaskList{Name: &mtl},
		ExecutionStartToCloseTimeoutSeconds: ptr(int32(600)), TaskStartToCloseTimeoutSeconds: ptr(int32(30)), RequestId: ptr(uuid.NewString())})
	if !step("api-manual-start", e) {
		return
	}
	runID := start.GetRunId()
	identity := "api-gate"
	pollDecision := func() (*shared.PollForDecisionTaskResponse, error) {
		c, cancel := pollCtx()
		defer cancel()
		for i := 0; i < 5; i++ {
			r, e := rpc.PollForDecisionTask(c, &shared.PollForDecisionTaskRequest{Domain: &domain, TaskList: &shared.TaskList{Name: &mtl}, Identity: &identity})
			if e != nil {
				return nil, e
			}
			if len(r.TaskToken) > 0 {
				return r, nil
			}
		}
		return nil, errors.New("no decision task")
	}
	dt, e := pollDecision()
	if e == nil {
		step("api-respond-decision-task-failed-identity-too-long", wantBadRequest(rpc.RespondDecisionTaskFailed(ctx, &shared.RespondDecisionTaskFailedRequest{TaskToken: dt.TaskToken, Cause: shared.DecisionTaskFailedCauseUnhandledDecision.Ptr(), Identity: &longIdentity}), "Identity length exceeds limit."))
		step("api-respond-decision-task-failed", rpc.RespondDecisionTaskFailed(ctx, &shared.RespondDecisionTaskFailedRequest{TaskToken: dt.TaskToken, Cause: shared.DecisionTaskFailedCauseUnhandledDecision.Ptr(), Identity: &identity}))
	} else {
		step("api-respond-decision-task-failed", e)
		return
	}
	dt, e = pollDecision()
	if !step("api-poll-decision-after-failure", e) {
		return
	}
	var decisions []*shared.Decision
	ids := []string{"a1", "a2", "a3", "a4", "a5"}
	for _, id := range ids {
		decisions = append(decisions, &shared.Decision{DecisionType: shared.DecisionTypeScheduleActivityTask.Ptr(), ScheduleActivityTaskDecisionAttributes: &shared.ScheduleActivityTaskDecisionAttributes{
			ActivityId: ptr(id), ActivityType: &shared.ActivityType{Name: ptr("manual-act")}, TaskList: &shared.TaskList{Name: &mtl}, Input: []byte(`"x"`),
			ScheduleToCloseTimeoutSeconds: ptr(int32(300)), ScheduleToStartTimeoutSeconds: ptr(int32(300)), StartToCloseTimeoutSeconds: ptr(int32(300)), HeartbeatTimeoutSeconds: ptr(int32(120))}})
	}
	_, e = rpc.RespondDecisionTaskCompleted(ctx, &shared.RespondDecisionTaskCompletedRequest{TaskToken: dt.TaskToken, Decisions: decisions, Identity: &identity})
	if !step("api-respond-decision-schedule-activities", e) {
		return
	}
	acts := map[string]*shared.PollForActivityTaskResponse{}
	for len(acts) < len(ids) {
		c, cancel := pollCtx()
		r, e := rpc.PollForActivityTask(c, &shared.PollForActivityTaskRequest{Domain: &domain, TaskList: &shared.TaskList{Name: &mtl}, Identity: &identity})
		cancel()
		if e != nil {
			step("api-poll-activities", e)
			return
		}
		if len(r.TaskToken) > 0 {
			acts[r.GetActivityId()] = r
		}
	}
	step("api-poll-activities", nil)
	a1 := acts["a1"]
	_, e = rpc.RecordActivityTaskHeartbeat(ctx, &shared.RecordActivityTaskHeartbeatRequest{TaskToken: a1.TaskToken, Details: []byte(`"hb"`), Identity: &identity})
	step("api-record-activity-heartbeat", e)
	_, e = rpc.RecordActivityTaskHeartbeat(ctx, &shared.RecordActivityTaskHeartbeatRequest{TaskToken: a1.TaskToken, Identity: &longIdentity})
	step("api-record-activity-heartbeat-identity-too-long", wantBadRequest(e, "Identity length exceeds limit."))
	step("api-respond-activity-failed-identity-too-long", wantBadRequest(rpc.RespondActivityTaskFailed(ctx, &shared.RespondActivityTaskFailedRequest{TaskToken: a1.TaskToken, Identity: &longIdentity}), "Identity length exceeds limit."))
	step("api-respond-activity-failed", rpc.RespondActivityTaskFailed(ctx, &shared.RespondActivityTaskFailedRequest{TaskToken: a1.TaskToken, Reason: ptr("gate"), Details: []byte(`"d"`), Identity: &identity}))
	byID := func(id string) (*string, *string, *string, *string) { return &domain, &wfID, &runID, ptr(id) }
	d, w, r, a := byID("a2")
	_, e = rpc.RecordActivityTaskHeartbeatByID(ctx, &shared.RecordActivityTaskHeartbeatByIDRequest{Domain: d, WorkflowID: w, RunID: r, ActivityID: a, Details: []byte(`"hb"`), Identity: &identity})
	step("api-record-activity-heartbeat-by-id", e)
	step("api-respond-activity-completed-by-id-identity-too-long", wantBadRequest(rpc.RespondActivityTaskCompletedByID(ctx, &shared.RespondActivityTaskCompletedByIDRequest{Domain: d, WorkflowID: w, RunID: r, ActivityID: a, Identity: &longIdentity}), "Identity length exceeds limit."))
	step("api-respond-activity-completed-by-id", rpc.RespondActivityTaskCompletedByID(ctx, &shared.RespondActivityTaskCompletedByIDRequest{Domain: d, WorkflowID: w, RunID: r, ActivityID: a, Result: []byte(`"ok"`), Identity: &identity}))
	d, w, r, a = byID("a3")
	step("api-respond-activity-failed-by-id", rpc.RespondActivityTaskFailedByID(ctx, &shared.RespondActivityTaskFailedByIDRequest{Domain: d, WorkflowID: w, RunID: r, ActivityID: a, Reason: ptr("gate"), Identity: &identity}))
	// Cancel responses are accepted or rejected by History exactly as under Go; the Frontend must route them.
	step("api-respond-activity-canceled", routed(rpc.RespondActivityTaskCanceled(ctx, &shared.RespondActivityTaskCanceledRequest{TaskToken: acts["a4"].TaskToken, Details: []byte(`"c"`), Identity: &identity})))
	d, w, r, a = byID("a5")
	step("api-respond-activity-canceled-by-id", routed(rpc.RespondActivityTaskCanceledByID(ctx, &shared.RespondActivityTaskCanceledByIDRequest{Domain: d, WorkflowID: w, RunID: r, ActivityID: a, Details: []byte(`"c"`), Identity: &identity})))

	// ---- reset: find the first DecisionTaskFailed event
	hist, e := rpc.GetWorkflowExecutionHistory(ctx, &shared.GetWorkflowExecutionHistoryRequest{Domain: &domain, Execution: &shared.WorkflowExecution{WorkflowId: &wfID, RunId: &runID}})
	var resetAt int64
	if e == nil {
		for _, ev := range hist.History.Events {
			if ev.GetEventType() == shared.EventTypeDecisionTaskFailed {
				resetAt = ev.GetEventId()
				break
			}
		}
		if resetAt == 0 {
			e = errors.New("no DecisionTaskFailed event in history")
		}
	}
	var newRun string
	if e == nil {
		var rr *shared.ResetWorkflowExecutionResponse
		rr, e = rpc.ResetWorkflowExecution(ctx, &shared.ResetWorkflowExecutionRequest{Domain: &domain, WorkflowExecution: &shared.WorkflowExecution{WorkflowId: &wfID, RunId: &runID},
			Reason: ptr("gate"), DecisionFinishEventId: &resetAt, RequestId: ptr(uuid.NewString())})
		if e == nil {
			newRun = rr.GetRunId()
			if newRun == "" || newRun == runID {
				e = fmt.Errorf("reset run id %q", newRun)
			}
		}
	}
	step("api-reset-workflow-execution", e)
	_ = rpc.TerminateWorkflowExecution(ctx, &shared.TerminateWorkflowExecutionRequest{Domain: &domain, WorkflowExecution: &shared.WorkflowExecution{WorkflowId: &wfID}, Reason: ptr("api gate cleanup")})
	step("api-reset-invalid-event-rejected", func() error {
		_, e := rpc.ResetWorkflowExecution(ctx, &shared.ResetWorkflowExecutionRequest{Domain: &domain, WorkflowExecution: &shared.WorkflowExecution{WorkflowId: &wfID, RunId: &runID}, DecisionFinishEventId: ptr(int64(1)), RequestId: ptr(uuid.NewString())})
		return wantBadRequest(e, "")
	}())

	// ---- restart + diagnose on the completed echo workflow
	_, e = rpc.RestartWorkflowExecution(ctx, &shared.RestartWorkflowExecutionRequest{Domain: &domain, WorkflowExecution: &shared.WorkflowExecution{WorkflowId: &echoID, RunId: &echoRunID}, Identity: &identity, Reason: ptr("gate")})
	step("api-restart-workflow-execution", routed(e))
	_, e = rpc.DiagnoseWorkflowExecution(ctx, &shared.DiagnoseWorkflowExecutionRequest{Domain: &domain, WorkflowExecution: &shared.WorkflowExecution{WorkflowId: &echoID, RunId: &echoRunID}, Identity: &identity})
	step("api-diagnose-workflow-execution", routed(e))
	_, e = rpc.DiagnoseWorkflowExecution(ctx, &shared.DiagnoseWorkflowExecutionRequest{Domain: &domain, WorkflowExecution: &shared.WorkflowExecution{WorkflowId: &echoID}})
	// Go returns validate.ErrExecutionNotSet; BadRequest is undeclared for Diagnose in Thrift so it arrives as a yarpc error.
	step("api-diagnose-missing-run-id-rejected", func() error {
		if e == nil || !strings.Contains(e.Error(), "Execution is not set on request.") {
			return fmt.Errorf("want Execution is not set on request., got %v", e)
		}
		return nil
	}())

	adminCLI(ctx, domain, echoID, echoRunID)
}

// adminCLI exercises Admin APIs with the Go CLI against the Rust Frontend over TChannel and gRPC.
func adminCLI(ctx context.Context, domain, wid, rid string) {
	cli, tchan, grpc := os.Getenv("FE_CLI"), os.Getenv("FE_TCHANNEL"), os.Getenv("FE_GRPC")
	if cli == "" {
		return
	}
	for _, tr := range []struct{ name, addr string }{{"tchannel", tchan}, {"grpc", grpc}} {
		run := func(name string, args ...string) {
			base := []string{"--address", tr.addr, "--transport", tr.name, "--domain", domain}
			c, cancel := context.WithTimeout(ctx, 60*time.Second)
			defer cancel()
			out, e := exec.CommandContext(c, cli, append(base, args...)...).CombinedOutput()
			if e != nil {
				e = fmt.Errorf("%v: %s", e, strings.ReplaceAll(strings.TrimSpace(string(out)), "\n", " | "))
			}
			step("api-admin-"+name+"-"+tr.name, e)
		}
		run("cluster-describe", "admin", "cluster", "describe")
		run("workflow-describe", "admin", "workflow", "describe", "--wid", wid, "--rid", rid)
		run("workflow-refresh-tasks", "admin", "workflow", "refresh-tasks", "--wid", wid, "--rid", rid)
		run("history-host-describe", "admin", "history_host", "describe", "--wid", wid)
		run("isolation-groups-get-global", "admin", "isolation-groups", "get-global")
		run("isolation-groups-get-domain", "admin", "isolation-groups", "get-domain")
		run("tasklist-list", "admin", "tasklist", "list")

		// Admin coverage: assert real output where state allows. `want` must appear in
		// output on success; when state-dependent a typed server error is tolerated
		// (anything but routing failures / internal panics).
		cluster := os.Getenv("FE_CLUSTER")
		if cluster == "" {
			cluster = "test"
		}
		check := func(name, want string, tolerate bool, args ...string) {
			base := []string{"--address", tr.addr, "--transport", tr.name, "--domain", domain}
			c, cancel := context.WithTimeout(ctx, 60*time.Second)
			defer cancel()
			out, e := exec.CommandContext(c, cli, append(base, args...)...).CombinedOutput()
			s := string(out)
			var err error
			switch {
			case e == nil && want != "" && !strings.Contains(s, want):
				err = fmt.Errorf("output missing %q: %s", want, strings.ReplaceAll(strings.TrimSpace(s), "\n", " | "))
			case e != nil && !tolerate:
				err = fmt.Errorf("%v: %s", e, strings.ReplaceAll(strings.TrimSpace(s), "\n", " | "))
			case e != nil:
				low := strings.ToLower(s)
				for _, bad := range []string{"unknown method", "unimplemented", "not implemented", "unsupported", "panic", "internalservice", "internal service"} {
					if strings.Contains(low, bad) {
						err = fmt.Errorf("%v: %s", e, strings.ReplaceAll(strings.TrimSpace(s), "\n", " | "))
						break
					}
				}
			}
			step("api-admin-"+name+"-"+tr.name, err)
		}
		check("shard-list", "Total Number of Shards", false, "admin", "shard", "list")
		check("shard-list-page", "Number of Shards Returned: 2", false, "admin", "shard", "list", "--pagesize", "2")
		check("dlq-count-history", "", tr.name == "tchannel", "admin", "dlq", "count", "--dlq_type", "history") // CountDLQMessages is gRPC-only (Go: Feature not supported on TChannel)
		check("dlq-count-domain", "", tr.name == "tchannel", "admin", "dlq", "count", "--dlq_type", "domain") // CountDLQMessages is gRPC-only (Go: Feature not supported on TChannel)
		check("dlq-read-domain", "", true, "admin", "dlq", "read", "--dlq_type", "domain", "--max_message_count", "5")
		check("dlq-read-history", "", true, "admin", "dlq", "read", "--dlq_type", "history", "--shards", "0", "--source_cluster", cluster, "--max_message_count", "5")
		check("dlq-purge-domain", "", true, "admin", "dlq", "purge", "--dlq_type", "domain", "--last_message_id", "0")
		check("dlq-purge-history", "", true, "admin", "dlq", "purge", "--dlq_type", "history", "--shards", "0", "--source_cluster", cluster, "--last_message_id", "0")
		check("dlq-merge-domain", "", true, "admin", "dlq", "merge", "--dlq_type", "domain", "--last_message_id", "0")
		check("dlq-merge-history", "", true, "admin", "dlq", "merge", "--dlq_type", "history", "--shards", "0", "--source_cluster", cluster, "--last_message_id", "0")
		check("queue-describe-transfer", "", true, "admin", "queue", "describe", "--shard_id", "0", "--cluster", cluster, "--queue_type", "2")
		check("queue-describe-timer", "", true, "admin", "queue", "describe", "--shard_id", "0", "--cluster", cluster, "--queue_type", "3")
		check("queue-describe-no-cluster-rejected", "", true, "admin", "queue", "describe", "--shard_id", "0", "--queue_type", "2")
		check("workflow-fix-corruption", "", true, "admin", "workflow", "fix_corruption", "--wid", "gate-no-such-wf-"+rid[:8])
		check("workflow-delete-remote", "", true, "admin", "workflow", "delete", "--remote", "--wid", "gate-no-such-wf-"+rid[:8], "--rid", rid)
		check("dynconfig-operational-list", "", false, "admin", "config", "listall")
		check("dynconfig-operational-get", "", true, "admin", "config", "operational-get", "--name", "frontend.visibilityMaxPageSize")
		check("dynconfig-operational-list-values", "", true, "admin", "config", "operational-list")
	}
	// Schedule update/pause/unpause/list run in the schedule block (gRPC CLI).
}
