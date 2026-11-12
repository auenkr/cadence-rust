// History scenario acceptance (FE_MODE=scen): behaviours that need Rust History to act on its own
// timers / transfer tasks, driven through the Rust Frontend by the Go SDK.
//  1. retry-policy workflow hitting its execution timeout -> next run has attempt+1; cron timeout -> next cron run
//  2. strong idempotency (history.enableStrongIdempotency is true in development.yaml): start/signal request-id dedup
//  3. history archival (file://, retention 0): closed run archived by the Go Worker archival workflow, then deleted;
//     GetWorkflowExecutionHistory falls back to the archive
//  4. auto-reset on a bad binary checksum
// Env: FE_DOMAIN, FE_DOMAIN_ARCH, FE_ARCH_DIR.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.uber.org/cadence/.gen/go/cadence/workflowserviceclient"
	"go.uber.org/cadence/.gen/go/shared"
	"go.uber.org/cadence/client"
	"go.uber.org/cadence/worker"
)

func scenHistory(ctx context.Context, rpc workflowserviceclient.Interface, domain, wid, runID string) ([]*shared.HistoryEvent, error) {
	var events []*shared.HistoryEvent
	var token []byte
	for {
		req := &shared.GetWorkflowExecutionHistoryRequest{Domain: &domain, Execution: &shared.WorkflowExecution{WorkflowId: &wid}, MaximumPageSize: ptr(int32(1000)), NextPageToken: token}
		if runID != "" {
			req.Execution.RunId = &runID
		}
		resp, err := rpc.GetWorkflowExecutionHistory(ctx, req)
		if err != nil {
			return nil, err
		}
		events = append(events, resp.History.Events...)
		if len(resp.NextPageToken) == 0 {
			return events, nil
		}
		token = resp.NextPageToken
	}
}

func scenStart(events []*shared.HistoryEvent) (*shared.WorkflowExecutionStartedEventAttributes, error) {
	if len(events) == 0 || events[0].WorkflowExecutionStartedEventAttributes == nil {
		return nil, errors.New("history does not begin with WorkflowExecutionStarted")
	}
	return events[0].WorkflowExecutionStartedEventAttributes, nil
}

func scenPoll(ctx context.Context, d time.Duration, f func() error) error {
	pctx, cancel := context.WithTimeout(ctx, d)
	defer cancel()
	return retryFor(pctx, f)
}

func scenDescribe(ctx context.Context, rpc workflowserviceclient.Interface, domain, wid, runID string) (*shared.DescribeWorkflowExecutionResponse, error) {
	ex := &shared.WorkflowExecution{WorkflowId: &wid}
	if runID != "" {
		ex.RunId = &runID
	}
	return rpc.DescribeWorkflowExecution(ctx, &shared.DescribeWorkflowExecutionRequest{Domain: &domain, Execution: ex})
}

func scenStartReq(domain, tl, wid, wtype string, execTimeout int32) *shared.StartWorkflowExecutionRequest {
	return &shared.StartWorkflowExecutionRequest{Domain: &domain, WorkflowId: &wid, WorkflowType: &shared.WorkflowType{Name: &wtype},
		TaskList: &shared.TaskList{Name: &tl}, ExecutionStartToCloseTimeoutSeconds: ptr(execTimeout), TaskStartToCloseTimeoutSeconds: ptr(int32(10)),
		RequestId: ptr(uuid.NewString()), WorkflowIdReusePolicy: shared.WorkflowIdReusePolicyAllowDuplicate.Ptr()}
}

func scenMain(rpc workflowserviceclient.Interface, domain, archDomain, archDir string) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	tl := "fe-gate-scen-" + uuid.NewString()

	// ---- 1a. retry policy + execution timeout -> attempt+1
	{
		wid := tl + "-retry"
		req := scenStartReq(domain, tl, wid, "main.SignalQueryWorkflow", 3)
		req.RetryPolicy = &shared.RetryPolicy{InitialIntervalInSeconds: ptr(int32(1)), BackoffCoefficient: ptr(1.0), MaximumAttempts: ptr(int32(2))}
		first, err := rpc.StartWorkflowExecution(ctx, req)
		if step("scen-retry-start", err) {
			run0 := first.GetRunId()
			var run1 string
			err = scenPoll(ctx, 2*time.Minute, func() error {
				ev, e := scenHistory(ctx, rpc, domain, wid, "")
				if e != nil {
					return e
				}
				sa, e := scenStart(ev)
				if e != nil {
					return e
				}
				if sa.GetAttempt() != 1 {
					return fmt.Errorf("current run attempt=%d, want 1", sa.GetAttempt())
				}
				if sa.GetInitiator() != shared.ContinueAsNewInitiatorRetryPolicy || sa.GetContinuedExecutionRunId() != run0 {
					return fmt.Errorf("initiator=%v continuedFrom=%q want RetryPolicy/%q", sa.GetInitiator(), sa.GetContinuedExecutionRunId(), run0)
				}
				cur, e := scenDescribe(ctx, rpc, domain, wid, "")
				if e != nil {
					return e
				}
				run1 = cur.WorkflowExecutionInfo.Execution.GetRunId()
				return nil
			})
			step("scen-retry-timeout-next-attempt", err)
			ev0, err := scenHistory(ctx, rpc, domain, wid, run0)
			if err == nil {
				last := ev0[len(ev0)-1]
				if last.GetEventType() != shared.EventTypeWorkflowExecutionContinuedAsNew || last.WorkflowExecutionContinuedAsNewEventAttributes.GetInitiator() != shared.ContinueAsNewInitiatorRetryPolicy || last.WorkflowExecutionContinuedAsNewEventAttributes.GetNewExecutionRunId() != run1 {
					err = fmt.Errorf("first run last event %v attrs=%v want ContinuedAsNew(RetryPolicy)->%s", last.GetEventType(), last.WorkflowExecutionContinuedAsNewEventAttributes, run1)
				}
			}
			step("scen-retry-first-run-continued-as-new", err)
			err = scenPoll(ctx, 2*time.Minute, func() error {
				d, e := scenDescribe(ctx, rpc, domain, wid, run1)
				if e != nil {
					return e
				}
				if d.WorkflowExecutionInfo.CloseStatus == nil || *d.WorkflowExecutionInfo.CloseStatus != shared.WorkflowExecutionCloseStatusTimedOut {
					return fmt.Errorf("close status %v want TimedOut (retries exhausted)", d.WorkflowExecutionInfo.CloseStatus)
				}
				return nil
			})
			step("scen-retry-exhausted-timed-out", err)
		}
	}

	// ---- 1b. cron workflow timing out -> next cron run
	{
		wid := tl + "-cron"
		req := scenStartReq(domain, tl, wid, "main.SignalQueryWorkflow", 3)
		req.CronSchedule = ptr("@every 5s")
		first, err := rpc.StartWorkflowExecution(ctx, req)
		if step("scen-cron-start", err) {
			run0 := first.GetRunId()
			err = scenPoll(ctx, 3*time.Minute, func() error {
				ev, e := scenHistory(ctx, rpc, domain, wid, "")
				if e != nil {
					return e
				}
				sa, e := scenStart(ev)
				if e != nil {
					return e
				}
				if sa.GetInitiator() != shared.ContinueAsNewInitiatorCronSchedule || sa.GetContinuedExecutionRunId() == "" {
					return fmt.Errorf("current run initiator=%v continuedFrom=%q want CronSchedule", sa.GetInitiator(), sa.GetContinuedExecutionRunId())
				}
				return nil
			})
			step("scen-cron-timeout-next-run", err)
			d, derr := scenDescribe(ctx, rpc, domain, wid, run0)
			if derr == nil && (d.WorkflowExecutionInfo.CloseStatus == nil || *d.WorkflowExecutionInfo.CloseStatus != shared.WorkflowExecutionCloseStatusContinuedAsNew) {
				derr = fmt.Errorf("first cron run close status %v want ContinuedAsNew", d.WorkflowExecutionInfo.CloseStatus)
			}
			step("scen-cron-first-run-continued-as-new", derr)
			step("scen-cron-terminate", rpc.TerminateWorkflowExecution(ctx, &shared.TerminateWorkflowExecutionRequest{Domain: &domain, WorkflowExecution: &shared.WorkflowExecution{WorkflowId: &wid}, Reason: ptr("scen done")}))
		}
	}

	// ---- 2. strong idempotency
	{
		w := worker.New(rpc, domain, tl+"-idem", worker.Options{DisableStickyExecution: true})
		w.RegisterWorkflow(EchoWorkflow)
		w.RegisterWorkflow(SignalQueryWorkflow)
		w.RegisterActivity(EchoActivity)
		if step("scen-idem-worker-start", w.Start()) {
			c := client.NewClient(rpc, domain, nil)
			itl := tl + "-idem"
			wid := itl + "-start"
			reqID := uuid.NewString()
			req := scenStartReq(domain, itl, wid, "main.EchoWorkflow", 60)
			req.RequestId = &reqID
			req.Input = []byte(`"idem"`)
			r1, err := rpc.StartWorkflowExecution(ctx, req)
			if step("scen-idem-start", err) {
				var out string
				step("scen-idem-start-completes", c.GetWorkflow(ctx, wid, r1.GetRunId()).Get(ctx, &out))
				r2, err := rpc.StartWorkflowExecution(ctx, req)
				if err == nil && r2.GetRunId() != r1.GetRunId() {
					err = fmt.Errorf("same request id after close started new run %s != %s", r2.GetRunId(), r1.GetRunId())
				}
				step("scen-idem-start-request-id-dedup-after-close", err)
				req2 := scenStartReq(domain, itl, wid, "main.EchoWorkflow", 60)
				req2.Input = []byte(`"idem"`)
				r3, err := rpc.StartWorkflowExecution(ctx, req2)
				if err == nil && r3.GetRunId() == r1.GetRunId() {
					err = errors.New("new request id returned the old run")
				}
				step("scen-idem-start-new-request-id-new-run", err)
			}
			swid := itl + "-signal"
			sr, err := rpc.StartWorkflowExecution(ctx, scenStartReq(domain, itl, swid, "main.SignalQueryWorkflow", 120))
			if step("scen-idem-signal-wf-start", err) {
				exe := &shared.WorkflowExecution{WorkflowId: &swid, RunId: ptr(sr.GetRunId())}
				sig := func(reqID string) error {
					return rpc.SignalWorkflowExecution(ctx, &shared.SignalWorkflowExecutionRequest{Domain: &domain, WorkflowExecution: exe, SignalName: ptr("wake"), Input: []byte(`"v"`), RequestId: &reqID})
				}
				rA, rB := uuid.NewString(), uuid.NewString()
				e1, e2, e3 := sig(rA), sig(rA), sig(rB)
				step("scen-idem-signal-calls-ok", firstErr(e1, e2, e3))
				countSig := func() (int, error) {
					ev, err := scenHistory(ctx, rpc, domain, swid, sr.GetRunId())
					n := 0
					for _, e := range ev {
						if e.GetEventType() == shared.EventTypeWorkflowExecutionSignaled {
							n++
						}
					}
					return n, err
				}
				err := scenPoll(ctx, 90*time.Second, func() error {
					n, err := countSig()
					if err == nil && n < 2 {
						err = fmt.Errorf("%d signal events so far, want 2", n)
					}
					return err
				})
				if err == nil {
					time.Sleep(2 * time.Second)
					n, e := countSig()
					if e == nil && n != 2 {
						e = fmt.Errorf("%d signal events, want exactly 2 (duplicate request id must be deduped)", n)
					}
					err = e
				}
				step("scen-idem-signal-request-id-dedup", err)
				_ = rpc.TerminateWorkflowExecution(ctx, &shared.TerminateWorkflowExecutionRequest{Domain: &domain, WorkflowExecution: exe, Reason: ptr("scen done")})
			}
			w.Stop()
		}
	}

	// ---- 3. history archival (retention 0 -> delete task archives through the Go Worker archival workflow)
	if archDomain != "" {
		atl := tl + "-arch"
		w := worker.New(rpc, archDomain, atl, worker.Options{DisableStickyExecution: true})
		w.RegisterWorkflow(EchoWorkflow)
		w.RegisterActivity(EchoActivity)
		if step("scen-archival-worker-start", w.Start()) {
			c := client.NewClient(rpc, archDomain, nil)
			wid := atl + "-wf"
			run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: wid, TaskList: atl, ExecutionStartToCloseTimeout: time.Minute, DecisionTaskStartToCloseTimeout: 10 * time.Second}, EchoWorkflow, "archive-me")
			var runID string
			if step("scen-archival-start", err) {
				runID = run.GetRunID()
				err = scenPoll(ctx, 6*time.Minute, func() error {
					var files []string
					_ = filepath.Walk(archDir, func(p string, info os.FileInfo, e error) error {
						if e == nil && info.Mode().IsRegular() {
							files = append(files, p)
						}
						return nil
					})
					if len(files) == 0 {
						return fmt.Errorf("no archive file under %s yet", archDir)
					}
					fmt.Printf("SDK archive files: %s\n", strings.Join(files, ","))
					return nil
				})
				step("scen-archival-history-file-written", err)
				err = scenPoll(ctx, 4*time.Minute, func() error {
					_, e := scenDescribe(ctx, rpc, archDomain, wid, runID)
					var nf *shared.EntityNotExistsError
					if errors.As(e, &nf) {
						return nil
					}
					return fmt.Errorf("history not deleted yet: %v", e)
				})
				step("scen-archival-history-deleted-after-retention", err)
				err = scenPoll(ctx, 2*time.Minute, func() error {
					ev, e := scenHistory(ctx, rpc, archDomain, wid, runID)
					if e != nil {
						return e
					}
					if len(ev) < 5 || ev[len(ev)-1].GetEventType() != shared.EventTypeWorkflowExecutionCompleted {
						return fmt.Errorf("archived history has %d events, last=%v", len(ev), ev[len(ev)-1].GetEventType())
					}
					return nil
				})
				step("scen-archival-get-history-falls-back-to-archive", err)
			}
			w.Stop()
		}
	}

	// ---- 4. auto-reset on a bad binary checksum
	{
		bad := "gate-bad-binary-" + uuid.NewString()[:8]
		worker.SetBinaryChecksum(bad)
		btl := tl + "-badbin"
		w := worker.New(rpc, domain, btl, worker.Options{DisableStickyExecution: true})
		w.RegisterWorkflow(SignalQueryWorkflow)
		if step("scen-autoreset-worker-start", w.Start()) {
			wid := btl + "-wf"
			sr, err := rpc.StartWorkflowExecution(ctx, scenStartReq(domain, btl, wid, "main.SignalQueryWorkflow", 300))
			if step("scen-autoreset-start", err) {
				run0 := sr.GetRunId()
				exe := &shared.WorkflowExecution{WorkflowId: &wid, RunId: &run0}
				// The first decision completes under the (still good) checksum, which creates the auto-reset point.
				err = scenPoll(ctx, 2*time.Minute, func() error {
					ev, e := scenHistory(ctx, rpc, domain, wid, run0)
					if e != nil {
						return e
					}
					for _, x := range ev {
						if x.GetEventType() == shared.EventTypeDecisionTaskCompleted {
							return nil
						}
					}
					return errors.New("first decision not completed")
				})
				step("scen-autoreset-first-decision-completed", err)
				_, err = rpc.UpdateDomain(ctx, &shared.UpdateDomainRequest{Name: &domain, Configuration: &shared.DomainConfiguration{
					// Go persists a bad-binary-only update only when another config field also changes.
					EmitMetric: ptr(true),
					BadBinaries: &shared.BadBinaries{Binaries: map[string]*shared.BadBinaryInfo{bad: {Reason: ptr("gate auto-reset"), Operator: ptr("gate")}}}}})
				step("scen-autoreset-mark-binary-bad", err)
				time.Sleep(15 * time.Second) // domain cache refresh in History
				step("scen-autoreset-signal", rpc.SignalWorkflowExecution(ctx, &shared.SignalWorkflowExecutionRequest{Domain: &domain, WorkflowExecution: exe, SignalName: ptr("wake"), Input: []byte(`"after-bad"`), RequestId: ptr(uuid.NewString())}))
				var newRun string
				err = scenPoll(ctx, 4*time.Minute, func() error {
					cur, e := scenDescribe(ctx, rpc, domain, wid, "")
					if e != nil {
						return e
					}
					newRun = cur.WorkflowExecutionInfo.Execution.GetRunId()
					if newRun == run0 {
						return errors.New("still on the original run")
					}
					return nil
				})
				step("scen-autoreset-new-run-started", err)
				if err == nil {
					d, derr := scenDescribe(ctx, rpc, domain, wid, run0)
					if derr == nil && (d.WorkflowExecutionInfo.CloseStatus == nil || *d.WorkflowExecutionInfo.CloseStatus != shared.WorkflowExecutionCloseStatusTerminated) {
						derr = fmt.Errorf("original run close status %v want Terminated", d.WorkflowExecutionInfo.CloseStatus)
					}
					step("scen-autoreset-original-run-terminated", derr)
					ev, herr := scenHistory(ctx, rpc, domain, wid, newRun)
					if herr == nil {
						sa, herr2 := scenStart(ev)
						if herr2 == nil && sa.GetOriginalExecutionRunId() == "" {
							herr2 = errors.New("reset run has no OriginalExecutionRunId")
						}
						herr = herr2
					}
					step("scen-autoreset-new-run-links-original", herr)
				}
			}
			w.Stop()
		}
		worker.SetBinaryChecksum("")
	}
	fmt.Printf("SCEN done failures=%d\n", failures)
}
