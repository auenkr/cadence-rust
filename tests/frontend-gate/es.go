// Advanced visibility acceptance (FE_MODE=es): the Go SDK starts workflows with search attributes through
// the Rust Frontend; the Go Worker indexer (Kafka -> Elasticsearch) indexes them; List/Scan/Count/ListClosed
// are then answered by the Rust Frontend from real Elasticsearch.
// Env: FE_ES_KEY (keyword search attribute added with `admin cluster add-search-attr` before this runs).
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.uber.org/cadence/.gen/go/cadence/workflowserviceclient"
	"go.uber.org/cadence/.gen/go/shared"
	"go.uber.org/cadence/client"
	"go.uber.org/cadence/worker"
)

func esMain(rpc workflowserviceclient.Interface, domain string) {
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()
	addedKey := os.Getenv("FE_ES_KEY")
	tl := "fe-gate-es-" + uuid.NewString()
	w := worker.New(rpc, domain, tl, worker.Options{DisableStickyExecution: true})
	w.RegisterWorkflow(EchoWorkflow)
	w.RegisterActivity(EchoActivity)
	if !step("es-worker-start", w.Start()) {
		os.Exit(2)
	}
	defer w.Stop()
	c := client.NewClient(rpc, domain, nil)
	opts := func(id string, sa map[string]interface{}) client.StartWorkflowOptions {
		return client.StartWorkflowOptions{ID: id, TaskList: tl, ExecutionStartToCloseTimeout: 5 * time.Minute,
			DecisionTaskStartToCloseTimeout: 10 * time.Second, SearchAttributes: sa}
	}
	marker := "gate-" + strings.ReplaceAll(uuid.NewString(), "-", "")[:12]

	// GetSearchAttributes lists the added key and the Custom* defaults.
	gs, err := rpc.GetSearchAttributes(ctx)
	if step("es-get-search-attributes", err) {
		missing := []string{}
		for _, k := range []string{"CustomKeywordField", "CustomIntField", "WorkflowID", addedKey} {
			if _, ok := gs.Keys[k]; !ok {
				missing = append(missing, k)
			}
		}
		step("es-get-search-attributes-keys", errIf(len(missing) > 0, "missing keys %v", missing))
	}

	// Validator: unknown key and wrongly typed value are rejected like Go's SearchAttributesValidator.
	_, err = c.StartWorkflow(ctx, opts(tl+"-badkey", map[string]interface{}{"NoSuchAttr": "x"}), EchoWorkflow, "x")
	step("es-start-invalid-key-rejected", errIf(err == nil || !strings.Contains(err.Error(), "NoSuchAttr is not a valid search attribute key"), "got %v", err))
	_, err = c.StartWorkflow(ctx, opts(tl+"-badval", map[string]interface{}{"CustomIntField": "not-an-int"}), EchoWorkflow, "x")
	step("es-start-invalid-value-rejected", errIf(err == nil || !strings.Contains(err.Error(), "is not a valid search attribute value for key CustomIntField"), "got %v", err))

	ids := []string{tl + "-a", tl + "-b"}
	for i, id := range ids {
		sa := map[string]interface{}{"CustomKeywordField": marker, "CustomIntField": 40 + i}
		if addedKey != "" {
			sa[addedKey] = "added-" + marker
		}
		run, err := c.ExecuteWorkflow(ctx, opts(id, sa), EchoWorkflow, "hi")
		if !step("es-start-"+id[len(id)-1:], err) {
			continue
		}
		var out string
		step("es-complete-"+id[len(id)-1:], firstErr(run.Get(ctx, &out), errIf(out != "hi", "result %q", out)))
	}

	list := func(query string) ([]*shared.WorkflowExecutionInfo, error) {
		var all []*shared.WorkflowExecutionInfo
		var token []byte
		for {
			resp, err := rpc.ListWorkflowExecutions(ctx, &shared.ListWorkflowExecutionsRequest{Domain: &domain, PageSize: ptr(int32(1)), NextPageToken: token, Query: &query})
			if err != nil {
				return nil, err
			}
			all = append(all, resp.Executions...)
			if len(resp.NextPageToken) == 0 {
				return all, nil
			}
			token = resp.NextPageToken
		}
	}
	poll := func(name string, f func() error) {
		deadline := time.Now().Add(4 * time.Minute)
		var last error
		for time.Now().Before(deadline) {
			if last = f(); last == nil {
				break
			}
			time.Sleep(3 * time.Second)
		}
		step(name, last)
	}
	q := fmt.Sprintf("CustomKeywordField = '%s' and CloseStatus = 0", marker)
	poll("es-list-closed-by-attribute", func() error {
		got, err := list(q)
		if err != nil {
			return err
		}
		if len(got) != 2 {
			return fmt.Errorf("want 2 executions, got %d", len(got))
		}
		return nil
	})
	poll("es-list-order-by-and-paging", func() error {
		got, err := list(fmt.Sprintf("CustomKeywordField = '%s' order by CustomIntField desc", marker))
		if err != nil {
			return err
		}
		if len(got) != 2 || got[0].Execution.GetWorkflowId() != ids[1] {
			return fmt.Errorf("unexpected order/len %d", len(got))
		}
		raw := got[0].SearchAttributes.GetIndexedFields()["CustomIntField"]
		var n int
		if json.Unmarshal(raw, &n) != nil || n != 41 {
			return fmt.Errorf("CustomIntField=%s", raw)
		}
		return nil
	})
	poll("es-count", func() error {
		resp, err := rpc.CountWorkflowExecutions(ctx, &shared.CountWorkflowExecutionsRequest{Domain: &domain, Query: ptr(q)})
		if err != nil {
			return err
		}
		return errIf(resp.GetCount() != 2, "count %d", resp.GetCount())
	})
	poll("es-scan", func() error {
		var n int
		var token []byte
		for {
			resp, err := rpc.ScanWorkflowExecutions(ctx, &shared.ListWorkflowExecutionsRequest{Domain: &domain, PageSize: ptr(int32(1)), NextPageToken: token, Query: ptr(q)})
			if err != nil {
				return err
			}
			n += len(resp.Executions)
			if len(resp.NextPageToken) == 0 {
				break
			}
			token = resp.NextPageToken
		}
		return errIf(n != 2, "scan returned %d", n)
	})
	poll("es-list-closed-api", func() error {
		now := time.Now().UnixNano()
		resp, err := rpc.ListClosedWorkflowExecutions(ctx, &shared.ListClosedWorkflowExecutionsRequest{Domain: &domain, MaximumPageSize: ptr(int32(100)),
			StartTimeFilter: &shared.StartTimeFilter{EarliestTime: ptr(now - int64(time.Hour)), LatestTime: ptr(now + int64(time.Hour))},
			ExecutionFilter: &shared.WorkflowExecutionFilter{WorkflowId: &ids[0]}})
		if err != nil {
			return err
		}
		return errIf(len(resp.Executions) != 1, "closed by id returned %d", len(resp.Executions))
	})
	// Query errors keep Go's message.
	_, err = rpc.ListWorkflowExecutions(ctx, &shared.ListWorkflowExecutionsRequest{Domain: &domain, Query: ptr("NoSuchAttr = 'x'")})
	step("es-list-invalid-attribute-rejected", errIf(err == nil || !strings.Contains(err.Error(), "invalid search attribute"), "got %v", err))
	_, err = rpc.ListWorkflowExecutions(ctx, &shared.ListWorkflowExecutionsRequest{Domain: &domain, Query: ptr("CustomKeywordField = ")})
	step("es-list-invalid-query-rejected", errIf(err == nil || !strings.Contains(err.Error(), "Invalid query"), "got %v", err))
}

func errIf(cond bool, format string, a ...interface{}) error {
	if cond {
		return fmt.Errorf(format, a...)
	}
	return nil
}

func firstErr(errs ...error) error {
	for _, e := range errs {
		if e != nil {
			return e
		}
	}
	return nil
}
