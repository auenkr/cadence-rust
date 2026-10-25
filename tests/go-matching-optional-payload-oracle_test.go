// This file is overlaid into go/service/matching/handler by the focused
// Actions workflow so the original Go handler and engine report their exact
// malformed-request behavior without modifying the imported Go source tree.
package handler

import (
	"context"
	"errors"
	"time"

	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/types"
	"go.uber.org/mock/gomock"
)

func (s *matchingEngineSuite) TestOptionalPayloadErrorOracle() {
	// The suite setup installed an AnyTimes GetDomainName matcher. Stop that
	// engine and replace its cache with a fresh mock so the unknown-domain
	// result cannot be shadowed by a broad expectation.
	s.matchingEngine.Stop()
	domainCache := cache.NewMockDomainCache(s.controller)
	domainCache.EXPECT().GetDomainByID(gomock.Any()).
		Return(cache.CreateDomainCacheEntry(matchingTestDomainName), nil).AnyTimes()
	domainCache.EXPECT().GetDomain(gomock.Any()).
		Return(cache.CreateDomainCacheEntry(matchingTestDomainName), nil).AnyTimes()
	domainCache.EXPECT().GetDomainName(gomock.Any()).
		DoAndReturn(func(id string) (string, error) {
			switch id {
			case "test-domain-id":
				return matchingTestDomainName, nil
			case "":
				return "", errors.New("oracle empty domain")
			case "missing-domain":
				return "", errors.New("oracle unknown domain")
			default:
				return "", errors.New("oracle unexpected domain ID: " + id)
			}
		}).AnyTimes()
	domainCache.EXPECT().RegisterDomainChangeCallback(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()
	domainCache.EXPECT().UnregisterDomainChangeCallback(gomock.Any()).AnyTimes()
	s.mockDomainCache = domainCache
	s.matchingEngine = s.newMatchingEngine(defaultTestConfig(), s.taskManager)
	handler := NewHandler(
		s.matchingEngine,
		defaultTestConfig(),
		s.mockDomainCache,
		s.matchingEngine.metricsClient,
		s.logger,
		s.logger,
	)
	handler.Start()

	valid := "test-domain-id"
	bad := &types.TaskList{Name: "/__cadence_sys//0"}
	exec := &types.WorkflowExecution{WorkflowID: "wf", RunID: "run"}
	type call func(ctx context.Context) (interface{}, error)
	cases := []struct {
		name string
		fn   call
	}{
		{"decision-poll-absent-pollrequest", func(ctx context.Context) (interface{}, error) {
			return handler.PollForDecisionTask(ctx, &types.MatchingPollForDecisionTaskRequest{DomainUUID: valid})
		}},
		{"decision-poll-empty-tasklist", func(ctx context.Context) (interface{}, error) {
			return handler.PollForDecisionTask(ctx, &types.MatchingPollForDecisionTaskRequest{DomainUUID: valid, PollRequest: &types.PollForDecisionTaskRequest{}})
		}},
		{"decision-poll-malformed-unknown-domain", func(ctx context.Context) (interface{}, error) {
			return handler.PollForDecisionTask(ctx, &types.MatchingPollForDecisionTaskRequest{DomainUUID: "missing-domain", PollRequest: &types.PollForDecisionTaskRequest{TaskList: bad}})
		}},
		{"activity-poll-absent-pollrequest", func(ctx context.Context) (interface{}, error) {
			return handler.PollForActivityTask(ctx, &types.MatchingPollForActivityTaskRequest{DomainUUID: valid})
		}},
		{"activity-poll-empty-tasklist", func(ctx context.Context) (interface{}, error) {
			return handler.PollForActivityTask(ctx, &types.MatchingPollForActivityTaskRequest{DomainUUID: valid, PollRequest: &types.PollForActivityTaskRequest{}})
		}},
		{"activity-poll-malformed", func(ctx context.Context) (interface{}, error) {
			return handler.PollForActivityTask(ctx, &types.MatchingPollForActivityTaskRequest{DomainUUID: valid, PollRequest: &types.PollForActivityTaskRequest{TaskList: bad}})
		}},
		{"add-decision-malformed-unknown-domain", func(ctx context.Context) (interface{}, error) {
			return handler.AddDecisionTask(ctx, &types.AddDecisionTaskRequest{DomainUUID: "missing-domain", Execution: exec, TaskList: bad})
		}},
		{"decision-poll-valid-name-unknown-domain", func(ctx context.Context) (interface{}, error) {
			return handler.PollForDecisionTask(ctx, &types.MatchingPollForDecisionTaskRequest{DomainUUID: "missing-domain", PollRequest: &types.PollForDecisionTaskRequest{TaskList: &types.TaskList{Name: "oracle-tl"}}})
		}},
		{"activity-poll-valid-name-unknown-domain", func(ctx context.Context) (interface{}, error) {
			return handler.PollForActivityTask(ctx, &types.MatchingPollForActivityTaskRequest{DomainUUID: "missing-domain", PollRequest: &types.PollForActivityTaskRequest{TaskList: &types.TaskList{Name: "oracle-tl"}}})
		}},
		{"add-decision-absent-tasklist", func(ctx context.Context) (interface{}, error) {
			return handler.AddDecisionTask(ctx, &types.AddDecisionTaskRequest{DomainUUID: valid, Execution: exec, ScheduleID: 1})
		}},
		{"add-activity-absent-tasklist", func(ctx context.Context) (interface{}, error) {
			return handler.AddActivityTask(ctx, &types.AddActivityTaskRequest{DomainUUID: valid, SourceDomainUUID: valid, Execution: exec, ScheduleID: 1})
		}},
		{"add-decision-absent-execution", func(ctx context.Context) (interface{}, error) {
			return handler.AddDecisionTask(ctx, &types.AddDecisionTaskRequest{DomainUUID: valid, TaskList: &types.TaskList{Name: "oracle-tl"}, ScheduleID: 1})
		}},
		{"cancel-poll-absent-tasklist", func(ctx context.Context) (interface{}, error) {
			return nil, handler.CancelOutstandingPoll(ctx, &types.CancelOutstandingPollRequest{DomainUUID: valid})
		}},
		{"cancel-poll-malformed", func(ctx context.Context) (interface{}, error) {
			return nil, handler.CancelOutstandingPoll(ctx, &types.CancelOutstandingPollRequest{DomainUUID: valid, TaskList: bad})
		}},
		{"update-absent-tasklist", func(ctx context.Context) (interface{}, error) {
			return handler.UpdateTaskListPartitionConfig(ctx, &types.MatchingUpdateTaskListPartitionConfigRequest{DomainUUID: valid, PartitionConfig: &types.TaskListPartitionConfig{}})
		}},
		{"update-absent-tasklist-absent-config", func(ctx context.Context) (interface{}, error) {
			return handler.UpdateTaskListPartitionConfig(ctx, &types.MatchingUpdateTaskListPartitionConfigRequest{DomainUUID: valid})
		}},
		{"update-malformed", func(ctx context.Context) (interface{}, error) {
			return handler.UpdateTaskListPartitionConfig(ctx, &types.MatchingUpdateTaskListPartitionConfigRequest{DomainUUID: valid, TaskList: bad, PartitionConfig: &types.TaskListPartitionConfig{}})
		}},
		{"refresh-absent-tasklist", func(ctx context.Context) (interface{}, error) {
			return handler.RefreshTaskListPartitionConfig(ctx, &types.MatchingRefreshTaskListPartitionConfigRequest{DomainUUID: valid})
		}},
		{"refresh-malformed", func(ctx context.Context) (interface{}, error) {
			return handler.RefreshTaskListPartitionConfig(ctx, &types.MatchingRefreshTaskListPartitionConfigRequest{DomainUUID: valid, TaskList: bad})
		}},
		{"query-malformed", func(ctx context.Context) (interface{}, error) {
			return handler.QueryWorkflow(ctx, &types.MatchingQueryWorkflowRequest{DomainUUID: valid, TaskList: bad})
		}},
	}
	for _, tc := range cases {
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		resp, err := func() (r interface{}, e error) {
			defer func() {
				if p := recover(); p != nil {
					e = errors.New("PANIC escaped handler")
				}
			}()
			return tc.fn(ctx)
		}()
		cancel()
		s.T().Logf("GO_OPTIONAL_ORACLE case=%s response=%T error_type=%T error=%v", tc.name, resp, err, err)
	}
}
