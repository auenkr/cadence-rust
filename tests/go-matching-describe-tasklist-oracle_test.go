// This file is overlaid into go/service/matching/handler by the focused
// Actions workflow so the original Go handler and engine report their exact
// malformed-request behavior without modifying the imported Go source tree.
package handler

import (
	"context"
	"errors"

	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/types"
	"go.uber.org/mock/gomock"
)

func (s *matchingEngineSuite) TestMalformedDescribeTaskListOracle() {
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
	s.matchingEngine.Start()
	handler := NewHandler(
		s.matchingEngine,
		defaultTestConfig(),
		s.mockDomainCache,
		s.matchingEngine.metricsClient,
		s.logger,
		s.logger,
	)

	valid := "test-domain-id"
	cases := []struct {
		name string
		req  *types.MatchingDescribeTaskListRequest
	}{
		{name: "absent-outer"},
		{name: "absent-nested", req: &types.MatchingDescribeTaskListRequest{DomainUUID: valid}},
		{name: "empty-task-list", req: &types.MatchingDescribeTaskListRequest{
			DomainUUID: valid,
			DescRequest: &types.DescribeTaskListRequest{TaskList: &types.TaskList{}},
		}},
		{name: "invalid-domain-absent-nested", req: &types.MatchingDescribeTaskListRequest{DomainUUID: "missing-domain"}},
		{name: "invalid-domain-empty-task-list", req: &types.MatchingDescribeTaskListRequest{
			DomainUUID:  "missing-domain",
			DescRequest: &types.DescribeTaskListRequest{TaskList: &types.TaskList{}},
		}},
		{name: "invalid-domain-malformed-reserved-name", req: &types.MatchingDescribeTaskListRequest{
			DomainUUID:  "missing-domain",
			DescRequest: &types.DescribeTaskListRequest{TaskList: &types.TaskList{Name: "/__cadence_sys//0"}},
		}},
		{name: "invalid-domain-valid-task-list", req: &types.MatchingDescribeTaskListRequest{
			DomainUUID:  "missing-domain",
			DescRequest: &types.DescribeTaskListRequest{TaskList: &types.TaskList{Name: "oracle-task-list"}},
		}},
		{name: "valid-domain-malformed-reserved-name", req: &types.MatchingDescribeTaskListRequest{
			DomainUUID:  valid,
			DescRequest: &types.DescribeTaskListRequest{TaskList: &types.TaskList{Name: "/__cadence_sys//0"}},
		}},
	}
	for _, tc := range cases {
		response, err := handler.DescribeTaskList(context.Background(), tc.req)
		s.T().Logf("GO_DESCRIBE_ORACLE case=%s response_nil=%t error_type=%T error=%v", tc.name, response == nil, err, err)
	}
}
