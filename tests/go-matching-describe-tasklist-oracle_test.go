// This file is overlaid into go/service/matching/handler by the focused
// Actions workflow so the original Go handler and engine report their exact
// malformed-request behavior without modifying the imported Go source tree.
package handler

import (
	"context"
	"errors"

	"github.com/uber/cadence/common/types"
)

func (s *matchingEngineSuite) TestMalformedDescribeTaskListOracle() {
	s.mockDomainCache.EXPECT().GetDomainName("missing-domain").
		Return("", errors.New("oracle unknown domain")).AnyTimes()
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
		{name: "invalid-domain-valid-task-list", req: &types.MatchingDescribeTaskListRequest{
			DomainUUID:  "missing-domain",
			DescRequest: &types.DescribeTaskListRequest{TaskList: &types.TaskList{Name: "oracle-task-list"}},
		}},
	}
	for _, tc := range cases {
		response, err := handler.DescribeTaskList(context.Background(), tc.req)
		s.T().Logf("GO_DESCRIBE_ORACLE case=%s response_nil=%t error_type=%T error=%v", tc.name, response == nil, err, err)
	}
}
