package tasklist

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/types"
)

// Exercise the real manager up to its cache boundary, without starting pumps.
func TestSourceDomainBoundaryManagerOracle(t *testing.T) {
	for _, source := range []string{"malformed-source", "\xffsource\x80"} {
		for _, forwarded := range []string{"", "child-partition"} {
			t.Run(source+"/"+forwarded, func(t *testing.T) {
				id := NewTestTaskListID(t, "destination-domain", "oracle", persistence.TaskListTypeDecision)
				manager, deps := setupMocksForTaskListManager(t, id, types.TaskListKindNormal)
				// NewManager arms this barrier; no background processes are needed here.
				manager.startWG.Done()
				lookupErr := &types.EntityNotExistsError{Message: "source lookup failed"}
				deps.mockDomainCache.EXPECT().GetDomainByID(source).Return(nil, lookupErr).Times(1)
				matched, err := manager.AddTask(context.Background(), AddTaskParams{
					TaskInfo: &persistence.TaskInfo{DomainID: source}, ForwardedFrom: forwarded,
				})
				require.False(t, matched)
				require.Same(t, lookupErr, err)
				t.Logf("source bytes=%x forwarded=%q error_type=%T error=%v", []byte(source), forwarded, err, err)
			})
		}
	}
}
