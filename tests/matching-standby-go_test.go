package tasklist

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/uber/cadence/common/client"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/types"
)

func TestRustMatchingStandbyDecisions(t *testing.T) {
	type result struct {
		TaskType   int   `json:"task_type"`
		ScheduleID int64 `json:"schedule_id"`
		PendingID  int64 `json:"pending_id"`
		State      int32 `json:"state"`
		Closed     bool  `json:"closed"`
		Started    bool  `json:"started"`
		Error      bool  `json:"error"`
	}
	var results []result
	for _, taskType := range []int{0, 1, 2} {
		for _, pendingID := range []int64{0, 9, 10, 11} {
			for _, state := range []int32{0, 1, 2} {
				for _, closed := range []bool{false, true} {
					completer := &taskCompleterImpl{taskListID: &Identifier{taskType: taskType}, logger: log.NewNoop()}
					response := &types.DescribeWorkflowExecutionResponse{WorkflowExecutionInfo: &types.WorkflowExecutionInfo{}}
					if closed {
						status := types.WorkflowExecutionCloseStatusCompleted
						response.WorkflowExecutionInfo.CloseStatus = &status
					}
					if pendingID > 0 {
						if taskType == 0 {
							decisionState := types.PendingDecisionState(state)
							response.PendingDecision = &types.PendingDecisionInfo{ScheduleID: pendingID, State: &decisionState}
						} else {
							activityState := types.PendingActivityState(state)
							response.PendingActivities = []*types.PendingActivityInfo{{ScheduleID: pendingID, State: &activityState}}
						}
					}
					task := &InternalTask{Event: &genericTaskInfo{TaskInfo: &persistence.TaskInfo{ScheduleID: 10}}}
					started, err := completer.isTaskStarted(task, response)
					results = append(results, result{taskType, 10, pendingID, state, closed, started, err != nil})
				}
			}
		}
	}
	data, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(os.Getenv("CADENCE_MATCHING_STANDBY_OUTPUT"), append(data, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
}

func TestRustMatchingQueryVersions(t *testing.T) {
	checker := client.NewVersionChecker()
	for _, value := range []string{"1", "v1.5", "01.05.0", "1.5.0+build.42", "2.0.0.1"} {
		if err := checker.SupportsStickyQuery("uber-go", value); err != nil {
			t.Fatalf("%s: %v", value, err)
		}
	}
	for _, value := range []string{"1.5.0-alpha", "2.0.0-rc1", "1.invalid", "1.5.bad", "1.5.0+", "1.5.0+bad..build", "1.5.0+build!", "1.5.9223372036854775808"} {
		if checker.SupportsStickyQuery("uber-go", value) == nil || checker.SupportsConsistentQuery("uber-go", value) == nil {
			t.Fatalf("invalid or prerelease feature %s accepted", value)
		}
	}
	if err := checker.SupportsConsistentQuery("uber-go", "v1.5"); err != nil {
		t.Fatal(err)
	}
	if checker.SupportsConsistentQuery("uber-go", "1") == nil {
		t.Fatal("1 must not support consistent queries")
	}
}
