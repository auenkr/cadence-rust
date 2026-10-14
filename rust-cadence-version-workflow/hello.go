package hello

import (
	"context"
	"time"

	"go.uber.org/cadence/workflow"
)

// HelloWorkflow waits for a timer then runs an activity and returns a greeting.
func HelloWorkflow(ctx workflow.Context) (string, error) {
	ao := workflow.ActivityOptions{
		ScheduleToStartTimeout: time.Minute,
		StartToCloseTimeout:    time.Minute,
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	// A short timer proves timer decisions against the Rust backend.
	_ = workflow.Sleep(ctx, time.Second)

	var result string
	err := workflow.ExecuteActivity(ctx, HelloActivity, "Rust Cadence").Get(ctx, &result)
	if err != nil {
		return "", err
	}
	// Keep this sample execution open so it can be inspected and signaled.
	var finish string
	workflow.GetSignalChannel(ctx, "finish").Receive(ctx, &finish)
	return result, nil
}

// HelloActivity is the side-effecting step.
func HelloActivity(ctx context.Context, name string) (string, error) {
	return "Hello " + name, nil
}
