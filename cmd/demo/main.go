// Command demo runs an example workflow against ./workflow-data, printing
// per-step decisions. It requires no external services.
//
// Usage:
//
//	go run ./cmd/demo -run demo-1
//
// Re-running the same -run id demonstrates idempotent replay: completed steps
// are not executed again. Delete the data directory to start fresh.
//
// The "flaky" step fails its first two attempts and succeeds on the third,
// demonstrating the retry/backoff path. Set WF_CRASH_STEP=<step id> to make
// the process os.Exit(1) right before that step executes, then rerun with the
// same -run id to observe crash recovery.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lisama569/durable-workflow-engine-go/workflow"
)

func main() {
	runID := flag.String("run", "demo-1", "run id (same id = same durable run)")
	dataDir := flag.String("data", "./workflow-data", "directory for persisted run state")
	crashStep := flag.String("crash", os.Getenv("WF_CRASH_STEP"), "os.Exit(1) before executing this step id")
	flag.Parse()

	def := &workflow.Definition{
		Name: "demo",
		Steps: []workflow.StepDef{
			{ID: "load", Input: "source=orders.csv", Retry: workflow.RetryPolicy{MaxAttempts: 3, InitialBackoff: 10 * time.Millisecond, Multiplier: 2}},
			{ID: "validate", Depends: []workflow.StepID{"load"}, Input: "schema=v2", Retry: workflow.RetryPolicy{MaxAttempts: 1}},
			// Two independent branches run concurrently after validate.
			{ID: "enrich-a", Depends: []workflow.StepID{"validate"}, Input: "service=A", Retry: workflow.RetryPolicy{MaxAttempts: 2}},
			{ID: "enrich-b", Depends: []workflow.StepID{"validate"}, Input: "service=B", Retry: workflow.RetryPolicy{MaxAttempts: 2}},
			// Flaky: fails twice, succeeds on the third attempt.
			{ID: "flaky", Depends: []workflow.StepID{"validate"}, Input: "mode=fail-twice",
				Retry: workflow.RetryPolicy{MaxAttempts: 3, InitialBackoff: 10 * time.Millisecond, Multiplier: 2}},
			{ID: "merge", Depends: []workflow.StepID{"enrich-a", "enrich-b", "flaky"}, Input: "format=report",
				Retry: workflow.RetryPolicy{MaxAttempts: 1}},
		},
	}

	registry := workflow.NewRegistry()
	if err := registry.Register(def); err != nil {
		fail(err)
	}
	store, err := workflow.NewFileStore(*dataDir)
	if err != nil {
		fail(err)
	}

	var attempts sync.Map // step id -> *int64, in this process only
	exec := workflow.StepExecutorFunc(func(ctx context.Context, req workflow.StepRequest) (workflow.StepResult, error) {
		if *crashStep != "" && req.Step.ID == workflow.StepID(*crashStep) {
			fmt.Printf("[demo] crash requested before step %s (attempt %d); exiting\n", req.Step.ID, req.Attempt)
			os.Exit(1)
		}
		counter, _ := attempts.LoadOrStore(string(req.Step.ID), new(int64))
		n := atomic.AddInt64(counter.(*int64), 1)

		ups := make([]string, 0, len(req.Inputs))
		for k, v := range req.Inputs {
			ups = append(ups, fmt.Sprintf("%s->%q", k, v))
		}
		fmt.Printf("[demo] executing step=%s attempt=%d in-process-call=%d upstream={%s}\n",
			req.Step.ID, req.Attempt, n, strings.Join(ups, ","))

		if req.Step.ID == "flaky" && n < 3 {
			return workflow.StepResult{}, fmt.Errorf("simulated transient failure #%d", n)
		}
		return workflow.StepResult{Output: fmt.Sprintf("out:%s#%d", req.Step.ID, req.Attempt)}, nil
	})

	engine := workflow.NewEngine(store, registry, exec, workflow.Options{
		Concurrency: 4,
		Logger:      workflow.NewStdLogger(),
	})

	state, err := engine.Submit(context.Background(), workflow.RunID(*runID), "demo")
	if err != nil {
		fail(err)
	}
	fmt.Printf("[demo] run %s final status: %s\n", state.ID, state.Status)
	for _, sd := range def.Steps {
		st := state.Steps[sd.ID]
		fmt.Printf("[demo]   step=%-9s status=%-9s attempts=%d output=%q\n",
			st.ID, st.Status, st.Attempts, st.Output)
	}
	if state.Status != workflow.RunSucceeded {
		os.Exit(2)
	}
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "demo error:", err)
	os.Exit(1)
}
