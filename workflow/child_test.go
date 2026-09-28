package workflow_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	wf "github.com/lisama569/durable-workflow-engine-go/workflow"
)

// TestMain dispatches the child-process mode used by the crash-restart test.
func TestMain(m *testing.M) {
	switch os.Getenv("WF_TEST_MODE") {
	case "child":
		childMain() // calls os.Exit internally
	case "dup-child":
		dupChildMain()
		os.Exit(0)
	}
	os.Exit(m.Run())
}

// TestMainChildProcess is a no-op test used only to produce a recognizable
// invocation name when the test binary runs in child mode.
func TestMainChildProcess(t *testing.T) {}

// dupChildMain is the cross-process duplicate-submit worker.
func dupChildMain() {
	dataDir := os.Getenv("WF_DATA_DIR")
	runID := os.Getenv("WF_RUN_ID")
	tag := os.Getenv("WF_TAG")

	def := &wf.Definition{Name: "dup-wf", Steps: []wf.StepDef{
		{ID: "dup-a", Retry: wf.RetryPolicy{MaxAttempts: 1}},
		{ID: "dup-b", Depends: []wf.StepID{"dup-a"}, Retry: wf.RetryPolicy{MaxAttempts: 1}},
	}}
	registry := wf.NewRegistry()
	_ = registry.Register(def)
	store, err := wf.NewFileStore(dataDir)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(3)
	}
	exec := wf.StepExecutorFunc(func(ctx context.Context, req wf.StepRequest) (wf.StepResult, error) {
		fmt.Printf("dup-child tag=%s pid=%d executing step=%s\n", tag, os.Getpid(), req.Step.ID)
		// Small overlap window so both processes genuinely race the create.
		time.Sleep(20 * time.Millisecond)
		return wf.StepResult{Output: "ok"}, nil
	})
	engine := wf.NewEngine(store, registry, exec, wf.Options{
		Concurrency: 1,
		Sleep:       func(ctx context.Context, d time.Duration) error { return nil },
	})
	st, err := engine.Submit(context.Background(), wf.RunID(runID), "dup-wf")
	if err != nil {
		fmt.Fprintln(os.Stderr, "submit:", err)
		os.Exit(4)
	}
	fmt.Printf("dup-child tag=%s pid=%d final=%s\n", tag, os.Getpid(), st.Status)
}
