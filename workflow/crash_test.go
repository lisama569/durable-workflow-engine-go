package workflow_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	wf "github.com/lisama569/durable-workflow-engine-go/workflow"
)

// TestRealProcessCrashAndRestart drives the engine in child processes:
//
//  1. process 1 runs until it crashes (os.Exit(1)) immediately before the
//     "report" step;
//  2. process 2 resumes the same RunID against the same data directory.
//
// Assertions: the steps completed before the crash run exactly once across
// both processes; the interrupted step runs once in process 2; the resumed
// run eventually SUCCEEDS. This test spawns real subprocesses (the TestMain
// mode switch at the bottom) and needs no external services.
func TestRealProcessCrashAndRestart(t *testing.T) {
	if os.Getenv("WF_TEST_MODE") != "" {
		return // parent-only test body; modes run below via runChild
	}
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")

	// Process 1: crash before "report".
	out1, err1 := runChild(t, dataDir, "crash-run", "report")
	if err1 == nil {
		t.Fatalf("child 1 should have crashed, output:\n%s", out1)
	}
	t.Logf("child 1 output:\n%s", out1)

	// Inspect the checkpoint left on disk after the hard exit.
	store, err := wf.NewFileStore(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	mid, err := store.LoadState(context.Background(), "crash-run")
	if err != nil {
		t.Fatalf("checkpoint missing after crash: %v", err)
	}
	for _, id := range []string{"extract", "transform"} {
		if mid.Steps[wf.StepID(id)].Status != wf.StepSucceeded {
			t.Fatalf("after crash, %s = %s, want SUCCEEDED", id, mid.Steps[wf.StepID(id)].Status)
		}
	}

	// Process 2: resume, no crash point.
	out2, err2 := runChild(t, dataDir, "crash-run", "")
	if err2 != nil {
		t.Fatalf("child 2 failed: %v\noutput:\n%s", err2, out2)
	}
	t.Logf("child 2 output:\n%s", out2)

	final, err := store.LoadState(context.Background(), "crash-run")
	if err != nil {
		t.Fatal(err)
	}
	if final.Status != wf.RunSucceeded {
		t.Fatalf("final status = %s, want SUCCEEDED", final.Status)
	}
	// Attempts persisted in state: pre-crash steps kept attempt 1; report
	// itself never committed an attempt before the exit.
	for _, id := range []string{"extract", "transform", "report"} {
		if final.Steps[wf.StepID(id)].Attempts != 1 {
			t.Fatalf("%s attempts = %d, want 1 across both processes",
				id, final.Steps[wf.StepID(id)].Attempts)
		}
	}
}

func runChild(t *testing.T, dataDir, runID, crashBefore string) ([]byte, error) {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "wf-crash-child.exe")
	build := exec.Command("go", "test", "-c", "-o", bin, "./workflow")
	build.Dir = repoRoot(t)
	if out, err := build.CombinedOutput(); err != nil {
		return out, fmt.Errorf("build child: %w", err)
	}
	cmd := exec.Command(bin, "-test.run=TestMainChildProcess", "-test.v")
	cmd.Env = append(os.Environ(),
		"WF_TEST_MODE=child",
		"WF_DATA_DIR="+dataDir,
		"WF_RUN_ID="+runID,
		"WF_CRASH_BEFORE="+crashBefore,
	)
	return cmd.CombinedOutput()
}

func repoRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	return filepath.Dir(wd) // workflow/ -> repo root
}

// ---- child process entrypoint ---------------------------------------------

var childCallCount int64

func childMain() {
	dataDir := os.Getenv("WF_DATA_DIR")
	runID := os.Getenv("WF_RUN_ID")
	crashBefore := os.Getenv("WF_CRASH_BEFORE")

	def := &wf.Definition{Name: "etl", Steps: []wf.StepDef{
		{ID: "extract", Retry: wf.RetryPolicy{MaxAttempts: 2}},
		{ID: "transform", Depends: []wf.StepID{"extract"}, Retry: wf.RetryPolicy{MaxAttempts: 1}},
		{ID: "report", Depends: []wf.StepID{"transform"}, Retry: wf.RetryPolicy{MaxAttempts: 1}},
	}}
	registry := wf.NewRegistry()
	if err := registry.Register(def); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(3)
	}
	store, err := wf.NewFileStore(dataDir)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(3)
	}
	exec := wf.StepExecutorFunc(func(ctx context.Context, req wf.StepRequest) (wf.StepResult, error) {
		n := atomic.AddInt64(&childCallCount, 1)
		fmt.Printf("child pid=%d executing step=%s attempt=%d in-process-call=%d\n",
			os.Getpid(), req.Step.ID, req.Attempt, n)
		if crashBefore != "" && string(req.Step.ID) == crashBefore {
			fmt.Printf("child pid=%d CRASH before %s\n", os.Getpid(), crashBefore)
			os.Exit(1)
		}
		return wf.StepResult{Output: "ok:" + string(req.Step.ID)}, nil
	})
	engine := wf.NewEngine(store, registry, exec, wf.Options{
		Concurrency: 1, // deterministic ordering for the crash test
		Sleep:       func(ctx context.Context, d time.Duration) error { return nil },
		Logger:      wf.NewStdLogger(),
	})
	state, err := engine.Submit(context.Background(), wf.RunID(runID), "etl")
	if err != nil {
		fmt.Fprintln(os.Stderr, "submit:", err)
		os.Exit(4)
	}
	fmt.Printf("child pid=%d final=%s\n", os.Getpid(), state.Status)
	if state.Status != wf.RunSucceeded {
		os.Exit(5)
	}
	os.Exit(0)
}
