// Command demo runs an example workflow against a local on-disk store. It
// demonstrates ordering + concurrency, retry with backoff, crash recovery
// and idempotent resubmission without any external service.
//
// Usage:
//
//	go run ./cmd/demo reset      # fresh run, step "flaky" fails twice then succeeds
//	go run ./cmd/demo crash      # cancel the process mid-run, then run "resume"
//	go run ./cmd/demo resume     # resume the interrupted execution
//	go run ./cmd/demo idem       # submit the same idempotency key twice
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/lisama569/durable-workflow-engine-go/engine"
	"github.com/lisama569/durable-workflow-engine-go/store"
	"github.com/lisama569/durable-workflow-engine-go/workflow"
)

const execID = "demo-execution"

func main() {
	mode := "reset"
	if len(os.Args) > 1 {
		mode = os.Args[1]
	}
	dir := filepath.Join(os.TempDir(), "durable-workflow-demo")
	if mode == "reset" || mode == "crash" {
		_ = os.RemoveAll(dir)
	}
	st, err := store.New(dir)
	if err != nil {
		log.Fatal(err)
	}
	logger := log.New(os.Stdout, "[demo] ", log.LstdFlags|log.Lmsgprefix)
	eng := engine.New(st, engine.Options{Logger: logger, MaxConcurrency: 4})

	// A flaky step: its attempt number is delivered from persisted state, so
	// "crash" followed by "resume" eventually succeeds without re-running
	// any step that already finished.
	flaky := workflow.StepDef{
		ID:    "flaky",
		Retry: workflow.RetryPolicy{MaxAttempts: 5, BaseDelay: 200 * time.Millisecond, MaxDelay: time.Second, Multiplier: 2},
		Run: func(ctx context.Context, in workflow.StepInput) (any, error) {
			logger.Printf("  -> flaky attempt %d (attempt key %s)", in.Attempt, in.AttemptKey)
			if in.Attempt <= 2 {
				return nil, fmt.Errorf("transient failure on attempt %d", in.Attempt)
			}
			return "flaky-ok", nil
		},
	}
	mk := func(id string, deps ...string) workflow.StepDef {
		return workflow.StepDef{
			ID:        id,
			DependsOn: deps,
			Run: func(ctx context.Context, in workflow.StepInput) (any, error) {
				logger.Printf("  -> %s running with inputs %v", id, in.Inputs)
				time.Sleep(200 * time.Millisecond)
				return id + "-ok", nil
			},
		}
	}
	def := &workflow.Definition{
		Name:    "demo-workflow",
		Version: 1,
		Steps: []workflow.StepDef{
			flaky,
			mk("left"),
			mk("right"),
			mk("join", "flaky", "left", "right"),
		},
	}

	switch mode {
	case "crash":
		ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
		defer cancel()
		st, err := eng.Run(ctx, execID, "", def)
		logger.Printf("run interrupted: %v; state left on disk at %s (status=%s)", err, dir, st.Status)
	case "resume", "reset":
		st, err := eng.Run(context.Background(), execID, "", def)
		if err != nil {
			log.Fatal(err)
		}
		logger.Printf("execution %q finished: %s", st.ID, st.Status)
		for _, id := range []string{"flaky", "left", "right", "join"} {
			s := st.Steps[id]
			logger.Printf("  step %-6s status=%-9s attempts=%d output=%v", id, s.Status, s.Attempts, s.Output)
		}
	case "idem":
		key := "demo-key"
		run := func(i int) {
			st, err := eng.Run(context.Background(), fmt.Sprintf("submitted-%d", i), key, def)
			if err != nil {
				log.Fatal(err)
			}
			logger.Printf("submit %d resolved to execution %q (%s)", i, st.ID, st.Status)
		}
		run(1)
		run(2)
	default:
		log.Fatalf("unknown mode %q (want reset|crash|resume|idem)", mode)
	}
}
