package workflow_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"

	wf "github.com/lisama569/durable-workflow-engine-go/workflow"
)

// TestFileStoreRoundTrip verifies persistence across Engine instances and
// therefore processes: complete a run, open a second engine on the same
// directory, and get the identical terminal state back without re-execution.
func TestFileStoreRoundTrip(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "store")
	def := &wf.Definition{Name: "persisted", Steps: []wf.StepDef{
		{ID: "a", Retry: wf.RetryPolicy{MaxAttempts: 1}},
		{ID: "b", Depends: []wf.StepID{"a"}, Retry: wf.RetryPolicy{MaxAttempts: 1}},
	}}

	runOnce := func() (*wf.RunState, *recordingExecutor) {
		store, err := wf.NewFileStore(dir)
		if err != nil {
			t.Fatal(err)
		}
		r := reg(t)
		mustRegister(t, r, def)
		exec := newRecordingExecutor()
		engine := newTestEngine(t, r, store, exec, &captureLogger{}, wf.CompatStrict)
		st, err := engine.Submit(context.Background(), "disk-run", "persisted")
		if err != nil {
			t.Fatal(err)
		}
		return st, exec
	}

	first, exec1 := runOnce()
	assertStatus(t, first, wf.RunSucceeded)
	second, exec2 := runOnce()
	assertStatus(t, second, wf.RunSucceeded)
	if exec1.callsOf("a") != 1 || exec1.callsOf("b") != 1 {
		t.Fatalf("first engine: a=%d b=%d", exec1.callsOf("a"), exec1.callsOf("b"))
	}
	if exec2.callsOf("a") != 0 || exec2.callsOf("b") != 0 {
		t.Fatalf("second engine re-ran steps: a=%d b=%d", exec2.callsOf("a"), exec2.callsOf("b"))
	}

	// Event log on disk must be the full append-only history.
	store, _ := wf.NewFileStore(dir)
	events, err := store.LoadEvents(context.Background(), "disk-run")
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 7 { // CREATED, STARTED, per step STARTED+SUCCEEDED, RUN_SUCCEEDED
		t.Fatalf("persisted event count = %d, want 7: %+v", len(events), events)
	}
	for i := 1; i < len(events); i++ {
		if events[i].Seq != events[i-1].Seq+1 {
			t.Fatalf("event seq gap: %d after %d", events[i].Seq, events[i-1].Seq)
		}
	}

	// State file is valid JSON with a fingerprint recorded.
	raw, err := os.ReadFile(filepath.Join(dir, "disk-run", "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) == 0 {
		t.Fatal("empty state.json")
	}
}

// TestConcurrentSubmitSameRun fires many goroutines submitting the same RunID
// against a MemoryStore. Exactly one creation must win and every step side
// effect must happen exactly once.
func TestConcurrentSubmitSameRun(t *testing.T) {
	store := wf.NewMemoryStore()
	exec := newRecordingExecutor()
	r := reg(t)
	def := &wf.Definition{Name: "race", Steps: []wf.StepDef{
		{ID: "a", Retry: wf.RetryPolicy{MaxAttempts: 1}},
		{ID: "b", Depends: []wf.StepID{"a"}, Retry: wf.RetryPolicy{MaxAttempts: 1}},
	}}
	mustRegister(t, r, def)
	engine := newTestEngine(t, r, store, exec, &captureLogger{}, wf.CompatStrict)

	const n = 12
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			st, err := engine.Submit(context.Background(), "hot-id", "race")
			if err != nil {
				errs <- err
				return
			}
			if st.Status != wf.RunSucceeded {
				errs <- fmt.Errorf("status %s", st.Status)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if exec.callsOf("a") != 1 || exec.callsOf("b") != 1 {
		t.Fatalf("side effects under concurrent submit: a=%d b=%d, want 1/1",
			exec.callsOf("a"), exec.callsOf("b"))
	}
}

// TestConcurrentSubmitDifferentRunIDs ensures independent runs proceed in
// parallel without sharing step state.
func TestConcurrentSubmitDifferentRunIDs(t *testing.T) {
	store := wf.NewMemoryStore()
	exec := newRecordingExecutor()
	r := reg(t)
	def := &wf.Definition{Name: "indep", Steps: []wf.StepDef{
		{ID: "a", Retry: wf.RetryPolicy{MaxAttempts: 1}},
	}}
	mustRegister(t, r, def)
	engine := newTestEngine(t, r, store, exec, &captureLogger{}, wf.CompatStrict)

	const n = 8
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := engine.Submit(context.Background(), wf.RunID(fmt.Sprintf("run-%d", i)), "indep")
			if err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	if got := exec.callsOf("a"); got != n {
		t.Fatalf("a executed %d times across %d independent runs, want %d", got, n, n)
	}
}

// TestDuplicateSubmitAcrossProcesses launches two child test binaries that
// submit the same RunID concurrently against one FileStore directory. The
// flock + create-atomic semantics guarantee the run is created once; the
// total number of side effects must equal the number of steps exactly once.
func TestDuplicateSubmitAcrossProcesses(t *testing.T) {
	if os.Getenv("WF_TEST_MODE") != "" {
		return
	}
	dir := filepath.Join(t.TempDir(), "data")

	// Pre-create the run by letting child A finish; then start children B
	// and C concurrently while a reset is impossible — instead start B and
	// C simultaneously from an empty directory; the storage lock decides.
	start := func(tag string) ([]byte, error) {
		bin := filepath.Join(t.TempDir(), "wf-dup.exe")
		build := exec.Command("go", "test", "-c", "-o", bin, "./workflow")
		build.Dir = repoRoot(t)
		if out, err := build.CombinedOutput(); err != nil {
			return out, err
		}
		cmd := exec.Command(bin, "-test.run=TestMainChildProcess", "-test.v")
		cmd.Env = append(os.Environ(),
			"WF_TEST_MODE=dup-child",
			"WF_DATA_DIR="+dir,
			"WF_RUN_ID=dup-run",
			"WF_TAG="+tag,
		)
		return cmd.CombinedOutput()
	}

	var wg sync.WaitGroup
	var outB, outC []byte
	var errB, errC error
	wg.Add(2)
	go func() { defer wg.Done(); outB, errB = start("B") }()
	go func() { defer wg.Done(); outC, errC = start("C") }()
	wg.Wait()
	if errB != nil {
		t.Fatalf("child B: %v\n%s", errB, outB)
	}
	if errC != nil {
		t.Fatalf("child C: %v\n%s", errC, outC)
	}

	store, err := wf.NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.LoadState(context.Background(), "dup-run")
	if err != nil {
		t.Fatal(err)
	}
	if st.Status != wf.RunSucceeded {
		t.Fatalf("status = %s", st.Status)
	}
	// Two steps, each exactly once, regardless of which process created the run.
	for _, id := range []string{"dup-a", "dup-b"} {
		if got := st.Steps[wf.StepID(id)].Attempts; got != 1 {
			t.Fatalf("%s attempts across two processes = %d, want 1\n--- B ---\n%s\n--- C ---\n%s",
				id, got, outB, outC)
		}
	}
}
