package workflow_test

import (
	"context"
	"errors"
	"testing"
	"time"

	wf "github.com/lisama569/durable-workflow-engine-go/workflow"
)

// TestDuplicateSubmitIsIdempotent submits the same RunID twice. The second
// Submit must return the terminal state without executing any step again.
func TestDuplicateSubmitIsIdempotent(t *testing.T) {
	store := wf.NewMemoryStore()
	exec := newRecordingExecutor()
	r := reg(t)
	def := &wf.Definition{Name: "dup", Steps: []wf.StepDef{
		{ID: "a", Retry: wf.RetryPolicy{MaxAttempts: 1}},
		{ID: "b", Depends: []wf.StepID{"a"}, Retry: wf.RetryPolicy{MaxAttempts: 1}},
	}}
	mustRegister(t, r, def)
	engine := newTestEngine(t, r, store, exec, &captureLogger{}, wf.CompatStrict)

	first, err := engine.Submit(context.Background(), "same-id", "dup")
	if err != nil {
		t.Fatal(err)
	}
	assertStatus(t, first, wf.RunSucceeded)

	second, err := engine.Submit(context.Background(), "same-id", "dup")
	if err != nil {
		t.Fatalf("duplicate submit returned error: %v", err)
	}
	assertStatus(t, second, wf.RunSucceeded)
	if exec.callsOf("a") != 1 || exec.callsOf("b") != 1 {
		t.Fatalf("side effects repeated on duplicate submit: a=%d b=%d",
			exec.callsOf("a"), exec.callsOf("b"))
	}
}

// TestResumeFromCheckpoint simulates a process crash: a step previously
// SUCCEEDED sits next to a step left RUNNING and a PENDING one. On resume the
// succeeded step is never executed; the interrupted one is reset and retried;
// the pending one runs once.
func TestResumeFromCheckpoint(t *testing.T) {
	store := wf.NewMemoryStore()
	exec := newRecordingExecutor()
	r := reg(t)
	def := &wf.Definition{Name: "resume", Steps: []wf.StepDef{
		{ID: "done", Retry: wf.RetryPolicy{MaxAttempts: 1}},
		{ID: "interrupted", Depends: []wf.StepID{"done"}, Retry: wf.RetryPolicy{MaxAttempts: 1}},
		{ID: "later", Depends: []wf.StepID{"interrupted"}, Retry: wf.RetryPolicy{MaxAttempts: 1}},
	}}
	mustRegister(t, r, def)
	engine := newTestEngine(t, r, store, exec, &captureLogger{}, wf.CompatStrict)

	// First, create and run to completion... then deliberately corrupt the
	// state into a crashed mid-flight snapshot and re-submit.
	if _, err := engine.Submit(context.Background(), "crash-1", "resume"); err != nil {
		t.Fatal(err)
	}
	st, err := store.LoadState(context.Background(), "crash-1")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	st.Status = wf.RunRunning
	st.Steps["done"].Status = wf.StepSucceeded
	st.Steps["done"].Attempts = 1
	st.Steps["done"].Output = "from-first-process"
	st.Steps["interrupted"].Status = wf.StepRunning
	st.Steps["interrupted"].Attempts = 1
	st.Steps["later"].Status = wf.StepPending
	st.Steps["later"].Attempts = 0
	for _, s := range st.Steps {
		s.LastEventAt = now
	}
	if err := store.SaveState(context.Background(), st); err != nil {
		t.Fatal(err)
	}
	// Reset in-process call counters by swapping in a fresh executor,
	// mirroring a brand-new process.
	exec2 := newRecordingExecutor()
	engine2 := newTestEngine(t, r, store, exec2, &captureLogger{}, wf.CompatStrict)

	resumed, err := engine2.Submit(context.Background(), "crash-1", "resume")
	if err != nil {
		t.Fatal(err)
	}
	assertStatus(t, resumed, wf.RunSucceeded)
	assertStep(t, resumed, "done", wf.StepSucceeded, 1)
	assertStep(t, resumed, "interrupted", wf.StepSucceeded, 1)
	assertStep(t, resumed, "later", wf.StepSucceeded, 1)
	// In the new process, the already-committed step must not have run.
	if exec2.callsOf("done") != 0 {
		t.Fatalf("completed step re-executed after restart: %d calls", exec2.callsOf("done"))
	}
	if exec2.callsOf("interrupted") != 1 || exec2.callsOf("later") != 1 {
		t.Fatalf("post-restart calls interrupted=%d later=%d, want 1/1",
			exec2.callsOf("interrupted"), exec2.callsOf("later"))
	}
	if got := resumed.Steps["done"].Output; got != "from-first-process" {
		t.Fatalf("completed step output was overwritten: %q", got)
	}
}

// TestDefinitionChangeStrictKeepsCheckpoint verifies that after a definition
// changes, resuming an old run in STRICT mode errors out without modifying
// the stored checkpoint and without running any step.
func TestDefinitionChangeStrictKeepsCheckpoint(t *testing.T) {
	store := wf.NewMemoryStore()
	exec := newRecordingExecutor()
	r := reg(t)
	v1 := &wf.Definition{Name: "svc", Steps: []wf.StepDef{
		{ID: "a", Input: "v1", Retry: wf.RetryPolicy{MaxAttempts: 1}},
		{ID: "b", Depends: []wf.StepID{"a"}, Input: "v1", Retry: wf.RetryPolicy{MaxAttempts: 1}},
	}}
	mustRegister(t, r, v1)
	engine := newTestEngine(t, r, store, exec, &captureLogger{}, wf.CompatStrict)

	st, err := engine.Submit(context.Background(), "run", "svc")
	if err != nil {
		t.Fatal(err)
	}
	assertStatus(t, st, wf.RunSucceeded)

	// Rewind the run to a non-terminal state (b not finished yet), as if the
	// first process had crashed. A new process boots with a changed
	// definition registered under a NEW name (the registry rejects in-place
	// replacement). Resuming the run must hit the fingerprint check.
	st, err = store.LoadState(context.Background(), "run")
	if err != nil {
		t.Fatal(err)
	}
	st.Status = wf.RunRunning
	st.Steps["b"].Status = wf.StepPending
	st.Steps["b"].Attempts = 0
	st.Steps["b"].Output = ""
	if err := store.SaveState(context.Background(), st); err != nil {
		t.Fatal(err)
	}

	v2 := &wf.Definition{Name: "svc-v2", Steps: []wf.StepDef{
		{ID: "a", Input: "v2-CHANGED", Retry: wf.RetryPolicy{MaxAttempts: 1}},
		{ID: "b", Depends: []wf.StepID{"a"}, Input: "v1", Retry: wf.RetryPolicy{MaxAttempts: 1}},
	}}
	mustRegister(t, r, v2)
	st.DefName = "svc-v2"
	if err := store.SaveState(context.Background(), st); err != nil {
		t.Fatal(err)
	}
	exec2 := newRecordingExecutor()
	engine2 := newTestEngine(t, r, store, exec2, &captureLogger{}, wf.CompatStrict)

	_, err = engine2.Submit(context.Background(), "run", "svc-v2")
	if !errors.Is(err, wf.ErrDefinitionChanged) {
		t.Fatalf("want ErrDefinitionChanged, got %v", err)
	}
	if exec2.callsOf("a") != 0 || exec2.callsOf("b") != 0 {
		t.Fatalf("steps executed despite incompatible definition: a=%d b=%d",
			exec2.callsOf("a"), exec2.callsOf("b"))
	}
	// Checkpoint is preserved with the old fingerprint and non-terminal status.
	after, _ := store.LoadState(context.Background(), "run")
	if after.Fingerprint != st.Fingerprint {
		t.Fatalf("fingerprint rewritten: got %s want %s", after.Fingerprint, st.Fingerprint)
	}
	if after.Status != wf.RunRunning || after.Steps["b"].Status != wf.StepPending {
		t.Fatalf("checkpoint mutated during rejected resume: status=%s b=%s",
			after.Status, after.Steps["b"].Status)
	}
}

// TestDefinitionChangeAdditive verifies the explicit escape hatch: a
// still-running run may adopt a definition that only appends steps, while a
// mutation of a known step is still rejected.
func TestDefinitionChangeAdditive(t *testing.T) {
	store := wf.NewMemoryStore()
	exec := newRecordingExecutor()
	r := reg(t)
	v1 := &wf.Definition{Name: "grow", Steps: []wf.StepDef{
		{ID: "a", Retry: wf.RetryPolicy{MaxAttempts: 1}},
		{ID: "b", Depends: []wf.StepID{"a"}, Retry: wf.RetryPolicy{MaxAttempts: 1}},
	}}
	mustRegister(t, r, v1)
	engine := newTestEngine(t, r, store, exec, &captureLogger{}, wf.CompatAllowAdditive)

	st, err := engine.Submit(context.Background(), "g1", "grow")
	if err != nil {
		t.Fatal(err)
	}
	assertStatus(t, st, wf.RunSucceeded)

	// Terminal runs must never be reopened even by an additive definition.
	v2 := &wf.Definition{Name: "grow-v2", Steps: []wf.StepDef{
		{ID: "a", Retry: wf.RetryPolicy{MaxAttempts: 1}},
		{ID: "b", Depends: []wf.StepID{"a"}, Retry: wf.RetryPolicy{MaxAttempts: 1}},
		{ID: "c", Depends: []wf.StepID{"b"}, Retry: wf.RetryPolicy{MaxAttempts: 1}},
	}}
	mustRegister(t, r, v2)
	g1stored, _ := store.LoadState(context.Background(), "g1")
	g1stored.DefName = "grow-v2"
	if err := store.SaveState(context.Background(), g1stored); err != nil {
		t.Fatal(err)
	}
	exec2 := newRecordingExecutor()
	engine2 := newTestEngine(t, r, store, exec2, &captureLogger{}, wf.CompatAllowAdditive)
	replayed, err := engine2.Submit(context.Background(), "g1", "grow-v2")
	if err != nil {
		t.Fatal(err)
	}
	assertStatus(t, replayed, wf.RunSucceeded)
	if exec2.callsOf("a") != 0 || exec2.callsOf("b") != 0 {
		t.Fatalf("terminal run reopened: a=%d b=%d", exec2.callsOf("a"), exec2.callsOf("b"))
	}

	// Non-terminal run with an appended step: the new step executes once.
	st3, _ := store.LoadState(context.Background(), "g1")
	st3.ID = "g2"
	st3.DefName = "grow-v2"
	st3.Status = wf.RunRunning
	if err := store.CreateState(context.Background(), st3); err != nil {
		t.Fatal(err)
	}
	exec3 := newRecordingExecutor()
	engine3 := newTestEngine(t, r, store, exec3, &captureLogger{}, wf.CompatAllowAdditive)
	grown, err := engine3.Submit(context.Background(), "g2", "grow-v2")
	if err != nil {
		t.Fatal(err)
	}
	// a/b already succeeded in v1 -> not re-run; appended c runs once.
	if exec3.callsOf("a") != 0 || exec3.callsOf("b") != 0 || exec3.callsOf("c") != 1 {
		t.Fatalf("additive resume calls a=%d b=%d c=%d, want 0/0/1",
			exec3.callsOf("a"), exec3.callsOf("b"), exec3.callsOf("c"))
	}
	assertStep(t, grown, "c", wf.StepSucceeded, 1)

	// Mutating a known step must still be rejected even in additive mode.
	v3 := &wf.Definition{Name: "grow-v3", Steps: []wf.StepDef{
		{ID: "a", Input: "changed", Retry: wf.RetryPolicy{MaxAttempts: 1}},
		{ID: "b", Depends: []wf.StepID{"a"}, Retry: wf.RetryPolicy{MaxAttempts: 1}},
	}}
	mustRegister(t, r, v3)
	st4, _ := store.LoadState(context.Background(), "g2")
	st4.ID = "g3"
	st4.DefName = "grow-v3"
	st4.Status = wf.RunRunning
	if err := store.CreateState(context.Background(), st4); err != nil {
		t.Fatal(err)
	}
	_, err = engine3.Submit(context.Background(), "g3", "grow-v3")
	if !errors.Is(err, wf.ErrDefinitionChanged) {
		t.Fatalf("mutating existing step: want ErrDefinitionChanged, got %v", err)
	}
}

func TestAllowDependencyFailureRunsAnyway(t *testing.T) {
	store := wf.NewMemoryStore()
	exec := newRecordingExecutor()
	exec.failUntil["bad"] = 9 // always fails
	r := reg(t)
	def := &wf.Definition{Name: "tolerant", Steps: []wf.StepDef{
		{ID: "bad", Retry: wf.RetryPolicy{MaxAttempts: 1}},
		{ID: "cleanup", Depends: []wf.StepID{"bad"},
			OnDependencyFailure: wf.AllowDependencyFailure,
			Retry:               wf.RetryPolicy{MaxAttempts: 1}},
	}}
	mustRegister(t, r, def)
	engine := newTestEngine(t, r, store, exec, &captureLogger{}, wf.CompatStrict)
	st, err := engine.Submit(context.Background(), "t1", "tolerant")
	if err != nil {
		t.Fatal(err)
	}
	assertStep(t, st, "bad", wf.StepFailed, 1)
	assertStep(t, st, "cleanup", wf.StepSucceeded, 1)
	// Run still fails overall because a required step failed; the point is
	// cleanup ran with the failed dependency omitted from inputs.
	assertStatus(t, st, wf.RunFailed)
}

func TestNextBackoffExponentialAndCapped(t *testing.T) {
	p := wf.RetryPolicy{MaxAttempts: 5, InitialBackoff: 10 * time.Millisecond, Multiplier: 2, MaxBackoff: 25 * time.Millisecond}
	cases := []struct {
		done int
		want time.Duration
	}{
		{1, 10 * time.Millisecond},
		{2, 20 * time.Millisecond},
		{3, 25 * time.Millisecond}, // capped
	}
	for _, c := range cases {
		if got := wf.ExportedNextBackoffForTest(p, c.done); got != c.want {
			t.Fatalf("after %d attempts: backoff=%s want %s", c.done, got, c.want)
		}
	}
}

func TestFingerprintStableAndSensitive(t *testing.T) {
	d1 := &wf.Definition{Name: "x", Steps: []wf.StepDef{
		{ID: "a", Depends: []wf.StepID{"b"}, Input: "i", Retry: wf.RetryPolicy{MaxAttempts: 2}},
		{ID: "b", Input: "j", Retry: wf.RetryPolicy{MaxAttempts: 1}},
	}}
	d2 := &wf.Definition{Name: "x", Steps: []wf.StepDef{
		{ID: "b", Input: "j", Retry: wf.RetryPolicy{MaxAttempts: 1}},
		// same dependency set, different declaration order
		{ID: "a", Depends: []wf.StepID{"b"}, Input: "i", Retry: wf.RetryPolicy{MaxAttempts: 2}},
	}}
	fp1, _ := d1.Fingerprint()
	fp2, _ := d2.Fingerprint()
	if fp1 != fp2 {
		t.Fatalf("reordering steps changed fingerprint: %s vs %s", fp1, fp2)
	}
	d3 := &wf.Definition{Name: "x", Steps: []wf.StepDef{
		{ID: "a", Depends: []wf.StepID{"b"}, Input: "i-CHANGED", Retry: wf.RetryPolicy{MaxAttempts: 2}},
		{ID: "b", Input: "j", Retry: wf.RetryPolicy{MaxAttempts: 1}},
	}}
	fp3, _ := d3.Fingerprint()
	if fp3 == fp1 {
		t.Fatal("input change did not change fingerprint")
	}
}

func TestInvalidDefinitionsRejected(t *testing.T) {
	bad := []*wf.Definition{
		{Name: "dup", Steps: []wf.StepDef{
			{ID: "a"}, {ID: "a"},
		}},
		{Name: "missing-dep", Steps: []wf.StepDef{
			{ID: "a", Depends: []wf.StepID{"ghost"}},
		}},
		{Name: "cycle", Steps: []wf.StepDef{
			{ID: "a", Depends: []wf.StepID{"c"}},
			{ID: "b", Depends: []wf.StepID{"a"}},
			{ID: "c", Depends: []wf.StepID{"b"}},
		}},
	}
	for _, d := range bad {
		if err := d.Validate(); err == nil {
			t.Fatalf("definition %q should be invalid", d.Name)
		}
	}
}
