package workflow_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	wf "github.com/lisama569/durable-workflow-engine-go/workflow"
)

// ---- test helpers ----------------------------------------------------------

// recordingExecutor counts attempts per step across goroutines and replays a
// script of failures / outputs / behaviors.
type recordingExecutor struct {
	mu        sync.Mutex
	calls     map[string]int
	failUntil map[string]int // step id: fail attempts strictly before this call number
	block     map[string]chan struct{}
	outputs   map[string]string
}

func newRecordingExecutor() *recordingExecutor {
	return &recordingExecutor{
		calls:     map[string]int{},
		failUntil: map[string]int{},
		block:     map[string]chan struct{}{},
		outputs:   map[string]string{},
	}
}

func (r *recordingExecutor) Execute(ctx context.Context, req wf.StepRequest) (wf.StepResult, error) {
	r.mu.Lock()
	r.calls[string(req.Step.ID)]++
	n := r.calls[string(req.Step.ID)]
	until := r.failUntil[string(req.Step.ID)]
	ch, blocked := r.block[string(req.Step.ID)]
	out := r.outputs[string(req.Step.ID)]
	r.mu.Unlock()

	if blocked {
		select {
		case <-ch:
		case <-ctx.Done():
			return wf.StepResult{}, ctx.Err()
		}
	}
	if n <= until {
		return wf.StepResult{}, fmt.Errorf("synthetic failure #%d for %s", n, req.Step.ID)
	}
	if out == "" {
		out = fmt.Sprintf("out:%s#%d", req.Step.ID, n)
	}
	return wf.StepResult{Output: out}, nil
}

func (r *recordingExecutor) callsOf(id string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls[id]
}

// captureLogger collects emitted events for assertions on the explanation log.
type captureLogger struct {
	mu     sync.Mutex
	events []wf.Event
}

func (c *captureLogger) Log(ctx context.Context, runID wf.RunID, ev wf.Event) {
	c.mu.Lock()
	c.events = append(c.events, ev)
	c.mu.Unlock()
}

func (c *captureLogger) kindsFor(step string) []wf.EventKind {
	c.mu.Lock()
	defer c.mu.Unlock()
	var kinds []wf.EventKind
	for _, ev := range c.events {
		if string(ev.StepID) == step {
			kinds = append(kinds, ev.Kind)
		}
	}
	return kinds
}

func noSleep(ctx context.Context, d time.Duration) error { return nil }

func newTestEngine(t *testing.T, r *wf.Registry, store wf.Store, exec wf.StepExecutor, log wf.Logger, mode wf.CompatMode) *wf.Engine {
	t.Helper()
	return wf.NewEngine(store, r, exec, wf.Options{
		Concurrency:   8,
		Compatibility: mode,
		Sleep:         noSleep,
		Logger:        log,
	})
}

func reg(t *testing.T) *wf.Registry {
	t.Helper()
	r := wf.NewRegistry()
	return r
}

func mustRegister(t *testing.T, r *wf.Registry, def *wf.Definition) {
	t.Helper()
	if err := r.Register(def); err != nil {
		t.Fatalf("register %s: %v", def.Name, err)
	}
}

func assertStatus(t *testing.T, st *wf.RunState, want wf.RunStatus) {
	t.Helper()
	if st.Status != want {
		t.Fatalf("run status = %s, want %s", st.Status, want)
	}
}

func assertStep(t *testing.T, st *wf.RunState, id string, want wf.StepStatus, wantAttempts int) {
	t.Helper()
	got, ok := st.Steps[wf.StepID(id)]
	if !ok {
		t.Fatalf("step %s missing from state", id)
	}
	if got.Status != want || got.Attempts != wantAttempts {
		t.Fatalf("step %s = {status:%s attempts:%d}, want {status:%s attempts:%d}",
			id, got.Status, got.Attempts, want, wantAttempts)
	}
}

// ---- tests -----------------------------------------------------------------

func TestHappyPathLinear(t *testing.T) {
	store := wf.NewMemoryStore()
	exec := newRecordingExecutor()
	log := &captureLogger{}
	r := reg(t)
	def := &wf.Definition{Name: "linear", Steps: []wf.StepDef{
		{ID: "a", Retry: wf.RetryPolicy{MaxAttempts: 1}},
		{ID: "b", Depends: []wf.StepID{"a"}, Retry: wf.RetryPolicy{MaxAttempts: 1}},
		{ID: "c", Depends: []wf.StepID{"b"}, Retry: wf.RetryPolicy{MaxAttempts: 1}},
	}}
	mustRegister(t, r, def)
	engine := newTestEngine(t, r, store, exec, log, wf.CompatStrict)

	st, err := engine.Submit(context.Background(), "run-1", "linear")
	if err != nil {
		t.Fatal(err)
	}
	assertStatus(t, st, wf.RunSucceeded)
	for _, id := range []string{"a", "b", "c"} {
		assertStep(t, st, id, wf.StepSucceeded, 1)
		if exec.callsOf(id) != 1 {
			t.Fatalf("step %s executed %d times, want 1", id, exec.callsOf(id))
		}
	}
	// Every step must log its input (empty here) and a success verdict.
	kinds := log.kindsFor("b")
	if len(kinds) != 2 || kinds[0] != wf.EventStepStarted || kinds[1] != wf.EventStepSucceeded {
		t.Fatalf("b event kinds = %v, want [STARTED SUCCEEDED]", kinds)
	}
}

func TestConcurrentBranchesIsolated(t *testing.T) {
	// fan -> {a, b, c} -> join; a/b/c run in parallel and each gets only its
	// own dependency's output, never another branch's.
	store := wf.NewMemoryStore()
	exec := newRecordingExecutor()
	exec.outputs["fan"] = "FAN"
	exec.outputs["a"] = "AAA"
	exec.outputs["b"] = "BBB"
	exec.outputs["c"] = "CCC"

	var maxConcurrent int32
	var inFlight int32
	var mu sync.Mutex
	seenInputs := map[string]map[string]string{}

	r := reg(t)
	def := &wf.Definition{Name: "fanout", Steps: []wf.StepDef{
		{ID: "fan", Retry: wf.RetryPolicy{MaxAttempts: 1}},
		{ID: "a", Depends: []wf.StepID{"fan"}, Retry: wf.RetryPolicy{MaxAttempts: 1}},
		{ID: "b", Depends: []wf.StepID{"fan"}, Retry: wf.RetryPolicy{MaxAttempts: 1}},
		{ID: "c", Depends: []wf.StepID{"fan"}, Retry: wf.RetryPolicy{MaxAttempts: 1}},
		{ID: "join", Depends: []wf.StepID{"a", "b", "c"}, Retry: wf.RetryPolicy{MaxAttempts: 1}},
	}}
	mustRegister(t, r, def)

	// Instrument executor: track concurrency and the inputs each branch saw.
	instrumented := wf.StepExecutorFunc(func(ctx context.Context, req wf.StepRequest) (wf.StepResult, error) {
		mu.Lock()
		ins := map[string]string{}
		for k, v := range req.Inputs {
			ins[string(k)] = v
		}
		seenInputs[string(req.Step.ID)] = ins
		cur := int32(0)
		switch string(req.Step.ID) {
		case "a", "b", "c":
			inFlight++
			cur = inFlight
			if cur > maxConcurrent {
				maxConcurrent = cur
			}
		}
		mu.Unlock()
		if cur > 0 {
			// Hold briefly so branches overlap.
			time.Sleep(15 * time.Millisecond)
		}
		res, err := exec.Execute(ctx, req)
		mu.Lock()
		switch string(req.Step.ID) {
		case "a", "b", "c":
			inFlight--
		}
		mu.Unlock()
		return res, err
	})

	engine := newTestEngine(t, r, store, instrumented, &captureLogger{}, wf.CompatStrict)
	st, err := engine.Submit(context.Background(), "run-c", "fanout")
	if err != nil {
		t.Fatal(err)
	}
	assertStatus(t, st, wf.RunSucceeded)
	if maxConcurrent < 2 {
		t.Fatalf("expected overlapping concurrent branches, max in-flight = %d", maxConcurrent)
	}
	// Each branch saw only fan's output; join saw all three branch outputs.
	for _, br := range []string{"a", "b", "c"} {
		ins := seenInputs[br]
		if len(ins) != 1 || ins["fan"] != "FAN" {
			t.Fatalf("branch %s inputs = %v, want only {fan:FAN}", br, ins)
		}
	}
	join := seenInputs["join"]
	if len(join) != 3 || join["a"] != "AAA" || join["b"] != "BBB" || join["c"] != "CCC" {
		t.Fatalf("join inputs = %v, want all three branch outputs", join)
	}
	// Every side effect happened exactly once.
	for _, id := range []string{"fan", "a", "b", "c", "join"} {
		if exec.callsOf(id) != 1 {
			t.Fatalf("step %s executed %d times, want 1", id, exec.callsOf(id))
		}
	}
}

func TestRetryThenSuccessPersistsAttempts(t *testing.T) {
	store := wf.NewMemoryStore()
	exec := newRecordingExecutor()
	exec.failUntil["flaky"] = 2 // fails calls 1,2; succeeds on 3
	log := &captureLogger{}
	r := reg(t)
	def := &wf.Definition{Name: "retry", Steps: []wf.StepDef{
		{ID: "flaky", Retry: wf.RetryPolicy{MaxAttempts: 3, InitialBackoff: time.Millisecond, Multiplier: 2}},
	}}
	mustRegister(t, r, def)
	engine := newTestEngine(t, r, store, exec, log, wf.CompatStrict)

	st, err := engine.Submit(context.Background(), "r", "retry")
	if err != nil {
		t.Fatal(err)
	}
	assertStatus(t, st, wf.RunSucceeded)
	assertStep(t, st, "flaky", wf.StepSucceeded, 3)
	if exec.callsOf("flaky") != 3 {
		t.Fatalf("flaky calls = %d, want 3", exec.callsOf("flaky"))
	}
	kinds := log.kindsFor("flaky")
	want := []wf.EventKind{wf.EventStepStarted, wf.EventStepRetry, wf.EventStepRetry, wf.EventStepSucceeded}
	if fmt.Sprint(kinds) != fmt.Sprint(want) {
		t.Fatalf("flaky events = %v, want %v", kinds, want)
	}
}

func TestRetryExhaustedSkipsDownstream(t *testing.T) {
	store := wf.NewMemoryStore()
	exec := newRecordingExecutor()
	exec.failUntil["boom"] = 5 // always fails (max attempts 2)
	r := reg(t)
	def := &wf.Definition{Name: "fail", Steps: []wf.StepDef{
		{ID: "boom", Retry: wf.RetryPolicy{MaxAttempts: 2}},
		{ID: "after", Depends: []wf.StepID{"boom"}, Retry: wf.RetryPolicy{MaxAttempts: 1}},
		{ID: "indep", Retry: wf.RetryPolicy{MaxAttempts: 1}},
	}}
	mustRegister(t, r, def)
	engine := newTestEngine(t, r, store, exec, &captureLogger{}, wf.CompatStrict)

	st, err := engine.Submit(context.Background(), "r", "fail")
	if err != nil {
		t.Fatal(err)
	}
	assertStatus(t, st, wf.RunFailed)
	assertStep(t, st, "boom", wf.StepFailed, 2)
	assertStep(t, st, "after", wf.StepSkipped, 0)
	assertStep(t, st, "indep", wf.StepSucceeded, 1)
	if exec.callsOf("after") != 0 {
		t.Fatalf("downstream after failed dep executed %d times, want 0", exec.callsOf("after"))
	}
	if exec.callsOf("indep") != 1 {
		t.Fatalf("independent step calls = %d, want 1", exec.callsOf("indep"))
	}
}
