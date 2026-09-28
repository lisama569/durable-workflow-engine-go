package engine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lisama569/durable-workflow-engine-go/internal/compat"
	"github.com/lisama569/durable-workflow-engine-go/store"
	"github.com/lisama569/durable-workflow-engine-go/workflow"
)

// --- test helpers ----------------------------------------------------------

type counters struct {
	mu sync.Mutex
	m  map[string]*int64
}

func newCounters() *counters { return &counters{m: map[string]*int64{}} }

func (c *counters) get(id string) *int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	p, ok := c.m[id]
	if !ok {
		var v int64
		p = &v
		c.m[id] = p
	}
	return p
}

func (c *counters) inc(id string) int64 { return atomic.AddInt64(c.get(id), 1) }
func (c *counters) val(id string) int64 { return atomic.LoadInt64(c.get(id)) }

func testEngine(t *testing.T, mode CompatibilityMode, conc int) (*engineHarness, *bytes.Buffer) {
	t.Helper()
	st, err := store.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	logger := log.New(&buf, "[test] ", log.LstdFlags)
	eng := New(st, Options{Logger: logger, MaxConcurrency: conc, Compatibility: mode,
		Sleep: func(ctx context.Context, d time.Duration) error { return nil }})
	return &engineHarness{store: st, eng: eng}, &buf
}

type engineHarness struct {
	store *store.Store
	eng   *Engine
}

func countingStep(id string, c *counters, run func(ctx context.Context, in workflow.StepInput, n int64) (any, error)) workflow.StepDef {
	return workflow.StepDef{
		ID:    id,
		Retry: workflow.RetryPolicy{MaxAttempts: 1},
		Run: func(ctx context.Context, in workflow.StepInput) (any, error) {
			n := c.inc(id)
			return run(ctx, in, n)
		},
	}
}

func passthrough(id string, c *counters, deps ...string) workflow.StepDef {
	s := countingStep(id, c, func(_ context.Context, in workflow.StepInput, n int64) (any, error) {
		return map[string]any{"n": n, "inputs": in.Inputs}, nil
	})
	s.DependsOn = deps
	return s
}

// --- 1. DAG + concurrency --------------------------------------------------

func TestDAGRunsInOrderAndConcurrently(t *testing.T) {
	h, _ := testEngine(t, RejectOnConflict, 4)
	c := newCounters()

	var maxConcurrent int64
	var cur int64
	gateStep := func(id string, deps ...string) workflow.StepDef {
		s := countingStep(id, c, func(ctx context.Context, in workflow.StepInput, n int64) (any, error) {
			curNow := atomic.AddInt64(&cur, 1)
			for {
				old := atomic.LoadInt64(&maxConcurrent)
				if curNow <= old || atomic.CompareAndSwapInt64(&maxConcurrent, old, curNow) {
					break
				}
			}
			time.Sleep(30 * time.Millisecond)
			atomic.AddInt64(&cur, -1)
			return id + ":ok", nil
		})
		s.DependsOn = deps
		return s
	}

	def := &workflow.Definition{Name: "dag", Steps: []workflow.StepDef{
		gateStep("a"),
		gateStep("b"), // independent of a -> concurrent
		passthrough("c", c, "a", "b"),
	}}

	st, err := h.eng.Run(context.Background(), "e1", "", def)
	if err != nil {
		t.Fatal(err)
	}
	if st.Status != workflow.ExecSucceeded {
		t.Fatalf("status=%s steps=%+v", st.Status, st.Steps)
	}
	for _, id := range []string{"a", "b", "c"} {
		if c.val(id) != 1 {
			t.Fatalf("step %q ran %d times, want 1", id, c.val(id))
		}
	}
	if maxConcurrent < 2 {
		t.Fatalf("independent steps did not overlap, max concurrency observed=%d", maxConcurrent)
	}
	// Inputs isolation: c must see outputs of both a and b.
	out := st.Steps["c"].Output.(map[string]any)
	ins := out["inputs"].(map[string]any)
	if ins["a"] != "a:ok" || ins["b"] != "b:ok" {
		t.Fatalf("step c inputs wrong: %v", ins)
	}
}

// --- 2. failure propagation ------------------------------------------------

func TestFailureSkipsDownstream(t *testing.T) {
	h, buf := testEngine(t, RejectOnConflict, 2)
	c := newCounters()
	boom := errors.New("boom")
	fail := countingStep("a", c, func(_ context.Context, _ workflow.StepInput, n int64) (any, error) {
		return nil, boom
	})
	down := passthrough("b", c, "a")
	indep := passthrough("c", c)
	def := &workflow.Definition{Name: "fail", Steps: []workflow.StepDef{fail, down, indep}}

	st, err := h.eng.Run(context.Background(), "e1", "", def)
	if err != nil {
		t.Fatal(err)
	}
	if st.Status != workflow.ExecFailed {
		t.Fatalf("status=%s", st.Status)
	}
	if st.Steps["a"].Status != workflow.StepFailed {
		t.Fatalf("a=%s", st.Steps["a"].Status)
	}
	if st.Steps["b"].Status != workflow.StepSkipped || c.val("b") != 0 {
		t.Fatalf("b must be skipped without executing: %s runs=%d", st.Steps["b"].Status, c.val("b"))
	}
	if st.Steps["c"].Status != workflow.StepSucceeded || c.val("c") != 1 {
		t.Fatalf("independent step c must still succeed: %s runs=%d", st.Steps["c"].Status, c.val("c"))
	}
	if !strings.Contains(buf.String(), "decision=fail_exhausted") ||
		!strings.Contains(buf.String(), `step "b" skipped`) {
		t.Fatalf("log must explain decisions:\n%s", buf.String())
	}
}

// --- 3. retries ------------------------------------------------------------

func TestRetryThenSuccess(t *testing.T) {
	h, _ := testEngine(t, RejectOnConflict, 1)
	c := newCounters()
	s := countingStep("a", c, func(_ context.Context, _ workflow.StepInput, n int64) (any, error) {
		if n < 3 {
			return nil, fmt.Errorf("transitory failure #%d", n)
		}
		return "done", nil
	})
	s.Retry = workflow.RetryPolicy{MaxAttempts: 4, BaseDelay: time.Millisecond}
	def := &workflow.Definition{Name: "retry", Steps: []workflow.StepDef{s}}

	st, err := h.eng.Run(context.Background(), "e1", "", def)
	if err != nil {
		t.Fatal(err)
	}
	if st.Status != workflow.ExecSucceeded {
		t.Fatalf("status=%s err=%s", st.Status, st.Steps["a"].Err)
	}
	if c.val("a") != 3 {
		t.Fatalf("step ran %d times, want 3", c.val("a"))
	}
	if st.Steps["a"].Attempts != 3 || len(st.Steps["a"].History) != 3 {
		t.Fatalf("attempt bookkeeping wrong: %+v", st.Steps["a"])
	}
	if st.Steps["a"].Output != "done" {
		t.Fatalf("output=%v", st.Steps["a"].Output)
	}
}

func TestRetryExhausted(t *testing.T) {
	h, _ := testEngine(t, RejectOnConflict, 1)
	c := newCounters()
	s := countingStep("a", c, func(_ context.Context, _ workflow.StepInput, _ int64) (any, error) {
		return nil, errors.New("nope")
	})
	s.Retry = workflow.RetryPolicy{MaxAttempts: 3}
	def := &workflow.Definition{Name: "retry-ex", Steps: []workflow.StepDef{s}}

	st, err := h.eng.Run(context.Background(), "e1", "", def)
	if err != nil {
		t.Fatal(err)
	}
	if st.Status != workflow.ExecFailed || c.val("a") != 3 {
		t.Fatalf("status=%s runs=%d", st.Status, c.val("a"))
	}
}

// --- 4. crash recovery -----------------------------------------------------

// failingStore wraps Save to simulate a process exit after the "running"
// marker was persisted but before the step function could finish.
type failAfterRunning struct {
	*store.Store
	once sync.Map
}

func TestResumeAfterCrashSkipsSucceededSteps(t *testing.T) {
	h, _ := testEngine(t, RejectOnConflict, 1)
	c := newCounters()

	// Step "a" crashes exactly once: the first time it observes attempt 1
	// with the persisted AttemptKey a1, it returns a context-like terminal
	// error after the engine persisted running. We instead simulate by
	// cancelling the context during step a's first run.
	ctx1, cancel := context.WithCancel(context.Background())
	aStart := make(chan struct{})
	s1 := countingStep("a", c, func(ctx context.Context, _ workflow.StepInput, n int64) (any, error) {
		if n == 1 {
			close(aStart)
			<-ctx.Done() // process "killed" mid-attempt
			return nil, ctx.Err()
		}
		return "a-ok", nil
	})
	s1.Retry = workflow.RetryPolicy{MaxAttempts: 5}
	b := passthrough("b", c, "a")
	def := &workflow.Definition{Name: "crash", Steps: []workflow.StepDef{s1, b}}

	go func() {
		<-aStart
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	st, err := h.eng.Run(ctx1, "exec-crash", "", def)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
	if st.Steps["a"].Status != workflow.StepPending || st.Steps["a"].Attempts != 1 {
		t.Fatalf("after crash a must be parked pending with attempts=1: %+v", st.Steps["a"])
	}

	// Restart: a resumes (once more), b runs once.
	st2, err := h.eng.Run(context.Background(), "exec-crash", "", def)
	if err != nil {
		t.Fatal(err)
	}
	if st2.Status != workflow.ExecSucceeded {
		t.Fatalf("status=%s a=%+v b=%+v", st2.Status, st2.Steps["a"], st2.Steps["b"])
	}
	if c.val("a") != 2 {
		t.Fatalf("a ran %d times total, want 2 (interrupted attempt + retry)", c.val("a"))
	}
	if c.val("b") != 1 {
		t.Fatalf("b ran %d times, want 1", c.val("b"))
	}
}

func TestResumeSkipsAlreadySucceeded(t *testing.T) {
	h, _ := testEngine(t, RejectOnConflict, 1)
	c := newCounters()
	// a succeeds, then b crashes. On resume a must NOT run again.
	ctx1, cancel := context.WithCancel(context.Background())
	bStart := make(chan struct{})
	a := passthrough("a", c)
	sb := countingStep("b", c, func(ctx context.Context, _ workflow.StepInput, n int64) (any, error) {
		if n == 1 {
			close(bStart)
			<-ctx.Done()
			return nil, ctx.Err()
		}
		return "b-ok", nil
	})
	sb.DependsOn = []string{"a"}
	sb.Retry = workflow.RetryPolicy{MaxAttempts: 5}
	def := &workflow.Definition{Name: "crash2", Steps: []workflow.StepDef{a, sb}}
	go func() {
		<-bStart
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	if _, err := h.eng.Run(ctx1, "exec-crash2", "", def); !errors.Is(err, context.Canceled) {
		t.Fatalf("want canceled, got %v", err)
	}
	st, err := h.eng.Run(context.Background(), "exec-crash2", "", def)
	if err != nil {
		t.Fatal(err)
	}
	if st.Status != workflow.ExecSucceeded {
		t.Fatalf("status=%s", st.Status)
	}
	if c.val("a") != 1 {
		t.Fatalf("succeeded step a re-ran %d times after restart, want 1", c.val("a"))
	}
	if c.val("b") != 2 {
		t.Fatalf("b runs=%d, want 2", c.val("b"))
	}
}

// --- 5. idempotent submission ----------------------------------------------

func TestIdempotentDoubleSubmit(t *testing.T) {
	h, _ := testEngine(t, RejectOnConflict, 2)
	c := newCounters()
	release := make(chan struct{})
	started := make(chan struct{}, 2)
	s := countingStep("a", c, func(_ context.Context, _ workflow.StepInput, _ int64) (any, error) {
		started <- struct{}{}
		<-release
		return "ok", nil
	})
	def := &workflow.Definition{Name: "idem", Steps: []workflow.StepDef{s}}

	var wg sync.WaitGroup
	states := make([]*workflow.ExecutionState, 2)
	for i := range states {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			st, err := h.eng.Run(context.Background(), fmt.Sprintf("exec-%d", i), "same-key", def)
			if err != nil {
				t.Errorf("run %d: %v", i, err)
				return
			}
			states[i] = st
		}(i)
	}
	<-started
	time.Sleep(30 * time.Millisecond) // let the second submit block on the lock
	close(release)
	wg.Wait()

	if states[0] == nil || states[1] == nil {
		t.Fatal("both runs must return a state")
	}
	if states[0].ID != states[1].ID {
		t.Fatalf("double submit produced two executions: %s vs %s", states[0].ID, states[1].ID)
	}
	if c.val("a") != 1 {
		t.Fatalf("step ran %d times for a double submit, want 1", c.val("a"))
	}
}

// --- 6. definition compatibility ------------------------------------------

func TestDefinitionAddStepSafe(t *testing.T) {
	h, _ := testEngine(t, RejectOnConflict, 2)
	c := newCounters()
	v1 := &workflow.Definition{Name: "evolve", Steps: []workflow.StepDef{passthrough("a", c)}}
	st, err := h.eng.Run(context.Background(), "exec-v", "", v1)
	if err != nil {
		t.Fatal(err)
	}
	if st.Status != workflow.ExecSucceeded {
		t.Fatal(st.Status)
	}
	v2 := &workflow.Definition{Name: "evolve", Steps: []workflow.StepDef{
		passthrough("a", c),
		passthrough("b", c, "a"),
	}}
	st2, err := h.eng.Run(context.Background(), "exec-v", "", v2)
	if err != nil {
		t.Fatal(err)
	}
	if st2.Status != workflow.ExecSucceeded {
		t.Fatal(st2.Status)
	}
	if c.val("a") != 1 {
		t.Fatalf("a must not re-run after additive change, runs=%d", c.val("a"))
	}
	if c.val("b") != 1 {
		t.Fatalf("new step b must run once, runs=%d", c.val("b"))
	}
}

func TestDefinitionChangeRejectedByDefault(t *testing.T) {
	h, _ := testEngine(t, RejectOnConflict, 1)
	c := newCounters()
	v1 := &workflow.Definition{Name: "evolve2", Steps: []workflow.StepDef{passthrough("a", c)}}
	if _, err := h.eng.Run(context.Background(), "exec-x", "", v1); err != nil {
		t.Fatal(err)
	}
	a2 := passthrough("a", c)
	a2.Version = 2
	v2 := &workflow.Definition{Name: "evolve2", Steps: []workflow.StepDef{a2}}
	_, err := h.eng.Run(context.Background(), "exec-x", "", v2)
	if !errors.Is(err, ErrDefinitionConflict) {
		t.Fatalf("want ErrDefinitionConflict, got %v", err)
	}
	var ce *ConflictError
	if !errors.As(err, &ce) || len(ce.Changes) != 1 || ce.Changes[0].Kind != compat.ChangeStepChanged {
		t.Fatalf("conflict must explain the change: %+v", err)
	}
	// Stored breakpoint untouched.
	newHash, _ := v2.Hash()
	persisted, _ := h.store.Load("exec-x")
	if persisted.Steps["a"].Status != workflow.StepSucceeded || persisted.DefinitionHash == newHash {
		t.Fatal("stored state must keep the old definition and breakpoint")
	}
	if c.val("a") != 1 {
		t.Fatalf("rejected change must not re-run step, runs=%d", c.val("a"))
	}
}

func TestDefinitionChangeAcceptNewSemantics(t *testing.T) {
	h, _ := testEngine(t, AcceptNewSemantics, 1)
	c := newCounters()
	v1 := &workflow.Definition{Name: "evolve3", Steps: []workflow.StepDef{
		passthrough("a", c),
		passthrough("b", c, "a"),
	}}
	if _, err := h.eng.Run(context.Background(), "exec-y", "", v1); err != nil {
		t.Fatal(err)
	}
	a2 := passthrough("a", c)
	a2.Version = 2
	v2 := &workflow.Definition{Name: "evolve3", Steps: []workflow.StepDef{
		a2,
		passthrough("b", c, "a"),
	}}
	st, err := h.eng.Run(context.Background(), "exec-y", "", v2)
	if err != nil {
		t.Fatal(err)
	}
	if st.Status != workflow.ExecSucceeded {
		t.Fatal(st.Status)
	}
	if c.val("a") != 2 {
		t.Fatalf("changed a must re-run once, runs=%d", c.val("a"))
	}
	if c.val("b") != 2 {
		t.Fatalf("successor b must re-run once, runs=%d", c.val("b"))
	}
	if st.MigratedFromHash == "" {
		t.Fatal("migration provenance must be recorded")
	}
}

// --- 7. interruption during backoff ---------------------------------------

func TestResumeAfterCrashDuringBackoff(t *testing.T) {
	dir := t.TempDir()
	st, _ := store.New(dir)
	var buf bytes.Buffer
	logger := log.New(&buf, "[test] ", log.LstdFlags)
	c := newCounters()

	// A sleeper that signals that the backoff has started, then blocks until
	// cancelled to simulate the process dying while waiting to retry.
	backoffStarted := make(chan struct{})
	blockSleep := func(ctx context.Context, d time.Duration) error {
		close(backoffStarted)
		<-ctx.Done()
		return ctx.Err()
	}
	mkEngine := func(sleeper Sleeper) *Engine {
		return New(st, Options{Logger: logger, MaxConcurrency: 1, Sleep: sleeper})
	}

	s := countingStep("a", c, func(_ context.Context, _ workflow.StepInput, n int64) (any, error) {
		if n < 2 {
			return nil, errors.New("first failure")
		}
		return "recovered", nil
	})
	s.Retry = workflow.RetryPolicy{MaxAttempts: 3, BaseDelay: time.Hour}
	def := &workflow.Definition{Name: "backoff-crash", Steps: []workflow.StepDef{s}}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-backoffStarted // cancel only once the step is parked in backoff
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()
	st1, err := mkEngine(blockSleep).Run(ctx, "exec-b", "", def)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want canceled during backoff, got %v", err)
	}
	if st1.Steps["a"].Attempts != 1 || st1.Steps["a"].Status != workflow.StepPending {
		t.Fatalf("step must be parked pending with attempts=1: %+v", st1.Steps["a"])
	}

	// Fresh process: same dir, new engine, default instant test sleep.
	eng2 := New(st, Options{Logger: logger, MaxConcurrency: 1,
		Sleep: func(ctx context.Context, d time.Duration) error { return nil }})
	st2, err := eng2.Run(context.Background(), "exec-b", "", def)
	if err != nil {
		t.Fatal(err)
	}
	if st2.Status != workflow.ExecSucceeded {
		t.Fatalf("status=%s err=%s", st2.Status, st2.Steps["a"].Err)
	}
	if c.val("a") != 2 {
		t.Fatalf("a ran %d times total, want 2", c.val("a"))
	}
}

// --- 8. resubmitting a finished execution ----------------------------------

func TestResubmitFinishedExecution(t *testing.T) {
	h, _ := testEngine(t, RejectOnConflict, 2)
	c := newCounters()
	def := &workflow.Definition{Name: "again", Steps: []workflow.StepDef{passthrough("a", c)}}
	st1, err := h.eng.Run(context.Background(), "exec-fin", "key-fin", def)
	if err != nil {
		t.Fatal(err)
	}
	st2, err := h.eng.Run(context.Background(), "exec-other", "key-fin", def)
	if err != nil {
		t.Fatal(err)
	}
	if st1.ID != st2.ID || st2.Status != workflow.ExecSucceeded {
		t.Fatalf("resubmit must resolve to the same completed execution: %s vs %s", st1.ID, st2.ID)
	}
	if c.val("a") != 1 {
		t.Fatalf("resubmitting a finished execution re-ran the step: %d", c.val("a"))
	}
}

// --- 9. concurrent branches keep isolated state ---------------------------

func TestConcurrentBranchesStateIsolation(t *testing.T) {
	h, _ := testEngine(t, RejectOnConflict, 8)
	c := newCounters()
	const branches = 20
	steps := []workflow.StepDef{}
	for i := 0; i < branches; i++ {
		id := fmt.Sprintf("b%02d", i)
		s := countingStep(id, c, func(_ context.Context, in workflow.StepInput, n int64) (any, error) {
			time.Sleep(time.Duration((n*7)%15) * time.Millisecond)
			return id, nil
		})
		steps = append(steps, s)
	}
	join := passthrough("join", c)
	for i := 0; i < branches; i++ {
		join.DependsOn = append(join.DependsOn, fmt.Sprintf("b%02d", i))
	}
	steps = append(steps, join)
	def := &workflow.Definition{Name: "fanout", Steps: steps}

	st, err := h.eng.Run(context.Background(), "exec-fan", "", def)
	if err != nil {
		t.Fatal(err)
	}
	if st.Status != workflow.ExecSucceeded {
		t.Fatal(st.Status)
	}
	ins := st.Steps["join"].Output.(map[string]any)["inputs"].(map[string]any)
	for i := 0; i < branches; i++ {
		id := fmt.Sprintf("b%02d", i)
		if c.val(id) != 1 {
			t.Fatalf("%s ran %d times", id, c.val(id))
		}
		if ins[id] != id {
			t.Fatalf("join input for %s = %v", id, ins[id])
		}
	}
}
