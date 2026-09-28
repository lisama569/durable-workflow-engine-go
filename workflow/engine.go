package workflow

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// CompatMode controls what happens when a resubmitted/historical run was
// recorded under a different definition fingerprint.
type CompatMode string

const (
	// CompatStrict refuses execution when fingerprints differ, but keeps the
	// stored checkpoint intact so an operator can resume with the original
	// definition or migrate explicitly. This is the default.
	CompatStrict CompatMode = "STRICT"
	// CompatAllowAdditive resumes a run when the new definition only adds
	// brand-new steps: every step present in the stored run is unchanged,
	// and new steps have no state yet. A run that already reached a terminal
	// state is never re-opened, even in additive mode.
	CompatAllowAdditive CompatMode = "ALLOW_ADDITIVE"
)

// Options configures an Engine.
type Options struct {
	// Concurrency is the maximum number of step attempts running at once.
	// Zero defaults to 4.
	Concurrency int
	// Compatibility selects the definition-change policy. Defaults to STRICT.
	Compatibility CompatMode
	// Now overrides the clock (tests). nil uses time.Now.
	Now func() time.Time
	// Sleep waits for a backoff (tests may intercept it). nil uses time.Sleep.
	Sleep func(ctx context.Context, d time.Duration) error
	// Logger receives every scheduling decision. nil disables logging.
	Logger Logger
}

// Engine drives runs against a Store and a Registry.
type Engine struct {
	store    Store
	registry *Registry
	executor StepExecutor
	opts     Options

	runLocksMu sync.Mutex
	runLocks   map[RunID]*sync.Mutex
}

// NewEngine builds an engine.
func NewEngine(store Store, registry *Registry, executor StepExecutor, opts Options) *Engine {
	if opts.Concurrency <= 0 {
		opts.Concurrency = 4
	}
	if opts.Compatibility == "" {
		opts.Compatibility = CompatStrict
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.Logger == nil {
		opts.Logger = nopLogger{}
	}
	return &Engine{
		store:    store,
		registry: registry,
		executor: executor,
		opts:     opts,
		runLocks: make(map[RunID]*sync.Mutex),
	}
}

// runLock serializes in-process Submits of the same run; different runs run
// without contention.
func (e *Engine) runLock(id RunID) *sync.Mutex {
	e.runLocksMu.Lock()
	defer e.runLocksMu.Unlock()
	m, ok := e.runLocks[id]
	if !ok {
		m = &sync.Mutex{}
		e.runLocks[id] = m
	}
	return m
}

// Submit creates a run and executes it to completion (or ctx cancellation).
//
// Submitting an existing RunID is idempotent:
//   - if the run is terminal, the stored terminal state is returned and no
//     step side effect is triggered;
//   - if the run is non-terminal, it is resumed from its checkpoint;
//   - if the current definition conflicts with the recorded fingerprint, the
//     behavior follows Options.Compatibility (default: error, checkpoint kept).
func (e *Engine) Submit(ctx context.Context, id RunID, defName string) (*RunState, error) {
	if id == "" {
		return nil, fmt.Errorf("workflow: empty run id")
	}
	def, ok := e.registry.Get(defName)
	if !ok {
		return nil, fmt.Errorf("workflow: definition %q not registered", defName)
	}
	fp, err := def.Fingerprint()
	if err != nil {
		return nil, err
	}

	mu := e.runLock(id)
	mu.Lock()
	defer mu.Unlock()

	existing, err := e.store.LoadState(ctx, id)
	switch {
	case err == nil:
		return e.resume(ctx, id, def, fp, existing)
	default:
		if !isRunNotFound(err) {
			return nil, err
		}
	}

	now := e.opts.Now()
	state := &RunState{
		ID:          id,
		DefName:     defName,
		Fingerprint: fp,
		Status:      RunPending,
		Steps:       make(map[StepID]*StepState, len(def.Steps)),
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	for _, sd := range def.Steps {
		fpStep, err := def.StepFingerprint(sd.ID)
		if err != nil {
			return nil, err
		}
		state.Steps[sd.ID] = &StepState{ID: sd.ID, Fingerprint: fpStep, Status: StepPending, LastEventAt: now}
	}
	if err := e.store.CreateState(ctx, state); err != nil {
		return nil, err
	}

	s := &scheduler{engine: e, def: def, state: state}
	if err := s.append(ctx, Event{
		Kind:   EventRunCreated,
		Detail: fmt.Sprintf("def=%s fingerprint=%s", defName, fp),
	}); err != nil {
		return nil, err
	}
	if err := s.run(ctx); err != nil {
		return e.store.LoadState(ctx, id)
	}
	return e.store.LoadState(ctx, id)
}

// resume handles Submit for an already-stored run.
func (e *Engine) resume(ctx context.Context, id RunID, def *Definition, fp string, state *RunState) (*RunState, error) {
	// Terminal runs are immutable: duplicate submission is recognized and
	// never re-triggers any side effect.
	if state.Status == RunSucceeded || state.Status == RunFailed {
		return state, nil
	}

	if state.Fingerprint != fp {
		switch e.opts.Compatibility {
		case CompatAllowAdditive:
			if err := checkAdditive(def, state, e.opts.Now()); err != nil {
				// Keep the stored checkpoint untouched.
				return state, err
			}
			state.Fingerprint = fp
			if err := e.store.SaveState(ctx, state); err != nil {
				return nil, err
			}
		default:
			// STRICT: refuse rather than silently executing old records under
			// new semantics. Nothing on disk is modified.
			return state, fmt.Errorf("%w: run %s recorded %s, current %s",
				ErrDefinitionChanged, id, state.Fingerprint, fp)
		}
	}

	events, err := e.store.LoadEvents(ctx, id)
	if err != nil {
		return nil, err
	}
	nextSeq := 0
	for _, ev := range events {
		if ev.Seq >= nextSeq {
			nextSeq = ev.Seq + 1
		}
	}

	s := &scheduler{engine: e, def: def, state: state, nextSeq: nextSeq, resumed: true}
	if err := s.run(ctx); err != nil {
		return e.store.LoadState(ctx, id)
	}
	return e.store.LoadState(ctx, id)
}

// checkAdditive verifies that the new definition is a pure extension of the
// checkpoints stored in state:
//   - every step the run already knows must still exist with an identical
//     per-step fingerprint (same dependencies, failure strategy, retry, input);
//   - steps not present in state are treated as newly appended and get a fresh
//     PENDING state;
//   - a definition that removes or mutates a known step is rejected, leaving
//     the stored checkpoint untouched.
func checkAdditive(def *Definition, state *RunState, now time.Time) error {
	for _, sd := range def.Steps {
		st, known := state.Steps[sd.ID]
		if !known {
			continue // genuinely new step; initialized below
		}
		curFP, err := def.StepFingerprint(sd.ID)
		if err != nil {
			return err
		}
		if curFP != st.Fingerprint {
			return fmt.Errorf("%w: step %q changed semantics for run %s (old %s, new %s)",
				ErrDefinitionChanged, sd.ID, state.ID, st.Fingerprint, curFP)
		}
	}
	// Stored steps missing from the new definition were removed -> reject.
	for id := range state.Steps {
		if _, ok := def.Step(id); !ok {
			return fmt.Errorf("%w: new definition removes step %q from run %s",
				ErrDefinitionChanged, id, state.ID)
		}
	}
	// Initialize state for appended steps in deterministic (definition) order.
	for _, sd := range def.Steps {
		if _, exists := state.Steps[sd.ID]; !exists {
			fpStep, err := def.StepFingerprint(sd.ID)
			if err != nil {
				return err
			}
			state.Steps[sd.ID] = &StepState{ID: sd.ID, Fingerprint: fpStep, Status: StepPending, LastEventAt: now}
		}
	}
	return nil
}

// Get returns the current persisted state of a run.
func (e *Engine) Get(ctx context.Context, id RunID) (*RunState, error) {
	return e.store.LoadState(ctx, id)
}

// Events returns the append-only event log of a run.
func (e *Engine) Events(ctx context.Context, id RunID) ([]Event, error) {
	return e.store.LoadEvents(ctx, id)
}

func isRunNotFound(err error) bool {
	return errorIs(err, ErrRunNotFound)
}

// errorIs wraps errors.Is in one place so the package has a single seam.
func errorIs(err, target error) bool {
	type unwrapper interface{ Unwrap() error }
	for err != nil {
		if err == target {
			return true
		}
		u, ok := err.(unwrapper)
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}
