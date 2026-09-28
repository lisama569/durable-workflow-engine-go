// Package engine orchestrates the durable execution of workflows.
//
// Execution model (see docs/design.md for full semantics):
//
//   - Steps whose dependencies all succeeded are eligible; up to
//     Options.MaxConcurrency eligible steps run at the same time.
//   - A step whose dependency failed or was skipped is marked "skipped";
//     downstream work is never started behind a failed dependency.
//   - Before each attempt the state is persisted as "running" with a unique
//     AttemptKey, so a crash marks the attempt; on resume the interrupted
//     attempt is discarded and the step retries from its persisted attempt
//     counter. Succeeded steps never run again.
//   - Repeated Run calls with the same idempotency key resolve to one
//     execution, regardless of process restarts.
package engine

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/lisama569/durable-workflow-engine-go/internal/compat"
	"github.com/lisama569/durable-workflow-engine-go/internal/retry"
	"github.com/lisama569/durable-workflow-engine-go/store"
	"github.com/lisama569/durable-workflow-engine-go/workflow"
)

// Logger is the minimal logging surface used by the engine.
type Logger interface {
	Printf(format string, args ...any)
}

// CompatibilityMode controls resume behaviour when the definition changed.
type CompatibilityMode int

const (
	// RejectOnConflict refuses to resume an incompatible execution; the
	// stored breakpoint is preserved untouched.
	RejectOnConflict CompatibilityMode = iota
	// AcceptCompatible resumes when no recorded result is affected by the
	// definition diff (additive steps, or changes to never-run steps).
	AcceptCompatible
	// AcceptNewSemantics accepts the new definition; steps whose recorded
	// result is invalidated by the diff (and their successors) are reset to
	// pending and re-executed. Unaffected breakpoints are kept.
	AcceptNewSemantics
)

// ConflictError explains why an execution could not be resumed.
type ConflictError struct {
	ExecutionID string
	OldHash     string
	NewHash     string
	Changes     []compat.Change
}

// Is marks ConflictError as ErrDefinitionConflict.
func (e *ConflictError) Is(target error) bool { return target == ErrDefinitionConflict }

func (e *ConflictError) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "engine: definition conflict for execution %q (old hash %s, new hash %s):", e.ExecutionID, short(e.OldHash), short(e.NewHash))
	for _, c := range e.Changes {
		fmt.Fprintf(&b, "\n  - %s on %q: %s", c.Kind, c.StepID, c.Detail)
	}
	b.WriteString("\nresume with AcceptNewSemantics to re-run affected steps, or keep the original definition")
	return b.String()
}

func short(h string) string {
	if len(h) > 12 {
		return h[:12]
	}
	return h
}

// ErrDefinitionConflict is returned (via ConflictError) when a definition
// change cannot be handled under the selected compatibility mode.
var ErrDefinitionConflict = errors.New("engine: workflow definition conflict")

// Sleeper waits for d while honouring ctx cancellation.
type Sleeper func(ctx context.Context, d time.Duration) error

// Options configures an Engine.
type Options struct {
	Logger Logger
	// MaxConcurrency bounds how many steps run at the same time (0 = 1).
	MaxConcurrency int
	// Compatibility selects the definition-change policy.
	Compatibility CompatibilityMode
	// Sleep is used for retry backoff; defaults to a context-aware timer.
	Sleep Sleeper
}

// Engine runs workflow definitions durably.
type Engine struct {
	store *store.Store
	opt   Options
}

// New constructs an engine backed by the given store.
func New(st *store.Store, opt Options) *Engine {
	if opt.MaxConcurrency <= 0 {
		opt.MaxConcurrency = 1
	}
	if opt.Logger == nil {
		opt.Logger = log.New(log.Writer(), "[engine] ", log.LstdFlags|log.Lmsgprefix)
	}
	if opt.Sleep == nil {
		opt.Sleep = defaultSleep
	}
	return &Engine{store: st, opt: opt}
}

func defaultSleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// Run starts or resumes the durable execution of def. The idempotencyKey may
// be empty; when supplied, repeated submissions resolve to the same execution.
// On context cancellation the persisted state is left intact and a subsequent
// Run resumes; the returned state is non-nil in that case.
func (e *Engine) Run(ctx context.Context, executionID, idempotencyKey string, def *workflow.Definition) (*workflow.ExecutionState, error) {
	if err := def.Validate(); err != nil {
		return nil, err
	}
	newHash, err := def.Hash()
	if err != nil {
		return nil, err
	}
	steps := make(map[string]workflow.StepDef, len(def.Steps))
	for _, s := range def.Steps {
		steps[s.ID] = retry.NormaliseStepDef(s)
	}

	// Idempotency: take the per-key lock FIRST and keep it for the whole
	// Run. Lock ordering is therefore always idem-key-lock -> execution
	// lock; the reverse order would deadlock when two submissions of the
	// same key use different execution ids.
	var keyLock store.Lock
	if idempotencyKey != "" {
		lk, kerr := e.store.AcquireKeyLock(ctx, idempotencyKey)
		if kerr != nil {
			return nil, fmt.Errorf("engine: acquire idempotency lock: %w", kerr)
		}
		keyLock = lk
		defer keyLock.Release()
	}

	effectiveID := executionID
	if idempotencyKey != "" {
		host, _ := os.Hostname()
		rec := &store.IdempotencyKey{
			Key:         idempotencyKey,
			ExecutionID: executionID,
			PID:         os.Getpid(),
			Host:        host,
			CreatedAt:   time.Now().UTC().Format(time.RFC3339Nano),
		}
		switch err := e.store.SaveIdempotencyLocked(rec); {
		case err == nil:
			// claimed by this submission
		case errors.Is(err, store.ErrAlreadyExists):
			existing, lerr := e.store.LoadIdempotency(idempotencyKey)
			if lerr != nil {
				return nil, fmt.Errorf("engine: resolve idempotency key: %w", lerr)
			}
			if existing.ExecutionID != effectiveID {
				e.logf("idempotency key %q already bound to execution %q; adopting it", idempotencyKey, existing.ExecutionID)
				effectiveID = existing.ExecutionID
			}
		default:
			return nil, fmt.Errorf("engine: claim idempotency key: %w", err)
		}
	}

	release, err := e.acquireWith(ctx, effectiveID)
	if err != nil {
		return nil, err
	}
	defer release()

	st, created, err := e.loadOrCreate(effectiveID, def, newHash)
	if err != nil {
		return nil, err
	}

	if !created && st.DefinitionHash != newHash {
		if st.Definition == nil {
			return st, &ConflictError{ExecutionID: effectiveID, OldHash: st.DefinitionHash, NewHash: newHash,
				Changes: []compat.Change{{Kind: "unknown", Detail: "historical execution lacks a definition snapshot; refusing to reinterpret it"}}}
		}
		verdict := compat.Evaluate(st.Definition, def.Snapshot(), st.DefinitionHash, newHash, st.Steps)
		switch {
		case verdict.Compatible:
			e.logf("definition for execution %q changed but change is safe/additive (%d change(s)); resuming", effectiveID, len(verdict.Changes))
			e.recordEvent(st, "", "definition_change_safe", describeChanges(verdict.Changes))
			st.DefinitionHash = newHash
			st.Definition = def.Snapshot()
			st.DefinitionVer = def.Version
		case e.opt.Compatibility == AcceptNewSemantics:
			e.applyNewSemantics(st, def, newHash, verdict)
		default:
			return st, &ConflictError{ExecutionID: effectiveID, OldHash: st.DefinitionHash, NewHash: newHash, Changes: verdict.Changes}
		}
	}

	e.recoverInterrupted(st)
	if err := e.store.Save(st); err != nil {
		return st, err
	}
	if err := e.schedule(ctx, st, steps); err != nil {
		return st, err
	}
	return st, nil
}

func (e *Engine) loadOrCreate(id string, def *workflow.Definition, newHash string) (*workflow.ExecutionState, bool, error) {
	st, err := e.store.Load(id)
	if errors.Is(err, workflow.ErrNotFound) {
		now := time.Now().UTC()
		st = &workflow.ExecutionState{
			ID:             id,
			WorkflowName:   def.Name,
			DefinitionHash: newHash,
			DefinitionVer:  def.Version,
			Definition:     def.Snapshot(),
			Status:         workflow.ExecRunning,
			Steps:          map[string]*workflow.StepState{},
			CreatedAt:      now,
			UpdatedAt:      now,
		}
		for _, s := range def.Steps {
			st.Steps[s.ID] = &workflow.StepState{ID: s.ID, Status: workflow.StepPending}
		}
		if err := e.store.Save(st); err != nil {
			return nil, false, err
		}
		e.recordEvent(st, "", "execution_created", fmt.Sprintf("workflow=%s version=%d definition=%s steps=%d", def.Name, def.Version, short(newHash), len(def.Steps)))
		if err := e.store.Save(st); err != nil {
			return nil, false, err
		}
		return st, true, nil
	}
	if err != nil {
		return nil, false, err
	}
	if st.WorkflowName != def.Name {
		return nil, false, fmt.Errorf("engine: execution %q belongs to workflow %q, not %q", id, st.WorkflowName, def.Name)
	}
	for _, s := range def.Steps {
		if _, ok := st.Steps[s.ID]; !ok {
			st.Steps[s.ID] = &workflow.StepState{ID: s.ID, Status: workflow.StepPending}
		}
	}
	return st, false, nil
}

func (e *Engine) applyNewSemantics(st *workflow.ExecutionState, def *workflow.Definition, newHash string, v compat.Verdict) {
	current := map[string]bool{}
	for _, s := range def.Steps {
		current[s.ID] = true
	}
	reset := map[string]bool{}
	for _, id := range v.ReplayRequired {
		if current[id] {
			reset[id] = true
			st.Steps[id] = &workflow.StepState{ID: id, Status: workflow.StepPending}
			e.recordEvent(st, id, "step_reset", "recorded result invalidated by definition change; step will re-run under new semantics")
		} else {
			// Removed step: drop its breakpoint, it is no longer part of the DAG.
			delete(st.Steps, id)
			e.recordEvent(st, id, "step_removed", "step removed from definition; its historical breakpoint was discarded")
		}
	}
	st.MigratedFromHash = st.DefinitionHash
	st.DefinitionHash = newHash
	st.Definition = def.Snapshot()
	st.DefinitionVer = def.Version
	st.Status = workflow.ExecRunning
	e.recordEvent(st, "", "definition_migrated", fmt.Sprintf("accepted new definition %s; replaying steps: %s", short(newHash), strings.Join(sortedKeys(reset), ",")))
	e.logf("execution %q migrated to definition %s; reset steps %v", st.ID, short(newHash), sortedKeys(reset))
}

// recoverInterrupted discards attempts left "running" by a process exit. The
// attempt counter is preserved (the retry budget was genuinely consumed up
// to the crash boundary), only the unfinished attempt itself is dropped.
func (e *Engine) recoverInterrupted(st *workflow.ExecutionState) {
	for _, s := range sortedStepStates(st.Steps) {
		if s.Status != workflow.StepRunning {
			continue
		}
		oldAttempt := s.AttemptKey
		s.Status = workflow.StepPending
		s.AttemptKey = ""
		if n := len(s.History); n > 0 {
			s.History[n-1].Err = "interrupted by process exit before attempt completed"
			if s.History[n-1].EndedAt.IsZero() {
				s.History[n-1].EndedAt = time.Now().UTC()
			}
		}
		e.recordEvent(st, s.ID, "attempt_recovered", fmt.Sprintf("discarded unfinished attempt %q; attempt counter kept at %d", oldAttempt, s.Attempts))
		e.logf("recovery: step %q had unfinished attempt %q, reset to pending (attempts so far=%d)", s.ID, oldAttempt, s.Attempts)
	}
}

// --- scheduling ------------------------------------------------------------

func (e *Engine) schedule(ctx context.Context, st *workflow.ExecutionState, defs map[string]workflow.StepDef) error {
	var mu sync.Mutex      // guards st / persistence inside executeStep
	var schedMu sync.Mutex // guards inflight only, held briefly
	// sem bounds concurrently running steps.
	sem := make(chan struct{}, e.opt.MaxConcurrency)
	// finishCh has one buffered slot per step: finishing goroutines must
	// never block on the scheduler (which only wakes once a slot is free).
	finishCh := make(chan string, len(defs)+1)
	inflight := map[string]bool{}

	markDone := func(id string) {
		schedMu.Lock()
		inflight[id] = false
		schedMu.Unlock()
		finishCh <- id
	}

	launch := func(sd workflow.StepDef) {
		schedMu.Lock()
		inflight[sd.ID] = true
		schedMu.Unlock()
		go func() {
			// The semaphore is acquired INSIDE the goroutine, never under
			// mu, otherwise a full pool would deadlock against finished
			// steps that need mu to record their outcome and release a slot.
			// A queued goroutine that finds ctx cancelled never enters the
			// pool; it parks its step pending (idempotently).
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				e.parkCancelled(sd.ID, st, &mu)
				markDone(sd.ID)
				return
			}
			e.executeStep(ctx, st, &mu, sd)
			<-sem
			markDone(sd.ID)
		}()
	}

	for {
		mu.Lock()
		var toSkip, toRun []workflow.StepDef
		for _, sd := range defsSorted(defs) {
			ss := st.Steps[sd.ID]
			schedMu.Lock()
			running := inflight[sd.ID]
			schedMu.Unlock()
			if ss == nil || ss.Status != workflow.StepPending || running {
				continue
			}
			switch depStatus(st, sd) {
			case depBlockedFailed:
				toSkip = append(toSkip, sd)
			case depReady:
				toRun = append(toRun, sd)
			}
		}
		for _, sd := range toSkip {
			st.Steps[sd.ID].Status = workflow.StepSkipped
			st.Steps[sd.ID].Err = "skipped because an upstream step failed"
			st.Steps[sd.ID].EndedAt = time.Now().UTC()
			e.recordEvent(st, sd.ID, "step_skipped", "upstream failure; dependencies did not all succeed")
			e.logf("step %q skipped (upstream failure)", sd.ID)
		}
		saveErr := e.store.Save(st)
		mu.Unlock()
		if saveErr != nil {
			return saveErr
		}
		// Launch outside stateMu: goroutines must be able to start running
		// (and to take mu inside their first attempt) immediately.
		for _, sd := range toRun {
			launch(sd)
		}
		schedMu.Lock()
		active := 0
		for _, r := range inflight {
			if r {
				active++
			}
		}
		schedMu.Unlock()

		if active == 0 {
			// Nothing in flight. With no skip/launch either the graph is
			// settled; skips may unlock another wave so keep looping then.
			if len(toSkip) == 0 && len(toRun) == 0 {
				break
			}
			continue
		}

		// Wait for at least one completion (launches beyond the concurrency
		// pool may still be queued on sem; only finished goroutines signal),
		// or cancellation.
		select {
		case <-finishCh:
		case <-ctx.Done():
			// Drain every goroutine launched in this and prior waves; queued
			// ones observe ctx and park pending immediately.
			for range active {
				<-finishCh
			}
			mu.Lock()
			settled := true
			for _, s := range st.Steps {
				if s.Status == workflow.StepPending || s.Status == workflow.StepRunning {
					settled = false
				}
			}
			_ = e.store.Save(st)
			mu.Unlock()
			if !settled {
				return ctx.Err()
			}
		}
	}

	final := workflow.ExecSucceeded
	for _, sd := range defsSorted(defs) {
		switch st.Steps[sd.ID].Status {
		case workflow.StepFailed, workflow.StepPending, workflow.StepRunning:
			final = workflow.ExecFailed
		}
	}
	st.Status = final
	e.recordEvent(st, "", "execution_finished", "final status: "+final)
	if err := e.store.Save(st); err != nil {
		return err
	}
	e.logf("execution %q finished: %s", st.ID, final)
	return nil
}

// parkCancelled records that a queued step never started because the run was
// cancelled. The step stays pending; only running steps lose their attempt.
func (e *Engine) parkCancelled(id string, st *workflow.ExecutionState, mu *sync.Mutex) {
	mu.Lock()
	defer mu.Unlock()
	if ss := st.Steps[id]; ss != nil && ss.Status == workflow.StepPending {
		e.recordEvent(st, id, "launch_cancelled", "execution cancelled before the step acquired a concurrency slot")
		_ = e.store.Save(st)
	}
}

type depVerdict int

const (
	depWaiting depVerdict = iota
	depReady
	depBlockedFailed
)

func depStatus(st *workflow.ExecutionState, sd workflow.StepDef) depVerdict {
	allDone := true
	for _, dep := range sd.DependsOn {
		s, ok := st.Steps[dep]
		if !ok {
			return depBlockedFailed
		}
		switch s.Status {
		case workflow.StepFailed, workflow.StepSkipped:
			return depBlockedFailed
		case workflow.StepSucceeded:
		default:
			allDone = false
		}
	}
	if allDone {
		return depReady
	}
	return depWaiting
}

// executeStep runs all attempts of one step. State mutations and persistence
// happen under mu; the user function and the backoff sleep run without it so
// concurrent steps never block each other.
func (e *Engine) executeStep(ctx context.Context, st *workflow.ExecutionState, mu *sync.Mutex, sd workflow.StepDef) {
	for {
		mu.Lock()
		if st.Steps[sd.ID].Status != workflow.StepPending {
			mu.Unlock()
			return
		}
		ss := st.Steps[sd.ID]
		ss.Attempts++
		attempt := ss.Attempts
		attemptKey := fmt.Sprintf("%s:%s:a%d", st.ID, sd.ID, attempt)
		ss.AttemptKey = attemptKey
		ss.Status = workflow.StepRunning
		now := time.Now().UTC()
		ss.StartedAt = now
		ss.History = append(ss.History, workflow.AttemptRecord{Number: attempt, StartedAt: now})
		in := make(map[string]any, len(sd.DependsOn))
		for _, dep := range sd.DependsOn {
			in[dep] = st.Steps[dep].Output
		}
		e.recordEvent(st, sd.ID, "attempt_started", fmt.Sprintf("attempt=%d key=%s inputs=%v", attempt, attemptKey, in))
		e.logf("step %q attempt %d starting (key=%s inputs=%v)", sd.ID, attempt, attemptKey, in)
		if err := e.store.Save(st); err != nil {
			mu.Unlock()
			e.logf("step %q persist-before-attempt failed: %v", sd.ID, err)
			return
		}
		mu.Unlock()

		out, callErr := sd.Run(ctx, workflow.StepInput{
			ExecutionID: st.ID,
			StepID:      sd.ID,
			Attempt:     attempt,
			AttemptKey:  attemptKey,
			Inputs:      in,
		})

		mu.Lock()
		cur := st.Steps[sd.ID]
		cur.History[len(cur.History)-1].EndedAt = time.Now().UTC()
		switch {
		case callErr != nil && ctx.Err() != nil:
			// Cancellation mid-attempt: do not burn the failure as a real
			// failure; park the step pending for a later resume.
			cur.History[len(cur.History)-1].Err = "interrupted: " + callErr.Error()
			cur.Err = callErr.Error()
			cur.Status = workflow.StepPending
			cur.AttemptKey = ""
			e.recordEvent(st, sd.ID, "attempt_interrupted", fmt.Sprintf("attempt=%d err=%q decision=park_pending_for_resume", attempt, callErr.Error()))
			e.logf("step %q attempt %d interrupted by cancellation: %v", sd.ID, attempt, callErr)
			_ = e.store.Save(st)
			mu.Unlock()
			return
		case callErr != nil:
			cur.History[len(cur.History)-1].Err = callErr.Error()
			cur.Err = callErr.Error()
			maxAttempts := sd.Retry.MaxAttempts
			if attempt < maxAttempts {
				wait := retry.Delay(sd.Retry, attempt+1)
				cur.Status = workflow.StepPending
				cur.AttemptKey = ""
				e.recordEvent(st, sd.ID, "attempt_failed_retry", fmt.Sprintf("attempt=%d/%d err=%q retry_after=%s decision=retry", attempt, maxAttempts, callErr.Error(), wait))
				e.logf("step %q attempt %d/%d failed: %v; retry after %s (decision=retry)", sd.ID, attempt, maxAttempts, callErr, wait)
				if err := e.store.Save(st); err != nil {
					mu.Unlock()
					e.logf("step %q persist-after-failure failed: %v", sd.ID, err)
					return
				}
				mu.Unlock()
				if werr := e.opt.Sleep(ctx, wait); werr != nil {
					return // parked pending; counter preserved
				}
				continue
			}
			cur.Status = workflow.StepFailed
			cur.AttemptKey = ""
			cur.EndedAt = time.Now().UTC()
			e.recordEvent(st, sd.ID, "step_failed", fmt.Sprintf("attempts=%d last_err=%q decision=fail_exhausted", attempt, callErr.Error()))
			e.logf("step %q failed permanently after %d attempt(s): %v (decision=fail_exhausted)", sd.ID, attempt, callErr)
			_ = e.store.Save(st)
			mu.Unlock()
			return
		default:
			cur.Status = workflow.StepSucceeded
			cur.Output = out
			cur.Err = ""
			cur.AttemptKey = ""
			cur.EndedAt = time.Now().UTC()
			e.recordEvent(st, sd.ID, "step_succeeded", fmt.Sprintf("attempts=%d decision=success output=%v", attempt, out))
			e.logf("step %q succeeded on attempt %d (output=%v decision=success)", sd.ID, attempt, out)
			_ = e.store.Save(st)
			mu.Unlock()
			return
		}
	}
}

// --- small helpers ---------------------------------------------------------

func (e *Engine) acquireWith(ctx context.Context, id string) (func(), error) {
	lk, err := e.store.Acquire(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("engine: acquire execution lock: %w", err)
	}
	return func() { _ = lk.Release() }, nil
}

func (e *Engine) recordEvent(st *workflow.ExecutionState, stepID, kind, detail string) {
	st.Events = append(st.Events, workflow.EventRecord{Time: time.Now().UTC(), StepID: stepID, Kind: kind, Detail: detail})
}

func (e *Engine) logf(format string, args ...any) { e.opt.Logger.Printf(format, args...) }

func describeChanges(cs []compat.Change) string {
	parts := make([]string, 0, len(cs))
	for _, c := range cs {
		parts = append(parts, string(c.Kind)+":"+c.StepID)
	}
	return strings.Join(parts, ",")
}

func defsSorted(defs map[string]workflow.StepDef) []workflow.StepDef {
	out := make([]workflow.StepDef, 0, len(defs))
	for _, sd := range defs {
		out = append(out, sd)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func sortedStepStates(m map[string]*workflow.StepState) []*workflow.StepState {
	out := make([]*workflow.StepState, 0, len(m))
	for _, s := range m {
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
