package workflow

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// scheduler contains the per-run DAG scheduling loop. It is intentionally
// unexported: callers only see Engine.Submit. One scheduler exists per
// Submit; the engine guarantees only one scheduler drives a given run at a
// time within the process (and FileStore's flock serializes writers across
// processes).
type scheduler struct {
	engine  *Engine
	def     *Definition
	state   *RunState
	nextSeq int
	// resumed is true when continuing an existing non-terminal run; the
	// original RUN_STARTED event already exists in its log.
	resumed bool

	mu sync.Mutex // guards state mutations while steps run concurrently
	wg sync.WaitGroup
}

// run executes pending steps of s.state until the run reaches a terminal
// state or ctx is cancelled. It never re-executes a SUCCEEDED step.
func (s *scheduler) run(ctx context.Context) error {
	now := s.engine.opts.Now()
	s.state.Status = RunRunning
	s.state.UpdatedAt = now
	if err := s.engine.store.SaveState(ctx, s.state); err != nil {
		return err
	}
	if !s.resumed {
		if err := s.append(ctx, Event{Kind: EventRunStarted}); err != nil {
			return err
		}
	}

	// Crash recovery: an attempt marked RUNNING was in flight when the
	// previous process died. Its side effects are unknown, so it goes back
	// to PENDING and will be retried (at-least-once). Completed steps are
	// untouched and never run again.
	s.recoverInterrupted(ctx)

	sem := make(chan struct{}, s.engine.opts.Concurrency)

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		s.mu.Lock()
		// 1. Propagate failures to steps that can no longer run.
		s.markSkipped(ctx)
		// 2. Find steps whose dependencies are now all terminal.
		ready := s.readySteps()
		s.mu.Unlock()

		if len(ready) == 0 {
			break // no progress possible this wave
		}

		for _, id := range ready {
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				s.wg.Wait()
				return ctx.Err()
			}
			s.wg.Add(1)
			go func(id StepID) {
				defer s.wg.Done()
				defer func() { <-sem }()
				s.executeStep(ctx, id)
			}(id)
		}
		s.wg.Wait()
	}

	return s.finalize(ctx)
}

// recoverInterrupted resets steps left RUNNING by a crashed process.
func (s *scheduler) recoverInterrupted(ctx context.Context) {
	var recovered []StepID
	for id, st := range s.state.Steps {
		if st.Status == StepRunning {
			st.Status = StepPending
			// The interrupted attempt had an unknown outcome, so it is not
			// charged to the retry budget: reset the counter as well as the
			// status. It is still visible in the event log.
			st.Attempts = 0
			st.StartedAt = nil
			recovered = append(recovered, id)
		}
	}
	if len(recovered) == 0 {
		return
	}
	sort.Slice(recovered, func(i, j int) bool { return recovered[i] < recovered[j] })
	now := s.engine.opts.Now()
	s.state.UpdatedAt = now
	if err := s.engine.store.SaveState(ctx, s.state); err != nil {
		// Persistence failure surfaces on the next write; the in-memory
		// recovery still holds for this process.
		return
	}
	_ = s.append(ctx, Event{
		Kind:   EventStepRetry,
		Detail: fmt.Sprintf("recovered interrupted run; steps reset to PENDING: %s", joinIDs(recovered)),
	})
}

// markSkipped marks PENDING steps SKIPPED when a dependency is FAILED or
// SKIPPED and the step requires successful dependencies. Must be called
// with s.mu held; it persists one event per newly skipped step.
func (s *scheduler) markSkipped(ctx context.Context) {
	for {
		progress := false
		for _, sd := range s.def.Steps {
			st := s.state.Steps[sd.ID]
			if st.Status != StepPending {
				continue
			}
			if sd.OnDependencyFailure == AllowDependencyFailure {
				continue
			}
			for _, dep := range sd.Depends {
				depState := s.state.Steps[dep]
				if depState.Status == StepFailed || depState.Status == StepSkipped {
					st.Status = StepSkipped
					finished := s.engine.opts.Now()
					st.FinishedAt = &finished
					st.LastEventAt = finished
					progress = true
					_ = s.appendLocked(ctx, Event{
						Kind:   EventStepSkipped,
						StepID: sd.ID,
						Detail: fmt.Sprintf("dependency %s ended %s; step never executes", dep, depState.Status),
					})
					break
				}
			}
		}
		if !progress {
			return
		}
	}
}

// readySteps returns PENDING steps that may start now. Must be called with
// s.mu held. Returned order is deterministic (definition order).
func (s *scheduler) readySteps() []StepID {
	var ready []StepID
	for _, sd := range s.def.Steps {
		st := s.state.Steps[sd.ID]
		if st.Status != StepPending {
			continue
		}
		allTerminal := true
		allSucceeded := true
		for _, dep := range sd.Depends {
			depState := s.state.Steps[dep]
			if !depState.Status.terminal() {
				allTerminal = false
			}
			if depState.Status != StepSucceeded {
				allSucceeded = false
			}
		}
		if !allTerminal {
			continue
		}
		if sd.OnDependencyFailure == AllowDependencyFailure || allSucceeded {
			ready = append(ready, sd.ID)
		}
	}
	return ready
}

// executeStep runs one step, including its retry loop, and persists every
// state transition before any side effect becomes observable as committed.
func (s *scheduler) executeStep(ctx context.Context, id StepID) {
	sd, ok := s.def.Step(id)
	if !ok {
		return
	}

	inputs, depStatus := s.buildInputs(sd)

	s.mu.Lock()
	st := s.state.Steps[id]
	st.Status = StepRunning
	st.Attempts++
	started := s.engine.opts.Now()
	st.StartedAt = &started
	st.LastEventAt = started
	if err := s.engine.store.SaveState(ctx, s.state); err != nil {
		s.mu.Unlock()
		return
	}
	_ = s.appendLocked(ctx, Event{
		Kind:    EventStepStarted,
		StepID:  id,
		Attempt: st.Attempts,
		Detail: fmt.Sprintf("input=%q upstream_inputs={%s}; verdict=dependencies satisfied, attempting execution",
			sd.Input, formatInputs(inputs)),
	})
	maxAttempts := sd.Retry.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 1
	}
	s.mu.Unlock()

	for {
		if err := ctx.Err(); err != nil {
			return // leave RUNNING; resume will recover it
		}
		req := StepRequest{
			RunID:            s.state.ID,
			Step:             sd,
			Attempt:          st.Attempts,
			Inputs:           inputs,
			DependencyStatus: depStatus,
		}
		res, execErr := s.engine.executor.Execute(ctx, req)

		s.mu.Lock()
		if execErr == nil {
			now := s.engine.opts.Now()
			st.Status = StepSucceeded
			st.Output = res.Output
			st.LastError = ""
			st.FinishedAt = &now
			st.LastEventAt = now
			_ = s.engine.store.SaveState(ctx, s.state)
			_ = s.appendLocked(ctx, Event{
				Kind:    EventStepSucceeded,
				StepID:  id,
				Attempt: st.Attempts,
				Detail:  fmt.Sprintf("output=%q; verdict=success after %d attempt(s)", res.Output, st.Attempts),
			})
			s.mu.Unlock()
			return
		}

		now := s.engine.opts.Now()
		st.LastError = execErr.Error()
		st.LastEventAt = now
		if st.Attempts >= maxAttempts {
			st.Status = StepFailed
			st.FinishedAt = &now
			_ = s.engine.store.SaveState(ctx, s.state)
			_ = s.appendLocked(ctx, Event{
				Kind:    EventStepFailed,
				StepID:  id,
				Attempt: st.Attempts,
				Detail: fmt.Sprintf("error=%q; verdict=retries exhausted (max_attempts=%d)",
					execErr.Error(), maxAttempts),
			})
			s.mu.Unlock()
			return
		}

		backoff := nextBackoff(sd.Retry, st.Attempts)
		_ = s.engine.store.SaveState(ctx, s.state)
		_ = s.appendLocked(ctx, Event{
			Kind:    EventStepRetry,
			StepID:  id,
			Attempt: st.Attempts,
			Detail: fmt.Sprintf("error=%q; verdict=retry %d/%d after %s backoff",
				execErr.Error(), st.Attempts+1, maxAttempts, backoff),
		})
		st.Attempts++
		st.LastEventAt = s.engine.opts.Now()
		_ = s.engine.store.SaveState(ctx, s.state)
		s.mu.Unlock()

		if s.engine.opts.Sleep != nil {
			if err := s.engine.opts.Sleep(ctx, backoff); err != nil {
				return
			}
		} else if backoff > 0 {
			t := time.NewTimer(backoff)
			select {
			case <-t.C:
			case <-ctx.Done():
				t.Stop()
				return
			}
		}
	}
}

// buildInputs snapshots succeeded dependencies' outputs for a step. The
// snapshot is taken once before the step's attempts so retries and
// concurrent branches cannot observe each other's mutating state.
func (s *scheduler) buildInputs(sd *StepDef) (map[StepID]string, map[StepID]StepStatus) {
	s.mu.Lock()
	defer s.mu.Unlock()
	inputs := make(map[StepID]string, len(sd.Depends))
	statuses := make(map[StepID]StepStatus, len(sd.Depends))
	for _, dep := range sd.Depends {
		depState := s.state.Steps[dep]
		statuses[dep] = depState.Status
		if depState.Status == StepSucceeded {
			inputs[dep] = depState.Output
		}
	}
	return inputs, statuses
}

// finalize writes the run-level terminal status.
func (s *scheduler) finalize(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	allDone := true
	anyFailure := false
	for _, sd := range s.def.Steps {
		switch s.state.Steps[sd.ID].Status {
		case StepSucceeded:
		case StepFailed, StepSkipped:
			anyFailure = true
		default:
			allDone = false
		}
	}
	if !allDone {
		// Context cancellation interrupted scheduling mid-flight.
		return ctx.Err()
	}
	now := s.engine.opts.Now()
	s.state.UpdatedAt = now
	if anyFailure {
		s.state.Status = RunFailed
		if err := s.engine.store.SaveState(ctx, s.state); err != nil {
			return err
		}
		return s.appendLocked(ctx, Event{
			Kind:   EventRunFailed,
			Detail: "verdict=at least one step failed or was skipped; downstream steps not executed",
		})
	}
	s.state.Status = RunSucceeded
	if err := s.engine.store.SaveState(ctx, s.state); err != nil {
		return err
	}
	return s.appendLocked(ctx, Event{
		Kind:   EventRunSucceeded,
		Detail: "verdict=all steps succeeded",
	})
}

// append fills seq/time and writes an event without touching s.mu state.
func (s *scheduler) append(ctx context.Context, ev Event) error {
	return s.appendLocked(ctx, ev)
}

// appendLocked is the single event writer. Caller holds (or may hold) s.mu.
func (s *scheduler) appendLocked(ctx context.Context, ev Event) error {
	ev.Seq = s.nextSeq
	s.nextSeq++
	if ev.At.IsZero() {
		ev.At = s.engine.opts.Now()
	}
	s.state.UpdatedAt = ev.At
	if err := s.engine.store.AppendEvent(ctx, s.state.ID, ev); err != nil {
		return err
	}
	s.engine.opts.Logger.Log(ctx, s.state.ID, ev)
	return nil
}

// nextBackoff computes the delay before the attempt after `attemptsDone`
// attempts: initial * multiplier^(attemptsDone-1), capped at MaxBackoff.
func nextBackoff(p RetryPolicy, attemptsDone int) time.Duration {
	if p.InitialBackoff <= 0 {
		return 0
	}
	d := float64(p.InitialBackoff)
	mult := p.Multiplier
	if mult <= 1 {
		mult = 1
	}
	for i := 1; i < attemptsDone; i++ {
		d *= mult
	}
	if p.MaxBackoff > 0 && d > float64(p.MaxBackoff) {
		d = float64(p.MaxBackoff)
	}
	return time.Duration(d)
}

func (s *StepStatus) terminal() bool {
	return *s == StepSucceeded || *s == StepFailed || *s == StepSkipped
}

func joinIDs(ids []StepID) string {
	parts := make([]string, len(ids))
	for i, id := range ids {
		parts[i] = string(id)
	}
	return strings.Join(parts, ",")
}

func formatInputs(inputs map[StepID]string) string {
	keys := make([]string, 0, len(inputs))
	for k := range inputs {
		keys = append(keys, string(k))
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=%q", k, inputs[StepID(k)]))
	}
	return strings.Join(parts, ",")
}
