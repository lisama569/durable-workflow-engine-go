package workflow

import (
	"context"
	"fmt"
	"sync"
)

// StepRequest is handed to a StepExecutor for one attempt.
//
// Inputs carries the outputs of succeeded dependencies keyed by step id.
// Failed or skipped dependencies are absent; the step can inspect
// DependencyStatus when it opted into AllowDependencyFailure.
type StepRequest struct {
	RunID            RunID
	Step             *StepDef
	Attempt          int
	Inputs           map[StepID]string
	DependencyStatus map[StepID]StepStatus
}

// StepResult is the outcome of one attempt.
type StepResult struct {
	Output string
}

// StepExecutor runs user code for a single step attempt. Implementations must
// be safe for concurrent use because independent steps run in parallel.
type StepExecutor interface {
	Execute(ctx context.Context, req StepRequest) (StepResult, error)
}

// StepExecutorFunc adapts a function to a StepExecutor.
type StepExecutorFunc func(ctx context.Context, req StepRequest) (StepResult, error)

// Execute implements StepExecutor.
func (f StepExecutorFunc) Execute(ctx context.Context, req StepRequest) (StepResult, error) {
	return f(ctx, req)
}

// Registry maps workflow names to their definitions.
type Registry struct {
	mu   sync.RWMutex
	defs map[string]*Definition
}

// NewRegistry creates an empty registry.
func NewRegistry() *Registry {
	return &Registry{defs: make(map[string]*Definition)}
}

// Register validates and stores a definition.
//
// Registering the same name with an identical fingerprint is a harmless
// no-op. Registering the same name with a different fingerprint returns an
// error: new, incompatible definitions should be registered under a new
// name (version). Historical runs always carry the fingerprint they were
// created with, so keeping an immutable copy per name/version is what makes
// later compatibility judgments stable.
func (r *Registry) Register(def *Definition) error {
	if err := def.Validate(); err != nil {
		return err
	}
	if _, err := def.Fingerprint(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if existing, ok := r.defs[def.Name]; ok {
		oldFP, _ := existing.Fingerprint()
		newFP, _ := def.Fingerprint()
		if oldFP != newFP {
			return fmt.Errorf("workflow: cannot register changed definition under existing name %q (old %s, new %s); register a new version name", def.Name, oldFP, newFP)
		}
		return nil
	}
	r.defs[def.Name] = def
	return nil
}

// Get returns the registered definition.
func (r *Registry) Get(name string) (*Definition, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	def, ok := r.defs[name]
	return def, ok
}
