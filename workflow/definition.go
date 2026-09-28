package workflow

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
)

// Fingerprint returns a stable hash of the definition's executable semantics.
//
// Only fields that change behavior participate: step ids, dependencies,
// failure strategy, retry policy and input payload. Step and dependency
// lists are sorted (dependencies are topological-insensitive as long as the
// set is the same), so cosmetic reordering does not change the fingerprint.
func (d *Definition) Fingerprint() (string, error) {
	canon, err := canonicalize(d)
	if err != nil {
		return "", err
	}
	raw, err := json.Marshal(canon)
	if err != nil {
		return "", fmt.Errorf("workflow: marshal definition: %w", err)
	}
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

// Validate checks structural integrity: unique ids, known dependencies,
// no cycles, and sane retry policies.
func (d *Definition) Validate() error {
	if d == nil {
		return fmt.Errorf("%w: nil definition", ErrInvalidDefinition)
	}
	if d.Name == "" {
		return fmt.Errorf("%w: missing name", ErrInvalidDefinition)
	}
	if len(d.Steps) == 0 {
		return fmt.Errorf("%w: workflow %q has no steps", ErrInvalidDefinition, d.Name)
	}
	seen := make(map[StepID]struct{}, len(d.Steps))
	for _, st := range d.Steps {
		if st.ID == "" {
			return fmt.Errorf("%w: workflow %q contains a step with an empty id", ErrInvalidDefinition, d.Name)
		}
		if _, dup := seen[st.ID]; dup {
			return fmt.Errorf("%w: workflow %q has duplicate step id %q", ErrInvalidDefinition, d.Name, st.ID)
		}
		seen[st.ID] = struct{}{}
		if st.Retry.MaxAttempts < 0 {
			return fmt.Errorf("%w: step %q has negative MaxAttempts", ErrInvalidDefinition, st.ID)
		}
		if st.Retry.Multiplier < 0 {
			return fmt.Errorf("%w: step %q has negative retry multiplier", ErrInvalidDefinition, st.ID)
		}
	}
	for _, st := range d.Steps {
		for _, dep := range st.Depends {
			if _, ok := seen[dep]; !ok {
				return fmt.Errorf("%w: step %q depends on unknown step %q", ErrInvalidDefinition, st.ID, dep)
			}
			if dep == st.ID {
				return fmt.Errorf("%w: step %q depends on itself", ErrInvalidDefinition, st.ID)
			}
		}
	}
	if err := d.checkAcyclic(); err != nil {
		return err
	}
	return nil
}

// checkAcyclic performs a DFS three-color cycle check.
func (d *Definition) checkAcyclic() error {
	const (
		white = 0
		gray  = 1
		black = 2
	)
	color := make(map[StepID]int, len(d.Steps))
	var visit func(id StepID, path []StepID) error
	visit = func(id StepID, path []StepID) error {
		switch color[id] {
		case gray:
			return fmt.Errorf("%w: dependency cycle detected at step %q (path: %v)", ErrInvalidDefinition, id, path)
		case black:
			return nil
		}
		color[id] = gray
		st, _ := d.Step(id)
		for _, dep := range st.Depends {
			if err := visit(dep, append(path, dep)); err != nil {
				return err
			}
		}
		color[id] = black
		return nil
	}
	for _, st := range d.Steps {
		if err := visit(st.ID, []StepID{st.ID}); err != nil {
			return err
		}
	}
	return nil
}

// Step looks up a step definition by id.
func (d *Definition) Step(id StepID) (*StepDef, bool) {
	for i := range d.Steps {
		if d.Steps[i].ID == id {
			return &d.Steps[i], true
		}
	}
	return nil, false
}

// StepFingerprint returns a stable digest of one step definition's
// executable semantics (dependencies, failure strategy, retry policy, input).
func (d *Definition) StepFingerprint(id StepID) (string, error) {
	sd, ok := d.Step(id)
	if !ok {
		return "", fmt.Errorf("%w: unknown step %q", ErrInvalidDefinition, id)
	}
	depends := make([]string, 0, len(sd.Depends))
	for _, dep := range sd.Depends {
		depends = append(depends, string(dep))
	}
	sort.Strings(depends)
	mode := sd.OnDependencyFailure
	if mode == "" {
		mode = FailOnDependencyFailure
	}
	maxAttempts := sd.Retry.MaxAttempts
	if maxAttempts == 0 {
		maxAttempts = 1
	}
	mult := sd.Retry.Multiplier
	if mult == 0 {
		mult = 1
	}
	raw, err := json.Marshal(canonicalStepDef{
		ID:                  string(sd.ID),
		Depends:             depends,
		OnDependencyFailure: string(mode),
		Retry: canonicalRetry{
			MaxAttempts:    maxAttempts,
			InitialBackoff: sd.Retry.InitialBackoff.Nanoseconds(),
			MaxBackoff:     sd.Retry.MaxBackoff.Nanoseconds(),
			Multiplier:     mult,
		},
		Input: sd.Input,
	})
	if err != nil {
		return "", fmt.Errorf("workflow: marshal step: %w", err)
	}
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

func canonicalize(d *Definition) (canonicalDefinition, error) {
	out := canonicalDefinition{Name: d.Name, Steps: make([]canonicalStepDef, 0, len(d.Steps))}
	for _, st := range d.Steps {
		depends := make([]string, 0, len(st.Depends))
		for _, dep := range st.Depends {
			depends = append(depends, string(dep))
		}
		sort.Strings(depends)
		mode := st.OnDependencyFailure
		if mode == "" {
			mode = FailOnDependencyFailure
		}
		maxAttempts := st.Retry.MaxAttempts
		if maxAttempts == 0 {
			maxAttempts = 1
		}
		mult := st.Retry.Multiplier
		if mult == 0 {
			mult = 1
		}
		out.Steps = append(out.Steps, canonicalStepDef{
			ID:                  string(st.ID),
			Depends:             depends,
			OnDependencyFailure: string(mode),
			Retry: canonicalRetry{
				MaxAttempts:    maxAttempts,
				InitialBackoff: st.Retry.InitialBackoff.Nanoseconds(),
				MaxBackoff:     st.Retry.MaxBackoff.Nanoseconds(),
				Multiplier:     mult,
			},
			Input: st.Input,
		})
	}
	sort.Slice(out.Steps, func(i, j int) bool { return out.Steps[i].ID < out.Steps[j].ID })
	return out, nil
}

// canonicalDefinition is the JSON shape used for fingerprinting.
type canonicalDefinition struct {
	Name  string             `json:"name"`
	Steps []canonicalStepDef `json:"steps"`
}

type canonicalStepDef struct {
	ID                  string         `json:"id"`
	Depends             []string       `json:"depends"`
	OnDependencyFailure string         `json:"on_dependency_failure"`
	Retry               canonicalRetry `json:"retry"`
	Input               string         `json:"input"`
}

type canonicalRetry struct {
	MaxAttempts    int     `json:"max_attempts"`
	InitialBackoff int64   `json:"initial_backoff_ns"`
	MaxBackoff     int64   `json:"max_backoff_ns"`
	Multiplier     float64 `json:"multiplier"`
}
