package workflow

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
)

// Validate checks structural invariants of the definition: unique step ids,
// existing dependencies, no self dependency and an acyclic dependency graph.
func (d *Definition) Validate() error {
	if d == nil {
		return fmt.Errorf("workflow: nil definition")
	}
	if d.Name == "" {
		return fmt.Errorf("workflow: definition name is empty")
	}
	if len(d.Steps) == 0 {
		return fmt.Errorf("workflow: definition %q has no steps", d.Name)
	}
	seen := make(map[string]struct{}, len(d.Steps))
	for _, st := range d.Steps {
		if st.ID == "" {
			return fmt.Errorf("workflow: definition %q contains a step with an empty id", d.Name)
		}
		if _, dup := seen[st.ID]; dup {
			return fmt.Errorf("workflow: definition %q contains duplicate step id %q", d.Name, st.ID)
		}
		if st.Run == nil {
			return fmt.Errorf("workflow: step %q has no Run function", st.ID)
		}
		seen[st.ID] = struct{}{}
	}
	depSet := make(map[string]map[string]struct{}, len(d.Steps))
	for _, st := range d.Steps {
		deps := make(map[string]struct{}, len(st.DependsOn))
		for _, dep := range st.DependsOn {
			if dep == st.ID {
				return fmt.Errorf("workflow: step %q depends on itself", st.ID)
			}
			if _, ok := seen[dep]; !ok {
				return fmt.Errorf("workflow: step %q depends on unknown step %q", st.ID, dep)
			}
			if _, dup := deps[dep]; dup {
				return fmt.Errorf("workflow: step %q lists dependency %q more than once", st.ID, dep)
			}
			deps[dep] = struct{}{}
		}
		depSet[st.ID] = deps
	}
	// Cycle detection with iterative DFS (0=unvisited,1=on-stack,2=done).
	color := make(map[string]int, len(d.Steps))
	var visit func(id string, path []string) error
	visit = func(id string, path []string) error {
		switch color[id] {
		case 1:
			return fmt.Errorf("workflow: dependency cycle detected: %v", append(path, id))
		case 2:
			return nil
		}
		color[id] = 1
		for dep := range depSet[id] {
			if err := visit(dep, append(path, id)); err != nil {
				return err
			}
		}
		color[id] = 2
		return nil
	}
	for _, st := range d.Steps {
		if err := visit(st.ID, nil); err != nil {
			return err
		}
	}
	return nil
}

// stableStep is the JSON-serialisable, order-independent view used for hashing.
// Step functions are opaque values in Go and therefore cannot be hashed;
// StepDef.Version / StepDef.Metadata let the author declare that a body
// changed.
type stableStep struct {
	ID        string            `json:"id"`
	DependsOn []string          `json:"depends_on"`
	Version   int               `json:"version"`
	Metadata  map[string]string `json:"metadata,omitempty"`
	Retry     retryHash         `json:"retry"`
}

type retryHash struct {
	MaxAttempts int     `json:"max_attempts"`
	BaseDelayNs int64   `json:"base_delay_ns"`
	MaxDelayNs  int64   `json:"max_delay_ns"`
	Multiplier  float64 `json:"multiplier"`
}

type stableDefinition struct {
	Name  string       `json:"name"`
	Steps []stableStep `json:"steps"`
}

// Hash returns a stable hash of the structural and semantic parts of the
// definition. It is independent of step declaration order.
func (d *Definition) Hash() (string, error) {
	if err := d.Validate(); err != nil {
		return "", err
	}
	stable := stableDefinition{Name: d.Name, Steps: make([]stableStep, 0, len(d.Steps))}
	for _, st := range d.Steps {
		deps := append([]string(nil), st.DependsOn...)
		sort.Strings(deps)
		ss := stableStep{
			ID:        st.ID,
			DependsOn: deps,
			Version:   st.Version,
			Metadata:  st.Metadata,
		}
		if st.Metadata != nil {
			ss.Metadata = make(map[string]string, len(st.Metadata))
			for k, v := range st.Metadata {
				ss.Metadata[k] = v
			}
		}
		ss.Retry = retryHash{
			MaxAttempts: st.Retry.MaxAttempts,
			BaseDelayNs: st.Retry.BaseDelay.Nanoseconds(),
			MaxDelayNs:  st.Retry.MaxDelay.Nanoseconds(),
			Multiplier:  st.Retry.Multiplier,
		}
		stable.Steps = append(stable.Steps, ss)
	}
	sort.Slice(stable.Steps, func(i, j int) bool { return stable.Steps[i].ID < stable.Steps[j].ID })
	raw, err := json.Marshal(stable)
	if err != nil {
		return "", fmt.Errorf("workflow: hash definition: %w", err)
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

func marshalHash(v any) string {
	raw, err := json.Marshal(v)
	if err != nil {
		// All inputs here are plain maps/slices/strings, so this cannot fail.
		return ""
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func toRetryHash(s StepDef) retryHash {
	return retryHash{
		MaxAttempts: s.Retry.MaxAttempts,
		BaseDelayNs: s.Retry.BaseDelay.Nanoseconds(),
		MaxDelayNs:  s.Retry.MaxDelay.Nanoseconds(),
		Multiplier:  s.Retry.Multiplier,
	}
}

func sortedCopy(in []string) []string {
	out := append([]string(nil), in...)
	sort.Strings(out)
	return out
}

// FingerprintStepDeps fingerprints only the dependency edges of one step.
func FingerprintStepDeps(s StepDef) string {
	return marshalHash(sortedCopy(s.DependsOn))
}

// FingerprintStepSemantics fingerprints the version, metadata and retry
// policy of one step (everything except dependency edges).
func FingerprintStepSemantics(s StepDef) string {
	return marshalHash(struct {
		Version  int               `json:"version"`
		Metadata map[string]string `json:"metadata,omitempty"`
		Retry    retryHash         `json:"retry"`
	}{s.Version, s.Metadata, toRetryHash(s)})
}

// FingerprintStep fingerprints dependency edges and semantics of one step.
func FingerprintStep(s StepDef) string {
	return marshalHash(struct {
		Deps     string            `json:"deps"`
		Version  int               `json:"version"`
		Metadata map[string]string `json:"metadata,omitempty"`
		Retry    retryHash         `json:"retry"`
	}{FingerprintStepDeps(s), s.Version, s.Metadata, toRetryHash(s)})
}
