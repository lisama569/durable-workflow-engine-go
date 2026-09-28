// Package workflow contains the declarative definition of a workflow and the
// serialisable execution state that is persisted between runs.
package workflow

import (
	"context"
	"errors"
	"time"
)

// StepFunc is the unit of work executed by the engine. Inputs of upstream
// steps are delivered via StepInput.Inputs. A step function MUST be
// idempotent with respect to AttemptKey: if the same AttemptKey is observed
// again after a crash, performing the side effect twice (or returning the
// previously produced result) must yield the same external effect.
type StepFunc func(ctx context.Context, in StepInput) (any, error)

// StepInput carries everything a step needs for one attempt.
type StepInput struct {
	ExecutionID string
	StepID      string
	Attempt     int
	AttemptKey  string
	Inputs      map[string]any
}

// RetryPolicy configures how a failing step is retried.
type RetryPolicy struct {
	MaxAttempts int           // total attempts including the first one; <=1 means no retry
	BaseDelay   time.Duration // delay before the 2nd attempt
	MaxDelay    time.Duration // cap for exponential backoff
	Multiplier  float64       // growth factor; <1 falls back to 2
}

// StepDef declares one step of a workflow.
type StepDef struct {
	ID        string
	DependsOn []string
	Retry     RetryPolicy
	Run       StepFunc
	// Metadata is an opaque part of the definition hash so that changing a
	// step body (which the engine cannot inspect) can still be expressed
	// explicitly by bumping Version or Metadata.
	Version  int
	Metadata map[string]string
}

// Definition is the complete, immutable declaration of a workflow.
type Definition struct {
	Name    string
	Version int
	Steps   []StepDef
}

// Step statuses.
const (
	StepPending   = "pending"
	StepRunning   = "running"
	StepSucceeded = "succeeded"
	StepFailed    = "failed"
	StepSkipped   = "skipped" // downstream of a failed step
)

// Execution statuses.
const (
	ExecRunning   = "running"
	ExecSucceeded = "succeeded"
	ExecFailed    = "failed"
)

// AttemptRecord describes one execution attempt of a step.
type AttemptRecord struct {
	Number    int       `json:"number"`
	StartedAt time.Time `json:"started_at"`
	EndedAt   time.Time `json:"ended_at,omitempty"`
	Err       string    `json:"err,omitempty"`
}

// StepState is the persisted state of one step.
type StepState struct {
	ID         string          `json:"id"`
	Status     string          `json:"status"`
	Attempts   int             `json:"attempts"`
	AttemptKey string          `json:"attempt_key,omitempty"`
	Output     any             `json:"output,omitempty"`
	Err        string          `json:"err,omitempty"`
	History    []AttemptRecord `json:"history,omitempty"`
	StartedAt  time.Time       `json:"started_at,omitempty"`
	EndedAt    time.Time       `json:"ended_at,omitempty"`
}

// StepDefSnapshot is the persisted, function-free view of one step. It lets
// the engine explain definition diffs against a historical execution without
// the original Go code being available.
type StepDefSnapshot struct {
	ID        string            `json:"id"`
	DependsOn []string          `json:"depends_on"`
	Version   int               `json:"version"`
	Metadata  map[string]string `json:"metadata,omitempty"`
	Retry     RetryPolicy       `json:"retry"`
}

// DefinitionSnapshot is the persisted view of a whole definition.
type DefinitionSnapshot struct {
	Name    string            `json:"name"`
	Version int               `json:"version"`
	Steps   []StepDefSnapshot `json:"steps"`
}

// Snapshot returns the persistable view of the definition.
func (d *Definition) Snapshot() *DefinitionSnapshot {
	s := &DefinitionSnapshot{Name: d.Name, Version: d.Version, Steps: make([]StepDefSnapshot, 0, len(d.Steps))}
	for _, st := range d.Steps {
		s.Steps = append(s.Steps, StepDefSnapshot{
			ID:        st.ID,
			DependsOn: append([]string(nil), st.DependsOn...),
			Version:   st.Version,
			Metadata:  st.Metadata,
			Retry:     st.Retry,
		})
	}
	return s
}

// ExecutionState is the persisted state of one whole execution.
type ExecutionState struct {
	ID             string                `json:"id"`
	WorkflowName   string                `json:"workflow_name"`
	DefinitionHash string                `json:"definition_hash"`
	DefinitionVer  int                   `json:"definition_version"`
	Status         string                `json:"status"`
	Steps          map[string]*StepState `json:"steps"`
	Definition     *DefinitionSnapshot   `json:"definition,omitempty"`
	CreatedAt      time.Time             `json:"created_at"`
	UpdatedAt      time.Time             `json:"updated_at"`
	// MigratedFromHash is set when a resume accepted a newer definition
	// (AcceptNewSemantics mode).
	MigratedFromHash string        `json:"migrated_from_hash,omitempty"`
	Events           []EventRecord `json:"events,omitempty"`
}

// EventRecord is an append-only, human-explainable decision log entry.
type EventRecord struct {
	Time   time.Time `json:"time"`
	StepID string    `json:"step_id,omitempty"`
	Kind   string    `json:"kind"`
	Detail string    `json:"detail"`
}

// ErrNotFound is returned when a persisted execution does not exist.
var ErrNotFound = errors.New("workflow: execution not found")
