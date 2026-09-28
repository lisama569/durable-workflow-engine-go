// Package workflow defines a local, durable workflow orchestration engine.
//
// A workflow is a DAG of steps. Each run is persisted to disk before and
// after every step attempt, so that:
//   - concurrent branches cannot observe each other's partial state;
//   - failed steps are retried with a configurable backoff policy;
//   - a crashed process can resume the same run without re-executing
//     completed steps;
//   - duplicate submit requests for the same RunID are detected and never
//     trigger side effects twice;
//   - changing a workflow definition never silently changes the semantics
//     of historical runs.
package workflow

import (
	"context"
	"time"
)

// RunID uniquely identifies one execution of a workflow.
type RunID string

// StepID uniquely identifies a step within a workflow definition.
type StepID string

// RunStatus is the overall status of a run.
type RunStatus string

const (
	// RunPending means the run was created but execution has not started.
	RunPending RunStatus = "PENDING"
	// RunRunning means at least one step is currently being scheduled.
	RunRunning RunStatus = "RUNNING"
	// RunSucceeded means every required step succeeded.
	RunSucceeded RunStatus = "SUCCEEDED"
	// RunFailed means a step exhausted its retries and the run cannot proceed.
	RunFailed RunStatus = "FAILED"
)

// StepStatus is the status of a single step within a run.
type StepStatus string

const (
	// StepPending means the step is waiting for its dependencies.
	StepPending StepStatus = "PENDING"
	// StepRunning means an attempt is (or was, before a crash) in progress.
	StepRunning StepStatus = "RUNNING"
	// StepSucceeded means the step completed successfully and must never run again.
	StepSucceeded StepStatus = "SUCCEEDED"
	// StepFailed means all attempts were exhausted.
	StepFailed StepStatus = "FAILED"
	// StepSkipped means an upstream dependency failed; the step never executed.
	StepSkipped StepStatus = "SKIPPED"
)

// FailureStrategy decides what happens to a step when one of its dependencies fails.
type FailureStrategy string

const (
	// FailOnDependencyFailure (default): if any dependency fails, this step is skipped
	// and the run is eventually marked FAILED.
	FailOnDependencyFailure FailureStrategy = "FAIL_ON_DEPENDENCY_FAILURE"
	// AllowDependencyFailure: the step still runs; failing dependencies are simply
	// omitted from its inputs.
	AllowDependencyFailure FailureStrategy = "ALLOW_DEPENDENCY_FAILURE"
)

// RetryPolicy configures attempts and backoff for a single step.
type RetryPolicy struct {
	// MaxAttempts is the total number of attempts (1 means no retry).
	// Zero defaults to 1.
	MaxAttempts int `json:"max_attempts"`
	// InitialBackoff before the second attempt. Zero means no waiting.
	InitialBackoff time.Duration `json:"initial_backoff"`
	// MaxBackoff caps the grown backoff. Zero means unbounded.
	MaxBackoff time.Duration `json:"max_backoff"`
	// Multiplier grows the backoff; <=1 means constant backoff.
	Multiplier float64 `json:"multiplier"`
}

// StepDef declares one node of the workflow DAG.
type StepDef struct {
	ID StepID `json:"id"`
	// Depends lists step IDs that must reach a terminal state before this step.
	Depends []StepID `json:"depends"`
	// OnDependencyFailure controls behavior when a dependency ends FAILED/SKIPPED.
	OnDependencyFailure FailureStrategy `json:"on_dependency_failure"`
	Retry               RetryPolicy     `json:"retry"`
	// Input is the step's static input payload, recorded verbatim in logs.
	Input string `json:"input"`
}

// Definition is an immutable workflow definition. Its fingerprint is part of
// every persisted run so definition changes can be detected later.
type Definition struct {
	Name  string    `json:"name"`
	Steps []StepDef `json:"steps"`
}

// StepState is the persisted state of one step inside a run.
type StepState struct {
	ID StepID `json:"id"`
	// Fingerprint is the digest of the step definition at run creation time.
	// It makes "existing step semantics unchanged" decidable even when only a
	// newer version of the workflow definition is registered.
	Fingerprint string     `json:"fingerprint,omitempty"`
	Status      StepStatus `json:"status"`
	Attempts    int        `json:"attempts"`
	Output      string     `json:"output,omitempty"`
	LastError   string     `json:"last_error,omitempty"`
	StartedAt   *time.Time `json:"started_at,omitempty"`
	FinishedAt  *time.Time `json:"finished_at,omitempty"`
	LastEventAt time.Time  `json:"last_event_at"`
}

// RunState is the full persisted state of one run.
type RunState struct {
	ID          RunID                 `json:"id"`
	DefName     string                `json:"def_name"`
	Fingerprint string                `json:"fingerprint"`
	Status      RunStatus             `json:"status"`
	Steps       map[StepID]*StepState `json:"steps"`
	CreatedAt   time.Time             `json:"created_at"`
	UpdatedAt   time.Time             `json:"updated_at"`
}

// EventKind enumerates the kinds of entries in a run's append-only event log.
type EventKind string

const (
	EventRunCreated    EventKind = "RUN_CREATED"
	EventRunStarted    EventKind = "RUN_STARTED"
	EventRunSucceeded  EventKind = "RUN_SUCCEEDED"
	EventRunFailed     EventKind = "RUN_FAILED"
	EventStepStarted   EventKind = "STEP_STARTED"
	EventStepRetry     EventKind = "STEP_RETRY"
	EventStepSucceeded EventKind = "STEP_SUCCEEDED"
	EventStepFailed    EventKind = "STEP_FAILED"
	EventStepSkipped   EventKind = "STEP_SKIPPED"
)

// Event is one persisted, append-only log entry.
type Event struct {
	Seq     int       `json:"seq"`
	At      time.Time `json:"at"`
	Kind    EventKind `json:"kind"`
	StepID  StepID    `json:"step_id,omitempty"`
	Attempt int       `json:"attempt,omitempty"`
	Detail  string    `json:"detail,omitempty"`
}

// Logger receives structured per-step decisions (inputs, attempts, verdicts).
type Logger interface {
	Log(ctx context.Context, runID RunID, ev Event)
}
