// Package retry computes exponential backoff delays.
package retry

import (
	"time"

	"github.com/lisama569/durable-workflow-engine-go/workflow"
)

// Defaults applied by Normalise when a retry policy leaves fields unset.
const (
	DefaultMaxAttempts = 3
	DefaultBaseDelay   = 100 * time.Millisecond
	DefaultMaxDelay    = 5 * time.Second
	DefaultMultiplier  = 2.0
)

// Normalise fills in defaults for unset / invalid retry fields.
// MaxAttempts==0 means "use default"; an explicitly negative value means
// "no retries" (one attempt only) so callers can express intent precisely.
func Normalise(p workflow.RetryPolicy) workflow.RetryPolicy {
	if p.MaxAttempts == 0 {
		p.MaxAttempts = DefaultMaxAttempts
	} else if p.MaxAttempts < 0 {
		p.MaxAttempts = 1
	}
	if p.BaseDelay == 0 {
		p.BaseDelay = DefaultBaseDelay
	}
	if p.MaxDelay == 0 {
		p.MaxDelay = DefaultMaxDelay
	}
	if p.MaxDelay > 0 && p.BaseDelay > p.MaxDelay {
		p.BaseDelay = p.MaxDelay
	}
	if p.Multiplier < 1 {
		p.Multiplier = DefaultMultiplier
	}
	return p
}

// Delay returns how long to wait before attempt number nextAttempt.
// nextAttempt is 1-based; the first retry (before attempt 2) uses BaseDelay,
// then BaseDelay*Multiplier, ... capped at MaxDelay. A negative MaxDelay
// means "no cap".
func Delay(p workflow.RetryPolicy, nextAttempt int) time.Duration {
	p = Normalise(p)
	if nextAttempt <= 1 {
		return 0
	}
	delay := float64(p.BaseDelay)
	for i := 2; i < nextAttempt; i++ {
		delay *= p.Multiplier
	}
	if p.MaxDelay > 0 && delay > float64(p.MaxDelay) {
		return p.MaxDelay
	}
	if delay < 0 {
		return 0
	}
	return time.Duration(delay)
}

// NormaliseStepDef returns a copy of s with its retry policy normalised.
func NormaliseStepDef(s workflow.StepDef) workflow.StepDef {
	s.Retry = Normalise(s.Retry)
	return s
}
