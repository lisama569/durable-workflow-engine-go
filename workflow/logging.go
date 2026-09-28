package workflow

import (
	"context"
	"log"
	"os"
	"strconv"
	"strings"
)

// StdLogger writes workflow events to an *log.Logger using a stable,
// greppable line format:
//
//	run=<id> seq=<n> kind=<KIND> step=<id> attempt=<n> detail=<...>
//
// STEP_STARTED details include the step's static input and its upstream
// inputs; STEP_RETRY/SKIPPED details state the verdict and its reason, so the
// log explains why every scheduling decision was made.
type StdLogger struct {
	l *log.Logger
}

// NewStdLogger logs to stderr.
func NewStdLogger() *StdLogger {
	return NewStdLoggerWith(log.New(os.Stderr, "", log.LstdFlags|log.Lmicroseconds))
}

// NewStdLoggerWith wraps a custom logger.
func NewStdLoggerWith(l *log.Logger) *StdLogger { return &StdLogger{l: l} }

// Log implements Logger.
func (lg *StdLogger) Log(ctx context.Context, runID RunID, ev Event) {
	var b strings.Builder
	b.WriteString("run=")
	b.WriteString(string(runID))
	b.WriteString(" seq=")
	b.WriteString(strconv.Itoa(ev.Seq))
	b.WriteString(" kind=")
	b.WriteString(string(ev.Kind))
	if ev.StepID != "" {
		b.WriteString(" step=")
		b.WriteString(string(ev.StepID))
	}
	if ev.Attempt > 0 {
		b.WriteString(" attempt=")
		b.WriteString(strconv.Itoa(ev.Attempt))
	}
	if ev.Detail != "" {
		b.WriteString(" detail=")
		b.WriteString(strconv.Quote(ev.Detail))
	}
	lg.l.Print(b.String())
}

// nopLogger is used when Options.Logger is nil.
type nopLogger struct{}

func (nopLogger) Log(ctx context.Context, runID RunID, ev Event) {}
