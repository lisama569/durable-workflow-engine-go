package workflow

import "time"

// ExportedNextBackoffForTest exposes nextBackoff to external tests.
func ExportedNextBackoffForTest(p RetryPolicy, attemptsDone int) time.Duration {
	return nextBackoff(p, attemptsDone)
}
