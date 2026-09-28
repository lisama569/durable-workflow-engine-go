package retry

import (
	"testing"
	"time"

	"github.com/lisama569/durable-workflow-engine-go/workflow"
)

func TestDelay(t *testing.T) {
	p := workflow.RetryPolicy{MaxAttempts: 5, BaseDelay: 100 * time.Millisecond, MaxDelay: time.Second, Multiplier: 2}
	cases := []struct {
		next int
		want time.Duration
	}{
		{1, 0},
		{2, 100 * time.Millisecond},
		{3, 200 * time.Millisecond},
		{4, 400 * time.Millisecond},
		{5, 800 * time.Millisecond},
		{6, time.Second}, // capped
	}
	for _, c := range cases {
		if got := Delay(p, c.next); got != c.want {
			t.Errorf("Delay(next=%d) = %v, want %v", c.next, got, c.want)
		}
	}
}

func TestNormalise(t *testing.T) {
	n := Normalise(workflow.RetryPolicy{})
	if n.MaxAttempts != DefaultMaxAttempts || n.Multiplier != DefaultMultiplier || n.BaseDelay != DefaultBaseDelay || n.MaxDelay != DefaultMaxDelay {
		t.Fatalf("unexpected defaults: %+v", n)
	}
	if Normalise(workflow.RetryPolicy{MaxAttempts: -1}).MaxAttempts != 1 {
		t.Fatal("negative MaxAttempts should mean one attempt")
	}
}
