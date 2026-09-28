package store

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lisama569/durable-workflow-engine-go/workflow"
)

func TestSaveLoadRoundtrip(t *testing.T) {
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	st := &workflow.ExecutionState{
		ID:    "exec-1",
		Steps: map[string]*workflow.StepState{"a": {ID: "a", Status: workflow.StepSucceeded, Attempts: 1, Output: "out"}},
	}
	if err := s.Save(st); err != nil {
		t.Fatal(err)
	}
	got, err := s.Load("exec-1")
	if err != nil {
		t.Fatal(err)
	}
	if got.Steps["a"].Output != "out" {
		t.Fatalf("roundtrip mismatch: %+v", got.Steps["a"])
	}
}

func TestLoadMissing(t *testing.T) {
	s, _ := New(t.TempDir())
	if _, err := s.Load("nope"); !errors.Is(err, workflow.ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

func TestIdempotencyCreateTwice(t *testing.T) {
	s, _ := New(t.TempDir())
	rec := &IdempotencyKey{Key: "k1", ExecutionID: "e1", PID: os.Getpid()}
	if err := s.SaveIdempotency(rec); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveIdempotency(rec); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("want ErrAlreadyExists, got %v", err)
	}
	got, err := s.LoadIdempotency("k1")
	if err != nil || got.ExecutionID != "e1" {
		t.Fatalf("load mismatch: %+v %v", got, err)
	}
}

func TestLockSerialises(t *testing.T) {
	s, _ := New(t.TempDir())
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	l1, err := s.Acquire(ctx, "exec-1")
	if err != nil {
		t.Fatal(err)
	}
	rel := make(chan struct{})
	got2 := make(chan Lock, 1)
	go func() {
		l2, err := s.Acquire(ctx, "exec-1")
		if err == nil {
			got2 <- l2
		}
		close(rel)
	}()
	select {
	case <-got2:
		t.Fatal("second acquire must block while first lock is held")
	case <-time.After(100 * time.Millisecond):
	}
	if err := l1.Release(); err != nil {
		t.Fatal(err)
	}
	select {
	case l2 := <-got2:
		_ = l2.Release()
	case <-time.After(2 * time.Second):
		t.Fatal("second acquire did not succeed after release")
	}
}

func TestStaleLockReclaimed(t *testing.T) {
	dir := t.TempDir()
	s, _ := New(dir)
	// Fabricate a lock owned by a definitely-dead PID.
	payload := `{"pid":2147480000,"host":"","created_at":"2020-01-01T00:00:00Z"}`
	if err := os.WriteFile(filepath.Join(dir, "locks", "dead.lock"), []byte(payload), 0o644); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	l, err := s.Acquire(ctx, "dead")
	if err != nil {
		t.Fatalf("stale lock should be reclaimed: %v", err)
	}
	_ = l.Release()
}
