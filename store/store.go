// Package store provides a dependency-free, file based persistence layer for
// workflow execution state. Every execution is one JSON document written
// atomically (temp file + rename); lock files serialise concurrent and
// cross-process access. Stale locks whose owner process has disappeared are
// reclaimed automatically.
package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"syscall"
	"time"

	"github.com/lisama569/durable-workflow-engine-go/workflow"
)

// ErrAlreadyExists is returned when an idempotency mapping already exists.
var ErrAlreadyExists = errors.New("store: idempotency key already exists")

// Store persists execution states inside a directory.
type Store struct {
	dir string
}

// New creates the persistence directory if needed.
func New(dir string) (*Store, error) {
	if dir == "" {
		return nil, errors.New("store: empty directory")
	}
	if err := os.MkdirAll(filepath.Join(dir, "locks"), 0o755); err != nil {
		return nil, fmt.Errorf("store: create dir: %w", err)
	}
	return &Store{dir: dir}, nil
}

// Dir returns the backing directory.
func (s *Store) Dir() string { return s.dir }

var safeID = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)

func (s *Store) safeName(id string) string {
	if safeID.MatchString(id) {
		return id
	}
	sum := sha256.Sum256([]byte(id))
	return "x" + hex.EncodeToString(sum[:])
}

func (s *Store) execPath(id string) string {
	return filepath.Join(s.dir, "exec-"+s.safeName(id)+".json")
}

func (s *Store) idemPath(key string) string {
	sum := sha256.Sum256([]byte(key))
	return filepath.Join(s.dir, "idem-"+hex.EncodeToString(sum[:])+".json")
}

// Load reads one execution state. It returns workflow.ErrNotFound when absent.
func (s *Store) Load(id string) (*workflow.ExecutionState, error) {
	raw, err := os.ReadFile(s.execPath(id))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, workflow.ErrNotFound
		}
		return nil, fmt.Errorf("store: load execution %q: %w", id, err)
	}
	var st workflow.ExecutionState
	if err := json.Unmarshal(raw, &st); err != nil {
		return nil, fmt.Errorf("store: decode execution %q: %w", id, err)
	}
	return &st, nil
}

// Save writes the state atomically: encode, write a sibling temp file, fsync
// it, then rename over the destination.
func (s *Store) Save(st *workflow.ExecutionState) error {
	if st == nil || st.ID == "" {
		return errors.New("store: cannot save state without id")
	}
	st.UpdatedAt = time.Now().UTC()
	raw, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return fmt.Errorf("store: encode execution %q: %w", st.ID, err)
	}
	final := s.execPath(st.ID)
	tmp, err := os.CreateTemp(s.dir, ".exec-*.tmp")
	if err != nil {
		return fmt.Errorf("store: create temp file: %w", err)
	}
	tmpName := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpName) }
	if _, err := tmp.Write(raw); err != nil {
		_ = tmp.Close()
		cleanup()
		return fmt.Errorf("store: write execution %q: %w", st.ID, err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		cleanup()
		return fmt.Errorf("store: fsync execution %q: %w", st.ID, err)
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return fmt.Errorf("store: close execution %q: %w", st.ID, err)
	}
	if err := os.Rename(tmpName, final); err != nil {
		cleanup()
		return fmt.Errorf("store: rename execution %q: %w", st.ID, err)
	}
	return nil
}

// IdempotencyKey maps an idempotency key to an execution id.
type IdempotencyKey struct {
	Key         string `json:"key"`
	ExecutionID string `json:"execution_id"`
	PID         int    `json:"pid"`
	Host        string `json:"host"`
	CreatedAt   string `json:"created_at"`
}

// LoadIdempotency resolves an idempotency key; returns workflow.ErrNotFound.
func (s *Store) LoadIdempotency(key string) (*IdempotencyKey, error) {
	raw, err := os.ReadFile(s.idemPath(key))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, workflow.ErrNotFound
		}
		return nil, fmt.Errorf("store: load idempotency key: %w", err)
	}
	var rec IdempotencyKey
	if err := json.Unmarshal(raw, &rec); err != nil {
		return nil, fmt.Errorf("store: decode idempotency key: %w", err)
	}
	return &rec, nil
}

// SaveIdempotency records a key -> execution id mapping. It fails with
// ErrAlreadyExists when the mapping is already on disk (another submit won
// the race), so the caller can load and adopt the winner's execution id.
// A per-key lock plus temp-file-and-rename guarantees that a concurrent
// LoadIdempotency never observes a half-written record.
func (s *Store) SaveIdempotency(rec *IdempotencyKey) error {
	lock, err := s.Acquire(context.Background(), "idem-lock:"+rec.Key)
	if err != nil {
		return fmt.Errorf("store: lock idempotency key: %w", err)
	}
	defer lock.Release()
	return s.SaveIdempotencyLocked(rec)
}

// AcquireKeyLock takes the per-key lock used by SaveIdempotency. Callers that
// must keep the claim stable across a subsequent operation (the engine keeps
// it while taking the execution lock, to avoid inverted lock ordering) hold
// it themselves and use SaveIdempotencyLocked.
func (s *Store) AcquireKeyLock(ctx context.Context, key string) (Lock, error) {
	return s.Acquire(ctx, "idem-lock:"+key)
}

// SaveIdempotencyLocked is SaveIdempotency without taking the per-key lock.
// The caller MUST hold AcquireKeyLock for the same key.
func (s *Store) SaveIdempotencyLocked(rec *IdempotencyKey) error {
	if rec == nil || rec.Key == "" || rec.ExecutionID == "" {
		return errors.New("store: incomplete idempotency record")
	}
	if _, err := os.Stat(s.idemPath(rec.Key)); err == nil {
		return ErrAlreadyExists
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("store: stat idempotency key: %w", err)
	}

	raw, err := json.MarshalIndent(rec, "", "  ")
	if err != nil {
		return fmt.Errorf("store: encode idempotency key: %w", err)
	}
	path := s.idemPath(rec.Key)
	tmp, err := os.CreateTemp(s.dir, ".idem-*.tmp")
	if err != nil {
		return fmt.Errorf("store: create temp idempotency file: %w", err)
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(raw); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return fmt.Errorf("store: write idempotency key: %w", err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("store: close idempotency key: %w", err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("store: publish idempotency key: %w", err)
	}
	return nil
}

// --- locking ---------------------------------------------------------------

type fileLock struct {
	path string
	// dead tells Release not to remove a lock that this process no longer owns.
	dead bool
}

type lockPayload struct {
	PID       int    `json:"pid"`
	Host      string `json:"host"`
	StartNs   int64  `json:"start_ns"`
	CreatedAt string `json:"created_at"`
}

// processStart is fixed for the lifetime of this process; together with PID
// it distinguishes a lock owned by THIS process (even one whose payload has
// not been fully written yet) from a stale lock left by a dead process.
var processStart = time.Now().UTC().UnixNano()

// Lock is an acquired exclusive lock.
type Lock interface {
	Release() error
}

func processAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	p, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	return p.Signal(syscall.Signal(0)) == nil
}

// Acquire obtains an exclusive lock for the given name (execution id or an
// idempotency namespace). It waits until the lock is available or ctx is
// cancelled, reclaiming a lock file whose owner is a different, dead process.
func (s *Store) Acquire(ctx context.Context, name string) (Lock, error) {
	path := filepath.Join(s.dir, "locks", s.safeName(name)+".lock")
	host, _ := os.Hostname()
	payload := lockPayload{PID: os.Getpid(), Host: host, StartNs: processStart, CreatedAt: time.Now().UTC().Format(time.RFC3339Nano)}
	raw, _ := json.Marshal(payload)

	for ctx.Err() == nil {
		f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
		if err == nil {
			_, werr := f.Write(raw)
			_ = f.Close()
			if werr != nil {
				_ = os.Remove(path)
				return nil, fmt.Errorf("store: write lock: %w", werr)
			}
			return &fileLock{path: path}, nil
		}
		if !errors.Is(err, os.ErrExist) {
			return nil, fmt.Errorf("store: acquire: %w", err)
		}
		// Inspect the existing lock and reclaim it only when its owner is a
		// DIFFERENT process that has exited. A lock carrying this process'
		// identity (including a zero-byte file whose payload this very
		// process has not finished writing) must never be reclaimed.

		if existing, rerr := os.ReadFile(path); rerr == nil {
			if bytes.TrimSpace(existing) == nil {
				if os.Getpid() != payload.PID {
					// Zero-byte lock left by a different, presumably dead,
					// process between create and write.
					if rmErr := os.Remove(path); rmErr == nil {
						continue
					}
				}
			} else {
				var p lockPayload
				if json.Unmarshal(existing, &p) == nil {
					sameProcess := p.PID == os.Getpid() && (p.StartNs == 0 || p.StartNs == processStart)
					sameHost := p.Host == "" || p.Host == host
					if sameHost && !sameProcess && !processAlive(p.PID) {
						if rmErr := os.Remove(path); rmErr == nil {
							continue
						}
					}
				}
			}
		}
		select {
		case <-ctx.Done():
		case <-time.After(20 * time.Millisecond):
		}
	}
	return nil, ctx.Err()
}

func (l *fileLock) Release() error {
	if l.dead {
		return nil
	}
	if err := os.Remove(l.path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}
