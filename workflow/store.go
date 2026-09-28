package workflow

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"syscall"
)

// Store persists run states and their append-only event logs.
//
// Durability contract: SaveState is atomic from the point of view of other
// processes (temp file + fsync + rename), so a crash can only ever expose the
// previous or the new state, never a torn one. AppendEvent is fsynced before
// it returns. The engine always records the new state first and appends its
// explanatory event second; an interrupted process may therefore lose at most
// the last human-readable event line, never a step's committed outcome.
// Completed steps are committed in state.json and are never executed again.
type Store interface {
	// LoadState returns ErrRunNotFound when no run with that id exists.
	LoadState(ctx context.Context, id RunID) (*RunState, error)
	// CreateState persists a brand-new run. It must fail with
	// ErrRunAlreadyExists if the id was already taken.
	CreateState(ctx context.Context, state *RunState) error
	// SaveState overwrites an existing run state atomically.
	SaveState(ctx context.Context, state *RunState) error

	// LoadEvents returns the full append-only event log of a run.
	LoadEvents(ctx context.Context, id RunID) ([]Event, error)
	// AppendEvent appends one event to the run's log.
	AppendEvent(ctx context.Context, id RunID, ev Event) error
}

var safeID = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)

func runDir(root string, id RunID) (string, error) {
	if !safeID.MatchString(string(id)) {
		return "", fmt.Errorf("workflow: unsafe run id %q", id)
	}
	return filepath.Join(root, string(id)), nil
}

// FileStore is a JSON-on-disk Store usable by real processes; one directory
// per run, state written atomically via temp-file + rename, and an flock per
// run so concurrent processes do not interleave writes.
type FileStore struct {
	root string
}

// NewFileStore opens (creating if needed) a file-backed store under root.
func NewFileStore(root string) (*FileStore, error) {
	if root == "" {
		return nil, errors.New("workflow: empty store root")
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, fmt.Errorf("workflow: create store root: %w", err)
	}
	return &FileStore{root: root}, nil
}

// lock acquires an exclusive flock on the run's lock file for the duration
// of fn, serializing writers across processes as well as goroutines.
func (s *FileStore) lock(id RunID, fn func(dir string) error) error {
	dir, err := runDir(s.root, id)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("workflow: create run dir: %w", err)
	}
	f, err := os.OpenFile(filepath.Join(dir, "lock"), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return fmt.Errorf("workflow: open lock: %w", err)
	}
	defer f.Close()
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		return fmt.Errorf("workflow: flock: %w", err)
	}
	defer syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	return fn(dir)
}

func writeAtomic(path string, data []byte, perm os.FileMode) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, perm)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, path)
}

func (s *FileStore) LoadState(ctx context.Context, id RunID) (*RunState, error) {
	dir, err := runDir(s.root, id)
	if err != nil {
		return nil, err
	}
	raw, err := os.ReadFile(filepath.Join(dir, "state.json"))
	if errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("%w: %s", ErrRunNotFound, id)
	}
	if err != nil {
		return nil, err
	}
	var state RunState
	if err := json.Unmarshal(raw, &state); err != nil {
		return nil, fmt.Errorf("%w: state.json for %s: %v", ErrIncompatible, id, err)
	}
	return &state, nil
}

func (s *FileStore) CreateState(ctx context.Context, state *RunState) error {
	return s.lock(state.ID, func(dir string) error {
		if _, err := os.Stat(filepath.Join(dir, "state.json")); err == nil {
			return fmt.Errorf("%w: %s", ErrRunAlreadyExists, state.ID)
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		data, err := json.MarshalIndent(state, "", "  ")
		if err != nil {
			return err
		}
		return writeAtomic(filepath.Join(dir, "state.json"), data, 0o644)
	})
}

func (s *FileStore) SaveState(ctx context.Context, state *RunState) error {
	return s.lock(state.ID, func(dir string) error {
		data, err := json.MarshalIndent(state, "", "  ")
		if err != nil {
			return err
		}
		return writeAtomic(filepath.Join(dir, "state.json"), data, 0o644)
	})
}

func (s *FileStore) LoadEvents(ctx context.Context, id RunID) ([]Event, error) {
	dir, err := runDir(s.root, id)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(filepath.Join(dir, "events.log"))
	if errors.Is(err, os.ErrNotExist) {
		// A run with no events yet (or a crash before the first append).
		if _, statErr := os.Stat(filepath.Join(dir, "state.json")); statErr != nil {
			return nil, fmt.Errorf("%w: %s", ErrRunNotFound, id)
		}
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var events []Event
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for sc.Scan() {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		var ev Event
		if err := json.Unmarshal(line, &ev); err != nil {
			return nil, fmt.Errorf("%w: events.log for %s: %v", ErrIncompatible, id, err)
		}
		events = append(events, ev)
	}
	return events, sc.Err()
}

func (s *FileStore) AppendEvent(ctx context.Context, id RunID, ev Event) error {
	return s.lock(id, func(dir string) error {
		data, err := json.Marshal(ev)
		if err != nil {
			return err
		}
		f, err := os.OpenFile(filepath.Join(dir, "events.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return err
		}
		defer f.Close()
		if _, err := f.Write(append(data, '\n')); err != nil {
			return err
		}
		return f.Sync()
	})
}

// MemoryStore is an in-process Store for tests. Returned states and saved
// states are deep-copied, so callers cannot mutate each other's snapshots;
// this is what keeps concurrent branches from polluting shared state.
type MemoryStore struct {
	mu     sync.Mutex
	states map[RunID]*RunState
	events map[RunID][]Event
}

// NewMemoryStore creates an empty in-memory store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		states: make(map[RunID]*RunState),
		events: make(map[RunID][]Event),
	}
}

func cloneState(in *RunState) *RunState {
	raw, err := json.Marshal(in)
	if err != nil {
		panic(err)
	}
	var out RunState
	if err := json.Unmarshal(raw, &out); err != nil {
		panic(err)
	}
	return &out
}

func (s *MemoryStore) LoadState(ctx context.Context, id RunID) (*RunState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	state, ok := s.states[id]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrRunNotFound, id)
	}
	return cloneState(state), nil
}

func (s *MemoryStore) CreateState(ctx context.Context, state *RunState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.states[state.ID]; ok {
		return fmt.Errorf("%w: %s", ErrRunAlreadyExists, state.ID)
	}
	s.states[state.ID] = cloneState(state)
	if _, ok := s.events[state.ID]; !ok {
		s.events[state.ID] = nil
	}
	return nil
}

func (s *MemoryStore) SaveState(ctx context.Context, state *RunState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.states[state.ID]; !ok {
		return fmt.Errorf("%w: %s", ErrRunNotFound, state.ID)
	}
	s.states[state.ID] = cloneState(state)
	return nil
}

func (s *MemoryStore) LoadEvents(ctx context.Context, id RunID) ([]Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	events, ok := s.events[id]
	if !ok {
		if _, okState := s.states[id]; !okState {
			return nil, fmt.Errorf("%w: %s", ErrRunNotFound, id)
		}
		return nil, nil
	}
	out := make([]Event, len(events))
	copy(out, events)
	return out, nil
}

func (s *MemoryStore) AppendEvent(ctx context.Context, id RunID, ev Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.states[id]; !ok {
		return fmt.Errorf("%w: %s", ErrRunNotFound, id)
	}
	s.events[id] = append(s.events[id], ev)
	return nil
}
