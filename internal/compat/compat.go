// Package compat decides whether an execution recorded under an old
// definition may be resumed under a new one.
//
// Compatibility rules (evaluated against the persisted execution, not just
// the definitions):
//
//   - An added step is always safe: its predecessors' outputs are available
//     and it has never run before, so it simply runs during the resume.
//   - A removed step is safe only when nothing recorded as succeeded depends
//     on it. Otherwise the old breakpoint expressed semantics that no longer
//     exist and is rejected rather than silently reinterpreted.
//   - A changed dependency edge, or a semantic change (version/metadata/
//     retry) to a step that already succeeded, invalidates that step's
//     recorded result. Resume is rejected by default; with
//     AcceptNewSemantics the affected step and all of its recorded
//     successors are reset to "pending" and re-executed under the new
//     definition.
//   - A semantic change to a step that has not succeeded yet is safe: the
//     new semantics run naturally on its first execution.
package compat

import (
	"fmt"

	"github.com/lisama569/durable-workflow-engine-go/workflow"
)

// ChangeKind categorises a single difference between two definitions.
type ChangeKind string

const (
	ChangeStepAdded   ChangeKind = "step_added"
	ChangeStepRemoved ChangeKind = "step_removed"
	ChangeStepChanged ChangeKind = "step_changed"
	ChangeDepsChanged ChangeKind = "dependencies_changed"
)

// Change describes one detected definition difference.
type Change struct {
	Kind   ChangeKind
	StepID string
	Detail string
}

func index(d *workflow.Definition) map[string]workflow.StepDef {
	m := make(map[string]workflow.StepDef, len(d.Steps))
	for _, s := range d.Steps {
		m[s.ID] = s
	}
	return m
}

// Diff compares two definitions by their structural/semantic fingerprints.
// Order is deterministic: removed, changed, deps, added, each sorted by id.
func Diff(oldDef, newDef *workflow.Definition) []Change {
	return DiffSnapshots(oldDef.Snapshot(), newDef.Snapshot())
}

func snapshotToDef(snap *workflow.DefinitionSnapshot) *workflow.Definition {
	d := &workflow.Definition{Name: snap.Name, Version: snap.Version, Steps: make([]workflow.StepDef, 0, len(snap.Steps))}
	for _, st := range snap.Steps {
		d.Steps = append(d.Steps, workflow.StepDef{
			ID:        st.ID,
			DependsOn: append([]string(nil), st.DependsOn...),
			Version:   st.Version,
			Metadata:  st.Metadata,
			Retry:     st.Retry,
		})
	}
	return d
}

// DiffSnapshots compares two persisted definition snapshots.
func DiffSnapshots(oldSnap, newSnap *workflow.DefinitionSnapshot) []Change {
	return diffIndex(index(snapshotToDef(oldSnap)), index(snapshotToDef(newSnap)))
}

func diffIndex(oldSteps, newSteps map[string]workflow.StepDef) []Change {
	var changes []Change
	var removed, added, changed, deps []string
	for id := range oldSteps {
		if _, ok := newSteps[id]; !ok {
			removed = append(removed, id)
		}
	}
	for id := range newSteps {
		if _, ok := oldSteps[id]; !ok {
			added = append(added, id)
		}
	}
	for id, o := range oldSteps {
		n, ok := newSteps[id]
		if !ok {
			continue
		}
		if workflow.FingerprintStepDeps(o) != workflow.FingerprintStepDeps(n) {
			deps = append(deps, id)
		} else if workflow.FingerprintStepSemantics(o) != workflow.FingerprintStepSemantics(n) {
			changed = append(changed, id)
		}
	}
	sortStrings(removed)
	sortStrings(added)
	sortStrings(changed)
	sortStrings(deps)
	for _, id := range removed {
		changes = append(changes, Change{Kind: ChangeStepRemoved, StepID: id, Detail: fmt.Sprintf("step %q exists in the old definition but not in the new one", id)})
	}
	for _, id := range changed {
		changes = append(changes, Change{Kind: ChangeStepChanged, StepID: id, Detail: fmt.Sprintf("step %q changed (version, metadata or retry policy)", id)})
	}
	for _, id := range deps {
		changes = append(changes, Change{Kind: ChangeDepsChanged, StepID: id, Detail: fmt.Sprintf("dependency edges of step %q changed", id)})
	}
	for _, id := range added {
		changes = append(changes, Change{Kind: ChangeStepAdded, StepID: id, Detail: fmt.Sprintf("step %q is new and has never run", id)})
	}
	return changes
}

// Verdict is the compatibility decision for a recorded execution.
type Verdict struct {
	// Compatible means the stored breakpoint can be resumed as-is under
	// the new definition with no step reset.
	Compatible bool
	// Safe reports whether resuming is possible at all. Added steps, and
	// semantic changes to not-yet-run steps, are safe. Removed/changed
	// steps with recorded results make it unsafe.
	Safe    bool
	Changes []Change
	// ReplayRequired lists step ids whose recorded success must be
	// discarded when accepting the new definition. It always includes the
	// directly affected steps and every recorded successor of them.
	ReplayRequired []string
}

func succeeded(steps map[string]*workflow.StepState, id string) bool {
	s, ok := steps[id]
	return ok && s.Status == workflow.StepSucceeded
}

// Evaluate decides whether a recorded execution can be resumed under newSnap.
// recordedHash/newHash are passed in so compat has no hashing dependency.
func Evaluate(oldSnap, newSnap *workflow.DefinitionSnapshot, recordedHash, newHash string, steps map[string]*workflow.StepState) Verdict {
	v := Verdict{Safe: true}
	if recordedHash == newHash {
		v.Compatible = true
		return v
	}
	oldDef := snapshotToDef(oldSnap)
	newDef := snapshotToDef(newSnap)
	changes := DiffSnapshots(oldSnap, newSnap)
	v.Changes = changes

	oldByName := index(oldDef)

	// Unsafe condition 1: a removed step has recorded success, or any
	// recorded-succeeded step depended (directly) on it.
	for _, c := range changes {
		if c.Kind != ChangeStepRemoved {
			continue
		}
		if succeeded(steps, c.StepID) {
			v.Safe = false
		}
		for id, old := range oldByName {
			for _, dep := range old.DependsOn {
				if dep == c.StepID && succeeded(steps, id) {
					v.Safe = false
				}
			}
		}
	}

	// Affected steps: changed/deps-changed steps, plus deps-changed-step
	// successors (the edge change invalidates downstream recorded output).
	affected := map[string]struct{}{}
	for _, c := range changes {
		switch c.Kind {
		case ChangeStepChanged, ChangeDepsChanged:
			affected[c.StepID] = struct{}{}
		}
	}

	// Unsafe condition 2: an affected step already produced a recorded
	// result. Under default policy we refuse rather than silently replay.
	for id := range affected {
		if succeeded(steps, id) {
			v.Safe = false
		}
	}

	// Replay closure: affected steps and every recorded successor.
	successorsOf := func(roots map[string]struct{}) map[string]struct{} {
		out := map[string]struct{}{}
		// reverse edges within the new definition
		rev := map[string][]string{}
		for _, s := range newDef.Steps {
			for _, dep := range s.DependsOn {
				rev[dep] = append(rev[dep], s.ID)
			}
		}
		var stack []string
		for r := range roots {
			stack = append(stack, r)
		}
		for len(stack) > 0 {
			cur := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			if _, seen := out[cur]; seen {
				continue
			}
			out[cur] = struct{}{}
			stack = append(stack, rev[cur]...)
		}
		return out
	}
	closure := successorsOf(affected)
	for id := range closure {
		// Only steps that actually carry a recorded result need resetting;
		// pending steps simply run under the new definition.
		if st, ok := steps[id]; ok && st.Status != workflow.StepPending {
			v.ReplayRequired = append(v.ReplayRequired, id)
		}
	}
	sortStrings(v.ReplayRequired)

	if v.Safe && len(v.ReplayRequired) == 0 {
		// Additive changes, or changes limited to steps that never ran:
		// no recorded result is questioned and nothing is reset.
		v.Compatible = true
	}
	return v
}

// sortStrings is a tiny local sort to keep imports minimal.
func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j-1] > s[j]; j-- {
			s[j-1], s[j] = s[j], s[j-1]
		}
	}
}
