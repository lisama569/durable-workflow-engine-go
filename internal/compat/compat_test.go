package compat

import (
	"context"
	"testing"

	"github.com/lisama569/durable-workflow-engine-go/workflow"
)

func step(id string, deps ...string) workflow.StepDef {
	return workflow.StepDef{ID: id, DependsOn: deps, Run: func(_ context.Context, _ workflow.StepInput) (any, error) { return nil, nil }}
}

func def(steps ...workflow.StepDef) *workflow.Definition {
	return &workflow.Definition{Name: "wf", Steps: steps}
}

func succeededState(ids ...string) map[string]*workflow.StepState {
	m := map[string]*workflow.StepState{}
	for _, id := range ids {
		m[id] = &workflow.StepState{ID: id, Status: workflow.StepSucceeded}
	}
	return m
}

func TestSameHashCompatible(t *testing.T) {
	d := def(step("a"), step("b", "a"))
	h, _ := d.Hash()
	v := Evaluate(d.Snapshot(), d.Snapshot(), h, h, nil)
	if !v.Compatible || !v.Safe || len(v.Changes) != 0 {
		t.Fatalf("identical definitions must be compatible: %+v", v)
	}
}

func TestAddStepSafe(t *testing.T) {
	old := def(step("a"))
	new := def(step("a"), step("b", "a"))
	oh, _ := old.Hash()
	nh, _ := new.Hash()
	v := Evaluate(old.Snapshot(), new.Snapshot(), oh, nh, succeededState("a"))
	if !v.Safe || !v.Compatible || len(v.ReplayRequired) != 0 {
		t.Fatalf("adding a downstream step must be safe: %+v", v)
	}
}

func TestChangeSucceededStepUnsafeAndReplayClosure(t *testing.T) {
	old := def(step("a"), step("b", "a"), step("c", "b"))
	changed := def(step("a"), step("b", "a"), step("c", "b"))
	ss := changed.Steps[1]
	ss.Version = 2
	changed.Steps[1] = ss
	oh, _ := old.Hash()
	nh, _ := changed.Hash()
	v := Evaluate(old.Snapshot(), changed.Snapshot(), oh, nh, succeededState("a", "b", "c"))
	if v.Safe || v.Compatible {
		t.Fatalf("changing a succeeded step must be unsafe by default: %+v", v)
	}
	want := map[string]bool{"b": true, "c": true}
	if len(v.ReplayRequired) != len(want) {
		t.Fatalf("replay closure = %v, want %v", v.ReplayRequired, want)
	}
	for _, id := range v.ReplayRequired {
		if !want[id] {
			t.Fatalf("unexpected replay step %q", id)
		}
	}
}

func TestChangeUnrunStepSafe(t *testing.T) {
	old := def(step("a"))
	changed := def(step("a"))
	ss := changed.Steps[0]
	ss.Version = 9
	changed.Steps[0] = ss
	oh, _ := old.Hash()
	nh, _ := changed.Hash()
	v := Evaluate(old.Snapshot(), changed.Snapshot(), oh, nh, succeededState())
	if !v.Safe || !v.Compatible {
		t.Fatalf("changing a step that never ran must be safe: %+v", v)
	}
}

func TestRemoveSucceededStepUnsafe(t *testing.T) {
	old := def(step("a"), step("b", "a"))
	new := def(step("a"))
	oh, _ := old.Hash()
	nh, _ := new.Hash()
	v := Evaluate(old.Snapshot(), new.Snapshot(), oh, nh, succeededState("a", "b"))
	if v.Safe || v.Compatible {
		t.Fatalf("removing a succeeded step must be unsafe: %+v", v)
	}
}

func TestRemoveNeverRunStepSafe(t *testing.T) {
	old := def(step("a"), step("b", "a"))
	new := def(step("a"))
	oh, _ := old.Hash()
	nh, _ := new.Hash()
	v := Evaluate(old.Snapshot(), new.Snapshot(), oh, nh, succeededState("a"))
	if !v.Safe || !v.Compatible {
		t.Fatalf("removing a never-run step must be safe: %+v", v)
	}
}

func TestDepsChangeReplaysSuccessors(t *testing.T) {
	old := def(step("a"), step("b"), step("c", "a", "b"))
	upd := def(step("a"), step("b"), step("c", "a"))
	oh, _ := old.Hash()
	nh, _ := upd.Hash()
	v := Evaluate(old.Snapshot(), upd.Snapshot(), oh, nh, succeededState("a", "b", "c"))
	if v.Safe {
		t.Fatalf("changing deps of a succeeded step must be unsafe")
	}
	want := map[string]bool{"c": true}
	if len(v.ReplayRequired) != len(want) {
		t.Fatalf("replay closure = %v, want [c]", v.ReplayRequired)
	}
}
