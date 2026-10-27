package compile

import (
	"fmt"
	"maps"
	"slices"

	"github.com/parallelworks/meridian/internal/jsonptr"
	"github.com/parallelworks/meridian/plan"
)

// A Parent is what a child run's Create is given of the step that calls
// its workflow, a try of a step of a job of another run (docs/design.md,
// Workflows a step calls). What the run is submitted is the step's with:,
// as Step gave it (plan.ReadyStep.With).
type Parent struct {
	// Run, Job, Step and Try name the step's try: the run whose step it
	// is, the key of its job, an instance's for a matrix job, the step's
	// index in its job and the try, from 0 (plan.ParentRun). Root is the
	// ID of the run at the top of the tree, and Depth how far below it the
	// child is: 1 for a child of one of its root's own steps.
	Run, Job  string
	Step, Try int
	Root      string
	Depth     int
	// Env is the step's environment, which the run's env: is laid over:
	// the workflow's, the job's and the step's env:, and the variables
	// format 1 gives a step, by name.
	Env map[string]string
	// Private and Secrets name what the step gives the run that is made
	// from an organization or user variable or a password, but no secret,
	// and from a secret, as plan.ParentRun's do: inputs. and a path for a
	// part of its with:, as plan.ReadyStep.WithPrivate and WithSecrets
	// name them, and env. and a name for a variable of Env, as EnvPrivate
	// and EnvSecrets do. The plan holds none of them: what was submitted
	// is the secret record's, as a password is, and Env's is the record's
	// too (plan.Record.Inherited).
	Private, Secrets []string
	// Masked are the strings the step's worker masks, as a Masker takes
	// them, which the run's jobs mask too (plan.Inherited.Masked).
	Masked []string
}

// sourceKeys are the keys of a step's with: that say where the workflow
// its uses: names is, and aren't the workflow's inputs.
var sourceKeys = []string{"$yaml", "$host", "$thumbnail"}

// check returns an error if p isn't what a step can give with with, the
// step's with:: a name in Private or Secrets that is neither inputs. and
// the path of a part of what the run is submitted (submitted), nor env.
// and the name of a variable of Env, or that is in them twice. That is the
// caller's mistake, and so Create fails closed: a name that holds nothing
// would hold nothing of the value it was meant to.
func (p *Parent) check(with map[string]any) error {
	if p == nil {
		return nil
	}
	submitted := p.submitted(with)
	seen := map[string]bool{}
	for _, names := range [][]string{p.Private, p.Secrets} {
		for _, name := range names {
			path := plan.SplitPath(name)
			switch {
			case seen[name]:
				return fmt.Errorf("compile: the step that calls the run names %s as private twice", name)
			case len(path) >= 2 && path[0] == "inputs":
				if _, ok := jsonptr.Lookup(submitted, path[1:]); !ok {
					return fmt.Errorf("compile: the step that calls the run names %s as private, a part that its with: doesn't hold", name)
				}
			case len(path) == 2 && path[0] == "env":
				if _, ok := p.Env[path[1]]; !ok {
					return fmt.Errorf("compile: the step that calls the run names %s as private, which its environment doesn't hold", name)
				}
			default:
				return fmt.Errorf("compile: the step that calls the run names %s as private, which is neither a part of its with: nor a variable of its environment", name)
			}
			seen[name] = true
		}
	}
	return nil
}

// submitted returns what the run is submitted, with, the step's with:,
// but for its sourceKeys, as format 1 drops them.
func (p *Parent) submitted(with map[string]any) map[string]any {
	if p == nil || with == nil {
		return with
	}
	out := maps.Clone(with)
	for _, k := range sourceKeys {
		delete(out, k)
	}
	return out
}

// held returns the names of the variables of Env that Private and
// Secrets name, which the secret record holds, and whether each is a
// secret, by name.
func (p *Parent) held() map[string]bool {
	out := map[string]bool{}
	for i, names := range [][]string{p.Private, p.Secrets} {
		for _, name := range names {
			if path := plan.SplitPath(name); len(path) == 2 && path[0] == "env" {
				out[path[1]] = i == 1
			}
		}
	}
	return out
}

// link returns what the plan holds of the step (plan.Run.Parent).
func (p *Parent) link() *plan.ParentRun {
	held := p.held()
	out := &plan.ParentRun{
		Run: p.Run, Job: p.Job, Step: p.Step, Try: p.Try, Root: p.Root, Depth: p.Depth,
		Private: slices.Sorted(slices.Values(p.Private)), Secrets: slices.Sorted(slices.Values(p.Secrets)),
	}
	for name, v := range p.Env {
		if _, ok := held[name]; ok {
			continue
		}
		if out.Env == nil {
			out.Env = map[string]string{}
		}
		out.Env[name] = v
	}
	return out
}

// inherited returns what the run's secret record holds of the step
// (plan.Record.Inherited), or nil for nothing.
func (p *Parent) inherited() *plan.Inherited {
	out := &plan.Inherited{Masked: slices.Clone(p.Masked)}
	for name := range p.held() {
		if out.Env == nil {
			out.Env = map[string]string{}
		}
		out.Env[name] = p.Env[name]
	}
	if out.Env == nil && out.Masked == nil {
		return nil
	}
	return out
}

// startParent returns the Parent that Start resolves a child run's inputs
// again with: what the plan names as private of what its step gave it.
func startParent(r *plan.ParentRun) *Parent {
	if r == nil {
		return nil
	}
	return &Parent{Private: r.Private, Secrets: r.Secrets}
}

// holdGiven keeps out of the plan what the step that calls the run gave it
// that is private (Options.Parent), which Create, and Start in turn, read
// as a variable's value, before any expression reads it: a part made from
// a secret as a secret, masked as what is made from one is, and the rest
// as private, as a value typed over a default made from them is
// (holdPath). A part of an input's value is held in that input's; one of
// a value for an input the workflow doesn't define, which a child run
// keeps as format 1 does, is held as it is. A password's value is a secret
// already, whatever made it.
func (r *inputResolver) holdGiven() {
	p := r.opts.Parent
	if p == nil {
		return
	}
	for _, g := range []struct {
		names []string
		kind  holding
	}{{p.Private, heldPrivate}, {p.Secrets, heldDerived}} {
		for _, name := range g.names {
			path := plan.SplitPath(name)
			if len(path) < 2 || path[0] != "inputs" {
				continue
			}
			path = path[1:]
			if _, ok := jsonptr.Lookup(r.values, path); !ok {
				continue
			}
			var in *field
			for n := len(path); n > 0 && in == nil; n-- {
				if f := r.fieldAt(path[:n]); f != nil {
					in = f.holder()
				}
			}
			if in != nil && in.typ == "password" {
				continue
			}
			r.holdPath(in, path, g.kind)
		}
	}
}
