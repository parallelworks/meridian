package plan

import (
	"slices"
	"strconv"
	"strings"

	"github.com/parallelworks/meridian/internal/jsonptr"
	"github.com/parallelworks/meridian/internal/jsonvalue"
)

// A CancelPath is what becomes of a job when its run is cancelled, or
// passes its own timeout:, as ACTIVATE has it: of one that hasn't
// started, and of one placed or running (Plan.CancelPath).
//
// A job placed or running runs on when its path is CancelPathOutlive, or
// the ReadyJob Ready gave it has IfAlways set, as it has when its if:'s
// value was the word always: ACTIVATE runs such a job on a context the
// run's cancellation and timeout don't reach. So the coordinator neither
// asks its attempt to stop (workerapi.Control.Cancel) nor withdraws its
// placement, and doesn't set its steps' JobState.Cancelled. It asks any
// other job's attempt to stop, and withdraws its placement. What stops
// one job of the run, as fail-fast: stops a matrix's other instances,
// stops it whatever its path. The coordinator does all of this; nothing
// in this package does.
type CancelPath int

const (
	// CancelPathSkip is the path of a job whose if: is unset, or reads
	// needs only as text, and neither env, an input whose value reads env,
	// nor a status function, and whose value isn't the word always. It
	// ends at once, without waiting for the jobs it needs, skipped because
	// they failed (ReadyResult.NeedsFailed): the coordinator has Ready
	// ready it at once, with the needs that have finished, which gives
	// that, or the job's problems if its if: fails to evaluate. A job that
	// needs none doesn't wait, so it is readied as ever.
	CancelPathSkip CancelPath = iota
	// CancelPathEnd is the path of a job whose if: reads what ACTIVATE
	// knows only once the jobs the job needs have finished, an output of
	// one, needs.<job>.result, env or an input whose value reads env, or
	// calls a status function, but can't be true once the run is
	// cancelled. It waits for the jobs it needs, and then Ready gives
	// Cancelled, unless the if:'s value is the word always, when it is
	// readied as ever, and runs on.
	CancelPathEnd
	// CancelPathOutlive is the path of a job whose if: can be true once the
	// run is cancelled: the word always, or one that calls always() or
	// cancelled(), or negates success() or failure(), or one of
	// CancelPathSkip's whose value is the word always. It waits for the
	// jobs it needs, and then is readied as ever, and runs on: the run's
	// cancellation doesn't stop it, before or after it starts, nor does the
	// run's timeout:.
	CancelPathOutlive
)

// CancelPath returns the path that the job named job, or its instance
// instance, takes once its run is cancelled or passes its own timeout:,
// whether it has started or not (CancelPath), so that the coordinator
// knows it before the jobs the job needs have finished. run is the run's
// values, from Start; instance is nil for a job without a strategy:, and
// for a matrix job that Ready is to expand. CancelPath reaches nothing,
// and the same plan and arguments give the same path, but where evaluating
// the if: runs out of the time the language gives an evaluation, which
// fails it. A job the plan doesn't have is the caller's mistake, and an
// error.
//
// ACTIVATE leaves an if: that reads an output of a job or a step, or env,
// or an input whose value reads env, or calls a status function, to its
// executor, which evaluates it once the job's needs have finished; it
// evaluates any other if: before, and then stops waiting for the needs of
// a job whose if: can't outlive the cancel. An input's value reads env
// when the run wasn't given it, and its default reads env, as the plan's
// values (Run.Inputs) say: Start evaluates such a default. Any other if:
// is evaluated here with run, and instance's matrix values, as Ready
// evaluates it, to see whether its value is the word always; one that
// reads matrix, asked about without an instance, and one that fails to
// evaluate, isn't.
func (p *Plan) CancelPath(job string, run RunValues, instance *Instance) (CancelPath, error) {
	j, err := p.job(job)
	if err != nil {
		return 0, err
	}
	shape := p.cancelShape(j)
	if shape != CancelPathSkip || len(j.If.Exprs()) == 0 || instance == nil && readsContext(j.If, "matrix") {
		return shape, nil
	}
	c := Contexts{Run: run}
	if instance != nil {
		c.Matrix = instance.Matrix
	}
	if d, ok := newEvaluation(PhaseReady, c).condition(j.If, at(j.Path, "if")); ok && d.always {
		return CancelPathOutlive, nil
	}
	return CancelPathSkip, nil
}

// cancelShape returns the path the job j's if: gives it by what it reads,
// as CancelPath does before it evaluates an if: of CancelPathSkip's.
func (p *Plan) cancelShape(j *Job) CancelPath {
	if v, ok := j.If.Literal(); ok && v == "always" {
		return CancelPathOutlive
	}
	used, afterCancel := statusChecks(j.If)
	switch {
	case afterCancel:
		return CancelPathOutlive
	case used:
		return CancelPathEnd
	}
	defs, _ := p.Run.Definitions.v.(map[string]any)
	later := &laterInputs{defs: defs, values: p.Run.Inputs.Get(), seen: map[string]bool{}}
	for _, e := range j.If.Exprs() {
		for _, r := range e.Refs() {
			switch {
			case r.Context == "env", r.Context == "needs" && !ReadsAsText(r.Context, r.Path):
				return CancelPathEnd
			case r.Context == "inputs" && later.reads(r.Path):
				return CancelPathEnd
			}
		}
	}
	return CancelPathSkip
}

// laterContexts are the contexts that an input's value can read only once
// a job runs, as the compiler names them: env above all, which is the only
// one an input definition may read.
var laterContexts = map[string]bool{"env": true, "needs": true, "steps": true, "job": true, "matrix": true}

// laterInputs finds the inputs whose values read one of laterContexts, as
// the compiler finds them: defs are the run's input definitions
// (Run.Definitions), and values the inputs' values the plan holds
// (Run.Inputs), which Start completes. seen holds the JSON Pointers of the
// definitions and the values read already.
type laterInputs struct {
	defs   map[string]any
	values any
	seen   map[string]bool
}

// reads reports whether the value of the input at path reads one of
// laterContexts: one the plan holds no value at path for, whose default
// does, or one of whose items' or rows' does, or the value of an input it
// reads in turn. ACTIVATE leaves such a value to its executor, and so an
// expression that reads it, while a value the run was given reads nothing.
// Of a list's rows, or a repeated wizard step's, it reads only what gives
// the field path names: that field's definition in the rows' template, and
// its place in each default row.
func (l *laterInputs) reads(path []string) bool {
	if len(path) == 0 || path[0] == "*" || path[0] == "[index]" {
		return false
	}
	if _, held := jsonptr.Lookup(l.values, path); held {
		return false
	}
	def, loc, n := inputDefinition(l.defs, path)
	if def == nil {
		return false
	}
	seen := l.seen
	// reads reports whether the expressions in v, a value, read one.
	reads := func(v any) bool {
		for _, e := range Literal(v).Exprs() {
			for _, r := range e.Refs() {
				if laterContexts[r.Context] || r.Context == "inputs" && l.reads(r.Path) {
					return true
				}
			}
		}
		return false
	}
	// walk reads the defaults in def, a definition at loc, and in the
	// definitions it holds.
	var walk func(def any, loc []string) bool
	walk = func(def any, loc []string) bool {
		m, ok := def.(map[string]any)
		pointer := jsonptr.Format(loc)
		if !ok || seen[pointer] {
			return false
		}
		seen[pointer] = true
		for k, x := range jsonvalue.Sorted(m) {
			if k == "default" && reads(x) || k != "default" && walk(x, append(slices.Clip(loc), k)) {
				return true
			}
		}
		return false
	}
	typ, _ := def["type"].(string)
	rows := "template"
	if typ == "step" {
		rows = "options"
	}
	if rest := path[n:]; len(rest) >= 2 && (typ == "list" || typ == "step" && def["multi"] == true) {
		// A field of the rows: its definition, and its value in each
		// default row.
		key := rest[1]
		template, _ := def[rows].(map[string]any)
		if walk(template[key], append(slices.Clip(loc), rows, key)) {
			return true
		}
		defaults, _ := def["default"].([]any)
		for i, row := range defaults {
			pointer := jsonptr.Format(append(slices.Clip(loc), "default", strconv.Itoa(i), key))
			if seen[pointer] {
				continue
			}
			seen[pointer] = true
			if m, ok := row.(map[string]any); ok && reads(m[key]) {
				return true
			}
		}
		return false
	}
	return walk(def, loc)
}

// inputDefinition returns the input definition, among defs, whose value
// is at the longest prefix of path that one's is, where it is among them,
// and that prefix's length, as the compiler places the inputs' values: a
// group's or a wizard step's inputs' values are under its key, unless it
// flattens them beside it, and a list's rows are its value's. It returns
// nil if none is.
func inputDefinition(defs map[string]any, path []string) (def map[string]any, loc []string, n int) {
	var visit func(defs map[string]any, at, valuePath []string)
	visit = func(defs map[string]any, at, valuePath []string) {
		flattenSteps := wizardFlattens(defs)
		for key, v := range jsonvalue.Sorted(defs) {
			d, ok := v.(map[string]any)
			if !ok || strings.HasPrefix(key, "$") {
				continue
			}
			here, place := append(slices.Clip(valuePath), key), append(slices.Clip(at), key)
			if len(here) > n && len(here) <= len(path) && slices.Equal(here, path[:len(here)]) {
				def, loc, n = d, place, len(here)
			}
			typ, _ := d["type"].(string)
			inner := here
			switch items, hasItems := d["items"].(map[string]any); {
			case typ == "step":
				options, ok := d["options"].(map[string]any)
				if !ok || d["multi"] == true {
					continue
				}
				if flattenSteps {
					inner = valuePath
				}
				visit(options, append(slices.Clip(place), "options"), inner)
			case hasItems && typ != "list":
				if d["flatten"] == true {
					inner = valuePath
				}
				visit(items, append(slices.Clip(place), "items"), inner)
			}
		}
	}
	visit(defs, nil, nil)
	return def, loc, n
}

// wizardFlattens reports whether the wizard steps among defs have their
// inputs' values beside their own: unless $meta.wizard.flatten is false.
func wizardFlattens(defs map[string]any) bool {
	meta, _ := defs["$meta"].(map[string]any)
	wizard, _ := meta["wizard"].(map[string]any)
	return wizard["flatten"] != false
}
