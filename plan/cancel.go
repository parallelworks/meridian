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
// reads in turn; or one the plan holds part of, a group, a wizard step, a
// list or one of its rows, a part of which it holds no value for reads one
// so. ACTIVATE leaves such a value to its executor, and so an expression
// that reads it, while a value the run was given reads nothing, nor does
// the key of a group or a wizard step that flattens its inputs beside it.
// Of a list's rows, or a repeated wizard step's, it reads only what gives
// the field path names: that field's definition in the rows' template, and
// its place in each default row.
func (l *laterInputs) reads(path []string) bool {
	if len(path) == 0 || path[0] == "*" || path[0] == "[index]" {
		return false
	}
	in, n, ok := findInput(l.defs, path)
	switch {
	case !ok:
		return false
	case n == len(path) && in.container && in.flatten:
		// Its inputs' values are beside its key, which holds none of them.
		return false
	}
	if v, held := jsonptr.Lookup(l.values, path); held {
		return l.holds(in, n, path, v)
	}
	def, loc := in.def, in.loc
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
	if rows, ok := rowsKey(def); ok && len(path) >= n+2 {
		// A field of the rows: its definition, and its value in each
		// default row.
		key := path[n+1]
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

// holds reports whether v, the value the plan holds at path, of the input
// in, whose own value is at path's first n keys, has a part the plan
// holds no value for that reads one of laterContexts, as Start completes
// the run's values: an input of a group or a wizard step read whole, or a
// field of a list's row, of the list read whole or the row. Any other
// value the plan holds is the run's, and reads nothing.
func (l *laterInputs) holds(in definedInput, n int, path []string, v any) bool {
	rows, isRows := rowsKey(in.def)
	template, _ := in.def[rows].(map[string]any)
	fields := func(row []string) bool {
		for _, f := range definedInputs(template, nil, row) {
			if l.reads(f.path) {
				return true
			}
		}
		return false
	}
	switch v := v.(type) {
	case map[string]any:
		switch {
		case n == len(path) && in.container:
			inner, loc, at := in.inner()
			for _, member := range definedInputs(inner, loc, at) {
				if l.reads(member.path) {
					return true
				}
			}
		case n == len(path)-1 && isRows:
			return fields(path)
		}
	case []any:
		if n == len(path) && isRows {
			for i := range v {
				if fields(append(slices.Clip(path), strconv.Itoa(i))) {
					return true
				}
			}
		}
	}
	return false
}

// A definedInput is an input definition, where it is among the run's
// input definitions, and where its value is, as the compiler places it.
type definedInput struct {
	def       map[string]any
	typ       string
	loc, path []string
	// container is set for a group or a wizard step that holds other
	// inputs, and flatten for one whose inputs' values are beside its own
	// key rather than under it.
	container, flatten bool
}

// definedInputs returns the input definitions among defs, which are at
// loc, with their values at path, in the order of their keys, without those
// they hold: a group's or a wizard step's inputs' values are under its
// key, unless it flattens them beside it, and a list's rows are its
// value's.
func definedInputs(defs map[string]any, loc, path []string) []definedInput {
	var out []definedInput
	flattenSteps := wizardFlattens(defs)
	for key, v := range jsonvalue.Sorted(defs) {
		def, ok := v.(map[string]any)
		if !ok || strings.HasPrefix(key, "$") {
			continue
		}
		typ, _ := def["type"].(string)
		in := definedInput{def: def, typ: typ, loc: slices.Concat(loc, []string{key}), path: slices.Concat(path, []string{key})}
		switch _, hasItems := def["items"].(map[string]any); {
		case typ == "header":
		case typ == "step":
			if _, ok := def["options"].(map[string]any); ok && def["multi"] != true {
				in.container, in.flatten = true, flattenSteps
			}
		case hasItems && typ != "list":
			in.container, in.flatten = true, def["flatten"] == true
		}
		out = append(out, in)
	}
	return out
}

// inner returns the input definitions a container holds, where they are,
// and where their values are; nil for any other input.
func (in definedInput) inner() (defs map[string]any, loc, path []string) {
	if !in.container {
		return nil, nil, nil
	}
	key := "items"
	if in.typ == "step" {
		key = "options"
	}
	defs, _ = in.def[key].(map[string]any)
	path = in.path
	if in.flatten {
		path = path[:len(path)-1]
	}
	return defs, slices.Concat(in.loc, []string{key}), path
}

// findInput returns the input definition, among defs and those they hold,
// whose value is at the longest prefix of path that one's is, and that
// prefix's length; of two, the first. ok is false if none is.
func findInput(defs map[string]any, path []string) (found definedInput, n int, ok bool) {
	var visit func(defs map[string]any, loc, at []string)
	visit = func(defs map[string]any, loc, at []string) {
		for _, in := range definedInputs(defs, loc, at) {
			if len(in.path) > n && len(in.path) <= len(path) && slices.Equal(in.path, path[:len(in.path)]) {
				found, n, ok = in, len(in.path), true
			}
			if inner, innerLoc, innerPath := in.inner(); inner != nil {
				visit(inner, innerLoc, innerPath)
			}
		}
	}
	visit(defs, nil, nil)
	return found, n, ok
}

// rowsKey returns the key of the definitions of the inputs that each row of
// def's value is a group of: template for a list, and options for a
// repeated wizard step. ok is false for any other input.
func rowsKey(def map[string]any) (key string, ok bool) {
	switch {
	case def["type"] == "list":
		return "template", true
	case def["type"] == "step" && def["multi"] == true:
		return "options", true
	}
	return "", false
}

// wizardFlattens reports whether the wizard steps among defs have their
// inputs' values beside their own: unless $meta.wizard.flatten is false.
func wizardFlattens(defs map[string]any) bool {
	meta, _ := defs["$meta"].(map[string]any)
	wizard, _ := meta["wizard"].(map[string]any)
	return wizard["flatten"] != false
}
