package compile

import (
	"encoding/json/v2"
	"maps"
	"slices"
	"strconv"
	"strings"

	"github.com/parallelworks/meridian/expr"
	"github.com/parallelworks/meridian/internal/jsonptr"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
)

// What Start checks once it has resolved the inputs again: that the
// secret record fits the plan, and that the run's shape is what Create
// decided.

// recordFor returns what the secret record holds, given, for each of the
// names the plan gives, by them, or an error if it holds another, or none
// for one: a record that doesn't fit the plan (ErrMismatch).
func recordFor[V any](names []string, given map[string]V, what string) (map[string]V, error) {
	out := make(map[string]V, len(names))
	for _, name := range names {
		v, ok := given[name]
		if !ok {
			return nil, mismatch("the secret record holds no %s %s", what, name)
		}
		out[name] = v
	}
	for name := range given {
		if _, ok := out[name]; !ok {
			return nil, mismatch("the plan names no %s %s", what, name)
		}
	}
	return out, nil
}

// checkFits returns an error (ErrMismatch) if what was submitted doesn't
// fit the plan: a value the resolution kept out of the plan, as a secret
// or as private, that the plan doesn't name, nor a value it is in, as a
// list whose rows are known only as the run starts. Of an input whose
// value the plan holds none of, as Create kept it out whole, the plan
// doesn't say which parts are kept out, so those aren't checked; a value
// for an input the workflow doesn't define is, and so is a password
// input's, which the plan names whenever the run has one. A record that
// lacks a value the plan names, as a password, is the run's problem
// (input_required), as it is when the run is created without it: the plan
// can name a value that has none as the run starts, as a list's whose
// default gives no rows.
func (st *starter) checkFits() error {
	r, p := st.r, st.p
	named := setOf(p.Run.Secrets)
	maps.Copy(named, setOf(p.Run.Private))
	planned, _ := p.Run.Inputs.Get().(map[string]any)
	for _, kept := range []map[string]any{r.secrets, r.private} {
		for k := range kept {
			path := slices.Concat([]string{"inputs"}, plan.SplitPath(k))
			if _, ok := planned[path[1]]; !ok && r.fieldAt(path[1:2]) != nil {
				if f := r.fieldAt(path[1:]); f == nil || f.typ != "password" || f.in != nil {
					continue
				}
			}
			found := false
			for n := len(path); n > 1 && !found; n-- {
				found = named[plan.JoinPath(path[:n]...)]
			}
			if !found {
				return mismatch("the plan names no value inputs.%s, which what was submitted holds", k)
			}
		}
	}
	return nil
}

// checkPlanned returns an error (ErrMismatch) if the values Start resolved
// from what was submitted aren't those the plan holds (plan.Run.Inputs),
// where Create would have held them: that means what was submitted isn't
// what the run was created from. It compares all of the values but what
// Create kept out of the plan, which Start finds as it resolves them: each
// input whose value Start made from a variable or a password, all of it,
// as Create keeps it out whole; the passwords, the values kept as secrets
// or as private, held, or taken for passwords; and the parts that mean
// their expressions (plan.Record.Means). Nor does it compare an input of a
// platform's type, which the Inputs resolver gives again as the run starts,
// and can give otherwise. Where an input's definition reads the workflow's
// env or the sessions, which Create doesn't know, and Start doesn't mark,
// it doesn't compare the values at all, as which of them Create kept out it
// can't tell; no known workflow has one.
func (st *starter) checkPlanned() error {
	r, p := st.r, st.p
	if st.readsUnmarked() {
		return nil
	}
	var out [][]string
	for _, made := range []map[string]bool{st.s.madePrivate, st.s.madeSecret} {
		for name := range made {
			path := plan.SplitPath(name)
			if path[0] != "inputs" || len(path) < 2 {
				continue
			}
			// The input whose value it is in, all of it, as a list's is for
			// the fields of its rows.
			for n := len(path); n > 1; n-- {
				if f := r.fieldAt(path[1:n]); f != nil {
					out = append(out, f.holder().path)
					break
				}
			}
		}
	}
	for _, kept := range []map[string]any{r.secrets, r.private} {
		for k := range kept {
			out = append(out, plan.SplitPath(k))
		}
	}
	for name := range r.means {
		if path := plan.SplitPath(name); len(path) > 1 {
			out = append(out, path[1:])
		}
	}
	for _, h := range r.held {
		out = append(out, h.path)
	}
	out = append(out, r.unknownSecrets...)
	for _, f := range r.passwords {
		out = append(out, f.path)
	}
	for _, f := range r.fields {
		if f.in == nil && platformTypes[f.typ] {
			out = append(out, f.path)
		}
	}
	skip := skipped{}
	for _, path := range out {
		skip.add(path)
	}
	planned, _ := p.Run.Inputs.Get().(map[string]any)
	// The inputs whose values differ, by name, never what they are: one the
	// plan holds that Start gives otherwise, or not at all, and one the
	// workflow doesn't define that Start gives and the plan doesn't hold.
	// Another that the plan doesn't hold Create kept out of it, all of it,
	// as one that read what wasn't known yet.
	var differ []string
	for k, q := range planned {
		if under, ok := skip.under(k); ok {
			if g, given := r.values[k]; !given || !sameExcept(g, q, under) {
				differ = append(differ, plan.JoinPath("inputs", k))
			}
		}
	}
	for k := range r.values {
		if _, ok := planned[k]; !ok && r.fieldAt([]string{k}) == nil {
			if _, compared := skip.under(k); compared {
				differ = append(differ, plan.JoinPath("inputs", k))
			}
		}
	}
	if len(differ) > 0 {
		slices.Sort(differ)
		return mismatch("what was submitted doesn't give the values the plan holds of %s", strings.Join(differ, ", "))
	}
	return nil
}

// readsUnmarked reports whether an expression of the input definitions
// reads the workflow's env or the sessions, which Create doesn't know, or
// may not, and so evaluates once the run starts, where Start marks neither
// as it marks a variable or a password.
func (st *starter) readsUnmarked() bool {
	exprs, _ := st.r.workflow.Exprs(definitionsPath)
	return slices.ContainsFunc(exprs, func(e *expr.Expr) bool {
		return slices.ContainsFunc(e.Refs(), func(ref expr.Ref) bool { return ref.Context == "env" || ref.Context == "sessions" })
	})
}

// skipped holds paths in values, by their keys: a key whose value is nil
// skips all that is under it.
type skipped map[string]skipped

// add skips what is at path, and under it.
func (s skipped) add(path []string) {
	for i, k := range path {
		next, ok := s[k]
		switch {
		case ok && next == nil:
			return // What is under it is skipped already.
		case i == len(path)-1:
			s[k] = nil
			return
		case !ok:
			next = skipped{}
			s[k] = next
		}
		s = next
	}
}

// under returns what is skipped under key, and false if all of it is.
func (s skipped) under(key string) (skipped, bool) {
	next, ok := s[key]
	return next, !ok || next != nil
}

// sameExcept reports whether a, what Start resolved, and b, what the plan
// holds, are the same values but for what skip holds, each number as the
// plan writes it: a list's item that skip holds is the same whatever it
// is, and so is a mapping's key, whether either holds it or not.
func sameExcept(a, b any, skip skipped) bool {
	switch b := b.(type) {
	case map[string]any:
		m, ok := a.(map[string]any)
		if !ok {
			return false
		}
		for k, v := range b {
			if under, compared := skip.under(k); compared {
				if w, ok := m[k]; !ok || !sameExcept(w, v, under) {
					return false
				}
			}
		}
		for k := range m {
			if _, ok := b[k]; !ok {
				if _, compared := skip.under(k); compared {
					return false
				}
			}
		}
		return true
	case []any:
		l, ok := a.([]any)
		if !ok || len(l) != len(b) {
			return false
		}
		for i := range b {
			if under, compared := skip.under(strconv.Itoa(i)); compared && !sameExcept(l[i], b[i], under) {
				return false
			}
		}
		return true
	}
	switch a.(type) {
	case map[string]any, []any:
		return false
	}
	return a == b || canonicalJSON(a) == canonicalJSON(b)
}

// canonicalJSON returns v's JSON, with each number as the plan writes it,
// so that what was read back from the plan and what Start resolved compare.
func canonicalJSON(v any) string {
	data, err := json.Marshal(v, json.Deterministic(true))
	if err != nil {
		return ""
	}
	var back any
	if err := json.Unmarshal(data, &back); err != nil {
		return ""
	}
	data, _ = json.Marshal(back, json.Deterministic(true))
	return string(data)
}

// conditionKeys are an input's conditions, in the order the compiler
// evaluates them (applyConditions).
var conditionKeys = []string{"hidden", "ignore", "optional"}

// checkDecided reports run_shape_changed where a condition of the run's
// shape that read a variable or a password as the run was created
// (plan.Run.Decisions) decided otherwise as it starts than the secret
// record says it did, decided. One the resolution didn't decide, as of an
// input in a group it ignored, isn't checked: the group's own is. A
// decision of no input's condition is an error (ErrMismatch).
func (st *starter) checkDecided(decided map[string]bool) error {
	r := st.r
	byLoc := make(map[string]*field, len(r.fields))
	for _, f := range r.fields {
		if _, ok := byLoc[jsonptr.Format(f.loc)]; !ok {
			byLoc[jsonptr.Format(f.loc)] = f
		}
	}
	for _, pointer := range st.p.Run.Decisions {
		loc, err := jsonptr.Split(pointer)
		if err != nil || len(loc) <= len(definitionsPath) || byLoc[jsonptr.Format(loc[:len(loc)-1])] == nil || !slices.Contains(conditionKeys, loc[len(loc)-1]) {
			return mismatch("the plan's decision %s is of no input's condition", pointer)
		}
		if got, ok := r.decided[pointer]; ok && got != decided[pointer] {
			f := byLoc[jsonptr.Format(loc[:len(loc)-1])]
			st.report(problems.RunShapeChanged, pointer, problems.Params{"key": loc[len(loc)-1], "name": f.label()})
		}
	}
	return nil
}

// checkNeeds evaluates again each job's needs: that read a variable or a
// password as the run was created (plan.Job.NeedsExpr), and reports
// run_shape_changed where one names other jobs than the plan's. It reads
// the inputs as the compiler's needs: read them.
func (st *starter) checkNeeds() {
	r := st.r
	scope := r.scope()
	jobs := map[string]bool{}
	for _, j := range st.p.Jobs {
		jobs[j.Name] = true
	}
	for _, j := range st.p.Jobs {
		if j.NeedsExpr.IsZero() {
			continue
		}
		pointer := jsonptr.Append(j.Path, "needs")
		v, read, err := j.NeedsExpr.EvalRead(scope)
		if err != nil {
			loc, _ := jsonptr.Split(pointer)
			st.reportEval(loc, err, read)
			continue
		}
		if !sameNeeds(v, j, jobs) {
			st.report(problems.RunShapeChanged, pointer, problems.Params{"key": "needs", "name": j.Name})
		}
	}
}

// sameNeeds reports whether v, what a job's needs: gives, is what j needs:
// the same jobs, each on all its instances or any one, as the compiler
// reads them (resolveNeeds), in any order. jobs holds the names of the
// run's jobs.
func sameNeeds(v any, j plan.Job, jobs map[string]bool) bool {
	var names []any
	switch v := v.(type) {
	case string:
		names = []any{v}
	case []any:
		names = v
	default:
		return false
	}
	got := map[string]bool{}
	for _, item := range names {
		s, ok := item.(string)
		if !ok {
			return false
		}
		job, anyInstance := strings.CutSuffix(s, ":any")
		if job == j.Name || !jobs[job] {
			return false
		}
		if _, seen := got[job]; !seen {
			got[job] = anyInstance
		}
	}
	if len(got) != len(j.Needs) {
		return false
	}
	for _, n := range j.Needs {
		if anyInstance, ok := got[n.Job]; !ok || anyInstance != n.Any {
			return false
		}
	}
	return true
}
