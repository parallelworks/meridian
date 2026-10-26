package compile

import (
	"maps"
	"slices"
	"strings"

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
// for an input the workflow doesn't define is. A record that lacks a value
// the plan names, as a password, is the run's problem (input_required),
// as it is when the run is created without it: the plan can name a value
// that has none as the run starts, as a list's whose default gives no
// rows.
func (st *starter) checkFits() error {
	r, p := st.r, st.p
	named := setOf(p.Run.Secrets)
	maps.Copy(named, setOf(p.Run.Private))
	planned, _ := p.Run.Inputs.Get().(map[string]any)
	for _, kept := range []map[string]any{r.secrets, r.private} {
		for k := range kept {
			path := slices.Concat([]string{"inputs"}, plan.SplitPath(k))
			if _, ok := planned[path[1]]; !ok && r.fieldAt(path[1:2]) != nil {
				continue
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
