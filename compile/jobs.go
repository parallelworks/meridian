package compile

import (
	"errors"
	"maps"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/parallelworks/meridian/expr"
	"github.com/parallelworks/meridian/internal/jsonptr"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
)

// The compiler checks each expression in the phase that evaluates it
// (plan.Phase), which decides what it can read (plan.Phase.Knows): a job's
// needs: in Create, its strategy: in Strategy, its if:, runs-on:,
// resources:, retry:, timeout: and ssh: in Ready, its env: in JobEnv, its
// steps in Step, and its outputs: in End. Its working-directory:, which
// Step evaluates, where the run's directory is known, is checked in Ready,
// so it reads no more than Ready knows. What it
// checks of the run as a whole is in run.

// jobInfo is what the checks of a job's expressions need to know about it.
type jobInfo struct {
	name  string
	loc   []string
	needs []plan.Need
	// needsExpr is the job's needs:, when it is an expression that read a
	// variable or a password, which Start evaluates again.
	needsExpr plan.Value
	// needed holds the names of the jobs it needs.
	needed map[string]bool
	// stepIDs maps its steps' ids to their indexes, and step is the index
	// of the step being checked, or the number of steps after them. steps
	// is the number of steps, and cleanup is set while a step's cleanup: is
	// checked, which runs once every step has, and reads each one's
	// outputs.
	stepIDs map[string]int
	step    int
	steps   int
	cleanup bool
	// matrix is set for a job with a strategy:, and matrixKeys holds its
	// matrix's keys when the workflow writes them out, and matrixNames
	// them sorted, once a reference reads a key it doesn't have.
	matrix      bool
	matrixKeys  map[string]bool
	matrixNames *nameList
	// earlier holds the ids of the steps before step earlierAt, or all of
	// them for a cleanup:, once a reference there reads a step that isn't
	// one.
	earlier   *nameList
	earlierAt int
}

// jobs compiles the workflow's jobs, in the order of their names.
func (c *compiler) jobs() []plan.Job {
	defs := mapping(c.doc.Value["jobs"])
	names := slices.Sorted(maps.Keys(defs))
	infos := make(map[string]*jobInfo, len(names))
	for _, name := range names {
		infos[name] = c.jobInfo(name, mapping(defs[name]))
	}
	c.jobInfos, c.jobNames = infos, newNameList(names)
	for _, name := range names {
		c.resolveNeeds(infos[name])
	}
	c.checkCycles(infos, names)
	jobs := make([]plan.Job, 0, len(names))
	for _, name := range names {
		jobs = append(jobs, c.job(infos[name]))
	}
	return jobs
}

func (c *compiler) jobInfo(name string, def map[string]any) *jobInfo {
	info := &jobInfo{
		name: name, loc: []string{"jobs", name}, needed: map[string]bool{},
		stepIDs: map[string]int{},
	}
	for i, s := range list(def["steps"]) {
		id := str(mapping(s)["id"])
		if id == "" {
			continue
		}
		if first, dup := info.stepIDs[id]; dup {
			c.reportAt(problems.DuplicateStepID, appendPath(info.loc, "steps", strconv.Itoa(i), "id"), problems.Params{
				"first": strconv.Itoa(first), "second": strconv.Itoa(i), "job": name, "id": id,
			})
			continue
		}
		info.stepIDs[id] = i
	}
	if strategy, ok := def["strategy"].(map[string]any); ok {
		info.matrix = true
		if m, ok := strategy["matrix"].(map[string]any); ok {
			info.matrixKeys = map[string]bool{}
			for k := range m {
				if k != "include" && k != "exclude" {
					info.matrixKeys[k] = true
				}
			}
			switch include := m["include"].(type) {
			case string:
				// An expression, whose keys aren't known.
				info.matrixKeys = nil
			case []any:
				for _, entry := range include {
					for k := range mapping(entry) {
						info.matrixKeys[k] = true
					}
				}
			}
		}
	}
	return info
}

// resolveNeeds reads a job's needs: one job's name, a list of them, or an
// expression of what is known when the run is created that gives either. A
// name followed by :any waits for any one instance of a matrix job. The
// expression can read a variable, since the plan holds only the jobs it
// names, but a problem with what it gives doesn't show it, and the plan
// keeps it for Start to evaluate again.
func (c *compiler) resolveNeeds(info *jobInfo) {
	loc := appendPath(info.loc, "needs")
	raw, ok := jsonptr.Lookup(c.doc.Value, loc)
	if !ok {
		return
	}
	fromVariable := false
	report := func(k *problems.Kind, params problems.Params) {
		if fromVariable {
			params = mask(params)
		}
		c.reportAt(k, loc, params)
	}
	if s, isString := raw.(string); isString {
		parsed, checked := c.checkString(loc, s, plan.PhaseCreate, info)
		if !checked {
			return
		}
		if t, isTemplate := parsed.(*expr.Template); isTemplate {
			// What decides the run's shape is one group with the inputs'
			// defaults and conditions, which shares their budget.
			v, marked, err := t.EvalMarked(c.inputs.shapeScope())
			if u, ok := errors.AsType[*expr.UnknownError](err); ok {
				c.reportAt(problems.NeedsNotKnown, loc, problems.Params{"name": u.Name})
				return
			}
			if err != nil {
				c.reportError(loc, 0, maskedIf(marked, err))
				return
			}
			raw, fromVariable = v, marked
			if marked {
				info.needsExpr = c.planned(loc, parsed)
			}
		}
	}
	var list []string
	switch v := raw.(type) {
	case string:
		list = []string{v}
	case []any:
		for _, item := range v {
			s, ok := item.(string)
			if !ok {
				report(problems.NeedsItemNotName, problems.Params{"value": problems.Show(item)})
				return
			}
			list = append(list, s)
		}
	default:
		report(problems.NeedsNotName, problems.Params{"value": problems.Show(raw)})
		return
	}
	for _, need := range list {
		job, anyInstance := strings.CutSuffix(need, ":any")
		switch {
		case job == info.name:
			report(problems.NeedsItself, problems.Params{"job": info.name})
		case c.jobInfos[job] == nil:
			s, ok := c.jobNames.suggest(job)
			report(problems.NeedsUnknownJob, problems.WithSuggestion(problems.Params{"job": info.name, "needed": job}, s, ok))
		case !info.needed[job]:
			info.needed[job] = true
			info.needs = append(info.needs, plan.Need{Job: job, Any: anyInstance})
		}
	}
}

// checkCycles reports jobs that need each other, once for each cycle.
func (c *compiler) checkCycles(infos map[string]*jobInfo, names []string) {
	const (
		unvisited = iota
		visiting
		done
	)
	state := map[string]int{}
	reported := map[string]bool{}
	var stack []string
	var visit func(name string)
	visit = func(name string) {
		state[name] = visiting
		stack = append(stack, name)
		for _, n := range infos[name].needs {
			switch state[n.Job] {
			case unvisited:
				visit(n.Job)
			case visiting:
				cycle := slices.Clone(stack[slices.Index(stack, n.Job):])
				key := strings.Join(slices.Sorted(slices.Values(cycle)), " ")
				if !reported[key] {
					reported[key] = true
					first := slices.Min(cycle)
					c.reportAt(problems.NeedsCycle, appendPath(infos[first].loc, "needs"),
						problems.Params{"jobs": problems.QuoteAll(slices.Sorted(slices.Values(cycle)))})
				}
			}
		}
		stack = stack[:len(stack)-1]
		state[name] = done
	}
	for _, name := range names {
		if state[name] == unvisited {
			visit(name)
		}
	}
}

func (c *compiler) job(info *jobInfo) plan.Job {
	at := func(keys ...string) []string { return appendPath(info.loc, keys...) }
	def := mapping(lookup(c.doc.Value, info.loc))
	j := plan.Job{Key: info.name, Name: info.name, Needs: info.needs, NeedsExpr: info.needsExpr, Path: jsonptr.Format(info.loc)}
	info.step = -1
	if _, ok := def["strategy"]; ok {
		j.Strategy = &plan.Strategy{
			Matrix:      c.value(at("strategy", "matrix"), plan.PhaseStrategy, info),
			FailFast:    c.value(at("strategy", "fail-fast"), plan.PhaseStrategy, info),
			MaxParallel: c.value(at("strategy", "max-parallel"), plan.PhaseStrategy, info),
		}
	}
	j.If = c.condition(at("if"), plan.PhaseReady, info)
	j.RunsOn = c.value(at("runs-on"), plan.PhaseReady, info)
	j.Resources = c.value(at("resources"), plan.PhaseReady, info)
	j.SSH = c.value(at("ssh"), plan.PhaseReady, info)
	c.checkDisconnectTimeout(at("ssh"))
	j.Timeout = c.value(at("timeout"), plan.PhaseReady, info)
	j.WorkingDirectory = c.value(at("working-directory"), plan.PhaseReady, info)
	if retry, ok := def["retry"].(map[string]any); ok {
		j.Retry = &plan.JobRetry{
			MaxRetries: c.value(at("retry", "max-retries"), plan.PhaseReady, info),
			Interval:   c.value(at("retry", "interval"), plan.PhaseReady, info),
		}
		for _, r := range list(retry["reasons"]) {
			j.Retry.Reasons = append(j.Retry.Reasons, str(r))
		}
	}
	j.Env = c.values(at("env"), plan.PhaseJobEnv, info)
	steps := list(def["steps"])
	info.steps = len(steps)
	j.Steps = make([]plan.Step, len(steps))
	for i := range steps {
		info.step = i
		j.Steps[i] = c.step(info, at("steps", strconv.Itoa(i)))
	}
	info.step = len(steps)
	j.Outputs = c.values(at("outputs"), plan.PhaseEnd, info)
	return j
}

func (c *compiler) step(info *jobInfo, loc []string) plan.Step {
	at := func(keys ...string) []string { return appendPath(loc, keys...) }
	def := mapping(lookup(c.doc.Value, loc))
	s := plan.Step{
		ID: str(def["id"]), Uses: str(def["uses"]), Shell: str(def["shell"]),
		EarlyCancel: str(def["early-cancel"]), Path: jsonptr.Format(loc),
	}
	s.Name = c.value(at("name"), plan.PhaseStep, info)
	s.If = c.condition(at("if"), plan.PhaseStep, info)
	s.Run = c.value(at("run"), plan.PhaseStep, info)
	s.With = c.value(at("with"), plan.PhaseStep, info)
	s.Env = c.values(at("env"), plan.PhaseStep, info)
	s.WorkingDirectory = c.value(at("working-directory"), plan.PhaseStep, info)
	s.SSH = c.value(at("ssh"), plan.PhaseStep, info)
	c.checkDisconnectTimeout(at("ssh"))
	s.Timeout = c.value(at("timeout"), plan.PhaseStep, info)
	s.IgnoreErrors = c.value(at("ignore-errors"), plan.PhaseStep, info)
	// A cleanup: runs once every step has, so it reads each one's outputs.
	info.cleanup = true
	s.Cleanup = c.value(at("cleanup"), plan.PhaseStep, info)
	info.cleanup = false
	if retry, ok := def["retry"].(map[string]any); ok {
		s.Retry = &plan.StepRetry{
			MaxRetries: c.value(at("retry", "max-retries"), plan.PhaseStep, info),
			Interval:   c.value(at("retry", "interval"), plan.PhaseStep, info),
			Timeout:    c.value(at("retry", "timeout"), plan.PhaseStep, info),
		}
		// clean-on-retry is true unless the workflow says otherwise.
		s.Retry.CleanOnRetry = plan.Literal(true)
		if _, ok := retry["clean-on-retry"]; ok {
			s.Retry.CleanOnRetry = c.value(at("retry", "clean-on-retry"), plan.PhaseStep, info)
		}
	}
	return s
}

// checkDisconnectTimeout warns of the disconnect-timeout of the format 1
// ssh: at loc, a job's or a step's that the workflow writes as a mapping,
// unless it is negative: Meridian doesn't honour it, and runs every step
// on a host as a negative one asks, through a lost connection
// (problems.SSHDisconnectTimeoutIgnored). One that an expression gives is
// warned of too, since what it gives isn't known yet. One that format 1
// can't read, and so fails the step for, is warned of as that
// (problems.SSHDisconnectTimeoutInvalid): Meridian ignores it, and runs
// the step.
func (c *compiler) checkDisconnectTimeout(loc []string) {
	ssh, _ := lookup(c.doc.Value, loc).(map[string]any)
	v, ok := ssh["disconnect-timeout"]
	if !ok {
		return
	}
	k := problems.SSHDisconnectTimeoutIgnored
	if s, isText := v.(string); !isText || !hasTemplate(s) {
		negative, valid := disconnectTimeout(v)
		switch {
		case !valid:
			k = problems.SSHDisconnectTimeoutInvalid
		case negative:
			return
		}
	}
	c.reportAt(k, appendPath(loc, "disconnect-timeout"), problems.Params{"value": problems.Show(v)})
}

// disconnectTimeout reads v, a disconnect-timeout as a workflow writes it,
// as format 1 reads one, as text: a number of seconds, or a whole number
// with one of d, h, m or s after it, with space around it or not, or
// nothing, for its default. It reports whether format 1 can read it, and
// whether it is negative, which never gives up: a number under 0, or a
// whole number under 0 with a unit, once multiplied out to nanoseconds as
// format 1 does, as -1, -5m or -2d. 1h30m, 1.5h and 300ms aren't read.
func disconnectTimeout(v any) (negative, valid bool) {
	switch v := v.(type) {
	case nil:
		return false, true
	case int64:
		return v < 0, true
	case float64:
		return v < 0, !math.IsNaN(v) && !math.IsInf(v, 0)
	case string:
		s := strings.TrimSpace(v)
		if s == "" {
			return false, true
		}
		if n, err := strconv.ParseFloat(s, 64); err == nil && !math.IsNaN(n) && !math.IsInf(n, 0) {
			return n < 0, true
		}
		if len(s) < 2 {
			return false, false
		}
		n, err := strconv.ParseInt(s[:len(s)-1], 10, 64)
		if err != nil {
			return false, false
		}
		// As format 1 multiplies, in int64, which can wrap.
		const second = int64(time.Second)
		switch s[len(s)-1] {
		case 'd':
			n *= 24 * 60 * 60 * second
		case 'h':
			n *= 60 * 60 * second
		case 'm':
			n *= 60 * second
		case 's':
			n *= second
		default:
			return false, false
		}
		return n < 0, true
	}
	return false, false
}
