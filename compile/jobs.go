package compile

import (
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"

	"github.com/parallelworks/meridian/expr"
	"github.com/parallelworks/meridian/format"
	"github.com/parallelworks/meridian/internal/hint"
	"github.com/parallelworks/meridian/internal/jsonptr"
	"github.com/parallelworks/meridian/plan"
)

// A phase is when a value is evaluated, which decides what it can read.
type phase int

const (
	// runCreated is when the run is created: the workflow's env, timeout,
	// sessions and links, and a job's needs.
	runCreated phase = iota
	// needsDone is when a job's dependencies have finished, before its
	// matrix is expanded: its strategy:.
	needsDone
	// jobReady is when a job, or a matrix instance of it, is ready to run:
	// its if:, runs-on:, resources:, retry:, timeout:, ssh: and
	// working-directory:.
	jobReady
	// jobEnv is when a job's env: is set, before its steps run.
	jobEnv
	// stepRuns is when a step is about to run.
	stepRuns
	// jobEnds is when a job ends, for its outputs.
	jobEnds
)

// has reports whether an expression evaluated in the phase can read a
// context, or "status" for the status functions.
func (p phase) has(context string) bool {
	switch context {
	case "inputs", "org", "vars", "remote", "sessions":
		return true
	case "needs", "status", "env":
		return p >= needsDone
	case "matrix":
		return p >= jobReady
	case "job":
		return p >= jobEnv
	case "steps":
		return p >= stepRuns
	}
	return false
}

func (p phase) when() string {
	switch p {
	case runCreated:
		return "when the run is created, which is when this is evaluated"
	case needsDone:
		return "in a job's strategy:, which is evaluated before its matrix is expanded"
	case jobReady:
		return "here, which is evaluated before the job's steps run"
	case jobEnv:
		return "in a job's env:, which is set before its steps run"
	}
	return "here"
}

// jobInfo is what the checks of a job's expressions need to know about it.
type jobInfo struct {
	name  string
	loc   []string
	needs []plan.Need
	// needed holds the names of the jobs it needs.
	needed map[string]bool
	// stepIDs maps its steps' ids to their indexes, and step is the index
	// of the step being checked, or the number of steps after them.
	stepIDs map[string]int
	step    int
	// matrix is set for a job with a strategy:, and matrixKeys holds its
	// matrix's keys when the workflow writes them out.
	matrix     bool
	matrixKeys map[string]bool
}

// jobs compiles the workflow's jobs, in the order of their names.
func (c *compiler) jobs() []plan.Job {
	defs := mapping(c.doc.Value["jobs"])
	names := slices.Sorted(maps.Keys(defs))
	infos := make(map[string]*jobInfo, len(names))
	for _, name := range names {
		infos[name] = c.jobInfo(name, mapping(defs[name]))
	}
	c.jobInfos = infos
	for _, name := range names {
		c.resolveNeeds(infos[name], names)
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
			c.reportAt(format.Error, CodeDuplicateID, appendPath(info.loc, "steps", strconv.Itoa(i), "id"),
				"steps %d and %d of job %s both have the id %s", first, i, name, id)
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
// name followed by :any waits for any one instance of a matrix job.
func (c *compiler) resolveNeeds(info *jobInfo, names []string) {
	loc := appendPath(info.loc, "needs")
	raw, ok := jsonptr.Lookup(c.doc.Value, loc)
	if !ok {
		return
	}
	if s, isString := raw.(string); isString {
		parsed, checked := c.checkString(loc, s, runCreated, info)
		if !checked {
			return
		}
		if t, isTemplate := parsed.(*expr.Template); isTemplate {
			v, err := t.Eval(expr.Scope{Contexts: compileTimeContexts(c.opts, c.inputs.evalValues())})
			if err != nil {
				c.reportAt(format.Error, CodeNeeds, loc, "needs must be known when the run is created: %v", err)
				return
			}
			raw = v
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
				c.reportAt(format.Error, CodeNeeds, loc, "needs lists jobs' names, not %s", describe(item))
				return
			}
			list = append(list, s)
		}
	default:
		c.reportAt(format.Error, CodeNeeds, loc, "needs is a job's name or a list of them, not %s", describe(raw))
		return
	}
	for _, need := range list {
		job, anyInstance := strings.CutSuffix(need, ":any")
		switch {
		case job == info.name:
			c.reportAt(format.Error, CodeNeeds, loc, "job %s needs itself", info.name)
		case !slices.Contains(names, job):
			c.reportAt(format.Error, CodeNeeds, loc, "%s",
				withSuggestion(fmt.Sprintf("job %s needs %s, which isn't a job of this workflow", info.name, job), job, names))
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
					c.reportAt(format.Error, CodeNeeds, appendPath(infos[first].loc, "needs"),
						"jobs %s need each other", hint.JoinList(slices.Sorted(slices.Values(cycle)), "and"))
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
	j := plan.Job{Key: info.name, Name: info.name, Needs: info.needs, Path: jsonptr.Format(info.loc)}
	info.step = -1
	if _, ok := def["strategy"]; ok {
		j.Strategy = &plan.Strategy{
			Matrix:      c.value(at("strategy", "matrix"), needsDone, info),
			FailFast:    c.value(at("strategy", "fail-fast"), needsDone, info),
			MaxParallel: c.value(at("strategy", "max-parallel"), needsDone, info),
		}
	}
	j.If = c.condition(at("if"), jobReady, info)
	j.RunsOn = c.value(at("runs-on"), jobReady, info)
	j.Resources = c.value(at("resources"), jobReady, info)
	j.SSH = c.value(at("ssh"), jobReady, info)
	j.Timeout = c.value(at("timeout"), jobReady, info)
	j.WorkingDirectory = c.value(at("working-directory"), jobReady, info)
	if retry, ok := def["retry"].(map[string]any); ok {
		j.Retry = &plan.JobRetry{
			MaxRetries: c.value(at("retry", "max-retries"), jobReady, info),
			Interval:   c.value(at("retry", "interval"), jobReady, info),
		}
		for _, r := range list(retry["reasons"]) {
			j.Retry.Reasons = append(j.Retry.Reasons, str(r))
		}
	}
	j.Env = c.values(at("env"), jobEnv, info)
	steps := list(def["steps"])
	j.Steps = make([]plan.Step, len(steps))
	for i := range steps {
		info.step = i
		j.Steps[i] = c.step(info, at("steps", strconv.Itoa(i)))
	}
	info.step = len(steps)
	j.Outputs = c.values(at("outputs"), jobEnds, info)
	return j
}

func (c *compiler) step(info *jobInfo, loc []string) plan.Step {
	at := func(keys ...string) []string { return appendPath(loc, keys...) }
	def := mapping(lookup(c.doc.Value, loc))
	s := plan.Step{
		ID: str(def["id"]), Uses: str(def["uses"]), Shell: str(def["shell"]),
		EarlyCancel: str(def["early-cancel"]), Path: jsonptr.Format(loc),
	}
	s.Name = c.value(at("name"), stepRuns, info)
	s.If = c.condition(at("if"), stepRuns, info)
	s.Run = c.value(at("run"), stepRuns, info)
	s.With = c.value(at("with"), stepRuns, info)
	s.Env = c.values(at("env"), stepRuns, info)
	s.WorkingDirectory = c.value(at("working-directory"), stepRuns, info)
	s.SSH = c.value(at("ssh"), stepRuns, info)
	s.Timeout = c.value(at("timeout"), stepRuns, info)
	s.IgnoreErrors = c.value(at("ignore-errors"), stepRuns, info)
	s.Cleanup = c.value(at("cleanup"), stepRuns, info)
	if retry, ok := def["retry"].(map[string]any); ok {
		s.Retry = &plan.StepRetry{
			MaxRetries: c.value(at("retry", "max-retries"), stepRuns, info),
			Interval:   c.value(at("retry", "interval"), stepRuns, info),
			Timeout:    c.value(at("retry", "timeout"), stepRuns, info),
		}
		s.Retry.CleanOnRetry, _ = retry["clean-on-retry"].(bool)
	}
	return s
}
