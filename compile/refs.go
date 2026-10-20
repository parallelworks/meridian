package compile

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/parallelworks/meridian/expr"
	"github.com/parallelworks/meridian/format"
)

// definitionKeys are keys of an input's definition, which inputs.x.type and
// the like read when x's value has none.
var definitionKeys = map[string]bool{
	"type": true, "label": true, "default": true, "hidden": true, "ignore": true, "optional": true,
	"description": true, "tooltip": true, "options": true, "items": true, "min": true, "max": true,
}

// checkRef checks a reference that an expression evaluated in phase ph
// makes, in job (nil outside jobs), and reports at pos, in the string at loc,
// what is wrong with it. A reference to something the workflow doesn't
// define is a warning, as ACTIVATE reads it as null or "" and runs the
// workflow, unless ACTIVATE fails the step or job that reads it. checkRef
// returns false if the reference is an error.
func (c *compiler) checkRef(r expr.Ref, ph phase, job *jobInfo, pos format.Position, loc []string) bool {
	if !ph.has(r.Context) {
		c.report(format.Error, pos, CodeContext, loc, "%s isn't known %s", r.Context, ph.when())
		return false
	}
	path := r.Path
	if len(path) == 0 || path[0] == "*" || path[0] == "[index]" {
		return true
	}
	// problem reports msg, if there is one.
	problem := func(severity format.Severity, msg string) bool {
		if msg == "" {
			return true
		}
		c.report(severity, pos, CodeReference, loc, "%s", msg)
		return severity != format.Error
	}
	switch r.Context {
	case "inputs":
		return problem(format.Warning, c.checkInputRef(path))
	case "needs":
		return problem(format.Error, c.checkNeedsRef(path, ph, job))
	case "steps":
		severity := format.Error
		if ph == jobEnds {
			// A job's outputs read a step's missing output as "".
			severity = format.Warning
		}
		return problem(severity, c.checkStepRef(path, job))
	case "matrix":
		switch {
		case job == nil || !job.matrix:
			return problem(format.Warning, fmt.Sprintf("job %s has no matrix, so matrix.%s is the text %q", jobName(job), path[0], "matrix."+path[0]))
		case job.matrixKeys != nil && !job.matrixKeys[path[0]]:
			return problem(format.Warning, withSuggestion(fmt.Sprintf("job %s's matrix has no %s", job.name, path[0]), path[0], slices.Sorted(maps.Keys(job.matrixKeys))))
		}
	case "sessions":
		sessions := mapping(c.doc.Value["sessions"])
		if _, ok := sessions[path[0]]; !ok {
			return problem(format.Warning, withSuggestion(fmt.Sprintf("the workflow has no session %s", path[0]), path[0], slices.Sorted(maps.Keys(sessions))))
		}
	case "vars":
		c.recordSecret(r)
	}
	return true
}

// reportProblem reports p, a probable mistake in an expression, at pos. If
// the mistake split a name the workflow has, such as an input pw-user in
// inputs.pw-user, it says so.
func (c *compiler) reportProblem(pos format.Position, loc []string, p expr.Problem, job *jobInfo) {
	msg := p.Msg
	if p.Meant != nil {
		if has := c.defines(*p.Meant, job); has != "" {
			msg += "; " + has
		}
	}
	c.report(format.Warning, pos, CodeWord, loc, "%s", msg)
}

// defines says what in the workflow r names, such as "the workflow has an
// input pw-user", or returns "" if it names nothing the workflow has.
func (c *compiler) defines(r expr.Ref, job *jobInfo) string {
	if len(r.Path) == 0 {
		return ""
	}
	name := r.Path[0]
	if r.Context == "inputs" {
		for _, f := range c.inputs.fields {
			if !f.header && slices.Equal(f.path, r.Path) {
				return "the workflow has an input " + strings.Join(r.Path, ".")
			}
		}
		return ""
	}
	if len(r.Path) > 1 {
		return ""
	}
	switch r.Context {
	case "needs":
		if _, ok := c.jobInfos[name]; ok {
			return "the workflow has a job " + name
		}
	case "steps":
		if job == nil {
			return ""
		}
		if _, ok := job.stepIDs[name]; ok {
			return fmt.Sprintf("job %s has a step with the id %s", job.name, name)
		}
	case "matrix":
		if job != nil && job.matrixKeys[name] {
			return fmt.Sprintf("job %s's matrix has %s", job.name, name)
		}
	case "sessions":
		if _, ok := mapping(c.doc.Value["sessions"])[name]; ok {
			return "the workflow has a session " + name
		}
	}
	return ""
}

// recordSecret records the secret variable r reads, if it reads one.
func (c *compiler) recordSecret(r expr.Ref) {
	if r.Context != "vars" || len(r.Path) == 0 {
		return
	}
	if u, ok := mapping(c.opts.Contexts["vars"])[r.Path[0]].(expr.Unknown); ok && u.Name != "" {
		c.secrets["vars."+r.Path[0]] = true
	}
}

func jobName(job *jobInfo) string {
	if job == nil {
		return "(none)"
	}
	return job.name
}

// checkInputRef checks that inputs.<path> names an input, and an input in
// it for a group's.
func (c *compiler) checkInputRef(path []string) string {
	var top []string
	for _, f := range c.inputs.fields {
		if len(f.path) == 1 && !f.header {
			top = append(top, f.key)
		}
	}
	var f *field
	for _, cand := range c.inputs.fields {
		if len(cand.path) == 1 && cand.key == path[0] {
			f = cand
		}
	}
	if f == nil {
		return withSuggestion(fmt.Sprintf("there is no input %s", path[0]), path[0], top)
	}
	for _, key := range path[1:] {
		if !f.container || f.flatten || definitionKeys[key] || key == "*" || key == "[index]" {
			return ""
		}
		var next *field
		var names []string
		for _, child := range f.children {
			names = append(names, child.key)
			if child.key == key {
				next = child
			}
		}
		if next == nil {
			return withSuggestion(fmt.Sprintf("input %s has no input %s", strings.Join(f.path, "."), key), key, names)
		}
		f = next
	}
	return ""
}

// checkNeedsRef checks that needs.<path> names a job this one needs, or in
// its steps and outputs this job itself, whose steps' outputs so far are
// its outputs. A job's outputs are whatever its steps write, so their names
// aren't checked.
func (c *compiler) checkNeedsRef(path []string, ph phase, job *jobInfo) string {
	name := path[0]
	needed, ok := c.jobInfos[name]
	self := job != nil && name == job.name && ph >= stepRuns
	switch {
	case job == nil:
		return "needs isn't known outside a job"
	case !ok:
		return withSuggestion(fmt.Sprintf("there is no job %s", name), name, slices.Sorted(maps.Keys(c.jobInfos)))
	case !job.needed[name] && !self:
		return fmt.Sprintf("job %s doesn't need %s, so it can't read its outputs; add %s to its needs", job.name, name, name)
	case len(path) == 1:
		return ""
	}
	switch path[1] {
	case "outputs":
	case "result":
	case "steps":
		if len(path) >= 3 {
			if _, ok := needed.stepIDs[path[2]]; !ok {
				return fmt.Sprintf("job %s has no step with the id %s", name, path[2])
			}
		}
	default:
		return fmt.Sprintf("needs.%s has outputs, result and steps, not %s", name, path[1])
	}
	return ""
}

// checkStepRef checks that steps.<path> names an earlier step of the job.
func (c *compiler) checkStepRef(path []string, job *jobInfo) string {
	id := path[0]
	i, ok := job.stepIDs[id]
	if !ok || i >= job.step {
		var earlier []string
		for k, j := range job.stepIDs {
			if j < job.step {
				earlier = append(earlier, k)
			}
		}
		slices.Sort(earlier)
		if ok {
			return fmt.Sprintf("step %s doesn't run before this, so its outputs aren't known here", id)
		}
		return withSuggestion(fmt.Sprintf("no earlier step of job %s has the id %s", job.name, id), id, earlier)
	}
	if len(path) >= 2 && path[1] != "outputs" {
		return fmt.Sprintf("steps.%s has outputs, not %s", id, path[1])
	}
	return ""
}
