package compile

import (
	"maps"
	"slices"
	"strconv"
	"strings"

	"github.com/parallelworks/meridian/expr"
	"github.com/parallelworks/meridian/internal/hint"
	"github.com/parallelworks/meridian/internal/jsonptr"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
)

// definitionKeys are keys of an input's definition, which inputs.x.type and
// the like read when x's value has none.
var definitionKeys = map[string]bool{
	"type": true, "label": true, "default": true, "hidden": true, "ignore": true, "optional": true,
	"description": true, "tooltip": true, "options": true, "items": true, "min": true, "max": true,
}

// A finding is a problem a check found: its kind and its message's params.
// The zero finding is none.
type finding struct {
	kind   *problems.Kind
	params problems.Params
}

// suggesting returns a finding of kind k with params, and what of names
// name was probably meant to be, if any.
func suggesting(k *problems.Kind, params problems.Params, name string, names *nameList) finding {
	s, ok := names.suggest(name)
	return finding{k, problems.WithSuggestion(params, s, ok)}
}

// A nameList is the names a misspelled name is probably one of. It finds
// what each name it is asked about was probably meant to be once, however
// often the workflow misspells it.
type nameList struct {
	names []string
	found map[string]suggestion
}

// A suggestion is what hint.Suggest found for a name.
type suggestion struct {
	name string
	ok   bool
}

func newNameList(names []string) *nameList { return &nameList{names: names} }

// suggest returns the name of the list that name probably meant, as
// hint.Suggest does.
func (l *nameList) suggest(name string) (string, bool) {
	if s, ok := l.found[name]; ok {
		return s.name, s.ok
	}
	s, ok := hint.Suggest(name, l.names)
	if l.found == nil {
		l.found = map[string]suggestion{}
	}
	l.found[name] = suggestion{s, ok}
	return s, ok
}

// inputNames is what the checks of references find of the inputs' names,
// found once: the names of the inputs at the top, the last input with each
// name there, and the paths of those that aren't headers; and the same of
// each group's inputs, once a reference reads one.
type inputNames struct {
	top     *nameList
	byKey   map[string]*field
	defined map[string]bool
	groups  map[*field]*groupNames
}

// groupNames is the names of a group's inputs, and the last with each name.
type groupNames struct {
	names *nameList
	byKey map[string]*field
}

// inputNames returns what the checks of references find of the inputs'
// names, finding it the first time.
func (c *compiler) inputNames() *inputNames {
	if c.names != nil {
		return c.names
	}
	n := &inputNames{byKey: map[string]*field{}, defined: map[string]bool{}, groups: map[*field]*groupNames{}}
	var top []string
	for _, f := range c.inputs.fields {
		if !f.header {
			n.defined[jsonptr.Format(f.path)] = true
		}
		if len(f.path) == 1 {
			n.byKey[f.key] = f
			if !f.header {
				top = append(top, f.key)
			}
		}
	}
	n.top = newNameList(top)
	c.names = n
	return n
}

// group returns the names of f's inputs, finding them the first time.
func (n *inputNames) group(f *field) *groupNames {
	g := n.groups[f]
	if g == nil {
		g = &groupNames{byKey: map[string]*field{}}
		names := make([]string, 0, len(f.children))
		for _, child := range f.children {
			names = append(names, child.key)
			g.byKey[child.key] = child
		}
		g.names = newNameList(names)
		n.groups[f] = g
	}
	return g
}

// checkRef checks a reference that an expression evaluated in phase ph
// makes, in job (nil outside jobs), and reports at byte offset at in the
// string in what is wrong with it. A reference to something the workflow doesn't
// define is a warning, as ACTIVATE reads it as null or "" and runs the
// workflow, unless ACTIVATE fails the step or job that reads it. checkRef
// returns false if the reference is an error.
func (c *compiler) checkRef(r expr.Ref, ph phase, job *jobInfo, in *spot, at int) bool {
	if !ph.has(r.Context) {
		c.reportIn(in, at, problems.ContextNotKnown, problems.Params{"context": r.Context, "phase": ph.name()})
		return false
	}
	c.recordSecret(r)
	path := r.Path
	if unnamed(path) {
		return true
	}
	var f finding
	switch r.Context {
	case "inputs":
		f = c.checkInputRef(path)
	case "needs":
		f = c.checkNeedsRef(path, ph, job)
	case "steps":
		// A job's outputs read a step's missing output as "".
		f = c.checkStepRef(path, job, ph == jobEnds)
	case "matrix":
		switch {
		case job == nil || !job.matrix:
			f = finding{problems.NoMatrix, problems.Params{
				"job": jobName(job), "key": path[0], "text": problems.Show("matrix." + path[0]),
			}}
		case job.matrixKeys != nil && !job.matrixKeys[path[0]]:
			if job.matrixNames == nil {
				job.matrixNames = newNameList(slices.Sorted(maps.Keys(job.matrixKeys)))
			}
			f = suggesting(problems.UndefinedMatrixKey, problems.Params{"job": job.name, "key": path[0]}, path[0], job.matrixNames)
		}
	case "sessions":
		sessions := mapping(c.doc.Value["sessions"])
		if _, ok := sessions[path[0]]; !ok {
			if c.sessionNames == nil {
				c.sessionNames = newNameList(slices.Sorted(maps.Keys(sessions)))
			}
			f = suggesting(problems.UndefinedSession, problems.Params{"name": path[0]}, path[0], c.sessionNames)
		}
	case "env":
		f = c.checkEnvRef(path[0], ph, job, in)
	case "org", "vars":
		c.checkVariableName(r, in, at)
	}
	if f.kind == nil {
		return true
	}
	c.reportIn(in, at, f.kind, f.params)
	return f.kind.Severity() != problems.Error
}

// reportProblem reports p, a probable mistake in an expression, at byte
// offset at in the string in. If the mistake split a name the workflow has,
// such as an input pw-user in inputs.pw-user, it says so.
func (c *compiler) reportProblem(in *spot, at int, p expr.Problem, job *jobInfo) {
	params := p.Params
	if p.Meant != nil {
		if defines, defined, inJob := c.defines(*p.Meant, job); defines != "" {
			params = maps.Clone(params)
			params["defines"], params["defined"], params["job"] = defines, defined, inJob
		}
	}
	c.reportIn(in, at, p.Kind, params)
}

// defines says what in the workflow r names: an input, a job, a step, a
// matrix key or a session, with its name and the job it is in, if any. It
// returns "" if r names nothing the workflow has.
func (c *compiler) defines(r expr.Ref, job *jobInfo) (defines, defined, in string) {
	if len(r.Path) == 0 {
		return "", "", ""
	}
	name := r.Path[0]
	if r.Context == "inputs" {
		if c.inputNames().defined[jsonptr.Format(r.Path)] {
			return "input", strings.Join(r.Path, "."), ""
		}
		return "", "", ""
	}
	if len(r.Path) > 1 {
		return "", "", ""
	}
	switch r.Context {
	case "needs":
		if _, ok := c.jobInfos[name]; ok {
			return "job", name, ""
		}
	case "steps":
		if job == nil {
			return "", "", ""
		}
		if _, ok := job.stepIDs[name]; ok {
			return "step", name, job.name
		}
	case "matrix":
		if job != nil && job.matrixKeys[name] {
			return "matrix", name, job.name
		}
	case "sessions":
		if _, ok := mapping(c.doc.Value["sessions"])[name]; ok {
			return "session", name, ""
		}
	}
	return "", "", ""
}

// recordSecret records the secret variables r reads, if it reads any: the
// one it names, or each of its context's, for a read that names none, as
// vars.* does.
func (c *compiler) recordSecret(r expr.Ref) {
	if r.Context != "org" && r.Context != "vars" {
		return
	}
	if !unnamed(r.Path) {
		if c.isSecret(r.Context, r.Path[0]) {
			c.secrets[r.Context+"."+r.Path[0]] = true
		}
		return
	}
	for name := range mapping(c.opts.Contexts[r.Context]) {
		if c.isSecret(r.Context, name) {
			c.secrets[r.Context+"."+name] = true
		}
	}
}

// unnamed reports whether a reference whose path in its context is path
// names nothing in it: it reads all of the context, each of its items as *
// does, or the one an [index] chooses.
func unnamed(path []string) bool {
	return len(path) == 0 || path[0] == "*" || path[0] == "[index]"
}

// isSecret reports whether the variable name in context, org or vars, is a
// secret: one whose value the caller's contexts don't give.
func (c *compiler) isSecret(context, name string) bool {
	u, ok := mapping(c.opts.Contexts[context])[name].(expr.Unknown)
	return ok && u.Name != ""
}

func jobName(job *jobInfo) string {
	if job == nil {
		return "(none)"
	}
	return job.name
}

// checkInputRef checks that inputs.<path> names an input, and an input in
// it for a group's.
func (c *compiler) checkInputRef(path []string) finding {
	names := c.inputNames()
	f := names.byKey[path[0]]
	if f == nil {
		return suggesting(problems.UndefinedInput, problems.Params{"name": path[0]}, path[0], names.top)
	}
	for _, key := range path[1:] {
		if !f.container || f.flatten || definitionKeys[key] || key == "*" || key == "[index]" {
			return finding{}
		}
		g := names.group(f)
		next := g.byKey[key]
		if next == nil {
			return suggesting(problems.UndefinedNestedInput, problems.Params{"group": strings.Join(f.path, "."), "name": key}, key, g.names)
		}
		f = next
	}
	return finding{}
}

// checkNeedsRef checks that needs.<path> names a job this one needs, or in
// its steps and outputs this job itself, whose steps' outputs so far are
// its outputs, as needs.<job>.outputs.<name>,
// needs.<job>.steps.<id>.outputs.<name> or needs.<job>.result. Any other
// reference to needs, such as needs.<job> or needs.<job>.<other key>, is a
// warning, as format 1 reads it as its own text (plan.ReadsAsText). A
// job's outputs are whatever its steps write, so their names aren't
// checked.
func (c *compiler) checkNeedsRef(path []string, ph phase, job *jobInfo) finding {
	name := path[0]
	needed, ok := c.jobInfos[name]
	self := job != nil && name == job.name && ph >= stepRuns
	switch {
	case job == nil:
		return finding{problems.NeedsOutsideJob, nil}
	case plan.ReadsAsText("needs", path):
		// Format 1 reads any other reference to needs as its own text,
		// whatever job it names.
		if len(path) >= 2 && path[1] != "outputs" && path[1] != "steps" {
			return finding{problems.NeedsUnknownKey, problems.Params{"job": name, "key": path[1]}}
		}
		return finding{problems.NeedsReadWhole, problems.Params{"job": name, "text": "needs." + strings.Join(path, ".")}}
	case !ok:
		return suggesting(problems.UndefinedJob, problems.Params{"job": name}, name, c.jobNames)
	case !job.needed[name] && !self:
		return finding{problems.JobNotNeeded, problems.Params{"job": job.name, "needed": name}}
	}
	if path[1] == "steps" {
		if _, ok := needed.stepIDs[path[2]]; !ok {
			return finding{problems.NeededJobNoStep, problems.Params{"job": name, "id": path[2]}}
		}
	}
	return finding{}
}

// checkEnvRef checks that env.<name>, read in phase ph in job (nil outside
// jobs) at the string in, names a variable env holds there: one the
// workflow's env: defines; once the job's env: is evaluated, one of
// format 1's and one the job's env: defines, as do its working-directory:,
// its steps and its outputs:, but for the variables format 1 defines for a
// step alone, such as OUTPUTS, which only its steps read
// (plan.IsFormatVariable, plan.IsJobStartVariable); and in a step, one its
// own env: does. Any other reads as "", which is a warning.
func (c *compiler) checkEnvRef(name string, ph phase, job *jobInfo, in *spot) finding {
	env := func(loc ...string) map[string]any { return mapping(lookup(c.doc.Value, loc)) }
	defined := []map[string]any{env("env")}
	if job != nil {
		dir := slices.Equal(in.loc, appendPath(job.loc, "working-directory"))
		if ph == stepRuns && plan.IsFormatVariable(name) || (ph >= jobEnv || dir) && plan.IsJobStartVariable(name) {
			return finding{}
		}
		if ph >= jobEnv || dir {
			defined = append(defined, env(appendPath(job.loc, "env")...))
		}
		if ph == stepRuns && job.step >= 0 {
			defined = append(defined, env(appendPath(job.loc, "steps", strconv.Itoa(job.step), "env")...))
		}
	}
	var names []string
	for _, m := range defined {
		if _, ok := m[name]; ok {
			return finding{}
		}
		names = append(names, slices.Collect(maps.Keys(m))...)
	}
	slices.Sort(names)
	return suggesting(problems.UndefinedEnv, problems.Params{"name": name}, name, newNameList(slices.Compact(names)))
}

// checkStepRef checks that steps.<path> names an output of an earlier step
// of the job, as steps.<id>.outputs.<name>. Any other reference to steps,
// such as steps.<id> or steps.<id>.result, is a warning, as format 1 reads
// it as its own text. In one of the job's outputs, which are read once its
// steps have run, a step that isn't there is a warning, as the output is
// empty, and so it is in a step's cleanup:, which runs once every step
// has, its own and later ones among them.
func (c *compiler) checkStepRef(path []string, job *jobInfo, output bool) finding {
	id := path[0]
	reach := job.step
	if job.cleanup {
		reach = job.steps
	}
	undefined, unknownKey := problems.UndefinedStep, problems.StepsUnknownKey
	switch {
	case output:
		undefined, unknownKey = problems.OutputUndefinedStep, problems.OutputStepsUnknownKey
	case job.cleanup:
		undefined, unknownKey = problems.CleanupUndefinedStep, problems.CleanupStepsUnknownKey
	}
	// Format 1 reads steps only as steps.<id>.outputs.<name>, and any other
	// reference to it as its own text, whatever step it names.
	switch {
	case len(path) >= 2 && path[1] != "outputs":
		return finding{unknownKey, problems.Params{"id": id, "key": path[1]}}
	case len(path) < 3:
		return finding{problems.StepsReadWhole, problems.Params{"id": id, "text": "steps." + strings.Join(path, ".")}}
	}
	i, ok := job.stepIDs[id]
	if !ok || i >= reach {
		if ok {
			return finding{problems.StepNotEarlier, problems.Params{"id": id}}
		}
		if job.earlier == nil || job.earlierAt != reach {
			var earlier []string
			for k, j := range job.stepIDs {
				if j < reach {
					earlier = append(earlier, k)
				}
			}
			slices.Sort(earlier)
			job.earlier, job.earlierAt = newNameList(earlier), reach
		}
		return suggesting(undefined, problems.Params{"job": job.name, "id": id}, id, job.earlier)
	}
	return finding{}
}
