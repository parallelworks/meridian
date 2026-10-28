package plan

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/parallelworks/meridian/problems"
)

// stepTargetContexts are the contexts a step's ssh: can read for the
// coordinator to evaluate it, for a step run (StepTarget): what Ready
// readies the job with. The step's retry counter is env's one variable it
// can read too (stepTargetEnv).
var stepTargetContexts = map[string]bool{
	"inputs": true, "needs": true, "matrix": true, "vars": true, "org": true,
	"secrets": true, "sessions": true, "remote": true,
}

// stepTargetEnv is the variable of env a step's ssh: can read for the
// coordinator to evaluate it: the try's retry counter, as format 1's
// failover between hosts reads it.
const stepTargetEnv = "PW_WORKFLOW_STEP_CURRENT_RETRY"

// StepSSHReadsWorker returns, for a step of the job whose ssh: reads what
// only the job's worker knows, which the coordinator can't evaluate it
// with, the first such thing it reads, as context.path: the job's steps,
// env but the retry counter, or the job's own outputs through needs; or
// "" for one that reads none of them.
func (j *Job) StepSSHReadsWorker(s *Step) string {
	for _, e := range s.SSH.Exprs() {
		for _, r := range e.Refs() {
			switch {
			case r.Context == "env" && len(r.Path) > 0 && r.Path[0] == stepTargetEnv:
			case r.Context == "needs" && len(r.Path) > 0 && r.Path[0] == j.Name:
				return "needs." + j.Name
			case !stepTargetContexts[r.Context]:
				return strings.Join(append([]string{r.Context}, r.Path...), ".")
			}
		}
	}
	return ""
}

// StepTarget evaluates the ssh: of the step at index of the job, for its
// try try, as the coordinator does for a step run (docs/design.md, Steps
// on another host), rather than take what the step's worker gave: with
// what Ready readies the job with, in, and the try's retry counter, as
// format 1 resolves a step's host before it connects. It returns the host
// it names, or nil for an ssh: that is null, or names no host, and
// whether evaluating it read a variable or a password; and
// step_ssh_reads_worker for one that reads what only the job's worker
// knows (StepSSHReadsWorker).
func (p *Plan) StepTarget(ctx context.Context, job string, index, try int, in ReadyInput) (*Inline, bool, problems.Diagnostics, error) {
	j, err := p.job(job)
	if err != nil {
		return nil, false, nil, err
	}
	if index < 0 || index >= len(j.Steps) {
		return nil, false, nil, fmt.Errorf("plan: the job %s has no step %d", job, index)
	}
	s := &j.Steps[index]
	pointer := at(s.Path, "ssh")
	if ref := j.StepSSHReadsWorker(s); ref != "" {
		return nil, false, problems.Diagnostics{problems.StepSSHReadsWorker.At("", problems.Position{}, pointer, problems.Params{"ref": ref})}, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, false, nil, err
	}
	status, _ := in.status()
	c := Contexts{Run: in.Run, Needs: in.Needs, Status: status, WorkerEnv: map[string]string{stepTargetEnv: strconv.Itoa(try)}}
	if i := in.Instance; i != nil {
		c.Matrix = i.Matrix
		if i.Private || i.Secret {
			c.Private = []string{"matrix"}
		}
		if i.Secret {
			c.Secrets = []string{"matrix"}
		}
	}
	e := newEvaluation(PhaseStep, c)
	var (
		v  any
		ok bool
	)
	read := e.reading(func() { v, _, ok = e.value(s.SSH, pointer) })
	if !ok || e.diags.HasErrors() {
		return nil, false, e.diags, nil
	}
	private := read.Private || read.Marked
	if m, isMapping := v.(map[string]any); v == nil || isMapping && NoRemoteHost(m) {
		return nil, private, e.diags, nil
	}
	return e.inline(v, pointer), private, e.diags, nil
}
