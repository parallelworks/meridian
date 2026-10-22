package plan

import (
	"context"
	jsonv2 "encoding/json/v2"
	"fmt"
	"maps"
	"slices"

	"github.com/parallelworks/meridian/expr"
	"github.com/parallelworks/meridian/internal/jsonptr"
	"github.com/parallelworks/meridian/internal/jsonvalue"
	"github.com/parallelworks/meridian/problems"
)

// An Outcome is how a job or a step ended: what needs.<job>.result reads.
type Outcome string

// The outcomes.
const (
	OutcomeSuccess   Outcome = "success"
	OutcomeFailure   Outcome = "failure"
	OutcomeCancelled Outcome = "cancelled"
	OutcomeSkipped   Outcome = "skipped"
)

// A Reason is why an attempt ended other than as its steps did, or why a
// worker was told to stop, which the attempt records. A job's retry: lists
// the reasons it may run again for.
type Reason string

// The reasons.
const (
	// ReasonWorkerLost is a worker lost after a step started: its backend
	// says it ended without completing its attempt, or a job that fences
	// itself let its lease lapse. A job whose retry: lists it runs again.
	ReasonWorkerLost Reason = "worker-lost"
	// ReasonSetupFailed is an attempt that failed before its first step
	// started, such as an image that couldn't be pulled: none of the
	// workflow's code ran, so the job is placed again, on another member of
	// its group when there is one.
	ReasonSetupFailed Reason = "setup-failed"
	// ReasonTimeout is a job or a run that ran past its timeout:, which a
	// worker or the coordinator stopped, or a scheduler ended at its time
	// limit.
	ReasonTimeout Reason = "timeout"
	// ReasonOutOfMemory is a job its scheduler or the kernel ended for
	// using more memory than it may.
	ReasonOutOfMemory Reason = "out-of-memory"
	// ReasonCancelled is a run that a person cancelled.
	ReasonCancelled Reason = "cancelled"
	// ReasonFailFast is a matrix instance cancelled because another
	// failed, as its strategy's fail-fast: asks.
	ReasonFailFast Reason = "fail-fast"
)

// A JobResult is how a job ended, as the jobs that need it read it: its
// outcome, which needs.<job>.result reads, and its outputs.
type JobResult struct {
	Outcome Outcome `json:"outcome"`
	// Outputs are the job's outputs, by name, with secrets masked.
	Outputs map[string]string `json:"outputs,omitempty"`
	// Private names the outputs that are private (EndResult.Private): the
	// coordinator keeps their values only in the run's secret record, and
	// the jobs that need the job read them as private.
	Private []string `json:"private,omitempty"`
	// Steps are the outputs of its steps that have ids, by id, with secrets
	// masked: needs.<job>.steps.<id>.outputs.<name>.
	Steps map[string]map[string]string `json:"steps,omitempty"`
	// NeedsFailed is set for a job that was skipped because a job it needs
	// failed, was cancelled or was itself skipped so (ReadyResult). The
	// jobs that need it read it as failed, as format 1 does: failure() is
	// true for them, and success() false, as it isn't for a job skipped by
	// its own if:.
	NeedsFailed bool `json:"needsFailed,omitzero"`
}

// Combine returns a matrix job's result from its instances', given in the
// order of their indexes, as docs/design.md says under Matrices: its
// outcome is failure if an instance failed, otherwise cancelled if one was
// cancelled, skipped if all were skipped, and success otherwise; an output,
// or a step's, that more than one instance set has the value of the one
// with the highest index, and is private if that instance's is. A job all
// of whose instances were skipped is skipped because its needs failed if
// one of them was.
//
// Which instance's output a job gives is a bug fix to format 1, in which
// it is the last to finish's: no workflow can rely on which that is, and
// none of those we know of reads a matrix job's outputs.
func Combine(instances []JobResult) JobResult {
	var out JobResult
	failed, cancelled, skipped, needsFailed := false, false, true, false
	private := map[string]bool{}
	for _, r := range instances {
		switch r.Outcome {
		case OutcomeFailure:
			failed = true
		case OutcomeCancelled:
			cancelled = true
		}
		skipped = skipped && r.Outcome == OutcomeSkipped
		needsFailed = needsFailed || r.NeedsFailed
		for name, v := range r.Outputs {
			if out.Outputs == nil {
				out.Outputs = map[string]string{}
			}
			out.Outputs[name] = v
			private[name] = slices.Contains(r.Private, name)
		}
		for id, outputs := range r.Steps {
			if out.Steps == nil {
				out.Steps = map[string]map[string]string{}
			}
			if out.Steps[id] == nil {
				out.Steps[id] = map[string]string{}
			}
			maps.Copy(out.Steps[id], outputs)
		}
	}
	for name, p := range jsonvalue.Sorted(private) {
		if p {
			out.Private = append(out.Private, name)
		}
	}
	switch {
	case failed:
		out.Outcome = OutcomeFailure
	case cancelled:
		out.Outcome = OutcomeCancelled
	case skipped:
		out.Outcome, out.NeedsFailed = OutcomeSkipped, needsFailed
	default:
		out.Outcome = OutcomeSuccess
	}
	return out
}

// Reads returns the part of run that the job reads, which is what an
// attempt of it receives when it is claimed: what the expressions that its
// worker evaluates read, in its env:, its steps and its outputs:, of the
// inputs, org and vars; and the workflow's env, which every step's
// environment holds, remote and sessions, whole. Of the inputs, it gives
// what each reference's path leads to, as far as the path names a key of a
// mapping or an index of a list, and all of what the path reads from
// there, as a * or a slice does, or text holding JSON; an item of a list
// before one it gives is null. Of org and vars, it gives each variable a
// reference names, and all of a context one reads whole, as vars.* does.
// Private and Secrets name what of it they name. What Ready evaluated, such
// as an instance's matrix values, an attempt receives in its ReadyJob. The
// coordinator never gives an attempt more.
func (j *Job) Reads(run RunValues) RunValues {
	all := run.tree()
	var grants [][]string
	for _, v := range j.workerValues() {
		for _, e := range v.Exprs() {
			for _, r := range e.Refs() {
				switch r.Context {
				case "inputs":
					grants = append(grants, granted(all, r.Path))
				case "org", "vars":
					path := []string{r.Context}
					if len(r.Path) > 0 && r.Path[0] != "*" && r.Path[0] != "[index]" {
						path = append(path, r.Path[0])
					}
					grants = append(grants, path)
				}
			}
		}
	}
	grants = outermost(grants)
	read := map[string]any{}
	for _, path := range grants {
		if v, ok := jsonptr.Lookup(all, path); ok {
			setPath(read, path, jsonvalue.Clone(v))
		}
	}
	out := fromTree(read)
	out.Format = run.Format
	out.Env, out.Remote, out.Sessions = maps.Clone(run.Env), maps.Clone(run.Remote), maps.Clone(run.Sessions)
	if !run.Inputs.IsZero() && out.Inputs.IsZero() {
		out.Inputs = DataOf(map[string]any{})
	}
	kept := func(names []string) []string {
		var out []string
		for _, name := range names {
			path := splitPath(name)
			if path[0] != "inputs" && path[0] != "org" && path[0] != "vars" ||
				slices.ContainsFunc(grants, func(g []string) bool { return isPrefix(g, path) || isPrefix(path, g) }) {
				out = append(out, name)
			}
		}
		return out
	}
	out.Private, out.Secrets = kept(run.Private), kept(run.Secrets)
	return out
}

// granted returns the path, inputs first, of what a reference to the
// inputs that follows path reads of all, the run's values: each key of a
// mapping and index of a list it names, up to what it reads all of, as a *,
// a slice or an index spelled otherwise do, or text, which it reads the
// JSON of.
func granted(all map[string]any, path []string) []string {
	out := []string{"inputs"}
	v := all["inputs"]
	for _, key := range path {
		if key == "*" || key == "[index]" {
			return out
		}
		switch c := v.(type) {
		case map[string]any:
			v = c[key]
		case []any:
			n, ok := index(key)
			if !ok {
				return out
			}
			if n < len(c) {
				v = c[n]
			} else {
				v = nil
			}
		default:
			return out
		}
		out = append(out, key)
	}
	return out
}

// workerValues returns the values of the job that its worker evaluates:
// its env:, each of its steps' and its outputs:.
func (j *Job) workerValues() []Value {
	var out []Value
	out = slices.AppendSeq(out, maps.Values(j.Env))
	for _, s := range j.Steps {
		out = append(out, s.Name, s.If, s.Run, s.With, s.WorkingDirectory, s.SSH, s.Timeout, s.IgnoreErrors, s.Cleanup)
		out = slices.AppendSeq(out, maps.Values(s.Env))
		if r := s.Retry; r != nil {
			out = append(out, r.MaxRetries, r.Interval, r.Timeout)
		}
	}
	return slices.AppendSeq(out, maps.Values(j.Outputs))
}

// ReadyInput is what the coordinator knows when a job's dependencies have
// finished.
type ReadyInput struct {
	// Run is what the run knows as a whole, from Start.
	Run RunValues
	// Needs are the results of the jobs the job needs, by job name: for a
	// need on any instance, the instance that finished first.
	Needs map[string]JobResult
	// Cancelled is set once the run is being cancelled, for cancelled().
	Cancelled bool
	// Instance is the matrix instance to ready, for a job whose matrix is
	// expanded: by Start, or by an earlier call to Ready. It is nil for a
	// job without a strategy:, and for one whose matrix Ready is to expand.
	Instance *Instance
	// Limits are the plan's (Run.Limits).
	Limits Limits
}

// ReadyResult is what Ready gives: a matrix's instances, a job skipped, or
// a job ready to place. Exactly one of Expansion, Skipped and Job is set,
// and NeedsFailed only with Skipped.
type ReadyResult struct {
	// Expansion holds the job's matrix instances, when Ready expanded its
	// matrix. The coordinator keeps them, and readies each instance on its
	// own, with ReadyInput.Instance.
	Expansion *Expansion
	// Skipped is set when the job's if: is false, and the job, or the
	// instance, ends skipped. For a matrix job whose if: doesn't read
	// matrix, it is set before the matrix is expanded, and the job has no
	// instances.
	Skipped bool
	// NeedsFailed is set with Skipped when the job is skipped because a job
	// it needs failed, was cancelled or was itself skipped so, as its
	// default if:, success(), or an if: that calls a status function,
	// decided: format 1's dependency failure. The coordinator records it in
	// the job's JobResult, so that the jobs that need it read it as failed.
	NeedsFailed bool
	// Job is the job as Ready evaluated it, to place and to run.
	Job *ReadyJob
}

// A ReadyJob is a job instance as Ready evaluated it: what the coordinator
// places, and the job a worker receives when it claims an attempt
// (EncodeReadyJob). What Ready evaluates is final, and never evaluated
// again; the job's env:, steps and outputs: stay the plan's, for Step and
// End to evaluate on the worker.
//
// A ReadyJob can hold private values, and secrets, such as matrix values
// made from a variable. So the coordinator never stores one as run state:
// Ready depends only on what is kept, so it runs Ready again when it needs
// the job, as when a worker claims an attempt of it.
//
// A ReadyJob is encoded as a plan is, so each of its types is the plan's
// own, and a change to one changes the plan's version.
type ReadyJob struct {
	// Key is the instance's key: the job's name, or for a matrix instance
	// its name, # and index, as in build#2.
	Key string `json:"key"`
	// Name is the job's name in the workflow.
	Name string `json:"name"`
	// Matrix holds the instance's matrix values, a mapping, or nothing for
	// a job without a matrix.
	Matrix Data `json:"matrix,omitzero"`
	// Target is where the job runs, from its runs-on: or its format 1
	// ssh:.
	Target Target `json:"target,omitzero"`
	// Resources are what the job's resources: asks for.
	Resources Resources `json:"resources,omitzero"`
	// Retry is the job's retry:, or nil without one.
	Retry *ReadyJobRetry `json:"retry,omitzero"`
	// TimeoutSeconds is the job's timeout:, in seconds, or 0 for none.
	TimeoutSeconds int64 `json:"timeoutSeconds,omitzero"`
	// WorkingDirectory is the job's working-directory:, or "" for the run
	// directory.
	WorkingDirectory string `json:"workingDirectory,omitempty"`
	// Env, Steps and Outputs are the job's env:, steps and outputs:, as the
	// plan holds them.
	Env     map[string]Value `json:"env,omitempty"`
	Steps   []Step           `json:"steps"`
	Outputs map[string]Value `json:"outputs,omitempty"`
	// Private and Secrets name the contexts Ready gives the job that were
	// made from a variable or a password, and from a secret, which Step and
	// End mark (Contexts.Private and Contexts.Secrets): matrix, for an
	// instance whose matrix values were (Instance.Private and
	// Instance.Secret).
	Private []string `json:"private,omitempty"`
	Secrets []string `json:"secrets,omitempty"`
	// Masked are the strings Ready made from a secret, as a Masker takes
	// them: each string that a template that read one gave, and those of
	// an instance's matrix values made from one. The worker masks them, as
	// it masks the run's secrets.
	Masked []string `json:"masked,omitempty"`
	// Path is the JSON Pointer of the job in the workflow, for messages.
	Path string `json:"path"`
}

// A Target is where a job runs, from its runs-on: or its format 1 ssh:. At
// most one of its fields is set; with none, the job runs in the group that
// whoever runs the workflow chose.
type Target struct {
	// Group names an execution group: a runs-on: execution-group:, or a
	// runs-on: that reads an execution-groups input, whose value is a
	// group's name.
	Group string `json:"group,omitempty"`
	// Environment is a runs-on: environment:, which only a platform's
	// Groups resolver knows.
	Environment *Environment `json:"environment,omitzero"`
	// ComputeTarget is a runs-on: that reads a compute-target input, which
	// only a platform's Groups resolver knows.
	ComputeTarget *ComputeTarget `json:"computeTarget,omitzero"`
	// Inline is the group a format 1 ssh: makes, from which the
	// coordinator makes a backend.Group.
	Inline *Inline `json:"inline,omitzero"`
}

// An Inline is the execution group a format 1 ssh: makes, of one member,
// as the plan holds it: the coordinator makes the backend.Group, so that
// what a backend adds to its members never changes the plan's encoding.
type Inline struct {
	// Name is the group's and its member's name: ssh.
	Name string `json:"name"`
	// Backend is the member's backend, ssh, and Isolation its isolation,
	// host: format 1's ssh: runs its commands on the host, outside any
	// container.
	Backend   string `json:"backend"`
	Isolation string `json:"isolation"`
	// SSH is the ssh: mapping, evaluated: the ssh backend's settings,
	// whose keys are format 1's, such as remoteHost and jumpNodeHost.
	SSH Data `json:"ssh"`
}

// Resources are CPUs, memory and GPUs, as a job's resources: asks for
// them, parsed. A zero field asks for none of it. The coordinator turns
// them into the backend.Resources a member offers, which are kept in the
// same units.
type Resources struct {
	// MilliCPU is in thousandths of a core: 4 is 4000, and 500m 500.
	MilliCPU int64 `json:"milliCPU,omitzero"`
	// Memory is in bytes: 8Gi is 8 << 30.
	Memory int64 `json:"memory,omitzero"`
	GPU    int   `json:"gpu,omitzero"`
}

// An Environment is a platform's scheduler environment, which a format 1
// runs-on: environment: names.
type Environment struct {
	// Cluster is the cluster's name, optionally prefixed with its owner, as
	// owner/cluster, and Name the environment's in it.
	Cluster string `json:"cluster"`
	Name    string `json:"name"`
	// Params are its runs-on: params:, the scheduling parameters, a
	// mapping, or nothing.
	Params Data `json:"params,omitzero"`
}

// A ComputeTarget is where a job runs as a compute-target input gives it,
// in a format 1 runs-on:.
type ComputeTarget struct {
	// Mode is environment, login or worker.
	Mode string `json:"mode"`
	// TargetID is the cluster, or user-workspace for the user workspace.
	TargetID      string `json:"targetId,omitempty"`
	EnvironmentID string `json:"environmentId,omitempty"`
	// SchedulingParams are the environment's scheduling parameters, a
	// mapping, or nothing.
	SchedulingParams Data   `json:"schedulingParams,omitzero"`
	NewWorker        bool   `json:"newWorker,omitzero"`
	WorkerID         string `json:"workerId,omitempty"`
}

// ReadyJobRetry is a job's retry: as Ready evaluated it, with the design's
// defaults: 3 retries, and a minute before the first, doubling up to an
// hour. The schema's descriptions still give 10 and 5 seconds, and change
// with the first change to format/.
type ReadyJobRetry struct {
	// Reasons are why an attempt may run again.
	Reasons []Reason `json:"reasons"`
	// MaxRetries is how many times the job may run again.
	MaxRetries int `json:"maxRetries"`
	// IntervalSeconds is how long the first new attempt waits, in seconds.
	// Each one after it waits twice as long as the one before, up to an
	// hour.
	IntervalSeconds int64 `json:"intervalSeconds"`
}

// Ready is the Ready phase: it evaluates what job leaves to the point at
// which its dependencies have finished, in the coordinator, with the
// status functions true or false as in.Needs and in.Cancelled say: a job
// fails, for failure(), when one it needs failed or was skipped because
// its own needs failed (JobResult.NeedsFailed), and is cancelled, for
// cancelled(), when the run is being cancelled or one it needs was
// cancelled, as in format 1. A job one needs that its own if: skipped
// counts as a success.
//
// For a job with a strategy: and no in.Instance, it first evaluates the
// job's if:, if it doesn't read matrix, and gives Skipped if it is false;
// otherwise it expands the matrix with Expand, and gives the Expansion. For
// an instance, or a job without a strategy:, it evaluates the job's if:,
// and gives Skipped if it is false; otherwise it gives the job, with its
// runs-on:, resources:, retry:, timeout:, working-directory: and ssh:
// evaluated. An instance's if: that doesn't read matrix gives what it gave
// before the matrix was expanded, unless the run has been cancelled since,
// so an instance of a matrix that Start expanded is skipped as its job
// would have been. A job's if: may read matrix in format 1, which GitHub
// Actions' doesn't: the compiler accepts it, and changing that would change
// what a workflow means.
//
// An if: is true when its value is: its truthiness, unless it is the word
// always, which is always(); and, unless it calls a status function, it
// is true only if success() is too, the default if:'s place it doesn't
// take. A job it skips because success() is false is skipped because its
// needs failed (NeedsFailed), and so is one an if: that calls a status
// function skips while a job it needs failed or was cancelled.
//
// Ready reaches nothing: the same job and input give the same result, byte
// for byte. Its diagnostics are the job's problems at the job's path, with
// no file position, and never show a value made from a variable or a
// password: an expression that fails, or a value that doesn't fit its key,
// such as resources: memory that isn't a quantity (resources_invalid), a
// timeout: or retry interval: that isn't a duration (duration_invalid), a
// retry max-retries: that isn't a number (count_invalid), a runs-on: that
// names no place (runs_on_invalid), or a working-directory: that isn't
// text (wrong_type). An error among them fails the job, and there is no
// result. It returns an error only when ctx is done before it finishes,
// and then ctx.Err().
func (j *Job) Ready(ctx context.Context, in ReadyInput) (*ReadyResult, problems.Diagnostics, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	status, needsOK := in.status()
	c := Contexts{Run: in.Run, Needs: in.Needs, Status: status}
	skipped := &ReadyResult{Skipped: true, NeedsFailed: !needsOK}
	if j.Strategy != nil && in.Instance == nil {
		e := newEvaluation(PhaseStrategy, c)
		if !readsContext(j.If, "matrix") {
			run, ok := e.condition(j.If, at(j.Path, "if"))
			switch {
			case !ok:
				return nil, e.diags, nil
			case !run:
				return skipped, nil, nil
			}
		}
		x, err := Expand(ctx, j, e.scope, in.Limits)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return nil, nil, ctxErr
			}
			e.reportError(at(j.Path, "strategy"), err, expr.Read{})
			return nil, e.diags, nil
		}
		return &ReadyResult{Expansion: x}, nil, nil
	}
	rj := &ReadyJob{Key: j.Key, Name: j.Name, Env: j.Env, Steps: j.Steps, Outputs: j.Outputs, Path: j.Path}
	if i := in.Instance; i != nil {
		rj.Key, rj.Matrix = i.Key, i.Matrix
		if i.Private || i.Secret {
			rj.Private = []string{"matrix"}
		}
		if i.Secret {
			rj.Secrets = []string{"matrix"}
		}
		c.Matrix = i.Matrix
	}
	c.Private, c.Secrets = rj.Private, rj.Secrets
	e := newEvaluation(PhaseReady, c)
	if i := in.Instance; i != nil && i.Secret {
		// The instance's matrix values were made from a secret.
		maskedStrings(i.Matrix.Get(), e.mask)
	}
	run, ok := e.condition(j.If, at(j.Path, "if"))
	switch {
	case !ok:
		return nil, e.diags, nil
	case !run:
		return skipped, nil, nil
	}
	rj.Target = e.target(j)
	rj.Resources = e.resources(j.Resources, at(j.Path, "resources"))
	rj.Retry = e.jobRetry(j.Retry, at(j.Path, "retry"))
	rj.TimeoutSeconds = e.duration(j.Timeout, at(j.Path, "timeout"), "timeout", 0)
	rj.WorkingDirectory, _ = e.directory(j.WorkingDirectory, at(j.Path, "working-directory"))
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if e.diags.HasErrors() {
		return nil, e.diags, nil
	}
	rj.Masked = e.masked
	return &ReadyResult{Job: rj}, e.diags, nil
}

// status returns how the job got here, for the status functions, from the
// results of the jobs it needs and whether the run is being cancelled, and
// whether its needs succeeded: none failed or was cancelled, or was
// skipped because its own needs failed.
func (in ReadyInput) status() (status expr.Status, needsOK bool) {
	status.Cancelled = in.Cancelled
	for _, r := range in.Needs {
		switch {
		case r.Outcome == OutcomeFailure, r.NeedsFailed:
			status.Failed = true
		case r.Outcome == OutcomeCancelled:
			status.Cancelled = true
		}
	}
	for _, r := range in.Needs {
		if r.Outcome == OutcomeFailure || r.Outcome == OutcomeCancelled || r.NeedsFailed {
			return status, false
		}
	}
	return status, true
}

// EncodeReadyJob encodes a ready job as JSON, as Encode encodes a plan's
// values, for a worker API claim.
func EncodeReadyJob(j *ReadyJob) ([]byte, error) {
	return jsonv2.Marshal(j, encoding)
}

// DecodeReadyJob decodes a ready job that EncodeReadyJob encoded, from a run
// whose plan is of version. It is as strict as Decode: it fails on a
// version this release doesn't read, with ErrVersion, and on a key it
// doesn't know, or one given twice.
func DecodeReadyJob(version int, data []byte) (*ReadyJob, error) {
	if version != Version {
		return nil, fmt.Errorf("%w %d; this release reads version %d", ErrVersion, version, Version)
	}
	j := new(ReadyJob)
	if err := jsonv2.Unmarshal(data, j, decoding, jsonv2.RejectUnknownMembers(true)); err != nil {
		return nil, fmt.Errorf("plan: %w", err)
	}
	return j, nil
}
