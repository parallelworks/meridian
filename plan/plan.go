package plan

import (
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
	"errors"
	"fmt"

	"github.com/parallelworks/meridian/expr"
	"github.com/parallelworks/meridian/internal/jsonvalue"
	"github.com/parallelworks/meridian/problems"
)

// Version is the version of the plan encoding this package reads and
// writes. It changes only for a change that can't be an addition.
const Version = 1

// A Plan is a compiled run: everything a coordinator needs to schedule it,
// and a worker to run its jobs, without the workflow file.
type Plan struct {
	// Version is the plan encoding's version.
	Version int `json:"version"`
	Run     Run `json:"run"`
	// Jobs are the run's jobs, each once, in the order of their names. A
	// matrix job's instances are made as the run goes (Expand).
	Jobs []Job `json:"jobs"`
	// Warnings are the compiler's warnings, which are kept with the run so
	// that they reach whoever runs the workflow.
	Warnings []problems.Diagnostic `json:"warnings,omitempty"`
}

// Run is what the plan holds about the run as a whole.
type Run struct {
	Source Source `json:"source"`
	// Format is the workflow's format.
	Format      int    `json:"format"`
	Name        string `json:"name,omitempty"`
	Description string `json:"description,omitempty"`
	Icon        string `json:"icon,omitempty"`
	Thumbnail   string `json:"thumbnail,omitempty"`
	// Inputs are the run's input values, with defaults applied and ignored
	// inputs left out: a mapping.
	Inputs Data `json:"inputs,omitzero"`
	// DeferredInputs are the defaults of inputs that read what is known
	// only once the run is underway, such as a secret variable, or what the
	// plan never holds, an organization or user variable or a password
	// input, by their input's path, as in group.name. An input's value that
	// reads such a thing in only a part of it, such as one of a list's rows,
	// is one too, with that part's expressions in it, and without the
	// values of the password fields in its rows, which are among the
	// secrets. They are evaluated as the run starts, with the other inputs'
	// values, and give the inputs' values.
	DeferredInputs map[string]Value `json:"deferredInputs,omitempty"`
	// Env is the workflow's env:.
	Env map[string]Value `json:"env,omitempty"`
	// Timeout is the run's timeout:.
	Timeout Value `json:"timeout,omitzero"`
	// Secrets names the secret variables the run reads, as in vars.token,
	// each of a context's for a read of every variable of the context, as
	// vars.* is; and its passwords, by their paths in the inputs' values: a
	// password input, as in inputs.token, a password field of a list's row,
	// as in inputs.creds.0.pass, a list whose rows have one and are
	// evaluated as the run starts, all of it, as in inputs.creds, and a
	// submitted value that the compiler keeps as one, as it can hold one or
	// a variable's value. Their values are never in the plan; each attempt
	// receives them when it is claimed. As the run starts, each password's
	// value is put in the inputs' values at its path.
	Secrets []string `json:"secrets,omitempty"`
	// Derived names the secrets, among Secrets, that are values the run was
	// given in the place of what is made from a password or a secret
	// variable, that aren't what makes it, as a value a person typed over
	// such a default is: they can be made from a secret, or not, so they are
	// masked as what is made from a secret is, not as a secret's own text.
	Derived []string `json:"derived,omitempty"`
	// Private names the values the run was given that the plan doesn't
	// hold, though they aren't secrets, by their paths in the inputs'
	// values: each in the place of what is made from an organization or
	// user variable that isn't a secret, and no password, that isn't what
	// makes it. They are kept as a variable's value is, only in the run's
	// secret record, and aren't masked. As the run starts, each is put in
	// the inputs' values at its path.
	Private []string `json:"private,omitempty"`
	// Variables names the organization and user variables the run reads
	// that aren't secrets, as in org.region and vars.region, in order. A
	// context's name alone, org or vars, means the run reads all of it, as
	// vars.* does. Their values are never in the plan either: the run reads
	// them once, as it starts.
	Variables []string `json:"variables,omitempty"`
	// Permissions are the workflow's permissions:.
	Permissions []string `json:"permissions,omitempty"`
	// Limits are Meridian's limits on the run's size that the phases after
	// Create apply, so that every replica, and every rerun, applies the
	// same ones. The compiler sets none yet; when it first does, the plan's
	// version rises, so that a replica of a release that doesn't know them
	// leaves the run alone.
	Limits Limits `json:"limits,omitzero"`
}

// Limits are Meridian's limits on a run's size, which whoever runs the
// workflow sets, and which aren't part of the format. A zero field is the
// default. The compiler sets none yet: they come with its Options.Limits.
type Limits struct {
	// MaxInstances is how many instances a matrix can have, or 0 for
	// DefaultMaxInstances.
	MaxInstances int `json:"maxInstances,omitzero"`
}

// A Source is where a workflow came from, and the exact text that ran.
type Source struct {
	// Location names the workflow: a file, a repository and a path in it,
	// or a workflow a platform stores.
	Location string `json:"location"`
	// Revision is a Git commit, or "sha256:" and the SHA-256 digest of the
	// text when no commit has it.
	Revision string `json:"revision"`
	// Digest is "sha256:" and the SHA-256 digest of the text, which a
	// workflow that a step's uses: names is pinned to beside its commit,
	// so that reading it again by its pin gives the same text or fails.
	// The compiler doesn't set it yet, for the run's own source or any
	// other; when it first does, the plan's version rises.
	Digest string `json:"digest,omitempty"`
}

// A Job is one of the workflow's jobs. A matrix job is one Job, whose
// instances are made from its Strategy as the run goes.
type Job struct {
	// Key is the job's name. A matrix instance's key, its name, # and its
	// index, as in build#2, is its Instance's.
	Key string `json:"key"`
	// Name is the job's name in the workflow.
	Name string `json:"name"`
	// Matrix is unused: a matrix instance's values are its Instance's. The
	// compiler never sets it, and it goes with the plan's next version.
	Matrix Data `json:"matrix,omitzero"`
	// Strategy is the job's strategy:. Start expands its matrix when its
	// values are known by then, and Ready otherwise, when it reads needs.
	Strategy *Strategy `json:"strategy,omitzero"`
	// Needs are the jobs this one waits for.
	Needs []Need `json:"needs,omitempty"`
	// If is the job's if:. Unset means success().
	If Value `json:"if,omitzero"`
	// RunsOn is the job's runs-on:, which picks where it runs.
	RunsOn Value `json:"runsOn,omitzero"`
	// Resources is what the job needs from where it runs.
	Resources Value `json:"resources,omitzero"`
	// SSH is the job's format 1 ssh:.
	SSH              Value            `json:"ssh,omitzero"`
	Retry            *JobRetry        `json:"retry,omitzero"`
	Timeout          Value            `json:"timeout,omitzero"`
	Env              map[string]Value `json:"env,omitempty"`
	WorkingDirectory Value            `json:"workingDirectory,omitzero"`
	Steps            []Step           `json:"steps"`
	Outputs          map[string]Value `json:"outputs,omitempty"`
	// Path is the JSON Pointer of the job in the workflow, for messages.
	Path string `json:"path"`
}

// A Need is a dependency on a job: on every one of its instances, or, with
// Any, on whichever finishes first.
type Need struct {
	Job string `json:"job"`
	Any bool   `json:"any,omitzero"`
}

// A Strategy is a matrix to expand once its values are known.
type Strategy struct {
	// Matrix is the strategy's matrix:, include and exclude among it.
	Matrix      Value `json:"matrix"`
	FailFast    Value `json:"failFast,omitzero"`
	MaxParallel Value `json:"maxParallel,omitzero"`
}

// JobRetry is a job's retry:, which runs the job again on another member.
type JobRetry struct {
	Reasons    []string `json:"reasons"`
	MaxRetries Value    `json:"maxRetries,omitzero"`
	Interval   Value    `json:"interval,omitzero"`
}

// A Step is one step of a job.
type Step struct {
	ID   string `json:"id,omitempty"`
	Name Value  `json:"name,omitzero"`
	// If is the step's if:. Unset means success().
	If Value `json:"if,omitzero"`
	// Run is the command the step runs.
	Run Value `json:"run,omitzero"`
	// Uses names a built-in action, or the workflow the step runs.
	Uses string `json:"uses,omitempty"`
	// Workflow is unused: the compiler never sets it. It gives way to the
	// pinned reference of the workflow a step's uses: names, a Source with
	// its Digest, from which the step's child run is compiled when the
	// step runs.
	Workflow         *Plan            `json:"workflow,omitzero"`
	With             Value            `json:"with,omitzero"`
	Env              map[string]Value `json:"env,omitempty"`
	Shell            string           `json:"shell,omitempty"`
	WorkingDirectory Value            `json:"workingDirectory,omitzero"`
	SSH              Value            `json:"ssh,omitzero"`
	Retry            *StepRetry       `json:"retry,omitzero"`
	Timeout          Value            `json:"timeout,omitzero"`
	IgnoreErrors     Value            `json:"ignoreErrors,omitzero"`
	EarlyCancel      string           `json:"earlyCancel,omitempty"`
	// Cleanup is the command the step's cleanup: runs at the end of its job.
	Cleanup Value `json:"cleanup,omitzero"`
	// Path is the JSON Pointer of the step in the workflow, for messages.
	Path string `json:"path"`
}

// StepRetry is a step's retry:.
type StepRetry struct {
	MaxRetries Value `json:"maxRetries,omitzero"`
	Interval   Value `json:"interval,omitzero"`
	Timeout    Value `json:"timeout,omitzero"`
	// CleanOnRetry is the step's clean-on-retry: true or false, or an
	// expression that gives one. The compiler fills in true, the format's
	// default, for a retry: without one. Unset, as in a plan a release
	// wrote before it was a Value, which held only true, means false.
	CleanOnRetry Value `json:"cleanOnRetry,omitzero"`
}

// Exprs returns every expression the plan holds, in its run and its jobs.
// The plan of a workflow that a step runs holds its own.
func (p *Plan) Exprs() []*expr.Expr {
	var out []*expr.Expr
	add := func(vs ...Value) {
		for _, v := range vs {
			out = append(out, v.Exprs()...)
		}
	}
	addAll := func(m map[string]Value) {
		for _, v := range jsonvalue.Sorted(m) {
			add(v)
		}
	}
	addAll(p.Run.DeferredInputs)
	addAll(p.Run.Env)
	add(p.Run.Timeout)
	for _, j := range p.Jobs {
		if s := j.Strategy; s != nil {
			add(s.Matrix, s.FailFast, s.MaxParallel)
		}
		add(j.If, j.RunsOn, j.Resources, j.SSH, j.Timeout, j.WorkingDirectory)
		if r := j.Retry; r != nil {
			add(r.MaxRetries, r.Interval)
		}
		addAll(j.Env)
		for _, s := range j.Steps {
			add(s.Name, s.If, s.Run, s.With, s.WorkingDirectory, s.SSH, s.Timeout, s.IgnoreErrors, s.Cleanup)
			addAll(s.Env)
			if r := s.Retry; r != nil {
				add(r.MaxRetries, r.Interval, r.Timeout, r.CleanOnRetry)
			}
		}
		addAll(j.Outputs)
	}
	return out
}

// Encode encodes a plan as JSON. The same plan always encodes to the same
// bytes. It fails for a plan whose JSON would nest more than MaxDepth
// levels.
func Encode(p *Plan) ([]byte, error) {
	return jsonv2.Marshal(p, encoding)
}

// MaxDepth is how many levels a plan's JSON can nest: the limit of
// encoding/json/jsontext, which writes and reads it. A Value or Data nests
// within the levels around it in the plan, as its Depth says.
const MaxDepth = 10000

var (
	// encoding writes a plan as encoding/json always has, so that its bytes
	// don't change: mappings in the order of their keys, a nil list or
	// mapping as null, <, >, &, U+2028 and U+2029 escaped, and invalid UTF-8
	// replaced. Values and data are written as they are walked.
	encoding = jsonv2.JoinOptions(
		jsonv2.Deterministic(true),
		jsonv2.FormatNilMapAsNull(true),
		jsonv2.FormatNilSliceAsNull(true),
		jsontext.EscapeForHTML(true),
		jsontext.EscapeForJS(true),
		jsontext.AllowInvalidUTF8(true),
		jsontext.AllowDuplicateNames(true),
		jsontext.PreserveRawStrings(true),
		jsonv2.WithMarshalers(jsonv2.JoinMarshalers(
			jsonv2.MarshalToFunc(func(enc *jsontext.Encoder, v Value) error { return encodeValue(enc, v.v) }),
			jsonv2.MarshalToFunc(func(enc *jsontext.Encoder, d Data) error { return jsonv2.MarshalEncode(enc, d.v) }),
		)),
	)
	// decoding reads values and data as they are walked, from JSON that is
	// read once. A key given twice and invalid UTF-8 are errors.
	decoding = jsonv2.WithUnmarshalers(jsonv2.JoinUnmarshalers(
		jsonv2.UnmarshalFromFunc(func(dec *jsontext.Decoder, v *Value) error {
			out, err := decodeValue(dec)
			if err == nil {
				*v = Value{out, true}
			}
			return err
		}),
		jsonv2.UnmarshalFromFunc(func(dec *jsontext.Decoder, d *Data) error {
			out, err := jsonvalue.Decode(dec, number)
			if err == nil {
				d.v = out
			}
			return err
		}),
	))
)

// ErrVersion is what Decode returns for a plan of a version this release
// doesn't read, such as one a newer release wrote.
var ErrVersion = errors.New("plan: unsupported version")

// Decode decodes a plan that Encode encoded. It fails on a plan of another
// version, with ErrVersion, and on keys it doesn't know, which a newer
// release wrote. Keys match exactly, and no key can be given twice.
func Decode(data []byte) (*Plan, error) {
	p := new(Plan)
	err := jsonv2.Unmarshal(data, p, decoding, jsonv2.RejectUnknownMembers(true))
	if err == nil && p.Version == Version {
		return p, nil
	}
	// A plan of another version need not decode as this one, so its version
	// is read on its own, and only when the plan doesn't decode as this
	// version's.
	var head struct {
		Version int `json:"version"`
	}
	if err := jsonv2.Unmarshal(data, &head); err != nil {
		return nil, fmt.Errorf("plan: %w", err)
	}
	if head.Version != Version {
		return nil, fmt.Errorf("%w %d; this release reads version %d", ErrVersion, head.Version, Version)
	}
	return nil, fmt.Errorf("plan: %w", err)
}
