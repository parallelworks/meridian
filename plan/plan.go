package plan

import (
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
	"errors"
	"fmt"
	"runtime/debug"
	"slices"
	"strings"

	"github.com/parallelworks/meridian/expr"
	"github.com/parallelworks/meridian/internal/jsonvalue"
	"github.com/parallelworks/meridian/problems"
)

// Version is the version of the plan encoding this package reads and
// writes: a Plan's, and a ReadyJob's, a Record's, and those of the types
// the worker API's bodies carry of a run, though the API has a version of
// its own (workerapi.Version). Every change to one raises it, an addition
// among them, since decoding refuses a member it doesn't know
// (docs/design.md, Plans across versions). Version 1 is frozen, as its
// schemas describe it (Schemas).
const Version = 1

// modulePath is Meridian's module.
const modulePath = "github.com/parallelworks/meridian"

// Release is the Meridian release this package is, as the module's
// version in the binary's build information says: the main module's, for
// meridian, or the version of Meridian a platform's binary requires. It is
// (devel), as go writes it, for a build of Meridian that isn't a release,
// such as one of a working tree, or of a module that replaces Meridian with
// a directory. The compiler records it in each plan (Run.Release).
var Release = release()

// release returns Release.
func release() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "(devel)"
	}
	m := &info.Main
	if m.Path != modulePath {
		i := slices.IndexFunc(info.Deps, func(d *debug.Module) bool { return d.Path == modulePath })
		if i < 0 {
			return "(devel)"
		}
		m = info.Deps[i]
	}
	if m.Replace != nil {
		m = m.Replace
	}
	if m.Version == "" {
		return "(devel)"
	}
	return m.Version
}

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

// Run is what the plan holds about the run as a whole. Secrets, Derived,
// Private and Variables name values by their paths, as
// JoinPath writes them: keys joined with dots, a key's own . and ~
// written ~1 and ~0.
type Run struct {
	Source Source `json:"source"`
	// Format is the workflow's format.
	Format int `json:"format"`
	// Release is the Meridian release that compiled the plan (Release), as
	// v1.2.3, or (devel) for a build of Meridian that isn't one. It says
	// which build made the plan, for whoever reads the run, and isn't part
	// of what the plan is: the plan's digest leaves it out (Digest), so a
	// rerun's plan, from another build, is the stored one.
	Release     string `json:"release"`
	Name        string `json:"name,omitempty"`
	Description string `json:"description,omitempty"`
	Icon        string `json:"icon,omitempty"`
	Thumbnail   string `json:"thumbnail,omitempty"`
	// Inputs are the run's input values, with defaults applied and ignored
	// inputs left out: a mapping.
	Inputs Data `json:"inputs,omitzero"`
	// Definitions are the workflow's input definitions, on.execute.inputs,
	// as the workflow writes them, with their expressions parsed, but for
	// what Withheld names. Start resolves the inputs again with them, as
	// the compiler did, so that relative references, reads of a definition
	// and lists' templates read what they read as the run was created. They
	// hold no value the run was given.
	Definitions Value `json:"definitions,omitzero"`
	// Withheld are the JSON Pointers, in the workflow, of the parts of the
	// input definitions that Definitions leaves out, as it can't hold a
	// password: each password input's default, a field's of a list's rows
	// among them, and the default of each list whose rows have a password
	// input, whose rows can hold one. The run's secret record holds them,
	// and Start puts them back.
	Withheld []string `json:"withheld,omitempty"`
	// Decisions are the JSON Pointers, in the workflow, of the inputs'
	// hidden, ignore and optional that read an organization or user
	// variable, or a password, as the run was created, in order. Start
	// decides each again, in the order the compiler did (docs/design.md,
	// Starting a run). What each decided can say something of what it
	// read, so the run's secret record holds it, and the plan holds the
	// inputs the same whatever it was: an input whose ignore is one, or
	// one in a group whose is, the plan holds out of Inputs, as a private
	// value or a derived one, and Start drops it if it is ignored; the
	// conditions in such a group are each one, whatever they read; and
	// whether an input one of them decides about is required, Start
	// decides. What a job's needs: decided shows in its Needs, as the plan
	// holds the jobs' graph, read with each input an ignore that is one
	// ignored as ignored. A job's needs: that read one is its
	// Job.NeedsExpr.
	Decisions []string `json:"decisions,omitempty"`
	// Env is the workflow's env:.
	Env map[string]Value `json:"env,omitempty"`
	// Timeout is the run's timeout:.
	Timeout Value `json:"timeout,omitzero"`
	// Remote is the remote context, by name, as the run was created: where
	// a workflow its caller fetched came from, such as remote.repo and
	// remote.branch.
	Remote map[string]string `json:"remote,omitempty"`
	// Sessions is the workflow's sessions:, and SessionNames what the
	// session provider gave for each as the run was created, which
	// sessions.<name> reads. Start makes the sessions from both.
	Sessions     Value             `json:"sessions,omitzero"`
	SessionNames map[string]string `json:"sessionNames,omitempty"`
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
	// same ones: those the compiler was given (compile.Options.Limits).
	Limits Limits `json:"limits,omitzero"`
}

// Limits are Meridian's limits on a run's size, which whoever runs the
// workflow sets, and which aren't part of the format. A zero field is the
// default.
type Limits struct {
	// MaxInstances is how many instances a matrix can have, or 0 for
	// DefaultMaxInstances.
	MaxInstances int `json:"maxInstances,omitzero"`
}

// A Source is where a workflow came from, and the exact text that ran.
type Source struct {
	// Location names the workflow to whoever can see the run: a file, a
	// repository and a path in it, or a workflow a platform stores.
	Location string `json:"location"`
	// Revision is a Git commit, or "sha256:" and the SHA-256 digest of the
	// text when no commit has it.
	Revision string `json:"revision"`
	// Digest is "sha256:" and the SHA-256 digest of the text, which a
	// workflow that a step's uses: names is pinned to beside its commit,
	// so that reading it again by its pin gives the same text or fails.
	Digest string `json:"digest,omitempty"`
	// Ref is what named the workflow, for one a step's uses: names: the
	// Sources resolver reads it again by Ref, at Revision, and gives the
	// remote context a child run of it reads, as remote.branch, from it.
	// It is empty for the run's own workflow.
	Ref SourceRef `json:"ref,omitzero"`
}

// A SourceRef is what a step's uses: names a workflow by, as format 1
// writes it: the uses: itself, and, in the step's with:, the workflow's
// file in the repository it names and the server that holds it.
type SourceRef struct {
	// Uses is the step's uses: as it is written, such as
	// github/<owner>/<repository>@<revision>, gitlab/<namespace>/<project>,
	// workflow/<name> or marketplace/<name>.
	Uses string `json:"uses"`
	// Path is the workflow's file in the repository, the step's with:
	// $yaml, or "" for the one Uses names, or the default.
	Path string `json:"path,omitempty"`
	// Host is the Git server that holds the repository, the step's with:
	// $host, or "" for the one Uses names, such as gitlab.com.
	Host string `json:"host,omitempty"`
}

// DefaultSourcePath is the workflow's file in a repository that a uses:
// names when its $yaml names none.
const DefaultSourcePath = "workflow.yaml"

// Normal returns r as format 1 reads it, as ACTIVATE does, so that two
// references that name the same workflow are equal. For a Git repository's,
// one whose uses: starts with github/ or gitlab/: its uses: without the
// space after it, or, on GitLab, after gitlab/ too, and without one slash
// and then .git after the repository's name, which is before the first @;
// its Path, DefaultSourcePath if it is "", and otherwise from the
// repository's root, as ACTIVATE fetches the file: without the slashes
// before it on GitHub, and without one on GitLab; and its Host in lower case
// without the space around it, gitlab.com if that leaves none, for a
// GitLab project's, and none for a GitHub repository's, whose server a
// $host doesn't name. Any other is r. Normal doesn't check what it reads:
// one it reads that isn't well formed names no workflow.
func (r SourceRef) Normal() SourceRef {
	var rest string
	out := SourceRef{Path: r.Path}
	switch {
	case strings.HasPrefix(r.Uses, "github/"):
		out.Uses = "github/"
		rest = strings.TrimPrefix(strings.TrimSpace(r.Uses), "github/")
		out.Path = strings.TrimLeft(out.Path, "/")
	case strings.HasPrefix(r.Uses, "gitlab/"):
		out.Uses = "gitlab/"
		rest = strings.TrimSpace(strings.TrimPrefix(r.Uses, "gitlab/"))
		if out.Host = strings.ToLower(strings.TrimSpace(r.Host)); out.Host == "" {
			out.Host = "gitlab.com"
		}
		out.Path = strings.TrimPrefix(out.Path, "/")
	default:
		return r
	}
	if r.Path == "" {
		out.Path = DefaultSourcePath
	}
	name, rev, hasRev := strings.Cut(rest, "@")
	out.Uses += strings.TrimSuffix(strings.TrimSuffix(name, "/"), ".git")
	if hasRev {
		out.Uses += "@" + rev
	}
	return out
}

// A Job is one of the workflow's jobs. A matrix job is one Job, whose
// instances are made from its Strategy as the run goes.
type Job struct {
	// Key is the job's name. A matrix instance's key, its name, # and its
	// index, as in build#2, is its Instance's.
	Key string `json:"key"`
	// Name is the job's name in the workflow.
	Name string `json:"name"`
	// Strategy is the job's strategy:. Start expands its matrix when its
	// values are known by then, and Ready otherwise, when it reads needs.
	Strategy *Strategy `json:"strategy,omitzero"`
	// Needs are the jobs this one waits for.
	Needs []Need `json:"needs,omitempty"`
	// NeedsExpr is the job's needs:, when it is an expression that read an
	// organization or user variable, or a password, as the run was
	// created: Start evaluates it again, and fails the run if it gives
	// other jobs than Needs.
	NeedsExpr Value `json:"needsExpr,omitzero"`
	// If is the job's if:. Unset means success(), and null, as an if:
	// written empty is, is false.
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

// RetriesFor reports whether the job's retry: lists reason: whether an
// attempt of the job that ended for it can be followed by another, as its
// max-retries allows, which Ready evaluates (ReadyJobRetry). Its reasons
// are always written out, so the plan says.
func (j *Job) RetriesFor(reason Reason) bool {
	return j.Retry != nil && slices.Contains(j.Retry.Reasons, string(reason))
}

// A Step is one step of a job.
type Step struct {
	ID   string `json:"id,omitempty"`
	Name Value  `json:"name,omitzero"`
	// If is the step's if:. Unset means success(), and null, as an if:
	// written empty is, is false.
	If Value `json:"if,omitzero"`
	// Run is the command the step runs.
	Run Value `json:"run,omitzero"`
	// Uses names a built-in action, or the workflow the step runs.
	Uses string `json:"uses,omitempty"`
	// Source is the workflow the step's uses: names, pinned as the run was
	// created, by its commit and its text's digest: the step's child run is
	// created from it, through the Sources resolver's Read, when the step
	// runs, so the run and its reruns run the same text. It is nil for a
	// built-in action.
	Source           *Source          `json:"source,omitzero"`
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
	// default, for a retry: without one, and unset means it too.
	CleanOnRetry Value `json:"cleanOnRetry,omitzero"`
}

// Exprs returns every expression the plan holds, in its run, its input
// definitions among them, and its jobs.
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
	add(p.Run.Definitions)
	addAll(p.Run.Env)
	add(p.Run.Timeout, p.Run.Sessions)
	for _, j := range p.Jobs {
		add(j.NeedsExpr)
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
			out, err := jsonvalue.Decode(dec, jsonvalue.Number)
			if err == nil {
				d.v = out
			}
			return err
		}),
	))
)

// checkJobOrder returns an error when p's jobs aren't in the order of
// their names, each once (Jobs).
func (p *Plan) checkJobOrder() error {
	for i := 1; i < len(p.Jobs); i++ {
		switch a, b := p.Jobs[i-1].Name, p.Jobs[i].Name; {
		case a == b:
			return fmt.Errorf("the job %q is in the plan twice", b)
		case a > b:
			return fmt.Errorf("the job %q comes after %q, where the jobs are in the order of their names", b, a)
		}
	}
	return nil
}

// ErrVersion is what Decode and DecodeReadyJob return for a plan, or a
// ready job, of a version this release doesn't read, such as one a newer
// release wrote; and for one of this version whose expressions name what
// this release doesn't know: a context, an operator or a function, or a
// function's arguments or an operator's negation it doesn't take
// (expr.ErrUnknown). A format gains these without a new plan version, so a
// newer release wrote that one too, and a release that knows them reads it.
var ErrVersion = errors.New("plan: unsupported version")

// ErrMalformed is what Decode, DecodeRecord and DecodeReadyJob return for
// what isn't a plan, a secret record or a ready job that this release
// reads: what isn't JSON, or isn't a mapping, or names no release's
// version, as one without a version does, or that doesn't decode as this
// release reads its version, as one with a key it doesn't know does, or
// what its version's schema refuses (Schemas). Decoding it again gives the
// same error, so a coordinator fails the run rather than read it again.
var ErrMalformed = errors.New("plan: malformed")

// Decode decodes a plan that Encode encoded. It fails on a plan of another
// version, with ErrVersion, and on what isn't a plan of this one, with
// ErrMalformed: keys it doesn't know, which a newer release wrote, among
// them, a member that is always written that is missing, null where a
// member is never null, and whatever else the version's schema refuses
// (Schemas). Keys match exactly, and no key can be given twice. An integer
// member is a whole number written as Encode writes one: 2.0, 2e0 and -0.0
// are refused, though JSON Schema takes them for integers. It fails too,
// with ErrMalformed, on jobs that aren't in the order of their names, each
// once, as Jobs is, since Ready finds a job by its name in that order. A
// plan of this version whose expressions name a context, an operator or a
// function this release doesn't know, which the schema takes, fails with
// ErrVersion: a newer release wrote it, and one that knows them reads it.
// Decode reports the first of what it refuses that it reads.
func Decode(data []byte) (*Plan, error) {
	p := new(Plan)
	if err := decodeVersioned(data, p, func() int { return p.Version }, "the plan"); err != nil {
		return nil, err
	}
	if err := p.checkJobOrder(); err != nil {
		return nil, fmt.Errorf("%w: the plan: %w", ErrMalformed, err)
	}
	return p, nil
}

// decodeVersioned decodes data into v, whose version version reads once
// it is decoded, as Decode does: it fails with ErrVersion if data is of a
// version this release doesn't read, and with ErrMalformed if it isn't
// what of names, of any release's version, or of this release's.
func decodeVersioned(data []byte, v any, version func() int, of string) error {
	err := jsonv2.Unmarshal(data, v, decoding, jsonv2.RejectUnknownMembers(true))
	if err == nil && version() == Version {
		if err := checkShape(data, v); err != nil {
			return fmt.Errorf("%w: %s: %w", ErrMalformed, of, err)
		}
		return nil
	}
	// One of another version need not decode as this one, so its version
	// is read on its own, and only when it doesn't decode as this
	// version's.
	var head struct {
		Version int `json:"version"`
	}
	switch err := jsonv2.Unmarshal(data, &head); {
	case err != nil:
		return fmt.Errorf("%w: %s: %w", ErrMalformed, of, err)
	case head.Version < 1:
		return fmt.Errorf("%w: %s names no version", ErrMalformed, of)
	case head.Version != Version:
		return fmt.Errorf("%w %d of %s; this release reads version %d", ErrVersion, head.Version, of, Version)
	}
	return decodeError(of, err)
}

// decodeError returns err, the error of decoding what of names, of this
// version: one that wraps ErrVersion if err wraps expr.ErrUnknown, as what
// a newer release of the version wrote, and ErrMalformed otherwise.
func decodeError(of string, err error) error {
	if errors.Is(err, expr.ErrUnknown) {
		return fmt.Errorf("%w: %s names what a newer release of version %d knows: %w", ErrVersion, of, Version, err)
	}
	return fmt.Errorf("%w: %s: %w", ErrMalformed, of, err)
}
