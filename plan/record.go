package plan

import (
	"crypto/sha256"
	"encoding/hex"
	jsonv2 "encoding/json/v2"
	"fmt"
	"maps"
	"slices"

	"github.com/parallelworks/meridian/internal/jsonvalue"
)

// A Record is a run's secret record: what a run needs that its plan and
// its run state can't hold, as docs/design.md says under The secret record.
// The coordinator keeps it, encrypted, in one place for the run, and
// deletes it when the run ends. Create gives its first part, what the run
// was given; Start adds what it read and made that is private, and so does
// each job as it ends. A rerun needs what was submitted again (Reruns).
type Record struct {
	// Version is the record encoding's version, the plan's: a release
	// reads the records of the plans it reads.
	Version int `json:"version"`
	// PlanDigest names the plan the record was made with, by its digest
	// (Digest), which Start checks. The digest leaves out the release that
	// compiled the plan, so a record a rerun's Create made, by another
	// build of Meridian, names the stored plan.
	PlanDigest string `json:"planDigest,omitempty"`

	// Submitted is what the run was submitted, a mapping, as it was given:
	// every input value, passwords among them. Start resolves the inputs
	// again from it, with every variable and password known.
	Submitted Data `json:"submitted,omitzero"`
	// Withheld are the parts of the input definitions the plan leaves out
	// (Run.Withheld), as the workflow writes them, by their JSON Pointers:
	// what can hold a password, such as a password input's default.
	Withheld map[string]Data `json:"withheld,omitempty"`
	// Decisions are what each condition of the run's shape that read a
	// variable or a password decided as the run was created
	// (Run.Decisions), by its JSON Pointer. What one decided can say
	// something of what it read, so the plan doesn't hold it.
	Decisions map[string]bool `json:"decisions,omitempty"`
	// Means names each part of Submitted that means the expression a form
	// made it from, one that reads a variable or a password, as Create
	// decided it: the part was what the expression gave then. Each is
	// named by its path in the inputs' values (JoinPath), as in
	// inputs.region, with the JSON Pointer of the expression in the
	// workflow. Start evaluates the expression in the part's place, so a
	// variable that changed since is read as it is. Which parts these are
	// says something of the variables' values, so the plan doesn't hold it.
	Means map[string]string `json:"means,omitempty"`

	// Run is the private half of the run's values, as Start completed them
	// (RunValues.Split): the variables Start read, and each value that
	// Private and Secrets name.
	Run *RunValues `json:"run,omitempty"`
	// TimeoutSeconds is the run's timeout:, in seconds, when evaluating it
	// read a variable or a password, or nil.
	TimeoutSeconds *int64 `json:"timeoutSeconds,omitempty"`
	// Expansions are the matrices whose instances are private
	// (Instance.Private), by job name, as Start or Ready expanded them.
	Expansions map[string]*Expansion `json:"expansions,omitempty"`
	// Jobs are what each job ended with that is private, by the key of
	// the job or of its matrix instance (JobResult.Split).
	Jobs map[string]JobRecord `json:"jobs,omitempty"`
	// Inherited is what a child run has of the step that calls it that its
	// plan doesn't hold (Run.Parent), or nil. Version 2.
	Inherited *Inherited `json:"inherited,omitzero"`
	// StepRun is what a step run holds of the step's try it runs, or nil
	// for any other run. Version 2.
	StepRun *StepRun `json:"stepRun,omitzero"`
}

// A StepRun is what a step run's secret record holds of the try of the
// step it runs on another host than its job's (docs/design.md, Steps on
// another host): where the step's ssh: put the try, and the state of its
// caller's job then, which the step run's worker runs the step with. A
// step run has no plan of its own, but its caller's, so the record holds
// what the try evaluated, which can read a variable or a secret.
type StepRun struct {
	// SSH is the step's ssh:, as the try evaluated it, which names the
	// host the step runs on; or nil for one that is null, or gives no
	// remoteHost, in a job on a host, which runs where its job would
	// without its ssh:.
	SSH *Inline `json:"ssh,omitzero"`
	// Private is set when evaluating the step's ssh: read a variable or a
	// password, so that where the step runs is private, as a job's target
	// that read one is.
	Private bool `json:"private,omitzero"`
	// State is the state of the caller's job at the try, a mapping, as
	// its worker gave it, which the coordinator keeps but doesn't read.
	State Data `json:"state,omitzero"`
}

// Inherited is what a child run's secret record holds of the step that
// calls it.
type Inherited struct {
	// Env holds the variables of the step's environment that the plan
	// doesn't hold, which ParentRun.Private and Secrets name, by name.
	Env map[string]string `json:"env,omitempty"`
	// Masked are the strings the step's worker masks, as a Masker takes
	// them: the texts of the secrets of the run that calls it, and what its
	// job and step made from one, which the child's jobs mask too, as
	// format 1 has a child mask its caller's secrets.
	Masked []string `json:"masked,omitempty"`
}

// A JobRecord is what the secret record holds of how a job ended: the
// values of its private outputs, and of its steps' outputs that hold a
// secret, which JobResult names but run state doesn't hold.
type JobRecord struct {
	// Outputs are the job's outputs that JobResult.Private or Secrets name,
	// by name.
	Outputs map[string]string `json:"outputs,omitempty"`
	// Steps are the outputs of its steps that JobResult.SecretSteps names,
	// by step id and name.
	Steps map[string]map[string]string `json:"steps,omitempty"`
}

// Split splits r into what the run's secret record holds of it and what run
// state holds. The record holds the values of the outputs r names as
// private or as secrets (Private and Secrets: either is private), and of
// its steps' outputs it names as holding a secret (SecretSteps); run state
// holds the rest, with the names. JoinJobResult puts them together again.
// Neither shares a mapping or a list with r.
func (r JobResult) Split() (record JobRecord, state JobResult) {
	state = r.clone()
	private := setOf(r.Private)
	for _, name := range r.Secrets {
		private[name] = true
	}
	for name, v := range jsonvalue.Sorted(r.Outputs) {
		if !private[name] {
			continue
		}
		if record.Outputs == nil {
			record.Outputs = map[string]string{}
		}
		record.Outputs[name] = v
		delete(state.Outputs, name)
	}
	if len(state.Outputs) == 0 {
		state.Outputs = nil
	}
	for id, outputs := range jsonvalue.Sorted(r.Steps) {
		for _, name := range r.SecretSteps[id] {
			v, ok := outputs[name]
			if !ok {
				continue
			}
			if record.Steps == nil {
				record.Steps = map[string]map[string]string{}
			}
			if record.Steps[id] == nil {
				record.Steps[id] = map[string]string{}
			}
			record.Steps[id][name] = v
			delete(state.Steps[id], name)
		}
		if len(state.Steps[id]) == 0 {
			delete(state.Steps, id)
		}
	}
	if len(state.Steps) == 0 {
		state.Steps = nil
	}
	return record, state
}

// JoinJobResult returns how a job ended from what Split split it into. It
// shares no mapping or list with either.
func JoinJobResult(record JobRecord, state JobResult) JobResult {
	out := state.clone()
	for name, v := range record.Outputs {
		if out.Outputs == nil {
			out.Outputs = map[string]string{}
		}
		out.Outputs[name] = v
	}
	for id, outputs := range record.Steps {
		if out.Steps == nil {
			out.Steps = map[string]map[string]string{}
		}
		if out.Steps[id] == nil {
			out.Steps[id] = map[string]string{}
		}
		maps.Copy(out.Steps[id], outputs)
	}
	return out
}

// clone returns a copy of r that shares no mapping or list with it.
func (r JobResult) clone() JobResult {
	out := r
	out.Outputs = maps.Clone(r.Outputs)
	out.Private, out.Secrets, out.Derived = slices.Clone(r.Private), slices.Clone(r.Secrets), slices.Clone(r.Derived)
	out.Steps = nil
	for id, outputs := range r.Steps {
		if out.Steps == nil {
			out.Steps = make(map[string]map[string]string, len(r.Steps))
		}
		out.Steps[id] = maps.Clone(outputs)
	}
	out.SecretSteps, out.DerivedSteps = cloneNames(r.SecretSteps), cloneNames(r.DerivedSteps)
	return out
}

// cloneNames returns a copy of names, lists of names by key, that shares no
// list with it.
func cloneNames(names map[string][]string) map[string][]string {
	if names == nil {
		return nil
	}
	out := make(map[string][]string, len(names))
	for k, v := range names {
		out[k] = slices.Clone(v)
	}
	return out
}

// Digest returns the SHA-256 digest of p's encoding as it reads back,
// without its release (Run.Release), as "sha256:" and its hex: the same
// for p as for the plan Decode gives of what Encode gave of it, as a
// coordinator stores it. A secret record names the plan it was made with
// by it (Record.PlanDigest). The release says which build compiled the
// plan, which isn't what the plan is: a rerun's Create, by another build
// than the stored plan's, a release or not, gives the same digest for the
// same plan. A plan holds no secret and no variable's value, so neither
// does its digest.
func Digest(p *Plan) (string, error) {
	data, err := Encode(digested(p))
	if err != nil {
		return "", err
	}
	// What Decode reads can encode otherwise than what was encoded, as
	// -0 reads as 0.
	back, err := Decode(data)
	if err != nil {
		return "", err
	}
	if data, err = Encode(back); err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

// HasDigest reports whether p's digest (Digest) is digest. A plan that
// Decode gave, as a coordinator's, encodes as it reads back, so one
// encoding is most often enough.
func HasDigest(p *Plan, digest string) (bool, error) {
	data, err := Encode(digested(p))
	if err != nil {
		return false, err
	}
	sum := sha256.Sum256(data)
	if "sha256:"+hex.EncodeToString(sum[:]) == digest {
		return true, nil
	}
	got, err := Digest(p)
	return got == digest, err
}

// digested returns p as Digest digests it: without its release, in a copy
// that shares the rest with p.
func digested(p *Plan) *Plan {
	if p.Run.Release == "" {
		return p
	}
	q := *p
	q.Run.Release = ""
	return &q
}

// EncodeRecord encodes a secret record as JSON, as Encode encodes a plan:
// the same record always encodes to the same bytes. The coordinator
// encrypts what it gives.
func EncodeRecord(r *Record) ([]byte, error) {
	return jsonv2.Marshal(r, encoding)
}

// DecodeRecord decodes a secret record that EncodeRecord encoded. As
// Decode does a plan, it fails on a record of another version, with
// ErrVersion, and with ErrMalformed on what isn't a record of this one: a
// key it doesn't know, what was submitted that isn't a mapping, null among
// it, and whatever else the version's schema refuses (Schemas).
func DecodeRecord(data []byte) (*Record, error) {
	r := new(Record)
	if err := decodeVersioned(data, r, func() int { return r.Version }, "the secret record"); err != nil {
		return nil, err
	}
	if _, ok := r.Submitted.Get().(map[string]any); !ok && !r.Submitted.IsZero() {
		return nil, fmt.Errorf("%w: the secret record: what was submitted isn't a mapping", ErrMalformed)
	}
	if sr := r.StepRun; sr != nil {
		if _, ok := sr.State.Get().(map[string]any); !ok && !sr.State.IsZero() {
			return nil, fmt.Errorf("%w: the secret record: a step run's state isn't a mapping", ErrMalformed)
		}
	}
	if (r.Inherited != nil || r.StepRun != nil) && r.Version < 2 {
		// What a later version added isn't its earlier versions', which
		// their schemas refuse.
		return nil, fmt.Errorf("%w: the secret record of version %d has what version 2 adds", ErrMalformed, r.Version)
	}
	return r, nil
}
