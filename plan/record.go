package plan

import (
	jsonv2 "encoding/json/v2"
	"fmt"
	"maps"
	"slices"
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
}

// A JobRecord is what the secret record holds of how a job ended: the
// values of its private outputs, and of its steps' outputs that hold a
// secret, which JobResult names but run state doesn't hold.
type JobRecord struct {
	// Outputs are the job's outputs that JobResult.Private names, by name.
	Outputs map[string]string `json:"outputs,omitempty"`
	// Steps are the outputs of its steps that JobResult.SecretSteps names,
	// by step id and name.
	Steps map[string]map[string]string `json:"steps,omitempty"`
}

// Split splits r into what the run's secret record holds of it, the values
// of the outputs it names as private and of the steps' outputs it names as
// holding a secret, and what run state holds: the rest, with the names.
// JoinJobResult puts them together again. Neither shares a mapping with r.
func (r JobResult) Split() (record JobRecord, state JobResult) {
	state = r
	state.Outputs = maps.Clone(r.Outputs)
	for _, name := range r.Private {
		if v, ok := state.Outputs[name]; ok {
			if record.Outputs == nil {
				record.Outputs = map[string]string{}
			}
			record.Outputs[name] = v
			delete(state.Outputs, name)
		}
	}
	if len(state.Outputs) == 0 {
		state.Outputs = nil
	}
	state.Steps = nil
	for id, outputs := range r.Steps {
		secret := r.SecretSteps[id]
		for name, v := range outputs {
			to := &state.Steps
			if slices.Contains(secret, name) {
				to = &record.Steps
			}
			if *to == nil {
				*to = map[string]map[string]string{}
			}
			if (*to)[id] == nil {
				(*to)[id] = map[string]string{}
			}
			(*to)[id][name] = v
		}
	}
	return record, state
}

// JoinJobResult returns how a job ended from what Split split it into. It
// shares no mapping with either.
func JoinJobResult(record JobRecord, state JobResult) JobResult {
	out := state
	out.Outputs = maps.Clone(state.Outputs)
	for name, v := range record.Outputs {
		if out.Outputs == nil {
			out.Outputs = map[string]string{}
		}
		out.Outputs[name] = v
	}
	out.Steps = nil
	for _, steps := range []map[string]map[string]string{state.Steps, record.Steps} {
		for id, outputs := range steps {
			if out.Steps == nil {
				out.Steps = map[string]map[string]string{}
			}
			if out.Steps[id] == nil {
				out.Steps[id] = map[string]string{}
			}
			maps.Copy(out.Steps[id], outputs)
		}
	}
	return out
}

// EncodeRecord encodes a secret record as JSON, as Encode encodes a plan:
// the same record always encodes to the same bytes. The coordinator
// encrypts what it gives.
func EncodeRecord(r *Record) ([]byte, error) {
	return jsonv2.Marshal(r, encoding)
}

// DecodeRecord decodes a secret record that EncodeRecord encoded. As
// Decode does a plan, it fails on a record of another version, with
// ErrVersion, and on keys it doesn't know.
func DecodeRecord(data []byte) (*Record, error) {
	r := new(Record)
	err := jsonv2.Unmarshal(data, r, decoding, jsonv2.RejectUnknownMembers(true))
	if err == nil && r.Version == Version {
		return r, nil
	}
	var head struct {
		Version int `json:"version"`
	}
	if err := jsonv2.Unmarshal(data, &head); err != nil {
		return nil, fmt.Errorf("plan: the secret record: %w", err)
	}
	if head.Version != Version {
		return nil, fmt.Errorf("%w %d of the secret record; this release reads version %d", ErrVersion, head.Version, Version)
	}
	return nil, fmt.Errorf("plan: the secret record: %w", err)
}
