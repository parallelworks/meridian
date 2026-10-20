package plan

import (
	"encoding/json"
	jsonv2 "encoding/json/v2"
	"errors"
	"fmt"

	"github.com/parallelworks/meridian/format"
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
	// Jobs are the run's job instances, in the order of their keys.
	Jobs []Job `json:"jobs"`
	// Warnings are the compiler's warnings, which are kept with the run so
	// that they reach whoever runs the workflow.
	Warnings []format.Diagnostic `json:"warnings,omitempty"`
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
	// only once the run is underway, such as a secret variable, by their
	// input's path, as in group.name. They are evaluated as the run starts,
	// with the other inputs' values, and give the inputs' values.
	DeferredInputs map[string]Value `json:"deferredInputs,omitempty"`
	// Env is the workflow's env:.
	Env map[string]Value `json:"env,omitempty"`
	// Timeout is the run's timeout:.
	Timeout Value `json:"timeout,omitzero"`
	// Secrets names the secret variables the run reads. Their values are
	// never in the plan; each attempt receives them when it is claimed.
	Secrets []string `json:"secrets,omitempty"`
	// Permissions are the workflow's permissions:.
	Permissions []string `json:"permissions,omitempty"`
}

// A Source is where a workflow came from, and the exact text that ran.
type Source struct {
	// Location names the workflow: a file, a repository and a path in it,
	// or a workflow a platform stores.
	Location string `json:"location"`
	// Revision is a Git commit, or "sha256:" and the SHA-256 digest of the
	// text when no commit has it.
	Revision string `json:"revision"`
}

// A Job is one instance of a job: the job itself, or one combination of its
// matrix values.
type Job struct {
	// Key identifies the instance in the run: the job's name, or for a
	// matrix instance its name, a # and its index, as in build#2.
	Key string `json:"key"`
	// Name is the job's name in the workflow.
	Name string `json:"name"`
	// Matrix holds a matrix instance's values: a mapping.
	Matrix Data `json:"matrix,omitzero"`
	// Strategy is the matrix of a job whose values aren't known until its
	// dependencies have finished, which the coordinator expands then. A job
	// with a Strategy has no instances yet.
	Strategy *Strategy `json:"strategy,omitempty"`
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
	Retry            *JobRetry        `json:"retry,omitempty"`
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
	Any bool   `json:"any,omitempty"`
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
	// Workflow is the compiled workflow a step's uses: runs.
	Workflow         *Plan            `json:"workflow,omitempty"`
	With             Value            `json:"with,omitzero"`
	Env              map[string]Value `json:"env,omitempty"`
	Shell            string           `json:"shell,omitempty"`
	WorkingDirectory Value            `json:"workingDirectory,omitzero"`
	SSH              Value            `json:"ssh,omitzero"`
	Retry            *StepRetry       `json:"retry,omitempty"`
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
	MaxRetries   Value `json:"maxRetries,omitzero"`
	Interval     Value `json:"interval,omitzero"`
	Timeout      Value `json:"timeout,omitzero"`
	CleanOnRetry bool  `json:"cleanOnRetry,omitempty"`
}

// Encode encodes a plan as JSON. The same plan always encodes to the same
// bytes.
func Encode(p *Plan) ([]byte, error) {
	return json.Marshal(p)
}

// ErrVersion is what Decode returns for a plan of a version this release
// doesn't read, such as one a newer release wrote.
var ErrVersion = errors.New("plan: unsupported version")

// Decode decodes a plan that Encode encoded. It fails on a plan of another
// version, with ErrVersion, and on keys it doesn't know, which a newer
// release wrote. Keys match exactly, and no key can be given twice.
func Decode(data []byte) (*Plan, error) {
	var head struct {
		Version int `json:"version"`
	}
	if err := jsonv2.Unmarshal(data, &head); err != nil {
		return nil, fmt.Errorf("plan: %w", err)
	}
	if head.Version != Version {
		return nil, fmt.Errorf("%w %d; this release reads version %d", ErrVersion, head.Version, Version)
	}
	p := new(Plan)
	if err := jsonv2.Unmarshal(data, p, jsonv2.RejectUnknownMembers(true)); err != nil {
		return nil, fmt.Errorf("plan: %w", err)
	}
	return p, nil
}
