// Package backend defines execution groups, their members, and the
// interface through which the coordinator starts workers on a member:
// local, ssh, kubernetes, slurm or pbs, each a package under this one.
//
// This package holds only types and interfaces, so that the compiler,
// which builds to WebAssembly, can read a group without importing a
// backend.
package backend

import "context"

// A Group is an execution group: a named source of capacity, whose members
// a job is placed on in order (docs/design.md, Execution groups).
type Group struct {
	Name    string   `json:"name"`
	Members []Member `json:"members"`
}

// A Member is one place a group can run jobs: a backend, its settings and
// the resources it offers.
type Member struct {
	// Name identifies the member in its group, in an attempt's record.
	Name string `json:"name"`
	// Backend names the backend that starts workers on the member: local,
	// ssh, kubernetes, slurm or pbs.
	Backend string `json:"backend"`
	// Settings are the backend's settings for the member, such as an SSH
	// host, a Slurm partition and account, or a Kubernetes namespace, as
	// JSON. Each backend defines its own.
	Settings map[string]any `json:"settings,omitempty"`
	// Isolation is how the member isolates jobs: the container runtime it
	// names, such as podman, docker or apptainer, or host. "" is the
	// backend's default, and a member never gets weaker isolation than it
	// names: a member whose runtime is missing fails its jobs.
	Isolation string `json:"isolation,omitempty"`
	// Resources are what the member offers a job.
	Resources Resources `json:"resources,omitzero"`
}

// Resources are what a member offers a job. A job fits a member that
// offers at least what its resources: asks for, and a job that asks for
// nothing fits every member. A zero field offers none of that resource.
type Resources struct {
	// MilliCPU is in thousandths of a core.
	MilliCPU int64 `json:"milliCPU,omitzero"`
	// Memory is in bytes.
	Memory int64 `json:"memory,omitzero"`
	GPU    int   `json:"gpu,omitzero"`
}

// A Backend starts workers on its members. The coordinator holds one for
// each backend a group's members name, and calls it for whichever replica
// handles a job, so a Backend keeps no state of its own between calls:
// what it needs to find a placement again is in the placement.
type Backend interface {
	// Start places a worker for an attempt on m. It returns once the
	// backend has the work, as a process started, a pod created or a batch
	// job submitted, not once the worker runs.
	//
	// Placement is idempotent. The placement carries the attempt's ID, in
	// a Slurm or PBS job's name, a pod's label or a process's arguments,
	// and Start first looks for one that carries it, as Find does, and
	// returns that one rather than place the attempt twice. A coordinator
	// that crashed between placing and recording a placement places it
	// again with the same request.
	Start(ctx context.Context, m *Member, req StartRequest) (Placement, error)
	// Find returns the placement on m that carries an attempt's ID, and
	// false if there is none.
	Find(ctx context.Context, m *Member, attempt string) (Placement, bool, error)
	// Check reports whether a placement's worker is alive: whether a
	// process runs, a pod hasn't ended, or a scheduler still has the job,
	// queued or running. A worker that stops heartbeating is checked here
	// before it is declared lost. A check that can't decide, such as one
	// over an SSH connection that is down, returns Unknown and the error
	// that says why, and the attempt isn't lost.
	Check(ctx context.Context, m *Member, p Placement) (State, error)
	// Cancel withdraws a placement: it removes a scheduler's job (scancel,
	// qdel), deletes a pod, or stops a process. The coordinator tells a
	// running worker to stop through the worker API first, and cancels its
	// placement only when that fails or a grace period passes. Cancelling a
	// placement that has ended is not an error.
	Cancel(ctx context.Context, m *Member, p Placement) error
}

// A StartRequest is what a backend starts a worker with.
type StartRequest struct {
	// Attempt is the attempt's ID, unique across runs, which the placement
	// carries.
	Attempt string
	// Name is a name for the work that a person can read, such as the run's
	// ID and the job's key, for a scheduler's queue.
	Name string
	// Coordinator is the address the worker reaches the coordinator at, and
	// KeyHash the hash of its TLS public key, or "" for a Unix socket, as
	// workerapi.EnvCoordinator and workerapi.EnvKeyHash give them to the
	// worker.
	Coordinator string
	KeyHash     string
	// BootstrapToken is the attempt's bootstrap token: random, single-use
	// and short-lived, which the worker exchanges for the attempt's
	// credential. It goes in the worker's environment, or in a file only
	// the user can read (workerapi.EnvBootstrapToken and
	// workerapi.EnvBootstrapTokenFile), and never on a command line, where
	// other users could see it, nor in a log.
	BootstrapToken string
	// TimeoutSeconds is the job's timeout:, in seconds, or 0 for none. A
	// scheduler is asked for it, plus a margin the member sets, as the
	// job's walltime.
	TimeoutSeconds int64
	// Resources is the job's resources:, as Ready evaluated it, which a
	// scheduler or Kubernetes is asked for; nil for none.
	Resources any
}

// A Placement is a worker that a backend started for an attempt. The
// coordinator records it with the attempt.
type Placement struct {
	// Attempt is the ID of the attempt it is for.
	Attempt string `json:"attempt"`
	// ID is the backend's own name for it: a Slurm or PBS job's ID, a pod's
	// name, or a process's ID.
	ID string `json:"id"`
}

// A State is what a backend says of a placement's worker.
type State int

const (
	// Unknown is a check that couldn't decide. It counts as not lost.
	Unknown State = iota
	// Alive is a worker that runs, or will once a scheduler starts it.
	Alive
	// Dead is a worker that has ended or will never start.
	Dead
	// TimedOut is a worker that a scheduler ended at its job's time limit,
	// as Slurm's TIMEOUT state says: dead, but timed out rather than lost
	// (plan.ReasonTimeout).
	TimedOut
)
