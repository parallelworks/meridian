// Package backend defines execution groups, their members, and the Driver
// through which the coordinator starts workers on a member: local, ssh,
// kubernetes, slurm or pbs, each a package under this one.
//
// This package holds only types and the interface, so that the compiler,
// which builds to WebAssembly, can name a group without importing a
// backend.
//
// # Errors
//
// An error from a Driver that is, or wraps, a foundation *problem.Problem
// says what is wrong with the member or with what was asked of it, such as
// a scheduler that rejected a job: the coordinator records it with the
// attempt, and its Code is the reason it compares, so that a job two
// members fail for the same reason fails. Any other error means the driver
// couldn't answer, such as a host it can't reach, and the coordinator
// tries again.
package backend

import (
	"context"
	"encoding/json/jsontext"
	"errors"
	"io"
	"net"

	"github.com/parallelworks/foundation/problem"
)

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
	// Backend names the backend whose Driver starts workers on the member:
	// local, ssh, kubernetes, slurm or pbs.
	Backend string `json:"backend"`
	// Settings are the backend's settings for the member, as JSON, which
	// its driver decodes, refusing what it doesn't know: an SSH host, a
	// Slurm partition and account, a Kubernetes namespace. The ssh
	// backend's are format 1's ssh: keys.
	Settings jsontext.Value `json:"settings,omitempty"`
	// Isolation is how the member isolates jobs: the container runtime it
	// names, such as podman, docker or apptainer, or host. "" is the
	// backend's default, and a member never gets weaker isolation than it
	// names: one whose runtime is missing fails its jobs.
	Isolation string `json:"isolation,omitempty"`
	// Resources are the most the member offers a job.
	Resources Resources `json:"resources,omitzero"`
	// MaxWalltimeSeconds is the longest walltime the member allows a job,
	// MarginSeconds what it adds to a job's timeout: for its walltime, for
	// starting the worker, pulling the image, running cleanup: commands and
	// reporting, and DefaultTimeoutSeconds the timeout of a job that has
	// none. Each is 0 when the member doesn't say, and its driver's Probe
	// may know it, as a scheduler's partition does.
	MaxWalltimeSeconds    int64 `json:"maxWalltimeSeconds,omitzero"`
	MarginSeconds         int64 `json:"marginSeconds,omitzero"`
	DefaultTimeoutSeconds int64 `json:"defaultTimeoutSeconds,omitzero"`
}

// Resources are CPUs, memory and GPUs: the most a member offers, or what
// a job asks of one (Demand), from the plan.Resources Ready parsed, in the
// same units. A zero field is undeclared: a job that doesn't declare one
// asks for none of it, and a member that doesn't leaves it to its
// driver's Probe.
type Resources struct {
	// MilliCPU is in thousandths of a core: 4 is 4000, and 500m 500.
	MilliCPU int64 `json:"milliCPU,omitzero"`
	// Memory is in bytes: 8Gi is 8 << 30.
	Memory int64 `json:"memory,omitzero"`
	GPU    int   `json:"gpu,omitzero"`
}

// A Driver starts workers on the members of one backend. The coordinator
// holds one for each backend its groups' members name, and any of its
// replicas calls it for any job, so a Driver keeps no state between calls
// that another replica would need: what it needs to find a placement again
// is in the placement, and in the member.
type Driver interface {
	// Probe says whether m can run a job that asks for demand now: whether
	// it is healthy and can offer what the job asks for, within its longest
	// walltime, and if so how soon it would start it. Placement leaves out
	// a member whose Fit has a Problem, and ranks the rest by their
	// Availability, in the group's order within each.
	Probe(ctx context.Context, m *Member, demand Demand) (Fit, error)
	// Connect makes m's workers able to reach the coordinator, at
	// coordinator, where this replica listens, and returns the endpoint
	// they reach it at from where they run: the coordinator's own Unix
	// socket for local; or meridian relay on a login node, which forwards
	// their TLS connections over SSH, for slurm and pbs. The workers of a
	// Forwarder's members, as the ssh backend's, reach it through a
	// listener on the member instead, which the coordinator serves.
	//
	// One replica at a time holds a member's connection, under a lease in
	// the store, and serves every attempt that reaches the coordinator
	// through it, of whichever job, as any replica can serve any job. It
	// calls Connect when it takes the lease, and closes what Connect set up
	// only when it gives the lease up. Connect returns the same endpoint
	// each time for the same member, so that the member's running workers,
	// placed through an earlier Connect, by this replica or one that has
	// stopped, reach the replica that took the lease over.
	Connect(ctx context.Context, m *Member, coordinator Endpoint) (Endpoint, io.Closer, error)
	// Place starts a worker for an attempt on m, and returns once the
	// backend has the work, as a process started, a pod created or a batch
	// job submitted, not once the worker runs.
	//
	// Placing is idempotent. The placement carries the attempt's ID, in a
	// Slurm or PBS job's name, a pod's label or a process's arguments. The
	// coordinator calls Find first, and makes the attempt's bootstrap token
	// and calls Place only when Find found none, so a coordinator that
	// crashed between placing and recording a placement finds it, and never
	// places the attempt twice.
	Place(ctx context.Context, m *Member, req PlaceRequest) (Placement, error)
	// Find returns the placement on m that carries an attempt's ID, and
	// false if there is none.
	Find(ctx context.Context, m *Member, attempt string) (Placement, bool, error)
	// Check says what has become of a placement's worker. A worker that
	// stops heartbeating is checked here before it is declared lost. A
	// check that can't decide, such as one over an SSH connection that is
	// down, gives Unknown and an error that says why, and the attempt isn't
	// lost. An error that is a problem says the check can't decide for a
	// cause that won't pass by itself, such as a host key that changed: the
	// coordinator takes the worker to run for a while
	// (coordinator.Timing.CheckGrace), and then loses the attempt with the
	// problem.
	Check(ctx context.Context, m *Member, p Placement) (Status, error)
	// Cancel withdraws a placement: it removes a scheduler's job (scancel,
	// qdel), deletes a pod, or stops a process. The coordinator tells a
	// running worker to stop through the worker API first, and cancels its
	// placement only when that fails or a grace period passes. Cancelling a
	// placement that has ended is not an error.
	Cancel(ctx context.Context, m *Member, p Placement) error
	// Forget removes what the driver keeps of a placement whose worker has
	// ended, once the coordinator has recorded how it ended, such as a
	// local worker's placement file and its log, so that what it keeps
	// doesn't grow. A placement whose worker still runs is kept, and an
	// error; one that isn't there is none.
	Forget(ctx context.Context, m *Member, p Placement) error
}

// A Forwarder is a Driver whose members' workers can't reach the
// coordinator where it listens, but through a listener the driver opens on
// the member, as the ssh backend's forwards a Unix socket on a host over
// SSH: the coordinator serves the worker API on what that listener
// accepts, to the attempts placed on the member, and on no other.
//
// One replica at a time forwards a member, under a lease in the store
// named by the member's key; the others place workers on it all the same,
// at the endpoint the one that holds the lease forwards. Connect isn't
// called on a Forwarder.
type Forwarder interface {
	Driver
	// MemberKey returns the key of m, which the store records with the
	// attempts placed on it, and names its lease by: the same for every
	// member that reaches the same place as the same user, whatever else
	// it is called, and nothing a person could read a host or a user
	// from. It is the name of an endpoint (ssh- and lower-case hex).
	MemberKey(m *Member) (string, error)
	// Endpoint returns where m's workers reach the coordinator from where
	// they run, as Forward listens there: the same each time for the same
	// member.
	Endpoint(ctx context.Context, m *Member) (Endpoint, error)
	// Forward listens at m's endpoint, on m, for this replica, and returns
	// the listener, whose connections are m's workers'. It ends with the
	// connection it is made over, or once it is closed.
	Forward(ctx context.Context, m *Member) (net.Listener, error)
}

// Demand is what a job asks of a member.
type Demand struct {
	Resources Resources
	// TimeoutSeconds is the job's timeout:, or 0 when it has none, and the
	// member's default applies.
	TimeoutSeconds int64
	// Parked are the attempts parked for the job: its run's callers, the
	// attempt whose step called the run, and that attempt's run's caller,
	// up to the root, each of whose workers waits for the child run below
	// it. Parked, each keeps its lease, but lends what it asked of its
	// member to its own descendants, which end before it goes on, so that
	// callers that wait don't starve their own children; never to another
	// job, which would still hold it when the caller went on. A driver
	// that counts what it placed against its member, as local does, leaves
	// these out, and ignores the IDs of attempts it didn't place. One that
	// can't release what a placement holds, as a batch scheduler's
	// allocation, ignores them all.
	Parked []string
	// Tries is how many of the coordinator's looks to place the job, in a
	// row, since it was last placed, found that a member's backend
	// couldn't answer: a driver that tries to reach a member as it probes
	// it, as the ssh backend does, counts the job's own tries by it, so
	// that a job gives up on a member only after tries of its own. A look
	// whose Probe answered ErrNotYet isn't counted.
	Tries int
}

// ErrNotYet is what a driver's Probe returns, or wraps, for a member it
// hasn't tried for the job yet, but will, as one another job is reaching
// now, or one the driver waits to try again after failing to: the
// coordinator asks again soon, and doesn't count the look as one the
// backend couldn't answer (Demand.Tries).
var ErrNotYet = errors.New("backend: the member hasn't been tried for the job yet")

// A Fit is what a member says of a job: whether it can run it, and how
// soon.
type Fit struct {
	// Problem says why the member can't run the job: it is unhealthy,
	// can't offer what the job asks for, or allows no walltime as long as
	// the job's timeout and its margin. It is nil when it can.
	Problem *problem.Problem
	// Availability is how soon the member would start the job.
	Availability Availability
	// MaxWalltimeSeconds, MarginSeconds and DefaultTimeoutSeconds are the
	// member's: its Member's, or else what its backend says, such as a
	// partition's limit and default. Each is 0 when there is none.
	MaxWalltimeSeconds    int64
	MarginSeconds         int64
	DefaultTimeoutSeconds int64
}

// Availability is how soon a member would start a job, which placement
// ranks members by, the lower first. The zero Availability is none, a
// Fit's with a Problem.
type Availability int

const (
	// StartsNow is a member with what the job asks for free now.
	StartsNow Availability = iota + 1
	// HasCapacity is a member that has what the job asks for, but busy:
	// it starts the job when work of its own ends.
	HasCapacity
	// WouldQueue is a member whose scheduler would queue the job.
	WouldQueue
)

// An Endpoint is where a worker reaches the coordinator.
type Endpoint struct {
	// Address is a Unix socket's path, which starts with /, or a host and
	// port, as in login1:7443, for TLS 1.3.
	Address string
	// KeyHash is sha256: and the hex of the SHA-256 digest of the
	// coordinator's TLS public key, as DER SubjectPublicKeyInfo, which the
	// worker checks the connection against; "" for a Unix socket. TLS ends
	// at the coordinator, not at a relay, and its replicas share one key,
	// so the hash holds whichever replica a worker reaches.
	KeyHash string
}

// A PlaceRequest is what a driver starts a worker with.
type PlaceRequest struct {
	// Attempt is the attempt's ID, unique across runs, which the placement
	// carries.
	Attempt string
	// Name is a name for the work that a person can read, such as the run's
	// ID and the job's key, for a scheduler's queue.
	Name string
	// Coordinator is where the worker reaches the coordinator, as Connect
	// gave it.
	Coordinator Endpoint
	// BootstrapToken is the attempt's bootstrap token: random and
	// single-use, valid until the attempt is claimed or its placement
	// withdrawn, however long a scheduler queues it. The coordinator keeps
	// only its hash. It goes in the worker's environment, or in a file only
	// the user can read, and never on a command line, where other users
	// could see it, nor in a log.
	BootstrapToken string
	// TimeoutSeconds is the job's timeout, or the member's default, in
	// seconds, or 0 for none. A scheduler is asked for it, and the
	// member's margin, as the job's walltime.
	TimeoutSeconds int64
	// Resources are what the job's resources: asks for.
	Resources Resources
	// Image is the image the job's steps run in, as repository@sha256:
	// and its digest, or "" for steps that run on the host.
	Image string
	// Run is the ID of the attempt's run, which, with the store's, names
	// the run's directory on a member that chooses its own, as a host the
	// ssh backend reaches does.
	Run string
	// RunDir is the run's directory on the member, where steps run by
	// default.
	RunDir string
	// Root is, for an attempt of a child run, the ID of its tree's root,
	// and Path the child's directory relative to the root's, as clean
	// as filepath.Rel gives it: "." for a child whose directory is its
	// root's. A member that chooses a run's directory itself, as a host
	// the ssh backend reaches does, places a child's at its root's there
	// joined with Path, as format 1 lays a tree's directories out the
	// same on each host as where the run started. Both are "" for a root,
	// and a member that takes RunDir as it is ignores them.
	Root, Path string
	// SchedulerParams are the scheduler's parameters a format 1 runs-on:
	// gives, as JSON, or nothing.
	SchedulerParams jsontext.Value
	// TraceParent is the W3C traceparent of the attempt's span, which the
	// worker continues, or "".
	TraceParent string
}

// A Placement is a worker that a driver started for an attempt. The
// coordinator records it with the attempt.
type Placement struct {
	// Attempt is the ID of the attempt it is for.
	Attempt string `json:"attempt"`
	// ID is the backend's own name for it: a Slurm or PBS job's ID, a pod's
	// name, or a process's ID with its start time, so that another process
	// given the same ID later isn't taken for it.
	ID string `json:"id"`
	// WorkerDigest is sha256: and the hex of the SHA-256 digest of the
	// worker binary the driver started. Every driver must give it, with
	// every placement, Place's and Find's: the coordinator records it with
	// the attempt, refuses to record a placement without it, counts an
	// attempt as placed only once it has it, which is what a private
	// placement shows of itself in the clear, and refuses the claims of an
	// attempt until then, and a claim from a worker that says another
	// (workerapi.WorkerDigestMismatch).
	WorkerDigest string `json:"workerDigest,omitempty"`
	// RunDir is the run's directory on the member, where the worker runs
	// the job's steps by default: the PlaceRequest's, or one the member
	// chose, as a host the ssh backend reaches does, under its user's
	// home, and for a child run in its root's there (PlaceRequest.Path),
	// which the coordinator records with the attempt, and gives the
	// worker as it claims it. Find gives it too, but "" where the driver
	// doesn't know it, for the PlaceRequest's.
	RunDir string `json:"runDir,omitempty"`
}

// A Status is what has become of a placement's worker.
type Status struct {
	State State
	// End is why a Dead placement's worker ended, when its backend knows.
	End End
}

// A State is where a placement's worker is.
type State int

const (
	// Unknown is a check that couldn't decide. It counts as not lost.
	Unknown State = iota
	// Queued is a worker that a scheduler holds, that hasn't started.
	Queued
	// Running is a worker that runs.
	Running
	// Dead is a worker that has ended, or will never start.
	Dead
)

// An End is why a placement's worker ended, as its backend says. The
// coordinator turns it into the attempt's reason: EndTimedOut into
// plan.ReasonTimeout and EndOutOfMemory into plan.ReasonOutOfMemory; any
// other end of a worker that didn't complete its attempt is
// plan.ReasonWorkerLost once a step has started, and
// plan.ReasonSetupFailed before.
type End string

const (
	// EndExited is a worker that exited, as one does once it has completed
	// its attempt.
	EndExited End = "exited"
	// EndTimedOut is a worker a scheduler ended at its job's time limit,
	// as Slurm's TIMEOUT state says.
	EndTimedOut End = "timeout"
	// EndOutOfMemory is a worker a scheduler or the kernel ended for using
	// more memory than its job may.
	EndOutOfMemory End = "out-of-memory"
	// EndPreempted is a worker a scheduler ended to make room for other
	// work.
	EndPreempted End = "preempted"
	// EndNodeFailed is a worker whose node failed.
	EndNodeFailed End = "node-failed"
	// EndWithdrawn is a placement that Cancel withdrew.
	EndWithdrawn End = "withdrawn"
)
