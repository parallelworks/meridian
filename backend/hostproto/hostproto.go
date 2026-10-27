// Package hostproto is the protocol of meridian host, the hidden command
// through which a coordinator drives the local backend (backend/local) on
// another host, over a channel it opens there, such as an SSH exec session:
// the ssh backend's helper on the host.
//
// # Framing
//
// The coordinator runs meridian host with one argument, the operation, one
// of the Op constants, and nothing secret in its arguments or its
// environment. Each message is one JSON object on a line of its own, in
// UTF-8, on the helper's standard input and output:
//
//  1. The helper first keeps the user's other processes out of its memory,
//     as meridian run does (internal/harden), then writes a Message with
//     Ready set, and its Protocol. One that can't writes a Message with its
//     Problem instead, and exits, as one asked for an operation it doesn't
//     have, as a newer coordinator's, does without hardening itself
//     (arguments_invalid).
//  2. Only once it has read Ready, and checked that Protocol is the one it
//     speaks, does the coordinator write the Request: for place, with the
//     attempt's bootstrap token, which so reaches the helper only once the
//     helper is hardened, and never through its arguments or environment.
//  3. The helper does the operation, writes a Message with what it gives,
//     its Problem, or its Error, and exits: 0 for a Message without
//     either, and 2 for one with either.
//
// A Problem says what is wrong with the member, or with what was asked,
// as a backend.Driver's problem does: the coordinator records it. An Error
// says the helper couldn't answer: the coordinator tries again, as for a
// Driver's error that isn't a problem. A Message never holds both.
//
// # Versions
//
// Version is the protocol's, which a helper of another protocol refuses
// to be driven by: the coordinator checks Ready's Protocol, and, from
// hello, the helper's Meridian version, before it sends anything secret.
// A later version can add members and operations; it changes what none of
// them means.
package hostproto

import (
	"github.com/parallelworks/foundation/problem"

	"github.com/parallelworks/meridian/backend"
)

// Version is the protocol's version.
const Version = 1

// The operations, one for each of a backend.Driver's calls, with hello,
// which says what the host is.
const (
	OpHello  = "hello"
	OpProbe  = "probe"
	OpPlace  = "place"
	OpFind   = "find"
	OpCheck  = "check"
	OpCancel = "cancel"
	OpForget = "forget"
)

// Ops are the operations, in the order the protocol documents them.
var Ops = []string{OpHello, OpProbe, OpPlace, OpFind, OpCheck, OpCancel, OpForget}

// MaxRequest is the most bytes of a Request the helper reads.
const MaxRequest = 1 << 20

// MaxSocketsDir is the longest Paths.Sockets is, in bytes, and
// MaxSocketName the longest name of a socket in it, so that a socket's
// path, the directory, a slash and its name, fits the address of a Unix
// socket on every system the worker runs on: 104 bytes on macOS and the
// BSDs, the NUL that ends it among them, and 108 on Linux.
const (
	MaxSocketsDir = 64
	MaxSocketName = 38
)

// A Request is what the coordinator asks of the helper, once it is ready.
// Each operation reads its own members, and refuses a Request without
// them: Member for each but hello; Demand for probe; Place for place;
// Attempt for find; and Placement for check, cancel and forget.
type Request struct {
	Member    *backend.Member    `json:"member,omitzero"`
	Demand    *Demand            `json:"demand,omitzero"`
	Place     *Place             `json:"place,omitzero"`
	Attempt   string             `json:"attempt,omitempty"`
	Placement *backend.Placement `json:"placement,omitzero"`
}

// A Demand is a backend.Demand, what a job asks a member for.
type Demand struct {
	Resources      backend.Resources `json:"resources,omitzero"`
	TimeoutSeconds int64             `json:"timeoutSeconds,omitzero"`
}

// A Place is a backend.PlaceRequest, with the run's and the store's IDs,
// which name the run's directory on the host.
type Place struct {
	Attempt string `json:"attempt"`
	Name    string `json:"name,omitempty"`
	// Coordinator is where the worker reaches the coordinator from the
	// host: the forwarded socket's path there.
	Coordinator    Endpoint          `json:"coordinator"`
	BootstrapToken string            `json:"bootstrapToken"`
	TimeoutSeconds int64             `json:"timeoutSeconds,omitzero"`
	Resources      backend.Resources `json:"resources,omitzero"`
	TraceParent    string            `json:"traceParent,omitempty"`
	Run            string            `json:"run"`
	Store          string            `json:"store"`
}

// An Endpoint is a backend.Endpoint.
type Endpoint struct {
	Address string `json:"address"`
	KeyHash string `json:"keyHash,omitempty"`
}

// A Message is a line the helper writes: Ready first, then what the
// operation gives, or its Problem, or its Error.
type Message struct {
	Protocol int  `json:"protocol"`
	Ready    bool `json:"ready,omitzero"`
	// Problem is what is wrong, as a backend.Driver's problem; Error that
	// the helper couldn't answer, as its other errors.
	Problem *problem.Problem `json:"problem,omitzero"`
	Error   string           `json:"error,omitempty"`

	Hello     *Hello             `json:"hello,omitzero"`
	Fit       *Fit               `json:"fit,omitzero"`
	Placement *backend.Placement `json:"placement,omitzero"`
	// RunDir is, for place, the run's directory on the host, which the
	// helper chose, and the worker makes if it isn't there.
	RunDir string `json:"runDir,omitempty"`
	// Found is, for find, whether the attempt has a placement on the host.
	Found  bool    `json:"found,omitzero"`
	Status *Status `json:"status,omitzero"`
}

// Hello is what the host is, and where the helper keeps what it keeps.
type Hello struct {
	// Version is the helper's Meridian version, as meridian version says.
	Version string `json:"version"`
	// OS and Arch are the host's, as Go names them.
	OS   string `json:"os"`
	Arch string `json:"arch"`
	// Host is the host's name, as it says it.
	Host string `json:"host"`
	// Home is the user's home directory on the host.
	Home string `json:"home"`
	// MachineID is the host's own ID, which, with its name, tells apart
	// the hosts that share a home, as a cluster's nodes do: Paths.State is
	// theirs.
	MachineID string `json:"machineID"`
	// Detach is how the helper starts a worker so that it outlives the
	// session it was placed from: DetachSession or DetachScope.
	Detach string `json:"detach"`
	// Binary is the file the helper runs, which a worker it places runs,
	// with its digest, as sha256: and hex.
	Binary Binary `json:"binary"`
	Paths  Paths  `json:"paths"`
}

// How the helper starts a worker detached from the session that placed it.
const (
	// DetachSession starts it in a session of its own (setsid), which
	// outlives the session that placed it where the system doesn't kill a
	// user's processes as their session ends.
	DetachSession = "session"
	// DetachScope starts it in a scope of the user's service manager
	// (systemd-run --user --scope), outside the session, where the system
	// kills a session's processes as it ends, and keeps the user's manager
	// running after their last session (lingering).
	DetachScope = "user-scope"
)

// A Binary is a file the helper runs, and its digest.
type Binary struct {
	Path   string `json:"path"`
	Digest string `json:"digest"`
}

// Paths are the directories the helper keeps what it keeps in on the host,
// each the user's own, which only they can use.
type Paths struct {
	// State holds the local backend's placements, which is this host's own
	// even where the home is shared: the state directory's
	// hosts/<machine ID>-<host name>.
	State string `json:"state"`
	// Cache holds workers' binaries, by their digests.
	Cache string `json:"cache"`
	// Sockets holds the sockets a coordinator listens on for the host's
	// workers, at most MaxSocketsDir bytes: the user's runtime directory's
	// meridian/, or a directory of a random name in /tmp that the helper
	// made, which no other user could make first. It is the host's own,
	// and the same from one hello to the next while it lasts.
	Sockets string `json:"sockets"`
	// Runs holds runs' directories, which the hosts that share the
	// user's home share, as a cluster's login and compute nodes do.
	Runs string `json:"runs"`
}

// A Fit is a backend.Fit.
type Fit struct {
	Problem               *problem.Problem `json:"problem,omitzero"`
	Availability          int              `json:"availability,omitzero"`
	MaxWalltimeSeconds    int64            `json:"maxWalltimeSeconds,omitzero"`
	MarginSeconds         int64            `json:"marginSeconds,omitzero"`
	DefaultTimeoutSeconds int64            `json:"defaultTimeoutSeconds,omitzero"`
}

// FitOf is f on the wire.
func FitOf(f backend.Fit) *Fit {
	return &Fit{
		Problem: f.Problem, Availability: int(f.Availability),
		MaxWalltimeSeconds: f.MaxWalltimeSeconds, MarginSeconds: f.MarginSeconds, DefaultTimeoutSeconds: f.DefaultTimeoutSeconds,
	}
}

// Fit is the backend.Fit f says.
func (f *Fit) Fit() backend.Fit {
	return backend.Fit{
		Problem: f.Problem, Availability: backend.Availability(f.Availability),
		MaxWalltimeSeconds: f.MaxWalltimeSeconds, MarginSeconds: f.MarginSeconds, DefaultTimeoutSeconds: f.DefaultTimeoutSeconds,
	}
}

// A Status is a backend.Status: State is unknown, queued, running or dead.
type Status struct {
	State string      `json:"state"`
	End   backend.End `json:"end,omitempty"`
}

// The states, as Status names them.
var states = []string{backend.Unknown: "unknown", backend.Queued: "queued", backend.Running: "running", backend.Dead: "dead"}

// StatusOf is s on the wire.
func StatusOf(s backend.Status) *Status {
	state := "unknown"
	if int(s.State) >= 0 && int(s.State) < len(states) {
		state = states[s.State]
	}
	return &Status{State: state, End: s.End}
}

// Status is the backend.Status s says: Unknown for a state it doesn't
// know.
func (s *Status) Status() backend.Status {
	for i, name := range states {
		if name == s.State {
			return backend.Status{State: backend.State(i), End: s.End}
		}
	}
	return backend.Status{State: backend.Unknown}
}

// Demand is the backend.Demand d says.
func (d *Demand) Demand() backend.Demand {
	return backend.Demand{Resources: d.Resources, TimeoutSeconds: d.TimeoutSeconds}
}

// Request is the backend.PlaceRequest p says, but its run's directory,
// which the helper chooses.
func (p *Place) Request() backend.PlaceRequest {
	return backend.PlaceRequest{
		Attempt: p.Attempt, Name: p.Name, Coordinator: backend.Endpoint{Address: p.Coordinator.Address, KeyHash: p.Coordinator.KeyHash},
		BootstrapToken: p.BootstrapToken, TimeoutSeconds: p.TimeoutSeconds, Resources: p.Resources, TraceParent: p.TraceParent,
	}
}
