// Package workerapi defines the worker API: the HTTP API through which a
// worker claims an attempt of a job and reports on it, which the
// coordinator serves, and a platform that embeds Meridian serves the same
// (docs/design.md, The worker API).
//
// The API's paths are under /v1/. Bodies are JSON, but a log upload's,
// which is the log's bytes, and a checkout's tree, which is a tar stream
// (PathCheckout), and both sides encode and decode them with
// Marshal and Unmarshal, which refuse a member the type doesn't have: a
// worker never runs a job without a part it doesn't understand, such as
// the container its steps run in. Unmarshal refuses what the bodies'
// schema refuses, so the coordinator reads a worker's report, such as how
// its job ended, only as the schema has it. A duration is in whole
// seconds and a time is RFC 3339. An error is an RFC 9457 problem, a
// foundation problem.Problem: one of foundation's for its status, such as
// unauthenticated for a 401, or one of the codes below.
//
// # Versions
//
// The API has a version of its own (Version), apart from the plan's: one
// number, which every change to the API raises, such as a path, a member
// of a body, or a kind of event, added. Each side speaks every version up
// to its own. As it claims, a worker says the newest version it speaks
// (ClaimRequest.APIVersion); a worker of a release from before the API
// had a version of its own says none, and speaks version 1. The
// coordinator serves the attempt at the lower of that and its own, which
// the claim's answer says (Claim.APIVersion): both sides speak that
// version until the attempt ends. A member a later version adds is left
// out when it is empty, and each side sends it only in an attempt whose
// version has it, so each reads the other's bodies strictly and never
// meets a member it doesn't know. Every body a side sends or reads of an
// attempt goes through CheckVersion, through MarshalAt, UnmarshalAt and
// WriteBody, which refuse a member that is later than the attempt's
// version. A path a later version adds, a worker calls only in such an
// attempt, and the coordinator answers it for another as a path the API
// doesn't have, 404. The paths stay under /v1/: a version is served
// beside every version before it, not in place of them.
//
// A coordinator never serves an attempt at a version that lacks what the
// attempt needs: it refuses a worker whose version is older than the
// oldest that has everything the attempt's job uses, with
// WorkerAPITooOld, before it sends anything of the run, rather than run
// the job without that part. Every job so far needs version 1.
//
// What a body carries of a run, the job Ready evaluated, the run's values
// and the results of the jobs it needs, is the plan's, encoded at the
// run's plan version (Claim.PlanVersion): a change to a type of package
// plan raises plan.Version, and the API's version only if a body gains a
// member for it. As it claims, a worker says the newest plan version it
// reads too (ClaimRequest.PlanVersion), and the coordinator refuses one
// that doesn't read the run's, with PlanTooNew, before it sends anything
// of the run.
//
// A coordinator replica answers a worker only for a run whose plan version
// it reads, as it claims only such runs, and only for an attempt whose
// API version it speaks; otherwise it answers 503 Service Unavailable,
// and the worker tries again, which reaches another replica. So it does
// for a path, or a method, of the API's that it doesn't have, of an
// attempt claimed at a later version than its own, which may have it. It
// answers first what needs neither version: a credential it doesn't take,
// or that has lapsed, 401, and a write of an attempt that no longer holds
// its job, Fenced. A ClaimRequest is sent before the worker knows either
// of the attempt's versions, so it never changes: what a later worker has
// to say comes after the claim.
//
// The JSON Schema of the bodies is beside the plan's, in plan.Schemas, as
// schema/<plan version>/workerapi.schema.json, which refers to the plan's
// for what a body carries of a run. It describes the bodies of the newest
// API version, and says of a member a later version adds which version
// that is, so a body of an earlier version is one without it. The plan's
// schemas of a version are frozen with it; this one follows the API's
// version, so an addition to the API changes each plan version's copy
// alike.
//
// # Credentials
//
// A backend starts a worker with the attempt's bootstrap token, which is
// random and single-use, and valid until the attempt is claimed or its
// placement withdrawn. The worker claims the attempt with it as an
// Authorization: Bearer token, and with a credential it makes itself
// (ClaimRequest.Credential), which is the attempt's from then on: the
// worker sends it as the bearer token on every later request, and the
// coordinator stores only its SHA-256 digest, as it stores the bootstrap
// token's. The coordinator checks every write against the attempt's
// fencing number, so a write from an attempt that another has superseded
// fails with Fenced. Where the attempt's backend recorded the digest of the
// worker binary it started, a claim whose Worker.Digest is another fails
// with WorkerDigestMismatch.
//
// The credential is good until the attempt ends, though its lease lapses,
// so that a worker cut off from the coordinator delivers what it kept. For
// a job that fences itself (Claim.SelfFencing) it is good only while
// heartbeats renew it, and lapses with the lease: every request with it is
// then 401 Unauthorized. A claim repeated once the attempt has been
// superseded, or has ended, fails with Fenced, as every write of it does;
// a bootstrap token whose placement was withdrawn before its claim claims
// nothing, 401. A worker whose request fails with 401 Unauthorized after
// its claim, or with Fenced, stops its job's steps as a cancellation does,
// and exits: its attempt has ended without it, and nothing it sends is
// stored. Once it has sent Complete, though, a heartbeat or a control poll
// that fails so may only say that its end was stored, though the answer
// was lost: it repeats Complete, whose answer says whether the attempt
// ended with its end or without it.
//
// # Retries and idempotency
//
// Every request is safe to repeat, so a worker repeats one that failed
// without an answer, or with a 5xx or 429 status, with backoff, and a
// report is never stored twice. A claim repeated with the same bootstrap
// token and credential is given the same Claim, but for its Control, which
// is the attempt's as of each claim. Events carry sequence
// numbers, from 1 for an attempt's first, and the coordinator stores each
// once, in order: of events that skip one, it stores those before the gap,
// or none of them, and says which by the sequence number it expects
// (OutOfOrder). A log upload names the byte offset it starts at, and the
// coordinator stores the bytes it doesn't have yet. Complete stores the
// attempt's end once: repeating it with the same report changes nothing,
// and a different one is refused, as Fenced. So does an ActionRequest, by
// its step and try: the coordinator performs the action once, and gives a
// repeat the same answer. A ChildRequest makes a step's try's child run
// once: repeating it is answered with the same child. A checkout's tree is
// read from the commit its step was pinned to, so a repeat of the request
// is given the same tree, which the worker writes again over what it wrote
// of it.
//
// # Errors
//
// Every error is an RFC 9457 problem: one of the codes below, or one of
// foundation's for its status, as unauthenticated for 401. A body the API
// can't read, as Unmarshal refuses it, is 400 Bad Request, and so is a log
// upload of a step the attempt's job doesn't have, and a Complete that
// says what only the coordinator says (plan.JobResult's NeedsFailed and
// Cancelled), that has a Problem but with the outcome failure and the
// reason setup-failed, or whose Seq is behind the events stored; and so
// are events with a member the attempt's version doesn't have, as LastTry
// in an attempt of version 1, and an ActionRequest of a step that uses no
// action the coordinator performs, or a checkout's of a try whose action
// wasn't answered success, or of another request than its action's. A log
// upload of more than MaxLogUpload bytes, an ActionRequest of more than
// MaxActionRequest, or a ChildRequest of more than MaxChildRequest, is 413
// Content Too Large. A path the API doesn't have is 404, as is a path a
// later version adds, of an attempt of an earlier one; and one of its paths
// with another method is 405, with an Allow header. The suite in package
// workerapitest checks a coordinator against all of this.
//
// While the coordinator can't be reached, a worker keeps running its
// steps, spools its logs to disk up to a limit, keeps its events in
// memory, and sends them in order once it can, unless its job fences
// itself. But before the
// first of the job's steps that runs, it sends that step's started event
// and waits until the coordinator has stored it, so whether the workflow's
// code may have run is always known.
//
// # Child runs
//
// A step whose uses: names a workflow runs it as a run of its own, a
// child of the step's try, which the coordinator creates and runs as it
// runs any (docs/design.md, Workflows a step calls). In an attempt of
// version 3 or later, the step's worker asks for it (PathChild), waits for
// it to end (PathChildEnd), and stops it when the step is stopped
// (PathChildStop); and a child run's attempt is told what it has of its
// tree (Claim.Tree).
package workerapi

import (
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
	"reflect"
	"time"

	"github.com/parallelworks/foundation/problem"

	"github.com/parallelworks/meridian/internal/jsonshape"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
)

// Version is the newest version of the API this release speaks: it speaks
// every version from 1 to it.
const Version = Version3

// Version2 is the version of the API that added Event.LastTry, which says
// whether a failed try was its step's last, and Claim.Control, the
// attempt's control as of its claim. Each side sends them only in an
// attempt of version 2 or later.
const Version2 = 2

// Version3 is the version of the API that added PathAction, through which
// a step that uses a built-in action the coordinator performs asks it for
// that action (ActionRequest, ActionAnswer), with what evaluating its
// with: read, and PathCheckout, through which a parallelworks/checkout
// step's try reads its tree once the coordinator has answered its action;
// and child runs: PathChild, PathChildEnd and PathChildStop, through which
// a step whose uses: names a workflow asks for the run of it, waits for it
// and stops it (ChildRequest, Child, ChildStop), and Claim.Tree, what a
// child run's attempt has of its tree. A worker asks only in an attempt of
// version 3 or later, and the coordinator answers the paths for another as
// ones the API doesn't have, 404. An attempt whose job has a step whose
// uses: names a workflow, or of a child run, needs version 3
// (WorkerAPITooOld). No release has written it yet, so what is added
// before one does joins it, as for a plan version.
const Version3 = 3

// Marshal encodes a body as the API writes it: encoding/json/v2's
// defaults, with mappings in the order of their keys. A nil list or
// mapping is written as an empty one. A body a side sends of an attempt
// goes through MarshalAt, which checks it against the attempt's version.
func Marshal(v any) ([]byte, error) {
	return jsonv2.Marshal(v, jsonv2.Deterministic(true))
}

// Unmarshal decodes a body as the API reads it, as strictly as its schema
// (plan.Schemas): a member the type doesn't have, or one given twice, is an
// error, and so is one that Marshal always writes that is missing, null,
// which Marshal never writes but for data that holds nothing, and what a
// package plan type the body holds refuses, such as an outcome that isn't
// one (plan.Outcome).
func Unmarshal(data []byte, v any) error {
	if err := jsonv2.Unmarshal(data, v, jsonv2.RejectUnknownMembers(true)); err != nil {
		return err
	}
	return jsonshape.Check(data, reflect.TypeOf(v).Elem(), bodies)
}

// bodies is how Marshal writes the bodies, which Unmarshal checks them
// against: a nil list or mapping as an empty one, and data as plan.Data
// does, which holds nothing where it is null.
var bodies = &jsonshape.Options{Zero: []reflect.Type{reflect.TypeFor[plan.Data]()}}

// The API's paths. Control is a GET; the others are POSTs.
const (
	// PathClaim claims the attempt a bootstrap token is for: ClaimRequest,
	// answered with a Claim.
	PathClaim = "/v1/claim"
	// PathHeartbeat renews the attempt's lease. It has no body, and is
	// answered with 204 No Content.
	PathHeartbeat = "/v1/heartbeat"
	// PathControl is a long poll for what the coordinator asks of the
	// worker, answered with a Control. Its query names the last version
	// the worker has seen (QueryVersion), and the coordinator answers once
	// it has a newer one, or after ControlWaitSeconds with the same one.
	// The worker polls again at once. In an attempt of version 2 or later,
	// its first poll names the version of the claim's control
	// (Claim.Control). In one of version 1, it names -1, for none, which
	// the coordinator answers at once, with the control as it is: the
	// worker reads its first control so before the job's first step
	// (Control).
	PathControl = "/v1/control"
	// PathEvents reports step events: Events, answered with an EventsAck.
	PathEvents = "/v1/events"
	// PathLogs uploads part of one log: at most MaxLogUpload of its bytes,
	// as application/octet-stream, with the log, the offset they start at,
	// and whether they end it, in the query (QueryPart, QueryStep,
	// QueryTry, QueryOffset, QueryEnd). Answered with a LogAck.
	PathLogs = "/v1/logs"
	// PathComplete reports the attempt's end: Complete, answered with 204
	// No Content.
	PathComplete = "/v1/complete"
	// PathAction asks the coordinator for the built-in action a step's try
	// uses, which the coordinator performs: ActionRequest, answered with an
	// ActionAnswer. It is of version 3 (Version3).
	PathAction = "/v1/action"
	// PathChild asks for the child run of a step's try whose uses: names a
	// workflow: ChildRequest, answered with a Child. It is of version 3.
	PathChild = "/v1/child"
	// PathChildEnd is a long poll for the end of the child run of a step's
	// try, named in its query (QueryStep, QueryTry): answered with its
	// Child once it has ended, or after ControlWaitSeconds as it is, and
	// the worker polls again. It is of version 3.
	PathChildEnd = "/v1/child/end"
	// PathChildStop stops the child run of a step's try: ChildStop,
	// answered with 204 No Content once the child's stop is stored. It is
	// of version 3.
	PathChildStop = "/v1/child/stop"
	// PathCheckout reads the tree of a parallelworks/checkout step's try,
	// once the coordinator has answered the try's action with success
	// (PathAction): the same ActionRequest, answered with the tree of the
	// commit the step was pinned to, or of its sparse_checkout's paths in
	// it, as a tar stream (CheckoutMediaType) whose files come to at most
	// MaxCheckoutBytes, in MaxCheckoutEntries entries at most. The
	// coordinator reads which tree from the run's plan, never from the
	// request, and serves it only for the request its action answered
	// success, as a repeat of it: any of an attempt that no longer holds
	// its job is fenced, and one of a try whose action wasn't answered
	// success, or another request for the try, is 400, the worker's bug,
	// which fails the step rather than halt the attempt. The tar holds
	// directories, files and symbolic links, with paths from the
	// repository's root, and no .git. A stream cut short ends without the
	// end of its chunked body, an error, and the worker asks again. It is
	// of version 3 (Version3).
	PathCheckout = "/v1/checkout"
)

// CheckoutMediaType is the media type of a tree's tar stream (PathCheckout).
const CheckoutMediaType = "application/x-tar"

// MaxCheckoutBytes is the most bytes the files of a checkout's tree come
// to, together, and MaxCheckoutEntries the most entries it has, its
// directories, files and links, so that no tree exhausts a host's inodes
// (PathCheckout).
const (
	MaxCheckoutBytes   = 1 << 30
	MaxCheckoutEntries = 1_000_000
)

// ControlWaitSeconds is the longest the coordinator holds a control poll
// before it answers.
const ControlWaitSeconds = 30

// MaxLogUpload is the most bytes one log upload holds.
const MaxLogUpload = 1 << 20

// MaxActionRequest is the most bytes an ActionRequest takes: a step's
// with:, as Step evaluated it, can hold its job's needs' outputs, each up
// to 1 MiB.
const MaxActionRequest = 4 << 20

// MaxChildRequest is the most bytes a ChildRequest takes, for the same
// reason as MaxActionRequest.
const MaxChildRequest = 4 << 20

// The query parameters of PathControl and PathLogs.
const (
	QueryVersion = "version"
	// QueryPart is the log's LogPart; QueryStep and QueryTry are the
	// step's index and its try, for a step's and a cleanup's.
	QueryPart = "part"
	QueryStep = "step"
	QueryTry  = "try"
	// QueryOffset is the byte offset the upload starts at, and QueryEnd,
	// true, says that its bytes end the log: the log's length is final.
	QueryOffset = "offset"
	QueryEnd    = "end"
)

// A LogPart is which of an attempt's logs one is.
type LogPart string

// The logs of an attempt.
const (
	// LogSetup is the job's setup before its first step: the worker
	// starting, its container made, its image pulled.
	LogSetup LogPart = "setup"
	// LogStep is a step's, for each of its tries: 0 for its first run, 1
	// for its first retry.
	LogStep LogPart = "step"
	// LogCleanup is a step's cleanup: command's.
	LogCleanup LogPart = "cleanup"
)

// The environment variables a backend starts meridian worker with, which
// say how it reaches the coordinator and what it claims. The bootstrap
// token is in one of them, or in a file one names, and never on the
// command line, where other users could read it. The worker removes them
// all from the environment its steps get.
const (
	// EnvCoordinator is the coordinator's address: a Unix socket's path,
	// which starts with /, or a host and port, as in login1:7443, for TLS
	// 1.3 (backend.Endpoint).
	EnvCoordinator = "MERIDIAN_COORDINATOR"
	// EnvKeySHA256 is sha256: and the hex of the SHA-256 digest of the
	// coordinator's TLS public key, as DER SubjectPublicKeyInfo, which the
	// worker checks the TLS connection against. It is unset for a Unix
	// socket.
	EnvKeySHA256 = "MERIDIAN_COORDINATOR_KEY_SHA256"
	// EnvBootstrapToken is the attempt's bootstrap token;
	// EnvBootstrapTokenFile the path of a file only the user can read that
	// holds it, which the worker deletes once it has read it; and
	// EnvBootstrapTokenFD the number of a stream Unix socket the worker
	// inherits, on which it writes one byte once it has kept the user's
	// other processes out of its memory, and then reads the token, to the
	// socket's end. One of the three is set. The socket is the local
	// backend's: no other process of the user's can read the token from
	// it, as one could from a file, or from the environment on macOS.
	EnvBootstrapToken     = "MERIDIAN_BOOTSTRAP_TOKEN"
	EnvBootstrapTokenFile = "MERIDIAN_BOOTSTRAP_TOKEN_FILE"
	EnvBootstrapTokenFD   = "MERIDIAN_BOOTSTRAP_TOKEN_FD"
)

// The codes of the API's own problems: each is a kind of package problems,
// in its registry named worker (problems.WorkerRegistry), whose type URI
// is /problems/worker/<code>, with its messages and its page, and each
// constant is its kind's code (TestCodesAreKinds).
const (
	// Fenced (409 Conflict) is a request from an attempt that another
	// attempt of its job has superseded, or a report that isn't a repeat
	// after the attempt completed. The worker stops its job's steps as a
	// cancellation does, and exits.
	Fenced problem.Code = "fenced"
	// OutOfOrder (409 Conflict) is events that skip a sequence number, a
	// log upload that starts past the end of the log or comes after its
	// end, or a Complete before the coordinator has every event up to its
	// Seq. Its param ParamExpected is the sequence number or the byte
	// offset the coordinator expects next, which the worker sends from
	// again: of events that skip one, the coordinator may have stored
	// those before the gap.
	OutOfOrder problem.Code = "out_of_order"
	// PlanTooNew (409 Conflict) is a claim from a worker that doesn't read
	// the version of the run's plan (ClaimRequest.PlanVersion). The
	// coordinator refuses it before it sends anything of the run, and the
	// worker exits; the attempt failed before its first step, and is
	// placed again.
	PlanTooNew problem.Code = "plan_too_new"
	// WorkerAPITooOld (409 Conflict) is a claim from a worker whose
	// version of the API (ClaimRequest.APIVersion) is older than the
	// attempt needs: the oldest version that has everything the
	// attempt's job uses. The coordinator refuses it before it sends
	// anything of the run, and the worker exits; the attempt failed before
	// its first step, and is placed again. Its params ParamVersion and
	// ParamNeeded are the worker's version and the one the attempt needs.
	WorkerAPITooOld problem.Code = "worker_api_too_old"
	// WorkerDigestMismatch (409 Conflict) is a claim from a worker whose
	// binary's digest (ClaimRequest.Worker.Digest) isn't the one the
	// attempt's placement started, as its backend recorded it: the binary
	// was replaced meanwhile, or another program claims with the token.
	// The coordinator refuses it before it sends anything of the run, and
	// the worker exits; the attempt failed before its first step, and is
	// placed again. Its params ParamPlaced and ParamClaimed are the two
	// digests.
	WorkerDigestMismatch problem.Code = "worker_digest_mismatch"
)

// ParamExpected is the param of an OutOfOrder problem.
const ParamExpected = "expected"

// ParamPlaced and ParamClaimed are the params of a WorkerDigestMismatch
// problem.
const (
	ParamPlaced  = "placed"
	ParamClaimed = "claimed"
)

// ParamVersion and ParamNeeded are the params of a WorkerAPITooOld problem.
const (
	ParamVersion = "version"
	ParamNeeded  = "needed"
)

// A ClaimRequest claims the attempt a bootstrap token is for. Its members
// never change, since a worker sends it before it knows which versions,
// of the API and of the run's plan, the attempt has.
type ClaimRequest struct {
	// Credential is the credential the worker makes for the attempt: 32
	// bytes from crypto/rand, as hex. The coordinator stores only its
	// SHA-256 digest, and a claim with the same bootstrap token and another
	// credential is refused, 401 Unauthorized.
	Credential string `json:"credential"`
	// APIVersion is the newest version of the API the worker speaks
	// (Version), which says it speaks every version up to it. A worker of
	// a release from before the API had a version of its own leaves it
	// out, and speaks version 1, as the claim's Speaks says.
	APIVersion int `json:"apiVersion,omitzero"`
	// PlanVersion is the newest plan version the worker reads (plan.Version).
	PlanVersion int `json:"planVersion"`
	// Worker is the worker that claims, for the attempt's record.
	Worker Worker `json:"worker"`
}

// Speaks returns the newest version of the API the claiming worker speaks:
// its APIVersion, or 1 when it leaves that out.
func (r *ClaimRequest) Speaks() int {
	if r.APIVersion == 0 {
		return 1
	}
	return r.APIVersion
}

// A Worker is the meridian binary that runs an attempt.
type Worker struct {
	// Version is its release.
	Version string `json:"version"`
	// Digest is sha256: and the hex of its binary's SHA-256 digest.
	Digest string `json:"digest"`
}

// A Claim is an attempt that a worker claimed: the job as Ready evaluated
// it, with all of the run that the job reads.
type Claim struct {
	// RunID and AttemptID identify the run and the attempt.
	RunID     string `json:"runID"`
	AttemptID string `json:"attemptID"`
	// APIVersion is the version of the API the coordinator serves the
	// attempt at, which both sides speak until it ends: the lower of the
	// worker's (ClaimRequest.APIVersion) and the coordinator's own. A claim
	// repeated is given the same.
	APIVersion int `json:"apiVersion"`
	// PlanVersion is the version of the run's plan, which Job is encoded
	// in.
	PlanVersion int `json:"planVersion"`
	// Job is the job as Ready evaluated it, as plan.EncodeReadyJob encodes
	// it: the worker decodes it with plan.DecodeReadyJob(PlanVersion, Job),
	// which is as strict as a plan's decoding.
	Job jsontext.Value `json:"job"`
	// Attempt is the attempt's number, from 1, and Group and Member where it
	// was placed: with the job's Name and Key, its job context.
	Attempt int    `json:"attempt"`
	Group   string `json:"group,omitempty"`
	Member  string `json:"member,omitempty"`
	// LeaseSeconds is how long the lease lasts after a heartbeat, and
	// HeartbeatSeconds how often the worker sends one. For a job that
	// fences itself, the worker sends them often enough to try three
	// times before it would start stopping the job (SelfFencing), more
	// often than HeartbeatSeconds if need be, saying so in its log, and
	// gives up on one that hasn't been answered in a third of the time
	// then left, to send another: a coordinator gives such a job a
	// HeartbeatSeconds of a fifth of LeaseSeconds at most.
	LeaseSeconds     int64 `json:"leaseSeconds"`
	HeartbeatSeconds int64 `json:"heartbeatSeconds"`
	// SelfFencing is set for a job whose retry: lists worker-lost, which
	// can have a new attempt while this one may still run: the worker
	// stops the job before its lease lapses. Counted from when it sent the
	// last heartbeat that succeeded, or the claim, on whichever of its
	// monotonic and wall clocks has gone further: once six tenths of
	// LeaseSeconds have passed without a renewal, it may start stopping
	// the job, sending every process group the attempt started SIGTERM,
	// so that a step has at most two tenths of the lease to handle it;
	// and by nine tenths it has sent them all SIGKILL, and stopped waiting
	// for them. It stops renewing the lease then, and sends what it has
	// until the lease lapses. A process that left its group, as with
	// setsid, isn't the worker's to stop. The coordinator starts a new
	// attempt only once LeaseSeconds have passed since the last renewal,
	// and a margin, by the store's clock.
	SelfFencing bool `json:"selfFencing,omitzero"`
	// Run is the part of the run that the job reads (plan.Job.Reads): its
	// inputs' values, variables and secrets among them. The worker masks
	// what Run.Secrets names (plan.RunValues.Masked).
	Run plan.RunValues `json:"run"`
	// Needs are the results of the jobs this one needs, by job name, as
	// plan.Job.ReadsNeeds cuts them: each one's outcome, and only the
	// outputs, and steps' outputs, that the job's expressions read, with
	// which of those are private, or secrets, which the worker masks.
	Needs map[string]plan.JobResult `json:"needs,omitempty"`
	// RunDir is the run's directory on the worker's host (PW_JOB_DIR),
	// where steps run by default.
	RunDir string `json:"runDir"`
	// Env are the variables a platform gives the job's steps, by name,
	// such as format 1's PW_JOB_NUMBER, PW_USER and PW_PLATFORM_HOST: the
	// worker sets them over the environment the workflow gives each step,
	// as format 1 sets its own, and its own over them, OUTPUTS, PW_JOB_DIR
	// and the retry counters among them. A value whose Secret is set, such
	// as PW_API_KEY's, the worker masks as it masks the run's secrets, and
	// never logs.
	Env map[string]EnvVar `json:"env,omitempty"`
	// Retry is how many earlier attempts of the job started a step: what
	// the job's retry: has used, which a step reads as
	// PW_WORKFLOW_JOB_CURRENT_RETRY. An attempt that failed before its
	// first step isn't counted.
	Retry int `json:"retry,omitzero"`
	// Container is the container the job's steps run in, or nil for steps
	// that run on the host.
	Container *Container `json:"container,omitempty"`
	// TraceParent is the W3C traceparent of the attempt's span, which the
	// worker continues, or "".
	TraceParent string `json:"traceParent,omitempty"`
	// Control is what the coordinator asks of the worker as of this claim,
	// in an attempt of version 2 or later, and nil in one of version 1: the
	// worker does as it asks before anything of the job runs, and polls
	// for what comes after it (PathControl).
	Control *Control `json:"control,omitzero"`
	// Tree is what an attempt of a child run has of its tree, in an
	// attempt of version 3 or later, which such an attempt needs; nil for
	// a run no step called.
	Tree *Tree `json:"tree,omitzero"`
}

// A Tree is what an attempt of a child run has of its tree (Claim.Tree):
// its identity, which is its root's, as format 1 gives a child, and the
// strings the worker of the step that called it masked. Every member is
// of version 3.
type Tree struct {
	// JobID is the root run's ID, which the job's steps read as
	// PW_JOB_ID; RootDir the root's run directory, PW_PARENT_JOB_DIR; and
	// WorkflowName format 1's name for the call, PW_WORKFLOW_NAME
	// (plan.SourceRef.CallName). The worker sets them as it sets its own,
	// over the platform's (Env) for none of them, and PW_JOB_DIR is the
	// child's own directory (RunDir).
	JobID        string `json:"jobID,omitzero"`
	RootDir      string `json:"rootDir,omitzero"`
	WorkflowName string `json:"workflowName,omitzero"`
	// Masked are the strings the worker of the step that called the run
	// masked for the step (plan.Inherited.Masked): its run's secrets', and
	// what its job and step made from one. The worker counts each as the
	// text of a secret that doesn't come from the run, as it counts a
	// secret Env gives (plan.JobState.Secrets), in each of the job's
	// phases: it masks each in the job's logs and problems however short,
	// and End names an output that holds one a secret.
	Masked []string `json:"masked,omitzero"`
}

// An EnvVar is a variable a platform gives a job's steps (Claim.Env).
type EnvVar struct {
	Value string `json:"value"`
	// Secret is set for a value the worker masks, and never logs.
	Secret bool `json:"secret,omitzero"`
}

// A Container is the container a job's steps run in. The worker runs on
// the host, creates it, runs each step in it with the runtime's exec, and
// removes it when the job ends.
type Container struct {
	// Runtime is the container runtime: podman, docker, apptainer,
	// singularity or container.
	Runtime string `json:"runtime"`
	// Image is the image as repository@sha256: and its digest, as in
	// ghcr.io/parallelworks/meridian-base@sha256:….
	Image string `json:"image"`
}

// Control is what the coordinator asks of a worker.
//
// What it asked before the claim applies from the job's first step, as
// format 1 starts a job whose context has already ended, as when the run
// was cancelled, or its timeout passed, or a job of it failed, before the
// attempt was placed. A coordinator puts what it has asked of the
// attempt's run in the control as it makes the attempt. In an attempt of
// version 2 or later, the claim holds the control (Claim.Control), and
// the worker does as it asks before the first step starts. In one of
// version 1, once it has claimed the attempt, the worker reads its first
// control with a poll the coordinator answers at once (PathControl), and
// does as it asks before the first step starts. It waits a few seconds at
// most for that first answer, and then starts the job's steps without it,
// and does as the control asks once a poll has it.
type Control struct {
	// Version grows each time the coordinator asks something more.
	Version int64 `json:"version"`
	// Cancel asks the worker to stop its job: to send each running step
	// SIGTERM, then SIGKILL after GraceSeconds, or its own grace period
	// for 0, run the cleanup commands, and complete the attempt. Reason
	// says why: cancelled, timeout for the run's, or fail-fast. A later
	// Cancel that gives less time makes SIGKILL sooner; one that gives
	// more doesn't make it later.
	//
	// A run past its timeout isn't cancelled, in format 1: its deadline
	// fails the step it stops. So a Cancel whose Reason is timeout stops
	// the job as the job's own timeout: does: the step it stops ends
	// failure, and so does one that comes to run after it, so failure()
	// is true for the steps after them and cancelled() false, even if a
	// cancellation follows, and the job ends failure, with the reason
	// timeout. As in format 1, a step with ignore-errors: that it stops
	// on its last try, with no retry left, succeeds instead; an earlier
	// try fails, and isn't tried again, as does a step waiting to try
	// again. A deadline that fails no step, as one that comes during a
	// last step that outlives a cancel, or the cleanups, or that stops
	// only steps that succeed so, leaves the job ending as its steps did.
	//
	// As in format 1, a step whose if: is cancel-aware, as always() is
	// (plan.ReadyStep.OutlivesCancel), still runs after a cancellation,
	// and to its end, so a job can end otherwise than cancelled, as
	// success when such steps were the last to run and none of its steps
	// was cancelled. The run's outcome comes from the coordinator's own
	// record that it cancelled the run, as format 1's comes from its
	// record of an external cancellation, not from its jobs'.
	Cancel       bool        `json:"cancel,omitzero"`
	Reason       plan.Reason `json:"reason,omitempty"`
	GraceSeconds int64       `json:"graceSeconds,omitzero"`
	// AnyJobFailed says that another job of the run failed: the worker
	// stops each step whose early-cancel: is any-job-failed, as Cancel
	// stops a step. The step ends cancelled, which cancels the rest of its
	// job, as format 1 has it: a later step runs only if it outlives a
	// cancel (plan.ReadyStep.OutlivesCancel), cancelled() is true, and the
	// job ends cancelled unless a step of it failed.
	AnyJobFailed bool `json:"anyJobFailed,omitzero"`
}

// Events are step events, in the order of their sequence numbers.
type Events struct {
	Events []Event `json:"events"`
}

// An Event is a step, or a step's cleanup: command, that started or
// finished. A step whose if: is false has only a finished event, with the
// outcome skipped.
type Event struct {
	// Seq numbers the attempt's events, from 1, one more each time.
	Seq int64 `json:"seq"`
	// Time is when it happened, by the worker's clock.
	Time time.Time `json:"time"`
	// Finished is set when the step finished, and unset when it started.
	Finished bool `json:"finished,omitzero"`
	// Step is the step's index in its job, from 0, and Try its retry: 0
	// for its first run. Cleanup is set for the step's cleanup: command,
	// whose outputs are the job's (plan.StepResult.CleanupOutputs).
	Step    int  `json:"step"`
	Try     int  `json:"try,omitzero"`
	Cleanup bool `json:"cleanup,omitzero"`
	// LastTry is set on the finished event of a step's try that failed,
	// when the step won't be tried again, in an attempt of version 2 or
	// later: so the coordinator knows at once that the step has failed,
	// whatever its retry: says, as format 1 counts its job failed from
	// then. A try the step tries again has it unset, and so has every
	// event of an attempt of version 1.
	LastTry bool `json:"lastTry,omitzero"`
	// Outcome, ExitCode, Outputs and Diagnostics are a finished event's:
	// the step's outcome as its job counts it; its command's exit status,
	// or -1 if it has none, as for a command a signal ended or a uses:
	// step; what it wrote to $OUTPUTS, masked as its logs are, for
	// showing, with a secret's text in it masked and the rest as written
	// (Complete.Result.Steps has them unmasked); and Step's problems with
	// it.
	Outcome     plan.Outcome         `json:"outcome,omitempty"`
	ExitCode    int                  `json:"exitCode,omitzero"`
	Outputs     map[string]string    `json:"outputs,omitempty"`
	Diagnostics problems.Diagnostics `json:"diagnostics,omitempty"`
}

// An EventsAck is the coordinator's answer to Events.
type EventsAck struct {
	// Seq is the sequence number of the last event the coordinator has
	// stored.
	Seq int64 `json:"seq"`
}

// A LogAck is the coordinator's answer to a log upload.
type LogAck struct {
	// Length is the log's length now, in bytes: where the next upload
	// starts. Ended is set once an upload has ended the log, and Length
	// is final.
	Length int64 `json:"length"`
	Ended  bool  `json:"ended,omitzero"`
}

// Complete is an attempt's end. The worker sends it once it has sent its
// events, and ended its logs.
type Complete struct {
	// Seq is the sequence number of the attempt's last event, or 0 for
	// none.
	Seq int64 `json:"seq"`
	// Result is how the job ended: its outcome, and its outputs and its
	// steps', none of them masked, with which of them are private or hold
	// a secret, as End gave them (plan.EndResult.Result). The coordinator
	// keeps it as the job's result, which the jobs that need the job read,
	// and what its Private and Secrets name only in the run's secret
	// record. NeedsFailed and Cancelled are the coordinator's, and the
	// worker leaves them unset.
	Result plan.JobResult `json:"result"`
	// Reason is why the job ended other than as its steps did, or "":
	// timeout for a job past its timeout:, or whose run is past the run's,
	// with the outcome failure, when the deadline failed a step; the
	// Control's reason for a job cancelled, with the outcome cancelled; or
	// setup-failed. As in format 1, a job whose deadline failed no step,
	// as one that stopped only steps with ignore-errors: on their last
	// try, which succeed, ends as its steps did, with no reason.
	Reason plan.Reason `json:"reason,omitempty"`
	// Problem is what failed before the first step started, with the
	// outcome failure and the reason setup-failed: an image that couldn't
	// be pulled, or a container runtime that is missing. None of the
	// workflow's code ran, so the coordinator places the job again; its
	// Code is the reason placement compares, so a job two members fail for
	// the same reason fails. It is nil otherwise.
	Problem *problem.Problem `json:"problem,omitempty"`
	// Diagnostics are the problems with the job's env:, from its JobEnv
	// phase, and End's with its outputs, and the worker's own with a
	// report it couldn't encode. A job whose env: fails has no steps that
	// ran.
	Diagnostics problems.Diagnostics `json:"diagnostics,omitempty"`
}

// An ActionRequest asks the coordinator for the built-in action that a
// step's try uses, of version 3 (PathAction): an action whose subject is
// the coordinator's, as cancel-jobs's is the run, which the worker can't
// perform. The coordinator reads which action it is from the run's plan,
// at the step, and never from the request, and answers a step that uses
// no action it performs as a request it can't read, 400. So it reads
// whether the step outlives its job's cancellation (ActionAnswer.Outcome).
//
// The coordinator performs the action once for a step's try: a repeat of
// the request is given the same answer, and one with another With is
// refused, as Fenced, as a different Complete is. It keeps a digest of
// With, never With itself, which can hold a secret. A request from an
// attempt that no longer holds its job is refused, as Fenced, before
// anything is performed.
//
// Every member is of version 3, and left out when it is zero, as a member
// a later version adds is.
type ActionRequest struct {
	// Step is the step's index in its job, from 0, and Try its try: 0 for
	// its first run.
	Step int `json:"step,omitzero"`
	Try  int `json:"try,omitzero"`
	// With is the step's with:, as Step evaluated it for the try: a
	// mapping, or nothing for a step without one.
	With plan.Data `json:"with,omitzero"`
	// Private and Secret say what evaluating With read, as Step decided it
	// (plan.ReadyStep.WithRead): a variable or a password, which makes
	// what it gives private, kept only where the run's secrets are and
	// never quoted, and a secret, which makes it masked, as one of the
	// run's secrets is. What performs the action keeps it so, as
	// update-session's session provider does (compile.Sessions.Update).
	Private bool `json:"private,omitzero"`
	Secret  bool `json:"secret,omitzero"`
}

// An ActionAnswer is how the action an ActionRequest asked for ended, as
// the coordinator performed it, with its problems.
type ActionAnswer struct {
	// Outcome is success or failure, as the action ended, and so the try,
	// as for a command that exits 0 or not; or cancelled, for a step whose
	// action the coordinator didn't perform, as the worker would have
	// stopped it: one of a job the coordinator is cancelling whose if:
	// doesn't outlive that, as the coordinator reads it from the run's plan
	// (plan.OutlivesCancel), never from the request, with the cancel's
	// Reason; or one whose early-cancel: is any-job-failed, once a job of
	// the run has failed (Control.AnyJobFailed), with none.
	Outcome plan.Outcome `json:"outcome,omitzero"`
	// Reason is why the attempt's control asks to cancel its job, as the
	// action is answered (Control.Reason), or "" for no cancel, or the
	// run's timeout's, which stops a job rather than cancel it: with the
	// outcome cancelled, why the action wasn't performed; with another, a
	// cancel that came with the action, as when it cancelled the step's
	// own job. The worker cancels its job for it at once, as it does once
	// it reads that control, so that the job's next step heeds it. The
	// outcome cancelled with no reason is the step's early-cancel:, which
	// the worker heeds as it does once it reads that a job failed.
	Reason plan.Reason `json:"reason,omitzero"`
	// Diagnostics are the action's problems: why it failed, and its
	// warnings, at the step's path in the workflow and under it.
	Diagnostics problems.Diagnostics `json:"diagnostics,omitzero"`
}

// A ChildRequest asks for the child run of a step's try whose uses: names
// a workflow (PathChild), of version 3: what the step's worker gives the
// child of the step, as Step evaluated it for the try (plan.ReadyStep).
// The coordinator reads which workflow it runs from the run's plan, at the
// step, and never from the request, and so whether the step outlives its
// job's cancellation, as its if: says (plan.OutlivesCancel).
//
// The coordinator makes a step's try's child once: a repeat of the request
// is answered with the same child, whatever it holds. A request from an
// attempt that no longer holds its job is refused, as Fenced, before
// anything is made. A step that doesn't outlive its job's cancellation, of
// an attempt whose control asks to cancel its job, for any reason but the
// run's timeout, or of a run that was stopped, is answered with a child
// that ended cancelled, without one made, as the worker would have stopped
// the step had it read the control first; and so, with no reason, is a
// step whose early-cancel: is any-job-failed, once a job of the run has
// failed (Control.AnyJobFailed). A request of a step whose uses: names no
// workflow, or of a try past the step's retry:, is 400.
//
// Every member is of version 3, and left out when it is zero.
type ChildRequest struct {
	// Step is the step's index in its job, from 0, and Try its try: 0 for
	// its first run.
	Step int `json:"step,omitzero"`
	Try  int `json:"try,omitzero"`
	// With is the step's with:, a mapping, which the child is submitted,
	// but for its $yaml, $host and $thumbnail.
	With plan.Data `json:"with,omitzero"`
	// Env is the step's environment, which the child's env: is laid over:
	// Step's (plan.ReadyStep.Env), with the variables the worker defines
	// for the step that format 1 defines (plan.IsFormatVariable) over it,
	// and never the step's process's own, which holds the platform's
	// credential.
	Env map[string]string `json:"env,omitzero"`
	// WithPrivate, WithSecrets, EnvPrivate and EnvSecrets name what of With
	// and Env Step made from a variable or a password but no secret, and
	// from a secret (plan.ReadyStep): the child keeps them out of its plan,
	// and masks those made from a secret.
	WithPrivate []string `json:"withPrivate,omitzero"`
	WithSecrets []string `json:"withSecrets,omitzero"`
	EnvPrivate  []string `json:"envPrivate,omitzero"`
	EnvSecrets  []string `json:"envSecrets,omitzero"`
	// Masked are the strings the worker masks for the step: its run's
	// secrets', and what its job and step made from one, which the child's
	// jobs mask too (Tree.Masked).
	Masked []string `json:"masked,omitzero"`
	// WorkingDirectory is the step's working directory, as Step gave it
	// (plan.ReadyStep.WorkingDirectory): "" for its run's directory, or a
	// path relative to it, or absolute. The child's directory is
	// subworkflows/<job>/step_<n> in it.
	WorkingDirectory string `json:"workingDirectory,omitzero"`
}

// A Child is the child run of a step's try, as it is (PathChild,
// PathChildEnd). Every member is of version 3.
type Child struct {
	// Run is the child run's ID, or "" for a child that wasn't made: one
	// whose Create failed, or that its step's job's cancel kept from being
	// made.
	Run string `json:"run,omitzero"`
	// Ended is set once the child has ended, with how: Outcome is the
	// child run's (success, failure or cancelled), and Reason why it ended
	// other than as its jobs did. The worker maps it to the step's end, as
	// docs/design.md says under Workflows a step calls: a child it stopped
	// ends its step cancelled, whatever its outcome. One that wasn't made
	// has ended, failure, with why in Diagnostics, or cancelled, with the
	// cancel's Reason, or with none for the step's early-cancel:.
	Ended   bool         `json:"ended,omitzero"`
	Outcome plan.Outcome `json:"outcome,omitzero"`
	Reason  plan.Reason  `json:"reason,omitzero"`
	// Diagnostics are why a child that wasn't made wasn't: its own
	// problems, as its Create gave them, or the coordinator's, at the
	// step's uses:, which the worker reports as the step's, masked as the
	// step's are.
	Diagnostics problems.Diagnostics `json:"diagnostics,omitzero"`
}

// A ChildStop stops the child run of a step's try (PathChildStop), as the
// step is stopped: the run's or its job's cancel, a timeout, or its
// early-cancel:. The coordinator stops the child as a person's cancel
// does, for Reason, and the worker keeps waiting for its end
// (PathChildEnd), so the step ends after the child's cleanups. Stopping a
// child that has ended, or was stopped already, changes nothing. Every
// member is of version 3.
type ChildStop struct {
	Step int `json:"step,omitzero"`
	Try  int `json:"try,omitzero"`
	// Reason is cancelled, or timeout for the step's own, its try's, its
	// job's or its run's.
	Reason plan.Reason `json:"reason,omitzero"`
}
