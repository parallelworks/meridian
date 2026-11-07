package batch

import (
	"bytes"
	jsonv2 "encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/parallelworks/foundation/problem"

	"github.com/parallelworks/meridian/backend"
	"github.com/parallelworks/meridian/internal/safepath"
	"github.com/parallelworks/meridian/problems"
)

// The files of a submission's directory.
const (
	// SpecFile is what meridian batch is to do (Spec), which Place writes.
	SpecFile = "spec.json"
	// TokenFile holds the attempt's bootstrap token, which only the user
	// can read, until meridian batch, Cancel or Forget removes it.
	TokenFile = "token"
	// JobFile holds the job's ID, once the scheduler has given it.
	JobFile = "job"
	// WithdrawnFile says that the job was withdrawn (Withdrawn).
	WithdrawnFile = "withdrawn"
	// StartedFile, TermFile and ExitFile are meridian batch's records
	// (Started, Term and Exit).
	StartedFile = "started.json"
	TermFile    = "term.json"
	ExitFile    = "exit.json"
	// OutFile is where the job's standard output and error go.
	OutFile = "out"
)

// maxRecord is the most bytes of a record, or a spec, read.
const maxRecord = 64 << 10

// maxToken is the most bytes of a token's file read.
const maxToken = 4096

// A Spec is what meridian batch is to do for an attempt.
type Spec struct {
	Attempt    string `json:"attempt"`
	Submission int    `json:"submission"`
	// Scheduler names the scheduler: slurm.
	Scheduler string `json:"scheduler"`
	// Digest is the digest of the binary the job runs, which the
	// placement says.
	Digest      string      `json:"digest"`
	Coordinator Coordinator `json:"coordinator"`
	// MarginSeconds is the member's margin: the worker stops the job's
	// steps half of it before the allocation ends, as the first half
	// started the worker, and the second is its cleanups' and reports'.
	MarginSeconds int64 `json:"marginSeconds"`
	// WalltimeSeconds is what the job asked for, or 0 for the partition's
	// default.
	WalltimeSeconds int64  `json:"walltimeSeconds,omitzero"`
	RunDir          string `json:"runDir,omitempty"`
}

// Coordinator is where the worker reaches the coordinator: Address, or
// the addresses the file AddressFile names, checked against KeyHash for
// TLS.
type Coordinator struct {
	Address     string `json:"address,omitempty"`
	AddressFile string `json:"addressFile,omitempty"`
	KeyHash     string `json:"keyHash,omitempty"`
}

// Started is what meridian batch records before it starts the worker.
type Started struct {
	Job    string    `json:"job"`
	Node   string    `json:"node"`
	BootID string    `json:"bootID,omitempty"`
	PID    int       `json:"pid"`
	Start  time.Time `json:"start"`
	// End is when the allocation ends, or zero where that can't be told.
	End time.Time `json:"end,omitzero"`
}

// Term is when meridian batch was first asked to stop, as the scheduler
// does at a job's time limit, or as it cancels it.
type Term struct {
	At     time.Time `json:"at"`
	Signal int       `json:"signal"`
}

// Exit is how meridian batch ended.
type Exit struct {
	// Status is the worker's exit status, or -1 for a worker a signal
	// ended, or one that didn't start.
	Status int `json:"status"`
	// Signal is the signal that ended the worker, or 0.
	Signal int `json:"signal,omitzero"`
	// OOMKills is how many processes of the job the kernel ended for its
	// memory while the worker ran.
	OOMKills int64     `json:"oomKills,omitzero"`
	At       time.Time `json:"at"`
	// Problem is why the worker didn't start, where meridian batch knows.
	Problem *Recorded `json:"problem,omitempty"`
}

// Withdrawn says that the driver cancelled a job: CannotStart is the
// reason it waited for, for one that could never start.
type Withdrawn struct {
	At          time.Time `json:"at"`
	CannotStart string    `json:"cannotStart,omitempty"`
}

// A Recorded is a problem as a record keeps it: its code and its params.
type Recorded struct {
	Code   problem.Code   `json:"code"`
	Params map[string]any `json:"params,omitempty"`
}

// Record returns p as a record keeps it.
func Record(p *problem.Problem) *Recorded {
	return &Recorded{Code: p.Code, Params: p.Params}
}

// recordedKinds are the kinds of problem meridian batch records, which a
// check gives as the placement's: why the worker didn't start. Another
// code in a record, which no release writes, is ignored.
var recordedKinds = []*problems.Kind{
	problems.WorkerBinaryUnsafe, problems.WorkerDigestMismatch, problems.SchedulerRelayUnreachable,
	problems.WorkerTokenNotTaken, problems.WorkerHardenFailed, problems.WorkerSystemUnsupported,
}

// problem returns the problem r records, made again from its kind, or nil
// for one of a kind meridian batch doesn't record.
func (r *Recorded) problem() *problem.Problem {
	for _, k := range recordedKinds {
		if k.Code() == r.Code {
			params := problems.Params{}
			for _, name := range k.Type().Params {
				params[name] = r.Params[name]
			}
			return k.Problem(params)
		}
	}
	return nil
}

// records are what a submission's directory says of its job.
type records struct {
	started   *Started
	term      *Term
	exit      *Exit
	withdrawn *Withdrawn
}

// readRecords reads the records of the submission sub, "" for none. One
// that can't be read is taken for one that isn't there: the check says
// what the rest give.
func readRecords(sub string) records {
	var r records
	if sub == "" {
		return r
	}
	root, err := os.OpenRoot(sub)
	if err != nil {
		return r
	}
	defer root.Close()
	read := func(name string, v any) bool {
		data, err := readIn(root, name, safepath.Private, maxRecord)
		return err == nil && jsonv2.Unmarshal(data, v) == nil
	}
	if s := new(Started); read(StartedFile, s) {
		r.started = s
	}
	if t := new(Term); read(TermFile, t) {
		r.term = t
	}
	if e := new(Exit); read(ExitFile, e) {
		r.exit = e
	}
	if w := new(Withdrawn); read(WithdrawnFile, w) {
		r.withdrawn = w
	}
	return r
}

// status returns the Status of a job whose meridian batch recorded how it
// ended.
func (r records) status(id string) backend.Status {
	dead := backend.Status{State: backend.Dead, End: backend.EndExited}
	switch {
	case r.exit.Problem != nil && r.exit.Problem.problem() != nil:
		return backend.Status{State: backend.Dead, Problem: r.exit.Problem.problem()}
	case r.withdrawn != nil:
		return withdrawnOr(id, r, dead)
	case r.exit.Signal == sigKill && r.exit.OOMKills > 0:
		return backend.Status{State: backend.Dead, End: backend.EndOutOfMemory}
	case r.timedOut():
		return backend.Status{State: backend.Dead, End: backend.EndTimedOut}
	}
	return dead
}

// sigKill is SIGKILL's number, as POSIX fixes it: the kernel's OOM killer
// sends it.
const sigKill = 9

// timedOut reports whether the job was signalled at its allocation's end,
// as a scheduler does at its time limit.
func (r records) timedOut() bool {
	return r.term != nil && r.started != nil && !r.started.End.IsZero() && !r.term.At.Before(r.started.End.Add(-termNearEnd))
}

// readIn reads the file name in root, at most limit bytes, if it is a
// regular file of the user's that other users can use no more than
// access allows. root keeps it from being one outside it.
func readIn(root *os.Root, name string, access safepath.Access, limit int64) ([]byte, error) {
	f, err := root.Open(name)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if err := safepath.CheckFileInfo(filepath.Join(root.Name(), name), info, access); err != nil {
		return nil, err
	}
	data, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, problems.FileOverLimit.Problem(problems.Params{"path": filepath.Join(root.Name(), name), "limit": limit})
	}
	return data, nil
}

// ReadSpec reads the spec of the submission dir, which the caller checked.
func ReadSpec(dir string) (*Spec, error) {
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	data, err := readIn(root, SpecFile, safepath.Private, maxRecord)
	if err != nil {
		return nil, err
	}
	s := new(Spec)
	if err := jsonv2.Unmarshal(data, s, jsonv2.RejectUnknownMembers(true)); err != nil {
		return nil, fmt.Errorf("batch: %s: %w", filepath.Join(dir, SpecFile), err)
	}
	return s, nil
}

// TakeToken reads the bootstrap token of the submission dir, which the
// caller checked, and removes it: it is good for one claim.
func TakeToken(dir string) (string, error) {
	root, err := os.OpenRoot(dir)
	if err != nil {
		return "", err
	}
	defer root.Close()
	data, err := readIn(root, TokenFile, safepath.Private, maxToken)
	if err != nil {
		return "", err
	}
	if err := root.Remove(TokenFile); err != nil {
		return "", err
	}
	token := strings.TrimSpace(string(data))
	if token == "" {
		return "", errors.New("batch: the token's file is empty")
	}
	return token, nil
}

// WriteRecord writes v, as JSON, to the file name of the submission dir,
// whole or not at all, readable only by the user.
func WriteRecord(dir, name string, v any) error {
	return writeJSON(dir, name, v)
}

func writeJSON(dir, name string, v any) error {
	data, err := jsonv2.Marshal(v, jsonv2.Deterministic(true))
	if err != nil {
		return err
	}
	return writeFile(dir, name, data)
}

func writeFile(dir, name string, data []byte) error {
	return safepath.WriteFile(filepath.Join(dir, name), data)
}

// recordedJob returns the job's ID that the submission sub's job file
// says, or else its started.json, or "".
func recordedJob(sub string) string {
	root, err := os.OpenRoot(sub)
	if err != nil {
		return ""
	}
	defer root.Close()
	if data, err := readIn(root, JobFile, safepath.Private, 256); err == nil {
		if id := strings.TrimSpace(string(data)); id != "" {
			return id
		}
	}
	if data, err := readIn(root, StartedFile, safepath.Private, maxRecord); err == nil {
		var s Started
		if jsonv2.Unmarshal(data, &s) == nil {
			return s.Job
		}
	}
	return ""
}

// markWithdrawn records that the submission sub's job was withdrawn,
// unless it says so already, which keeps the first reason.
func markWithdrawn(sub, cannotStart string) error {
	if _, err := os.Lstat(filepath.Join(sub, WithdrawnFile)); err == nil {
		return nil
	}
	return writeJSON(sub, WithdrawnFile, &Withdrawn{At: time.Now(), CannotStart: cannotStart})
}

// removeToken removes the submission sub's token, if it is there.
func removeToken(sub string) error {
	if err := os.Remove(filepath.Join(sub, TokenFile)); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return nil
}

// maxSaid is the most bytes of what a scheduler, or a job, said that a
// problem shows.
const maxSaid = 512

// Said returns s as a problem shows what a scheduler or a job said: on one
// line, its control characters escaped, at most maxSaid bytes.
func Said(s string) string {
	q := strconv.QuoteToGraphic(strings.TrimSpace(s))
	q = q[1 : len(q)-1]
	if len(q) > maxSaid {
		q = q[:maxSaid]
		for !utf8.ValidString(q) {
			q = q[:len(q)-1]
		}
	}
	return q
}

// lastLine returns the last line the job wrote to its output in the
// submission sub, as Said shows it, or "".
func lastLine(sub string) string {
	root, err := os.OpenRoot(sub)
	if err != nil {
		return ""
	}
	defer root.Close()
	f, err := root.Open(OutFile)
	if err != nil {
		return ""
	}
	defer f.Close()
	info, err := f.Stat()
	// The scheduler makes it with the user's umask: others may read it.
	if err != nil || safepath.CheckFileInfo(filepath.Join(sub, OutFile), info, safepath.Unchangeable) != nil {
		return ""
	}
	const tail = 4096
	if _, err := f.Seek(max(info.Size()-tail, 0), io.SeekStart); err != nil {
		return ""
	}
	data, _ := io.ReadAll(io.LimitReader(f, tail))
	lines := bytes.Split(bytes.TrimRight(data, "\r\n \t"), []byte("\n"))
	return Said(string(lines[len(lines)-1]))
}

// ensureDir makes dir the user's own, if it isn't there, which only they
// can use, and checks it, and the way to it.
func ensureDir(dir string) error {
	if !filepath.IsAbs(dir) {
		return fmt.Errorf("batch: the directory %q isn't absolute", dir)
	}
	return safepath.EnsureDir(dir, safepath.Private)
}

// submissionDir is the directory of the attempt's nth submission.
func (d *Driver) submissionDir(attempt string, n int) string {
	return filepath.Join(d.Dir, attempt, strconv.Itoa(n))
}

// submissions returns the numbers of the attempt's submissions, the
// lowest first.
func (d *Driver) submissions(attempt string) ([]int, error) {
	entries, err := os.ReadDir(filepath.Join(d.Dir, attempt))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	var ns []int
	for _, e := range entries {
		if n, err := strconv.Atoi(e.Name()); err == nil && n > 0 && strconv.Itoa(n) == e.Name() && e.IsDir() {
			ns = append(ns, n)
		}
	}
	slices.Sort(ns)
	return ns, nil
}

// newSubmission makes the attempt's next submission's directory, and
// returns it and its number: mkdir, which is atomic on NFS too, chooses
// it, however many processes place the attempt at once.
func (d *Driver) newSubmission(attempt string) (string, int, error) {
	if err := ensureDir(filepath.Join(d.Dir, attempt)); err != nil {
		return "", 0, err
	}
	ns, err := d.submissions(attempt)
	if err != nil {
		return "", 0, err
	}
	n := 1
	if len(ns) > 0 {
		n = ns[len(ns)-1] + 1
	}
	for ; ; n++ {
		sub := d.submissionDir(attempt, n)
		err := os.Mkdir(sub, 0o700)
		if err == nil {
			return sub, n, safepath.CheckDir(sub, safepath.Private)
		}
		if !errors.Is(err, fs.ErrExist) {
			return "", 0, err
		}
	}
}

// submissionOf returns the directory of the attempt's submission whose job
// is id, or "" if none says so.
func (d *Driver) submissionOf(attempt, id string) (string, error) {
	if !attemptPattern.MatchString(attempt) {
		return "", nil
	}
	ns, err := d.submissions(attempt)
	if err != nil {
		return "", err
	}
	for _, n := range slices.Backward(ns) {
		if sub := d.submissionDir(attempt, n); recordedJob(sub) == id {
			return sub, nil
		}
	}
	return "", nil
}

// removeSubmission removes the attempt's nth submission, never following
// a link out of the driver's directory.
func (d *Driver) removeSubmission(attempt string, n int) error {
	return d.removeIn(filepath.Join(attempt, strconv.Itoa(n)))
}

// removeAttempt removes the attempt's submissions.
func (d *Driver) removeAttempt(attempt string) error {
	if !attemptPattern.MatchString(attempt) {
		return nil
	}
	return d.removeIn(attempt)
}

func (d *Driver) removeIn(rel string) error {
	root, err := os.OpenRoot(d.Dir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	}
	defer root.Close()
	return root.RemoveAll(rel)
}

// staleAfter is how long after its job ended an attempt's submissions are
// kept, unless the coordinator has the driver forget them sooner; and
// abandonedAfter how long after anything last changed in a submission
// whose job ended without saying so, as one killed at its time limit, on a
// node that failed, or cancelled as it queued.
const (
	staleAfter     = 24 * time.Hour
	abandonedAfter = 30 * 24 * time.Hour
)

// sweep removes, once a driver, as it first places a job, the submissions
// of attempts whose jobs all ended long ago, which no coordinator had the
// driver forget (stale).
func (d *Driver) sweep() {
	d.swept.Do(func() {
		entries, err := os.ReadDir(d.Dir)
		if err != nil {
			return
		}
		for _, e := range entries {
			attempt := e.Name()
			if !e.IsDir() || !attemptPattern.MatchString(attempt) {
				continue
			}
			ns, err := d.submissions(attempt)
			if err != nil || len(ns) == 0 {
				continue
			}
			if !slices.ContainsFunc(ns, func(n int) bool { return !d.stale(d.submissionDir(attempt, n)) }) {
				_ = d.removeAttempt(attempt)
			}
		}
	})
}

// stale reports whether the submission sub's job ended long ago: its exit
// record, or its withdrawal, or, for one that has no job, the submission
// itself, is staleAfter old; or its allocation ended, as started.json says,
// staleAfter ago; or, for one whose allocation's end isn't told, nothing in
// it has changed for abandonedAfter.
func (d *Driver) stale(sub string) bool {
	modified := func(name string) (time.Time, bool) {
		info, err := os.Lstat(filepath.Join(sub, name))
		if err != nil {
			return time.Time{}, false
		}
		return info.ModTime(), true
	}
	for _, name := range []string{ExitFile, WithdrawnFile} {
		if t, ok := modified(name); ok && time.Since(t) > staleAfter {
			return true
		}
	}
	newest, ok := modified(".")
	if !ok {
		return false
	}
	if recordedJob(sub) == "" {
		return time.Since(newest) > staleAfter
	}
	// A job that runs or waits longer than abandonedAfter, quietly, on a
	// partition without a time limit, keeps its submission while its
	// allocation may still run.
	if r := readRecords(sub); r.started != nil && !r.started.End.IsZero() {
		return time.Since(r.started.End) > staleAfter
	}
	entries, _ := os.ReadDir(sub)
	for _, e := range entries {
		if t, ok := modified(e.Name()); ok && t.After(newest) {
			newest = t
		}
	}
	return time.Since(newest) > abandonedAfter
}

// jobEnviron returns base, os.Environ() for nil, but GODEBUG, which could
// turn off what the worker requires of its cryptography, every MERIDIAN_
// variable and those withhold names.
func jobEnviron(base, withhold []string) []string {
	if base == nil {
		base = os.Environ()
	}
	out := make([]string, 0, len(base))
	for _, kv := range base {
		name, _, _ := strings.Cut(kv, "=")
		if name != "GODEBUG" && !strings.HasPrefix(name, "MERIDIAN_") && !slices.Contains(withhold, name) {
			out = append(out, kv)
		}
	}
	return out
}
