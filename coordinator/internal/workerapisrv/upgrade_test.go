package workerapisrv_test

import (
	"bytes"
	"fmt"
	"net/http"
	"slices"
	"testing"
	"time"

	"github.com/parallelworks/meridian/coordinator/internal/coordtest"
	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/coordinator/internal/workerapisrv"
	"github.com/parallelworks/meridian/internal/versiontest"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
	"github.com/parallelworks/meridian/workerapi"
)

// A replica of an older release, in a rolling upgrade, which doesn't read
// the plan of an attempt's run, answers the attempt's worker 503, which it
// tries again, as it does once a replica that reads it serves the socket:
// its claim, and, once a replica that reads the plan has taken the claim,
// every request it authorizes with its credential. It stores nothing.
func TestOlderReplica(t *testing.T) {
	r := newRig(t, rigOptions{})
	current := r.socket
	r.socket = ""
	r.serve(rigOptions{older: true})
	older := newClient(t, r.socket)
	r.socket = current
	// A run of this release's newest plan version, which the older
	// replica doesn't read.
	run := r.newestRun()
	r.readier.set(run, &workerapisrv.Ready{Job: &plan.ReadyJob{Key: "build", Name: "build", Path: "/jobs/build", Steps: []plan.Step{{Run: plan.Literal("true"), Path: "/jobs/build/steps/0"}}}, Run: plan.RunValues{Format: 1}})
	a, token := r.newAttempt(run, "build", placed{})
	credential := coordtest.Credential(t)
	claim := claimRequest(t, credential, conformanceDigest)
	if status, body, err := older.send(http.MethodPost, workerapi.PathClaim, claim, token); err != nil || status != http.StatusServiceUnavailable {
		t.Errorf("an older replica answered a claim %d, %v: %s", status, err, body)
	}
	if got := r.attempt(a.ID); !got.ClaimedAt.IsZero() {
		t.Error("an older replica stored a claim")
	}
	// The worker tries again, and a replica that reads the plan takes it.
	if status, body, err := newClient(t, current).send(http.MethodPost, workerapi.PathClaim, claim, token); err != nil || status != http.StatusOK {
		t.Fatalf("the claim was answered %d, %v: %s", status, err, body)
	}
	if status, body, err := older.send(http.MethodGet, workerapi.PathControl+"?"+workerapi.QueryVersion+"=0", nil, credential); err != nil || status != http.StatusServiceUnavailable {
		t.Errorf("an older replica answered a control poll %d, %v: %s", status, err, body)
	}
	if status, body, err := older.send(http.MethodPost, workerapi.PathComplete, []byte(`{"seq":0,"result":{"outcome":"success"}}`), credential); err != nil || status != http.StatusServiceUnavailable {
		t.Errorf("an older replica answered an end %d, %v: %s", status, err, body)
	}
}

// A claim is served at the lower of the worker's version of the worker API
// and the replica's, which the attempt keeps: a worker of a later version
// than the replica's is served at the replica's, and one of an earlier
// version at its own, by a replica of a later one. A replica that doesn't
// speak the version an attempt was claimed at, as one of an older release
// in a rolling upgrade, answers its worker 503, which it tries again: its
// claim repeated, and every request it authorizes with its credential. It
// stores nothing.
func TestAPIVersions(t *testing.T) {
	r := newRig(t, rigOptions{})
	current := newClient(t, r.socket)
	socket := r.socket
	r.socket = ""
	r.serve(rigOptions{configure: func(s *workerapisrv.Server) { workerapisrv.SetSpeaks(s, workerapi.Version+1) }})
	newer := newClient(t, r.socket)
	r.socket = socket
	job := &plan.ReadyJob{Key: "build", Name: "build", Path: "/jobs/build", Steps: []plan.Step{{Run: plan.Literal("true"), Path: "/jobs/build/steps/0"}}}

	for _, tc := range []struct {
		name    string
		replica *client
		worker  int
		want    int
	}{
		{"a later worker, by the current replica", current, workerapi.Version + 1, workerapi.Version},
		{"the current worker, by a later replica", newer, workerapi.Version, workerapi.Version},
		{"a later worker, by a later replica", newer, workerapi.Version + 1, workerapi.Version + 1},
		// A worker of a release from before the API had a version says
		// none, and speaks version 1.
		{"a worker without a version, by a later replica", newer, 0, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			run := r.newRun(nil)
			r.readier.set(run, &workerapisrv.Ready{Job: job, Run: plan.RunValues{Format: 1}})
			a, token := r.newAttempt(run, "build", placed{})
			credential := coordtest.Credential(t)
			claim := claimRequestAt(t, credential, conformanceDigest, tc.worker)
			status, body, err := tc.replica.send(http.MethodPost, workerapi.PathClaim, claim, token)
			if err != nil || status != http.StatusOK {
				t.Fatalf("the claim was answered %d, %v: %s", status, err, body)
			}
			var c workerapi.Claim
			if err := workerapi.Unmarshal(body, &c); err != nil {
				t.Fatal(err)
			}
			if c.APIVersion != tc.want {
				t.Errorf("a worker of version %d was served at %d, not %d", tc.worker, c.APIVersion, tc.want)
			}
			if got := r.attempt(a.ID); got.APIVersion != tc.want {
				t.Errorf("the attempt was claimed at version %d, not %d", got.APIVersion, tc.want)
			}
			// The claim repeated, to either replica, is the same, but by a
			// replica that doesn't speak the attempt's version.
			for name, replica := range map[string]*client{"the current replica": current, "a later replica": newer} {
				status, again, err := replica.send(http.MethodPost, workerapi.PathClaim, claim, token)
				switch {
				case err != nil:
					t.Fatal(err)
				case replica == current && tc.want > workerapi.Version:
					if status != http.StatusServiceUnavailable {
						t.Errorf("%s answered the claim repeated of an attempt of version %d %d: %s", name, tc.want, status, again)
					}
				case status != http.StatusOK || string(again) != string(body):
					t.Errorf("%s answered the claim repeated %d:\n%s\nnot\n%s", name, status, again, body)
				}
			}
			// A path, or a method, the current replica doesn't have is
			// another replica's, for an attempt of a later version, which
			// may have it; and one the API doesn't have, for another.
			for _, req := range []struct{ method, path string }{{http.MethodPost, "/v1/later"}, {http.MethodPut, workerapi.PathEvents}} {
				want := http.StatusNotFound
				switch {
				case tc.want > workerapi.Version:
					want = http.StatusServiceUnavailable
				case req.method == http.MethodPut:
					want = http.StatusMethodNotAllowed
				}
				if status, body, err := current.send(req.method, req.path, nil, credential); err != nil || status != want {
					t.Errorf("the current replica answered %s %s %d, %v, not %d: %s", req.method, req.path, status, err, want, body)
				}
			}
			if tc.want <= workerapi.Version {
				return
			}
			for _, req := range []struct{ method, path, bearer string }{
				{http.MethodPost, "/v1/later", ""},
				{http.MethodPost, "/v1/later", coordtest.Credential(t)},
				{http.MethodPost, "/v2/later", credential},
			} {
				if status, body, err := current.send(req.method, req.path, nil, req.bearer); err != nil || status != http.StatusNotFound {
					t.Errorf("the current replica answered %s %s, without the attempt's credential or outside /v1/, %d, %v: %s", req.method, req.path, status, err, body)
				}
			}
			if status, body, err := current.send(http.MethodGet, workerapi.PathControl+"?"+workerapi.QueryVersion+"=0", nil, credential); err != nil || status != http.StatusServiceUnavailable {
				t.Errorf("the current replica answered a control poll of an attempt of a later version %d, %v: %s", status, err, body)
			}
			if status, body, err := current.send(http.MethodPost, workerapi.PathComplete, []byte(`{"seq":0,"result":{"outcome":"success"}}`), credential); err != nil || status != http.StatusServiceUnavailable {
				t.Errorf("the current replica answered an end of an attempt of a later version %d, %v: %s", status, err, body)
			}
			if got := r.attempt(a.ID); got.State == store.AttemptEnded {
				t.Error("the current replica stored the end of an attempt of a later version")
			}
			if status, body, err := newer.send(http.MethodPost, workerapi.PathComplete, []byte(`{"seq":0,"result":{"outcome":"success"}}`), credential); err != nil || status != http.StatusNoContent {
				t.Errorf("the later replica answered the end %d, %v: %s", status, err, body)
			}
		})
	}
}

// A replica that can't serve an attempt, as one of an older release that
// doesn't speak the version of the API it was claimed at, or doesn't read
// its run's plan, still answers what needs neither: an attempt superseded
// meanwhile, as after a rollback that left only such replicas, is told it
// no longer holds its job, by its heartbeat, its events, a log upload and
// its claim repeated, as a replica that serves it tells it, so that its
// worker stops; and a credential that has lapsed with a lease is 401. A
// control poll and an end, whose answers depend on the attempt, are
// another replica's still, 503.
func TestForeignAttemptFenced(t *testing.T) {
	r := newRig(t, rigOptions{})
	socket := r.socket
	r.socket = ""
	r.serve(rigOptions{configure: func(s *workerapisrv.Server) { workerapisrv.SetSpeaks(s, workerapi.Version+1) }})
	newer := newClient(t, r.socket)
	r.socket = ""
	r.serve(rigOptions{older: true})
	older := newClient(t, r.socket)
	r.socket = socket
	current := newClient(t, socket)
	job := &plan.ReadyJob{Key: "build", Name: "build", Path: "/jobs/build", Steps: []plan.Step{{Run: plan.Literal("true"), Path: "/jobs/build/steps/0"}}}
	for _, tc := range []struct {
		name    string
		replica *client
		// apiVersion is what the attempt is claimed at.
		apiVersion int
	}{
		{"a replica that doesn't speak the attempt's version", current, workerapi.Version + 1},
		{"a replica that doesn't read the run's plan", older, workerapi.Version},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, selfFencing := range []bool{false, true} {
				// Of the newest plan version, which the older replica
				// doesn't read.
				run := r.newestRun()
				r.readier.set(run, &workerapisrv.Ready{Job: job, Run: plan.RunValues{Format: 1}})
				a, token := r.newAttempt(run, "build", placed{selfFencing: selfFencing, lease: 60, heartbeat: 10})
				credential := coordtest.Credential(t)
				claim := claimRequestAt(t, credential, conformanceDigest, tc.apiVersion)
				if status, body, err := newer.send(http.MethodPost, workerapi.PathClaim, claim, token); err != nil || status != http.StatusOK {
					t.Fatalf("the claim was answered %d, %v: %s", status, err, body)
				}
				if status, body, _ := tc.replica.send(http.MethodPost, workerapi.PathHeartbeat, nil, credential); status != http.StatusServiceUnavailable {
					t.Errorf("a heartbeat of an attempt that holds its job was answered %d: %s", status, body)
				}
				if selfFencing {
					// Its lease lapsed a day ago.
					r.inTx(func(tx *store.Tx) error {
						_, err := tx.SQL().ExecContext(t.Context(), "UPDATE meridian_attempts SET renewed_at = renewed_at - ? WHERE id = ?", (24 * time.Hour).Microseconds(), a.ID)
						return err
					})
					if status, body, _ := tc.replica.send(http.MethodPost, workerapi.PathHeartbeat, nil, credential); status != http.StatusUnauthorized {
						t.Errorf("a heartbeat of an attempt whose credential lapsed was answered %d: %s", status, body)
					}
					continue
				}
				r.inTx(func(tx *store.Tx) error {
					_, _, err := tx.NewAttempt(store.NewAttempt{Run: a.Run, Job: a.Job, LeaseSeconds: 60, HeartbeatSeconds: 10})
					return err
				})
				for _, req := range []struct {
					name, method, path string
					body               []byte
					bearer             string
				}{
					{"a heartbeat", http.MethodPost, workerapi.PathHeartbeat, nil, credential},
					{"events", http.MethodPost, workerapi.PathEvents, []byte(`{"events":[]}`), credential},
					{"a log upload", http.MethodPost, workerapi.PathLogs + "?part=setup&offset=0", []byte("x"), credential},
					{"the claim repeated", http.MethodPost, workerapi.PathClaim, claim, token},
				} {
					status, body, err := tc.replica.send(req.method, req.path, req.body, req.bearer)
					if err != nil {
						t.Fatal(err)
					}
					if status != http.StatusConflict {
						t.Errorf("%s of a superseded attempt was answered %d: %s", req.name, status, body)
						continue
					}
					_ = wantProblem(t, req.name+" of a superseded attempt", status, body, workerapi.Fenced)
				}
				for _, req := range []struct{ name, method, path string }{
					{"a control poll", http.MethodGet, workerapi.PathControl + "?" + workerapi.QueryVersion + "=0"},
					{"an end", http.MethodPost, workerapi.PathComplete},
				} {
					if status, body, _ := tc.replica.send(req.method, req.path, []byte(`{"seq":0,"result":{"outcome":"success"}}`), credential); status != http.StatusServiceUnavailable {
						t.Errorf("%s of a superseded attempt was answered %d: %s", req.name, status, body)
					}
				}
			}
		})
	}
}

// The server reads and writes every body of an attempt at the attempt's
// version of the worker API, through workerapi.CheckVersion, with members
// made a later version's than this one (versiontest): of an attempt of
// this version, it refuses events and an end that hold one, 400, and
// stores none of them, and never sends one, as a control that asks one,
// which it answers 500 instead; of an attempt of the later version, it
// takes and sends them. It doesn't run in parallel with others, whose
// bodies the members it sets would change.
func TestGatePinned(t *testing.T) {
	later := workerapi.Version + 1
	t.Cleanup(versiontest.Set("Event.cleanup", later))
	t.Cleanup(versiontest.Set("Control.anyJobFailed", later))
	t.Cleanup(versiontest.Set("Complete.reason", later))
	r := newRig(t, rigOptions{configure: func(s *workerapisrv.Server) { workerapisrv.SetSpeaks(s, later) }})
	c := newClient(t, r.socket)
	job := &plan.ReadyJob{Key: "build", Name: "build", Path: "/jobs/build", Steps: []plan.Step{{Run: plan.Literal("true"), Path: "/jobs/build/steps/0"}}}
	for _, version := range []int{workerapi.Version, later} {
		run := r.newRun(nil)
		r.readier.set(run, &workerapisrv.Ready{Job: job, Run: plan.RunValues{Format: 1}})
		a, token := r.newAttempt(run, "build", placed{})
		credential := coordtest.Credential(t)
		if status, body, err := c.send(http.MethodPost, workerapi.PathClaim, claimRequestAt(t, credential, conformanceDigest, version), token); err != nil || status != http.StatusOK {
			t.Fatalf("the claim was answered %d, %v: %s", status, err, body)
		}

		events := []byte(`{"events":[{"seq":1,"time":"2026-01-02T03:04:05Z","step":0,"cleanup":true}]}`)
		status, body, err := c.send(http.MethodPost, workerapi.PathEvents, events, credential)
		var stored []workerapi.Event
		r.inTx(func(tx *store.Tx) error {
			var err error
			stored, err = tx.Events(a.ID)
			return err
		})
		switch {
		case err != nil:
			t.Fatal(err)
		case version < later && (status != http.StatusBadRequest || len(stored) != 0):
			t.Errorf("an attempt of version %d's events with a later member were answered %d, and %d stored: %s", version, status, len(stored), body)
		case version == later && (status != http.StatusOK || len(stored) != 1 || !stored[0].Cleanup):
			t.Errorf("an attempt of version %d's events were answered %d, and stored as %+v: %s", version, status, stored, body)
		}

		r.inTx(func(tx *store.Tx) error { return askControl(tx, a.ID, workerapi.Control{AnyJobFailed: true}) })
		status, body, err = c.send(http.MethodGet, workerapi.PathControl+"?"+workerapi.QueryVersion+"=0", nil, credential)
		switch {
		case err != nil:
			t.Fatal(err)
		case version < later && (status != http.StatusInternalServerError || bytes.Contains(body, []byte("anyJobFailed"))):
			t.Errorf("an attempt of version %d was sent a later member, %d: %s", version, status, body)
		case version == later && (status != http.StatusOK || !bytes.Contains(body, []byte(`"anyJobFailed":true`))):
			t.Errorf("an attempt of version %d wasn't sent its control, %d: %s", version, status, body)
		}

		end := fmt.Appendf(nil, `{"seq":%d,"result":{"outcome":"failure"},"reason":"timeout"}`, len(stored))
		status, body, err = c.send(http.MethodPost, workerapi.PathComplete, end, credential)
		ended := r.attempt(a.ID).State == store.AttemptEnded
		switch {
		case err != nil:
			t.Fatal(err)
		case version < later && (status != http.StatusBadRequest || ended):
			t.Errorf("an attempt of version %d's end with a later member was answered %d, and stored: %v: %s", version, status, ended, body)
		case version == later && (status != http.StatusNoContent || !ended):
			t.Errorf("an attempt of version %d's end was answered %d, and stored: %v: %s", version, status, ended, body)
		}
	}
}

// A claim that a replica of a later version of the API takes meanwhile,
// between the current replica's refusals and its claim, at the later
// version, is another replica's: the current one answers 503, which the
// worker tries again, rather than a claim of a version it doesn't speak.
func TestClaimedMeanwhile(t *testing.T) {
	var (
		r     *rig
		claim func()
	)
	r = newRig(t, rigOptions{configure: func(s *workerapisrv.Server) {
		workerapisrv.SetBeforeClaim(s, func() { claim() })
	}})
	run := r.newRun(nil)
	r.readier.set(run, &workerapisrv.Ready{Job: &plan.ReadyJob{Key: "build", Name: "build", Path: "/jobs/build", Steps: []plan.Step{{Run: plan.Literal("true")}}}})
	a, token := r.newAttempt(run, "build", placed{})
	credential := coordtest.Credential(t)
	claim = func() {
		r.inTx(func(tx *store.Tx) error {
			_, err := tx.Claim(token, credential, workerapi.Worker{Version: "v0", Digest: conformanceDigest}, workerapi.Version+1)
			return err
		})
	}
	status, body, err := newClient(t, r.socket).send(http.MethodPost, workerapi.PathClaim, claimRequestAt(t, credential, conformanceDigest, workerapi.Version+1), token)
	if err != nil || status != http.StatusServiceUnavailable {
		t.Errorf("a claim that a later replica took meanwhile was answered %d, %v: %s", status, err, body)
	}
	if got := r.attempt(a.ID); got.APIVersion != workerapi.Version+1 {
		t.Errorf("the attempt is claimed at version %d", got.APIVersion)
	}
}

// A worker whose version of the worker API is older than the attempt needs,
// as a worker of this version is for an attempt that needs a later one,
// is refused before anything of the run is sent, with worker_api_too_old,
// and the attempt ends, failed before its first step, with the problem,
// which its job's supervisor is woken to place again.
func TestWorkerAPITooOld(t *testing.T) {
	r := newRig(t, rigOptions{configure: func(s *workerapisrv.Server) {
		workerapisrv.SetSpeaks(s, workerapi.Version+1)
		workerapisrv.SetNeeds(s, workerapi.Version+1)
	}})
	c := newClient(t, r.socket)
	run := r.newRun(nil)
	r.readier.set(run, &workerapisrv.Ready{Job: &plan.ReadyJob{Key: "build", Name: "build", Path: "/jobs/build", Steps: []plan.Step{{Run: plan.Literal("true")}}}})
	a, token := r.newAttempt(run, "build", placed{})
	status, body, err := c.send(http.MethodPost, workerapi.PathClaim, claimRequest(t, coordtest.Credential(t), conformanceDigest), token)
	if err != nil || status != http.StatusConflict {
		t.Fatalf("a claim of a worker older than the attempt needs was answered %d, %v: %s", status, err, body)
	}
	p := wantProblem(t, "a claim of a worker too old", status, body, workerapi.WorkerAPITooOld)
	if p.Params[workerapi.ParamVersion] != float64(workerapi.Version) || p.Params[workerapi.ParamNeeded] != float64(workerapi.Version+1) {
		t.Errorf("the problem's params are %v", p.Params)
	}
	if bytes.Contains(body, []byte(`"build"`)) || bytes.Contains(body, []byte("/runs/")) {
		t.Errorf("the refusal holds something of the run: %s", body)
	}
	got := r.attempt(a.ID)
	if got.State != store.AttemptEnded || got.End.Kind != store.EndSetupFailed || got.End.Reason != plan.ReasonSetupFailed ||
		got.End.Problem == nil || got.End.Problem.Code != problems.WorkerAPITooOld.Code() || !got.ClaimedAt.IsZero() {
		t.Errorf("the attempt is %+v, ended %+v", got, got.End)
	}
	if woken := r.wokenJobs(); !slices.Contains(woken, run+"/build") {
		t.Errorf("the job's supervisor wasn't woken: %v", woken)
	}
}

// An end stored as the attempt is withdrawn, between the server's checks
// of it and its transaction, is refused, fenced, and the attempt stays
// withdrawn.
func TestEndWithdrawnMeanwhile(t *testing.T) {
	var r *rig
	var a *store.Attempt
	r = newRig(t, rigOptions{configure: func(s *workerapisrv.Server) {
		workerapisrv.SetBeforeEnd(s, func() {
			r.inTx(func(tx *store.Tx) error { return tx.EndAttempt(a.ID, store.AttemptEnd{Kind: store.EndWithdrawn}) })
		})
	}})
	c := newClient(t, r.socket)
	a, _, credential := r.claimed(c, placed{})
	status, body, err := c.send(http.MethodPost, workerapi.PathComplete, []byte(`{"seq":0,"result":{"outcome":"success"}}`), credential)
	if err != nil {
		t.Fatal(err)
	}
	_ = wantProblem(t, "an end of an attempt withdrawn meanwhile", status, body, workerapi.Fenced)
	if got := r.attempt(a.ID); got.End.Kind != store.EndWithdrawn {
		t.Errorf("the attempt ended %+v", got.End)
	}
}
