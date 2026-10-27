package workerapisrv_test

import (
	"net/http"
	"testing"

	"github.com/parallelworks/meridian/coordinator/internal/coordtest"
	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/coordinator/internal/workerapisrv"
	"github.com/parallelworks/meridian/plan"
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
	run := r.newRun(nil)
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
