package workerapisrv_test

import (
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/parallelworks/meridian/coordinator/internal/coordtest"
	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/coordinator/internal/workerapisrv"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/workerapi"
)

// An attempt whose job needs version 3 of the worker API, as the Readier
// says one that calls a workflow, or of a child run, does, refuses a
// worker of version 2 at its claim (worker_api_too_old); one of version 3
// is given what the attempt has of its tree, of the step's try a step run
// runs, and whether its run's directory is one its member chose, which a
// claim of version 2, of a job that doesn't need them, isn't, though its
// run's directory is.
func TestChildClaimNeeds(t *testing.T) {
	parallel(t)
	r := newRig(t, rigOptions{})
	c := newClient(t, r.socket)
	tree := &workerapi.Tree{JobID: "3", RootDir: "/runs/3", WorkflowName: "github.acme.tools", Masked: []string{"s3cr"}}
	job := &plan.ReadyJob{Key: "build", Name: "build", Path: "/jobs/build", Steps: []plan.Step{{Run: plan.Literal("true")}}}
	stepRun := &workerapi.StepRunClaim{Step: 2, Try: 1, State: workerapi.StepRunState{Steps: []workerapi.StepRunResult{{Outcome: plan.OutcomeSuccess}}, MaxRetries: 3}}

	const hostDir = "/home/alice/.local/state/meridian/runs/3-store/subworkflows/main/step_0"
	r.readier.needs = workerapi.Version3
	run := r.newRun(nil)
	r.readier.set(run, &workerapisrv.Ready{Job: job, Run: plan.RunValues{Format: 1}, Tree: tree, StepRun: stepRun, RunDir: hostDir, RunDirOnMember: true})
	_, token := r.newAttempt(run, "build", placed{})
	status, body, err := c.send(http.MethodPost, workerapi.PathClaim, claimRequestAt(t, coordtest.Credential(t), conformanceDigest, workerapi.Version2), token)
	if err != nil || status != http.StatusConflict {
		t.Fatalf("a claim of a worker of version 2 of a job that needs 3 was answered %d, %v: %s", status, err, body)
	}
	p := wantProblem(t, "a claim of a worker too old", status, body, workerapi.WorkerAPITooOld)
	if p.Params[workerapi.ParamNeeded] != float64(workerapi.Version3) {
		t.Errorf("the problem's params are %v", p.Params)
	}

	_, token = r.newAttempt(run, "build", placed{})
	status, body, err = c.send(http.MethodPost, workerapi.PathClaim, claimRequest(t, coordtest.Credential(t), conformanceDigest), token)
	if err != nil || status != http.StatusOK {
		t.Fatalf("the claim was answered %d, %v: %s", status, err, body)
	}
	var claim workerapi.Claim
	if err := workerapi.Unmarshal(body, &claim); err != nil || claim.Tree == nil || claim.Tree.JobID != "3" || claim.Tree.Masked[0] != "s3cr" {
		t.Errorf("a child's claim has of its tree %+v, %v", claim.Tree, err)
	}
	if s := claim.StepRun; s == nil || s.Step != 2 || s.Try != 1 || s.State.MaxRetries != 3 || len(s.State.Steps) != 1 {
		t.Errorf("a step run's claim has of its step's try %+v", s)
	}
	if claim.RunDir != hostDir || !claim.RunDirOnMember {
		t.Errorf("a child's claim on a member that chose its directory gives %q, chosen %v", claim.RunDir, claim.RunDirOnMember)
	}

	// One whose member took the run's directory as it was says so.
	local := r.newRun(nil)
	r.readier.set(local, &workerapisrv.Ready{Job: job, Run: plan.RunValues{Format: 1}, Tree: tree})
	_, token = r.newAttempt(local, "build", placed{})
	status, body, err = c.send(http.MethodPost, workerapi.PathClaim, claimRequest(t, coordtest.Credential(t), conformanceDigest), token)
	if err != nil || status != http.StatusOK {
		t.Fatalf("the claim was answered %d, %v: %s", status, err, body)
	}
	claim = workerapi.Claim{}
	if err := workerapi.Unmarshal(body, &claim); err != nil || claim.RunDir != "/runs/"+local || claim.RunDirOnMember {
		t.Errorf("a child's claim on a member that took its directory gives %q, chosen %v, %v", claim.RunDir, claim.RunDirOnMember, err)
	}

	r.readier.needs = 1
	other := r.newRun(nil)
	r.readier.set(other, &workerapisrv.Ready{Job: job, Run: plan.RunValues{Format: 1}, Tree: tree, StepRun: stepRun, RunDir: hostDir, RunDirOnMember: true})
	_, token = r.newAttempt(other, "build", placed{})
	status, body, err = c.send(http.MethodPost, workerapi.PathClaim, claimRequestAt(t, coordtest.Credential(t), conformanceDigest, workerapi.Version2), token)
	if err != nil || status != http.StatusOK {
		t.Fatalf("the claim of version 2 was answered %d, %v: %s", status, err, body)
	}
	claim = workerapi.Claim{}
	if err := workerapi.Unmarshal(body, &claim); err != nil || claim.Tree != nil || claim.StepRun != nil || claim.RunDirOnMember || claim.RunDir != hostDir {
		t.Errorf("a claim of version 2 has of its tree %+v, of a step's try %+v, and gives %q, chosen %v, %v", claim.Tree, claim.StepRun, claim.RunDir, claim.RunDirOnMember, err)
	}
}

// A poll for a child's end that another process ended, which no one tells
// the server of, is answered once the server reads the store again.
func TestChildEndRechecked(t *testing.T) {
	parallel(t)
	r := newRig(t, rigOptions{hold: 10 * time.Second, configure: func(s *workerapisrv.Server) {
		workerapisrv.SetChildRecheck(s, 50*time.Millisecond)
	}})
	c := newClient(t, r.socket)
	a, _, credential := r.claimed(c, placed{})
	body, err := workerapi.Marshal(workerapi.ChildRequest{With: plan.DataOf(map[string]any{"$yaml": "w.yaml"})})
	if err != nil {
		t.Fatal(err)
	}
	if status, answer, err := c.send(http.MethodPost, workerapi.PathChild, body, credential); err != nil || status != http.StatusOK {
		t.Fatalf("the ask for a child was answered %d, %v: %s", status, err, answer)
	}
	go func() {
		// The delay is the test: the end comes while the poll waits,
		// with nothing telling the server.
		time.Sleep(200 * time.Millisecond) //nolint:forbidigo // as above
		r.inTx(func(tx *store.Tx) error {
			id, err := tx.Child(a.ID, 0, 0)
			if err != nil {
				return err
			}
			return tx.EndRun(id, plan.OutcomeSuccess, "")
		})
	}()
	start := time.Now()
	q := url.Values{workerapi.QueryStep: {"0"}, workerapi.QueryTry: {"0"}}
	status, answer, err := c.send(http.MethodGet, workerapi.PathChildEnd+"?"+q.Encode(), nil, credential)
	var child workerapi.Child
	if err != nil || status != http.StatusOK || workerapi.Unmarshal(answer, &child) != nil || !child.Ended || time.Since(start) > 5*time.Second {
		t.Errorf("a poll for a child another process ended was answered %d, %v, %+v, after %v: %s", status, err, child, time.Since(start), answer)
	}
}
