package workerapisrv_test

import (
	"cmp"
	"testing"
	"time"

	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/coordinator/internal/workerapisrv"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/workerapi"
	"github.com/parallelworks/meridian/workerapi/workerapitest"
)

// The server serves the worker API as package workerapi says, as the suite
// every coordinator passes checks.
func TestConformance(t *testing.T) {
	workerapitest.Conformance(t, newTarget(t))
}

// A target is a rig as workerapitest.Target: each attempt is of a run of
// its own, whose plan is of a version the server reads, but for one of a
// newer plan version, which a replica of an older release serves.
type target struct {
	*rig
	// older is the socket of a replica that reads only plan versions
	// before plan.Version.
	older string
}

var _ workerapitest.Target = (*target)(nil)

func newTarget(t *testing.T) *target {
	r := newRig(t, rigOptions{})
	tg := &target{rig: r}
	before := r.socket
	r.socket = ""
	r.serve(rigOptions{older: true})
	tg.older, r.socket = r.socket, before
	return tg
}

func (tg *target) NewAttempt(t *testing.T, a workerapitest.Attempt) (string, string, string) {
	t.Helper()
	// The run is of the attempt's plan version, or this release's newest,
	// which an older replica serves as one of a newer version.
	c := created(t)
	c.Plan.Version = min(cmp.Or(a.PlanVersion, plan.Version), plan.Version)
	c.Record.Version = c.Plan.Version
	run := tg.newRunOf(c)
	tg.readier.set(run, &workerapisrv.Ready{Job: a.Job, Run: a.Run, Needs: a.Needs})
	at, token := tg.newAttempt(run, "build", placed{
		selfFencing: a.SelfFencing, lease: a.LeaseSeconds, heartbeat: a.HeartbeatSeconds, digest: conformanceDigest,
	})
	if a.PlanVersion > plan.Version {
		return tg.older, token, at.ID
	}
	return tg.socket, token, at.ID
}

func (tg *target) Withdraw(t *testing.T, id string) {
	tg.inTx(func(tx *store.Tx) error { return tx.EndAttempt(id, store.AttemptEnd{Kind: store.EndWithdrawn}) })
}

func (tg *target) Supersede(t *testing.T, id string) {
	tg.inTx(func(tx *store.Tx) error {
		a, err := tx.Attempt(id)
		if err != nil {
			return err
		}
		_, _, err = tx.NewAttempt(store.NewAttempt{Run: a.Run, Job: a.Job, SelfFencing: a.SelfFencing, LeaseSeconds: a.LeaseSeconds, HeartbeatSeconds: a.HeartbeatSeconds})
		return err
	})
}

func (tg *target) Lapse(t *testing.T, id string) {
	tg.inTx(func(tx *store.Tx) error {
		// A day before its last renewal: its lease has lapsed since.
		_, err := tx.SQL().ExecContext(t.Context(), "UPDATE meridian_attempts SET renewed_at = renewed_at - ? WHERE id = ?", (24 * time.Hour).Microseconds(), id)
		return err
	})
}

func (tg *target) Control(t *testing.T, id string, c workerapi.Control) {
	tg.inTx(func(tx *store.Tx) error { return askControl(tx, id, c) })
	tg.server.Notify(id)
}

func (tg *target) ControlHold() time.Duration { return 2 * time.Second }

func (tg *target) Stored(t *testing.T, id string) workerapitest.Stored {
	t.Helper()
	var s workerapitest.Stored
	a := tg.attempt(id)
	tg.inTx(func(tx *store.Tx) error {
		var err error
		s.Events, err = tx.Events(id)
		return err
	})
	s.Logs = tg.logs.Of(a.Run, a.Job, a.Number)
	end, err := a.Complete()
	if err != nil {
		t.Fatal(err)
	}
	s.Complete = end
	return s
}
