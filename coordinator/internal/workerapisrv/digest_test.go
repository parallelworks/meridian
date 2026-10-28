package workerapisrv_test

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"net/http"
	"testing"

	"github.com/parallelworks/meridian/coordinator/internal/seal"
	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/workerapi"
)

// An end of an attempt that completed is answered as the first was,
// whatever it holds: the end stored first is the attempt's, and the store
// keeps nothing made from it, which can hold a secret, to tell a repeat of
// it from another end. An end of an attempt that ended otherwise is fenced.
func TestEndRepeated(t *testing.T) {
	r := newRig(t, rigOptions{})
	c := newClient(t, r.socket)
	a, _, credential := r.claimed(c, placed{})
	secretEnd := []byte(`{"seq":0,"result":{"outcome":"success","outputs":{"token":"hunter2"},"secrets":["token"]}}`)
	if status, body, _ := c.send(http.MethodPost, workerapi.PathComplete, secretEnd, credential); status != http.StatusNoContent {
		t.Fatalf("the end was answered %d: %s", status, body)
	}
	first := r.attempt(a.ID).End
	for what, end := range map[string][]byte{"the same end": secretEnd, "another end": []byte(`{"seq":0,"result":{"outcome":"failure"}}`)} {
		if status, body, _ := c.send(http.MethodPost, workerapi.PathComplete, end, credential); status != http.StatusNoContent {
			t.Errorf("%s again was answered %d: %s", what, status, body)
		}
	}
	if got := r.attempt(a.ID).End; got.Outcome != plan.OutcomeSuccess || string(got.Report) != string(first.Report) {
		t.Errorf("the attempt's end is %+v, not the first, %+v", got, first)
	}
	r.inTx(func(tx *store.Tx) error {
		var digest []byte
		if err := tx.SQL().QueryRowContext(t.Context(), "SELECT complete_digest FROM meridian_attempts WHERE id = ?", a.ID).Scan(&digest); err != nil {
			return err
		}
		if digest != nil {
			t.Errorf("the store keeps %x of the end", digest)
		}
		return nil
	})

	for _, kind := range []store.EndKind{store.EndWithdrawn, store.EndLost, store.EndSuperseded} {
		b, _, credential := r.claimed(c, placed{})
		r.inTx(func(tx *store.Tx) error { return tx.EndAttempt(b.ID, store.AttemptEnd{Kind: kind}) })
		status, body, _ := c.send(http.MethodPost, workerapi.PathComplete, secretEnd, credential)
		_ = wantProblem(t, "an end of an attempt "+string(kind), status, body, workerapi.Fenced)
	}
}

// An action's request, whose with: can hold a secret, is kept by the digest
// that its run's data key gives it (seal.Sealer.Digest), and not by a
// plain SHA-256 of it, against which whoever holds the store could test a
// guess at the secret. A repeat matches it, and another request for the
// same try doesn't, and is fenced, but where the digest isn't keyed, as a
// release before kept it; so is a request of an attempt whose run has
// ended, whose data key went with it.
func TestActionDigestKeyed(t *testing.T) {
	r := newRig(t, rigOptions{})
	c := newClient(t, r.socket)
	a, credential := r.claimedAction(c)
	request := actionRequest(t, 0)
	for _, what := range []string{"the request", "the same request again"} {
		if status, body, err := c.send(http.MethodPost, workerapi.PathAction, request, credential); err != nil || status != http.StatusOK {
			t.Fatalf("%s was answered %d, %v: %s", what, status, err, body)
		}
	}
	var (
		stored  *store.Action
		wrapped []byte
	)
	r.inTx(func(tx *store.Tx) error {
		var err error
		if stored, err = tx.Action(a.ID, 1, 0); err != nil {
			return err
		}
		wrapped, err = tx.WrappedKey(a.Run)
		return err
	})
	want, err := r.sealer.Digest(t.Context(), a.Run, wrapped, seal.PurposeActionDigest, a.ID+"/1/0", request)
	if err != nil {
		t.Fatal(err)
	}
	if plain := sha256.Sum256(request); hmac.Equal(stored.Digest[:], plain[:]) || !hmac.Equal(stored.Digest[:], want) {
		t.Errorf("the store keeps %x of the request, not its run's digest of it, %x", stored.Digest, want)
	}
	other, err := workerapi.Marshal(&workerapi.ActionRequest{Step: 1, Try: 0, With: plan.DataOf(map[string]any{"jobs": []any{"y"}})})
	if err != nil {
		t.Fatal(err)
	}
	status, body, _ := c.send(http.MethodPost, workerapi.PathAction, other, credential)
	_ = wantProblem(t, "another request for the same try", status, body, workerapi.Fenced)

	// A digest that isn't keyed, as a release before kept, which the store
	// erases, can't be checked: another request is taken for a repeat.
	r.inTx(func(tx *store.Tx) error {
		_, err := tx.SQL().ExecContext(t.Context(), "UPDATE meridian_actions SET digest = zeroblob(32), keyed = 0 WHERE attempt_id = ?", a.ID)
		return err
	})
	if status, body, err := c.send(http.MethodPost, workerapi.PathAction, other, credential); err != nil || status != http.StatusOK || !bytes.Contains(body, []byte(`"success"`)) {
		t.Errorf("another request for a try whose digest isn't keyed was answered %d, %v: %s", status, err, body)
	}
	if n := r.actions.times(); n != 1 {
		t.Errorf("the action was performed %d times", n)
	}

	b, credential := r.claimedAction(c)
	r.inTx(func(tx *store.Tx) error { return tx.EndAttempt(b.ID, store.AttemptEnd{Kind: store.EndWithdrawn}) })
	r.inTx(func(tx *store.Tx) error { return tx.EndRun(b.Run, plan.OutcomeCancelled, plan.ReasonCancelled) })
	status, body, _ = c.send(http.MethodPost, workerapi.PathAction, request, credential)
	_ = wantProblem(t, "a request of an attempt whose run has ended", status, body, workerapi.Fenced)
}
