package coordinator

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/plan"
)

// A run keeps the texts of the workflows its tree pins beside it, in the
// transaction that stores it, which its child runs are created from; and a
// text that isn't its digest's stores no run.
func TestCreateRunKeepsTexts(t *testing.T) {
	parallel(t)
	c := open(t, Options{})
	created, diags, err := compile.Create(t.Context(), "workflow.yaml", []byte(oneJob), nil, compile.Options{Source: plan.Source{Location: "workflow.yaml"}})
	if err != nil || diags.HasErrors() {
		t.Fatal(diags, err)
	}
	text := "jobs: {a: {steps: [{run: echo}]}}\n"
	sum := sha256.Sum256([]byte(text))
	kept := compile.PinnedText{Digest: "sha256:" + hex.EncodeToString(sum[:]), Text: []byte(text)}
	created.Texts = []compile.PinnedText{kept}
	id, err := c.CreateRun(t.Context(), created, runOptions)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.store.View(t.Context(), func(tx *store.Tx) error {
		got, err := tx.Text(id, kept.Digest)
		if err != nil || string(got) != text {
			t.Errorf("the run keeps %q, %v", got, err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	created.Texts[0].Text = []byte("jobs: {}\n")
	if _, err := c.CreateRun(t.Context(), created, runOptions); err == nil {
		t.Error("a run with a text that isn't its digest's was stored")
	}
}
