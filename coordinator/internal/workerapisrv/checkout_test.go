package workerapisrv_test

import (
	"archive/tar"
	"bytes"
	"errors"
	"io"
	"net/http"
	"testing"

	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/coordinator/internal/workerapisrv"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/workerapi"
)

// treeNames returns the names of the members of the tar stream data.
func treeNames(t *testing.T, data []byte) []string {
	t.Helper()
	var names []string
	tr := tar.NewReader(bytes.NewReader(data))
	for {
		h, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return names
		}
		if err != nil {
			t.Fatalf("the tree isn't a tar stream: %v", err)
		}
		names = append(names, h.Name)
	}
}

// A checkout's tree is served for the request whose action was answered
// success, as a tar stream, and again for a repeat of it, as Actions
// writes it: but not for a try whose action wasn't asked for, or wasn't
// answered success, nor for another request for the try, 400; and any
// once the attempt is superseded is fenced.
func TestCheckoutTree(t *testing.T) {
	r := newRig(t, rigOptions{})
	c := newClient(t, r.socket)
	a, credential := r.claimedAction(c)

	if status, body, err := c.send(http.MethodPost, workerapi.PathCheckout, actionRequest(t, 0), credential); err != nil || status != http.StatusBadRequest {
		t.Errorf("the tree of a try whose action wasn't asked for was answered %d, %v: %s", status, err, body)
	}
	if status, body, err := c.send(http.MethodPost, workerapi.PathAction, actionRequest(t, 0), credential); err != nil || status != http.StatusOK {
		t.Fatalf("the action was answered %d, %v: %s", status, err, body)
	}
	for _, what := range []string{"the tree", "the tree asked for again"} {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "http://coordinator"+workerapi.PathCheckout, bytes.NewReader(actionRequest(t, 0)))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+credential)
		resp, err := c.http.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil || resp.StatusCode != http.StatusOK || resp.Header.Get("Content-Type") != workerapi.CheckoutMediaType {
			t.Fatalf("%s was answered %d, %s, %v", what, resp.StatusCode, resp.Header.Get("Content-Type"), err)
		}
		if names := treeNames(t, data); len(names) != 2 || names[0] != "README" || names[1] != "docs/" {
			t.Errorf("%s holds %v", what, names)
		}
	}

	other, err := workerapi.Marshal(&workerapi.ActionRequest{Step: 1, With: plan.DataOf(map[string]any{"jobs": []any{"y"}})})
	if err != nil {
		t.Fatal(err)
	}
	// The worker's bug, which fails the step, rather than halt the attempt.
	if status, body, err := c.send(http.MethodPost, workerapi.PathCheckout, other, credential); err != nil || status != http.StatusBadRequest {
		t.Errorf("the tree of another request for the try was answered %d, %v: %s", status, err, body)
	}
	// The action asked for again so is still fenced.
	status, body, err := c.send(http.MethodPost, workerapi.PathAction, other, credential)
	if err != nil {
		t.Fatal(err)
	}
	_ = wantProblem(t, "the action of another request for the try", status, body, workerapi.Fenced)

	r.actions.setAnswer(func(*store.Tx, *workerapisrv.ActionCall) (workerapi.ActionAnswer, error) {
		return workerapi.ActionAnswer{Outcome: plan.OutcomeFailure}, nil
	})
	if status, body, err := c.send(http.MethodPost, workerapi.PathAction, actionRequest(t, 1), credential); err != nil || status != http.StatusOK || !bytes.Contains(body, []byte(`"failure"`)) {
		t.Fatalf("the action's next try was answered %d, %v: %s", status, err, body)
	}
	if status, body, err := c.send(http.MethodPost, workerapi.PathCheckout, actionRequest(t, 1), credential); err != nil || status != http.StatusBadRequest {
		t.Errorf("the tree of a try whose action failed was answered %d, %v: %s", status, err, body)
	}
	if status, body, err := c.send(http.MethodGet, workerapi.PathCheckout, nil, credential); err != nil || status != http.StatusMethodNotAllowed {
		t.Errorf("a GET of a tree was answered %d, %v: %s", status, err, body)
	}

	r.newAttempt(a.Run, "build", placed{})
	status, body, err = c.send(http.MethodPost, workerapi.PathCheckout, actionRequest(t, 0), credential)
	if err != nil {
		t.Fatal(err)
	}
	_ = wantProblem(t, "the tree once the attempt was superseded", status, body, workerapi.Fenced)
}

// A tree that Actions fails to write is answered 500 when it fails before
// it writes, and the worker asks again; and is cut short when it fails
// after, so that the worker reads the request as one that failed, rather
// than take what it had for the whole tree.
func TestCheckoutTreeFails(t *testing.T) {
	r := newRig(t, rigOptions{})
	c := newClient(t, r.socket)
	_, credential := r.claimedAction(c)
	if status, body, err := c.send(http.MethodPost, workerapi.PathAction, actionRequest(t, 0), credential); err != nil || status != http.StatusOK {
		t.Fatalf("the action was answered %d, %v: %s", status, err, body)
	}

	r.actions.setTreeFault(treeFailsFirst)
	if status, body, err := c.send(http.MethodPost, workerapi.PathCheckout, actionRequest(t, 0), credential); err != nil || status != http.StatusInternalServerError {
		t.Errorf("a tree that couldn't be read was answered %d, %v: %s", status, err, body)
	}

	r.actions.setTreeFault(treeFailsAfter)
	status, body, err := c.send(http.MethodPost, workerapi.PathCheckout, actionRequest(t, 0), credential)
	if err == nil {
		t.Errorf("a tree cut short was read whole, answered %d: %q", status, body)
	}

	r.actions.setTreeFault("")
	if status, body, err := c.send(http.MethodPost, workerapi.PathCheckout, actionRequest(t, 0), credential); err != nil || status != http.StatusOK {
		t.Errorf("the tree asked for again was answered %d, %v: %s", status, err, body)
	}
}
