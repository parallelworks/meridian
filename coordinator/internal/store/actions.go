package store

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"

	"github.com/parallelworks/meridian/workerapi"
)

// An Action is what the store keeps of a built-in action that a step's try
// asked the coordinator for (workerapi.PathAction): the SHA-256 digest of
// the whole request, as the worker API encodes it, its step, try, with:
// and outlivesCancel, by which a repeat is known from another request for
// the same try, never the request, whose with: can hold a secret; and the
// answer it was given.
type Action struct {
	Digest [sha256.Size]byte
	Answer workerapi.ActionAnswer
}

// Action returns what is stored of the action that the step's try of the
// attempt id asked for, or ErrNotFound before it was answered.
func (tx *Tx) Action(id string, step, try int) (*Action, error) {
	var digest, answer []byte
	err := tx.queryRow("SELECT digest, answer FROM meridian_actions WHERE attempt_id = ? AND step = ? AND try = ?", id, step, try).Scan(&digest, &answer)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	a := new(Action)
	if len(digest) != len(a.Digest) {
		return nil, fmt.Errorf("%w: the digest of attempt %s's action of step %d's try %d", ErrMalformed, id, step, try)
	}
	copy(a.Digest[:], digest)
	if err := workerapi.Unmarshal(answer, &a.Answer); err != nil {
		return nil, fmt.Errorf("%w: attempt %s's action of step %d's try %d: %w", ErrMalformed, id, step, try, err)
	}
	return a, nil
}

// RecordAction stores the answer to the action that the step's try of the
// attempt id asked for, as a, which every repeat of its request is given:
// ErrState if the try has an answer already.
func (tx *Tx) RecordAction(id string, step, try int, a Action) error {
	answer, err := workerapi.Marshal(&a.Answer)
	if err != nil {
		return err
	}
	now, err := tx.now()
	if err != nil {
		return err
	}
	res, err := tx.exec(`INSERT INTO meridian_actions (attempt_id, step, try, digest, answer, stored_at) VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT (attempt_id, step, try) DO NOTHING`, id, step, try, a.Digest[:], answer, now)
	if err != nil {
		return err
	}
	return oneRow(res, fmt.Errorf("%w: attempt %s's step %d's try %d has an action's answer", ErrState, id, step, try))
}

// SameRequest reports whether a request whose digest is digest is the one
// a was the answer to, or a repeat of it.
func (a *Action) SameRequest(digest [sha256.Size]byte) bool {
	return bytes.Equal(a.Digest[:], digest[:])
}
