package store

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/parallelworks/meridian/compile"
)

// KeepTexts keeps texts, the texts of the workflows the run run pins
// (compile.Created.Texts), beside it, in the transaction that stores it,
// so that a child run of its tree is created from the text the run was
// created with, without a fetch, by any replica. A text is kept once, by
// its digest, whatever runs pin it; one that another run kept already must
// be the same bytes, as its digest says it is. Only the bytes are kept:
// where the run found a text is its plan's, so another run that pins the
// same bytes from elsewhere learns nothing of it. A text that isn't its
// digest's is refused, and so is a run that has ended or isn't there
// (ErrState, ErrNotFound).
//
// They are kept while the run is: nothing deletes a run yet. Whatever
// comes to delete one deletes its rows of meridian_run_sources with it,
// and then the texts no run pins.
func (tx *Tx) KeepTexts(run string, texts []compile.PinnedText) error {
	n, err := tx.liveRun(run)
	if err != nil {
		return err
	}
	for _, t := range texts {
		if err := checkText(t.Digest, t.Text); err != nil {
			return fmt.Errorf("store: a text run %s pins: %w", run, err)
		}
		if _, err := tx.exec(`INSERT INTO meridian_sources (digest, text) VALUES (?, ?)
ON CONFLICT (digest) DO NOTHING`, t.Digest, t.Text); err != nil {
			return err
		}
		if _, err := tx.exec(`INSERT INTO meridian_run_sources (run_id, digest) VALUES (?, ?)
ON CONFLICT (run_id, digest) DO NOTHING`, n, t.Digest); err != nil {
			return err
		}
	}
	return nil
}

// Text returns the text of the digest digest, which the run run pins:
// ErrNotFound for one the run doesn't, and ErrMalformed for bytes that
// aren't the digest's text, which the store changed since.
func (tx *Tx) Text(run, digest string) ([]byte, error) {
	n, ok := runNumber(run)
	if !ok {
		return nil, ErrNotFound
	}
	var text []byte
	err := tx.queryRow(`SELECT s.text FROM meridian_sources s
JOIN meridian_run_sources r ON r.digest = s.digest
WHERE r.run_id = ? AND s.digest = ?`, n, digest).Scan(&text)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if err := checkText(digest, text); err != nil {
		return nil, fmt.Errorf("%w: a text run %s pins: %w", ErrMalformed, run, err)
	}
	return text, nil
}

// checkText checks that text is the text of digest: sha256: and the hex
// of its SHA-256 digest, as a plan's Source writes it.
func checkText(digest string, text []byte) error {
	sum := sha256.Sum256(text)
	if want := "sha256:" + hex.EncodeToString(sum[:]); digest != want {
		return fmt.Errorf("its digest is %s, not %s", want, digest)
	}
	return nil
}
