package seal

import (
	"context"
	"crypto/cipher"
	"crypto/hkdf"
	"crypto/sha256"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/parallelworks/meridian/plan"
)

// A Purpose says what a part of a run's secrets sealed apart from its
// record is (SealPart), or what a digest of the run's is of (Digest): a
// label, versioned, as in meridian/job-record/1. It is UTF-8, not empty,
// and holds no NUL. A part sealed for one purpose opens for no other, and
// a digest of one purpose is no other's.
type Purpose string

// PurposeJobRecord is a job's part of its run's secret record: what the
// record holds of the job, or of a matrix instance (plan.Record.Jobs),
// whose key is the part's ID (SealJob).
const PurposeJobRecord Purpose = "meridian/job-record/1"

// PurposePlacement is where an attempt is placed when where its job runs
// is private: its group, member and backend, and, once its backend has
// placed it, the backend's ID for the placement. The attempt's ID is the
// part's ID.
const PurposePlacement Purpose = "meridian/placement/1"

// A PartID names a part of a run's secrets sealed apart from its record:
// what it is for, its ID among the run's parts of that purpose, as a job's
// key, which is UTF-8, not empty, and holds no NUL, and its generation,
// from 1, one more each time the part is sealed again, which the caller
// keeps apart from the part, as it keeps the record's.
type PartID struct {
	Purpose    Purpose
	ID         string
	Generation uint64
}

// A SealedPart is a part as the store keeps it: its ID, and the part,
// sealed (SealPart).
type SealedPart struct {
	PartID
	Sealed []byte
}

// SealPart seals data, a part of the secrets of the run runID that is kept
// apart from its record, as id, and returns it sealed: a byte that says how
// it was sealed, then the nonce, the ciphertext and GCM's tag. It is
// encrypted with AES-256-GCM, with a random nonce, under a key of the
// part's own, which HKDF-SHA256 derives from the run's data key (which
// Keys wrapped as wrapped, Sealed.Key, and unwraps once), with no salt and
// the info
//
//	purpose NUL storeID NUL runID NUL partID NUL generation
//
// which is the part's associated data too. So a part opens only as what it
// was sealed as, the latest generation of its own purpose, ID, run and
// store; and since each derived key seals one generation of one part, once,
// or again only for a write that raced another, the parts, however many a
// run has, take nothing from the bound on the nonces under its data key
// (MaxReseals). The caller clears data once it is done with it.
func (s *Sealer) SealPart(ctx context.Context, runID string, wrapped []byte, id PartID, data []byte) ([]byte, error) {
	if err := checkPartID(runID, id); err != nil {
		return nil, err
	}
	key, err := s.unwrap(ctx, runID, wrapped)
	if err != nil {
		return nil, err
	}
	defer clear(key)
	info := s.partInfo(runID, id)
	aead, err := partAEAD(key, info)
	if err != nil {
		return nil, err
	}
	return aead.Seal([]byte{version}, nil, data, info), nil //nolint:gosec // NewGCMWithRandomNonce takes no nonce
}

// OpenParts opens parts, parts of the secrets of the run runID that are
// kept apart from its record, under keys that the run's data key derives,
// which Keys wrapped as wrapped and unwraps once for all of them, and
// returns what each holds, in their order. A part that doesn't open as what
// it names, its purpose's, ID's, run's and store's latest, is
// record_unreadable, as a record is (Open). The caller clears what it
// returns once it is done with it.
func (s *Sealer) OpenParts(ctx context.Context, runID string, wrapped []byte, parts []SealedPart) ([][]byte, error) {
	if err := checkID("run", runID); err != nil {
		return nil, err
	}
	for _, p := range parts {
		if err := checkPartID(runID, p.PartID); err != nil {
			return nil, err
		}
		if err := checkVersion(runID, p.Sealed); err != nil {
			return nil, err
		}
	}
	if len(parts) == 0 {
		return nil, nil
	}
	key, err := s.unwrap(ctx, runID, wrapped)
	if err != nil {
		return nil, err
	}
	defer clear(key)
	out := make([][]byte, len(parts))
	for i, p := range parts {
		info := s.partInfo(runID, p.PartID)
		aead, err := partAEAD(key, info)
		if err != nil {
			clearAll(out)
			return nil, err
		}
		if out[i], err = aead.Open(nil, nil, p.Sealed[1:], info); err != nil { //nolint:gosec // NewGCMWithRandomNonce takes no nonce
			clearAll(out)
			return nil, unreadable(runID)
		}
	}
	return out, nil
}

// partAEAD returns AES-256-GCM, with a random nonce, under the key of the
// part whose info is info: derived from the run's data key key with
// HKDF-SHA256.
func partAEAD(key, info []byte) (cipher.AEAD, error) {
	partKey, err := hkdf.Key(sha256.New, key, nil, string(info), keySize)
	if err != nil {
		return nil, err
	}
	defer clear(partKey)
	return newAEAD(partKey)
}

// partInfo is the HKDF info, and the associated data, of the part id of the
// run runID.
func (s *Sealer) partInfo(runID string, id PartID) []byte {
	return []byte(string(id.Purpose) + "\x00" + s.store + "\x00" + runID + "\x00" + id.ID + "\x00" + strconv.FormatUint(id.Generation, 10))
}

// checkPartID checks a caller's run ID and part ID: neither the purpose
// nor the ID holds a NUL, which separates them from what follows.
func checkPartID(runID string, id PartID) error {
	if err := check(runID, id.Generation); err != nil {
		return err
	}
	for what, v := range map[string]string{"purpose": string(id.Purpose), "ID": id.ID} {
		if v == "" || !utf8.ValidString(v) || strings.Contains(v, "\x00") {
			return fmt.Errorf("seal: the part %s %q is empty, isn't UTF-8, or holds a NUL", what, v)
		}
	}
	return nil
}

// checkVersion checks the first byte of a sealed record or part, which is
// read before it is authenticated (Open says why).
func checkVersion(runID string, sealed []byte) error {
	switch {
	case len(sealed) == 0 || sealed[0] == 0:
		return unreadable(runID)
	case sealed[0] != version:
		return fmt.Errorf("%w: run %s's secret record is sealed in version %d, and this release seals in version %d",
			plan.ErrVersion, runID, sealed[0], version)
	}
	return nil
}

// clearAll clears each of bufs.
func clearAll(bufs [][]byte) {
	for _, b := range bufs {
		clear(b)
	}
}
