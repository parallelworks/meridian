// Package seal encrypts a run's secret record (plan.Record) for the store,
// as docs/design.md says under The secret record: under a data key of the
// run's own, which the coordinator's Keys wraps, bound to the run and to
// its store, so that a record opens only as the record of the run it was
// sealed for.
//
// A run's data key is 32 bytes from crypto/rand, made when its record is
// first sealed, which Keys wraps for the store's ID and the run's joined
// with a slash, as in 7f3a9c/42. The record, as plan.EncodeRecord encodes
// it, is encrypted under it with AES-256-GCM and a random nonce
// (cipher.NewGCMWithRandomNonce), through the standard library's FIPS
// 140-3 module, with the associated data
//
//	"meridian/record/1" NUL storeID NUL runID
//
// Two runs of one workflow with the same plain inputs have the same plan,
// whose digest the record names, so this binding is all that keeps one
// run's record from being opened as another's. The store's ID is random,
// made once when the store is, so a run's ID that a store reuses, as a
// store whose runs are numbered would after its database is replaced,
// can't open another store's record either. Each time a part is added, the
// record is sealed again whole, under the same data key, with a new nonce
// and the same associated data (Reseal).
//
// # Errors
//
// A caller tells what went wrong from the error alone:
//
//   - One that is a problem (problems.IsProblem) is one trying again can't
//     fix, and fails the run: record_unreadable (problems.RecordUnreadable),
//     for a record that doesn't open as its run's, or what Keys gave, such
//     as a wrapped key it can't unwrap for the run.
//   - One that wraps plan.ErrMalformed is a record that opened but doesn't
//     decode, which fails the run too; one that wraps plan.ErrVersion, a
//     record sealed or encoded by a later release, which leaves the run to
//     one, as plan.DecodeRecord's errors say.
//   - Any other, as one of Keys' that isn't a problem, or the context's
//     end, can pass, and the caller tries again later.
//
// A store or run ID that is empty, a store ID that holds a NUL or a slash,
// and a record to seal of another version than this release's are the
// caller's mistakes.
//
// No error holds a data key, nor any of what a record holds, and so none
// quotes what decoding a record said, which can show a value in it. A data
// key and a record's encoding are cleared once used, and nothing decrypted
// is kept between calls.
package seal

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"fmt"
	"strings"

	"github.com/parallelworks/meridian/coordinator"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
)

// keySize is the size of a run's data key: AES-256's.
const keySize = 32

// version is the first byte of a sealed record, which says how it was
// sealed, so that a later way can be told from this one. The associated
// data names it too.
const version = 1

// associatedPrefix starts a sealed record's associated data, which the
// store's ID and the run's follow.
const associatedPrefix = "meridian/record/1\x00"

// A Sealer seals the secret records of a store's runs.
type Sealer struct {
	keys  coordinator.Keys
	store string
}

// New returns the Sealer of the store whose ID is storeID, which wraps data
// keys with keys. A store makes its ID once, at random; it is never empty,
// and holds neither a NUL nor a slash, which separate it from a run's ID.
func New(keys coordinator.Keys, storeID string) (*Sealer, error) {
	if keys == nil {
		return nil, errors.New("seal: no Keys")
	}
	if storeID == "" || strings.ContainsAny(storeID, "\x00/") {
		return nil, fmt.Errorf("seal: the store ID %q is empty, or holds a NUL or a slash", storeID)
	}
	return &Sealer{keys: keys, store: storeID}, nil
}

// A Sealed is a run's secret record as the store keeps it.
type Sealed struct {
	// Key is the record's data key, as Keys wrapped it for the run.
	Key []byte
	// Record is the record, encrypted under the data key: a byte that says
	// how it was sealed, then the nonce, the ciphertext and GCM's tag.
	Record []byte
}

// Seal seals rec, the secret record of the run runID, under a new data
// key, which Keys wraps for the run.
func (s *Sealer) Seal(ctx context.Context, runID string, rec *plan.Record) (Sealed, error) {
	if err := check(runID, rec); err != nil {
		return Sealed{}, err
	}
	key := make([]byte, keySize)
	defer clear(key)
	if _, err := rand.Read(key); err != nil {
		return Sealed{}, err
	}
	wrapped, err := s.keys.Wrap(ctx, s.keyID(runID), key)
	if err != nil {
		return Sealed{}, fmt.Errorf("seal: wrapping run %s's data key: %w", runID, err)
	}
	sealed, err := s.seal(runID, key, rec)
	if err != nil {
		return Sealed{}, err
	}
	return Sealed{Key: wrapped, Record: sealed}, nil
}

// Reseal seals rec, the run runID's secret record with a part added, in
// the place of sealed.Record: under the data key sealed.Key wraps, which
// it keeps, with a new nonce.
func (s *Sealer) Reseal(ctx context.Context, runID string, sealed Sealed, rec *plan.Record) (Sealed, error) {
	if err := check(runID, rec); err != nil {
		return Sealed{}, err
	}
	key, err := s.unwrap(ctx, runID, sealed.Key)
	if err != nil {
		return Sealed{}, err
	}
	defer clear(key)
	record, err := s.seal(runID, key, rec)
	if err != nil {
		return Sealed{}, err
	}
	return Sealed{Key: sealed.Key, Record: record}, nil
}

// Open opens sealed, the secret record of the run runID, and decodes it.
// Each call decrypts and decodes it anew, and the record it returns is the
// caller's alone.
func (s *Sealer) Open(ctx context.Context, runID string, sealed Sealed) (*plan.Record, error) {
	if runID == "" {
		return nil, errNoRun
	}
	// What the record's first byte says is read before Keys is asked.
	switch {
	case len(sealed.Record) == 0 || sealed.Record[0] == 0:
		return nil, unreadable(runID)
	case sealed.Record[0] != version:
		return nil, fmt.Errorf("%w: run %s's secret record is sealed in version %d, and this release seals in version %d",
			plan.ErrVersion, runID, sealed.Record[0], version)
	}
	key, err := s.unwrap(ctx, runID, sealed.Key)
	if err != nil {
		return nil, err
	}
	defer clear(key)
	aead, err := newAEAD(key)
	if err != nil {
		return nil, err
	}
	data, err := aead.Open(nil, nil, sealed.Record[1:], s.associated(runID)) //nolint:gosec // NewGCMWithRandomNonce takes no nonce
	if err != nil {
		return nil, unreadable(runID)
	}
	defer clear(data)
	rec, err := plan.DecodeRecord(data)
	if err != nil {
		// What DecodeRecord says can quote a value in the record, so only
		// what kind of error it is is kept.
		if errors.Is(err, plan.ErrVersion) {
			return nil, fmt.Errorf("%w: run %s's secret record is of a version this release doesn't read", plan.ErrVersion, runID)
		}
		return nil, fmt.Errorf("%w: run %s's secret record doesn't decode", plan.ErrMalformed, runID)
	}
	return rec, nil
}

// seal encrypts rec, the secret record of the run runID, under key.
func (s *Sealer) seal(runID string, key []byte, rec *plan.Record) ([]byte, error) {
	data, err := plan.EncodeRecord(rec)
	if err != nil {
		// What the encoder says can quote a value in the record.
		return nil, fmt.Errorf("seal: run %s's secret record doesn't encode", runID)
	}
	defer clear(data)
	aead, err := newAEAD(key)
	if err != nil {
		return nil, err
	}
	// The nonce is random, made by Seal, and follows the version.
	return aead.Seal([]byte{version}, nil, data, s.associated(runID)), nil //nolint:gosec // NewGCMWithRandomNonce takes no nonce
}

// unwrap returns the data key of the run runID's secret record, which Keys
// wrapped as wrapped. One that isn't an AES-256 key is no record's.
func (s *Sealer) unwrap(ctx context.Context, runID string, wrapped []byte) ([]byte, error) {
	key, err := s.keys.Unwrap(ctx, s.keyID(runID), wrapped)
	if err != nil {
		return nil, fmt.Errorf("seal: unwrapping run %s's data key: %w", runID, err)
	}
	if len(key) != keySize {
		clear(key)
		return nil, unreadable(runID)
	}
	return key, nil
}

// keyID is the ID Keys wraps the run runID's data key for: the store's ID
// and the run's, joined with a slash, which the store's never holds.
func (s *Sealer) keyID(runID string) string {
	return s.store + "/" + runID
}

// associated is the associated data of the run runID's secret record.
func (s *Sealer) associated(runID string) []byte {
	return []byte(associatedPrefix + s.store + "\x00" + runID)
}

// newAEAD returns AES-256-GCM under key, with a random nonce first in what
// it seals.
func newAEAD(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCMWithRandomNonce(block)
}

// errNoRun is the caller's mistake of giving no run ID.
var errNoRun = errors.New("seal: no run ID")

// check checks a caller's run ID, and the record it gives to be sealed,
// which must be of this release's version, which DecodeRecord reads back.
func check(runID string, rec *plan.Record) error {
	switch {
	case runID == "":
		return errNoRun
	case rec == nil:
		return fmt.Errorf("seal: no secret record for run %s", runID)
	case rec.Version != plan.Version:
		return fmt.Errorf("seal: run %s's secret record is of version %d, not this release's %d", runID, rec.Version, plan.Version)
	}
	return nil
}

// unreadable is the problem of the run runID's secret record, which doesn't
// open as the run's.
func unreadable(runID string) error {
	return problems.RecordUnreadable.Problem(problems.Params{"run": runID})
}
