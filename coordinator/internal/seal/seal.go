// Package seal encrypts a run's secret record (plan.Record) for the store,
// as docs/design.md says under The secret record: under a data key of the
// run's own, which the coordinator's Keys wraps, bound to the run, to its
// store and to the record's generation, so that a record opens only as the
// latest record of the run it was sealed for.
//
// A run's data key is 32 bytes from crypto/rand, made when its record is
// first sealed, which Keys wraps for the store's ID and the run's joined
// with a slash, as in 7f3a9c/42. The record, as plan.EncodeRecord encodes
// it, is encrypted under it with AES-256-GCM and a random nonce
// (cipher.NewGCMWithRandomNonce), through the standard library's FIPS
// 140-3 module, with the associated data
//
//	"meridian/record/1" NUL storeID NUL runID NUL generation
//
// where the generation is a number written in decimal.
//
// Two runs of one workflow with the same plain inputs have the same plan,
// whose digest the record names, so this binding is all that keeps one
// run's record from being opened as another's. The store's ID is random,
// made once when the store is, so a run's ID that a store reuses, as a
// store whose runs are numbered would after its database is replaced,
// can't open another store's record either.
//
// Each time a part is added, the record is sealed again whole, under the
// same data key, with a new nonce and the next generation (Reseal). Seal's
// generation is 1, and each Reseal's one more than the one it replaces. The
// caller keeps the run's current generation with the run's own state, not
// beside the record, and Open takes it, so a record that a later one
// replaced doesn't open in its place: a record rolled back in the store
// fails as one changed does. A store restored whole, the run's state with
// its record, is one this can't tell from the store as it was.
//
// A run's record is sealed again at most MaxReseals times, which keeps the
// chance that two of the nonces under its data key are the same below
// 2^-48, far inside what GCM's random nonces allow (NIST SP 800-38D).
//
// A run's secrets can also be kept in parts apart from its record, so that
// writing one seals only that one, as each job's end stores what the record
// holds of the job (SealJob, OpenJobs). A part is sealed (SealPart,
// OpenParts) with AES-256-GCM and a random nonce under a key of its own,
// which HKDF-SHA256 derives from the run's data key, with no salt and the
// info
//
//	purpose NUL storeID NUL runID NUL partID NUL generation
//
// which is the part's associated data too: its purpose a versioned label,
// as meridian/job-record/1 (PurposeJobRecord), and its ID its own among
// the run's parts of that purpose, as a job's key. So a part opens only as
// the latest generation of its own purpose, ID, run and store, which the
// caller keeps apart from it, as it keeps the record's; and since each
// derived key seals one generation of one part, the parts take nothing
// from the bound on the nonces under the data key.
//
// # Errors
//
// A caller tells what went wrong from the error alone:
//
//   - One that is a problem (problems.IsProblem) is one trying again can't
//     fix, and fails the run: record_unreadable (problems.RecordUnreadable)
//     for a record that doesn't open as its run's latest,
//     record_reseal_limit (problems.RecordResealLimit) for one sealed again
//     MaxReseals times, or what Keys gave, such as a wrapped key it can't
//     unwrap for the run.
//   - One that wraps plan.ErrMalformed is a record that opened but doesn't
//     decode, which fails the run too; one that wraps plan.ErrVersion, a
//     record sealed or encoded by a later release, which leaves the run to
//     one, as plan.DecodeRecord's errors say.
//   - Any other, as one of Keys' that isn't a problem, or the context's
//     end, can pass, and the caller tries again later.
//
// What is the caller's mistake is a plain error: a record to seal of
// another version than this release's, a generation of 0, and a store or
// run ID that is empty, isn't UTF-8, or holds a NUL or a slash. The store's
// ID holding neither is what makes the associated data and Keys' ID read
// only one way; the run's holding neither keeps them so whatever a store's
// ID is, and one in UTF-8 reaches a Keys that carries it as text, as a key
// management service's encryption context does, as it is.
//
// A record's first byte, which says how it was sealed, is read before the
// record is authenticated, so that a record of a later release is left to
// one without asking Keys. So one byte written to the store can leave a run
// waiting for a release that never comes. That is accepted: it costs only
// the run's availability, which whoever can write the store can end anyway
// by deleting the record, and nothing of the record is read on that path.
//
// No error holds a data key, nor any of what a record holds, and so none
// quotes what decoding a record said, which can show a value in it. Nothing
// decrypted is kept between calls. Clearing is best-effort: a data key and
// a record's encoding are cleared once used, but not what the standard
// library and the runtime keep of them out of reach: AES's key schedule and
// GCM's hash key, inside the cipher; the pooled buffer encoding/json/v2
// encodes the record in; and the record's strings, which Go can't clear,
// in the record a caller seals and in the one Open returns.
package seal

import (
	"cmp"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
)

// MaxReseals is how many times a run's record is sealed again, by Reseal,
// under its data key.
const MaxReseals = 1 << 24

// keySize is the size of a run's data key: AES-256's.
const keySize = 32

// version is the first byte of a sealed record, which says how it was
// sealed, so that a later way can be told from this one. The associated
// data names it too.
const version = 1

// associatedPrefix starts a sealed record's associated data, which the
// store's ID, the run's and the generation follow.
const associatedPrefix = "meridian/record/1\x00"

// Keys wraps runs' data keys: the coordinator's Keys (coordinator.Keys),
// whose methods and rules these are. The package names them itself, since
// the coordinator, which seals its runs' records here, imports it.
type Keys interface {
	// Wrap encrypts key, the data key of the secret record of the run
	// runID, bound to runID.
	Wrap(ctx context.Context, runID string, key []byte) ([]byte, error)
	// Unwrap decrypts a data key that Wrap wrapped for runID.
	Unwrap(ctx context.Context, runID string, wrapped []byte) ([]byte, error)
}

// A Sealer seals the secret records of a store's runs.
type Sealer struct {
	keys  Keys
	store string
}

// New returns the Sealer of the store whose ID is storeID, which wraps data
// keys with keys. A store makes its ID once, at random; it is UTF-8, never
// empty, and holds neither a NUL nor a slash, which separate it from a
// run's ID.
func New(keys Keys, storeID string) (*Sealer, error) {
	if keys == nil {
		return nil, errors.New("seal: no Keys")
	}
	if err := checkID("store", storeID); err != nil {
		return nil, err
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
// key, which Keys wraps for the run. It returns the record's generation,
// 1, which the caller keeps with the run's state.
func (s *Sealer) Seal(ctx context.Context, runID string, rec *plan.Record) (Sealed, uint64, error) {
	if err := cmp.Or(check(runID, 1), checkRecord(runID, rec)); err != nil {
		return Sealed{}, 0, err
	}
	key := make([]byte, keySize)
	defer clear(key)
	if _, err := rand.Read(key); err != nil {
		return Sealed{}, 0, err
	}
	wrapped, err := s.keys.Wrap(ctx, s.keyID(runID), key)
	if err != nil {
		return Sealed{}, 0, fmt.Errorf("seal: wrapping run %s's data key: %w", runID, err)
	}
	sealed, err := s.seal(runID, 1, key, rec)
	if err != nil {
		return Sealed{}, 0, err
	}
	return Sealed{Key: wrapped, Record: sealed}, 1, nil
}

// Reseal seals rec, the run runID's secret record with a part added, in
// the place of sealed.Record, whose generation is gen: under the data key
// sealed.Key wraps, which it keeps, with a new nonce and the next
// generation, which it returns for the caller to keep in gen's place. A
// record of a generation past MaxReseals isn't sealed again.
func (s *Sealer) Reseal(ctx context.Context, runID string, gen uint64, sealed Sealed, rec *plan.Record) (Sealed, uint64, error) {
	if err := cmp.Or(check(runID, gen), checkRecord(runID, rec)); err != nil {
		return Sealed{}, 0, err
	}
	if gen > MaxReseals {
		return Sealed{}, 0, problems.RecordResealLimit.Problem(problems.Params{"run": runID, "limit": MaxReseals})
	}
	key, err := s.unwrap(ctx, runID, sealed.Key)
	if err != nil {
		return Sealed{}, 0, err
	}
	defer clear(key)
	record, err := s.seal(runID, gen+1, key, rec)
	if err != nil {
		return Sealed{}, 0, err
	}
	return Sealed{Key: sealed.Key, Record: record}, gen + 1, nil
}

// Open opens sealed, the secret record of the run runID, whose generation
// is gen, as the caller keeps it, and decodes it. Each call decrypts and
// decodes it anew, and the record it returns is the caller's alone.
func (s *Sealer) Open(ctx context.Context, runID string, gen uint64, sealed Sealed) (*plan.Record, error) {
	if err := check(runID, gen); err != nil {
		return nil, err
	}
	// What the record's first byte says is read before Keys is asked.
	if err := checkVersion(runID, sealed.Record); err != nil {
		return nil, err
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
	data, err := aead.Open(nil, nil, sealed.Record[1:], s.associated(runID, gen)) //nolint:gosec // NewGCMWithRandomNonce takes no nonce
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

// seal encrypts rec, the secret record of the run runID, of the generation
// gen, under key.
func (s *Sealer) seal(runID string, gen uint64, key []byte, rec *plan.Record) ([]byte, error) {
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
	return aead.Seal([]byte{version}, nil, data, s.associated(runID, gen)), nil //nolint:gosec // NewGCMWithRandomNonce takes no nonce
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
// and the run's, joined with a slash, which neither holds.
func (s *Sealer) keyID(runID string) string {
	return s.store + "/" + runID
}

// associated is the associated data of the run runID's secret record of
// the generation gen.
func (s *Sealer) associated(runID string, gen uint64) []byte {
	return []byte(associatedPrefix + s.store + "\x00" + runID + "\x00" + strconv.FormatUint(gen, 10))
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

// check checks a caller's run ID and the generation it gives.
func check(runID string, gen uint64) error {
	if err := checkID("run", runID); err != nil {
		return err
	}
	if gen == 0 {
		return fmt.Errorf("seal: run %s's secret record has no generation 0", runID)
	}
	return nil
}

// checkRecord checks the record a caller gives to be sealed, which must be
// of a version this release reads, its plan's, which DecodeRecord reads
// back.
func checkRecord(runID string, rec *plan.Record) error {
	switch {
	case rec == nil:
		return fmt.Errorf("seal: no secret record for run %s", runID)
	case rec.Version < 1 || rec.Version > plan.Version:
		return fmt.Errorf("seal: run %s's secret record is of version %d, which this release doesn't read", runID, rec.Version)
	}
	return nil
}

// checkID checks a store's or a run's ID, what names: it must be UTF-8,
// not empty, and hold neither a NUL nor a slash.
func checkID(what, id string) error {
	if id == "" || !utf8.ValidString(id) || strings.ContainsAny(id, "\x00/") {
		return fmt.Errorf("seal: the %s ID %q is empty, isn't UTF-8, or holds a NUL or a slash", what, id)
	}
	return nil
}

// unreadable is the problem of the run runID's secret record, which doesn't
// open as the run's latest.
func unreadable(runID string) error {
	return problems.RecordUnreadable.Problem(problems.Params{"run": runID})
}
