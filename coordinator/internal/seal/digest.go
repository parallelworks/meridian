package seal

import (
	"context"
	"crypto/hkdf"
	"crypto/hmac"
	"crypto/sha256"
	"fmt"
	"strings"
	"unicode/utf8"
)

// PurposeActionDigest is the digest of a request for the action a step's
// try uses, as the worker API writes it (workerapi.ActionRequest), whose
// ID is the attempt's, the step's index and the try, joined with slashes
// (Digest).
const PurposeActionDigest Purpose = "meridian/action-digest/1"

// Digest returns the digest of data, which the coordinator keeps of the run
// runID in data's place, to know data again, as purpose, with id its own
// among the run's digests of that purpose, as a request's ID. Its purpose
// is a label of its own, as a part's is (Purpose), and its ID is UTF-8, not
// empty, and holds no NUL.
//
// data can hold a secret, as a step's with: does, and a plain SHA-256 of
// it would let whoever holds the store test a guess at one. So the digest
// is an HMAC-SHA256, under a key that HKDF-SHA256 derives from the run's
// data key (which Keys wrapped as wrapped, and unwraps once), with no salt
// and the info
//
//	purpose NUL storeID NUL runID NUL id
//
// which makes it its purpose's, ID's, run's and store's alone; and once the
// run's data key is deleted, as its record is when it ends, no one can
// check data against it, the coordinator included. The caller compares two
// digests with hmac.Equal.
func (s *Sealer) Digest(ctx context.Context, runID string, wrapped []byte, purpose Purpose, id string, data []byte) ([]byte, error) {
	if err := checkID("run", runID); err != nil {
		return nil, err
	}
	for what, v := range map[string]string{"purpose": string(purpose), "ID": id} {
		if v == "" || !utf8.ValidString(v) || strings.Contains(v, "\x00") {
			return nil, fmt.Errorf("seal: the digest's %s %q is empty, isn't UTF-8, or holds a NUL", what, v)
		}
	}
	key, err := s.unwrap(ctx, runID, wrapped)
	if err != nil {
		return nil, err
	}
	defer clear(key)
	info := string(purpose) + "\x00" + s.store + "\x00" + runID + "\x00" + id
	macKey, err := hkdf.Key(sha256.New, key, nil, info, sha256.Size)
	if err != nil {
		return nil, err
	}
	defer clear(macKey)
	mac := hmac.New(sha256.New, macKey)
	mac.Write(data)
	return mac.Sum(nil), nil
}
