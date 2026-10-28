package seal_test

import (
	"bytes"
	"crypto/fips140"
	"crypto/hmac"
	"crypto/sha256"
	"errors"
	"testing"

	"github.com/parallelworks/foundation/problem"

	"github.com/parallelworks/meridian/coordinator/internal/seal"
	"github.com/parallelworks/meridian/problems"
)

// A digest is the HMAC-SHA256 that Digest says, under a key that HKDF-SHA256
// derives from the run's data key with the digest's info, in FIPS 140-3
// mode: not a plain SHA-256 of what it is of, which would let whoever holds
// the store test a guess at a secret in it, nor an HMAC under the data key
// itself. The same data, purpose and ID give the same digest again, which
// is what the coordinator checks a repeat with; a key of another run, of
// another store, or of the same run's ID with another data key gives
// another, and so does another purpose, ID or data. Each digest unwraps the
// data key once, and clears it.
func TestDigest(t *testing.T) {
	if !fips140.Enforced() {
		t.Fatal("the tests don't run under GODEBUG=fips140=only")
	}
	keys := newKeys(t, false)
	s := newSealer(t, keys, "store-1")
	sealed := sealRecord(t, s, "42")
	data := []byte(`{"step":3,"try":0,"with":{"apiKey":"` + secret + `"}}`)
	digest := func(s *seal.Sealer, runID string, wrapped []byte, purpose seal.Purpose, id string, data []byte) []byte {
		t.Helper()
		d, err := s.Digest(t.Context(), runID, wrapped, purpose, id, data)
		if err != nil {
			t.Fatal(err)
		}
		return d
	}
	before := keys.unwraps
	d := digest(s, "42", sealed.Key, seal.PurposeActionDigest, "attempt-1/3/0", data)
	if keys.unwraps != before+1 {
		t.Errorf("a digest unwrapped the data key %d times", keys.unwraps-before)
	}
	if plain := sha256.Sum256(data); bytes.Equal(d, plain[:]) {
		t.Error("the digest is a plain SHA-256 of what it is of")
	}
	dataKey := keys.dataKeys()[0]
	info := "meridian/action-digest/1\x00store-1\x0042\x00attempt-1/3/0"
	if bytes.Equal(d, hmacOf(dataKey, data)) {
		t.Error("the digest is an HMAC under the run's data key itself")
	}
	if want := hmacOf(partKey(t, dataKey, info), data); !bytes.Equal(d, want) {
		t.Errorf("the digest is %x, not the HMAC-SHA256 under the key HKDF-SHA256 derives with the info %q, %x", d, info, want)
	}
	if again := digest(s, "42", sealed.Key, seal.PurposeActionDigest, "attempt-1/3/0", bytes.Clone(data)); !hmac.Equal(again, d) {
		t.Errorf("the same data's digest again is %x, not %x", again, d)
	}

	// Another store's run 42, and run 42 of this store sealed again under
	// another data key, as after the store's database is replaced.
	otherStore := newSealer(t, keys, "store-2")
	otherKeys := newKeys(t, false)
	otherKey := newSealer(t, otherKeys, "store-1")
	others := map[string][]byte{
		"another run":      digest(s, "43", sealRecord(t, s, "43").Key, seal.PurposeActionDigest, "attempt-1/3/0", data),
		"another store":    digest(otherStore, "42", sealRecord(t, otherStore, "42").Key, seal.PurposeActionDigest, "attempt-1/3/0", data),
		"another data key": digest(otherKey, "42", sealRecord(t, otherKey, "42").Key, seal.PurposeActionDigest, "attempt-1/3/0", data),
		"another purpose":  digest(s, "42", sealed.Key, "meridian/other-digest/1", "attempt-1/3/0", data),
		"another ID":       digest(s, "42", sealed.Key, seal.PurposeActionDigest, "attempt-1/3/1", data),
		"other data":       digest(s, "42", sealed.Key, seal.PurposeActionDigest, "attempt-1/3/0", append(bytes.Clone(data), ' ')),
	}
	for what, other := range others {
		if hmac.Equal(other, d) {
			t.Errorf("%s gives the same digest", what)
		}
	}
	if !keys.cleared() || !otherKeys.cleared() {
		t.Error("a data key wasn't cleared")
	}
}

// hmacOf is the HMAC-SHA256 of data under key.
func hmacOf(key, data []byte) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write(data)
	return mac.Sum(nil)
}

// A digest's purpose and ID that are empty or hold a NUL, and a run's ID
// that holds a slash, are the caller's mistake, refused without asking Keys;
// a wrapped key that Keys can't unwrap for the run is Keys' problem.
func TestDigestRefused(t *testing.T) {
	keys := newKeys(t, false)
	s := newSealer(t, keys, "store-1")
	sealed := sealRecord(t, s, "42")
	before := keys.unwraps
	for _, tc := range []struct {
		run     string
		purpose seal.Purpose
		id      string
	}{
		{"42", "", "attempt-1/3/0"},
		{"42", "meridian/action-digest/1\x00", "attempt-1/3/0"},
		{"42", seal.PurposeActionDigest, ""},
		{"42", seal.PurposeActionDigest, "attempt-1\x003/0"},
		{"42", seal.PurposeActionDigest, "\xff"},
		{"4/2", seal.PurposeActionDigest, "attempt-1/3/0"},
	} {
		if d, err := s.Digest(t.Context(), tc.run, sealed.Key, tc.purpose, tc.id, nil); err == nil || problems.IsProblem(err) {
			t.Errorf("the digest of run %q, %q, %q is %x, %v", tc.run, tc.purpose, tc.id, d, err)
		}
	}
	if keys.unwraps != before {
		t.Error("a digest refused asked Keys")
	}
	_, err := s.Digest(t.Context(), "43", sealed.Key, seal.PurposeActionDigest, "attempt-1/3/0", nil)
	if p, ok := errors.AsType[*problem.Problem](err); !ok || p.Code != problems.KeyUnwrapFailed.Code() {
		t.Errorf("the digest under another run's wrapped key is %v", err)
	}
}
