// Package coordtest holds what the tests of the coordinator's packages
// share: a directory only the test's user can use, as the store asks of
// its database's; a store template, migrated once, which their new stores
// are copies of; a credential as a worker makes one; and Keys that wrap
// runs' data keys under a key of their own.
package coordtest

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/hex"
	"os"
	"sync"
	"testing"
)

// PrivateDir returns a directory of the test's that only its user can use,
// as the store asks of its database's: t.TempDir makes it as the umask
// allows.
func PrivateDir(t testing.TB) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil { //nolint:gosec // a directory's owner needs to search it
		t.Fatal(err)
	}
	return dir
}

// Credential returns a credential as a worker makes one: 32 random bytes,
// in hex.
func Credential(t testing.TB) string {
	t.Helper()
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(b)
}

// Keys wraps data keys under a key of its own, with AES-256-GCM, bound to
// the run's ID, as a key file does; and fails as a test says (Fail).
type Keys struct {
	aead cipher.AEAD

	mu                 sync.Mutex
	wrapErr, unwrapErr error
}

// NewKeys returns Keys under a new random key.
func NewKeys(t testing.TB) *Keys {
	t.Helper()
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		t.Fatal(err)
	}
	aead, err := cipher.NewGCMWithRandomNonce(block)
	if err != nil {
		t.Fatal(err)
	}
	return &Keys{aead: aead}
}

// Wrap wraps key, the run runID's data key.
func (k *Keys) Wrap(_ context.Context, runID string, key []byte) ([]byte, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.wrapErr != nil {
		return nil, k.wrapErr
	}
	return k.aead.Seal(nil, nil, key, []byte(runID)), nil //nolint:gosec // NewGCMWithRandomNonce takes no nonce
}

// Unwrap unwraps wrapped, the run runID's data key as Wrap wrapped it.
func (k *Keys) Unwrap(_ context.Context, runID string, wrapped []byte) ([]byte, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.unwrapErr != nil {
		return nil, k.unwrapErr
	}
	return k.aead.Open(nil, nil, wrapped, []byte(runID))
}

// Fail has Wrap return wrap, and Unwrap unwrap, from now on, while they
// aren't nil.
func (k *Keys) Fail(wrap, unwrap error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.wrapErr, k.unwrapErr = wrap, unwrap
}
