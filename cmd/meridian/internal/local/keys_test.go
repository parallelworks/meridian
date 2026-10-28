package local

import (
	"bytes"
	"crypto/rand"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/parallelworks/meridian/internal/kindtest"
	"github.com/parallelworks/meridian/problems"
)

func dataKey(t *testing.T) []byte {
	t.Helper()
	key := make([]byte, keySize)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	return key
}

func TestKeysWrapUnwrap(t *testing.T) {
	dir := t.TempDir()
	k := NewKeys(dir)
	key := dataKey(t)
	wrapped, err := k.Wrap(t.Context(), "run-1", key)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(wrapped, key) {
		t.Error("the wrapped key holds the key")
	}
	again, err := k.Wrap(t.Context(), "run-1", key)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(wrapped, again) {
		t.Error("two wraps of a key are the same: the nonce isn't random")
	}
	// Another Keys, as another process has, reads the same key file.
	got, err := NewKeys(dir).Unwrap(t.Context(), "run-1", wrapped)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, key) {
		t.Error("Unwrap didn't give the key Wrap wrapped")
	}

	info, err := os.Stat(filepath.Join(dir, KeyFile))
	if err != nil {
		t.Fatal(err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Errorf("the key file's mode is %04o, want 0600", perm)
	}
	if entries, _ := os.ReadDir(dir); len(entries) != 1 {
		t.Errorf("the state directory holds %d files, want only the key file", len(entries))
	}
}

func TestKeysUnwrapRefuses(t *testing.T) {
	k := NewKeys(t.TempDir())
	wrapped, err := k.Wrap(t.Context(), "run-1", dataKey(t))
	if err != nil {
		t.Fatal(err)
	}
	_, err = k.Unwrap(t.Context(), "run-2", wrapped)
	if p := kindtest.Want(t, err, problems.KeyUnwrapFailed); p.Params["run"] != "run-2" {
		t.Errorf("the problem's params are %v", p.Params)
	}
	changed := bytes.Clone(wrapped)
	changed[len(changed)-1] ^= 1
	_, err = k.Unwrap(t.Context(), "run-1", changed)
	kindtest.Want(t, err, problems.KeyUnwrapFailed)
	_, err = k.Unwrap(t.Context(), "run-1", nil)
	kindtest.Want(t, err, problems.KeyWrappingUnknown)
	other := append([]byte{wrapVersion + 1}, wrapped[1:]...)
	_, err = k.Unwrap(t.Context(), "run-1", other)
	kindtest.Want(t, err, problems.KeyWrappingUnknown)
	another := NewKeys(t.TempDir())
	if _, err := another.Wrap(t.Context(), "run-0", dataKey(t)); err != nil {
		t.Fatal(err)
	}
	_, err = another.Unwrap(t.Context(), "run-1", wrapped)
	kindtest.Want(t, err, problems.KeyUnwrapFailed)
}

func TestKeysWrapRefusesShortKey(t *testing.T) {
	if _, err := NewKeys(t.TempDir()).Wrap(t.Context(), "run-1", make([]byte, 16)); err == nil {
		t.Error("Wrap took a 16-byte key")
	}
}

// TestKeysUnwrapCreatesNothing checks that Unwrap never makes a key file,
// which couldn't unwrap what was wrapped before it, and that a key file
// that isn't there, or whose directory isn't, is a problem, which trying
// again won't fix: one that has gone since it wrapped a key among them.
func TestKeysUnwrapCreatesNothing(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, KeyFile)
	_, err := NewKeys(dir).Unwrap(t.Context(), "run-1", []byte{wrapVersion, 1, 2, 3})
	if p := kindtest.Want(t, err, problems.KeyFileMissing); p.Params["path"] != path {
		t.Errorf("the problem's params are %v", p.Params)
	}
	if _, err := os.Stat(path); !errors.Is(err, fs.ErrNotExist) {
		t.Error("Unwrap created a key file")
	}

	wrapped, err := NewKeys(dir).Wrap(t.Context(), "run-1", dataKey(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	_, err = NewKeys(dir).Unwrap(t.Context(), "run-1", wrapped)
	kindtest.Want(t, err, problems.KeyFileMissing)

	gone := filepath.Join(dir, "gone")
	_, err = NewKeys(gone).Unwrap(t.Context(), "run-1", wrapped)
	if p := kindtest.Want(t, err, problems.KeyFileMissing); p.Params["path"] != filepath.Join(gone, KeyFile) {
		t.Errorf("the problem's params are %v", p.Params)
	}
}

func TestKeysRefuseOpenKeyFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, KeyFile)
	if err := os.WriteFile(path, make([]byte, keySize), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := NewKeys(dir).Wrap(t.Context(), "run-1", dataKey(t))
	kindtest.Want(t, err, problems.FileNotPrivate)
}

func TestKeysRefuseShortKeyFile(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, KeyFile), []byte("short"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := NewKeys(dir).Wrap(t.Context(), "run-1", dataKey(t))
	if p := kindtest.Want(t, err, problems.KeyFileInvalid); p.Params["size"] != 5 {
		t.Errorf("the problem's params are %v", p.Params)
	}
}

// TestKeysCreateOnce checks that Keys of a state directory without a key
// file, wrapping at once as separate processes would, all use the same key
// file: none replaces another's.
func TestKeysCreateOnce(t *testing.T) {
	dir := t.TempDir()
	key := dataKey(t)
	const n = 8
	wrapped := make([][]byte, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() {
			wrapped[i], errs[i] = NewKeys(dir).Wrap(t.Context(), "run-1", key)
		})
	}
	wg.Wait()
	k := NewKeys(dir)
	for i := range n {
		if errs[i] != nil {
			t.Fatal(errs[i])
		}
		if got, err := k.Unwrap(t.Context(), "run-1", wrapped[i]); err != nil || !bytes.Equal(got, key) {
			t.Errorf("Unwrap of wrap %d: %v", i, err)
		}
	}
}

// TestKeysDerive checks the keys derived from the key file: one for a
// purpose is the same each time, and from another Keys of the directory,
// which creates the key file if it isn't there; and one for another
// purpose, or of another key file, is another.
func TestKeysDerive(t *testing.T) {
	dir := t.TempDir()
	a, err := NewKeys(dir).Derive("ssh member names", 32)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, KeyFile)); err != nil {
		t.Errorf("Derive made no key file: %v", err)
	}
	again, err := NewKeys(dir).Derive("ssh member names", 32)
	if err != nil || !bytes.Equal(a, again) || len(a) != 32 {
		t.Errorf("the key for one purpose is %x, then %x, %v", a, again, err)
	}
	other, err := NewKeys(dir).Derive("another purpose", 32)
	if err != nil || bytes.Equal(a, other) {
		t.Errorf("the keys of two purposes are %x and %x, %v", a, other, err)
	}
	elsewhere, err := NewKeys(t.TempDir()).Derive("ssh member names", 32)
	if err != nil || bytes.Equal(a, elsewhere) {
		t.Errorf("the keys of two key files are %x and %x, %v", a, elsewhere, err)
	}
	if wrapped, err := NewKeys(dir).Wrap(t.Context(), "run-1", dataKey(t)); err != nil || len(wrapped) == 0 {
		t.Errorf("the key file Derive made doesn't wrap: %v", err)
	}
}
