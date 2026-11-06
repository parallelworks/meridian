package local

import (
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/parallelworks/meridian/internal/kindtest"
	"github.com/parallelworks/meridian/problems"
)

// TestCoordinatorKey checks that the coordinator's key is made once, as an
// ECDSA key on P-256 in PKCS #8 PEM, in a file only the user can read; and
// that another process of the state directory, as one that takes a
// member's listener over, reads the same key, and so serves the same hash.
func TestCoordinatorKey(t *testing.T) {
	dir := t.TempDir()
	key, err := CoordinatorKey(dir)
	if err != nil {
		t.Fatal(err)
	}
	if key.Curve != elliptic.P256() {
		t.Errorf("the key is on %s", key.Curve.Params().Name)
	}
	path := filepath.Join(dir, CoordinatorKeyFile)
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Errorf("the key file's mode is %04o, want 0600", perm)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	block, _ := pem.Decode(data)
	if block == nil || block.Type != "PRIVATE KEY" {
		t.Fatalf("the key file holds %q", data)
	}
	if _, err := x509.ParsePKCS8PrivateKey(block.Bytes); err != nil {
		t.Errorf("the key file's key isn't PKCS #8: %v", err)
	}
	if entries, _ := os.ReadDir(dir); len(entries) != 1 {
		t.Errorf("the state directory holds %d files, want only the key file", len(entries))
	}

	again, err := CoordinatorKey(dir)
	if err != nil {
		t.Fatal(err)
	}
	if !again.Equal(key) {
		t.Error("another read of the state directory gave another key")
	}
	if other, err := CoordinatorKey(t.TempDir()); err != nil || other.Equal(key) {
		t.Errorf("another state directory's key is the same, %v", err)
	}
}

// TestCoordinatorKeyCreateOnce checks that processes of a state directory
// without a key file, each making one at once, all read the same key: none
// replaces another's.
func TestCoordinatorKeyCreateOnce(t *testing.T) {
	dir := t.TempDir()
	const n = 8
	keys := make([]*ecdsa.PrivateKey, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() { keys[i], errs[i] = CoordinatorKey(dir) })
	}
	wg.Wait()
	for i := range n {
		if errs[i] != nil {
			t.Fatal(errs[i])
		}
		if !keys[i].Equal(keys[0]) {
			t.Errorf("process %d read another key", i)
		}
	}
}

// TestCoordinatorKeyRefuses checks that a key file others can read, one
// that is a symbolic link, one too large, and one that isn't an ECDSA key
// on P-256 in PKCS #8 PEM, are problems, and that none is replaced.
func TestCoordinatorKeyRefuses(t *testing.T) {
	pemOf := func(t *testing.T, key any) string {
		t.Helper()
		der, err := x509.MarshalPKCS8PrivateKey(key)
		if err != nil {
			t.Fatal(err)
		}
		return string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}))
	}
	p384, err := ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	_, ed, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	p256, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	good := pemOf(t, p256)
	for _, tc := range []struct {
		name string
		data string
		mode os.FileMode
		want *problems.Kind
	}{
		{"others can read it", good, 0o644, problems.FileNotPrivate},
		{"too large", strings.Repeat("a", maxCoordinatorKey+1), 0o600, problems.FileOverLimit},
		{"not PEM", "not a key", 0o600, problems.CoordinatorKeyInvalid},
		{"another block", strings.Replace(good, "PRIVATE KEY", "EC PRIVATE KEY", 2), 0o600, problems.CoordinatorKeyInvalid},
		{"more after it", good + "more", 0o600, problems.CoordinatorKeyInvalid},
		{"more before it", "more\n" + good, 0o600, problems.CoordinatorKeyInvalid},
		{"headers", strings.Replace(good, "-----\n", "-----\nProc-Type: 4,ENCRYPTED\n\n", 1), 0o600, problems.CoordinatorKeyInvalid},
		{"P-384", pemOf(t, p384), 0o600, problems.CoordinatorKeyInvalid},
		{"Ed25519", pemOf(t, ed), 0o600, problems.CoordinatorKeyInvalid},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, CoordinatorKeyFile)
			if err := os.WriteFile(path, []byte(tc.data), tc.mode); err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(path, tc.mode); err != nil {
				t.Fatal(err)
			}
			_, err := CoordinatorKey(dir)
			if p := kindtest.Want(t, err, tc.want); p.Params["path"] != path {
				t.Errorf("the problem's params are %v", p.Params)
			}
			if data, err := os.ReadFile(path); err != nil || string(data) != tc.data {
				t.Errorf("the key file was changed: %v", err)
			}
		})
	}

	t.Run("a link", func(t *testing.T) {
		dir := t.TempDir()
		target := filepath.Join(dir, "elsewhere.key")
		if err := os.WriteFile(target, []byte(good), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(target, filepath.Join(dir, CoordinatorKeyFile)); err != nil {
			t.Fatal(err)
		}
		_, err := CoordinatorKey(dir)
		kindtest.Want(t, err, problems.FileNotRegular)
	})
}
