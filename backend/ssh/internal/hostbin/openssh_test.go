package hostbin

import (
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/crypto/ssh"

	"github.com/parallelworks/meridian/backend/ssh/internal/sshconn"
	"github.com/parallelworks/meridian/internal/opensshtest"
)

// TestOpenSSHInstall checks Install against OpenSSH's sshd, where
// opensshtest.EnvSSHD names one, as CI's ssh job does, and the system's sh,
// which runs its scripts: the binary is copied into the cache its
// MERIDIAN_CACHE_DIR names, checked against its digest, and found there
// the next time, without being copied again.
func TestOpenSSHInstall(t *testing.T) {
	c := newClient(t)
	cache := t.TempDir()
	s := opensshtest.New(t, opensshtest.Options{Authorized: []ssh.PublicKey{c.key.PublicKey()}, Env: []string{"MERIDIAN_CACHE_DIR=" + cache}})
	u, err := user.Current()
	if err != nil {
		t.Fatal(err)
	}
	conn := c.dial(s.KnownHosts(""), sshconn.Target{Host: s.Addr(), User: u.Username})
	b := binary(t, "a meridian binary\n", "")
	got, host, err := Install(t.Context(), conn, "farm", b)
	if err != nil {
		t.Fatal(err)
	}
	want := filepath.Join(physical(t, filepath.Join(cache, "bin")), strings.TrimPrefix(digestOf("a meridian binary\n"), "sha256:"), "meridian")
	if got.Path != want || host.Cache != filepath.Dir(filepath.Dir(want)) {
		t.Errorf("Install gave %+v, %+v, want %s", got, host, want)
	}
	if data, err := os.ReadFile(want); err != nil || string(data) != "a meridian binary\n" {
		t.Errorf("the host has %q, %v", data, err)
	}
	info, err := os.Stat(want)
	if err != nil {
		t.Fatal(err)
	}
	again, _, err := Install(t.Context(), conn, "farm", b)
	if err != nil || again != got {
		t.Fatalf("a second Install gave %+v, %v", again, err)
	}
	if now, err := os.Stat(want); err != nil || !os.SameFile(info, now) {
		t.Error("a second Install copied the binary again")
	}
}
