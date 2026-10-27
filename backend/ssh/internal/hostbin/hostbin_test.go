package hostbin

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/parallelworks/foundation/problem"
	"golang.org/x/crypto/ssh"

	"github.com/parallelworks/meridian/backend/ssh/internal/sshconn"
	"github.com/parallelworks/meridian/backend/ssh/internal/sshtest"
	"github.com/parallelworks/meridian/problems"
)

// A rig is a host, the test's own machine through a test SSH server, whose
// home is a directory of the test's, and a connection to it.
type rig struct {
	t      *testing.T
	home   string
	server *sshtest.Server
	conn   *sshconn.Conn
}

// newRig starts a host whose environment has env over the test's, and
// connects to it, with OpenSSH's defaults, there being no ssh client named.
func newRig(t *testing.T, env ...string) *rig {
	t.Helper()
	r := &rig{t: t, home: t.TempDir()}
	client := t.TempDir()
	if err := os.Mkdir(filepath.Join(client, ".ssh"), 0o700); err != nil {
		t.Fatal(err)
	}
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	block, err := ssh.MarshalPrivateKey(k, "")
	if err != nil {
		t.Fatal(err)
	}
	write(t, filepath.Join(client, ".ssh", "id_ecdsa"), pem.EncodeToMemory(block))
	signer, err := ssh.NewSignerFromKey(k)
	if err != nil {
		t.Fatal(err)
	}
	env = append([]string{"HOME=" + r.home, "XDG_CACHE_HOME=", "MERIDIAN_CACHE_DIR="}, env...)
	r.server = sshtest.New(t, sshtest.Options{Authorized: []ssh.PublicKey{signer.PublicKey()}, Env: env})
	write(t, filepath.Join(client, ".ssh", "known_hosts"), []byte(r.server.KnownHosts("")))
	d := &sshconn.Dialer{SSH: filepath.Join(client, "no-ssh"), Home: client, User: "tester", Getenv: func(string) string { return "" }}
	if r.conn, err = d.Dial(t.Context(), sshconn.Target{Host: r.server.Addr()}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.conn.Close() })
	return r
}

func write(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

// binary writes a file that stands for meridian, and returns Binaries
// whose own binary is it, with digest, or its own digest for "", for the
// system of the test's machine, which is the host: as uname says it, not
// as the test binary was built, which can be for 386 on amd64.
func binary(t *testing.T, content, digest string) *Binaries {
	t.Helper()
	path := filepath.Join(t.TempDir(), "meridian")
	write(t, path, []byte(content))
	if digest == "" {
		digest = digestOf(content)
	}
	goos, goarch := machine(t)
	return &Binaries{Version: "v1.2.3", self: func() (string, string, error) { return path, digest, nil }, goos: goos, goarch: goarch}
}

// machine returns the test machine's system, as Go names it, from uname.
func machine(t *testing.T) (string, string) {
	t.Helper()
	name := func(flag string) string {
		out, err := exec.Command("uname", flag).Output()
		if err != nil {
			t.Skipf("uname %s: %v", flag, err)
		}
		return strings.TrimSpace(string(out))
	}
	goos, goarch, ok := system(name("-s"), name("-m"))
	if !ok {
		t.Skip("the test's machine is a system the worker doesn't run on")
	}
	return goos, goarch
}

func digestOf(content string) string {
	sum := sha256.Sum256([]byte(content))
	return "sha256:" + hex.EncodeToString(sum[:])
}

// paramsOf returns the params of err's problem of kind k, or fails the
// test.
func paramsOf(t *testing.T, err error, k *problems.Kind) problems.Params {
	t.Helper()
	p, ok := errors.AsType[*problem.Problem](err)
	if !ok || p.Code != k.Code() {
		t.Fatalf("got %v, want a problem of %s", err, k.Code())
	}
	return p.Params
}

// uploads counts the commands that copied a binary.
func uploads(commands []string) int {
	n := 0
	for _, c := range commands {
		if strings.Contains(c, `cat > "$f"`) {
			n++
		}
	}
	return n
}

// TestInstall checks that the binary is copied into the host's cache, by
// its digest, in directories only the user can open, once: a second
// Install finds it there, and copies nothing.
func TestInstall(t *testing.T) {
	r := newRig(t)
	b := binary(t, "a meridian binary\n", "")
	got, host, err := Install(t.Context(), r.conn, "farm", b)
	if err != nil {
		t.Fatal(err)
	}
	hexDigest := strings.TrimPrefix(digestOf("a meridian binary\n"), "sha256:")
	want := filepath.Join(r.home, ".cache", "meridian", "bin", hexDigest, "meridian")
	if got.Path != want || got.Digest != digestOf("a meridian binary\n") {
		t.Errorf("Install gave %+v, want %s", got, want)
	}
	if goos, goarch := machine(t); host.GOOS != goos || host.GOARCH != goarch || host.Cache != filepath.Join(r.home, ".cache", "meridian", "bin") {
		t.Errorf("the host is %+v", host)
	}
	data, err := os.ReadFile(want)
	if err != nil || string(data) != "a meridian binary\n" {
		t.Errorf("the host has %q, %v", data, err)
	}
	for path, mode := range map[string]os.FileMode{want: 0o700, filepath.Dir(want): 0o700, filepath.Join(r.home, ".cache"): 0o700} {
		if info, err := os.Stat(path); err != nil || info.Mode().Perm() != mode {
			t.Errorf("%s's mode is %v, %v; want %v", path, info.Mode().Perm(), err, mode)
		}
	}
	if _, _, err := Install(t.Context(), r.conn, "farm", b); err != nil {
		t.Fatal(err)
	}
	if n := uploads(r.server.Commands()); n != 1 {
		t.Errorf("two Installs copied the binary %d times, want once", n)
	}
}

// TestInstallReplaces checks that a file at the binary's place that isn't
// it, as one cut short, is replaced.
func TestInstallReplaces(t *testing.T) {
	r := newRig(t)
	b := binary(t, "a meridian binary\n", "")
	hexDigest := strings.TrimPrefix(digestOf("a meridian binary\n"), "sha256:")
	dir := filepath.Join(r.home, ".cache", "meridian", "bin", hexDigest)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	write(t, filepath.Join(dir, "meridian"), []byte("a meridian bi"))
	got, _, err := Install(t.Context(), r.conn, "farm", b)
	if err != nil {
		t.Fatal(err)
	}
	if data, err := os.ReadFile(got.Path); err != nil || string(data) != "a meridian binary\n" {
		t.Errorf("the host has %q, %v", data, err)
	}
}

// TestInstallUnverified checks that a copy whose digest on the host isn't
// the binary's is removed, and a problem that says the digest it has; and
// that a host with no tool to compute one keeps none.
func TestInstallUnverified(t *testing.T) {
	r := newRig(t)
	b := binary(t, "a meridian binary\n", digestOf("another binary\n"))
	_, _, err := Install(t.Context(), r.conn, "farm", b)
	p := paramsOf(t, err, problems.SSHWorkerBinaryUnverified)
	if p["found"] != digestOf("a meridian binary\n") || p["digest"] != digestOf("another binary\n") || p["host"] != "farm" {
		t.Errorf("the problem's params are %v", p)
	}
	left, _ := filepath.Glob(filepath.Join(r.home, ".cache", "meridian", "bin", "*", "*"))
	left2, _ := filepath.Glob(filepath.Join(r.home, ".cache", "meridian", "bin", "*", ".meridian.*"))
	if len(left)+len(left2) != 0 {
		t.Errorf("a copy that isn't the binary was kept: %v %v", left, left2)
	}

	// A PATH of the tools Install needs but none that computes a digest.
	bin := t.TempDir()
	for _, tool := range []string{"uname", "mkdir", "cat", "chmod", "mv", "rm"} {
		path, err := exec.LookPath(tool)
		if err != nil {
			t.Skipf("no %s", tool)
		}
		if err := os.Symlink(path, filepath.Join(bin, tool)); err != nil {
			t.Fatal(err)
		}
	}
	bare := newRig(t, "PATH="+bin)
	_, _, err = Install(t.Context(), bare.conn, "farm", binary(t, "a meridian binary\n", ""))
	if p := paramsOf(t, err, problems.SSHWorkerBinaryUnverified); p["found"] != "none" {
		t.Errorf("the problem's params are %v", p)
	}
}

// fakeUname returns a PATH with a uname, first, that says os and machine.
func fakeUname(t *testing.T, os_, machine string) string {
	t.Helper()
	bin := t.TempDir()
	script := "#!/bin/sh\ncase \"$1\" in -s) echo " + os_ + ";; -m) echo " + machine + ";; *) echo " + os_ + ";; esac\n"
	if err := os.WriteFile(filepath.Join(bin, "uname"), []byte(script), 0o700); err != nil { //nolint:gosec // a test's script, which runs
		t.Fatal(err)
	}
	return "PATH=" + bin + ":" + os.Getenv("PATH")
}

// TestHostSystems checks a host's system: one the worker doesn't run on
// is refused; macOS keeps its cache where meridian does there; and a host
// of another system than the coordinator's gets the binary fetched for it,
// or a problem that says where to put one.
func TestHostSystems(t *testing.T) {
	r := newRig(t, fakeUname(t, "OpenBSD", "amd64"))
	_, _, err := Install(t.Context(), r.conn, "farm", binary(t, "a meridian binary\n", ""))
	if p := paramsOf(t, err, problems.SSHHostUnsupported); p["os"] != "OpenBSD" || p["arch"] != "amd64" {
		t.Errorf("the problem's params are %v", p)
	}

	mac := newRig(t, fakeUname(t, "Darwin", "arm64"))
	b := binary(t, "a meridian binary\n", "")
	b.goos, b.goarch = "darwin", "arm64"
	got, _, err := Install(t.Context(), mac.conn, "mac", b)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(got.Path, filepath.Join(mac.home, "Library", "Caches", "Meridian", "bin")+"/") {
		t.Errorf("on macOS the binary is at %s", got.Path)
	}

	// The coordinator runs elsewhere than linux/arm64, the host's system.
	arm := newRig(t, fakeUname(t, "Linux", "aarch64"))
	b = binary(t, "a meridian binary\n", "")
	b.goos, b.goarch, b.Dir = "freebsd", "amd64", t.TempDir()
	_, _, err = Install(t.Context(), arm.conn, "arm", b)
	want := filepath.Join(b.Dir, "v1.2.3", "linux-arm64", "meridian")
	if p := paramsOf(t, err, problems.SSHWorkerBinaryMissing); p["path"] != want || p["os"] != "linux" || p["arch"] != "arm64" || p["version"] != "v1.2.3" {
		t.Errorf("the problem's params are %v", p)
	}
	if err := os.MkdirAll(filepath.Dir(want), 0o700); err != nil {
		t.Fatal(err)
	}
	write(t, want, []byte("an arm64 binary\n"))
	got, _, err = Install(t.Context(), arm.conn, "arm", b)
	if err != nil {
		t.Fatal(err)
	}
	if data, err := os.ReadFile(got.Path); err != nil || string(data) != "an arm64 binary\n" || got.Digest != digestOf("an arm64 binary\n") {
		t.Errorf("the host has %q at %s, %v", data, got.Path, err)
	}
}

// TestCacheVariables checks that the host's cache is where meridian finds
// its own there: MERIDIAN_CACHE_DIR, then an absolute XDG_CACHE_HOME.
func TestCacheVariables(t *testing.T) {
	for _, tc := range []struct {
		env  func(dir string) string
		want func(dir string) string
	}{
		{func(dir string) string { return "MERIDIAN_CACHE_DIR=" + dir }, func(dir string) string { return filepath.Join(dir, "bin") }},
		{func(dir string) string { return "XDG_CACHE_HOME=" + dir }, func(dir string) string { return filepath.Join(dir, "meridian", "bin") }},
	} {
		dir := t.TempDir()
		r := newRig(t, tc.env(dir))
		_, host, err := Install(t.Context(), r.conn, "farm", binary(t, "a meridian binary\n", ""))
		if err != nil {
			t.Fatal(err)
		}
		if host.Cache != tc.want(dir) {
			t.Errorf("with %s the cache is %s, want %s", tc.env(dir), host.Cache, tc.want(dir))
		}
	}
}

// TestSystem checks the systems the worker runs on, by uname's names.
func TestSystem(t *testing.T) {
	for _, tc := range []struct {
		os, machine, goos, goarch string
		ok                        bool
	}{
		{"Linux", "x86_64", "linux", "amd64", true},
		{"Linux", "aarch64", "linux", "arm64", true},
		{"Linux", "i686", "linux", "386", true},
		{"Linux", "armv7l", "linux", "arm", true},
		{"Darwin", "arm64", "darwin", "arm64", true},
		{"FreeBSD", "amd64", "freebsd", "amd64", true},
		{"OpenBSD", "amd64", "", "", false},
		{"Linux", "ppc64le", "", "", false},
	} {
		goos, goarch, ok := system(tc.os, tc.machine)
		if goos != tc.goos || goarch != tc.goarch || ok != tc.ok {
			t.Errorf("system(%q, %q) = %q, %q, %v", tc.os, tc.machine, goos, goarch, ok)
		}
	}
}

// TestMarked checks that what a login script prints is never taken for
// what the script says.
func TestMarked(t *testing.T) {
	out := "Welcome!\nMERIDIAN-HOSTS are many\nMERIDIAN-HOST Linux x86_64 /home/a b/.cache/meridian/bin\n"
	if got, ok := marked(out, "MERIDIAN-HOST"); !ok || got != "Linux x86_64 /home/a b/.cache/meridian/bin" {
		t.Errorf("marked gave %q, %v", got, ok)
	}
	if _, ok := marked("MERIDIAN-INSTALLEDx\n", "MERIDIAN-INSTALLED"); ok {
		t.Error("a line that only starts with the marker was taken for it")
	}
}
