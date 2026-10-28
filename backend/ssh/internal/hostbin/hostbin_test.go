package hostbin

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	jsonv2 "encoding/json/v2"
	"encoding/pem"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/parallelworks/foundation/problem"
	"golang.org/x/crypto/ssh"

	"github.com/parallelworks/meridian/backend/ssh/internal/sshconn"
	"github.com/parallelworks/meridian/internal/sshtest"
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
// connects to it, with OpenSSH's defaults, there being no ssh client named,
// nor a config.
func newRig(t *testing.T, env ...string) *rig {
	t.Helper()
	return newRigWith(t, sshtest.Options{}, env...)
}

// newRigWith is newRig, with a host that takes opts as well.
func newRigWith(t *testing.T, opts sshtest.Options, env ...string) *rig {
	t.Helper()
	r := &rig{t: t, home: t.TempDir()}
	c := newClient(t)
	env = append([]string{"HOME=" + r.home, "XDG_CACHE_HOME=", "MERIDIAN_CACHE_DIR="}, env...)
	opts.Authorized, opts.Env = []ssh.PublicKey{c.key.PublicKey()}, env
	r.server = sshtest.New(t, opts)
	r.conn = c.dial(r.server.KnownHosts(""), sshconn.Target{Host: r.server.Addr()})
	return r
}

// A client is the test's side of a connection: a home with a key of its
// own.
type client struct {
	t   *testing.T
	dir string
	key ssh.Signer
}

// newClient makes a client's home, with a new key.
func newClient(t *testing.T) *client {
	t.Helper()
	c := &client{t: t, dir: t.TempDir()}
	if err := os.Mkdir(filepath.Join(c.dir, ".ssh"), 0o700); err != nil {
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
	write(t, filepath.Join(c.dir, ".ssh", "id_ecdsa"), pem.EncodeToMemory(block))
	if c.key, err = ssh.NewSignerFromKey(k); err != nil {
		t.Fatal(err)
	}
	return c
}

// dial connects to target, whose host's key knownHosts holds, with
// OpenSSH's defaults, there being no ssh client named, nor a config, until
// the test ends.
func (c *client) dial(knownHosts string, target sshconn.Target) *sshconn.Conn {
	c.t.Helper()
	write(c.t, filepath.Join(c.dir, ".ssh", "known_hosts"), []byte(knownHosts))
	d := &sshconn.Dialer{SSH: filepath.Join(c.dir, "no-ssh"), ConfigFile: filepath.Join(c.dir, "no-config"), Home: c.dir, User: "tester", Getenv: func(string) string { return "" }}
	conn, err := d.Dial(c.t.Context(), target)
	if err != nil {
		c.t.Fatal(err)
	}
	c.t.Cleanup(func() { _ = conn.Close() })
	return conn
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
	return &Binaries{Version: "v1.2.3", Self: func() (string, string, error) { return path, digest, nil }, goos: goos, goarch: goarch}
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

// uploads counts the scripts that copied a binary.
func uploads(scripts []string) int {
	n := 0
	for _, c := range scripts {
		if strings.Contains(c, `cat > "$f"`) {
			n++
		}
	}
	return n
}

// physical is path with its symbolic links resolved, as the host says
// where things are; its last element needn't be there.
func physical(t *testing.T, path string) string {
	t.Helper()
	dir, err := filepath.EvalSymlinks(filepath.Dir(path))
	if err != nil {
		t.Fatal(err)
	}
	return filepath.Join(dir, filepath.Base(path))
}

// withHostPath runs the scripts with path as their PATH until the test
// ends.
func withHostPath(t *testing.T, path string) {
	t.Helper()
	old := hostPath
	hostPath = path
	t.Cleanup(func() { hostPath = old })
}

// tools returns a directory of links to the tools a script runs, but
// those named in fakes, which are scripts of the content given.
func tools(t *testing.T, fakes map[string]string, names ...string) string {
	t.Helper()
	bin := t.TempDir()
	for _, tool := range names {
		path, err := exec.LookPath(tool)
		if err != nil {
			t.Skipf("no %s", tool)
		}
		if err := os.Symlink(path, filepath.Join(bin, tool)); err != nil {
			t.Fatal(err)
		}
	}
	for name, script := range fakes {
		if err := os.WriteFile(filepath.Join(bin, name), []byte(script), 0o700); err != nil { //nolint:gosec // a test's script, which runs
			t.Fatal(err)
		}
	}
	return bin
}

// scriptTools are the tools Install's scripts run, but one to compute a
// digest with.
var scriptTools = []string{"uname", "id", "od", "find", "mkdir", "cat", "chmod", "mv", "rm"}

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
	if got.Path != physical(t, want) || got.Digest != digestOf("a meridian binary\n") {
		t.Errorf("Install gave %+v, want %s", got, want)
	}
	if goos, goarch := machine(t); host.GOOS != goos || host.GOARCH != goarch || host.Cache != physical(t, filepath.Join(r.home, ".cache", "meridian", "bin")) {
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
	again, _, err := Install(t.Context(), r.conn, "farm", b)
	if err != nil || again != got {
		t.Fatalf("a second Install gave %+v, %v", again, err)
	}
	if n := uploads(r.server.Scripts()); n != 1 {
		t.Errorf("two Installs copied the binary %d times, want once", n)
	}
}

// readAheadSh writes a sh that reads its script as dash's and BusyBox's
// do, ahead of the command it runs, and returns its path: after login, a
// line a login script prints before sh runs, if it isn't "", and once what
// is written to it promptly has come, it takes what there is of its
// standard input, up to 64 KiB, with one read, and runs all of it as
// script, with the system's sh, whose commands read only what comes after.
func readAheadSh(t *testing.T, login string) string {
	t.Helper()
	dir := t.TempDir()
	sh := filepath.Join(dir, "sh")
	printed := ""
	if login != "" {
		printed = "printf '%s\\n' '" + login + "'\n"
	}
	write(t, sh, []byte("#!/bin/sh\n"+printed+"sleep 0.2\nb="+dir+"/$$\ndd bs=65536 count=1 of=\"$b\" 2>/dev/null\nexec /bin/sh \"$b\"\n"))
	if err := os.Chmod(sh, 0o700); err != nil { //nolint:gosec // a test's sh, which runs
		t.Fatal(err)
	}
	return sh
}

// TestInstallReadAhead checks that the binary is copied whole to a host
// whose sh reads ahead of the command it runs: once the script has said
// that it is ready for it, and not with the script, which such a sh would
// take as more of it.
func TestInstallReadAhead(t *testing.T) {
	r := newRigWith(t, sshtest.Options{Sh: readAheadSh(t, "")})
	got, _, err := Install(t.Context(), r.conn, "farm", binary(t, "a meridian binary\n", ""))
	if err != nil {
		t.Fatal(err)
	}
	if data, err := os.ReadFile(got.Path); err != nil || string(data) != "a meridian binary\n" {
		t.Errorf("the host has %q, %v", data, err)
	}
}

// TestInstallLoginReady checks that a login script that prints what the
// copy's script says once it is ready for the binary isn't taken for it,
// on a host whose sh reads ahead: were the binary copied then, that sh
// would run it as script.
func TestInstallLoginReady(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "ran")
	r := newRigWith(t, sshtest.Options{Sh: readAheadSh(t, `{"said":"ready"}`)})
	content := "touch " + marker + "\n"
	got, _, err := Install(t.Context(), r.conn, "farm", binary(t, content, ""))
	if _, err := os.Stat(marker); err == nil {
		t.Error("the binary ran as script")
	}
	if err != nil {
		t.Fatal(err)
	}
	if data, err := os.ReadFile(got.Path); err != nil || string(data) != content {
		t.Errorf("the host has %q, %v", data, err)
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
// the binary's is removed, and a problem that says the digest it has; that
// a host with no tool to compute one keeps none; and that the tool is the
// system's, not one the user's PATH finds first.
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

	// A sha256sum first on the user's PATH that says what Install wants.
	liar := tools(t, map[string]string{"sha256sum": "#!/bin/sh\ncat > /dev/null\necho " + strings.TrimPrefix(digestOf("another binary\n"), "sha256:") + "  -\n"})
	lied := newRig(t, "PATH="+liar+":"+os.Getenv("PATH"))
	_, _, err = Install(t.Context(), lied.conn, "farm", b)
	if p := paramsOf(t, err, problems.SSHWorkerBinaryUnverified); p["found"] != digestOf("a meridian binary\n") {
		t.Errorf("with a sha256sum on the user's PATH, the problem's params are %v", p)
	}

	withHostPath(t, tools(t, nil, scriptTools...))
	bare := newRig(t)
	_, _, err = Install(t.Context(), bare.conn, "farm", binary(t, "a meridian binary\n", ""))
	if p := paramsOf(t, err, problems.SSHWorkerBinaryUnverified); p["found"] != "none" {
		t.Errorf("the problem's params are %v", p)
	}
}

// TestInstallFails checks that a copy the script stops writing, as one
// whose chmod fails, is removed.
func TestInstallFails(t *testing.T) {
	withHostPath(t, tools(t, map[string]string{"chmod": "#!/bin/sh\nexit 1\n"}, "uname", "id", "od", "find", "mkdir", "cat", "mv", "rm", "sha256sum"))
	r := newRig(t)
	if _, _, err := Install(t.Context(), r.conn, "farm", binary(t, "a meridian binary\n", "")); err == nil {
		t.Fatal("an Install whose chmod failed succeeded")
	}
	left, _ := filepath.Glob(filepath.Join(r.home, ".cache", "meridian", "bin", "*", ".meridian.*"))
	if len(left) != 0 {
		t.Errorf("the copy was left: %v", left)
	}
}

// TestInstallUnsafeCache checks that a cache another user could change is
// refused before a binary in it is run or one is written to it: a cache
// directory others can write; a directory in it others can write; and a
// binary there that is a link to one elsewhere, or that others can write.
func TestInstallUnsafeCache(t *testing.T) {
	b := binary(t, "a meridian binary\n", "")
	hexDigest := strings.TrimPrefix(digestOf("a meridian binary\n"), "sha256:")

	open := t.TempDir()
	if err := os.Chmod(open, 0o777); err != nil { //nolint:gosec // a directory others can write, which the test checks isn't used
		t.Fatal(err)
	}
	r := newRig(t, "MERIDIAN_CACHE_DIR="+open)
	_, _, err := Install(t.Context(), r.conn, "farm", b)
	if p := paramsOf(t, err, problems.SSHHostDirUnusable); p["reason"] != "path_unsafe" || p["at"] != physical(t, open) || p["host"] != "farm" {
		t.Errorf("a cache others can write gives %v", p)
	}
	if _, err := os.Stat(filepath.Join(open, "bin")); err == nil {
		t.Error("Install wrote to a cache others can write")
	}

	r = newRig(t)
	bin := filepath.Join(r.home, ".cache", "meridian", "bin")
	if err := os.MkdirAll(bin, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(bin, 0o770); err != nil { //nolint:gosec // a directory its group can write, which the test checks isn't used
		t.Fatal(err)
	}
	_, _, err = Install(t.Context(), r.conn, "farm", b)
	if p := paramsOf(t, err, problems.SSHHostDirUnusable); p["reason"] != "directory_not_private" || p["at"] != physical(t, bin) {
		t.Errorf("a cache directory its group can write gives %v", p)
	}

	// A link, at the binary's place, to a file elsewhere that has the
	// binary's content.
	if err := os.Chmod(bin, 0o700); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(bin, hexDigest)
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	elsewhere := filepath.Join(t.TempDir(), "meridian")
	write(t, elsewhere, []byte("a meridian binary\n"))
	if err := os.Symlink(elsewhere, filepath.Join(dir, "meridian")); err != nil {
		t.Fatal(err)
	}
	_, _, err = Install(t.Context(), r.conn, "farm", b)
	if p := paramsOf(t, err, problems.SSHHostDirUnusable); p["reason"] != "file_not_regular" || p["at"] != filepath.Join(physical(t, dir), "meridian") {
		t.Errorf("a link at the binary's place gives %v", p)
	}

	if err := os.Remove(filepath.Join(dir, "meridian")); err != nil {
		t.Fatal(err)
	}
	write(t, filepath.Join(dir, "meridian"), []byte("a meridian binary\n"))
	if err := os.Chmod(filepath.Join(dir, "meridian"), 0o722); err != nil { //nolint:gosec // a binary others can write, which the test checks isn't used
		t.Fatal(err)
	}
	_, _, err = Install(t.Context(), r.conn, "farm", b)
	if p := paramsOf(t, err, problems.SSHHostDirUnusable); p["reason"] != "file_changeable" {
		t.Errorf("a binary others can write gives %v", p)
	}
}

// TestInstallOwners checks the owners Install's scripts require: with an
// id that says the user is another, a directory on the way to the cache
// that the test's user owns is path_unsafe, which it would be had another
// user made it; and a directory that mkdir made that others can change,
// as a default ACL can have it, is directory_not_private, found by the
// check after mkdir.
func TestInstallOwners(t *testing.T) {
	b := binary(t, "a meridian binary\n", "")
	other := strconv.Itoa(os.Getuid() + 1)
	withHostPath(t, tools(t, map[string]string{"id": "#!/bin/sh\necho " + other + "\n"}, "uname", "od", "find", "mkdir", "cat", "chmod", "mv", "rm", "sha256sum"))
	r := newRig(t)
	_, _, err := Install(t.Context(), r.conn, "farm", b)
	if p := paramsOf(t, err, problems.SSHHostDirUnusable); p["reason"] != "path_unsafe" {
		t.Errorf("a way through the directories of another user gives %v", p)
	}

	mkdir, err := exec.LookPath("mkdir")
	if err != nil {
		t.Skip("no mkdir")
	}
	withHostPath(t, tools(t, map[string]string{"mkdir": "#!/bin/sh\n" + mkdir + ` "$@" || exit; for a; do :; done; chmod 770 "$a"` + "\n"}, "uname", "id", "od", "find", "cat", "chmod", "mv", "rm", "sha256sum"))
	r = newRig(t)
	_, _, err = Install(t.Context(), r.conn, "farm", b)
	hexDigest := strings.TrimPrefix(digestOf("a meridian binary\n"), "sha256:")
	if p := paramsOf(t, err, problems.SSHHostDirUnusable); p["reason"] != "directory_not_private" || p["at"] != filepath.Join(physical(t, r.home), ".cache", "meridian", "bin", hexDigest) {
		t.Errorf("a directory mkdir made that others can change gives %v", p)
	}
}

// TestInstallLinkedCache checks that a binary found in a cache reached
// through a symbolic link is checked, and said to be, where it is: at the
// physical path whose way was checked, as one copied there is.
func TestInstallLinkedCache(t *testing.T) {
	target := t.TempDir()
	link := filepath.Join(t.TempDir(), "cache")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	r := newRig(t, "MERIDIAN_CACHE_DIR="+link)
	b := binary(t, "a meridian binary\n", "")
	copied, _, err := Install(t.Context(), r.conn, "farm", b)
	if err != nil {
		t.Fatal(err)
	}
	found, _, err := Install(t.Context(), r.conn, "farm", b)
	if err != nil {
		t.Fatal(err)
	}
	if want := filepath.Join(physical(t, target), "bin", strings.TrimPrefix(digestOf("a meridian binary\n"), "sha256:"), "meridian"); copied.Path != want || found.Path != want {
		t.Errorf("through a link, the binary copied is at %s, and found at %s, want %s", copied.Path, found.Path, want)
	}
}

// TestScriptOwners checks the scripts' checks of who owns what, run on the
// test's machine as a user that owns nothing there: ok takes a directory
// of the user's or root's, own only the user's, and held only a binary of
// the user's.
func TestScriptOwners(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "meridian")
	write(t, file, []byte("a meridian binary\n"))
	if err := os.Chmod(file, 0o700); err != nil {
		t.Fatal(err)
	}
	run := func(u, check string) string {
		t.Helper()
		out, err := exec.Command("sh", "-c", prelude()+wayScript+"u="+u+"; "+check+"; exit 0", "sh", dir, file).Output() //nolint:noctx // a script of the test's, which ends
		if err != nil {
			t.Fatal(err)
		}
		return strings.TrimSpace(string(out))
	}
	me, other := strconv.Itoa(os.Getuid()), strconv.Itoa(os.Getuid()+1)
	for _, tc := range []struct{ u, check, want string }{
		{me, `ok "$1" && echo yes`, "yes"},
		{other, `ok "$1" && echo yes`, ""},
		{other, `ok / && echo yes`, "yes"},
		{me, `own "$1" && echo yes`, "yes"},
		{other, `own "$1" && echo yes`, ""},
		{me, `held "$2" "$1"; echo held`, "held"},
	} {
		if got := run(tc.u, tc.check); got != tc.want {
			t.Errorf("as %s, %s says %q, want %q", tc.u, tc.check, got, tc.want)
		}
	}
	var got said
	if err := jsonv2.Unmarshal([]byte(run(other, `held "$2" "$1"; echo held`)), &got); err != nil || got.Said != "unusable" || got.Reason != hex.EncodeToString([]byte("file_not_owned")) {
		t.Errorf("as another user, held says %+v, %v", got, err)
	}
}

// fakeUname returns a directory of the tools Install runs, with a uname
// that says os and machine.
func fakeUname(t *testing.T, os_, machine string) string {
	t.Helper()
	names := append([]string{"sha256sum"}, scriptTools[1:]...)
	script := "#!/bin/sh\ncase \"$1\" in -s) echo " + os_ + ";; -m) echo " + machine + ";; *) echo " + os_ + ";; esac\n"
	return tools(t, map[string]string{"uname": script}, names...)
}

// TestHostSystems checks a host's system: one the worker doesn't run on
// is refused; macOS keeps its cache where meridian does there; and a host
// of another system than the coordinator's gets the binary fetched for it,
// or a problem that says where to put one.
func TestHostSystems(t *testing.T) {
	withHostPath(t, fakeUname(t, "OpenBSD", "amd64"))
	r := newRig(t)
	_, _, err := Install(t.Context(), r.conn, "farm", binary(t, "a meridian binary\n", ""))
	if p := paramsOf(t, err, problems.SSHHostUnsupported); p["os"] != "OpenBSD" || p["arch"] != "amd64" {
		t.Errorf("the problem's params are %v", p)
	}

	withHostPath(t, fakeUname(t, "Darwin", "arm64"))
	mac := newRig(t)
	b := binary(t, "a meridian binary\n", "")
	b.goos, b.goarch = "darwin", "arm64"
	got, _, err := Install(t.Context(), mac.conn, "mac", b)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(got.Path, physical(t, filepath.Join(mac.home, "Library", "Caches", "Meridian", "bin"))+"/") {
		t.Errorf("on macOS the binary is at %s", got.Path)
	}

	// The coordinator runs elsewhere than linux/arm64, the host's system.
	withHostPath(t, fakeUname(t, "Linux", "aarch64"))
	arm := newRig(t)
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

// TestForFetched checks the binary fetched for another system: one that
// is a link, or that others can change, isn't taken, nor one in a
// directory others can change; and with no directory to look in, there is
// none.
func TestForFetched(t *testing.T) {
	b := binary(t, "a meridian binary\n", "")
	b.goos, b.goarch, b.Dir = "freebsd", "amd64", t.TempDir()
	path := filepath.Join(b.Dir, "v1.2.3", "linux-arm64", "meridian")
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	elsewhere := filepath.Join(t.TempDir(), "meridian")
	write(t, elsewhere, []byte("an arm64 binary\n"))
	if err := os.Symlink(elsewhere, path); err != nil {
		t.Fatal(err)
	}
	_, err := b.For("arm", "linux", "arm64")
	if p := paramsOf(t, err, problems.WorkerBinaryUnsafe); p["path"] != path || p["code"] != string(problems.FileNotRegular.Code()) {
		t.Errorf("a link gives %v", p)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	write(t, path, []byte("an arm64 binary\n"))
	if err := os.Chmod(path, 0o722); err != nil { //nolint:gosec // a binary others can write, which the test checks isn't taken
		t.Fatal(err)
	}
	_, err = b.For("arm", "linux", "arm64")
	if p := paramsOf(t, err, problems.WorkerBinaryUnsafe); p["code"] != string(problems.FileChangeable.Code()) {
		t.Errorf("a binary others can write gives %v", p)
	}
	if err := os.Chmod(path, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(filepath.Dir(path), 0o777); err != nil { //nolint:gosec // a directory others can write, which the test checks isn't taken from
		t.Fatal(err)
	}
	_, err = b.For("arm", "linux", "arm64")
	paramsOf(t, err, problems.WorkerBinaryUnsafe)
	if err := os.Chmod(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if got, err := b.For("arm", "linux", "arm64"); err != nil || got.Path != path || got.Digest != digestOf("an arm64 binary\n") {
		t.Errorf("the binary fetched gives %+v, %v", got, err)
	}
	b.Dir = ""
	if _, err := b.For("arm", "linux", "arm64"); err == nil {
		t.Error("with no directory of binaries, another system's was found")
	}
}

// TestCacheVariables checks that the host's cache is where meridian finds
// its own there: MERIDIAN_CACHE_DIR, then an absolute XDG_CACHE_HOME; that
// a relative MERIDIAN_CACHE_DIR, or no HOME, is the problem meridian's own
// would be; and that a HOME that holds a newline is where the binary is,
// whole.
func TestCacheVariables(t *testing.T) {
	for _, tc := range []struct {
		env  func(dir string) string
		want func(dir string) string
	}{
		{func(dir string) string { return "MERIDIAN_CACHE_DIR=" + dir }, func(dir string) string { return filepath.Join(dir, "bin") }},
		{func(dir string) string { return "MERIDIAN_CACHE_DIR=" + dir + "//" }, func(dir string) string { return filepath.Join(dir, "bin") }},
		{func(dir string) string { return "XDG_CACHE_HOME=" + dir }, func(dir string) string { return filepath.Join(dir, "meridian", "bin") }},
	} {
		dir := t.TempDir()
		r := newRig(t, tc.env(dir))
		_, host, err := Install(t.Context(), r.conn, "farm", binary(t, "a meridian binary\n", ""))
		if err != nil {
			t.Fatal(err)
		}
		if want := physical(t, tc.want(dir)); host.Cache != want {
			t.Errorf("with %s the cache is %s, want %s", tc.env(dir), host.Cache, want)
		}
	}

	// Cleaned before anything is there, as meridian's own is: a host
	// whose binary is missing says this cache.
	dir := t.TempDir()
	r := newRig(t, "MERIDIAN_CACHE_DIR="+dir+"/./x/..//")
	if h, err := probe(t.Context(), r.conn, "farm"); err != nil || h.Cache != dir+"/bin" {
		t.Errorf("the cache said is %+v, %v, want %s/bin", h, err, dir)
	}

	r = newRig(t, "MERIDIAN_CACHE_DIR=cache")
	_, _, err := Install(t.Context(), r.conn, "farm", binary(t, "a meridian binary\n", ""))
	if p := paramsOf(t, err, problems.DirectoryVariableRelative); p["variable"] != "MERIDIAN_CACHE_DIR" || p["value"] != "cache" {
		t.Errorf("a relative MERIDIAN_CACHE_DIR gives %v", p)
	}
	r = newRig(t, "HOME=")
	_, _, err = Install(t.Context(), r.conn, "farm", binary(t, "a meridian binary\n", ""))
	paramsOf(t, err, problems.HomeUnknown)

	home := filepath.Join(t.TempDir(), "a\n{\"said\":\"installed\",\"at\":\"2f\"}")
	if err := os.Mkdir(home, 0o700); err != nil {
		t.Fatal(err)
	}
	r = newRig(t, "HOME="+home)
	got, _, err := Install(t.Context(), r.conn, "farm", binary(t, "a meridian binary\n", ""))
	if err != nil {
		t.Fatal(err)
	}
	if data, err := os.ReadFile(got.Path); err != nil || string(data) != "a meridian binary\n" || !strings.HasPrefix(got.Path, physical(t, home)+"/") {
		t.Errorf("with a HOME that holds a newline, the binary is at %q: %q, %v", got.Path, data, err)
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
