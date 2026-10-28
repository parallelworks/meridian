package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"encoding/pem"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"golang.org/x/crypto/ssh"

	backendssh "github.com/parallelworks/meridian/backend/ssh"
	"github.com/parallelworks/meridian/cmd/meridian/internal/local"
	"github.com/parallelworks/meridian/internal/sshtest"
)

// An sshHost is a host meridian runs jobs on through the ssh backend: the
// test's own machine, through an SSH server in the test's process, whose
// user's home is a directory of the test's. meridian reaches it as the
// cluster hpc of its clusters.yaml, whose host is farm, an alias that the
// SSH config the test gives the ssh backend resolves to the server; and
// runs the test binary there, as meridian, which it puts there as its own.
type sshHost struct {
	server *sshtest.Server
	// home is the host's user's home.
	home string
	// setUp has meridian read the test's SSH config.
	setUp func(*cli)
}

// newSSHHost starts a host for meridian of the state and config
// directories env names, as newSSHHost says, and writes its clusters.yaml.
func newSSHHost(t *testing.T, env map[string]string, with ...func(*sshtest.Options)) *sshHost {
	t.Helper()
	if _, err := exec.LookPath("ssh"); err != nil {
		t.Skip("no ssh client to read the SSH config with")
	}
	h := &sshHost{home: t.TempDir()}
	// The user's runtime directory on the host, short enough for its
	// sockets.
	runtimeDir, err := os.MkdirTemp("", "msh")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(runtimeDir) })
	client := t.TempDir()
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	block, err := ssh.MarshalPrivateKey(k, "")
	if err != nil {
		t.Fatal(err)
	}
	keyFile := filepath.Join(client, "id_ecdsa")
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(block), 0o600); err != nil {
		t.Fatal(err)
	}
	signer, err := ssh.NewSignerFromKey(k)
	if err != nil {
		t.Fatal(err)
	}
	opts := sshtest.Options{Authorized: []ssh.PublicKey{signer.PublicKey()}, Env: []string{
		"HOME=" + h.home, "XDG_RUNTIME_DIR=" + runtimeDir, envMain + "=1",
		"XDG_CACHE_HOME=", "XDG_STATE_HOME=", "XDG_CONFIG_HOME=",
		local.EnvCacheDir + "=", local.EnvStateDir + "=", local.EnvConfigDir + "=", "TMPDIR=",
	}}
	for _, f := range with {
		f(&opts)
	}
	h.server = sshtest.New(t, opts)
	knownHosts := filepath.Join(client, "known_hosts")
	if err := os.WriteFile(knownHosts, []byte(h.server.KnownHosts("")), 0o600); err != nil {
		t.Fatal(err)
	}
	config := filepath.Join(client, "config")
	if err := os.WriteFile(config, fmt.Appendf(nil, `Host farm
  HostName %s
  Port %s
  IdentityFile %s
  IdentitiesOnly yes
  IdentityAgent none
  UserKnownHostsFile %s
  GlobalKnownHostsFile /dev/null
`, h.server.Host(), h.server.Port(), keyFile, knownHosts), 0o600); err != nil {
		t.Fatal(err)
	}
	binaries := hostBinaries(t)
	h.setUp = func(c *cli) {
		c.ssh = func(o *backendssh.Options) { o.ConfigFile, o.Binaries = config, binaries }
	}
	dir := env[local.EnvConfigDir]
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, local.ClustersFile), []byte("hpc:\n  host: farm\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return h
}

// hostBinaries returns a directory of binaries for systems other than the
// test binary's, which holds the test binary for the test machine's, as
// uname says it, where the test binary was built for another architecture,
// as 386 on amd64, which runs it: so that meridian puts the test binary
// on the host either way, which runs as meridian there.
func hostBinaries(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	out, err := exec.Command("uname", "-m").Output() //nolint:noctx // uname, once
	if err != nil {
		t.Skipf("uname -m: %v", err)
	}
	arch := map[string]string{"x86_64": "amd64", "amd64": "amd64", "aarch64": "arm64", "arm64": "arm64"}[strings.TrimSpace(string(out))]
	if arch == "" || arch == runtime.GOARCH {
		return dir
	}
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(self)
	if err != nil {
		t.Fatal(err)
	}
	version, _ := buildVersion()
	fetched := filepath.Join(dir, version, runtime.GOOS+"-"+arch)
	if err := os.MkdirAll(fetched, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(fetched, "meridian"), data, 0o700); err != nil { //nolint:gosec // the test binary, which runs as meridian
		t.Fatal(err)
	}
	return dir
}

// sshWorkflow is a workflow of one job, main, on the cluster its input
// names, whose step says where it runs, then waits until a file in the
// host's home, released, is there, then says it is done.
const sshWorkflow = `on:
  execute:
    inputs:
      cluster:
        type: compute-clusters
jobs:
  main:
    ssh:
      remoteHost: ${{ inputs.cluster.ip }}
    steps:
      - run: |
          echo "home $HOME"
          echo "dir $PWD"
          echo "job dir $PW_JOB_DIR"
          while [ ! -e "$HOME/released" ]; do sleep 0.05; done
          echo done
`

// endedWithin returns what the command did once it has ended, or fails the test
// after a while, as when a worker on a host never reaches the coordinator.
func endedWithin(t *testing.T, cmd *commandRun) result {
	t.Helper()
	select {
	case r := <-cmd.done:
		return r
	case <-time.After(3 * time.Minute):
		t.Fatalf("the command hasn't ended:\n%s%s", cmd.stdout.String(), cmd.stderr.String())
		return result{}
	}
}

// release lets the step of sshWorkflow on h go on.
func (h *sshHost) release(t *testing.T) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(h.home, "released"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
}

// TestSSHRun checks a job whose ssh: names a cluster of clusters.yaml, run
// by meridian run on the host the cluster's host leads to: meridian puts
// itself there, places the job's worker there through meridian host, and
// serves the worker on a socket it forwards there over SSH, through which
// the worker claims its attempt, runs its step there, in the run's
// directory there, with the host's environment, and sends its log, and
// completes; and meridian forgets the worker's placement there.
func TestSSHRun(t *testing.T) {
	env := runEnv(t)
	h := newSSHHost(t, env)
	h.release(t)
	r := endedWithin(t, startCommand(t, env, h.setUp, "run", "--input", "cluster=hpc", writeFile(t, "w.yaml", sshWorkflow)))
	if r.code != exitOK || !strings.HasSuffix(r.stdout, "run 1 finished: success\n") {
		t.Fatalf("meridian run of a job on a host = %+v", r)
	}
	runs := filepath.Join(h.home, ".local", "state", "meridian", "runs")
	log := stepLog(t, env, "1")
	for _, want := range []string{"home " + h.home + "\n", "dir " + runs + "/", "job dir " + runs + "/", "done\n"} {
		if !strings.Contains(log, want) {
			t.Errorf("the step's log has no %q:\n%s", want, log)
		}
	}
	// The worker's placement on the host is forgotten once its end is
	// recorded.
	waitUntil(t, "the placement to be forgotten", func() bool {
		left, _ := filepath.Glob(filepath.Join(h.home, ".local", "state", "meridian", "hosts", "*", "placements", "*"))
		return len(left) == 0
	})
}

// TestSSHOutage checks a job on a host whose connection drops as its step
// runs: meridian connects again, and forwards the socket again, at the same
// path, through which the worker, which went on, sends what it held; and
// the run ends with its step's log whole.
func TestSSHOutage(t *testing.T) {
	env := runEnv(t)
	h := newSSHHost(t, env)
	running := startCommand(t, env, h.setUp, "run", "--input", "cluster=hpc", writeFile(t, "w.yaml", sshWorkflow))
	waitUntil(t, "the step to start", func() bool { return strings.Contains(running.stdout.String(), "main step 0 started") })
	h.server.Drop()
	h.release(t)
	if r := endedWithin(t, running); r.code != exitOK || !strings.HasSuffix(r.stdout, "run 1 finished: success\n") {
		t.Fatalf("meridian run of a job on a host whose connection dropped = %+v", r)
	}
	if log := stepLog(t, env, "1"); strings.Count(log, "done\n") != 1 || strings.Count(log, "home ") != 1 {
		t.Errorf("the step's log is\n%s", log)
	}
}

// TestSSHTakeover checks a job on a host whose meridian run is left
// running: the worker goes on, cut off, while no process holds its
// socket; meridian resume of the run takes the socket over, as it finds
// the attempt on the host's member, and forwards it again, and the run
// ends with its step's log whole.
func TestSSHTakeover(t *testing.T) {
	env := runEnv(t)
	h := newSSHHost(t, env)
	no := false
	interrupt := make(chan os.Signal, 1)
	running := startCommand(t, env, func(c *cli) {
		h.setUp(c)
		c.interactive, c.signals = &no, interrupt
	}, "run", "--input", "cluster=hpc", writeFile(t, "w.yaml", sshWorkflow))
	waitUntil(t, "the step to start", func() bool { return strings.Contains(running.stdout.String(), "main step 0 started") })
	interrupt <- os.Interrupt
	if r := endedWithin(t, running); r.code != exitError || !strings.Contains(r.stdout, "run 1 left running") {
		t.Fatalf("meridian run interrupted = %+v", r)
	}
	h.release(t)
	r := endedWithin(t, startCommand(t, env, h.setUp, "resume", "1"))
	if r.code != exitOK || !strings.HasSuffix(r.stdout, "run 1 finished: success\n") {
		t.Fatalf("meridian resume of a run on a host = %+v", r)
	}
	if log := stepLog(t, env, "1"); strings.Count(log, "done\n") != 1 || strings.Count(log, "home ") != 1 {
		t.Errorf("the step's log is\n%s", log)
	}
}

// TestSSHForwardingRefused checks a job on a host whose sshd forwards no
// Unix sockets: its workers couldn't reach the coordinator, so none is
// placed, and the job fails, with ssh_forwarding_refused.
func TestSSHForwardingRefused(t *testing.T) {
	env := runEnv(t)
	h := newSSHHost(t, env, func(o *sshtest.Options) { o.NoForwarding = true })
	h.release(t)
	r := endedWithin(t, startCommand(t, env, h.setUp, "run", "--input", "cluster=hpc", writeFile(t, "w.yaml", sshWorkflow)))
	if r.code != exitInvalid || !strings.Contains(r.stdout+r.stderr, "(ssh_forwarding_refused)") {
		t.Fatalf("meridian run of a job on a host that forwards no sockets = %+v", r)
	}
	if placed, _ := filepath.Glob(filepath.Join(h.home, ".local", "state", "meridian", "hosts", "*", "placements", "*")); len(placed) != 0 {
		t.Errorf("a worker was placed: %v", placed)
	}
}
