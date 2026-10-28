package main

import (
	"context"
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

	localbackend "github.com/parallelworks/meridian/backend/local"
	backendssh "github.com/parallelworks/meridian/backend/ssh"
	"github.com/parallelworks/meridian/cmd/meridian/internal/local"
	"github.com/parallelworks/meridian/internal/racetest"
	"github.com/parallelworks/meridian/internal/sshdtest"
	"github.com/parallelworks/meridian/internal/sshtest"
)

// An sshHost is a host meridian runs jobs on through the ssh backend: the
// test's own machine, through an SSH server in the test's process, or
// OpenSSH's sshd where sshdtest.EnvSSHD names one, as CI's ssh job runs
// these tests, whose user's home is a directory of the test's. meridian
// reaches it as the cluster hpc of its clusters.yaml, whose host is farm,
// an alias that the SSH config the test gives the ssh backend resolves to
// the server; and runs the test binary there, as meridian, which it puts
// there as its own.
type sshHost struct {
	server sshServer
	// home is the host's user's home.
	home string
	// setUp has meridian read the test's SSH config.
	setUp func(*cli)
}

// An sshServer is the SSH server of an sshHost: sshtest's, or sshdtest's.
type sshServer interface {
	Host() string
	Port() string
	KnownHosts(name string) string
	// Drop ends every connection the server has.
	Drop()
}

// newSSHHost starts a host for meridian of the state and config
// directories env names, as newSSHHost says, and writes its clusters.yaml.
func newSSHHost(t *testing.T, env map[string]string, with ...func(*sshtest.Options)) *sshHost {
	t.Helper()
	if _, err := exec.LookPath("ssh"); err != nil {
		t.Skip("no ssh client to read the SSH config with")
	}
	h := &sshHost{home: t.TempDir()}
	// A race that meridian host, or a worker it starts, reports there
	// fails the test, once its workers are stopped.
	racetest.Watch(t, h.home)
	t.Cleanup(func() { h.stopWorkers(t) })
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
		racetest.Env(h.home),
	}}
	for _, f := range with {
		f(&opts)
	}
	if sshdtest.Path() != "" {
		// sshd sets HOME and the rest of a session's environment itself,
		// and then what it is told to over them.
		h.server = sshdtest.New(t, sshdtest.Options{
			Authorized: opts.Authorized, Env: opts.Env, NoForwarding: opts.NoForwarding,
			KeyExchanges: opts.KeyExchanges, Ciphers: opts.Ciphers,
		})
	} else {
		h.server = sshtest.New(t, opts)
	}
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

// stopWorkers stops the workers meridian placed on h that are still
// running, as those of a test that failed before its run ended: each
// placement the host's state records, through the local backend that
// meridian host runs there, which stops the worker only if it is still
// the process the placement started. A worker meridian host starts runs
// in a session of its own, so the SSH server's end doesn't stop it.
func (h *sshHost) stopWorkers(t *testing.T) {
	ctx := context.WithoutCancel(t.Context())
	hosts, _ := filepath.Glob(filepath.Join(h.home, ".local", "state", "meridian", "hosts", "*"))
	for _, state := range hosts {
		d := &localbackend.Driver{State: state, KillAfter: time.Second}
		placements, _ := filepath.Glob(filepath.Join(state, "placements", "*.json"))
		for _, file := range placements {
			attempt := strings.TrimSuffix(filepath.Base(file), ".json")
			if p, ok, err := d.Find(ctx, nil, attempt); err == nil && ok {
				_ = d.Cancel(ctx, nil, p)
			}
		}
	}
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
	parallel(t)
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

// sshChild is a workflow a step calls whose job runs on the host its input
// names: each try of its step records itself in a file of its directory,
// fails while it is the first, and writes where it ran, and how many
// tries the directory has seen.
const sshChild = `on:
  execute:
    inputs:
      host: {type: string}
jobs:
  work:
    ssh:
      remoteHost: ${{ inputs.host }}
    steps:
      - run: |
          echo try >> tries
          tries=$(grep -c . tries)
          printf 'CHILD_DIR=%s\nCHILD_JOB_DIR=%s\nCHILD_PARENT=%s\nTRIES=%s\n' "$PWD" "$PW_JOB_DIR" "$PW_PARENT_JOB_DIR" "$tries" > made.env
          [ "$tries" -ge 2 ]
`

// sshCaller is a workflow of one job, main, on the cluster its input
// names, whose step calls sshChild, its child's job on the same host,
// tried again once, and whose next step reads, on the host, what the
// child wrote in its directory, subworkflows/main/step_0 in the root's,
// as format 1's workflows do.
const sshCaller = `on:
  execute:
    inputs:
      cluster:
        type: compute-clusters
jobs:
  main:
    ssh:
      remoteHost: ${{ inputs.cluster.ip }}
    steps:
      - uses: github/acme/flows@main
        with: {$yaml: flows/child.yaml, host: "${{ inputs.cluster.ip }}"}
        retry: {max-retries: 1, interval: 1s}
      - run: |
          dir="${PW_PARENT_JOB_DIR}/subworkflows/main/step_0"
          . "$dir/made.env"
          echo "root $PW_PARENT_JOB_DIR"
          echo "tries $TRIES"
          if [ "$CHILD_DIR" = "$dir" ] && [ "$CHILD_JOB_DIR" = "$dir" ] && [ "$CHILD_PARENT" = "$PW_PARENT_JOB_DIR" ]; then
            echo "the child ran in its directory"
          fi
`

// TestSSHChild checks a step on a host that calls a workflow whose job runs
// on the same host: the child's job runs in the same place in its root's
// run directory there as on the coordinator's machine,
// subworkflows/main/step_0, with the host's PW_JOB_DIR and
// PW_PARENT_JOB_DIR, where the step's next try finds what its last left,
// as no try cleans a host's copy of the directory, and where the caller's
// next step, on the host, reads what the child wrote.
func TestSSHChild(t *testing.T) {
	flowsRepo(t, map[string]string{"flows/child.yaml": sshChild})
	env := runEnv(t)
	h := newSSHHost(t, env)
	r := endedWithin(t, startCommand(t, env, h.setUp, "run", "--input", "cluster=hpc", writeFile(t, "w.yaml", sshCaller)))
	if r.code != exitOK || !strings.HasSuffix(r.stdout, "run 1 finished: success\n") {
		t.Fatalf("meridian run of a call on a host = %+v", r)
	}
	runs := filepath.Join(h.home, ".local", "state", "meridian", "runs")
	log := stepLogAt(t, env, "1", 1)
	for _, want := range []string{"root " + runs + "/1-", "tries 2\n", "the child ran in its directory\n"} {
		if !strings.Contains(log, want) {
			t.Errorf("the caller's next step's log has no %q:\n%s", want, log)
		}
	}
}

// sshLocalCaller is a workflow whose job submit, on the machine the run
// started on, calls sshChild, its child's job on the cluster its input
// names, which none of its own jobs ran on before, tried again once; and
// whose job main, on that host, reads what the child wrote there, by a
// path relative to the run's directory there, as format 1's workflows do.
const sshLocalCaller = `on:
  execute:
    inputs:
      cluster:
        type: compute-clusters
jobs:
  submit:
    steps:
      - uses: github/acme/flows@main
        with: {$yaml: flows/child.yaml, host: "${{ inputs.cluster.ip }}"}
        retry: {max-retries: 1, interval: 1s}
  main:
    needs: [submit]
    ssh:
      remoteHost: ${{ inputs.cluster.ip }}
    steps:
      - run: |
          . ./subworkflows/submit/step_0/made.env
          echo "root $PWD"
          echo "tries $TRIES"
          if [ "$CHILD_DIR" = "$PWD/subworkflows/submit/step_0" ] && [ "$CHILD_JOB_DIR" = "$CHILD_DIR" ] && [ "$CHILD_PARENT" = "$PWD" ] && [ "$PW_PARENT_JOB_DIR" = "$PWD" ]; then
            echo "the child ran in its directory"
          fi
`

// TestSSHChildOfLocalCaller checks a step on the machine the run started
// on that calls a workflow whose job runs on a host: the child's job runs
// in the same place in its root's run directory on the host, which
// meridian host makes, as none of the root's jobs ran there before; its
// next try finds what its last left there, as the caller cleans only its
// own machine's copy of the directory; and a job of the root on the host
// reads what the child wrote by a path relative to its run's directory.
func TestSSHChildOfLocalCaller(t *testing.T) {
	flowsRepo(t, map[string]string{"flows/child.yaml": sshChild})
	env := runEnv(t)
	h := newSSHHost(t, env)
	r := endedWithin(t, startCommand(t, env, h.setUp, "run", "--input", "cluster=hpc", writeFile(t, "w.yaml", sshLocalCaller)))
	if r.code != exitOK || !strings.HasSuffix(r.stdout, "run 1 finished: success\n") {
		t.Fatalf("meridian run of a call of a job on a host = %+v", r)
	}
	runs := filepath.Join(h.home, ".local", "state", "meridian", "runs")
	log := stepLog(t, env, "1")
	for _, want := range []string{"root " + runs + "/1-", "tries 2\n", "the child ran in its directory\n"} {
		if !strings.Contains(log, want) {
			t.Errorf("the host job's log has no %q:\n%s", want, log)
		}
	}
}

// TestSSHOutage checks a job on a host whose connection drops as its step
// runs: meridian connects again, and forwards the socket again, at the same
// path, through which the worker, which went on, sends what it held; and
// the run ends with its step's log whole.
func TestSSHOutage(t *testing.T) {
	parallel(t)
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
	parallel(t)
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
	parallel(t)
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

// TestSSHAlgorithmsUnapproved checks a job on a host whose sshd offers
// only chacha20-poly1305 and curve25519, which FIPS 140-3 doesn't
// approve: meridian can't connect to it, so it places no worker, and the
// job fails, with ssh_algorithms_unapproved. It needs OpenSSH's sshd:
// sshtest's server, in a process under FIPS 140-only mode, can't offer
// them.
func TestSSHAlgorithmsUnapproved(t *testing.T) {
	parallel(t)
	if sshdtest.Path() == "" {
		t.Skipf("%s names no sshd, which can offer what FIPS 140-only mode doesn't run", sshdtest.EnvSSHD)
	}
	env := runEnv(t)
	h := newSSHHost(t, env, func(o *sshtest.Options) {
		o.KeyExchanges = []string{"curve25519-sha256", "curve25519-sha256@libssh.org"}
		o.Ciphers = []string{"chacha20-poly1305@openssh.com"}
	})
	h.release(t)
	r := endedWithin(t, startCommand(t, env, h.setUp, "run", "--input", "cluster=hpc", writeFile(t, "w.yaml", sshWorkflow)))
	if r.code == exitOK || !strings.Contains(r.stdout+r.stderr, "(ssh_algorithms_unapproved)") {
		t.Fatalf("meridian run of a job on a host that offers only unapproved algorithms = %+v", r)
	}
	if placed, _ := filepath.Glob(filepath.Join(h.home, ".local", "state", "meridian", "hosts", "*", "placements", "*")); len(placed) != 0 {
		t.Errorf("a worker was placed: %v", placed)
	}
}
