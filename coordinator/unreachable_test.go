package coordinator

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"encoding/pem"
	"fmt"
	"os"
	osexec "os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/crypto/ssh"

	"github.com/parallelworks/meridian/backend"
	backendssh "github.com/parallelworks/meridian/backend/ssh"
	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/internal/sshtest"
	"github.com/parallelworks/meridian/plan"
)

// stalledHostJobs are as many jobs as the coordinator works at once
// (sharedWorkers), each on the host farm.
var stalledHostJobs = func() string {
	var b strings.Builder
	b.WriteString("jobs:\n")
	for i := range sharedWorkers {
		fmt.Fprintf(&b, "  j%d:\n    ssh:\n      remoteHost: farm\n    steps:\n      - run: \"true\"\n", i)
	}
	return b.String()
}()

// TestStalledHostHoldsNoWorkers checks that a host that doesn't answer,
// as one whose network drops what it is sent, doesn't hold the workers
// that every run's supervisors share while the jobs placed on it are
// probed: another run, on the local backend, is placed and ends at once
// meanwhile, rather than once the host's probes have each tried it in
// full, which, before, held every worker for as long as they took.
func TestStalledHostHoldsNoWorkers(t *testing.T) {
	parallel(t)
	if _, err := osexec.LookPath("ssh"); err != nil {
		t.Skip("no ssh client to read the SSH config with")
	}
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := ssh.NewSignerFromKey(k)
	if err != nil {
		t.Fatal(err)
	}
	server := sshtest.New(t, sshtest.Options{Authorized: []ssh.PublicKey{signer.PublicKey()}})
	server.Stall()
	t.Cleanup(server.Resume)
	client := t.TempDir()
	block, err := ssh.MarshalPrivateKey(k, "")
	if err != nil {
		t.Fatal(err)
	}
	keyFile, knownHosts, config := filepath.Join(client, "id_ecdsa"), filepath.Join(client, "known_hosts"), filepath.Join(client, "config")
	for path, data := range map[string][]byte{
		keyFile:    pem.EncodeToMemory(block),
		knownHosts: []byte(server.KnownHosts("")),
		config: fmt.Appendf(nil, `Host farm
  HostName %s
  Port %s
  IdentityFile %s
  IdentitiesOnly yes
  IdentityAgent none
  UserKnownHostsFile %s
  GlobalKnownHostsFile /dev/null
  ConnectTimeout 2
`, server.Host(), server.Port(), keyFile, knownHosts),
	} {
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	key := make([]byte, backendssh.MinKeyLen)
	_, _ = rand.Read(key)
	sshDriver := backendssh.New(backendssh.Options{
		ConfigFile: config, Home: client, User: "tester", Getenv: func(string) string { return "" },
		Version: "test", Binaries: t.TempDir(), Store: "teststore", Key: key,
	})
	t.Cleanup(func() { _ = sshDriver.Close() })
	c := open(t, Options{Drivers: map[string]backend.Driver{testBackend: newTestDriver(), "ssh": sshDriver}})
	stalled, _ := create(t, c, stalledHostJobs, nil, nil)
	serve(t, c)
	// The stalled host's jobs are being placed, each probing the host.
	waitFor(t, func() bool { return !storedRun(t, c, stalled).StartedAt.IsZero() })
	time.Sleep(time.Second) //nolint:forbidigo // the supervisors' first looks, which nothing signals
	began := time.Now()
	local, _ := create(t, c, oneJob, nil, nil)
	for deadline := began.Add(time.Minute); storedRun(t, c, local).State != store.RunEnded; {
		if time.Now().After(deadline) {
			t.Fatal("a run on the local backend didn't end while a host that doesn't answer was probed")
		}
		time.Sleep(10 * time.Millisecond) //nolint:forbidigo // what is waited for has no signal
	}
	if took := time.Since(began); took > 5*time.Second {
		t.Errorf("a run on the local backend took %v to end while a host that doesn't answer was probed", took)
	}
	if r := storedRun(t, c, local); r.Outcome != plan.OutcomeSuccess {
		t.Errorf("the run on the local backend ended %s", r.Outcome)
	}
}
