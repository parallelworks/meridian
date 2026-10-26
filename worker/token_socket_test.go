//go:build linux || darwin || freebsd

package worker

import (
	"io"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"

	"github.com/parallelworks/meridian/problems"
	"github.com/parallelworks/meridian/workerapi"
)

// tokenSocket returns the worker's end of a socket pair, as a descriptor's
// number, and the backend's, which gives the token once the worker says it
// is ready, unless token is "", and records whether it was.
func tokenSocket(t *testing.T, token string) (string, <-chan bool) {
	t.Helper()
	fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	backend := os.NewFile(uintptr(fds[1]), "backend")
	ready := make(chan bool, 1)
	go func() {
		defer backend.Close()
		b := make([]byte, 1)
		_, err := io.ReadFull(backend, b)
		ready <- err == nil && b[0] == '1'
		if err == nil && token != "" {
			_, _ = backend.WriteString(token)
		}
	}()
	return strconv.Itoa(fds[0]), ready
}

// TestTokenSocket checks that a worker takes its bootstrap token from the
// socket the local backend gives it, only once it has said it is ready,
// and refuses a descriptor that isn't a socket, and a token in two places.
func TestTokenSocket(t *testing.T) {
	t.Parallel()
	fd, ready := tokenSocket(t, "the-token\n")
	ep, rest, err := readEnv([]string{"MERIDIAN_COORDINATOR=/c.sock", workerapi.EnvBootstrapTokenFD + "=" + fd, "HOME=/home/u"}, true)
	if err != nil || ep.token != "the-token" || len(rest) != 1 {
		t.Fatalf("got %+v, %v, %v", ep, rest, err)
	}
	if !<-ready {
		t.Error("the worker didn't say it was ready before it read its token")
	}

	// A backend that ends without giving one gives no token.
	fd, _ = tokenSocket(t, "")
	if _, _, err := readEnv([]string{"MERIDIAN_COORDINATOR=/c.sock", workerapi.EnvBootstrapTokenFD + "=" + fd}, true); problemOf(err, problems.WorkerEnvironmentMissing) == nil {
		t.Errorf("no token: %v", err)
	}

	file, err := os.Create(filepath.Join(t.TempDir(), "f"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	// A descriptor of its own, which readEnv closes.
	dup, err := syscall.Dup(int(file.Fd()))
	if err != nil {
		t.Fatal(err)
	}
	for name, environ := range map[string][]string{
		"a file":         {"MERIDIAN_COORDINATOR=/c.sock", workerapi.EnvBootstrapTokenFD + "=" + strconv.Itoa(dup)},
		"no number":      {"MERIDIAN_COORDINATOR=/c.sock", workerapi.EnvBootstrapTokenFD + "=three"},
		"standard input": {"MERIDIAN_COORDINATOR=/c.sock", workerapi.EnvBootstrapTokenFD + "=0"},
		"and a file":     {"MERIDIAN_COORDINATOR=/c.sock", workerapi.EnvBootstrapTokenFD + "=9", workerapi.EnvBootstrapTokenFile + "=/x"},
		"and a token":    {"MERIDIAN_COORDINATOR=/c.sock", workerapi.EnvBootstrapTokenFD + "=9", workerapi.EnvBootstrapToken + "=t"},
	} {
		if _, _, err := readEnv(environ, false); problemOf(err, problems.WorkerEnvironmentInvalid) == nil {
			t.Errorf("%s: %v", name, err)
		}
	}
}
