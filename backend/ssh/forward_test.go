package ssh

import (
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/parallelworks/foundation/problem"

	"github.com/parallelworks/meridian/backend/hostproto"
	"github.com/parallelworks/meridian/internal/sshtest"
	"github.com/parallelworks/meridian/problems"
)

// TestForward checks a member's listener: its endpoint is a socket in the
// sockets directory hello says, named by the keyed hash of the member, the
// same each time; Forward has sshd listen there, once it has removed a
// socket an earlier holder left, and a worker that connects there reaches
// the listener; the listener ends with its connection; and a file there
// that isn't a socket is left, and the forward refused.
func TestForward(t *testing.T) {
	r := newRig(t)
	r.probe()
	e, err := r.d.Endpoint(t.Context(), r.m)
	if err != nil {
		t.Fatal(err)
	}
	name, err := r.d.Name(r.m)
	if err != nil {
		t.Fatal(err)
	}
	if key, err := r.d.MemberKey(r.m); err != nil || key != name {
		t.Errorf("the member's key is %q, %v, want its name %s", key, err, name)
	}
	if filepath.Base(e.Address) != strings.TrimPrefix(name, "ssh-")+".sock" || len(filepath.Base(e.Address)) > hostproto.MaxSocketName ||
		filepath.Base(filepath.Dir(e.Address)) != "meridian" {
		t.Errorf("the endpoint is %s", e.Address)
	}
	if again, _, err := r.d.Connect(t.Context(), r.m, e); err != nil || again != e {
		t.Errorf("Connect gives %+v, %v, want %+v", again, err, e)
	}

	// A socket an earlier holder left there.
	stale, err := net.ListenUnix("unix", &net.UnixAddr{Name: e.Address, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	stale.SetUnlinkOnClose(false)
	_ = stale.Close()
	l, err := r.d.Forward(t.Context(), r.m)
	if err != nil {
		t.Fatal(err)
	}
	if info, err := os.Lstat(e.Address); err != nil || info.Mode().Perm() != 0o600 || info.Mode()&os.ModeSocket == 0 {
		t.Errorf("the socket is %v, %v", info, err)
	}
	go func() {
		c, err := net.Dial("unix", e.Address) //nolint:noctx // a worker's connection, which the test ends
		if err != nil {
			return
		}
		defer c.Close()
		_, _ = c.Write([]byte("hello"))
	}()
	c, err := l.Accept()
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(c)
	if err != nil || string(got) != "hello" {
		t.Errorf("the listener read %q, %v", got, err)
	}
	_ = c.Close()

	// It ends with its connection.
	accepted := make(chan error, 1)
	go func() {
		_, err := l.Accept()
		accepted <- err
	}()
	r.server.Drop()
	select {
	case err := <-accepted:
		if err == nil {
			t.Error("the listener accepted a connection after its own ended")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the listener outlived its connection")
	}

	// A file there that isn't a socket isn't removed.
	if err := os.WriteFile(e.Address, []byte("not a socket"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err = r.d.Forward(t.Context(), r.m)
	if p, ok := errors.AsType[*problem.Problem](err); !ok || p.Code != problems.SSHForwardingRefused.Code() || p.Params["path"] != e.Address {
		t.Errorf("forwarding where a file is gives %v", err)
	}
	if data, err := os.ReadFile(e.Address); err != nil || string(data) != "not a socket" {
		t.Errorf("the file there is %q, %v", data, err)
	}
}

// TestForwardRefused checks that a host whose sshd forwards no Unix
// sockets is ssh_forwarding_refused.
func TestForwardRefused(t *testing.T) {
	r := newRig(t, func(o *sshtest.Options) { o.NoForwarding = true })
	r.probe()
	_, err := r.d.Forward(t.Context(), r.m)
	if p, ok := errors.AsType[*problem.Problem](err); !ok || p.Code != problems.SSHForwardingRefused.Code() || p.Params["host"] != r.server.Addr() {
		t.Errorf("a host that forwards no sockets gives %v", err)
	}
}
