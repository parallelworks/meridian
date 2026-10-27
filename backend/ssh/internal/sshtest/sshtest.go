// Package sshtest is an SSH server for tests, in the test's process, on a
// port of the loopback interface: it runs exec requests as the test's
// user, forwards Unix sockets (streamlocal-forward@openssh.com) and TCP
// connections (direct-tcpip), which jump hosts connect through, and lets a
// test break its connections, as a network or a host does: drop them,
// stall them, refuse sessions, or present another host key.
//
// It negotiates what the test's process may run, which, under Meridian's
// FIPS 140-only mode, is what Meridian's client offers. A host that offers
// algorithms the process can't run is Offering's, which only says what it
// offers.
package sshtest

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"testing"

	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"
)

// Options are what a Server offers and accepts.
type Options struct {
	// HostKeys are the server's host keys: a new ECDSA P-256 key for nil.
	HostKeys []ssh.Signer
	// Authorized are the keys a user may log in with, as any user.
	Authorized []ssh.PublicKey
	// KeyExchanges, Ciphers and MACs are what the server offers, or
	// x/crypto/ssh's defaults, as the process's mode allows them, for nil.
	KeyExchanges []string
	Ciphers      []string
	MACs         []string
	// Env is added to the environment the server runs exec requests with,
	// the test process's, as sshd sets a session's.
	Env []string
}

// A Server is an SSH server on the loopback interface.
type Server struct {
	t        testing.TB
	ln       net.Listener
	opts     Options
	mu       sync.Mutex
	hostKeys []ssh.Signer
	conns    map[*faultConn]bool
	// stall, while it isn't nil, holds every read and write of every
	// connection until it is closed.
	stall chan struct{}
	// refuse is how many more sessions to refuse, or -1 for every one.
	refuse int
	// logins are the users who logged in, in order, each once for its
	// connection, which loggedIn holds by its session's ID.
	logins   []string
	loggedIn map[string]bool
	// commands are the exec requests' commands, in order.
	commands []string
	wg       sync.WaitGroup
}

// New starts a server, which stops when the test ends.
func New(t testing.TB, opts Options) *Server {
	t.Helper()
	keys := opts.HostKeys
	if len(keys) == 0 {
		keys = []ssh.Signer{NewKey(t)}
	}
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &Server{t: t, ln: ln, opts: opts, hostKeys: keys, conns: map[*faultConn]bool{}}
	s.wg.Go(s.accept)
	t.Cleanup(func() {
		_ = ln.Close()
		s.Resume()
		s.Drop()
		s.wg.Wait()
	})
	return s
}

// NewKey returns a new ECDSA P-256 key.
func NewKey(t testing.TB) ssh.Signer {
	t.Helper()
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	s, err := ssh.NewSignerFromKey(k)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// Addr is the server's address, as host:port.
func (s *Server) Addr() string { return s.ln.Addr().String() }

// Host is its address's host.
func (s *Server) Host() string { h, _, _ := net.SplitHostPort(s.Addr()); return h }

// Port is its address's port.
func (s *Server) Port() string { _, p, _ := net.SplitHostPort(s.Addr()); return p }

// KnownHosts returns a known_hosts line for each of the server's host
// keys, under name, or its address for "".
func (s *Server) KnownHosts(name string) string {
	if name == "" {
		name = s.Addr()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var b bytes.Buffer
	for _, k := range s.hostKeys {
		b.WriteString(knownhosts.Line([]string{knownhosts.Normalize(name)}, k.PublicKey()) + "\n")
	}
	return b.String()
}

// SetHostKeys makes the server present keys from its next connection on.
func (s *Server) SetHostKeys(keys ...ssh.Signer) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hostKeys = keys
}

// Logins are the users who have logged in, in order.
func (s *Server) Logins() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.logins...)
}

// Commands are the commands of the exec requests the server has run, in
// order.
func (s *Server) Commands() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.commands...)
}

// Drop closes every connection, as a host that went away does.
func (s *Server) Drop() {
	s.mu.Lock()
	conns := make([]*faultConn, 0, len(s.conns))
	for c := range s.conns {
		conns = append(conns, c)
	}
	s.mu.Unlock()
	for _, c := range conns {
		_ = c.Close()
	}
}

// Stall holds every connection's traffic, without closing it, as a
// network that drops what it carries does, until Resume.
func (s *Server) Stall() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stall == nil {
		s.stall = make(chan struct{})
	}
}

// Resume lets the traffic Stall held go.
func (s *Server) Resume() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stall != nil {
		close(s.stall)
		s.stall = nil
	}
}

// RefuseSessions refuses the next n sessions opened, as sshd refuses those
// past its MaxSessions, or every one for -1.
func (s *Server) RefuseSessions(n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.refuse = n
}

// gate returns what holds a connection's traffic now, or nil.
func (s *Server) gate() chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stall
}

func (s *Server) accept() {
	for {
		c, err := s.ln.Accept()
		if err != nil {
			return
		}
		fc := &faultConn{Conn: c, s: s}
		s.mu.Lock()
		s.conns[fc] = true
		s.mu.Unlock()
		s.wg.Go(func() {
			defer func() {
				s.mu.Lock()
				delete(s.conns, fc)
				s.mu.Unlock()
				_ = c.Close()
			}()
			s.serve(fc)
		})
	}
}

// config is the server's config for a new connection.
func (s *Server) config() *ssh.ServerConfig {
	s.mu.Lock()
	keys := s.hostKeys
	s.mu.Unlock()
	authorized := map[string]bool{}
	for _, k := range s.opts.Authorized {
		authorized[string(k.Marshal())] = true
	}
	c := &ssh.ServerConfig{
		PublicKeyCallback: func(meta ssh.ConnMetadata, key ssh.PublicKey) (*ssh.Permissions, error) {
			if !authorized[string(key.Marshal())] {
				return nil, errors.New("sshtest: key not authorized")
			}
			return nil, nil
		},
		// A login is recorded once the server has checked the key's
		// signature, which the key's callback, asked first whether the
		// key would do, comes before; and before it says the login
		// succeeded, so that a client whose Dial has returned finds it.
		AuthLogCallback: func(meta ssh.ConnMetadata, _ string, err error) {
			if err == nil {
				s.login(meta)
			}
		},
	}
	c.KeyExchanges, c.Ciphers, c.MACs = s.opts.KeyExchanges, s.opts.Ciphers, s.opts.MACs
	for _, k := range keys {
		c.AddHostKey(k)
	}
	return c
}

func (s *Server) serve(c net.Conn) {
	conn, chans, reqs, err := ssh.NewServerConn(c, s.config())
	if err != nil {
		return
	}
	defer conn.Close()
	var wg sync.WaitGroup
	defer wg.Wait()
	forwards := &forwards{}
	defer forwards.closeAll()
	wg.Go(func() {
		for req := range reqs {
			s.global(conn, req, forwards, &wg)
		}
	})
	for nc := range chans {
		switch nc.ChannelType() {
		case "session":
			if s.refused() {
				_ = nc.Reject(ssh.ResourceShortage, "sshtest: too many sessions")
				continue
			}
			ch, reqs, err := nc.Accept()
			if err != nil {
				continue
			}
			wg.Go(func() { s.session(ch, reqs) })
		case "direct-tcpip":
			var p struct {
				Host       string
				Port       uint32
				OriginHost string
				OriginPort uint32
			}
			if ssh.Unmarshal(nc.ExtraData(), &p) != nil {
				_ = nc.Reject(ssh.ConnectionFailed, "sshtest: bad request")
				continue
			}
			to, err := (&net.Dialer{}).DialContext(s.t.Context(), "tcp", net.JoinHostPort(p.Host, strconv.Itoa(int(p.Port))))
			if err != nil {
				_ = nc.Reject(ssh.ConnectionFailed, err.Error())
				continue
			}
			ch, reqs, err := nc.Accept()
			if err != nil {
				_ = to.Close()
				continue
			}
			go ssh.DiscardRequests(reqs)
			wg.Go(func() { pipe(ch, to) })
		default:
			_ = nc.Reject(ssh.UnknownChannelType, "sshtest: unknown channel type")
		}
	}
}

// login records a connection's user, once for the connection.
func (s *Server) login(meta ssh.ConnMetadata) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.loggedIn == nil {
		s.loggedIn = map[string]bool{}
	}
	if id := string(meta.SessionID()); !s.loggedIn[id] {
		s.loggedIn[id] = true
		s.logins = append(s.logins, meta.User())
	}
}

// refused reports whether to refuse the session being opened.
func (s *Server) refused() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case s.refuse < 0:
		return true
	case s.refuse > 0:
		s.refuse--
		return true
	}
	return false
}

// global answers a connection's global request: a keepalive, which it
// refuses, as OpenSSH does, and so answers; and the forwarding of a Unix
// socket, and its end.
func (s *Server) global(conn *ssh.ServerConn, req *ssh.Request, f *forwards, wg *sync.WaitGroup) {
	switch req.Type {
	case "streamlocal-forward@openssh.com":
		var p struct{ Path string }
		if ssh.Unmarshal(req.Payload, &p) != nil {
			_ = req.Reply(false, nil)
			return
		}
		ln, err := (&net.ListenConfig{}).Listen(s.t.Context(), "unix", p.Path)
		if err != nil {
			_ = req.Reply(false, nil)
			return
		}
		_ = os.Chmod(p.Path, 0o600)
		f.add(p.Path, ln)
		_ = req.Reply(true, nil)
		wg.Go(func() {
			for {
				c, err := ln.Accept()
				if err != nil {
					return
				}
				payload := ssh.Marshal(struct{ Path, Reserved string }{p.Path, ""})
				ch, reqs, err := conn.OpenChannel("forwarded-streamlocal@openssh.com", payload)
				if err != nil {
					_ = c.Close()
					continue
				}
				go ssh.DiscardRequests(reqs)
				wg.Go(func() { pipe(ch, c) })
			}
		})
	case "cancel-streamlocal-forward@openssh.com":
		var p struct{ Path string }
		_ = ssh.Unmarshal(req.Payload, &p)
		_ = req.Reply(f.close(p.Path), nil)
	default:
		if req.WantReply {
			_ = req.Reply(false, nil)
		}
	}
}

// forwards are a connection's forwarded Unix sockets, by path.
type forwards struct {
	mu  sync.Mutex
	lns map[string]net.Listener
}

func (f *forwards) add(path string, ln net.Listener) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.lns == nil {
		f.lns = map[string]net.Listener{}
	}
	f.lns[path] = ln
}

func (f *forwards) close(path string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	ln, ok := f.lns[path]
	if ok {
		_ = ln.Close()
		delete(f.lns, path)
	}
	return ok
}

func (f *forwards) closeAll() {
	f.mu.Lock()
	defer f.mu.Unlock()
	for path, ln := range f.lns {
		_ = ln.Close()
		delete(f.lns, path)
	}
}

// session serves a session: an exec request runs its command with sh -c,
// as the test's user, with the server's Env, and its exit status is sent.
// Others are refused.
func (s *Server) session(ch ssh.Channel, reqs <-chan *ssh.Request) {
	defer ch.Close()
	for req := range reqs {
		if req.Type != "exec" {
			if req.WantReply {
				_ = req.Reply(false, nil)
			}
			continue
		}
		var p struct{ Command string }
		if ssh.Unmarshal(req.Payload, &p) != nil {
			_ = req.Reply(false, nil)
			continue
		}
		_ = req.Reply(true, nil)
		s.mu.Lock()
		s.commands = append(s.commands, p.Command)
		s.mu.Unlock()
		cmd := exec.Command("sh", "-c", p.Command) //nolint:gosec,noctx // a test's command, run as the test's user, as sshd runs it
		cmd.Env = append(os.Environ(), s.opts.Env...)
		cmd.Stdin, cmd.Stdout, cmd.Stderr = ch, ch, ch.Stderr()
		status := 0
		if err := cmd.Run(); err != nil {
			status = 255
			if exit, ok := errors.AsType[*exec.ExitError](err); ok {
				status = exit.ExitCode()
			}
		}
		_, _ = ch.SendRequest("exit-status", false, binary.BigEndian.AppendUint32(nil, uint32(status))) //nolint:gosec // an exit status
		return
	}
}

// pipe copies between a channel and a connection until either ends.
func pipe(ch ssh.Channel, c net.Conn) {
	defer ch.Close()
	defer c.Close()
	done := make(chan struct{}, 2)
	go func() { _, _ = io.Copy(ch, c); done <- struct{}{} }()
	go func() { _, _ = io.Copy(c, ch); done <- struct{}{} }()
	<-done
}

// A faultConn is a connection whose traffic the server can hold.
type faultConn struct {
	net.Conn
	s *Server
}

func (c *faultConn) Read(b []byte) (int, error) {
	n, err := c.Conn.Read(b)
	if g := c.s.gate(); g != nil {
		<-g
	}
	return n, err
}

func (c *faultConn) Write(b []byte) (int, error) {
	if g := c.s.gate(); g != nil {
		<-g
	}
	return c.Conn.Write(b)
}
