package sshconn

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"os/user"
	"strings"
	"sync"
	"time"

	"github.com/parallelworks/foundation/problem"
	"golang.org/x/crypto/ssh"

	"github.com/parallelworks/meridian/problems"
)

// DefaultConnectTimeout bounds connecting to a host, and the handshake,
// where its config's ConnectTimeout doesn't.
const DefaultConnectTimeout = 30 * time.Second

// maxHops is how deep ProxyJumps may nest, the hosts a host's own
// ProxyJump names included, before Meridian takes them for a loop.
const maxHops = 8

// A Dialer connects to hosts as the user: with their ssh_config, their
// known_hosts, their agent and their key files.
type Dialer struct {
	// SSH is the ssh client that resolves the user's config, as ssh -G:
	// ssh, found on PATH, for "". Without one, OpenSSH's defaults apply,
	// and each connection has a problems.SSHConfigNotRead among its notes.
	SSH string
	// ConfigFile is the config ssh reads in place of the user's and the
	// system's, as ssh -F, or "" for those.
	ConfigFile string
	// Home and User are the local user's home directory and name: the
	// process's, os.UserHomeDir's and os/user's, for "".
	Home string
	User string
	// Getenv reads the environment, SSH_AUTH_SOCK: os.Getenv for nil.
	Getenv func(string) string
	// ConnectTimeout bounds connecting, and the handshake, where a host's
	// config's ConnectTimeout doesn't: DefaultConnectTimeout for 0.
	ConnectTimeout time.Duration
	// KeepAlive is how often a connection is asked whether it is alive,
	// and KeepAliveTimeout how long it has to answer before it is closed,
	// where the host's config has no ServerAliveInterval:
	// DefaultKeepAlive and DefaultKeepAliveTimeout for 0.
	KeepAlive        time.Duration
	KeepAliveTimeout time.Duration
	// Sessions is how many sessions a connection opens at once:
	// DefaultSessions for 0.
	Sessions int
}

func (d *Dialer) getenv(name string) string {
	if d.Getenv != nil {
		return d.Getenv(name)
	}
	return os.Getenv(name)
}

func (d *Dialer) home() string {
	if d.Home != "" {
		return d.Home
	}
	home, _ := os.UserHomeDir()
	return home
}

func (d *Dialer) localUser() string {
	if d.User != "" {
		return d.User
	}
	if u, err := user.Current(); err == nil {
		return u.Username
	}
	return ""
}

// Dial connects to t. A problem with the config, the host's key, the
// algorithms the host offers, or the login is a *problem.Problem, of
// problems.SSHConfigUnreadable, SSHHostKeyUnknown, SSHHostKeyChanged,
// SSHHostKeyRevoked, SSHAlgorithmsUnapproved or SSHAuthFailed: trying
// again won't help until the user changes something. Any other error, as
// a host that doesn't answer, may pass.
func (d *Dialer) Dial(ctx context.Context, t Target) (*Conn, error) {
	host, port := t.Host, ""
	h, p, given := hostPort(t.Host)
	if given {
		host, port = h, p
	}
	jump := t.JumpHost
	if jump != "" && t.JumpUser != "" {
		jump = t.JumpUser + "@" + jump
	}
	c, err := d.resolve(ctx, host, t.User, port, jump)
	if err != nil {
		return nil, err
	}
	if given {
		// host:port is where to connect, as format 1 takes it: not the
		// config's HostName, nor its ProxyJump or ProxyCommand, but a jump
		// host the target names.
		c.alias, c.hostname, c.proxyCommand = t.Host, host, ""
		if jump == "" {
			c.proxyJump = nil
		}
	}
	client, closers, err := d.connect(ctx, c, 0)
	if err != nil {
		return nil, err
	}
	var notes []*problem.Problem
	if !c.read {
		notes = append(notes, problems.SSHConfigNotRead.Problem(problems.Params{"host": t.Host}))
	}
	interval, timeout := d.keepAlive(c)
	return newConn(client, closers, notes, interval, timeout, d.sessions()), nil
}

// keepAlive returns how often c's host is asked whether it is alive, and
// how long it has to answer: ServerAliveInterval, and as many of them as
// ServerAliveCountMax, as OpenSSH counts them, or the Dialer's.
func (d *Dialer) keepAlive(c *hostConfig) (interval, timeout time.Duration) {
	if c.aliveInterval > 0 {
		return c.aliveInterval, c.aliveInterval * time.Duration(c.aliveCountMax)
	}
	interval, timeout = d.KeepAlive, d.KeepAliveTimeout
	if interval <= 0 {
		interval = DefaultKeepAlive
	}
	if timeout <= 0 {
		timeout = DefaultKeepAliveTimeout
	}
	return interval, timeout
}

func (d *Dialer) sessions() int {
	if d.Sessions > 0 {
		return d.Sessions
	}
	return DefaultSessions
}

// connect connects to c's host, through its ProxyCommand, or the last
// host of its ProxyJump, which connects through the others, or else
// directly, and returns the client, and what closes what it connected
// through, in the order to close them. depth is how many hosts it is
// connecting through.
func (d *Dialer) connect(ctx context.Context, c *hostConfig, depth int) (_ *ssh.Client, closers []func(), err error) {
	if depth > maxHops {
		return nil, nil, problems.SSHConfigUnreadable.Problem(problems.Params{"host": c.alias, "reason": "ProxyJump " + strings.Join(c.proxyJump, ",")})
	}
	defer func() {
		if err != nil {
			closeAll(closers)
		}
	}()
	addr := net.JoinHostPort(c.hostname, c.port)
	var conn net.Conn
	switch {
	case c.proxyCommand != "":
		cmd, err := d.proxyCommand(c)
		if err != nil {
			return nil, nil, err
		}
		if conn, err = startProxy(cmd); err != nil {
			return nil, nil, fmt.Errorf("sshconn: %s: ProxyCommand: %w", c.alias, err)
		}
	case len(c.proxyJump) > 0:
		last := c.proxyJump[len(c.proxyJump)-1]
		host, user, port, ok := hop(last)
		if !ok {
			return nil, nil, problems.SSHConfigUnreadable.Problem(problems.Params{"host": c.alias, "reason": "ProxyJump " + strings.Join(c.proxyJump, ",")})
		}
		hc, err := d.resolve(ctx, host, user, port, strings.Join(c.proxyJump[:len(c.proxyJump)-1], ","))
		if err != nil {
			return nil, nil, err
		}
		through, throughClosers, err := d.connect(ctx, hc, depth+1)
		if err != nil {
			return nil, nil, err
		}
		closers = append(closers, func() { _ = through.Close() })
		closers = append(closers, throughClosers...)
		if conn, err = through.DialContext(ctx, "tcp", addr); err != nil {
			return nil, closers, fmt.Errorf("sshconn: %s through %s: %w", c.alias, hc.alias, err)
		}
	default:
		dialer := net.Dialer{Timeout: d.connectTimeout(c)}
		if conn, err = dialer.DialContext(ctx, "tcp", addr); err != nil {
			return nil, nil, fmt.Errorf("sshconn: %s: %w", c.alias, err)
		}
	}
	client, err := d.handshake(ctx, c, conn, addr)
	if err != nil {
		_ = conn.Close()
		return nil, closers, err
	}
	return client, closers, nil
}

func (d *Dialer) connectTimeout(c *hostConfig) time.Duration {
	switch {
	case c.connectTimeout > 0:
		return c.connectTimeout
	case d.ConnectTimeout > 0:
		return d.ConnectTimeout
	}
	return DefaultConnectTimeout
}

// handshake runs the SSH handshake over conn, within the connect timeout:
// the key exchange, with the algorithms pinned, the host's key, checked
// against known_hosts, and the login.
func (d *Dialer) handshake(ctx context.Context, c *hostConfig, conn net.Conn, addr string) (*ssh.Client, error) {
	check, err := d.newHostKeyCheck(ctx, c)
	if err != nil {
		return nil, err
	}
	signers, closeAgent := d.signers(ctx, c)
	defer closeAgent()
	config := &ssh.ClientConfig{
		Config:            sshConfig(),
		User:              c.user,
		Auth:              []ssh.AuthMethod{ssh.PublicKeys(signers...)},
		HostKeyCallback:   check.callback,
		HostKeyAlgorithms: check.algorithms(),
	}
	type result struct {
		client *ssh.Client
		err    error
	}
	done := make(chan result, 1)
	go func() {
		cc, chans, reqs, err := ssh.NewClientConn(conn, addr, config)
		if err != nil {
			done <- result{err: err}
			return
		}
		done <- result{client: ssh.NewClient(cc, chans, reqs)}
	}()
	// A connection through a jump host has no deadlines of its own, so
	// closing it is what ends a handshake that takes too long.
	timer := time.NewTimer(d.connectTimeout(c))
	defer timer.Stop()
	var r result
	select {
	case r = <-done:
	case <-timer.C:
		_ = conn.Close()
		<-done
		return nil, fmt.Errorf("sshconn: %s: %w", c.alias, errHandshakeTimeout)
	case <-ctx.Done():
		_ = conn.Close()
		<-done
		return nil, ctx.Err()
	}
	if r.err == nil {
		return r.client, nil
	}
	if p := check.problem(); p != nil {
		return nil, p
	}
	if negotiation, ok := errors.AsType[*ssh.AlgorithmNegotiationError](r.err); ok {
		return nil, problems.SSHAlgorithmsUnapproved.Problem(problems.Params{
			"host": c.alias, "category": category(negotiation.What), "offered": strings.Join(negotiation.RequestedAlgorithms, ", "),
		})
	}
	if strings.Contains(r.err.Error(), "ssh: unable to authenticate") {
		return nil, problems.SSHAuthFailed.Problem(problems.Params{"host": c.alias, "user": c.user, "keys": len(signers)})
	}
	return nil, fmt.Errorf("sshconn: %s: %w", c.alias, r.err)
}

var errHandshakeTimeout = errors.New("the SSH handshake took longer than the connect timeout")

// category is which of a host's algorithms x/crypto/ssh found none of
// Meridian's among, as a message's select reads it: key-exchange,
// host-key, cipher or mac.
func category(what string) string {
	switch {
	case strings.Contains(what, "key exchange"):
		return "key-exchange"
	case strings.Contains(what, "host key"):
		return "host-key"
	case strings.Contains(what, "cipher"):
		return "cipher"
	case strings.Contains(what, "MAC"):
		return "mac"
	}
	return what
}

// closeAll calls each of closers, in order.
func closeAll(closers []func()) {
	for _, c := range closers {
		c()
	}
}

// startProxy starts a ProxyCommand, cmd, as OpenSSH does, through sh -c,
// and returns a connection over its standard input and output. Closing it
// ends the command.
func startProxy(cmd string) (net.Conn, error) {
	c := exec.Command("sh", "-c", cmd) //nolint:gosec,noctx // the user's own ProxyCommand, run as ssh runs it, for as long as the connection lasts
	stdin, err := c.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := c.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := c.Start(); err != nil {
		return nil, err
	}
	return &proxyConn{cmd: c, stdin: stdin, stdout: stdout}, nil
}

// A proxyConn is a connection over a ProxyCommand's standard input and
// output.
type proxyConn struct {
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stdout io.ReadCloser
	once   sync.Once
}

func (c *proxyConn) Read(b []byte) (int, error)  { return c.stdout.Read(b) }
func (c *proxyConn) Write(b []byte) (int, error) { return c.stdin.Write(b) }

// Close closes the command's input, and ends it.
func (c *proxyConn) Close() error {
	c.once.Do(func() {
		_ = c.stdin.Close()
		_ = c.cmd.Process.Kill()
		_ = c.cmd.Wait()
	})
	return nil
}

func (c *proxyConn) LocalAddr() net.Addr              { return proxyAddr{} }
func (c *proxyConn) RemoteAddr() net.Addr             { return proxyAddr{} }
func (c *proxyConn) SetDeadline(time.Time) error      { return nil }
func (c *proxyConn) SetReadDeadline(time.Time) error  { return nil }
func (c *proxyConn) SetWriteDeadline(time.Time) error { return nil }

// proxyAddr is a ProxyCommand's end of a connection.
type proxyAddr struct{}

func (proxyAddr) Network() string { return "proxycommand" }
func (proxyAddr) String() string  { return "proxycommand" }
