package sshconn

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io/fs"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/parallelworks/meridian/problems"
)

// A hostConfig is what Meridian uses of the user's ssh_config for one host,
// as `ssh -G` resolves it, with Match, Include, its defaults and the
// command line's options applied.
type hostConfig struct {
	// alias is the host as it was named, which problems name it by, and
	// ProxyCommand's %n; hostname and port where it is.
	alias    string
	hostname string
	port     string
	user     string
	// identityFiles are the key files to offer, their paths expanded,
	// identitiesOnly whether to offer only them, and identityAgent the
	// agent's socket, or "" for none.
	identityFiles  []string
	identitiesOnly bool
	identityAgent  string
	// proxyJump are the hosts to connect through, each [user@]host[:port],
	// the first first; proxyCommand a command whose standard input and
	// output carry the connection, its tokens not yet expanded. At most
	// one is set.
	proxyJump    []string
	proxyCommand string
	// knownHosts are the files the host's key is checked against, the
	// user's then the system's, and hostKeyAlias the name it is looked up
	// by there, or "" for its hostname and port.
	knownHosts   []string
	hostKeyAlias string
	// connectTimeout bounds connecting and the handshake, or 0 for the
	// Dialer's; aliveInterval and aliveCountMax are ServerAliveInterval and
	// ServerAliveCountMax, aliveInterval 0 for none set.
	connectTimeout time.Duration
	aliveInterval  time.Duration
	aliveCountMax  int
	// read is whether ssh read the user's config, rather than there being
	// no ssh client to read it with, when the defaults are used.
	read bool
}

// defaultIdentityFiles are the key files OpenSSH offers when the config
// names none, in its order.
var defaultIdentityFiles = []string{"id_rsa", "id_ecdsa", "id_ecdsa_sk", "id_ed25519", "id_ed25519_sk", "id_xmss"}

// resolve returns the config of host, as ssh -G gives it: with user, port
// and jump, ProxyJump's [user@]host[:port], over the config's where they
// aren't "". Without an ssh client, it gives OpenSSH's defaults, with
// read false. A config ssh can't read is problems.SSHConfigUnreadable.
func (d *Dialer) resolve(ctx context.Context, host, user, port, jump string) (*hostConfig, error) {
	args := []string{"-G"}
	if d.ConfigFile != "" {
		args = append(args, "-F", d.ConfigFile)
	}
	if user != "" {
		args = append(args, "-l", user)
	}
	if port != "" {
		args = append(args, "-p", port)
	}
	if jump != "" {
		args = append(args, "-J", jump)
	}
	// What follows -- is the host, whatever it starts with.
	args = append(args, "--", host)
	cmd := exec.CommandContext(ctx, d.sshClient(), args...) //nolint:gosec // ssh -G, with what a target names as its arguments, the host after --, and no shell
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err := cmd.Run()
	var exitErr *exec.ExitError
	switch {
	case err == nil:
		return d.parse(host, stdout.Bytes()), nil
	case errors.Is(err, exec.ErrNotFound) || errors.Is(err, fs.ErrNotExist):
		return d.defaults(host, user, port, jump), nil
	case ctx.Err() != nil:
		return nil, ctx.Err()
	case errors.As(err, &exitErr):
		return nil, problems.SSHConfigUnreadable.Problem(problems.Params{"host": host, "reason": said(stderr.String(), err)})
	}
	return nil, problems.SSHConfigUnreadable.Problem(problems.Params{"host": host, "reason": err.Error()})
}

// sshClient is the ssh client that reads the config.
func (d *Dialer) sshClient() string {
	if d.SSH != "" {
		return d.SSH
	}
	return "ssh"
}

// said is what ssh wrote to its standard error, its lines joined, or
// err's text, if it wrote nothing.
func said(stderr string, err error) string {
	var lines []string
	for line := range strings.Lines(stderr) {
		if line = strings.TrimSpace(line); line != "" && !strings.HasPrefix(line, "Pseudo-terminal will not be allocated") {
			lines = append(lines, line)
		}
	}
	if len(lines) == 0 {
		return err.Error()
	}
	return strings.Join(lines, "; ")
}

// parse reads ssh -G's output, a keyword and its value on each line.
func (d *Dialer) parse(alias string, out []byte) *hostConfig {
	c := &hostConfig{alias: alias, port: "22", aliveCountMax: 3, read: true}
	agentSet := false
	s := bufio.NewScanner(bytes.NewReader(out))
	for s.Scan() {
		key, value, _ := strings.Cut(s.Text(), " ")
		value = strings.TrimSpace(value)
		switch strings.ToLower(key) {
		case "hostname":
			c.hostname = value
		case "port":
			c.port = value
		case "user":
			c.user = value
		case "identityfile":
			c.identityFiles = append(c.identityFiles, value)
		case "identitiesonly":
			c.identitiesOnly = value == "yes"
		case "identityagent":
			agentSet = true
			switch value {
			case "none":
			case "SSH_AUTH_SOCK":
				c.identityAgent = d.getenv("SSH_AUTH_SOCK")
			default:
				c.identityAgent = value
			}
		case "proxyjump":
			if value != "none" {
				c.proxyJump = strings.Split(value, ",")
			}
		case "proxycommand":
			if value != "none" {
				c.proxyCommand = value
			}
		case "userknownhostsfile", "globalknownhostsfile":
			c.knownHosts = append(c.knownHosts, strings.Fields(value)...)
		case "hostkeyalias":
			if value != "none" {
				c.hostKeyAlias = value
			}
		case "connecttimeout":
			if n, err := strconv.Atoi(value); err == nil && n > 0 {
				c.connectTimeout = time.Duration(n) * time.Second
			}
		case "serveraliveinterval":
			if n, err := strconv.Atoi(value); err == nil && n > 0 {
				c.aliveInterval = time.Duration(n) * time.Second
			}
		case "serveralivecountmax":
			if n, err := strconv.Atoi(value); err == nil && n > 0 {
				c.aliveCountMax = n
			}
		}
	}
	if c.hostname == "" {
		c.hostname = alias
	}
	if !agentSet {
		c.identityAgent = d.getenv("SSH_AUTH_SOCK")
	}
	for i, f := range c.identityFiles {
		c.identityFiles[i] = d.expand(f, c)
	}
	for i, f := range c.knownHosts {
		c.knownHosts[i] = d.expand(f, c)
	}
	return c
}

// defaults is the config OpenSSH gives a host when the user's says
// nothing: what an ssh client would read, where there is none to read it.
func (d *Dialer) defaults(host, user, port, jump string) *hostConfig {
	c := &hostConfig{alias: host, hostname: host, port: "22", user: user, aliveCountMax: 3, identityAgent: d.getenv("SSH_AUTH_SOCK")}
	if port != "" {
		c.port = port
	}
	if c.user == "" {
		c.user = d.localUser()
	}
	if jump != "" {
		c.proxyJump = strings.Split(jump, ",")
	}
	home := d.home()
	for _, f := range defaultIdentityFiles {
		c.identityFiles = append(c.identityFiles, filepath.Join(home, ".ssh", f))
	}
	c.knownHosts = []string{
		filepath.Join(home, ".ssh", "known_hosts"), filepath.Join(home, ".ssh", "known_hosts2"),
		"/etc/ssh/ssh_known_hosts", "/etc/ssh/ssh_known_hosts2",
	}
	return c
}

// expand expands what a path in the config can hold, as OpenSSH does: ~
// for the home directory, and the tokens %d, the home directory, %u, the
// local user, %h, the host's name, %r, the remote user, and %%. Others are
// left as they are, and name no file.
func (d *Dialer) expand(path string, c *hostConfig) string {
	if rest, ok := strings.CutPrefix(path, "~/"); ok {
		path = filepath.Join(d.home(), rest)
	} else if path == "~" {
		path = d.home()
	}
	if !strings.Contains(path, "%") {
		return path
	}
	out, _ := expandTokens(path, func(t byte) (string, bool) {
		switch t {
		case 'd':
			return d.home(), true
		case 'u':
			return d.localUser(), true
		case 'h':
			return c.hostname, true
		case 'r':
			return c.user, true
		}
		return "", false
	})
	return out
}

// expandTokens replaces each %x in s with what value gives for x, and %%
// with %. It reports false, leaving one as it is, for a token value
// doesn't know.
func expandTokens(s string, value func(byte) (string, bool)) (string, bool) {
	var b strings.Builder
	ok := true
	for i := 0; i < len(s); i++ {
		if s[i] != '%' || i == len(s)-1 {
			b.WriteByte(s[i])
			continue
		}
		i++
		if s[i] == '%' {
			b.WriteByte('%')
			continue
		}
		v, known := value(s[i])
		if !known {
			ok = false
			b.WriteByte('%')
			b.WriteByte(s[i])
			continue
		}
		b.WriteString(v)
	}
	return b.String(), ok
}

// proxyCommand expands c's ProxyCommand's tokens, as OpenSSH does: %h the
// host's name, %p its port, %r the remote user, %n the host as it was
// named, %u the local user, %d the home directory, and %%. Another is
// problems.SSHProxyCommandToken, which names the token, not the command,
// which can hold a secret.
func (d *Dialer) proxyCommand(c *hostConfig) (string, error) {
	var unknown byte
	cmd, ok := expandTokens(c.proxyCommand, func(t byte) (string, bool) {
		switch t {
		case 'h':
			return c.hostname, true
		case 'p':
			return c.port, true
		case 'r':
			return c.user, true
		case 'n':
			return c.alias, true
		case 'u':
			return d.localUser(), true
		case 'd':
			return d.home(), true
		}
		if unknown == 0 {
			unknown = t
		}
		return "", false
	})
	if !ok {
		return "", problems.SSHProxyCommandToken.Problem(problems.Params{"host": c.alias, "token": "%" + string(unknown)})
	}
	return cmd, nil
}

// hop reads one of ProxyJump's hosts, [user@]host[:port], or
// ssh://[user@]host[:port], and reports false for one that names no host.
func hop(spec string) (host, user, port string, ok bool) {
	spec = strings.TrimPrefix(strings.TrimSpace(spec), "ssh://")
	if at := strings.LastIndexByte(spec, '@'); at >= 0 {
		user, spec = spec[:at], spec[at+1:]
	}
	if h, p, isHostPort := hostPort(spec); isHostPort {
		return h, user, p, true
	}
	spec = strings.TrimSuffix(strings.TrimPrefix(spec, "["), "]")
	return spec, user, "", spec != ""
}
