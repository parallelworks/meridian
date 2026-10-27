package sshconn

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"os"
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
	// output carry the connection, its tokens not yet expanded; and
	// jumpHost, with jumpUser, a target's jump host, over both. At most
	// one is set.
	proxyJump    []string
	proxyCommand string
	jumpHost     string
	jumpUser     string
	// knownHosts are the files the host's key is checked against, the
	// user's then the system's, and hostKeyAlias the name it is looked up
	// by there, or "" for its hostname and port. revokedHostKeys is the
	// file of keys that are revoked, RevokedHostKeys, or "" for none; and
	// knownHostsCommand whether KnownHostsCommand names a command.
	knownHosts        []string
	hostKeyAlias      string
	revokedHostKeys   string
	knownHostsCommand bool
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

// systemConfig is the system's ssh_config, which ssh reads after the
// user's.
const systemConfig = "/etc/ssh/ssh_config"

// resolve returns the config of host, on port where it isn't "", as
// ssh -G gives it, as user where it isn't "", over the config's User, as
// ssh -l gives it, so that a Match on the user, and a %r, whether ssh
// expands it, as in UserKnownHostsFile and RevokedHostKeys, or expand
// does, as in IdentityFile, see the same user, as with ssh. The host and
// the user are ones their callers checked, as Target.check checks a
// target's (validHostname, validUser): an ssh before 10.1 prints a user as
// it is given, so a newline in one would start a line of its own, which
// parse could take for the config's (CVE-2025-61984). It refuses others,
// unchecked, before anything runs. Without an ssh client, OpenSSH's
// defaults apply (defaults). A config ssh can't read is
// problems.SSHConfigUnreadable.
func (d *Dialer) resolve(ctx context.Context, host, port, user string) (*hostConfig, error) {
	if !validHostname(host) || !validUser(user) {
		return nil, errors.New("sshconn: a host or a user that wasn't checked")
	}
	client, err := command(d.sshClient())
	var c *hostConfig
	switch {
	case errors.Is(err, exec.ErrNotFound) || errors.Is(err, fs.ErrNotExist):
		c, err = d.defaults(host, port)
	case err == nil:
		c, err = d.readConfig(ctx, client, host, port, user)
	}
	if err != nil {
		return nil, err
	}
	if user != "" {
		c.user = user
	}
	d.expandPaths(c)
	return c, nil
}

// expandPaths expands the paths of c's files (expand), once its user is
// known.
func (d *Dialer) expandPaths(c *hostConfig) {
	for i, f := range c.identityFiles {
		c.identityFiles[i] = d.expand(f, c)
	}
	for i, f := range c.knownHosts {
		c.knownHosts[i] = d.expand(f, c)
	}
	if c.revokedHostKeys != "" {
		c.revokedHostKeys = d.expand(c.revokedHostKeys, c)
	}
}

// readConfig runs the ssh client at client as ssh -G, for host, on port
// where it isn't "", as user where it isn't "", and reads what it prints.
func (d *Dialer) readConfig(ctx context.Context, client, host, port, user string) (*hostConfig, error) {
	args := []string{"-G"}
	if d.ConfigFile != "" {
		args = append(args, "-F", d.ConfigFile)
	}
	// -l's value is the next argument, whatever it starts with.
	if user != "" {
		args = append(args, "-l", user)
	}
	if port != "" {
		args = append(args, "-p", port)
	}
	// What follows -- is the host, whatever it starts with.
	args = append(args, "--", host)
	cmd := exec.CommandContext(ctx, client, args...) //nolint:gosec // ssh -G, a checked command, with a checked user and host, and no shell
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err := cmd.Run()
	switch {
	case err == nil:
		return d.parse(host, port, stdout.String())
	case ctx.Err() != nil:
		return nil, ctx.Err()
	}
	if _, ok := errors.AsType[*exec.ExitError](err); ok {
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

// parse reads ssh -G's output, a keyword and its value on each line, for
// alias, asked for on port where it isn't "". It takes the output only as
// ssh prints it: each keyword Meridian reads on one line, but
// IdentityFile, which ssh prints for each file; host, which ssh prints
// first, alias, in any case; and port, where one was asked for, it.
// Otherwise it is problems.SSHConfigOutputUnexpected, which names the
// keyword, rather than a config that something other than the user's
// could have written.
func (d *Dialer) parse(alias, port, out string) (*hostConfig, error) {
	c := &hostConfig{alias: alias, port: "22", aliveCountMax: 3, read: true}
	unexpected := func(keyword string) error {
		return problems.SSHConfigOutputUnexpected.Problem(problems.Params{"host": alias, "keyword": keyword})
	}
	agentSet := false
	seen := map[string]bool{}
	for line := range strings.Lines(out) {
		key, value, _ := strings.Cut(strings.TrimRight(line, "\r\n"), " ")
		key, value = strings.ToLower(key), strings.TrimSpace(value)
		read := true
		switch key {
		case "host":
			if !strings.EqualFold(value, alias) {
				return nil, unexpected(key)
			}
		case "hostname":
			c.hostname = value
		case "port":
			if port != "" && !samePort(value, port) {
				return nil, unexpected(key)
			}
			c.port = value
		case "user":
			c.user = value
		case "identityfile":
			c.identityFiles = append(c.identityFiles, value)
			continue
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
		case "revokedhostkeys":
			if value != "none" {
				c.revokedHostKeys = value
			}
		case "knownhostscommand":
			c.knownHostsCommand = value != "none"
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
		default:
			read = false
		}
		if read {
			if seen[key] {
				return nil, unexpected(key)
			}
			seen[key] = true
		}
	}
	if c.hostname == "" {
		c.hostname = alias
	}
	if !agentSet {
		c.identityAgent = d.getenv("SSH_AUTH_SOCK")
	}
	return c, nil
}

// samePort reports whether two ports, as ssh and a target write them, are
// the same number.
func samePort(a, b string) bool {
	x, errX := strconv.Atoi(a)
	y, errY := strconv.Atoi(b)
	return errX == nil && errY == nil && x == y
}

// defaults is the config OpenSSH gives a host when the user's says
// nothing: what an ssh client would read, where there is none to read it,
// with read false. Where there is a config for one to read, ConfigFile, or
// else the user's or the system's, it isn't ignored, as it could revoke a
// host's key: that is problems.SSHClientMissing.
func (d *Dialer) defaults(host, port string) (*hostConfig, error) {
	home := d.home()
	files := []string{filepath.Join(home, ".ssh", "config"), systemConfig}
	if d.ConfigFile != "" {
		files = []string{d.ConfigFile}
	}
	for _, f := range files {
		if _, err := os.Stat(f); !errors.Is(err, fs.ErrNotExist) {
			return nil, problems.SSHClientMissing.Problem(problems.Params{"host": host, "path": f})
		}
	}
	c := &hostConfig{alias: host, hostname: host, port: "22", user: d.localUser(), aliveCountMax: 3, identityAgent: d.getenv("SSH_AUTH_SOCK")}
	if port != "" {
		c.port = port
	}
	for _, f := range defaultIdentityFiles {
		c.identityFiles = append(c.identityFiles, filepath.Join(home, ".ssh", f))
	}
	c.knownHosts = []string{
		filepath.Join(home, ".ssh", "known_hosts"), filepath.Join(home, ".ssh", "known_hosts2"),
		"/etc/ssh/ssh_known_hosts", "/etc/ssh/ssh_known_hosts2",
	}
	return c, nil
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
