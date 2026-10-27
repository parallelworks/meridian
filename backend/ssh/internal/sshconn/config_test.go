package sshconn

import (
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/parallelworks/foundation/problem"

	"github.com/parallelworks/meridian/problems"
)

// testDialer is a Dialer whose user and environment are the test's.
func testDialer() *Dialer {
	env := map[string]string{"SSH_AUTH_SOCK": "/run/agent.sock"}
	return &Dialer{Home: "/home/tester", User: "tester", Getenv: func(name string) string { return env[name] }}
}

// TestParse checks what Meridian takes from ssh -G's output: the host and
// its login, the key files, with ~ and their tokens expanded, the agent,
// the jumps, the known_hosts files and the timeouts.
func TestParse(t *testing.T) {
	d := testDialer()
	c := d.parse("alias", []byte(`host alias
user alice
hostname 10.0.0.5
port 2222
identityfile ~/.ssh/id_ecdsa
identityfile %d/keys/%u-%r@%h
identitiesonly yes
proxyjump j1,bob@j2:2200
userknownhostsfile ~/.ssh/known_hosts /tmp/kh2
globalknownhostsfile /etc/ssh/ssh_known_hosts
hostkeyalias farm
connecttimeout 7
serveraliveinterval 20
serveralivecountmax 2
stricthostkeychecking no
`))
	want := &hostConfig{
		alias: "alias", hostname: "10.0.0.5", port: "2222", user: "alice",
		identityFiles:  []string{"/home/tester/.ssh/id_ecdsa", "/home/tester/keys/tester-alice@10.0.0.5"},
		identitiesOnly: true, identityAgent: "/run/agent.sock",
		proxyJump:  []string{"j1", "bob@j2:2200"},
		knownHosts: []string{"/home/tester/.ssh/known_hosts", "/tmp/kh2", "/etc/ssh/ssh_known_hosts"}, hostKeyAlias: "farm",
		connectTimeout: 7 * time.Second, aliveInterval: 20 * time.Second, aliveCountMax: 2, read: true,
	}
	assertConfig(t, c, want)

	for _, tc := range []struct{ agent, want string }{
		{"identityagent none\n", ""},
		{"identityagent SSH_AUTH_SOCK\n", "/run/agent.sock"},
		{"identityagent /tmp/a.sock\n", "/tmp/a.sock"},
		{"", "/run/agent.sock"},
	} {
		if got := d.parse("h", []byte(tc.agent)).identityAgent; got != tc.want {
			t.Errorf("with %q, the agent is %q, want %q", tc.agent, got, tc.want)
		}
	}
	c = d.parse("h", []byte("proxyjump none\nproxycommand none\n"))
	if c.proxyJump != nil || c.proxyCommand != "" || c.hostname != "h" || c.port != "22" {
		t.Errorf("none for ProxyJump and ProxyCommand, and no hostname, give %+v", c)
	}
	if c = d.parse("h", []byte("proxycommand nc %h %p\n")); c.proxyCommand != "nc %h %p" {
		t.Errorf("ProxyCommand is %q, its tokens to be expanded later", c.proxyCommand)
	}
}

// TestDefaults checks the config OpenSSH gives a host when there is no
// ssh client to read the user's.
func TestDefaults(t *testing.T) {
	c := testDialer().defaults("h", "", "2200", "j")
	want := &hostConfig{
		alias: "h", hostname: "h", port: "2200", user: "tester", aliveCountMax: 3, identityAgent: "/run/agent.sock",
		proxyJump: []string{"j"},
		identityFiles: []string{
			"/home/tester/.ssh/id_rsa", "/home/tester/.ssh/id_ecdsa", "/home/tester/.ssh/id_ecdsa_sk",
			"/home/tester/.ssh/id_ed25519", "/home/tester/.ssh/id_ed25519_sk", "/home/tester/.ssh/id_xmss",
		},
		knownHosts: []string{"/home/tester/.ssh/known_hosts", "/home/tester/.ssh/known_hosts2", "/etc/ssh/ssh_known_hosts", "/etc/ssh/ssh_known_hosts2"},
	}
	assertConfig(t, c, want)
}

func assertConfig(t *testing.T, got, want *hostConfig) {
	t.Helper()
	if got.alias != want.alias || got.hostname != want.hostname || got.port != want.port || got.user != want.user ||
		!slices.Equal(got.identityFiles, want.identityFiles) || got.identitiesOnly != want.identitiesOnly ||
		got.identityAgent != want.identityAgent || !slices.Equal(got.proxyJump, want.proxyJump) ||
		got.proxyCommand != want.proxyCommand || !slices.Equal(got.knownHosts, want.knownHosts) ||
		got.hostKeyAlias != want.hostKeyAlias || got.connectTimeout != want.connectTimeout ||
		got.aliveInterval != want.aliveInterval || got.aliveCountMax != want.aliveCountMax || got.read != want.read {
		t.Errorf("the config is\n%+v\nwant\n%+v", got, want)
	}
}

// TestProxyCommandTokens checks that a ProxyCommand's tokens are expanded
// as OpenSSH expands them, and that one Meridian doesn't know is a problem
// with the config, never run as it is.
func TestProxyCommandTokens(t *testing.T) {
	d := testDialer()
	c := &hostConfig{alias: "a", hostname: "10.0.0.5", port: "22", user: "alice", proxyCommand: "x %h %p %r %n %u %d 100%%"}
	got, err := d.proxyCommand(c)
	if want := "x 10.0.0.5 22 alice a tester /home/tester 100%"; err != nil || got != want {
		t.Errorf("the command is %q, %v; want %q", got, err, want)
	}
	c.proxyCommand = "x %C"
	if _, err := d.proxyCommand(c); !isKind(err, problems.SSHConfigUnreadable) {
		t.Errorf("a token Meridian doesn't know gives %v", err)
	}
}

// TestHop checks how a ProxyJump's host is read: [user@]host[:port], an
// IPv6 address in brackets.
func TestHop(t *testing.T) {
	for _, tc := range []struct {
		spec, host, user, port string
		ok                     bool
	}{
		{"j", "j", "", "", true},
		{"bob@j", "j", "bob", "", true},
		{"bob@j:2200", "j", "bob", "2200", true},
		{"[::1]:2200", "::1", "", "2200", true},
		{"bob@[::1]", "::1", "bob", "", true},
		{"bob@", "", "bob", "", false},
	} {
		host, user, port, ok := hop(tc.spec)
		if host != tc.host || user != tc.user || port != tc.port || ok != tc.ok {
			t.Errorf("hop(%q) = %q, %q, %q, %v; want %q, %q, %q, %v", tc.spec, host, user, port, ok, tc.host, tc.user, tc.port, tc.ok)
		}
	}
}

// TestCategory checks which category a negotiation that found nothing in
// common is reported in.
func TestCategory(t *testing.T) {
	for what, want := range map[string]string{
		"key exchange": "key-exchange", "host key": "host-key",
		"client to server cipher": "cipher", "server to client MAC": "mac",
	} {
		if got := category(what); got != want {
			t.Errorf("category(%q) = %q, want %q", what, got, want)
		}
	}
}

// isKind reports whether err is a problem of kind k.
func isKind(err error, k *problems.Kind) bool {
	var p *problem.Problem
	return errors.As(err, &p) && p.Code == k.Code()
}
