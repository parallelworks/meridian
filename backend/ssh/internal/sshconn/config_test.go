package sshconn

import (
	"crypto/rand"
	"crypto/rsa"
	"errors"
	"io"
	"math/big"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/parallelworks/foundation/problem"
	"golang.org/x/crypto/ssh"

	"github.com/parallelworks/meridian/problems"
)

// testDialer is a Dialer whose user and environment are the test's, and
// whose config, which no ssh client reads, isn't there.
func testDialer() *Dialer {
	env := map[string]string{"SSH_AUTH_SOCK": "/run/agent.sock"}
	return &Dialer{Home: "/home/tester", User: "tester", ConfigFile: "/nonexistent/ssh_config", Getenv: func(name string) string { return env[name] }}
}

// parsed parses out as ssh -G's output for alias, asked for on no port in
// particular, and fails the test if it can't.
func parsed(t *testing.T, d *Dialer, alias, out string) *hostConfig {
	t.Helper()
	c, err := d.parse(alias, "", out)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// TestParse checks what Meridian takes from ssh -G's output: the host and
// its login, the key files, with ~ and their tokens expanded, the agent,
// the jumps, the known_hosts files, the revoked keys, and the timeouts.
func TestParse(t *testing.T) {
	d := testDialer()
	c := parsed(t, d, "alias", `host alias
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
revokedhostkeys ~/.ssh/revoked
connecttimeout 7
serveraliveinterval 20
serveralivecountmax 2
stricthostkeychecking no
`)
	d.expandPaths(c)
	want := &hostConfig{
		alias: "alias", hostname: "10.0.0.5", port: "2222", user: "alice",
		identityFiles:  []string{"/home/tester/.ssh/id_ecdsa", "/home/tester/keys/tester-alice@10.0.0.5"},
		identitiesOnly: true, identityAgent: "/run/agent.sock",
		proxyJump:  []string{"j1", "bob@j2:2200"},
		knownHosts: []string{"/home/tester/.ssh/known_hosts", "/tmp/kh2", "/etc/ssh/ssh_known_hosts"}, hostKeyAlias: "farm",
		revokedHostKeys: "/home/tester/.ssh/revoked",
		connectTimeout:  7 * time.Second, aliveInterval: 20 * time.Second, aliveCountMax: 2, read: true,
	}
	assertConfig(t, c, want)

	for _, tc := range []struct{ agent, want string }{
		{"identityagent none\n", ""},
		{"identityagent SSH_AUTH_SOCK\n", "/run/agent.sock"},
		{"identityagent /tmp/a.sock\n", "/tmp/a.sock"},
		{"", "/run/agent.sock"},
	} {
		if got := parsed(t, d, "h", tc.agent).identityAgent; got != tc.want {
			t.Errorf("with %q, the agent is %q, want %q", tc.agent, got, tc.want)
		}
	}
	c = parsed(t, d, "h", "proxyjump none\nproxycommand none\nrevokedhostkeys none\n")
	if c.proxyJump != nil || c.proxyCommand != "" || c.revokedHostKeys != "" || c.hostname != "h" || c.port != "22" {
		t.Errorf("none for ProxyJump, ProxyCommand and RevokedHostKeys, and no hostname, give %+v", c)
	}
	if c = parsed(t, d, "h", "proxycommand nc %h %p\n"); c.proxyCommand != "nc %h %p" {
		t.Errorf("ProxyCommand is %q, its tokens to be expanded later", c.proxyCommand)
	}
	if c = parsed(t, d, "h", "knownhostscommand /usr/bin/keys %H\n"); !c.knownHostsCommand {
		t.Error("a KnownHostsCommand isn't noticed")
	}
}

// TestParseStrict checks that ssh -G's output is taken only as ssh prints
// it: a keyword Meridian reads once, but IdentityFile, the host asked for,
// in any case, and the port asked for, as a number; and that what it
// doesn't read may come more than once.
func TestParseStrict(t *testing.T) {
	d := testDialer()
	for _, tc := range []struct{ port, out, keyword string }{
		{"", "user x\nproxycommand touch /tmp/p\nproxycommand none\n", "proxycommand"},
		{"", "user x\nuser alice\n", "user"},
		{"", "hostname a\nhostname b\n", "hostname"},
		{"", "userknownhostsfile /a\nuserknownhostsfile /b\n", "userknownhostsfile"},
		{"", "revokedhostkeys /r\nrevokedhostkeys none\n", "revokedhostkeys"},
		{"", "identityagent none\nidentityagent /tmp/a.sock\n", "identityagent"},
		{"", "host other\n", "host"},
		{"", "host alias\nhost alias\n", "host"},
		{"2222", "port 22\n", "port"},
	} {
		_, err := d.parse("alias", tc.port, tc.out)
		if p, ok := errors.AsType[*problem.Problem](err); !ok || p.Code != problems.SSHConfigOutputUnexpected.Code() || p.Params["keyword"] != tc.keyword || p.Params["host"] != "alias" {
			t.Errorf("%q, asked for on port %q, gives %v, want a problem with %s", tc.out, tc.port, err, tc.keyword)
		}
	}
	c, err := d.parse("Alias", "02222", "host Alias\nport 2222\nidentityfile ~/a\nidentityfile ~/b\nsendenv A\nsendenv B\n")
	if err != nil || len(c.identityFiles) != 2 || c.port != "2222" {
		t.Errorf("ssh -G's output as it prints it gives %+v, %v", c, err)
	}
}

// TestDefaults checks the config OpenSSH gives a host when there is no
// ssh client to read the user's, and that there is none only where there
// is no config for one to read: ConfigFile, or the user's.
func TestDefaults(t *testing.T) {
	d := testDialer()
	c, err := d.defaults("h", "2200")
	if err != nil {
		t.Fatal(err)
	}
	want := &hostConfig{
		alias: "h", hostname: "h", port: "2200", user: "tester", aliveCountMax: 3, identityAgent: "/run/agent.sock",
		identityFiles: []string{
			"/home/tester/.ssh/id_rsa", "/home/tester/.ssh/id_ecdsa", "/home/tester/.ssh/id_ecdsa_sk",
			"/home/tester/.ssh/id_ed25519", "/home/tester/.ssh/id_ed25519_sk", "/home/tester/.ssh/id_xmss",
		},
		knownHosts: []string{"/home/tester/.ssh/known_hosts", "/home/tester/.ssh/known_hosts2", "/etc/ssh/ssh_known_hosts", "/etc/ssh/ssh_known_hosts2"},
	}
	assertConfig(t, c, want)

	write := func(path string) {
		t.Helper()
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("Host *\n  RevokedHostKeys ~/.ssh/revoked\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	d.ConfigFile = filepath.Join(t.TempDir(), "config")
	write(d.ConfigFile)
	user := testDialer()
	user.ConfigFile, user.Home = "", t.TempDir()
	write(filepath.Join(user.Home, ".ssh", "config"))
	for d, path := range map[*Dialer]string{d: d.ConfigFile, user: filepath.Join(user.Home, ".ssh", "config")} {
		_, err := d.defaults("h", "")
		if p, ok := errors.AsType[*problem.Problem](err); !ok || p.Code != problems.SSHClientMissing.Code() || p.Params["path"] != path {
			t.Errorf("with %s and no ssh client, the defaults give %v", path, err)
		}
	}
}

func assertConfig(t *testing.T, got, want *hostConfig) {
	t.Helper()
	if got.alias != want.alias || got.hostname != want.hostname || got.port != want.port || got.user != want.user ||
		!slices.Equal(got.identityFiles, want.identityFiles) || got.identitiesOnly != want.identitiesOnly ||
		got.identityAgent != want.identityAgent || !slices.Equal(got.proxyJump, want.proxyJump) ||
		got.proxyCommand != want.proxyCommand || got.jumpHost != want.jumpHost || got.jumpUser != want.jumpUser ||
		!slices.Equal(got.knownHosts, want.knownHosts) || got.hostKeyAlias != want.hostKeyAlias ||
		got.revokedHostKeys != want.revokedHostKeys || got.knownHostsCommand != want.knownHostsCommand ||
		got.connectTimeout != want.connectTimeout || got.aliveInterval != want.aliveInterval ||
		got.aliveCountMax != want.aliveCountMax || got.read != want.read {
		t.Errorf("the config is\n%+v\nwant\n%+v", got, want)
	}
}

// TestResolveUser checks that ssh -G is given the user, as ssh -l, so that
// it reads the config as that user's, and that resolve gives it none it
// wasn't given checked: one that an ssh before 10.1 would print as it was
// given, a newline in it starting a line of the config's own, nor a host
// that isn't safe, runs nothing.
func TestResolveUser(t *testing.T) {
	dir := t.TempDir()
	fake := filepath.Join(dir, "ssh")
	args := filepath.Join(dir, "args")
	script := "#!/bin/sh\nprintf '%s\\n' \"$@\" >> '" + args + "'\nuser=tester\nwhile [ $# -gt 0 ]; do\n\tcase \"$1\" in -l) user=\"$2\"; shift ;; esac\n\tshift\ndone\nprintf 'user %s\\n' \"$user\"\n"
	if err := os.WriteFile(fake, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	d := testDialer()
	d.SSH = fake
	c, err := d.resolve(t.Context(), "farm", "", "alice")
	if err != nil || c.user != "alice" {
		t.Fatalf("resolve gives %+v, %v", c, err)
	}
	data, err := os.ReadFile(args)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Split(strings.TrimSpace(string(data)), "\n"); !slices.Equal(got, []string{"-G", "-F", d.ConfigFile, "-l", "alice", "--", "farm"}) {
		t.Errorf("ssh was given %q", got)
	}
	if err := os.Remove(args); err != nil {
		t.Fatal(err)
	}
	pwned := filepath.Join(dir, "pwned")
	for _, tc := range []struct{ host, user string }{
		{"farm", "x\nproxycommand touch " + pwned},
		{"farm", "-oProxyCommand=touch " + pwned},
		{"a;touch " + pwned, ""},
	} {
		if c, err := d.resolve(t.Context(), tc.host, "", tc.user); err == nil {
			t.Errorf("resolve(%q, %q) gives %+v", tc.host, tc.user, c)
		}
	}
	for _, f := range []string{args, pwned} {
		if _, err := os.Stat(f); err == nil {
			t.Errorf("an unchecked host or user ran something: %s exists", f)
		}
	}
}

// TestResolveUserTokens checks, with the ssh client on PATH, that %r means
// the same user wherever the config has it, as with ssh: in the files ssh
// expands it in itself, UserKnownHostsFile and RevokedHostKeys, in those
// Meridian expands it in, IdentityFile, and to a Match on the user.
func TestResolveUserTokens(t *testing.T) {
	if _, err := exec.LookPath("ssh"); err != nil {
		t.Skip("no ssh client")
	}
	config := filepath.Join(t.TempDir(), "config")
	text := "Host farm\n  UserKnownHostsFile ~/.ssh/known_hosts_%r\n  RevokedHostKeys ~/.ssh/revoked_%r\n  IdentityFile ~/.ssh/id_%r\nMatch user alice\n  Port 2222\n"
	if err := os.WriteFile(config, []byte(text), 0o600); err != nil {
		t.Fatal(err)
	}
	d := testDialer()
	d.ConfigFile = config
	c, err := d.resolve(t.Context(), "farm", "", "alice")
	if err != nil {
		t.Fatal(err)
	}
	userFile := slices.ContainsFunc(c.knownHosts, func(f string) bool { return strings.HasSuffix(f, "/.ssh/known_hosts_alice") })
	if c.user != "alice" || c.port != "2222" || !userFile ||
		!strings.HasSuffix(c.revokedHostKeys, "/.ssh/revoked_alice") || !slices.Contains(c.identityFiles, "/home/tester/.ssh/id_alice") {
		t.Errorf("as alice, the config is %+v", c)
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
	c.proxyCommand = "x %C secret %L"
	_, err = d.proxyCommand(c)
	if p, ok := errors.AsType[*problem.Problem](err); !ok || p.Code != problems.SSHProxyCommandToken.Code() || p.Params["token"] != "%C" || len(p.Params) != 2 {
		t.Errorf("a token Meridian doesn't know gives %v, which must name the token, and not the command", err)
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
		{"ssh://bob@j:2200", "j", "bob", "2200", true},
		{"ssh://j", "j", "", "", true},
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

// TestValidNames checks which hosts and users are safe to give ssh, as
// OpenSSH 10.1's valid_hostname and valid_ruser say: a user's control
// characters too, which 9.6's took.
func TestValidNames(t *testing.T) {
	for host, want := range map[string]bool{
		"farm": true, "farm-1.example.org": true, "10.0.0.5:2222": true, "[::1]:22": true, "": true, "h\u00e9te": true,
		"-oProxyCommand=x": false, "a;b": false, "a b": false, "a\tb": false, "a$b": false, "a`b`": false, "a|b": false,
		"a&b": false, "a>b": false, "a(b)": false, "a{b}": false, "a'b": false, "a\"b": false, "a\\b": false, "a\x01b": false, "a\x7fb": false,
		"a\nproxycommand touch p": false, "a\rb": false, "a\x00b": false, "a\x1b[31m": false,
	} {
		if got := validHostname(host); got != want {
			t.Errorf("validHostname(%q) = %v, want %v", host, got, want)
		}
	}
	for host, want := range map[string]bool{
		"farm": true, "10.0.0.5:2222": true, "[::1]:22": true, "": true,
		"alice@farm": false, "ssh://farm": false, "ssh://alice@farm:22": false, "a,b": false, "sftp://farm": false,
	} {
		if got := hostForm(host); got != want {
			t.Errorf("hostForm(%q) = %v, want %v", host, got, want)
		}
	}
	for user, want := range map[string]bool{
		"": true, "alice": true, "a.b-c_d": true, "host$": true, "a b": true, "a\\b": true,
		"-l": false, "a -b": false, "a;b": false, "a`b": false, "a(b": false, "a'b": false, "a\\": false, "a|b": false,
		"x\nproxycommand touch p": false, "a\rb": false, "a\x00b": false, "a\x1b[31m": false, "a\x7f": false, "a\tb": false,
	} {
		if got := validUser(user); got != want {
			t.Errorf("validUser(%q) = %v, want %v", user, got, want)
		}
	}
}

// fakeSigner is a signer of a key it can't sign with, for what usable
// says of the key.
type fakeSigner struct{ key ssh.PublicKey }

func (s fakeSigner) PublicKey() ssh.PublicKey { return s.key }
func (s fakeSigner) Sign(io.Reader, []byte) (*ssh.Signature, error) {
	return nil, errors.New("fakeSigner: can't sign")
}

// TestUsableCertificates checks that a certificate is held to the rules
// for its key: an RSA key's of fewer than 2048 bits isn't used, and one's
// of 2048 signs only with SHA-2.
func TestUsableCertificates(t *testing.T) {
	small, err := ssh.NewPublicKey(&rsa.PublicKey{N: new(big.Int).Lsh(big.NewInt(1), 1023), E: 65537})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := usable(fakeSigner{&ssh.Certificate{Key: small, CertType: ssh.UserCert}}); ok {
		t.Error("an RSA certificate of 1024 bits is used")
	}
	k, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := ssh.NewSignerFromKey(k)
	if err != nil {
		t.Fatal(err)
	}
	cert := &ssh.Certificate{Key: signer.PublicKey(), CertType: ssh.UserCert, ValidPrincipals: []string{"alice"}, ValidBefore: ssh.CertTimeInfinity}
	if err := cert.SignCert(rand.Reader, signer); err != nil {
		t.Fatal(err)
	}
	certSigner, err := ssh.NewCertSigner(cert, signer)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := usable(certSigner)
	ms, isMulti := got.(ssh.MultiAlgorithmSigner)
	if !ok || !isMulti || !slices.Equal(ms.Algorithms(), []string{ssh.KeyAlgoRSASHA512, ssh.KeyAlgoRSASHA256}) {
		t.Errorf("an RSA certificate of 2048 bits gives %v, %v; want it to sign with SHA-2 only", got, ok)
	}
}
