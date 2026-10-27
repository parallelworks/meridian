package sshconn

import (
	"crypto/rand"
	"crypto/rsa"
	"errors"
	"io"
	"math/big"
	"slices"
	"testing"
	"time"

	"github.com/parallelworks/foundation/problem"
	"golang.org/x/crypto/ssh"

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
// OpenSSH 9.6's valid_hostname and valid_ruser say.
func TestValidNames(t *testing.T) {
	for host, want := range map[string]bool{
		"farm": true, "farm-1.example.org": true, "10.0.0.5:2222": true, "[::1]:22": true, "": true, "h\u00e9te": true,
		"-oProxyCommand=x": false, "a;b": false, "a b": false, "a\tb": false, "a$b": false, "a`b`": false, "a|b": false,
		"a&b": false, "a>b": false, "a(b)": false, "a{b}": false, "a'b": false, "a\"b": false, "a\\b": false, "a\x01b": false, "a\x7fb": false,
	} {
		if got := validHostname(host); got != want {
			t.Errorf("validHostname(%q) = %v, want %v", host, got, want)
		}
	}
	for user, want := range map[string]bool{
		"": true, "alice": true, "a.b-c_d": true, "host$": true, "a b": true, "a\\b": true,
		"-l": false, "a -b": false, "a;b": false, "a`b": false, "a(b": false, "a'b": false, "a\\": false, "a|b": false,
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
