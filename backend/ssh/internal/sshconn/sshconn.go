// Package sshconn connects to hosts over SSH as the user would with ssh:
// through their ssh_config, as `ssh -G` resolves it, checking each host's
// key against their known_hosts, and logging in with their agent's keys
// and their key files. It negotiates only the algorithms FIPS 140-3
// approves that x/crypto/ssh can run in FIPS 140-only mode, and never
// trusts a host key on first use (docs/design.md, Security).
//
// A Pool keeps one connection to each host, which its users share: it
// sends keepalives, so a connection that stops answering is closed, dials
// again with a backoff, and bounds how many sessions run on a connection at
// once, under sshd's MaxSessions.
package sshconn

import (
	"net"
	"strings"

	"golang.org/x/crypto/ssh"
)

// The algorithms Meridian negotiates. x/crypto/ssh drops what FIPS 140-3
// doesn't approve once FIPS mode is on, but still offers two that fail in
// FIPS 140-only mode, which Meridian runs in: AES-GCM, which it builds with
// an arbitrary nonce that the mode refuses, and the ML-KEM hybrid key
// exchange, whose X25519 the mode refuses too. So the lists are pinned,
// each of their entries is tested under the mode, and a host that offers
// none of a list's can't be used (problems.SSHAlgorithmsUnapproved).
var (
	keyExchanges = []string{ssh.KeyExchangeECDHP256, ssh.KeyExchangeECDHP384, ssh.KeyExchangeECDHP521}
	ciphers      = []string{ssh.CipherAES128CTR, ssh.CipherAES192CTR, ssh.CipherAES256CTR}
	macs         = []string{ssh.HMACSHA256ETM, ssh.HMACSHA512ETM, ssh.HMACSHA256, ssh.HMACSHA512}
	// certAlgorithms and keyAlgorithms are the host key algorithms, of
	// certificates and of plain keys, in the order offered when
	// known_hosts says nothing of the host's (hostKeyCheck.algorithms).
	certAlgorithms = []string{
		ssh.CertAlgoECDSA256v01, ssh.CertAlgoECDSA384v01, ssh.CertAlgoECDSA521v01,
		ssh.CertAlgoED25519v01, ssh.CertAlgoRSASHA512v01, ssh.CertAlgoRSASHA256v01,
	}
	keyAlgorithms = []string{
		ssh.KeyAlgoECDSA256, ssh.KeyAlgoECDSA384, ssh.KeyAlgoECDSA521,
		ssh.KeyAlgoED25519, ssh.KeyAlgoRSASHA512, ssh.KeyAlgoRSASHA256,
	}
)

// sshConfig returns the transport's algorithms, pinned.
func sshConfig() ssh.Config {
	return ssh.Config{KeyExchanges: keyExchanges, Ciphers: ciphers, MACs: macs}
}

// A Target is a host to connect to, as a format 1 ssh: names it.
type Target struct {
	// Host is remoteHost: a name the user's ssh_config may know, as an
	// alias, an address, or a host and port, host:port, which is taken as
	// it is, without the config's HostName, ProxyJump or ProxyCommand.
	Host string
	// User is remoteUser, over the config's User, or "" for the config's,
	// or else the local user.
	User string
	// JumpHost is jumpNodeHost, a host to connect through, over the
	// config's ProxyJump and ProxyCommand, with JumpUser, jumpNodeUser, or
	// "" for the config's, or else the local user. "" connects as the
	// config says.
	JumpHost string
	JumpUser string
}

// key is what a Pool keys a connection to t by: the host and the login,
// which format 1 keys the connections it reuses by too.
func (t Target) key() string {
	return strings.Join([]string{t.Host, t.User, t.JumpHost, t.JumpUser}, "\x00")
}

// hostPort splits a host given as host:port, and reports false for one
// that isn't, which names a host the config may know.
func hostPort(host string) (string, string, bool) {
	h, p, err := net.SplitHostPort(host)
	if err != nil || h == "" || p == "" {
		return "", "", false
	}
	return h, p, true
}
