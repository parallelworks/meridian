package sshconn

import (
	"context"
	"errors"
	"net"
	"slices"

	"golang.org/x/crypto/ssh"

	"github.com/parallelworks/meridian/problems"
)

// A hostKeyCheck checks a host's key against the user's known_hosts, and
// keeps what it saw, so that a handshake it fails is reported as the
// problem with the key.
type hostKeyCheck struct {
	// host is the host as it was named, which problems name it by; name
	// as known_hosts names it, its HostKeyAlias, or else its hostname,
	// with its port if it isn't 22, in brackets; and addr the hostname, or
	// alias, and port a host certificate's principals are checked against.
	host  string
	name  string
	addr  string
	known knownHosts
	// key is the key the host presented, and err what the check said of it.
	key ssh.PublicKey
	err error
}

// errRevoked is the check's error for a key known_hosts revokes.
var errRevoked = errors.New("sshconn: the host's key is revoked")

// A keyMismatch is the check's error for a key known_hosts doesn't have:
// want are the keys it has for the host, of any type.
type keyMismatch struct{ want []knownHost }

func (*keyMismatch) Error() string { return "sshconn: the host's key isn't in known_hosts" }

// newHostKeyCheck reads c's known_hosts files: the user's and the
// system's, whichever exist, for c's host. It is looked up by its
// HostKeyAlias, or else its hostname and port, as OpenSSH looks it up,
// never by its address, as CheckHostIP, which OpenSSH no longer does by
// default, would.
func (d *Dialer) newHostKeyCheck(ctx context.Context, c *hostConfig) (*hostKeyCheck, error) {
	host := c.hostname
	if c.hostKeyAlias != "" {
		host = c.hostKeyAlias
	}
	name := host
	if c.hostKeyAlias == "" && c.port != "22" {
		name = "[" + host + "]:" + c.port
	}
	known, err := d.readKnownHosts(ctx, c.knownHosts, name, c.alias)
	if err != nil {
		return nil, err
	}
	return &hostKeyCheck{host: c.alias, name: name, addr: net.JoinHostPort(host, c.port), known: known}, nil
}

// callback checks the key the host presents: a host certificate, signed
// by an authority known_hosts names for the host, for the host, and
// valid, or else its key, as OpenSSH falls back to it; or a key
// known_hosts has for it. A revoked key is refused either way.
func (h *hostKeyCheck) callback(_ string, remote net.Addr, key ssh.PublicKey) error {
	h.key = key
	checker := &ssh.CertChecker{
		IsHostAuthority: func(auth ssh.PublicKey, _ string) bool { return h.known.authority(auth, h.name) },
		IsRevoked: func(cert *ssh.Certificate) bool {
			return h.known.revoked(cert.SignatureKey, h.name) || h.known.revoked(cert.Key, h.name)
		},
		HostKeyFallback: h.plain,
	}
	h.err = checker.CheckHostKey(h.addr, remote, key)
	if cert, ok := key.(*ssh.Certificate); ok && h.err != nil && !errors.Is(h.err, errRevoked) {
		if err := h.plain(h.addr, remote, cert.Key); err == nil || errors.Is(err, errRevoked) {
			h.err = err
		}
	}
	return h.err
}

// plain checks a key that isn't a certificate against those known_hosts
// has for the host.
func (h *hostKeyCheck) plain(_ string, _ net.Addr, key ssh.PublicKey) error {
	if h.known.revoked(key, h.name) {
		return errRevoked
	}
	want := h.known.keys(h.name)
	for i := range want {
		if keysEqual(want[i].key, key) {
			return nil
		}
	}
	return &keyMismatch{want: want}
}

// algorithms are the host key algorithms to offer, as OpenSSH orders
// them, so that a host with keys of several types presents one the user
// has: certificates first if known_hosts names an authority for the host,
// then those of the types it has keys of, then the rest.
func (h *hostKeyCheck) algorithms() []string {
	var first []string
	if h.known.hasAuthority(h.name) {
		first = slices.Clone(certAlgorithms)
	}
	for _, k := range h.known.keys(h.name) {
		first = append(first, algorithmsOf(k.key.Type())...)
	}
	out := slices.Clone(first)
	for _, a := range slices.Concat(certAlgorithms, keyAlgorithms) {
		if !slices.Contains(out, a) {
			out = append(out, a)
		}
	}
	return out
}

// algorithmsOf returns the algorithms that sign with a key of a type, of
// those Meridian offers.
func algorithmsOf(keyType string) []string {
	switch keyType {
	case ssh.KeyAlgoRSA:
		return []string{ssh.KeyAlgoRSASHA512, ssh.KeyAlgoRSASHA256}
	case ssh.KeyAlgoECDSA256, ssh.KeyAlgoECDSA384, ssh.KeyAlgoECDSA521, ssh.KeyAlgoED25519:
		return []string{keyType}
	}
	return nil
}

// problem returns the problem with the host's key, once the handshake has
// failed, or nil if the check passed or didn't run: a revoked key, one
// known_hosts doesn't have, or one that differs from a key of the same
// type it has for the host. A key of a type it has none of for the host is
// unknown, as OpenSSH takes it, not changed; so is a certificate whose
// authority it doesn't name.
func (h *hostKeyCheck) problem() error {
	if h.err == nil || h.key == nil {
		return nil
	}
	fingerprint := ssh.FingerprintSHA256(h.key)
	if errors.Is(h.err, errRevoked) {
		return problems.SSHHostKeyRevoked.Problem(problems.Params{"host": h.host, "fingerprint": fingerprint})
	}
	keyType := h.key.Type()
	if cert, ok := h.key.(*ssh.Certificate); ok {
		keyType = cert.Key.Type()
	}
	if mismatch, ok := errors.AsType[*keyMismatch](h.err); ok {
		for _, k := range mismatch.want {
			if k.key.Type() == keyType {
				return problems.SSHHostKeyChanged.Problem(problems.Params{
					"host": h.host, "type": h.key.Type(), "fingerprint": fingerprint, "file": k.file, "line": k.line,
				})
			}
		}
	}
	return problems.SSHHostKeyUnknown.Problem(problems.Params{"host": h.host, "type": h.key.Type(), "fingerprint": fingerprint})
}
