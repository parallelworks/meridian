package sshconn

import (
	"context"
	"crypto/rsa"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"
)

// minRSABits is the smallest RSA key FIPS 140-3 lets a process sign with.
const minRSABits = 2048

// signers are the keys to log in to c's host with, in the order they are
// offered, as OpenSSH orders them: the agent's, then the key files the
// config names, then the user's other key pairs in ~/.ssh, a private key
// beside a .pub, unless IdentitiesOnly, when only the files' keys are
// offered, from the agent if it has them. A key file that needs a
// passphrase is skipped, since Meridian never asks for one: its key is
// offered if the agent holds it. Keys FIPS 140-3 doesn't let it sign with,
// DSA and RSA under 2048 bits, are skipped, and RSA keys sign only with
// SHA-2. It returns what closes the connection to the agent, once the
// handshake is done.
func (d *Dialer) signers(ctx context.Context, c *hostConfig) ([]ssh.Signer, func()) {
	var files []ssh.Signer
	for _, f := range c.identityFiles {
		if s, ok := readKey(f); ok {
			files = append(files, s)
		}
	}
	var pairs []ssh.Signer
	if !c.identitiesOnly {
		matches, _ := filepath.Glob(filepath.Join(d.home(), ".ssh", "*.pub"))
		for _, pub := range matches {
			if s, ok := readKey(strings.TrimSuffix(pub, ".pub")); ok {
				pairs = append(pairs, s)
			}
		}
	}
	closeAgent := func() {}
	var agents []ssh.Signer
	if c.identityAgent != "" {
		if conn, err := (&net.Dialer{}).DialContext(ctx, "unix", c.identityAgent); err == nil {
			closeAgent = func() { _ = conn.Close() }
			agents, _ = agent.NewClient(conn).Signers()
			if c.identitiesOnly {
				wanted := publicKeys(c.identityFiles)
				agents = slices.DeleteFunc(agents, func(s ssh.Signer) bool { return !wanted[string(s.PublicKey().Marshal())] })
			}
		}
	}
	var out []ssh.Signer
	seen := map[string]bool{}
	for _, s := range slices.Concat(agents, files, pairs) {
		blob := string(s.PublicKey().Marshal())
		if seen[blob] {
			continue
		}
		seen[blob] = true
		if s, ok := usable(s); ok {
			out = append(out, s)
		}
	}
	return out, closeAgent
}

// readKey reads a private key file, and reports false for one that isn't
// there, can't be read, or needs a passphrase.
func readKey(path string) (ssh.Signer, bool) {
	data, err := os.ReadFile(path) //nolint:gosec // a key file the user's ssh_config names, or one of their own in ~/.ssh
	if err != nil {
		return nil, false
	}
	s, err := ssh.ParsePrivateKey(data)
	return s, err == nil
}

// publicKeys returns the public keys of key files, from each one's .pub,
// or else from the private key, by their wire encoding.
func publicKeys(files []string) map[string]bool {
	out := map[string]bool{}
	for _, f := range files {
		if data, err := os.ReadFile(f + ".pub"); err == nil { //nolint:gosec // beside a key file the user's ssh_config names
			if pub, _, _, _, err := ssh.ParseAuthorizedKey(data); err == nil {
				out[string(pub.Marshal())] = true
				continue
			}
		}
		if s, ok := readKey(f); ok {
			out[string(s.PublicKey().Marshal())] = true
		}
	}
	return out
}

// usable returns s as Meridian may sign with it, and false for a key FIPS
// 140-3 doesn't let it sign with: an RSA key signs only with SHA-2, and
// only if it has 2048 bits or more; a DSA key not at all. A certificate is
// held to the rules for its key.
func usable(s ssh.Signer) (ssh.Signer, bool) {
	key := s.PublicKey()
	if cert, ok := key.(*ssh.Certificate); ok {
		key = cert.Key
	}
	switch key.Type() {
	case ssh.KeyAlgoRSA:
		pub, ok := key.(ssh.CryptoPublicKey)
		if !ok {
			return nil, false
		}
		if k, ok := pub.CryptoPublicKey().(*rsa.PublicKey); !ok || k.N.BitLen() < minRSABits {
			return nil, false
		}
		as, ok := s.(ssh.AlgorithmSigner)
		if !ok {
			return nil, false
		}
		ms, err := ssh.NewSignerWithAlgorithms(as, []string{ssh.KeyAlgoRSASHA512, ssh.KeyAlgoRSASHA256})
		return ms, err == nil
	case "ssh-dss":
		return nil, false
	}
	return s, true
}
