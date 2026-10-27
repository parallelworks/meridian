package sshconn

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/parallelworks/foundation/problem"
	"golang.org/x/crypto/ssh"

	"github.com/parallelworks/meridian/problems"
)

// krlMagic starts a KRL, OpenSSH's binary list of revoked keys and
// certificates.
const krlMagic = "SSHKRL\n\x00"

// revocations are what a config's RevokedHostKeys file revokes: the keys
// it lists, one to a line, as authorized_keys has them, or, for a KRL,
// what ssh-keygen -Q says it revokes.
type revocations struct {
	host string
	file string
	keys []ssh.PublicKey
	krl  bool
}

// readRevocations reads c's RevokedHostKeys file, or returns nil where the
// config names none. A file that can't be read, as one that isn't there,
// is problems.SSHRevokedHostKeysUnreadable, as OpenSSH refuses every key
// then: what it revokes can't be known. A line that isn't a key is
// skipped, as OpenSSH skips it.
func readRevocations(c *hostConfig) (*revocations, error) {
	if c.revokedHostKeys == "" {
		return nil, nil
	}
	r := &revocations{host: c.alias, file: c.revokedHostKeys}
	data, err := os.ReadFile(r.file) //nolint:gosec // the revoked keys file the user's ssh_config names
	if err != nil {
		return nil, r.unreadable(err)
	}
	if bytes.HasPrefix(data, []byte(krlMagic)) {
		r.krl = true
		return r, nil
	}
	for line := range strings.Lines(string(data)) {
		if line = strings.TrimSpace(line); line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if key, _, _, _, err := ssh.ParseAuthorizedKey([]byte(line)); err == nil {
			r.keys = append(r.keys, key)
		}
	}
	return r, nil
}

// unreadable is the problem with r's file that err is: err itself if it is
// a problem, as an ssh-keygen that isn't safe to run.
func (r *revocations) unreadable(err error) error {
	if p := asProblem(err); p != nil {
		return p
	}
	return problems.SSHRevokedHostKeysUnreadable.Problem(problems.Params{"host": r.host, "file": r.file, "reason": err.Error()})
}

// revokes reports whether r revokes key, or, for a certificate, its key or
// its authority's, as OpenSSH checks both. A KRL ssh-keygen can't check
// a key against, as where there is none, is
// problems.SSHRevokedHostKeysUnreadable.
func (r *revocations) revokes(ctx context.Context, d *Dialer, key ssh.PublicKey) (bool, error) {
	keys := []ssh.PublicKey{key}
	if cert, ok := key.(*ssh.Certificate); ok {
		keys = append(keys, cert.Key, cert.SignatureKey)
	}
	if !r.krl {
		for _, k := range keys {
			for _, revoked := range r.keys {
				if keysEqual(k, revoked) {
					return true, nil
				}
			}
		}
		return false, nil
	}
	revoked, err := d.keygenRevoked(ctx, r.file, keys)
	if err != nil {
		return false, r.unreadable(err)
	}
	return revoked, nil
}

// keygenRevoked asks ssh-keygen -Q whether the KRL krl revokes any of keys,
// which it writes to files of their own, in a directory only the user can
// open. It exits 0 when the KRL revokes none of them, and 1, saying one is
// REVOKED, when it revokes one; anything else, as a KRL it can't read, is
// an error.
func (d *Dialer) keygenRevoked(ctx context.Context, krl string, keys []ssh.PublicKey) (bool, error) {
	keygen, err := command(d.keygen())
	if err != nil {
		return false, err
	}
	dir, err := os.MkdirTemp("", "meridian-krl-")
	if err != nil {
		return false, err
	}
	defer os.RemoveAll(dir)
	args := []string{"-Q", "-f", krl}
	for i, k := range keys {
		path := filepath.Join(dir, strconv.Itoa(i)+".pub")
		if err := os.WriteFile(path, ssh.MarshalAuthorizedKey(k), 0o600); err != nil {
			return false, err
		}
		args = append(args, path)
	}
	cmd := exec.CommandContext(ctx, keygen, args...) //nolint:gosec // ssh-keygen, a checked command, with the user's KRL and the host's keys as its arguments, and no shell
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err == nil {
		return false, nil
	}
	if exit, ok := errors.AsType[*exec.ExitError](err); ok && exit.ExitCode() == 1 && bytes.Contains(out, []byte(": REVOKED")) {
		return true, nil
	}
	return false, errors.New(said(stderr.String(), err))
}

// asProblem returns err's problem, or nil for an error that isn't one.
func asProblem(err error) *problem.Problem {
	p, _ := errors.AsType[*problem.Problem](err)
	return p
}
