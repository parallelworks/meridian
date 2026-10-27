package sshconn

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io/fs"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"

	"golang.org/x/crypto/ssh"

	"github.com/parallelworks/meridian/problems"
)

// A knownHost is a line of a known_hosts file: a key, the hosts it is
// for, and its marker, @cert-authority, @revoked, or "".
type knownHost struct {
	marker   string
	patterns []string
	key      ssh.PublicKey
	file     string
	line     int
}

// knownHosts are the lines of the known_hosts files that can be for a
// host. A hashed line, whose hosts are an HMAC-SHA1 of each, is one only
// if ssh-keygen -F found it for the host: FIPS 140-3 doesn't let Meridian
// compute HMAC-SHA1, so OpenSSH's tool matches it, as ssh -G reads the
// config. Without ssh-keygen, hashed lines match no host.
type knownHosts []knownHost

// readKnownHosts reads files, those of them that exist, for the host
// named name, as known_hosts names it: host, or [host]:port for a port
// other than 22. A line that can't be read is skipped, as OpenSSH skips
// it. A file that can't be read is problems.SSHConfigUnreadable, of the
// host alias; and one with hashed lines that ssh-keygen can't match,
// there being none, problems.SSHKnownHostsUnmatched, since one of them
// could revoke the host's key: Meridian doesn't check a key against part
// of known_hosts.
func (d *Dialer) readKnownHosts(ctx context.Context, files []string, name, alias string) (knownHosts, error) {
	var out knownHosts
	for _, f := range files {
		data, err := os.ReadFile(f) //nolint:gosec // a known_hosts file the user's ssh_config names
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, problems.SSHConfigUnreadable.Problem(problems.Params{"host": alias, "reason": err.Error()})
		}
		var hashed map[int]bool
		n := 0
		s := bufio.NewScanner(bytes.NewReader(data))
		s.Buffer(nil, 1<<20)
		for s.Scan() {
			n++
			kh, isHashed, ok := parseKnownHost(s.Text())
			if !ok {
				continue
			}
			kh.file, kh.line = f, n
			if isHashed {
				if hashed == nil {
					if hashed, err = d.keygenFind(ctx, f, name); err != nil {
						if p := asProblem(err); p != nil {
							return nil, p
						}
						return nil, problems.SSHKnownHostsUnmatched.Problem(problems.Params{"host": alias, "file": f, "reason": err.Error()})
					}
				}
				if !hashed[n] {
					continue
				}
				kh.patterns = []string{name}
			}
			out = append(out, kh)
		}
	}
	return out, nil
}

// parseKnownHost reads a line of known_hosts, and reports whether its
// hosts are hashed, and false for a line that has no key.
func parseKnownHost(line string) (kh knownHost, hashed, ok bool) {
	fields := strings.Fields(line)
	if len(fields) == 0 || strings.HasPrefix(fields[0], "#") {
		return knownHost{}, false, false
	}
	if strings.HasPrefix(fields[0], "@") {
		kh.marker, fields = fields[0], fields[1:]
		if kh.marker != "@cert-authority" && kh.marker != "@revoked" {
			return knownHost{}, false, false
		}
	}
	if len(fields) < 3 {
		return knownHost{}, false, false
	}
	key, _, _, _, err := ssh.ParseAuthorizedKey([]byte(strings.Join(fields[1:], " ")))
	if err != nil {
		return knownHost{}, false, false
	}
	kh.key = key
	if strings.HasPrefix(fields[0], "|1|") {
		return kh, true, true
	}
	kh.patterns = strings.Split(fields[0], ",")
	return kh, false, true
}

// foundLine is how ssh-keygen -F says which line it found.
var foundLine = regexp.MustCompile(`^# Host .* found: line (\d+)`)

// keygenFind returns the lines of file that ssh-keygen -F finds for name,
// by their numbers, and an error if it can't say: there is no ssh-keygen,
// one that isn't safe to run (command), or it failed. It exits 0 when it
// found lines, and 1 when it found none.
func (d *Dialer) keygenFind(ctx context.Context, file, name string) (map[int]bool, error) {
	keygen, err := command(d.keygen())
	if err != nil {
		return nil, err
	}
	cmd := exec.CommandContext(ctx, keygen, "-F", name, "-f", file) //nolint:gosec // ssh-keygen, a checked command, with a host's name and a known_hosts file as its arguments, and no shell
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if exit, ok := errors.AsType[*exec.ExitError](err); ok && exit.ExitCode() == 1 {
		err = nil
	}
	if err != nil {
		return nil, errors.New(said(stderr.String(), err))
	}
	lines := map[int]bool{}
	for line := range strings.Lines(string(out)) {
		if m := foundLine.FindStringSubmatch(line); m != nil {
			if n, err := strconv.Atoi(m[1]); err == nil {
				lines[n] = true
			}
		}
	}
	return lines, nil
}

// keygen is ssh-keygen, beside the ssh client where one is named.
func (d *Dialer) keygen() string {
	if d.SSH != "" && strings.Contains(d.SSH, "/") {
		return d.SSH[:strings.LastIndexByte(d.SSH, '/')+1] + "ssh-keygen"
	}
	return "ssh-keygen"
}

// matches reports whether kh is for the host named name: one of its
// patterns matches it and none of those negated, with !, does, as OpenSSH
// matches them, without regard to case.
func (kh *knownHost) matches(name string) bool {
	name = strings.ToLower(name)
	matched := false
	for _, p := range kh.patterns {
		negated := strings.HasPrefix(p, "!")
		if !wildcardMatch(strings.ToLower(strings.TrimPrefix(p, "!")), name) {
			continue
		}
		if negated {
			return false
		}
		matched = true
	}
	return matched
}

// wildcardMatch reports whether s matches pattern, in which * matches any
// run of characters, and ? any one.
func wildcardMatch(pattern, s string) bool {
	px, sx := 0, 0
	star, mark := -1, 0
	for sx < len(s) {
		switch {
		case px < len(pattern) && (pattern[px] == '?' || pattern[px] == s[sx]):
			px++
			sx++
		case px < len(pattern) && pattern[px] == '*':
			star, mark = px, sx
			px++
		case star >= 0:
			px = star + 1
			mark++
			sx = mark
		default:
			return false
		}
	}
	for px < len(pattern) && pattern[px] == '*' {
		px++
	}
	return px == len(pattern)
}

// revoked reports whether a line marks key revoked for name.
func (k knownHosts) revoked(key ssh.PublicKey, name string) bool {
	for i := range k {
		if k[i].marker == "@revoked" && k[i].matches(name) && keysEqual(k[i].key, key) {
			return true
		}
	}
	return false
}

// authority reports whether a line makes key an authority for name's
// host certificates.
func (k knownHosts) authority(key ssh.PublicKey, name string) bool {
	for i := range k {
		if k[i].marker == "@cert-authority" && k[i].matches(name) && keysEqual(k[i].key, key) {
			return true
		}
	}
	return false
}

// hasAuthority reports whether a line names an authority for name's host
// certificates.
func (k knownHosts) hasAuthority(name string) bool {
	for i := range k {
		if k[i].marker == "@cert-authority" && k[i].matches(name) {
			return true
		}
	}
	return false
}

// keys are the keys the files hold for name, of any type.
func (k knownHosts) keys(name string) []knownHost {
	var out []knownHost
	for i := range k {
		if k[i].marker == "" && k[i].matches(name) {
			out = append(out, k[i])
		}
	}
	return out
}

func keysEqual(a, b ssh.PublicKey) bool {
	return bytes.Equal(a.Marshal(), b.Marshal())
}
