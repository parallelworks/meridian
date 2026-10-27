// Package hostbin puts the worker's binary on a host: the binary for the
// host's system, copied over an SSH connection into the host's cache of
// binaries, by its digest, once, and checked there against its digest
// before it is kept, so that meridian host and the workers it starts run
// it (docs/design.md, The ssh backend).
//
// It talks to the host through exec sessions whose commands are /bin/sh
// -c and one line of script, with no single quote in it, so that a user's
// login shell, which reads the command first, csh among them, passes it to
// sh as it is. What the script prints that matters is on lines of its own
// that start with MERIDIAN-, which a login script's own output, as a
// message of the day, can't be taken for.
package hostbin

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/parallelworks/meridian/backend/local"
	"github.com/parallelworks/meridian/backend/ssh/internal/sshconn"
	"github.com/parallelworks/meridian/internal/safepath"
	"github.com/parallelworks/meridian/problems"
)

// A Binary is a meridian binary, where it is, and its digest, as sha256:
// and hex.
type Binary struct {
	Path   string
	Digest string
}

// Binaries find the binary to copy to a host of a system.
type Binaries struct {
	// Version is this release's, of which the binary for another system
	// must be.
	Version string
	// Dir holds binaries for systems other than this process's, which the
	// user fetched: Dir/<version>/<os>-<arch>/meridian, in directories only
	// the user can open. "" is none.
	Dir string

	// self returns the file this process runs, and its digest, and goos
	// and goarch are its system: local.Executable's, runtime's, for "".
	// Tests change them.
	self         func() (string, string, error)
	goos, goarch string
}

// For returns the binary for a host of a system, as Go names it: this
// process's own, for its own system, checked as the local backend checks
// the binary it starts (problems.WorkerBinaryUnsafe); and otherwise the
// one the user fetched for the system into Dir, if they did
// (problems.SSHWorkerBinaryMissing), whose way only the user can change.
// host names the host, for the problem.
func (b *Binaries) For(host, goos, goarch string) (Binary, error) {
	ownOS, ownArch := cmpOr(b.goos, runtime.GOOS), cmpOr(b.goarch, runtime.GOARCH)
	if goos == ownOS && goarch == ownArch {
		self := b.self
		if self == nil {
			self = local.Executable
		}
		path, digest, err := self()
		return Binary{Path: path, Digest: digest}, err
	}
	path := ""
	if b.Dir != "" {
		path = filepath.Join(b.Dir, b.Version, goos+"-"+goarch, "meridian")
	}
	missing := problems.SSHWorkerBinaryMissing.Problem(problems.Params{"host": host, "os": goos, "arch": goarch, "version": b.Version, "path": path})
	if path == "" {
		return Binary{}, missing
	}
	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return Binary{}, missing
	}
	if err != nil {
		return Binary{}, err
	}
	if !info.Mode().IsRegular() {
		return Binary{}, missing
	}
	if err := safepath.Check(filepath.Dir(path), true); err != nil {
		return Binary{}, err
	}
	digest, err := fileDigest(path)
	return Binary{Path: path, Digest: digest}, err
}

func cmpOr(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

// fileDigest returns a file's digest, as sha256: and hex.
func fileDigest(path string) (string, error) {
	f, err := os.Open(path) //nolint:gosec // a binary in Dir, whose way only the user can change
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return "sha256:" + hex.EncodeToString(h.Sum(nil)), nil
}

// A Host is what Install found of a host: its system, as uname -s and -m
// say it and as Go names it, and its cache of binaries.
type Host struct {
	OS, Arch     string
	GOOS, GOARCH string
	Cache        string
}

// cacheScript sets c to the host's cache of binaries, as meridian finds its
// cache directory there (cmd/meridian/internal/local.Locate), and its bin/:
// MERIDIAN_CACHE_DIR, or on macOS ~/Library/Caches/Meridian, or else
// $XDG_CACHE_HOME/meridian, if it is absolute, or ~/.cache/meridian.
const cacheScript = `s=$(uname -s); ` +
	`if [ -n "$MERIDIAN_CACHE_DIR" ]; then c="$MERIDIAN_CACHE_DIR"; ` +
	`elif [ "$s" = Darwin ]; then c="$HOME/Library/Caches/Meridian"; ` +
	`else case "$XDG_CACHE_HOME" in /*) c="$XDG_CACHE_HOME/meridian";; *) c="$HOME/.cache/meridian";; esac; fi; ` +
	`c="$c/bin"; `

// hashScript sets h to the SHA-256 of the file "$f", in hex, with the
// first of the tools that the systems the worker runs on have, or to
// none if the host has none of them.
const hashScript = `if command -v sha256sum >/dev/null 2>&1; then h=$(sha256sum < "$f"); ` +
	`elif command -v shasum >/dev/null 2>&1; then h=$(shasum -a 256 < "$f"); ` +
	`elif command -v sha256 >/dev/null 2>&1; then h=$(sha256 -q < "$f"); ` +
	`elif command -v openssl >/dev/null 2>&1; then h=$(openssl dgst -sha256 < "$f"); h=${h##* }; ` +
	`else h=none; fi; h=${h%% *}; `

// command is the exec command that runs script with sh, which mustn't hold
// a single quote, and so passes through any login shell as it is.
func command(script string) string {
	if strings.Contains(script, "'") {
		panic("hostbin: a script with a single quote: " + script) //nolint:forbidigo // the scripts are this package's own, with only a hex digest put in: a quote is a bug here
	}
	return "/bin/sh -c '" + script + "'"
}

// Install puts the binary for host's system in its cache of binaries, by
// its digest, as its cache's <hex>/meridian, if it isn't there already,
// and returns where it is there: the host's system, from uname, which
// must be one the worker runs on (problems.SSHHostUnsupported), finds the
// binary to copy (Binaries.For). The copy is written to a file of its own
// in a directory only the user can open, checked against the binary's
// digest there, and only then renamed into place, so a run that stops
// halfway, or two at once, leave no binary that isn't whole; one that
// doesn't match is removed, and is problems.SSHWorkerBinaryUnverified, as
// is a host that has no tool to check one with. host names the host, for
// the problems.
func Install(ctx context.Context, conn *sshconn.Conn, host string, b *Binaries) (Binary, *Host, error) {
	h, err := probe(ctx, conn, host)
	if err != nil {
		return Binary{}, nil, err
	}
	local, err := b.For(host, h.GOOS, h.GOARCH)
	if err != nil {
		return Binary{}, h, err
	}
	hexDigest, ok := strings.CutPrefix(local.Digest, "sha256:")
	if !ok || hexDigest == "" || strings.Trim(hexDigest, "0123456789abcdef") != "" {
		return Binary{}, h, fmt.Errorf("hostbin: %s isn't a digest", local.Digest)
	}
	remote := h.Cache + "/" + hexDigest + "/meridian"
	// Already there, whole: as a host that shares its home with another
	// that got it has it.
	out, err := run(ctx, conn, cacheScript+`f="$c/`+hexDigest+`/meridian"; if [ -f "$f" ]; then `+hashScript+`printf "MERIDIAN-DIGEST %s\n" "$h"; fi`, nil)
	if err != nil {
		return Binary{}, h, err
	}
	if found, _ := marked(out, "MERIDIAN-DIGEST"); found == hexDigest {
		return Binary{Path: remote, Digest: local.Digest}, h, nil
	}
	f, err := os.Open(local.Path) //nolint:gosec // the binary For found, which it checked
	if err != nil {
		return Binary{}, h, err
	}
	defer f.Close()
	out, err = run(ctx, conn, cacheScript+`set -e; umask 077; d="$c/`+hexDigest+`"; mkdir -p "$d"; f="$d/.meridian.$$"; cat > "$f"; `+
		hashScript+`if [ "$h" = "`+hexDigest+`" ]; then chmod 700 "$f"; mv -f "$f" "$d/meridian"; printf "MERIDIAN-INSTALLED\n"; `+
		`else rm -f "$f"; printf "MERIDIAN-DIGEST %s\n" "$h"; fi`, f)
	if err != nil {
		return Binary{}, h, err
	}
	if _, installed := marked(out, "MERIDIAN-INSTALLED"); !installed {
		found, _ := marked(out, "MERIDIAN-DIGEST")
		if found != "none" {
			found = "sha256:" + found
		}
		return Binary{}, h, problems.SSHWorkerBinaryUnverified.Problem(problems.Params{"host": host, "path": remote, "digest": local.Digest, "found": found})
	}
	return Binary{Path: remote, Digest: local.Digest}, h, nil
}

// probe asks the host its system and its cache of binaries.
func probe(ctx context.Context, conn *sshconn.Conn, host string) (*Host, error) {
	out, err := run(ctx, conn, cacheScript+`printf "MERIDIAN-HOST %s %s %s\n" "$s" "$(uname -m)" "$c"`, nil)
	if err != nil {
		return nil, err
	}
	said, _ := marked(out, "MERIDIAN-HOST")
	fields := strings.SplitN(said, " ", 3)
	if len(fields) != 3 || !strings.HasPrefix(fields[2], "/") {
		return nil, fmt.Errorf("hostbin: %s didn't say its system: %q", host, out)
	}
	h := &Host{OS: fields[0], Arch: fields[1], Cache: fields[2]}
	var ok bool
	if h.GOOS, h.GOARCH, ok = system(h.OS, h.Arch); !ok {
		return nil, problems.SSHHostUnsupported.Problem(problems.Params{"host": host, "os": h.OS, "arch": h.Arch})
	}
	return h, nil
}

// system returns Go's names for a system uname names, and false for one
// the worker doesn't run on: other than Linux, macOS and FreeBSD.
func system(os, machine string) (goos, goarch string, ok bool) {
	switch os {
	case "Linux":
		goos = "linux"
	case "Darwin":
		goos = "darwin"
	case "FreeBSD":
		goos = "freebsd"
	default:
		return "", "", false
	}
	switch machine {
	case "x86_64", "amd64":
		goarch = "amd64"
	case "aarch64", "arm64":
		goarch = "arm64"
	case "i386", "i486", "i586", "i686":
		goarch = "386"
	case "armv6l", "armv7l", "armv7", "arm":
		goarch = "arm"
	default:
		return "", "", false
	}
	return goos, goarch, true
}

// marked returns what follows marker on the last line of out that starts
// with it, alone or before a space, and false if no line does.
func marked(out, marker string) (string, bool) {
	var last string
	found := false
	s := bufio.NewScanner(strings.NewReader(out))
	for s.Scan() {
		if rest, ok := strings.CutPrefix(s.Text(), marker); ok && (rest == "" || rest[0] == ' ') {
			last, found = strings.TrimPrefix(rest, " "), true
		}
	}
	return last, found
}

// run runs script on conn, with stdin, if it isn't nil, as its standard
// input, and returns what it wrote to its standard output. One that fails
// is an error with what it wrote to its standard error.
func run(ctx context.Context, conn *sshconn.Conn, script string, stdin io.Reader) (string, error) {
	s, err := conn.NewSession(ctx)
	if err != nil {
		return "", err
	}
	defer s.Close()
	var stdout, stderr strings.Builder
	s.Stdout, s.Stderr = &stdout, &stderr
	if stdin != nil {
		s.Stdin = stdin
	}
	done := make(chan error, 1)
	go func() { done <- s.Run(command(script)) }()
	select {
	case err = <-done:
	case <-ctx.Done():
		_ = s.Close()
		<-done
		return "", ctx.Err()
	}
	if err != nil {
		return "", fmt.Errorf("hostbin: %w: %s", err, strings.TrimSpace(stderr.String()))
	}
	return stdout.String(), nil
}
