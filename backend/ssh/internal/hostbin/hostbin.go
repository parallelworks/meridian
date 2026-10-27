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
// message of the day, can't be taken for, with each path in hex, so that
// one that holds a newline can't cut its line short.
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
	"path"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/parallelworks/foundation/problem"

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
	// user fetched: Dir/<version>/<os>-<arch>/meridian. A host of another
	// system needs it.
	Dir string
	// Self returns the binary for this process's own system, and its
	// digest: local.Executable, the file this process runs, for nil, which
	// it checks as the local backend checks the binary it starts. A
	// process that isn't meridian, as a test, names meridian's.
	Self func() (path, digest string, err error)

	// goos and goarch are this process's system: runtime's, for "". Tests
	// change them.
	goos, goarch string
}

// For returns the binary for a host of a system, as Go names it: this
// process's own, for its own system, checked as the local backend checks
// the binary it starts; and otherwise the one the user fetched for the
// system into Dir, if they did (problems.SSHWorkerBinaryMissing), checked
// the same way, without following a symbolic link: a regular file of the
// user's or root's that no other user can change, nor any directory on
// the way to it (problems.WorkerBinaryUnsafe). host names the host, for
// the problems.
func (b *Binaries) For(host, goos, goarch string) (Binary, error) {
	ownOS, ownArch := cmpOr(b.goos, runtime.GOOS), cmpOr(b.goarch, runtime.GOARCH)
	if goos == ownOS && goarch == ownArch {
		self := b.Self
		if self == nil {
			self = local.Executable
		}
		file, digest, err := self()
		return Binary{Path: file, Digest: digest}, err
	}
	if b.Dir == "" {
		return Binary{}, errors.New("hostbin: no directory of binaries for other systems")
	}
	file := filepath.Join(b.Dir, b.Version, goos+"-"+goarch, "meridian")
	info, err := os.Lstat(file)
	if errors.Is(err, os.ErrNotExist) {
		return Binary{}, problems.SSHWorkerBinaryMissing.Problem(problems.Params{"host": host, "os": goos, "arch": goarch, "version": b.Version, "path": file})
	}
	if err != nil {
		return Binary{}, err
	}
	unsafe := func(code problem.Code) error {
		return problems.WorkerBinaryUnsafe.Problem(problems.Params{"path": file, "code": string(code)})
	}
	if !info.Mode().IsRegular() {
		return Binary{}, unsafe(problems.FileNotRegular.Code())
	}
	f, _, err := safepath.OpenExecutable(file, file)
	if err != nil {
		if p, ok := errors.AsType[*problem.Problem](err); ok {
			return Binary{}, unsafe(p.Code)
		}
		return Binary{}, err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return Binary{}, err
	}
	return Binary{Path: file, Digest: "sha256:" + hex.EncodeToString(h.Sum(nil))}, nil
}

func cmpOr(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

// A Host is what Install found of a host: its system, as uname -s and -m
// say it and as Go names it, and its cache of binaries.
type Host struct {
	OS, Arch     string
	GOOS, GOARCH string
	Cache        string
}

// hostPath is the PATH every script runs with: the system's own
// directories, so that what it runs, the tool that computes a digest among
// it, is the system's, and not what a directory the user's PATH names
// first holds, which another user could change. Tests change it.
var hostPath = "/usr/bin:/bin:/usr/sbin:/sbin"

// prelude starts every script: hostPath; hx, which writes its argument in
// hex, as one word, so that a path, whatever it holds, passes on one line
// as one field; no, which
// says why a directory can't be used, as a problem's code, with the
// directory and where on the way to it, and ends the script; and the
// user's ID.
func prelude() string {
	return `PATH=` + hostPath + `; export PATH; ` +
		`hx() { set -- $(printf "%s" "$1" | od -An -v -tx1); _h=; for b; do _h="$_h$b"; done; echo "$_h"; }; ` +
		`no() { printf "MERIDIAN-UNUSABLE %s %s %s\n" "$1" "$(hx "$2")" "$(hx "$3")"; exit 0; }; ` +
		`u=$(id -u); `
}

// cacheScript sets c to the host's cache of binaries, as meridian finds its
// cache directory there (cmd/meridian/internal/local.Locate), and its bin/:
// MERIDIAN_CACHE_DIR, which must be absolute, or on macOS
// ~/Library/Caches/Meridian, or else $XDG_CACHE_HOME/meridian, if it is
// absolute, or ~/.cache/meridian, where HOME must be absolute. It says
// which of those isn't, and ends the script.
const cacheScript = `s=$(uname -s); x=; case "$XDG_CACHE_HOME" in /*) x=1;; esac; ` +
	`if [ -n "$MERIDIAN_CACHE_DIR" ]; then case "$MERIDIAN_CACHE_DIR" in /*) c="$MERIDIAN_CACHE_DIR";; ` +
	`*) printf "MERIDIAN-RELATIVE %s\n" "$(hx "$MERIDIAN_CACHE_DIR")"; exit 0;; esac; ` +
	`elif [ "$s" != Darwin ] && [ -n "$x" ]; then c="$XDG_CACHE_HOME/meridian"; ` +
	`else case "$HOME" in /*) ;; *) printf "MERIDIAN-NOHOME\n"; exit 0;; esac; ` +
	`if [ "$s" = Darwin ]; then c="$HOME/Library/Caches/Meridian"; else c="$HOME/.cache/meridian"; fi; fi; ` +
	`c="$c/bin"; `

// wayScript defines way, which checks the directory it is given, which
// must be there, and the way to it, as the physical path p, which it sets,
// from /: each directory on the way the user's or root's, which no other
// user can change, unless it is sticky, as /tmp is; and the cache of
// binaries, c, and each directory in it, the user's own, which no other
// user can change, sticky or not. Otherwise it says why, as path_unsafe or
// directory_not_private, and ends the script. Another user who could
// change one could swap the binary for one of theirs, which the user's
// workers would run, with their tokens.
const wayScript = `ok() { test -n "$(find "$1" -prune \( -user "$u" -o -user 0 \) \( -perm -1000 -o \( ! -perm -g=w ! -perm -o=w \) \) -print)"; }; ` +
	`own() { test -n "$(find "$1" -prune -user "$u" ! -perm -g=w ! -perm -o=w -print)"; }; ` +
	`way() { p=$(cd "$1" && pwd -P) || no directory_unavailable "$1" "$1"; q=; ` +
	`if [ -d "$c" ]; then q=$(cd "$c" && pwd -P) || no directory_unavailable "$1" "$c"; fi; ` +
	`ok / || no path_unsafe "$1" /; x=; r="${p#/}"; ` +
	`while [ -n "$r" ]; do n="${r%%/*}"; if [ "$n" = "$r" ]; then r=; else r="${r#*/}"; fi; x="$x/$n"; ` +
	`m=; if [ -n "$q" ]; then case "$x/" in "$q"/*) m=1;; esac; fi; ` +
	`if [ -n "$m" ]; then own "$x" || no directory_not_private "$1" "$x"; else ok "$x" || no path_unsafe "$1" "$x"; fi; done; }; `

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
// and returns where it is there, as a physical path: the host's system,
// from uname, which must be one the worker runs on
// (problems.SSHHostUnsupported), finds the binary to copy (Binaries.For).
//
// The cache must be the user's alone, and the way to it, which only the
// user or root can change (problems.SSHHostDirUnusable): it is checked
// before a binary there is taken, which must be a regular file of the
// user's that no other user can change, and before anything is written
// there. The copy is written to a new file of its own there, which can't
// be one another user made, nor a link they made, checked against the
// binary's digest, and only then renamed into place, so a run that stops
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
	// Already there, whole: as a host that shares its home with another
	// that got it has it.
	out, err := run(ctx, conn, prelude()+cacheScript+wayScript+`d="$c/`+hexDigest+`"; f="$d/meridian"; `+
		`if [ -e "$f" ] || [ -L "$f" ]; then way "$d"; `+
		`if [ -L "$f" ] || [ ! -f "$f" ]; then no file_not_regular "$d" "$f"; fi; `+
		`test -n "$(find "$f" -prune -user "$u" -print)" || no file_not_owned "$d" "$f"; `+
		`own "$f" || no file_changeable "$d" "$f"; `+
		hashScript+`printf "MERIDIAN-DIGEST %s %s\n" "$h" "$(hx "$p/meridian")"; fi`, nil)
	if err != nil {
		return Binary{}, h, err
	}
	if err := saidUnusable(out, host); err != nil {
		return Binary{}, h, err
	}
	if found, at, err := digestSaid(out); err != nil {
		return Binary{}, h, err
	} else if found == hexDigest {
		h.Cache = path.Dir(path.Dir(at))
		return Binary{Path: at, Digest: local.Digest}, h, nil
	}
	f, err := os.Open(local.Path) //nolint:gosec // the binary For found, which it checked
	if err != nil {
		return Binary{}, h, err
	}
	defer f.Close()
	// What of the cache is there is checked before mkdir makes the rest,
	// and all of it after. The new file is made with noclobber, which
	// doesn't open one that is there, in a directory way checked that no
	// other user can change; and it is removed if the script stops before
	// it is renamed.
	out, err = run(ctx, conn, prelude()+cacheScript+wayScript+`set -e; umask 077; d="$c/`+hexDigest+`"; `+
		`e="$d"; while [ -n "$e" ] && [ ! -d "$e" ]; do e="${e%/*}"; done; way "${e:-/}"; mkdir -p "$d"; way "$d"; `+
		`f="$p/.meridian.$$"; trap "rm -f \"\$f\"" 0; trap "exit 1" 1 2 13 15; set -C; cat > "$f"; set +C; `+
		hashScript+`if [ "$h" = "`+hexDigest+`" ]; then chmod 700 "$f"; mv -f "$f" "$p/meridian"; printf "MERIDIAN-INSTALLED %s\n" "$(hx "$p/meridian")"; `+
		`else rm -f "$f"; printf "MERIDIAN-DIGEST %s\n" "$h"; fi`, f)
	if err != nil {
		return Binary{}, h, err
	}
	if err := saidUnusable(out, host); err != nil {
		return Binary{}, h, err
	}
	installed, ok := marked(out, "MERIDIAN-INSTALLED")
	if !ok {
		found, _ := marked(out, "MERIDIAN-DIGEST")
		if found != "none" {
			found = "sha256:" + found
		}
		return Binary{}, h, problems.SSHWorkerBinaryUnverified.Problem(problems.Params{
			"host": host, "path": h.Cache + "/" + hexDigest + "/meridian", "digest": local.Digest, "found": found,
		})
	}
	at, err := unhex(installed)
	if err != nil {
		return Binary{}, h, err
	}
	h.Cache = path.Dir(path.Dir(at))
	return Binary{Path: at, Digest: local.Digest}, h, nil
}

// probe asks the host its system and its cache of binaries. A cache
// variable it can't use is a problem, as meridian's own there would be:
// problems.DirectoryVariableRelative or problems.HomeUnknown.
func probe(ctx context.Context, conn *sshconn.Conn, host string) (*Host, error) {
	out, err := run(ctx, conn, prelude()+cacheScript+`printf "MERIDIAN-HOST %s %s %s\n" "$s" "$(uname -m)" "$(hx "$c")"`, nil)
	if err != nil {
		return nil, err
	}
	if value, ok := marked(out, "MERIDIAN-RELATIVE"); ok {
		v, err := unhex(value)
		if err != nil {
			return nil, err
		}
		return nil, problems.DirectoryVariableRelative.Problem(problems.Params{"variable": "MERIDIAN_CACHE_DIR", "value": v})
	}
	if _, ok := marked(out, "MERIDIAN-NOHOME"); ok {
		return nil, problems.HomeUnknown.Problem(problems.Params{"variable": "MERIDIAN_CACHE_DIR"})
	}
	said, _ := marked(out, "MERIDIAN-HOST")
	fields := strings.SplitN(said, " ", 3)
	if len(fields) != 3 {
		return nil, fmt.Errorf("hostbin: %s didn't say its system: %q", host, out)
	}
	h := &Host{OS: fields[0], Arch: fields[1]}
	if h.Cache, err = unhex(fields[2]); err != nil || !strings.HasPrefix(h.Cache, "/") {
		return nil, fmt.Errorf("hostbin: %s didn't say its cache: %q", host, out)
	}
	// Cleaned, as meridian's own is. The host's paths are a POSIX
	// system's, whatever this one's are.
	h.Cache = path.Clean(h.Cache)
	var ok bool
	if h.GOOS, h.GOARCH, ok = system(h.OS, h.Arch); !ok {
		return nil, problems.SSHHostUnsupported.Problem(problems.Params{"host": host, "os": h.OS, "arch": h.Arch})
	}
	return h, nil
}

// saidUnusable returns the problem with a directory a script said it
// can't use, or nil if it said none.
func saidUnusable(out, host string) error {
	said, ok := marked(out, "MERIDIAN-UNUSABLE")
	if !ok {
		return nil
	}
	fields := strings.SplitN(said, " ", 3)
	if len(fields) != 3 {
		return fmt.Errorf("hostbin: %s said a directory can't be used, but not which: %q", host, said)
	}
	dir, err := unhex(fields[1])
	if err != nil {
		return err
	}
	at, err := unhex(fields[2])
	if err != nil {
		return err
	}
	return problems.SSHHostDirUnusable.Problem(problems.Params{"host": host, "path": dir, "at": at, "reason": fields[0]})
}

// digestSaid returns the digest the script said the binary in the cache
// has, and where it is, or "" if it said none.
func digestSaid(out string) (digest, at string, err error) {
	said, ok := marked(out, "MERIDIAN-DIGEST")
	if !ok {
		return "", "", nil
	}
	digest, rest, _ := strings.Cut(said, " ")
	at, err = unhex(rest)
	return digest, at, err
}

// unhex reads what hx wrote: bytes in hex, with spaces between them.
func unhex(s string) (string, error) {
	b, err := hex.DecodeString(strings.ReplaceAll(s, " ", ""))
	if err != nil {
		return "", fmt.Errorf("hostbin: %q isn't hex: %w", s, err)
	}
	return string(b), nil
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
	s.Buffer(nil, 1<<20)
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
