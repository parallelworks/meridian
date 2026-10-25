package worker

import (
	"bufio"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/parallelworks/meridian/problems"
	"github.com/parallelworks/meridian/workerapi"
)

// TestReadEnv checks how a worker reads where its coordinator is, and its
// bootstrap token, from its environment, and that its steps get none of
// it.
func TestReadEnv(t *testing.T) {
	t.Parallel()
	key := "sha256:" + strings.Repeat("ab", 32)
	for _, tc := range []struct {
		name    string
		environ []string
		address string
		token   string
		tls     bool
		// fails is the kind of problem readEnv reports, if it fails.
		fails *problems.Kind
	}{
		{name: "socket", environ: []string{"MERIDIAN_COORDINATOR=/run/c.sock", "MERIDIAN_BOOTSTRAP_TOKEN=t"}, address: "/run/c.sock", token: "t"},
		{name: "tls", environ: []string{"MERIDIAN_COORDINATOR=login1:7443", "MERIDIAN_COORDINATOR_KEY_SHA256=" + key, "MERIDIAN_BOOTSTRAP_TOKEN=t"}, address: "login1:7443", token: "t", tls: true},
		{name: "last wins", environ: []string{"MERIDIAN_COORDINATOR=/a", "MERIDIAN_BOOTSTRAP_TOKEN=t", "MERIDIAN_COORDINATOR=/b"}, address: "/b", token: "t"},
		{name: "no coordinator", environ: []string{"MERIDIAN_BOOTSTRAP_TOKEN=t"}, fails: problems.WorkerEnvironmentMissing},
		{name: "tls without a key", environ: []string{"MERIDIAN_COORDINATOR=login1:7443", "MERIDIAN_BOOTSTRAP_TOKEN=t"}, fails: problems.WorkerEnvironmentInvalid},
		{name: "a key that isn't sha256", environ: []string{"MERIDIAN_COORDINATOR=login1:7443", "MERIDIAN_COORDINATOR_KEY_SHA256=" + strings.Repeat("ab", 32), "MERIDIAN_BOOTSTRAP_TOKEN=t"}, fails: problems.WorkerEnvironmentInvalid},
		{name: "a short key", environ: []string{"MERIDIAN_COORDINATOR=login1:7443", "MERIDIAN_COORDINATOR_KEY_SHA256=sha256:abab", "MERIDIAN_BOOTSTRAP_TOKEN=t"}, fails: problems.WorkerEnvironmentInvalid},
		{name: "a key for a socket", environ: []string{"MERIDIAN_COORDINATOR=/run/c.sock", "MERIDIAN_COORDINATOR_KEY_SHA256=" + key, "MERIDIAN_BOOTSTRAP_TOKEN=t"}, fails: problems.WorkerEnvironmentInvalid},
		{name: "no token", environ: []string{"MERIDIAN_COORDINATOR=/run/c.sock"}, fails: problems.WorkerEnvironmentMissing},
		{name: "two tokens", environ: []string{"MERIDIAN_COORDINATOR=/run/c.sock", "MERIDIAN_BOOTSTRAP_TOKEN=t", "MERIDIAN_BOOTSTRAP_TOKEN_FILE=/x"}, fails: problems.WorkerEnvironmentInvalid},
	} {
		environ := append(slices.Clone(tc.environ), "PATH=/bin", "HOME=/home/u")
		ep, rest, err := readEnv(environ)
		if tc.fails != nil {
			if problemOf(err, tc.fails) == nil {
				t.Errorf("%s: got %v, want %s", tc.name, err, tc.fails.Code())
			}
			continue
		}
		if err != nil {
			t.Errorf("%s: %v", tc.name, err)
			continue
		}
		if ep.address != tc.address || ep.token != tc.token || ep.socket() == tc.tls || (len(ep.keyHash) == 32) != tc.tls {
			t.Errorf("%s: got %+v", tc.name, ep)
		}
		if tc.tls && hex.EncodeToString(ep.keyHash) != strings.Repeat("ab", 32) {
			t.Errorf("%s: the key's hash is %x", tc.name, ep.keyHash)
		}
		if want := []string{"PATH=/bin", "HOME=/home/u"}; !reflect.DeepEqual(rest, want) {
			t.Errorf("%s: the steps get %v, want %v", tc.name, rest, want)
		}
	}
}

// TestTokenFile checks that a worker reads its bootstrap token from a file
// only its user can read, and removes it, and refuses one others can read,
// or a symbolic link.
func TestTokenFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	private, shared := filepath.Join(dir, "private"), filepath.Join(dir, "shared")
	if err := os.WriteFile(private, []byte("token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(shared, []byte("token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(shared, 0o640); err != nil { //nolint:gosec // a file others can read, which the worker refuses
		t.Fatal(err)
	}
	ep, _, err := readEnv([]string{"MERIDIAN_COORDINATOR=/c.sock", workerapi.EnvBootstrapTokenFile + "=" + private})
	if err != nil || ep.token != "token" {
		t.Fatalf("got %q, %v", ep.token, err)
	}
	if _, err := os.Stat(private); !os.IsNotExist(err) {
		t.Errorf("the token's file is still there: %v", err)
	}
	if _, _, err := readEnv([]string{"MERIDIAN_COORDINATOR=/c.sock", workerapi.EnvBootstrapTokenFile + "=" + shared}); problemOf(err, problems.FileNotPrivate) == nil {
		t.Errorf("read a token from a file others can read: %v", err)
	}
	// Nor through a symbolic link, which could name another's file.
	target, link := filepath.Join(dir, "target"), filepath.Join(dir, "link")
	if err := os.WriteFile(target, []byte("token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if _, _, err := readEnv([]string{"MERIDIAN_COORDINATOR=/c.sock", workerapi.EnvBootstrapTokenFile + "=" + link}); problemOf(err, problems.FileNotRegular) == nil {
		t.Errorf("read a token through a symbolic link: %v", err)
	}
}

// TestCredential checks that each credential is 32 random bytes, as hex.
func TestCredential(t *testing.T) {
	t.Parallel()
	a, b := newCredential(), newCredential()
	if len(a) != 64 || a == b {
		t.Errorf("credentials %q and %q", a, b)
	}
	if _, err := hex.DecodeString(a); err != nil {
		t.Error(err)
	}
}

// TestParseOutputs checks that $OUTPUTS is read as format 1 reads it.
func TestParseOutputs(t *testing.T) {
	t.Parallel()
	got, notUTF8, err := parseOutputs(" a = 1 \nb=x=y\nno equals\nc=line\\nnext\\ttab\r\na=2\n=empty\nlast=no newline")
	want := map[string]string{"a": "2", "b": "x=y", "c": "line\nnext\ttab", "": "empty", "last": "no newline"}
	if err != nil || notUTF8 != nil || !reflect.DeepEqual(got, want) {
		t.Errorf("got %q, %v and %v, want %q", got, notUTF8, err, want)
	}
	// A file with a line too long for bufio.Scanner, with which format 1
	// reads it, isn't read, as in format 1.
	for _, n := range []int{bufio.MaxScanTokenSize - 3, bufio.MaxScanTokenSize - 2, bufio.MaxScanTokenSize - 1} {
		for _, text := range []string{"b=" + strings.Repeat("v", n), "a=1\nb=" + strings.Repeat("v", n) + "\n", "b=" + strings.Repeat("v", n) + "\r\n"} {
			scanner := bufio.NewScanner(strings.NewReader(text))
			for scanner.Scan() {
			}
			got, _, err := parseOutputs(text)
			if (err == nil) != (scanner.Err() == nil) || err == nil && len(got["b"]) < n {
				t.Errorf("a file of %d bytes: got %d outputs and %v; bufio.Scanner: %v", len(text), len(got), err, scanner.Err())
			}
		}
	}
	if got := written(map[string]string{"a": "1", "b": "2"}, map[string]string{"a": "1", "b": "3", "c": "4"}); !reflect.DeepEqual(got, map[string]string{"b": "3", "c": "4"}) {
		t.Errorf("written: got %v", got)
	}
}

// TestParseOutputsNotUTF8 checks that each byte of $OUTPUTS that isn't
// UTF-8, in a name or a value, is replaced with U+FFFD, as encoding/json,
// with which format 1 stores outputs, replaces it, and that the outputs it
// was replaced in are named, as the last line that sets each left it. A
// line's length is its bytes as written, as format 1's bufio.Scanner
// counts them.
func TestParseOutputsNotUTF8(t *testing.T) {
	t.Parallel()
	got, notUTF8, err := parseOutputs("a\xff = 1\nb= x\xe2\x82y \xa0\nc=ok\nd=\xff\nd=fixed\ne=fixed\ne=\xed\xa0\x80\nf=\xef\xbf\xbd\nb\xfe=two\n")
	want := map[string]string{
		"a\uFFFD": "1", "b": "x\uFFFD\uFFFDy \uFFFD", "c": "ok", "d": "fixed", "e": "\uFFFD\uFFFD\uFFFD", "f": "\uFFFD", "b\uFFFD": "two",
	}
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Errorf("got %q and %v, want %q", got, err, want)
	}
	if want := map[string]bool{"a\uFFFD": true, "b": true, "e": true, "b\uFFFD": true}; !reflect.DeepEqual(notUTF8, want) {
		t.Errorf("the outputs not UTF-8 are %v, want %v", notUTF8, want)
	}
	// Two names that differ only in bytes that aren't UTF-8 are one.
	if got, _, _ := parseOutputs("n\xff=1\nn\xfe=2\n"); !reflect.DeepEqual(got, map[string]string{"n\uFFFD": "2"}) {
		t.Errorf("got %q", got)
	}
	for _, s := range []string{"", "ok", "\uFFFD", "\xff\xfe", "x\xe2\x82y", "\xed\xa0\x80", "\xf4\x90\x80\x80", "\xc0\xaf", "é\xe9"} {
		data, err := json.Marshal(s)
		if err != nil {
			t.Fatal(err)
		}
		var stored string
		if err := json.Unmarshal(data, &stored); err != nil {
			t.Fatal(err)
		}
		if got, replaced := validUTF8(s); got != stored || replaced != (s != stored) {
			t.Errorf("validUTF8(%q) = %q, %t; encoding/json stores %q", s, got, replaced, stored)
		}
	}
	line := "v=" + strings.Repeat("\xff", maxOutputsLine-2)
	if got, _, err := parseOutputs(line); err != nil || got["v"] != strings.Repeat("\uFFFD", maxOutputsLine-2) {
		t.Errorf("a line of %d bytes not UTF-8: got %d bytes and %v", len(line), len(got["v"]), err)
	}
}

// TestShellCommand checks how a step's shell: runs its script: {0} is the
// script's path, which comes last without one, and format 1's default is
// a bash that isn't a login shell.
func TestShellCommand(t *testing.T) {
	t.Parallel()
	const script = "/tmp/a dir/script.sh"
	for _, tc := range []struct {
		shell string
		want  []string
	}{
		{"", []string{"/bin/bash", script}},
		{"bash -l {0}", []string{"bash", "-l", script}},
		{"sh -e", []string{"sh", "-e", script}},
		{"/usr/bin/bash --noprofile --norc -eo pipefail {0}", []string{"/usr/bin/bash", "--noprofile", "--norc", "-eo", "pipefail", script}},
	} {
		if got := shellCommand(tc.shell, script).Args; !slices.Equal(got, tc.want) {
			t.Errorf("%q: got %q, want %q", tc.shell, got, tc.want)
		}
	}
}
