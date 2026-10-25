package worker

import (
	"bufio"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"unicode/utf8"
)

// maxOutputs is the most bytes of $OUTPUTS the worker reads: a job's
// outputs are capped at 1 MiB (docs/design.md, Outputs). They are counted
// as they are kept, each byte that isn't UTF-8 as the 3 of the U+FFFD
// that replaces it, so that what the worker reports keeps to the cap.
const maxOutputs = 1 << 20

// maxOutputsLine is how long a line of $OUTPUTS can be, newline aside, for
// format 1 to read the file: shorter than bufio.Scanner's longest token,
// with which ACTIVATE reads it.
const maxOutputsLine = bufio.MaxScanTokenSize - 1

// errOutputsTooLarge is what readOutputs returns for a file larger than
// maxOutputs, errOutputsLineTooLong for one with a line longer than
// maxOutputsLine, and errOutputsNotFile for a path a step put something
// other than a regular file at, such as a FIFO, which reading would wait
// on.
var (
	errOutputsTooLarge    = errors.New("worker: $OUTPUTS is too large")          //nolint:forbidigo // outputsWritten reports it as OutputsTooLarge
	errOutputsLineTooLong = errors.New("worker: a line of $OUTPUTS is too long") //nolint:forbidigo // outputsWritten reports it as OutputsLineTooLong
	errOutputsNotFile     = errors.New("worker: $OUTPUTS isn't a file")          //nolint:forbidigo // outputsWritten reports it as OutputsNotFile
)

// readOutputs reads the outputs a step wrote to the file named by $OUTPUTS,
// as format 1 reads them: each line that holds an =, as the name before
// its first = and the value after it, each with the white space around it
// trimmed, and the two characters \n, \t and \r in a value made a newline,
// a tab and a carriage return; a name written again has its last value.
// Each byte of a name or a value that isn't UTF-8 is replaced with U+FFFD,
// as format 1 stores its outputs, with encoding/json, which replaces it so:
// JSON, as the worker API's is, holds only UTF-8. It returns the names of
// the outputs it replaced a byte in too, as notUTF8.
// It reads the file name in the worker's directory d, or what a symbolic
// link there names, as format 1 follows it: the step's user can read it
// anyway. It returns the file's size too, as its bytes are kept
// (validLen), and errOutputsTooLarge for one larger than maxOutputs, which
// it doesn't read if the file's own size is. As format 1, it reads nothing
// of a file with a line longer than maxOutputsLine, as written, and
// returns errOutputsLineTooLong.
func readOutputs(d *privateDir, name string) (outputs map[string]string, notUTF8 map[string]bool, size int64, err error) {
	f, err := openOutputs(d, name)
	if err != nil {
		return nil, nil, 0, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, nil, 0, err
	}
	if !info.Mode().IsRegular() {
		return nil, nil, 0, errOutputsNotFile
	}
	if info.Size() > maxOutputs {
		return nil, nil, info.Size(), errOutputsTooLarge
	}
	data, err := io.ReadAll(io.LimitReader(f, maxOutputs+1))
	if err != nil {
		return nil, nil, 0, err
	}
	text := string(data)
	if size = int64(validLen(text)); size > maxOutputs {
		return nil, nil, size, errOutputsTooLarge
	}
	outputs, notUTF8, err = parseOutputs(text)
	return outputs, notUTF8, size, err
}

// openOutputs opens $OUTPUTS without waiting for a FIFO's writer: the
// file name in d, or what a symbolic link there names.
func openOutputs(d *privateDir, name string) (*os.File, error) {
	dir, root, _, err := d.current()
	if err != nil {
		return nil, err
	}
	info, err := root.Lstat(name)
	if err != nil {
		return nil, err
	}
	if info.Mode()&os.ModeSymlink == 0 {
		return root.OpenFile(name, os.O_RDONLY|openNoFollow|openNonBlock, 0)
	}
	target, err := root.Readlink(name)
	if err != nil {
		return nil, err
	}
	if !filepath.IsAbs(target) {
		target = filepath.Join(dir, target)
	}
	return os.OpenFile(target, os.O_RDONLY|openNonBlock, 0) //nolint:gosec // what a step's link names, which its user can read
}

var unescapeOutput = strings.NewReplacer(`\n`, "\n", `\t`, "\t", `\r`, "\r")

// parseOutputs reads text, what a step wrote to $OUTPUTS, as readOutputs
// says, and returns the outputs, and the names of those it replaced a byte
// that isn't UTF-8 in. Two names that differ only in such bytes are one
// once they are replaced, as a name written again.
func parseOutputs(text string) (outputs map[string]string, notUTF8 map[string]bool, err error) {
	outputs = map[string]string{}
	for line := range strings.Lines(text) {
		line = strings.TrimSuffix(line, "\n")
		if len(line) > maxOutputsLine {
			return nil, nil, errOutputsLineTooLong
		}
		line = strings.TrimSuffix(line, "\r")
		name, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		name, nameReplaced := validUTF8(strings.TrimSpace(name))
		value, valueReplaced := validUTF8(unescapeOutput.Replace(strings.TrimSpace(value)))
		outputs[name] = value
		if nameReplaced || valueReplaced {
			if notUTF8 == nil {
				notUTF8 = map[string]bool{}
			}
			notUTF8[name] = true
		} else {
			delete(notUTF8, name)
		}
	}
	return outputs, notUTF8, nil
}

// validUTF8 returns s with each byte of it that isn't UTF-8 replaced with
// U+FFFD, as encoding/json writes s, and reports whether it replaced one.
// strings.ToValidUTF8 would replace a run of such bytes with one U+FFFD,
// which format 1 doesn't.
func validUTF8(s string) (string, bool) {
	if utf8.ValidString(s) {
		return s, false
	}
	var b strings.Builder
	b.Grow(len(s))
	// Ranging over a string gives U+FFFD for each byte that isn't UTF-8.
	for _, r := range s {
		b.WriteRune(r)
	}
	return b.String(), true
}

// validLen returns how many bytes s is once validUTF8 has replaced its bytes
// that aren't UTF-8: 3 for each of them, U+FFFD's.
func validLen(s string) int {
	if utf8.ValidString(s) {
		return len(s)
	}
	n := 0
	for _, r := range s {
		n += utf8.RuneLen(r)
	}
	return n
}

// written returns what of after isn't in before: the outputs a step that
// shares its job's $OUTPUTS wrote, from what the file held before and after
// it ran.
func written(before, after map[string]string) map[string]string {
	out := map[string]string{}
	for name, value := range after {
		if old, ok := before[name]; !ok || old != value {
			out[name] = value
		}
	}
	return out
}
