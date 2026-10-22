package worker

import (
	"bufio"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// maxOutputs is the most bytes of $OUTPUTS the worker reads: a job's
// outputs are capped at 1 MiB (docs/design.md, Outputs).
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
	errOutputsTooLarge    = errors.New("worker: $OUTPUTS is too large")
	errOutputsLineTooLong = errors.New("worker: a line of $OUTPUTS is too long")
	errOutputsNotFile     = errors.New("worker: $OUTPUTS isn't a file")
)

// readOutputs reads the outputs a step wrote to the file named by $OUTPUTS,
// as format 1 reads them: each line that holds an =, as the name before
// its first = and the value after it, each with the white space around it
// trimmed, and the two characters \n, \t and \r in a value made a newline,
// a tab and a carriage return; a name written again has its last value.
// It reads the file name in the worker's directory d, or what a symbolic
// link there names, as format 1 follows it: the step's user can read it
// anyway. It returns the file's size too, and errOutputsTooLarge, without
// reading it, for one larger than maxOutputs. As format 1, it reads nothing
// of a file with a line longer than maxOutputsLine, and returns
// errOutputsLineTooLong.
func readOutputs(d *privateDir, name string) (map[string]string, int64, error) {
	f, err := openOutputs(d, name)
	if err != nil {
		return nil, 0, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, 0, err
	}
	if !info.Mode().IsRegular() {
		return nil, 0, errOutputsNotFile
	}
	if info.Size() > maxOutputs {
		return nil, info.Size(), errOutputsTooLarge
	}
	data, err := io.ReadAll(io.LimitReader(f, maxOutputs+1))
	if err != nil {
		return nil, 0, err
	}
	if len(data) > maxOutputs {
		return nil, int64(len(data)), errOutputsTooLarge
	}
	outputs, err := parseOutputs(string(data))
	return outputs, int64(len(data)), err
}

// openOutputs opens $OUTPUTS without waiting for a FIFO's writer: the
// file name in d, or what a symbolic link there names.
func openOutputs(d *privateDir, name string) (*os.File, error) {
	dir, root, err := d.current()
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

func parseOutputs(text string) (map[string]string, error) {
	outputs := map[string]string{}
	for line := range strings.Lines(text) {
		line = strings.TrimSuffix(line, "\n")
		if len(line) > maxOutputsLine {
			return nil, errOutputsLineTooLong
		}
		line = strings.TrimSuffix(line, "\r")
		name, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		outputs[strings.TrimSpace(name)] = unescapeOutput.Replace(strings.TrimSpace(value))
	}
	return outputs, nil
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
