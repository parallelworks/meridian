package worker

import (
	"bufio"
	"errors"
	"io"
	"os"
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
// maxOutputs, and errOutputsLineTooLong for one with a line longer than
// maxOutputsLine.
var (
	errOutputsTooLarge    = errors.New("worker: $OUTPUTS is too large")
	errOutputsLineTooLong = errors.New("worker: a line of $OUTPUTS is too long")
)

// readOutputs reads the outputs a step wrote to the file named by $OUTPUTS,
// as format 1 reads them: each line that holds an =, as the name before
// its first = and the value after it, each with the white space around it
// trimmed, and the two characters \n, \t and \r in a value made a newline,
// a tab and a carriage return; a name written again has its last value.
// It returns the file's size too, and errOutputsTooLarge, without reading
// it, for one larger than maxOutputs. As format 1, it reads nothing of a
// file with a line longer than maxOutputsLine, and returns
// errOutputsLineTooLong.
func readOutputs(path string) (map[string]string, int64, error) {
	f, err := os.Open(path) //nolint:gosec // a $OUTPUTS file in the worker's own directory
	if err != nil {
		return nil, 0, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, 0, err
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
