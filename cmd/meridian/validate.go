package main

import (
	"bytes"
	"context"
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"strings"

	"github.com/parallelworks/foundation/problem"

	"github.com/parallelworks/meridian/cmd/meridian/internal/local"
	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/format"
	"github.com/parallelworks/meridian/internal/jsonvalue"
	"github.com/parallelworks/meridian/problems"
)

// maxInputsSize is the largest file of input values validate reads.
const maxInputsSize = 1 << 20

// runValidate compiles a workflow without running it, with the user's
// variables and groups, and the input values given, and reports every
// error and warning.
func runValidate(ctx context.Context, c *cli, fs *flag.FlagSet, args []string) int {
	var set []string
	fs.Func("input", "sets the input at `path` to value, as path=value, where a path is an input's name, or a group's and its input's, as group.name; it can be given again", func(s string) error {
		if !strings.Contains(s, "=") {
			return errors.New("an input is given as path=value")
		}
		set = append(set, s)
		return nil
	})
	inputsFile := fs.String("inputs", "", "reads input values from the JSON `file`, an object of them by name; -input sets values over them")
	out := fs.String("format", "text", "writes errors and warnings as `text`, one a line as compilers write them, or as json, an RFC 9457 validation problem")
	positional, err := parseFlags(fs, args)
	if err != nil {
		return flagError(c, fs, err)
	}
	if len(positional) != 1 {
		return c.errorf("validate takes one workflow file, or - for standard input; run meridian help validate")
	}
	if *out != "text" && *out != "json" {
		return c.errorf("-format is text or json, not %q", *out)
	}
	path := positional[0]

	dirs, err := local.Locate(c.getenv)
	if err != nil {
		return c.errorf("%v", err)
	}
	cfg, err := local.LoadConfig(dirs.Config, c.getenv)
	if err != nil {
		return c.errorf("%v", err)
	}
	contexts, err := cfg.Contexts(ctx)
	if err != nil {
		return c.errorf("%v", err)
	}
	submitted, err := submittedInputs(*inputsFile, set)
	if err != nil {
		return c.errorf("%v", err)
	}
	name, data, tooLarge, err := readWorkflow(c, path)
	if err != nil {
		return c.errorf("can't read the workflow: %v", err)
	}
	diags := tooLarge
	if diags == nil {
		_, diags = compile.Compile(name, data, submitted, compile.Options{Inputs: cfg, Contexts: contexts})
	}
	if *out == "json" {
		err = writeProblemJSON(c.stdout, diags)
	} else {
		err = writeText(c.stdout, diags)
	}
	if err != nil {
		return c.errorf("%v", err)
	}
	if diags.HasErrors() {
		return exitInvalid
	}
	return exitOK
}

// stdinName is what diagnostics call a workflow read from standard input.
const stdinName = "<stdin>"

// readWorkflow reads the workflow file at path, or standard input for -,
// and returns its name for diagnostics and its text. A file larger than
// the compiler reads isn't kept: it is a diagnostic, with its size.
func readWorkflow(c *cli, path string) (string, []byte, problems.Diagnostics, error) {
	name, r := stdinName, c.stdin
	if path != "-" {
		f, err := os.Open(path) //nolint:gosec // the workflow the user named
		if err != nil {
			return "", nil, nil, err
		}
		defer f.Close()
		name, r = path, f
	}
	limit := format.DefaultMaxSize
	data, err := io.ReadAll(io.LimitReader(r, int64(limit)+1))
	if err != nil {
		return "", nil, nil, err
	}
	if len(data) <= limit {
		return name, data, nil, nil
	}
	rest, err := io.Copy(io.Discard, r)
	if err != nil {
		return "", nil, nil, err
	}
	size := int64(len(data)) + rest
	return name, nil, problems.Diagnostics{problems.FileTooLarge.At(name, problems.Position{}, "", problems.Params{"size": size, "limit": limit})}, nil
}

// submittedInputs returns the input values of the JSON file, if any, with
// each path=value of set over them.
func submittedInputs(file string, set []string) (map[string]any, error) {
	values := map[string]any{}
	if file != "" {
		var err error
		if values, err = readInputsFile(file); err != nil {
			return nil, fmt.Errorf("-inputs %s: %w", file, err)
		}
	}
	for _, s := range set {
		path, value, _ := strings.Cut(s, "=")
		if err := setPath(values, path, value); err != nil {
			return nil, fmt.Errorf("-input %s: %w", s, err)
		}
	}
	return values, nil
}

// readInputsFile reads a JSON object of input values. A number is an
// int64 when it is a whole one that fits, as YAML reads one, and a float64
// otherwise.
func readInputsFile(name string) (map[string]any, error) {
	f, err := os.Open(name) //nolint:gosec // the file of inputs the user named
	if err != nil {
		return nil, err
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, maxInputsSize+1))
	if err != nil {
		return nil, err
	}
	if len(data) > maxInputsSize {
		return nil, fmt.Errorf("it is larger than %d bytes", maxInputsSize)
	}
	dec := jsontext.NewDecoder(bytes.NewReader(data))
	v, err := jsonvalue.Decode(dec, jsonNumber)
	if err != nil {
		return nil, err
	}
	if _, err := dec.ReadToken(); !errors.Is(err, io.EOF) {
		return nil, errors.New("it has more than one JSON value")
	}
	values, ok := v.(map[string]any)
	if !ok {
		return nil, errors.New("it isn't a JSON object of input values by name")
	}
	return values, nil
}

// jsonNumber reads a JSON number as YAML would: an int64 when it is whole
// and fits, and a float64 otherwise.
func jsonNumber(s string) (any, error) {
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		return n, nil
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil || math.IsInf(f, 0) {
		return nil, fmt.Errorf("%s is too large a number", s)
	}
	return f, nil
}

// setPath sets the value at path, names joined with dots, in values, to
// value, making a mapping for each name before the last that has none.
func setPath(values map[string]any, path, value string) error {
	names := strings.Split(path, ".")
	for i, name := range names {
		if name == "" {
			return errors.New("an input's path is names joined with dots")
		}
		if i == len(names)-1 {
			values[name] = value
			return nil
		}
		next, ok := values[name]
		if !ok {
			m := map[string]any{}
			values[name] = m
			values = m
			continue
		}
		if values, ok = next.(map[string]any); !ok {
			return fmt.Errorf("%s has a value already, which isn't a group of inputs", strings.Join(names[:i+1], "."))
		}
	}
	return nil
}

// writeText writes the diagnostics one a line, as compilers write them:
// file:line:column: severity: message (code).
func writeText(w io.Writer, diags problems.Diagnostics) error {
	for _, d := range diags {
		if _, err := fmt.Fprintln(w, d.String()); err != nil {
			return err
		}
	}
	return nil
}

// writeProblemJSON writes the diagnostics as JSON, on one line. A workflow
// with errors is an RFC 9457 validation problem, as over HTTP, with an
// entry for each error, and its warnings in a warnings member beside them;
// one without is an object of its warnings alone, as a successful result
// carries them.
func writeProblemJSON(w io.Writer, diags problems.Diagnostics) error {
	// A list with no warnings is [], not null.
	warnings, err := jsonv2.Marshal(diags.Warnings())
	if err != nil {
		return err
	}
	var out []byte
	if errs := diags.Errors(); len(errs) > 0 {
		entries := make([]*problem.FieldError, len(errs))
		for i := range errs {
			entries[i] = &errs[i].FieldError
		}
		if out, err = jsonv2.Marshal(problem.ValidationFailed(entries...)); err != nil {
			return err
		}
		// A problem is an object, to which the warnings are added as a
		// member of its own, as RFC 9457 lets a problem type's extensions
		// be.
		out = append(out[:len(out)-1], `,"warnings":`...)
	} else {
		out = []byte(`{"warnings":`)
	}
	out = append(append(out, warnings...), "}\n"...)
	_, err = w.Write(out)
	return err
}
