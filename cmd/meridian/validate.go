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
	"io/fs"
	"maps"
	"os"
	"slices"
	"sync"

	"github.com/parallelworks/foundation/problem"

	"github.com/parallelworks/meridian/cmd/meridian/internal/local"
	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/coordinator"
	"github.com/parallelworks/meridian/format"
	"github.com/parallelworks/meridian/internal/jsonvalue"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
)

// maxInputsSize is the largest file of input values validate reads.
const maxInputsSize = 1 << 20

// formats are what validate's -format takes.
var formats = []string{"text", "json"}

// runValidate creates a run of a workflow without running it, with the
// user's variables and groups, the workflows its steps' uses: name, and the
// input values given, and reports every error and warning. There is no
// session provider here, so a workflow with sessions can't run. With
// -start, it starts the run too, as far as it can without running
// anything: it reads every variable the run reads, secret ones too,
// evaluates what waits for the start and checks it, and reports what would
// fail the run as it starts.
func runValidate(ctx context.Context, c *cli, fs *flag.FlagSet, args []string) int {
	inputs := inputFlags(fs)
	out := fs.String("format", "text", "writes errors and warnings as `text`, one a line as compilers write them, or as json, an RFC 9457 validation problem")
	start := fs.Bool("start", false, "starts the run too, without running it: reads every variable it reads, secret ones too, and checks what waits for the run's start")
	positional, err := parseFlags(fs, args)
	if err != nil {
		return flagError(c, fs, err)
	}
	if len(positional) != 1 {
		return c.failf(problems.ArgumentsInvalid, problems.Params{"command": "validate"})
	}
	if !slices.Contains(formats, *out) {
		return c.failf(problems.FlagValueInvalid, problems.Params{"command": "validate", "flag": "format", "value": *out})
	}
	// From here, the command's own problems are JSON too.
	c.json = *out == "json"
	w, err := compileWorkflow(ctx, c, inputs, positional[0])
	if err != nil {
		return c.fail(err)
	}
	diags := w.diags
	if *start && w.created != nil {
		_, started, err := compile.Start(ctx, w.created.Plan, compile.StartInput{
			Variables: w.cfg, Record: w.created.Record, Inputs: w.cfg, Sessions: local.SessionNames{},
		})
		if err != nil {
			return c.fail(err)
		}
		diags = append(diags, located(w.name, w.data, started)...)
		diags.Sort()
	}
	if *out == "json" {
		err = writeProblemJSON(c.stdout, diags)
	} else {
		err = writeText(c.stdout, diags)
	}
	if err != nil {
		return c.fail(err)
	}
	if diags.HasErrors() {
		return exitInvalid
	}
	return exitOK
}

// The options of the run's input values, which validate and run share.
type inputOptions struct {
	set  []string
	file *string
}

// inputFlags defines the options of the run's input values on fs.
func inputFlags(fs *flag.FlagSet) *inputOptions {
	o := &inputOptions{}
	fs.Func("input", "sets the input at `path` to value, as path=value, where a path is an input's name, or a group's and its input's, as group.name, converted by its input's type: true or false for a boolean, a JSON number for a number, JSON for a list, a group or another structure, and the text as it is for text; it can be given again", func(s string) error {
		o.set = append(o.set, s)
		return nil
	})
	o.file = fs.String("inputs", "", "reads input values from the JSON `file`, an object of them by name, as it writes them; -input sets values over them")
	return o
}

// A compiled workflow is what compileWorkflow gave: the workflow's name for
// diagnostics, its text, and what Create gave of it, with the
// directories and config it was compiled with.
type compiled struct {
	name    string
	data    []byte
	created *compile.Created
	diags   problems.Diagnostics
	dirs    local.Dirs
	cfg     *local.Config
}

// compileWorkflow creates a run of the workflow at path, or standard input
// for -, without storing it, as validate and run do: with the user's
// variables and groups, the workflows its steps' uses: name, the input
// values inputs gives, and its sessions checked and named as this
// machine serves them, at the login name of the user who runs meridian
// (local.SessionNames). A workflow too large to read has no run, only its
// diagnostic; one with errors may have none.
func compileWorkflow(ctx context.Context, c *cli, inputs *inputOptions, path string) (*compiled, error) {
	sets, err := parseInputSets(inputs.set)
	if err != nil {
		return nil, err
	}
	dirs, err := local.Locate(c.getenv)
	if err != nil {
		return nil, err
	}
	cfg, err := local.LoadConfig(dirs.Config, c.lookupEnv)
	if err != nil {
		return nil, err
	}
	submitted, err := submittedInputs(ctx, c, *inputs.file)
	if err != nil {
		return nil, err
	}
	name, data, tooLarge, err := readWorkflow(ctx, c, path)
	if err != nil {
		return nil, err
	}
	w := &compiled{name: name, data: data, diags: tooLarge, dirs: dirs, cfg: cfg}
	if len(tooLarge) > 0 {
		return w, nil
	}
	// Each -input is converted by the type the workflow gives its input, as
	// the workflow is read to compile it.
	if err := setInputs(submitted, inputDefinitions(name, data), sets); err != nil {
		return nil, err
	}
	var sources compile.Sources = &cacheSources{dirs: dirs, servers: cfg.GitLabServers(), workflows: cfg.Workflows()}
	if c.sources != nil {
		sources = c.sources
	}
	w.created, w.diags, err = compile.Create(ctx, name, data, submitted, compile.Options{
		Source: plan.Source{Location: workflowLocation(path)},
		Inputs: cfg, Variables: cfg, Groups: cfg, Sources: sources, Sessions: local.SessionNames{User: local.UserLogin()},
	})
	if err != nil {
		return nil, err
	}
	return w, nil
}

// cacheSources is the local Sources, whose copies of repositories are in
// the cache directory, which it makes the first time it is asked for a
// workflow, so that a workflow that names none makes none.
type cacheSources struct {
	dirs local.Dirs
	// servers are the GitLab servers the user registered, and workflows
	// the workflows a platform stores that the user mapped to workflows in
	// Git.
	servers   []string
	workflows local.Workflows
	// mu guards sources, which the coordinator's requests for checkouts'
	// trees open at once.
	mu      sync.Mutex
	sources *local.Sources
}

var (
	_ compile.Sources   = (*cacheSources)(nil)
	_ coordinator.Trees = (*cacheSources)(nil)
)

func (s *cacheSources) open() (*local.Sources, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sources == nil {
		if err := s.dirs.EnsureCache(); err != nil {
			return nil, err
		}
		s.sources = local.NewSources(s.dirs.Cache, s.servers, s.workflows)
	}
	return s.sources, nil
}

func (s *cacheSources) Resolve(ctx context.Context, ref plan.SourceRef) (*compile.Workflow, error) {
	sources, err := s.open()
	if err != nil {
		return nil, err
	}
	return sources.Resolve(ctx, ref)
}

func (s *cacheSources) Read(ctx context.Context, source plan.Source) (*compile.Workflow, error) {
	sources, err := s.open()
	if err != nil {
		return nil, err
	}
	return sources.Read(ctx, source)
}

func (s *cacheSources) Pin(ctx context.Context, repo, revision string) (string, error) {
	sources, err := s.open()
	if err != nil {
		return "", err
	}
	return sources.Pin(ctx, repo, revision)
}

func (s *cacheSources) SurveyTree(ctx context.Context, repo, commit string, paths []string) (coordinator.TreeSurvey, error) {
	sources, err := s.open()
	if err != nil {
		return coordinator.TreeSurvey{}, err
	}
	return sources.SurveyTree(ctx, repo, commit, paths)
}

func (s *cacheSources) WriteTree(ctx context.Context, repo, commit string, paths []string, w io.Writer) error {
	sources, err := s.open()
	if err != nil {
		return err
	}
	return sources.WriteTree(ctx, repo, commit, paths, w)
}

// located returns diags, the run's problems as it starts, which have no
// file position, each where the workflow writes what it is about, in the
// file name, which holds data.
func located(name string, data []byte, diags problems.Diagnostics) problems.Diagnostics {
	doc, _ := format.Load(name, data)
	out := make(problems.Diagnostics, len(diags))
	for i, d := range diags {
		out[i] = d
		if k, ok := d.Kind(); ok && doc != nil {
			out[i] = k.At(name, doc.ReportPosition(d.Path()), d.Path(), d.Params)
		}
	}
	return out
}

// stdinName is what diagnostics call a workflow read from standard input.
const stdinName = "<stdin>"

// content is what read read of a file.
type content struct {
	// data is its bytes, when there are at most read's limit.
	data []byte
	// over says it is larger than the limit, and size how large, or -1 for
	// a stream, which read stops reading past the limit.
	over bool
	size int64
}

// read reads the file at path, or standard input for -, up to limit bytes.
// It stats a regular file, standard input when it is one too, and doesn't
// read one larger than limit. It reads
// in a goroutine of its own, so that ctx's end, as on an interrupt, stops
// it waiting on a pipe or a FIFO that never ends, with problems.Interrupted.
// It returns the operating system's error as it is.
func read(ctx context.Context, c *cli, path string, limit int) (content, error) {
	type result struct {
		content
		err error
	}
	done := make(chan result, 1)
	go func() {
		r := c.stdin
		if path != "-" {
			f, err := os.Open(path) //nolint:gosec // the file the user named
			if err != nil {
				done <- result{err: err}
				return
			}
			defer f.Close()
			r = f
		}
		// A regular file, standard input among them when it is one, has its
		// size, and one larger than limit isn't read.
		if st, ok := r.(interface{ Stat() (fs.FileInfo, error) }); ok {
			if info, err := st.Stat(); err == nil && info.Mode().IsRegular() && info.Size() > int64(limit) {
				done <- result{over: true, size: info.Size()}
				return
			}
		}
		data, err := io.ReadAll(io.LimitReader(r, int64(limit)+1))
		if len(data) > limit {
			done <- result{over: true, size: -1}
			return
		}
		done <- result{content{data: data, size: int64(len(data))}, err}
	}()
	select {
	case r := <-done:
		return r.content, r.err
	case <-ctx.Done():
		return content{}, problems.Interrupted.Problem(nil)
	}
}

// readWorkflow reads the workflow file at path, or standard input for -,
// and returns its name for diagnostics and its text. A file larger than
// the compiler reads isn't kept: it is a diagnostic, with its size if it
// is a regular file.
func readWorkflow(ctx context.Context, c *cli, path string) (string, []byte, problems.Diagnostics, error) {
	name := path
	if path == "-" {
		name = stdinName
	}
	limit := format.DefaultMaxSize
	got, err := read(ctx, c, path, limit)
	switch {
	case problems.IsProblem(err):
		return "", nil, nil, err
	case err != nil:
		return "", nil, nil, problems.FileUnreadable.Problem(problems.Params{"path": path, "reason": err.Error()})
	case got.over && got.size < 0:
		return name, nil, problems.Diagnostics{problems.WorkflowTooLarge.At(name, problems.Position{}, "", problems.Params{"limit": limit})}, nil
	case got.over:
		return name, nil, problems.Diagnostics{problems.FileTooLarge.At(name, problems.Position{}, "", problems.Params{"size": got.size, "limit": limit})}, nil
	}
	return name, got.data, nil, nil
}

// submittedInputs returns the input values of the JSON file, if any, as it
// writes them: format 1 reads each value as it was submitted.
func submittedInputs(ctx context.Context, c *cli, file string) (map[string]any, error) {
	if file == "" {
		return map[string]any{}, nil
	}
	return readInputsFile(ctx, c, file)
}

// inputDefinitions returns the input definitions of the workflow data
// holds, as the compiler reads them, or none if it doesn't load.
func inputDefinitions(name string, data []byte) map[string]any {
	doc, _ := format.Load(name, data)
	if doc == nil {
		return nil
	}
	on, _ := doc.Value["on"].(map[string]any)
	execute, _ := on["execute"].(map[string]any)
	defs, _ := execute["inputs"].(map[string]any)
	return defs
}

// readInputsFile reads a JSON object of input values. A number is an
// int64 when it is a whole one that fits, as YAML reads one, and a float64
// otherwise.
func readInputsFile(ctx context.Context, c *cli, name string) (map[string]any, error) {
	got, err := read(ctx, c, name, maxInputsSize)
	switch {
	case problems.IsProblem(err):
		return nil, err
	case err != nil:
		return nil, problems.FileUnreadable.Problem(problems.Params{"path": name, "reason": err.Error()})
	case got.over:
		return nil, problems.FileOverLimit.Problem(problems.Params{"path": name, "limit": maxInputsSize})
	}
	at := problems.Params{problems.ParamFile: name}
	dec := jsontext.NewDecoder(bytes.NewReader(got.data))
	v, err := jsonvalue.Decode(dec, jsonNumber)
	if err != nil {
		if p, ok := errors.AsType[*problem.Problem](err); ok {
			p.Params[problems.ParamFile] = name
			return nil, p
		}
		params := maps.Clone(at)
		params["reason"] = err.Error()
		return nil, problems.InputsFileInvalid.Problem(params)
	}
	if _, err := dec.ReadToken(); !errors.Is(err, io.EOF) {
		return nil, problems.InputsFileExtra.Problem(at)
	}
	values, ok := v.(map[string]any)
	if !ok {
		return nil, problems.InputsFileNotObject.Problem(at)
	}
	return values, nil
}

// jsonNumber reads a JSON number as YAML would (jsonvalue.Number), and
// one too large for a float64 is a problem.
func jsonNumber(s string) (any, error) {
	n, err := jsonvalue.Number(s)
	if err != nil {
		return nil, problems.InputsNumberTooLarge.Problem(problems.Params{"number": s})
	}
	return n, nil
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
