package format

import (
	"bytes"
	"cmp"
	"errors"
	"io"
	"iter"
	"math"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"

	"github.com/parallelworks/meridian/internal/jsonptr"
	"github.com/parallelworks/meridian/problems"
)

// DefaultMaxSize is the largest workflow file a Loader accepts unless it sets
// another limit. It is the limit ACTIVATE sets.
const DefaultMaxSize = 100 << 10

// A Loader loads workflow files. The zero Loader is ready to use.
type Loader struct {
	// MaxSize is the largest file, in bytes, the loader accepts. Zero means
	// DefaultMaxSize.
	MaxSize int
}

// A Document is a workflow file that loaded without errors.
type Document struct {
	// File is the file's name, as Load was given it.
	File string
	// Format is the workflow's format: its format key, or 1 without one.
	Format int
	// Value is the workflow as JSON values: map[string]any, []any, string,
	// bool, int64, float64 and nil. Keys the format ignores, which were
	// reported as warnings, are left out (Dropped).
	Value map[string]any

	// dropped holds the keys left out of Value, by where they are.
	dropped   []droppedKey
	positions map[string]problems.Position
	keys      map[string]problems.Position
	// styles holds each string's YAML style, and lines the file's lines,
	// so that a place in a string can be found in the file.
	styles map[string]yaml.Style
	lines  []string
	runes  *lineRunes
}

// Position returns where the value at a JSON Pointer (RFC 6901) starts, or
// the zero Position if the document has no value there.
func (d *Document) Position(pointer string) problems.Position {
	return d.positions[pointer]
}

// KeyPosition returns where the key of the value at a JSON Pointer is, or
// where the value starts if it has no key, as an item of a list has none.
func (d *Document) KeyPosition(pointer string) problems.Position {
	if p, ok := d.keys[pointer]; ok {
		return p
	}
	return d.positions[pointer]
}

// A droppedKey is a key the format doesn't define, which Value leaves out:
// its path in the workflow, and its value.
type droppedKey struct {
	path  []string
	value any
}

// Dropped returns the keys the format doesn't define, which were reported
// as warnings and are left out of Value, each by its path in the workflow,
// with its value, in the order they were dropped. A stage that reads more
// of the workflow than the format defines reads them here, as the compiler
// does the syntax of every expression in the input definitions. Each path
// is a copy, which the caller can change; the values are the document's.
func (d *Document) Dropped() iter.Seq2[[]string, any] {
	return func(yield func([]string, any) bool) {
		for _, k := range d.dropped {
			if !yield(slices.Clone(k.path), k.value) {
				return
			}
		}
	}
}

// ReportPosition returns where a diagnostic about the value at a JSON
// Pointer points: at a mapping's key, which names it, and at where anything
// else starts.
func (d *Document) ReportPosition(pointer string) problems.Position {
	tokens, err := jsonptr.Split(pointer)
	if err != nil {
		return problems.Position{}
	}
	if v, _ := jsonptr.Lookup(d.Value, tokens); isMapping(v) {
		return d.KeyPosition(pointer)
	}
	return d.Position(pointer)
}

func isMapping(v any) bool {
	_, ok := v.(map[string]any)
	return ok
}

// Load loads a workflow file with the zero Loader.
func Load(name string, data []byte) (*Document, problems.Diagnostics) {
	var l Loader
	return l.Load(name, data)
}

// Load parses a workflow file and validates it against the schema of its
// format. name is the file's name in diagnostics. Load returns every problem
// it finds, warnings included, and a nil Document if any of them is an error.
func (l *Loader) Load(name string, data []byte) (*Document, problems.Diagnostics) {
	p := &parser{
		file:   name,
		keys:   make(map[string]problems.Position),
		values: make(map[string]problems.Position),
		styles: make(map[string]yaml.Style),
	}
	value := p.parse(data, cmp.Or(l.MaxSize, DefaultMaxSize))
	format := 0
	if !p.diags.HasErrors() {
		if format = p.format(value); format != 0 {
			value = p.validate(schemas[format](), value)
		}
	}
	p.diags.Sort()
	if p.diags.HasErrors() {
		return nil, p.diags
	}
	// Every format's schema requires a mapping at the top.
	return &Document{
		File:      name,
		Format:    format,
		Value:     value.(map[string]any),
		dropped:   p.dropped,
		positions: p.values,
		keys:      p.keys,
		styles:    p.styles,
		lines:     strings.Split(string(data), "\n"),
		runes:     new(lineRunes),
	}, p.diags
}

// A parser loads one file. It records where each key and value is, by JSON
// Pointer, so that diagnostics found later can point into the file.
type parser struct {
	file   string
	keys   map[string]problems.Position
	values map[string]problems.Position
	styles map[string]yaml.Style
	diags  problems.Diagnostics
	// dropped holds the keys validate dropped.
	dropped []droppedKey
}

// report reports a problem of kind k at pos, about the value at path.
func (p *parser) report(k *problems.Kind, pos problems.Position, path string, params problems.Params) {
	p.diags = append(p.diags, k.At(p.file, pos, path, params))
}

// parse turns YAML into JSON values. ACTIVATE reads workflows the same way,
// with the same YAML library: aliases and merge keys are refused, a repeated
// key keeps its last value, and only the first document is read.
func (p *parser) parse(data []byte, maxSize int) any {
	if len(data) > maxSize {
		p.report(problems.FileTooLarge, problems.Position{}, "", problems.Params{"size": len(data), "limit": maxSize})
		return nil
	}
	dec := yaml.NewDecoder(bytes.NewReader(data))
	var doc yaml.Node
	if err := dec.Decode(&doc); err != nil {
		if !errors.Is(err, io.EOF) {
			p.syntaxError(problems.YAMLSyntax, err)
		}
		return nil
	}
	var next yaml.Node
	switch err := dec.Decode(&next); {
	case errors.Is(err, io.EOF):
	case err != nil:
		p.syntaxError(problems.ExtraDocumentInvalid, err)
	case !emptyDocument(&next):
		p.report(problems.ExtraDocument, position(&next), "", nil)
	}
	return p.value(doc.Content[0], "")
}

var syntaxErrorRE = regexp.MustCompile(`^yaml: line (\d+): (.*)$`)

// syntaxError reports err, the YAML library's error, as a problem of kind
// k, whose reason is the library's message.
func (p *parser) syntaxError(k *problems.Kind, err error) {
	msg := strings.TrimPrefix(err.Error(), "yaml: ")
	var pos problems.Position
	if m := syntaxErrorRE.FindStringSubmatch(err.Error()); m != nil {
		pos.Line, _ = strconv.Atoi(m[1])
		msg = m[2]
	}
	p.report(k, pos, "", problems.Params{"reason": msg})
}

// emptyDocument reports whether a YAML document has no content, as after a
// trailing document separator.
func emptyDocument(doc *yaml.Node) bool {
	if len(doc.Content) == 0 {
		return true
	}
	n := doc.Content[0]
	return n.Kind == yaml.ScalarNode && n.ShortTag() == "!!null" && n.Value == ""
}

func position(n *yaml.Node) problems.Position {
	return problems.Position{Line: n.Line, Column: n.Column}
}

// value converts a YAML node to a JSON value, recording its position and its
// descendants' under path.
func (p *parser) value(n *yaml.Node, path string) any {
	p.values[path] = position(n)
	switch n.Kind {
	case yaml.MappingNode:
		return p.mapping(n, path)
	case yaml.SequenceNode:
		s := make([]any, len(n.Content))
		for i, item := range n.Content {
			s[i] = p.value(item, jsonptr.Append(path, strconv.Itoa(i)))
		}
		return s
	case yaml.ScalarNode:
		return p.scalar(n, path)
	case yaml.AliasNode:
		p.report(problems.YAMLAlias, position(n), path, nil)
		return nil
	default:
		return nil
	}
}

func (p *parser) mapping(n *yaml.Node, path string) map[string]any {
	// A key's last occurrence wins, so find it first.
	last := make(map[string]int, len(n.Content)/2)
	for i := 0; i < len(n.Content); i += 2 {
		k := n.Content[i]
		switch {
		case k.Kind != yaml.ScalarNode:
			p.report(problems.YAMLCollectionKey, position(k), path, nil)
			continue
		case k.ShortTag() == "!!merge":
			p.report(problems.YAMLMergeKey, position(k), path, nil)
			continue
		}
		if j, ok := last[k.Value]; ok {
			p.report(problems.DuplicateKey, position(k), jsonptr.Append(path, k.Value),
				problems.Params{"key": k.Value, "first_line": strconv.Itoa(n.Content[j].Line)})
		}
		last[k.Value] = i
	}
	m := make(map[string]any, len(last))
	for i := 0; i < len(n.Content); i += 2 {
		k := n.Content[i]
		if j, ok := last[k.Value]; !ok || j != i {
			continue
		}
		path := jsonptr.Append(path, k.Value)
		p.keys[path] = position(k)
		m[k.Value] = p.value(n.Content[i+1], path)
	}
	return m
}

func (p *parser) scalar(n *yaml.Node, path string) any {
	switch n.ShortTag() {
	case "!!str":
		p.styles[path] = n.Style
		return n.Value
	case "!!merge":
		p.report(problems.YAMLMergeKey, position(n), path, nil)
		return nil
	}
	var v any
	if err := n.Decode(&v); err != nil {
		// A scalar that doesn't decode as its tag says is read as its text,
		// as ACTIVATE reads it.
		return n.Value
	}
	switch v := v.(type) {
	case int:
		return int64(v)
	case int64:
		return v
	case uint64:
		return float64(v)
	case float64:
		if math.IsInf(v, 0) || math.IsNaN(v) {
			p.report(problems.NumberNotJSON, position(n), path, problems.Params{"value": n.Value})
			return nil
		}
		return v
	case time.Time:
		// ACTIVATE stores the workflow as JSON, which writes a timestamp in
		// RFC 3339.
		return v.Format(time.RFC3339Nano)
	default:
		return v
	}
}

// format returns the workflow's format, or 0 after reporting that it is one
// this package doesn't know. A format key that isn't an integer is left for
// format 1's schema to report.
func (p *parser) format(value any) int {
	m, ok := value.(map[string]any)
	if !ok {
		return 1
	}
	n, ok := m["format"].(int64)
	switch {
	case !ok || n < 1:
		return 1
	case n <= latestFormat:
		return int(n)
	}
	p.report(problems.FormatTooNew, p.values["/format"], "/format",
		problems.Params{"format": strconv.FormatInt(n, 10), "latest": strconv.Itoa(latestFormat)})
	return 0
}
