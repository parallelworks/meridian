package format

import (
	"bytes"
	"cmp"
	"errors"
	"fmt"
	"io"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"

	"github.com/parallelworks/meridian/internal/jsonptr"
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
	// reported as warnings, are left out.
	Value map[string]any

	positions map[string]Position
	keys      map[string]Position
	// styles holds each string's YAML style, and lines the file's lines,
	// so that a place in a string can be found in the file.
	styles map[string]yaml.Style
	lines  []string
}

// Position returns where the value at a JSON Pointer (RFC 6901) starts, or
// the zero Position if the document has no value there.
func (d *Document) Position(pointer string) Position {
	return d.positions[pointer]
}

// KeyPosition returns where the key of the value at a JSON Pointer is, or
// where the value starts if it has no key, as an item of a list has none.
func (d *Document) KeyPosition(pointer string) Position {
	if p, ok := d.keys[pointer]; ok {
		return p
	}
	return d.positions[pointer]
}

// ReportPosition returns where a diagnostic about the value at a JSON
// Pointer points: at a mapping's key, which names it, and at where anything
// else starts.
func (d *Document) ReportPosition(pointer string) Position {
	tokens, err := jsonptr.Split(pointer)
	if err != nil {
		return Position{}
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
func Load(name string, data []byte) (*Document, Diagnostics) {
	var l Loader
	return l.Load(name, data)
}

// Load parses a workflow file and validates it against the schema of its
// format. name is the file's name in diagnostics. Load returns every problem
// it finds, warnings included, and a nil Document if any of them is an error.
func (l *Loader) Load(name string, data []byte) (*Document, Diagnostics) {
	p := &parser{
		file:   name,
		keys:   make(map[string]Position),
		values: make(map[string]Position),
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
		positions: p.values,
		keys:      p.keys,
		styles:    p.styles,
		lines:     strings.Split(string(data), "\n"),
	}, p.diags
}

// A parser loads one file. It records where each key and value is, by JSON
// Pointer, so that diagnostics found later can point into the file.
type parser struct {
	file   string
	keys   map[string]Position
	values map[string]Position
	styles map[string]yaml.Style
	diags  Diagnostics
}

func (p *parser) report(severity Severity, pos Position, code Code, path, format string, args ...any) {
	p.diags = append(p.diags, Diagnostic{
		File:     p.file,
		Position: pos,
		Severity: severity,
		Code:     code,
		Path:     path,
		Message:  fmt.Sprintf(format, args...),
	})
}

// parse turns YAML into JSON values. ACTIVATE reads workflows the same way,
// with the same YAML library: aliases and merge keys are refused, a repeated
// key keeps its last value, and only the first document is read.
func (p *parser) parse(data []byte, maxSize int) any {
	if len(data) > maxSize {
		p.report(Error, Position{}, CodeTooLarge, "", "the file is %d bytes, more than the limit of %d", len(data), maxSize)
		return nil
	}
	dec := yaml.NewDecoder(bytes.NewReader(data))
	var doc yaml.Node
	if err := dec.Decode(&doc); err != nil {
		if !errors.Is(err, io.EOF) {
			p.syntaxError(Error, err)
		}
		return nil
	}
	var next yaml.Node
	switch err := dec.Decode(&next); {
	case errors.Is(err, io.EOF):
	case err != nil:
		p.syntaxError(Warning, err)
	case !emptyDocument(&next):
		p.report(Warning, Position{next.Line, next.Column}, CodeExtraDocument, "",
			"the file has more than one YAML document; only the first is read")
	}
	return p.value(doc.Content[0], "")
}

var syntaxErrorRE = regexp.MustCompile(`^yaml: line (\d+): (.*)$`)

func (p *parser) syntaxError(severity Severity, err error) {
	msg := strings.TrimPrefix(err.Error(), "yaml: ")
	var pos Position
	if m := syntaxErrorRE.FindStringSubmatch(err.Error()); m != nil {
		pos.Line, _ = strconv.Atoi(m[1])
		msg = m[2]
	}
	if severity == Warning {
		p.report(Warning, pos, CodeExtraDocument, "",
			"the file has more than one YAML document, and only the first is read; the rest isn't valid YAML: %s", msg)
		return
	}
	p.report(Error, pos, CodeSyntax, "", "invalid YAML: %s", msg)
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

func position(n *yaml.Node) Position {
	return Position{n.Line, n.Column}
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
		p.report(Error, position(n), CodeUnsupported, path, "YAML aliases aren't allowed")
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
			p.report(Error, position(k), CodeUnsupported, path, "a key must be a string, not a YAML collection")
			continue
		case k.ShortTag() == "!!merge":
			p.report(Error, position(k), CodeUnsupported, path, "YAML merge keys (<<) aren't allowed")
			continue
		}
		if j, ok := last[k.Value]; ok {
			p.report(Warning, position(k), CodeDuplicateKey, jsonptr.Append(path, k.Value),
				"%q appears again, so its value on line %d is ignored", k.Value, n.Content[j].Line)
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
	case "!!null":
		return nil
	case "!!merge":
		p.report(Error, position(n), CodeUnsupported, path, "YAML merge keys (<<) aren't allowed")
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
			p.report(Error, position(n), CodeUnsupported, path, "%s isn't a number JSON can hold", n.Value)
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
	p.report(Error, p.values["/format"], CodeFormat, "/format",
		"format %d is newer than this version of Meridian, which runs format %d and earlier", n, latestFormat)
	return 0
}
