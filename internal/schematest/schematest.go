// Package schematest checks what Meridian encodes against the JSON Schemas
// of the plan and of the worker API's bodies (plan.Schemas), for tests.
package schematest

import (
	"bytes"
	"fmt"
	"io/fs"
	"net/url"
	"path"
	"strings"
	"sync"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"

	"github.com/parallelworks/meridian/plan"
)

// dir is the directory, in the repository, that plan.Schemas holds the
// schemas of: each version's refer to each other by their names, as they
// are in the same directory.
const dir = "plan"

var (
	compileOnce sync.Once
	compileErr  error
	// mu guards compiler, which one goroutine uses at a time, and
	// compiled, the schemas it compiled by URL.
	mu       sync.Mutex
	compiler *jsonschema.Compiler
	compiled = map[string]*jsonschema.Schema{}
)

// noLoader refuses to load a schema: every schema is added to the compiler.
type noLoader struct{}

func (noLoader) Load(url string) (any, error) {
	return nil, fmt.Errorf("schematest: no schema at %s", url)
}

// Schema returns the schema at path in the repository, as
// plan/schema/1/readyjob.schema.json, or one of its definitions, as
// plan/schema/1/plan.schema.json#/$defs/Expansion.
func Schema(t testing.TB, path string) *jsonschema.Schema {
	t.Helper()
	s, err := compile("file:///"+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// probe returns a schema that what fits the schema at path fits, and
// nothing else, which says only whether a document fits: the library then
// skips making the errors that say why one doesn't, which take most of its
// time for such a document, as package format does.
func probe(t testing.TB, path string) *jsonschema.Schema {
	t.Helper()
	s, err := compile("urn:schematest:probe:"+url.PathEscape(path), map[string]any{"if": map[string]any{"$ref": "file:///" + path}, "else": false})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// compile returns the schema at the URL id, which it adds as doc first if
// doc isn't nil.
func compile(id string, doc any) (*jsonschema.Schema, error) {
	compileOnce.Do(load)
	if compileErr != nil {
		return nil, compileErr
	}
	mu.Lock()
	defer mu.Unlock()
	if s, ok := compiled[id]; ok {
		return s, nil
	}
	if doc != nil {
		if err := compiler.AddResource(id, doc); err != nil {
			return nil, err
		}
	}
	s, err := compiler.Compile(id)
	if err != nil {
		return nil, fmt.Errorf("schematest: %s: %w", id, err)
	}
	compiled[id] = s
	return s, nil
}

// load adds every schema to compiler.
func load() {
	compiler = jsonschema.NewCompiler()
	compiler.DefaultDraft(jsonschema.Draft2020)
	compiler.UseLoader(noLoader{})
	err := fs.WalkDir(plan.Schemas, ".", func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(p, ".schema.json") {
			return err
		}
		data, err := fs.ReadFile(plan.Schemas, p)
		if err != nil {
			return err
		}
		doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(data))
		if err != nil {
			return fmt.Errorf("%s: %w", path.Join(dir, p), err)
		}
		return compiler.AddResource("file:///"+path.Join(dir, p), doc)
	})
	if err != nil {
		compileErr = fmt.Errorf("schematest: %w", err)
	}
}

// Validate checks data, JSON that what names, against s, and reports
// where it doesn't fit.
func Validate(t testing.TB, s *jsonschema.Schema, what string, data []byte) {
	t.Helper()
	if err := Check(s, data); err != nil {
		t.Errorf("%s doesn't fit its schema: %v\n%s", what, err, data)
	}
}

// Check returns an error that says where data, JSON, doesn't fit s, or nil.
func Check(s *jsonschema.Schema, data []byte) error {
	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(data))
	if err != nil {
		return err
	}
	return s.Validate(doc)
}
