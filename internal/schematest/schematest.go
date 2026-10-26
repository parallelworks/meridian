// Package schematest checks what Meridian encodes against the JSON Schemas
// of the plan and of the worker API's bodies, for tests.
package schematest

import (
	"bytes"
	"fmt"
	"io/fs"
	"path"
	"strings"
	"sync"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"

	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/workerapi"
)

// sources are the schemas, by the directories they are in, in the
// repository: they refer to each other by their paths relative to each
// other, as the repository has them.
var sources = map[string]fs.FS{
	"plan":      plan.Schemas,
	"workerapi": workerapi.Schemas,
}

var (
	compileOnce sync.Once
	compiler    *jsonschema.Compiler
	compileErr  error
	compiled    sync.Map
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
	compileOnce.Do(load)
	if compileErr != nil {
		t.Fatal(compileErr)
	}
	if s, ok := compiled.Load(path); ok {
		return s.(*jsonschema.Schema)
	}
	s, err := compiler.Compile("file:///" + path)
	if err != nil {
		t.Fatalf("schematest: %s: %v", path, err)
	}
	compiled.Store(path, s)
	return s
}

// Paths returns the paths, in the repository, of every schema.
func Paths() ([]string, error) {
	var out []string
	for dir, fsys := range sources {
		err := fs.WalkDir(fsys, ".", func(p string, d fs.DirEntry, err error) error {
			if err == nil && !d.IsDir() && strings.HasSuffix(p, ".schema.json") {
				out = append(out, path.Join(dir, p))
			}
			return err
		})
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

// load adds every schema to compiler.
func load() {
	compiler = jsonschema.NewCompiler()
	compiler.DefaultDraft(jsonschema.Draft2020)
	compiler.UseLoader(noLoader{})
	for dir, fsys := range sources {
		err := fs.WalkDir(fsys, ".", func(p string, d fs.DirEntry, err error) error {
			if err != nil || d.IsDir() || !strings.HasSuffix(p, ".schema.json") {
				return err
			}
			data, err := fs.ReadFile(fsys, p)
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
			return
		}
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
