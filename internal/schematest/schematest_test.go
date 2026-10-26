package schematest

import (
	"bytes"
	"io/fs"
	"path"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"

	"github.com/parallelworks/meridian/plan"
)

// A version's schemas refer to each other by their names, so each compiles
// wherever their directory is, as where a platform serves them from, with
// nothing but the others beside it.
func TestSchemasResolveWhereverTheirDirectoryIs(t *testing.T) {
	versions, err := fs.ReadDir(plan.Schemas, "schema")
	if err != nil || len(versions) == 0 {
		t.Fatalf("no versions: %v", err)
	}
	for _, version := range versions {
		dir := path.Join("schema", version.Name())
		files, err := fs.ReadDir(plan.Schemas, dir)
		if err != nil {
			t.Fatal(err)
		}
		c := jsonschema.NewCompiler()
		c.DefaultDraft(jsonschema.Draft2020)
		c.UseLoader(noLoader{})
		base := "https://schemas.example.com/meridian/" + version.Name() + "/"
		for _, f := range files {
			data, err := fs.ReadFile(plan.Schemas, path.Join(dir, f.Name()))
			if err != nil {
				t.Fatal(err)
			}
			doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(data))
			if err != nil {
				t.Fatal(err)
			}
			if err := c.AddResource(base+f.Name(), doc); err != nil {
				t.Fatal(err)
			}
		}
		for _, f := range files {
			if _, err := c.Compile(base + f.Name()); err != nil {
				t.Errorf("%s/%s: %v", dir, f.Name(), err)
			}
		}
	}
}
