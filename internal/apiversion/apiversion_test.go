package apiversion

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestOnlyTestsSet checks what the package doc promises: no code of the
// module's but a _test.go file calls SetSince, so that a release never makes a
// member of the worker API's bodies another version's than its table says.
func TestOnlyTestsSetSince(t *testing.T) {
	fset := token.NewFileSet()
	root := filepath.Join("..", "..")
	calls := 0
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			name := d.Name()
			if path != root && (strings.HasPrefix(name, ".") || name == "testdata") {
				return filepath.SkipDir
			}
			// A directory with a go.mod of its own is another module.
			if _, err := os.Stat(filepath.Join(path, "go.mod")); path != root && err == nil {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		f, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
		}
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || sel.Sel.Name != "SetSince" {
				return true
			}
			if pkg, ok := sel.X.(*ast.Ident); ok && pkg.Name == "apiversion" {
				calls++
				if !strings.HasSuffix(path, "_test.go") {
					t.Errorf("%s calls apiversion.SetSince, which only a test may", fset.Position(call.Pos()))
				}
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	// The check finds the tests' own calls, so that it can't pass by
	// finding nothing.
	if calls == 0 {
		t.Error("no file calls apiversion.SetSince")
	}
}
