package expr

import (
	"maps"
	"os"
	"slices"
	"strings"
	"testing"
)

// TestGrammarDocumented checks that docs/expressions.md, the grammar of
// expressions, has each operator, function and context the parser knows as a
// token, so that the parser can't gain one the document doesn't name.
func TestGrammarDocumented(t *testing.T) {
	data, err := os.ReadFile("../docs/expressions.md")
	if err != nil {
		t.Fatal(err)
	}
	doc := string(data)
	tokens := []string{"and", "or", "?", ":", "!"}
	for _, s := range symbolOperators {
		tokens = append(tokens, s.sym)
	}
	for w := range negatable {
		tokens = append(tokens, "!"+w)
	}
	tokens = slices.AppendSeq(tokens, maps.Keys(prefixOperators))
	tokens = slices.AppendSeq(tokens, maps.Keys(wordOperators))
	tokens = slices.AppendSeq(tokens, maps.Keys(functions))
	tokens = slices.AppendSeq(tokens, maps.Keys(contexts))
	slices.Sort(tokens)
	for _, tok := range slices.Compact(tokens) {
		if !strings.Contains(doc, `"`+tok+`"`) {
			t.Errorf(`docs/expressions.md has no token "%s"`, tok)
		}
	}
}
