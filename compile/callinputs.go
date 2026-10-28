package compile

import (
	"slices"
	"strings"

	"github.com/parallelworks/meridian/format"
	"github.com/parallelworks/meridian/internal/inputpath"
	"github.com/parallelworks/meridian/internal/jsonptr"
	"github.com/parallelworks/meridian/problems"
)

// checkCallInputs reports each required input of the workflow called that
// the step at loc, in the workflow doc, leaves out of its with:
// (child_input_missing), as format 1 refuses to create a run any of whose
// calls, down its tree, does. It reports them at root, the uses: of the
// run's own step that the path of calls starts at, with chain, the
// locations of the workflows on that path, from the run's own to called;
// and a step that more than one path reaches, once.
func (c *compiler) checkCallInputs(t *tree, root, chain []string, doc *format.Document, loc []string, step map[string]any, called *format.Document) {
	if called == nil || !t.once("inputs "+jsonptr.Format(root)+"\x00"+doc.File+"\x00"+jsonptr.Format(loc)) {
		return
	}
	defs, _ := jsonptr.Lookup(called.Value, definitionsPath)
	defsMap, _ := defs.(map[string]any)
	for _, input := range requiredLeftOut(defsMap, step["with"]) {
		c.reportAt(problems.ChildInputMissing, root, problems.Params{"input": input, "chain": problems.QuoteAll(chain)})
	}
}

// requiredLeftOut returns the names, as input_required names them
// (inputLabel), of the inputs that defs, a workflow's input definitions,
// require, which with, the with: of a step that calls the workflow, as it
// is written, leaves out: that it holds no value for, or "" for one none
// of whose options: is "", as the child's Create would find them
// (applyConditions). An input is required that has no default:, isn't a
// boolean, which is false without one, and that neither it nor a group or
// a wizard step around it makes hidden or ignored, and that isn't
// optional. What is known only as the step runs is the child's Create's
// to check, then: a value of with: that an expression gives, or a with:
// that is one, and an input whose hidden:, ignore: or optional:, or a
// group's or a step's hidden: or ignore:, is an expression.
func requiredLeftOut(defs map[string]any, with any) []string {
	if s, ok := with.(string); ok && strings.Contains(s, "${{") {
		return nil
	}
	var out []string
	var visit func(defs map[string]any, loc, path []string)
	visit = func(defs map[string]any, loc, path []string) {
		for _, in := range inputpath.Inputs(defs, loc, path) {
			hidden, hiddenKnown := writtenCondition(in.Def, "hidden")
			ignore, ignoreKnown := writtenCondition(in.Def, "ignore")
			if hidden || ignore || !hiddenKnown || !ignoreKnown {
				continue
			}
			if in.Container {
				visit(in.Inner())
				continue
			}
			_, hasDefault := in.Def["default"]
			_, hasItems := in.Def["items"]
			// A wizard step that holds no inputs, as one with multi true,
			// whose value is rows, has no value it requires; and format 1
			// reads a list with items: as a group, where Meridian reads it
			// as a list.
			if hasDefault || hasItems || in.Type == "header" || in.Type == "boolean" || in.Type == "step" {
				continue
			}
			if optional, known := writtenCondition(in.Def, "optional"); optional || !known {
				continue
			}
			if leftOut(with, in.Path, in.Def) {
				out = append(out, inputLabel(in.Path, in.Def))
			}
		}
	}
	visit(defs, definitionsPath, nil)
	return out
}

// writtenCondition returns the value of def's hidden, ignore or optional,
// key, as condition decides it, and whether the text says it: false for
// one that is an expression, which the run's values decide.
func writtenCondition(def map[string]any, key string) (decided, known bool) {
	v, ok := def[key]
	if !ok {
		return false, true
	}
	if s, isString := v.(string); isString && strings.Contains(s, "${{") {
		return false, false
	}
	return conditionValue(v), true
}

// leftOut reports whether with, a step's with: as it is written, leaves out
// the value at path of the input that def defines: whether it has none
// there, as applyConditions finds one missing, or "" for an input none of
// whose options: is "". A value that an expression gives, there or around
// it, is known only as the step runs.
func leftOut(with any, path []string, def map[string]any) bool {
	v := with
	for _, key := range path {
		if s, ok := v.(string); ok && strings.Contains(s, "${{") {
			return false
		}
		m, _ := v.(map[string]any)
		next, ok := m[key]
		if !ok {
			return true
		}
		v = next
	}
	if v != "" {
		return false
	}
	// "" is a choice of a dropdown's that has it among its options, as
	// findOptions finds it, and one an option's expression gives is known
	// only as the run's values are.
	options := def["options"]
	return !hasTemplate(options) && !slices.ContainsFunc(optionValues(options), func(o any) bool { return o == "" })
}
