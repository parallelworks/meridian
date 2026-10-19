package compile

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"

	"github.com/parallelworks/meridian/expr"
	"github.com/parallelworks/meridian/format"
	"github.com/parallelworks/meridian/plan"
)

// Codes of the problems the compiler reports about inputs.
const (
	// CodeRequired means an input that needs a value has none.
	CodeRequired format.Code = "required"
	// CodeUnknownInput means a value for an input the workflow doesn't
	// define.
	CodeUnknownInput format.Code = "unknown-input"
	// CodeInputValue means a value that doesn't fit its input: the wrong
	// type, outside a number's range, or not one of a dropdown's options.
	CodeInputValue format.Code = "input-value"
	// CodePlatform means an input of a type that only a platform provides,
	// with no platform to provide it.
	CodePlatform format.Code = "platform"
	// CodeExpression means an expression that can't be evaluated, or
	// expressions that read each other.
	CodeExpression format.Code = "expression"
)

// ErrInputType is what an InputResolver returns for a type it doesn't
// provide.
var ErrInputType = errors.New("compile: no such input type")

// An InputResolver provides the input types that name a platform's
// resources, such as clusters and Slurm partitions.
type InputResolver interface {
	// ResolveInput checks the value given for an input of a platform type
	// and returns what the run sees for it, such as a cluster's details for
	// its name. It returns ErrInputType for a type it doesn't provide.
	ResolveInput(typ string, definition map[string]any, value any) (any, error)
}

// platformTypes are the input types whose values name a platform's
// resources.
var platformTypes = map[string]bool{
	"compute-clusters": true, "compute-resources": true, "compute-target": true, "execution-groups": true,
	"kubernetes-clusters": true, "kubernetes-namespaces": true, "kubernetes-pods": true, "kubernetes-pvc": true,
	"kubernetes-secrets": true, "kubernetes-configmaps": true, "kubernetes-statefulsets": true,
	"kubernetes-deployments": true, "kubernetes-services": true, "kubernetes-workloads": true,
	"region": true, "zone": true, "instance-type": true, "bucket": true, "storage": true,
	"organization-groups": true, "slurm-accounts": true, "slurm-partitions": true, "slurm-qos": true,
	"pbs-queues": true, "ai-model": true, "allocations": true,
}

// Inputs are a run's input values.
type Inputs struct {
	// Values are the input values, with defaults applied and ignored inputs
	// left out. A default that reads something the compiler doesn't know,
	// such as a secret variable or env, stays a template, which is
	// evaluated once the run is underway.
	Values plan.Value
	// Secrets are the values of password inputs, by their path in the
	// values, such as k8s.token. The plan doesn't hold them.
	Secrets map[string]any
}

// Options are what the compiler reads besides the workflow.
type Options struct {
	// Inputs provides the input types that name a platform's resources.
	// Nil means there is no platform.
	Inputs InputResolver
	// Contexts are what expressions read when the run is created, besides
	// inputs: org, vars, remote and sessions. The value of a secret
	// variable is an expr.Unknown, so that an expression that reads it is
	// evaluated once the run is underway.
	Contexts map[string]any
}

// definitionsPath is where a workflow defines its inputs.
var definitionsPath = []string{"on", "execute", "inputs"}

// ResolveInputs checks a run's submitted input values against the workflow's
// input definitions, applies defaults, and drops ignored inputs, as
// docs/design.md describes. It reports every problem it finds.
func ResolveInputs(doc *format.Document, submitted map[string]any, opts Options) (*Inputs, format.Diagnostics) {
	r := &inputResolver{
		doc:     doc,
		opts:    opts,
		eval:    &expr.Document{Root: doc.Value, Definitions: definitionsPath},
		values:  copyMap(submitted),
		secrets: map[string]any{},
	}
	defs, _ := lookup(doc.Value, definitionsPath).(map[string]any)
	r.collect(defs, definitionsPath, nil, nil)
	r.checkUnknown(submitted, nil)
	for _, f := range r.fields {
		if f.header {
			deletePath(r.values, f.path)
		}
	}
	r.fillDefaults()
	r.applyConditions()
	r.checkValues()
	r.diags.Sort()
	return &Inputs{Values: plan.Literal(r.values), Secrets: r.secrets}, r.diags
}

// A field is an input definition.
type field struct {
	key string
	def map[string]any
	// loc is the definition's path in the workflow, and path its value's
	// path in the values.
	loc, path []string
	typ       string
	group     *field
	// container is set for a group or a wizard step, which holds other
	// inputs rather than a value of its own; flatten for a step whose
	// inputs' values are beside its own key rather than under it.
	container, flatten bool
	// children are a container's inputs.
	children []*field
	// header is set for a header, which has no value.
	header bool

	hasDefault bool
	hidden     bool
	ignored    bool
	optional   bool
	deferred   bool
}

type inputResolver struct {
	doc     *format.Document
	opts    Options
	eval    *expr.Document
	values  map[string]any
	secrets map[string]any
	fields  []*field
	diags   format.Diagnostics
}

// report reports a problem with what is at loc in the workflow: a mapping,
// such as an input's definition, at its key, and anything else where it
// starts.
func (r *inputResolver) report(severity format.Severity, code format.Code, loc []string, msg string, args ...any) {
	ptr := pointer(loc)
	pos := r.doc.Position(ptr)
	if _, isMapping := lookup(r.doc.Value, loc).(map[string]any); isMapping {
		pos = r.doc.KeyPosition(ptr)
	}
	r.diags = append(r.diags, format.Diagnostic{
		File: r.doc.File, Position: pos, Severity: severity, Code: code, Path: ptr,
		Message: fmt.Sprintf(msg, args...),
	})
}

// collect records the definitions in defs, at loc in the workflow, whose
// values are at path.
func (r *inputResolver) collect(defs map[string]any, loc, path []string, group *field) {
	flattenSteps := true
	if meta, ok := defs["$meta"].(map[string]any); ok {
		if wizard, ok := meta["wizard"].(map[string]any); ok && wizard["flatten"] == false {
			flattenSteps = false
		}
	}
	for _, key := range slices.Sorted(maps.Keys(defs)) {
		def, ok := defs[key].(map[string]any)
		if !ok || strings.HasPrefix(key, "$") {
			continue
		}
		typ, _ := def["type"].(string)
		f := &field{key: key, def: def, loc: appendPath(loc, key), path: appendPath(path, key), typ: typ, group: group}
		_, f.hasDefault = def["default"]
		r.fields = append(r.fields, f)
		if group != nil {
			group.children = append(group.children, f)
		}
		switch items, hasItems := def["items"].(map[string]any); {
		case typ == "header":
			f.header = true
		case typ == "step":
			options, ok := def["options"].(map[string]any)
			if !ok || def["multi"] == true {
				continue
			}
			f.container, f.flatten = true, flattenSteps
			r.collect(options, appendPath(f.loc, "options"), f.childPath(), f)
		case hasItems && typ != "list":
			f.container = true
			r.collect(items, appendPath(f.loc, "items"), f.childPath(), f)
		}
	}
}

// childPath is where a container's inputs' values are.
func (f *field) childPath() []string {
	if f.flatten {
		return f.path[:len(f.path)-1]
	}
	return f.path
}

// label names an input in messages: its path, and its label if it has one.
func (f *field) label() string {
	name := strings.Join(f.path, ".")
	if l, ok := f.def["label"].(string); ok && l != "" && l != name {
		return fmt.Sprintf("%s (%s)", name, l)
	}
	return name
}

// checkUnknown reports submitted values for inputs the workflow doesn't
// define, at path in the values.
func (r *inputResolver) checkUnknown(values map[string]any, path []string) {
	known := map[string]*field{}
	for _, f := range r.fields {
		if !f.container || !f.flatten {
			if slices.Equal(f.path[:len(f.path)-1], path) {
				known[f.key] = f
			}
		}
	}
	for _, key := range slices.Sorted(maps.Keys(values)) {
		f, ok := known[key]
		if !ok {
			names := slices.Sorted(maps.Keys(known))
			msg := fmt.Sprintf("%s isn't an input of this workflow", strings.Join(appendPath(path, key), "."))
			if s, ok := suggest(key, names); ok {
				msg += fmt.Sprintf("; did you mean %s?", strings.Join(appendPath(path, s), "."))
			}
			r.report(format.Error, CodeUnknownInput, definitionsPath, "%s", msg)
			continue
		}
		if !f.container || f.flatten || values[key] == nil {
			continue
		}
		child, ok := values[key].(map[string]any)
		if !ok {
			r.report(format.Error, CodeInputValue, f.loc, "%s takes a mapping of its inputs' values, not %s", f.label(), describe(values[key]))
			delete(r.values, key)
			continue
		}
		r.checkUnknown(child, f.path)
	}
}

// fillDefaults gives each input without a value its default. A default that
// is an expression is evaluated with the other inputs' values, defaults
// included, in the order they need.
func (r *inputResolver) fillDefaults() {
	var pending []*field
	for _, f := range r.fields {
		if f.container || f.header || !r.missing(f) || !f.hasDefault {
			continue
		}
		def, err := plan.ValueOf(f.def["default"])
		if err != nil {
			continue // The schema checks reported it.
		}
		if v, ok := def.Literal(); ok {
			r.setDefault(f, v)
			continue
		}
		pending = append(pending, f)
	}
	for len(pending) > 0 {
		progress := false
		for i := 0; i < len(pending); i++ {
			f := pending[i]
			if r.readsPending(f, pending) {
				continue
			}
			pending = slices.Delete(pending, i, i+1)
			i--
			progress = true
			v, err := r.evalAt(appendPath(f.loc, "default"))
			var ue *expr.UnknownError
			switch {
			case errors.As(err, &ue):
				// Known once the run is underway: the template stays.
				t, _ := expr.ParseTemplate(f.def["default"].(string))
				setPath(r.values, f.path, t)
				f.deferred = true
			case err != nil:
				r.report(format.Error, CodeExpression, appendPath(f.loc, "default"), "the default of %s: %v", f.label(), err)
			default:
				r.setDefault(f, v)
			}
		}
		if !progress {
			names := make([]string, len(pending))
			for i, f := range pending {
				names[i] = strings.Join(f.path, ".")
			}
			r.report(format.Error, CodeExpression, pending[0].loc, "the defaults of %s read each other", strings.Join(names, " and "))
			return
		}
	}
}

// setDefault sets f's value to its default, unless the default is "", which
// gives no value.
func (r *inputResolver) setDefault(f *field, v any) {
	if v != "" && v != nil {
		setPath(r.values, f.path, v)
	}
}

// readsPending reports whether f's default reads another input whose default
// isn't known yet.
func (r *inputResolver) readsPending(f *field, pending []*field) bool {
	t, err := expr.ParseTemplate(f.def["default"].(string))
	if err != nil {
		return false
	}
	exprs, _ := t.Exprs()
	for _, e := range exprs {
		for _, ref := range e.Refs() {
			if ref.Context != "inputs" {
				continue
			}
			for _, p := range pending {
				if p != f && overlaps(ref.Path, p.path) {
					return true
				}
			}
		}
	}
	return false
}

// overlaps reports whether one path leads to the other.
func overlaps(a, b []string) bool {
	n := min(len(a), len(b))
	return slices.Equal(a[:n], b[:n])
}

// missing reports whether f has no value: none at all, "" when "" isn't one
// of its options, or the text of its default when that is an expression,
// which a form sends when it can't evaluate the default itself.
func (r *inputResolver) missing(f *field) bool {
	v, ok := lookupPath(r.values, f.path)
	if !ok || v == nil {
		return true
	}
	if s, isString := v.(string); isString && s != "" && s == f.def["default"] && strings.Contains(s, "${{") {
		return true
	}
	return v == "" && !hasEmptyOption(f.def["options"])
}

func hasEmptyOption(options any) bool {
	for _, o := range optionValues(options) {
		if o == "" {
			return true
		}
	}
	return false
}

// applyConditions evaluates each input's hidden, ignore and optional, drops
// the values of ignored inputs, and reports required inputs with no value.
func (r *inputResolver) applyConditions() {
	for _, f := range r.fields {
		if f.group != nil && f.group.ignored {
			f.ignored = true
			continue
		}
		f.hidden = r.condition(f, "hidden") || f.group != nil && f.group.hidden
		f.ignored = r.condition(f, "ignore")
		f.optional = r.condition(f, "optional")
		if f.ignored {
			r.drop(f)
		}
	}
	for _, f := range r.fields {
		if f.container || f.header || f.ignored || f.hasDefault || !r.missing(f) {
			continue
		}
		switch {
		case f.optional || f.hidden:
		case f.typ == "boolean":
			setPath(r.values, f.path, false)
		default:
			r.report(format.Error, CodeRequired, f.loc, "%s needs a value", f.label())
		}
	}
}

// condition evaluates one of f's hidden, ignore and optional, which is true
// if it is true or the text "true".
func (r *inputResolver) condition(f *field, key string) bool {
	raw, ok := f.def[key]
	if !ok {
		return false
	}
	v := raw
	if _, isString := raw.(string); isString {
		var err error
		if v, err = r.evalAt(appendPath(f.loc, key)); err != nil {
			r.report(format.Error, CodeExpression, appendPath(f.loc, key), "the %s of %s: %v", key, f.label(), err)
			return false
		}
	}
	switch v := v.(type) {
	case bool:
		return v
	case string:
		return strings.EqualFold(v, "true")
	}
	return false
}

// drop removes the values of an ignored input, and of the inputs it holds.
func (r *inputResolver) drop(f *field) {
	if !f.container || !f.flatten {
		deletePath(r.values, f.path)
		return
	}
	for _, c := range f.children {
		r.drop(c)
	}
}

// evalAt evaluates the string at loc in the workflow, where it is, with the
// values so far.
func (r *inputResolver) evalAt(loc []string) (any, error) {
	t, err := expr.ParseTemplate(lookup(r.doc.Value, loc).(string))
	if err != nil {
		return nil, err
	}
	contexts := map[string]any{"inputs": knownValues(r.values)}
	for k, v := range r.opts.Contexts {
		if k != "inputs" {
			contexts[k] = v
		}
	}
	return t.Eval(r.eval.Scope(loc, expr.Scope{Contexts: contexts}))
}

// knownValues copies values with each deferred default as an expr.Unknown,
// so that an expression that reads one can't be evaluated yet.
func knownValues(v any) any {
	switch v := v.(type) {
	case *expr.Template:
		return expr.Unknown{Name: "inputs"}
	case map[string]any:
		out := make(map[string]any, len(v))
		for k, item := range v {
			out[k] = knownValues(item)
		}
		return out
	case []any:
		out := make([]any, len(v))
		for i, item := range v {
			out[i] = knownValues(item)
		}
		return out
	}
	return v
}

// checkValues checks each value against its input's type, converts numbers
// and booleans given as text, resolves platform types, and moves password
// values to the secrets.
func (r *inputResolver) checkValues() {
	for _, f := range r.fields {
		if f.header || f.ignored || f.deferred {
			continue
		}
		v, ok := lookupPath(r.values, f.path)
		if !ok || v == nil {
			continue
		}
		if f.container {
			continue
		}
		out, err := r.checkValue(f, v)
		if err != nil {
			code := CodeInputValue
			if errors.Is(err, errNoPlatform) {
				code = CodePlatform
			}
			r.report(format.Error, code, f.loc, "%s: %v", f.label(), err)
			continue
		}
		if f.typ == "password" {
			r.secrets[strings.Join(f.path, ".")] = out
			deletePath(r.values, f.path)
			continue
		}
		setPath(r.values, f.path, out)
	}
}

var errNoPlatform = errors.New("needs a platform that provides it")

func (r *inputResolver) checkValue(f *field, v any) (any, error) {
	switch {
	case platformTypes[f.typ]:
		if r.opts.Inputs == nil {
			return nil, fmt.Errorf("an input of type %s %w", f.typ, errNoPlatform)
		}
		out, err := r.opts.Inputs.ResolveInput(f.typ, f.def, v)
		if errors.Is(err, ErrInputType) {
			return nil, fmt.Errorf("an input of type %s %w", f.typ, errNoPlatform)
		}
		return out, err
	case f.typ == "number":
		n, ok := toNumber(v)
		if !ok {
			return nil, fmt.Errorf("takes a number, not %s", describe(v))
		}
		if lo, ok := toNumber(f.def["min"]); ok && n < lo {
			return nil, fmt.Errorf("%s is less than its minimum, %s", formatNumber(n), formatNumber(lo))
		}
		if hi, ok := toNumber(f.def["max"]); ok && n > hi {
			return nil, fmt.Errorf("%s is more than its maximum, %s", formatNumber(n), formatNumber(hi))
		}
		return number(n), nil
	case f.typ == "boolean":
		switch v := v.(type) {
		case bool:
			return v, nil
		case string:
			switch strings.ToLower(strings.TrimSpace(v)) {
			case "true":
				return true, nil
			case "false":
				return false, nil
			}
		}
		return nil, fmt.Errorf("takes true or false, not %s", describe(v))
	case f.typ == "list":
		if _, ok := v.([]any); !ok {
			return nil, fmt.Errorf("takes a list, not %s", describe(v))
		}
		return v, nil
	case f.typ == "multi-dropdown":
		list, ok := v.([]any)
		if !ok {
			return nil, fmt.Errorf("takes a list of its options, not %s", describe(v))
		}
		for _, item := range list {
			if err := checkOption(f, item); err != nil {
				return nil, err
			}
		}
		return v, nil
	case f.typ == "dropdown":
		if err := checkOption(f, v); err != nil {
			return nil, err
		}
		return v, nil
	}
	// Text: string, password, editor, color-picker and the like.
	switch v := v.(type) {
	case string:
		return v, nil
	case bool:
		return strconv.FormatBool(v), nil
	case map[string]any, []any:
		return nil, fmt.Errorf("takes text, not %s", describe(v))
	}
	if n, ok := toNumber(v); ok {
		return formatNumber(n), nil
	}
	return v, nil
}

// checkOption checks that v is one of a dropdown's options, when they are
// written in the workflow rather than computed.
func checkOption(f *field, v any) error {
	options := optionValues(f.def["options"])
	if len(options) == 0 || hasTemplate(f.def["options"]) {
		return nil
	}
	for _, o := range options {
		if looseEqual(o, v) {
			return nil
		}
	}
	shown := make([]string, len(options))
	for i, o := range options {
		shown[i] = describe(o)
	}
	return fmt.Errorf("%s isn't one of its options, which are %s", describe(v), strings.Join(shown, ", "))
}

// optionValues returns a dropdown's option values: its options' values, or
// for options grouped by key, every group's.
func optionValues(options any) []any {
	var out []any
	switch o := options.(type) {
	case []any:
		for _, item := range o {
			if m, ok := item.(map[string]any); ok {
				out = append(out, m["value"])
			} else {
				out = append(out, item)
			}
		}
	case map[string]any:
		for _, k := range slices.Sorted(maps.Keys(o)) {
			out = append(out, optionValues(o[k])...)
		}
	}
	return out
}

func hasTemplate(v any) bool {
	switch v := v.(type) {
	case string:
		return strings.Contains(v, "${{")
	case []any:
		return slices.ContainsFunc(v, hasTemplate)
	case map[string]any:
		for _, item := range v {
			if hasTemplate(item) {
				return true
			}
		}
	}
	return false
}
