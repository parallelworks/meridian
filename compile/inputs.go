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
	"github.com/parallelworks/meridian/internal/hint"
	"github.com/parallelworks/meridian/internal/jsonptr"
	"github.com/parallelworks/meridian/internal/jsonvalue"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
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
	// left out.
	Values map[string]any
	// Deferred are the defaults that read something the compiler doesn't
	// know, such as a secret variable, by their input's path, as in
	// group.name. They are evaluated once the run is underway.
	Deferred map[string]plan.Value
	// Secrets are the values of password inputs, by their path in the
	// values, such as k8s.token. The plan doesn't hold them.
	Secrets map[string]any
}

// Options are what the compiler reads besides the workflow.
type Options struct {
	// Source names the workflow. Without a Revision, the revision is the
	// SHA-256 digest of the workflow's text.
	Source plan.Source
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
func ResolveInputs(doc *format.Document, submitted map[string]any, opts Options) (*Inputs, problems.Diagnostics) {
	r := resolveInputs(doc, submitted, opts)
	return &Inputs{Values: r.values, Deferred: r.deferred(), Secrets: r.secrets}, r.diags
}

// resolveInputs resolves the inputs, keeping their definitions for the
// compiler's checks of what reads them.
func resolveInputs(doc *format.Document, submitted map[string]any, opts Options) *inputResolver {
	r := &inputResolver{
		doc:      doc,
		opts:     opts,
		workflow: &expr.Workflow{Root: doc.Value, Definitions: definitionsPath},
		budget:   expr.Scope{}.Budgeted(),
		values:   jsonvalue.CloneMap(submitted),
		secrets:  map[string]any{},
	}
	defs, _ := lookup(doc.Value, definitionsPath).(map[string]any)
	r.collect(defs, definitionsPath, nil, nil)
	r.checkJSON(r.values)
	r.checkUnknown(r.values, nil)
	for _, f := range r.fields {
		if f.header {
			deletePath(r.values, f.path)
		}
	}
	r.fillDefaults()
	r.applyConditions()
	r.checkValues()
	r.diags.Sort()
	return r
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
	// deferred is set for an input whose default reads something the
	// compiler doesn't know, and dflt holds the default.
	deferred bool
	dflt     plan.Value
}

type inputResolver struct {
	reporter
	opts     Options
	workflow *expr.Workflow
	// budget holds the one budget that every evaluation of the inputs'
	// expressions shares.
	budget  expr.Scope
	values  map[string]any
	secrets map[string]any
	fields  []*field
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
	for key, v := range jsonvalue.Sorted(defs) {
		def, ok := v.(map[string]any)
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

// checkJSON reports, and drops, submitted values that aren't JSON values,
// which a plan can't hold.
func (r *inputResolver) checkJSON(values map[string]any) {
	for key, v := range jsonvalue.Sorted(values) {
		if what, bad := notJSON(v); what != "" {
			r.reportAt(problems.InputNotJSON, definitionsPath, problems.Params{"input": key, "what": what, "value": bad})
			delete(values, key)
		}
	}
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
	for key, v := range jsonvalue.Sorted(values) {
		f, ok := known[key]
		if !ok {
			s, ok := hint.Suggest(key, slices.Sorted(maps.Keys(known)))
			params := problems.Params{"input": strings.Join(appendPath(path, key), ".")}
			r.reportAt(problems.UnknownInput, definitionsPath, problems.WithSuggestion(params, strings.Join(appendPath(path, s), "."), ok))
			continue
		}
		if !f.container || f.flatten || v == nil {
			continue
		}
		child, ok := v.(map[string]any)
		if !ok {
			r.reportAt(problems.InputGroupNotMapping, f.loc, problems.Params{"input": f.label(), "value": problems.Show(v)})
			delete(values, key)
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
		f.dflt = def
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
			switch {
			case expr.IsUnknown(err):
				// Known once the run is underway.
				f.deferred = true
			case err != nil:
				r.reportError(appendPath(f.loc, "default"), 0, err)
			default:
				r.setDefault(f, v)
			}
		}
		if !progress {
			names := make([]string, len(pending))
			for i, f := range pending {
				names[i] = strings.Join(f.path, ".")
			}
			r.reportAt(problems.DefaultsReadEachOther, pending[0].loc, problems.Params{"inputs": problems.QuoteAll(names)})
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
	for _, e := range f.dflt.Exprs() {
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
	v, ok := jsonptr.Lookup(r.values, f.path)
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
			r.reportAt(problems.InputRequired, f.loc, problems.Params{"input": f.label()})
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
			if u, ok := errors.AsType[*expr.UnknownError](err); ok {
				r.reportAt(problems.InputConditionNotKnown, appendPath(f.loc, key),
					problems.Params{"key": key, "input": f.label(), "name": u.Name})
			} else {
				r.reportError(appendPath(f.loc, key), 0, err)
			}
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

// evalAt evaluates what is at loc in the workflow, where it is, with the
// values so far: the expressions in a string, and in each string of a list or
// mapping.
func (r *inputResolver) evalAt(loc []string) (any, error) {
	scope := r.budget
	scope.Contexts = compileTimeContexts(r.opts, r.evalValues())
	return r.workflow.Eval(loc, scope)
}

// deferred returns the defaults that are evaluated once the run is
// underway, by their inputs' paths.
func (r *inputResolver) deferred() map[string]plan.Value {
	var out map[string]plan.Value
	for _, f := range r.fields {
		if f.deferred && !f.ignored {
			if out == nil {
				out = map[string]plan.Value{}
			}
			out[strings.Join(f.path, ".")] = f.dflt
		}
	}
	return out
}

// evalValues returns the values for expressions to read: each deferred
// default unknown, and each input with no value null, or false for a
// boolean, which is the value it gets, rather than its definition.
func (r *inputResolver) evalValues() any {
	values := jsonvalue.CloneMap(r.values)
	for _, f := range r.fields {
		if f.container || f.header {
			continue
		}
		if f.deferred && !f.ignored {
			setPath(values, f.path, expr.Unknown{Name: "inputs"})
			continue
		}
		if _, ok := jsonptr.Lookup(values, f.path); ok {
			continue
		}
		var v any
		if f.typ == "boolean" && !f.hasDefault {
			v = false
		}
		setPath(values, f.path, v)
	}
	return values
}

// compileTimeContexts returns the contexts known when the run is created:
// inputs, and the caller's org, vars, remote and sessions. org, vars and
// remote default to empty, which reads every name as "".
func compileTimeContexts(opts Options, inputs any) map[string]any {
	contexts := map[string]any{"org": map[string]any{}, "vars": map[string]any{}, "remote": map[string]any{}}
	maps.Copy(contexts, opts.Contexts)
	contexts["inputs"] = inputs
	return contexts
}

// checkValues checks each value against its input's type, converts numbers
// and booleans given as text, resolves platform types, and moves password
// values to the secrets.
func (r *inputResolver) checkValues() {
	for _, f := range r.fields {
		if f.header || f.ignored || f.deferred {
			continue
		}
		v, ok := jsonptr.Lookup(r.values, f.path)
		if !ok || v == nil {
			continue
		}
		if f.container {
			continue
		}
		out, bad := r.checkValue(f, v)
		if bad.kind != nil {
			bad.params["input"] = f.label()
			r.reportAt(bad.kind, f.loc, bad.params)
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

// checkValue checks v, the value of f, and returns it as the run sees it, or
// what is wrong with it: a finding whose params the caller adds the input
// to.
func (r *inputResolver) checkValue(f *field, v any) (any, finding) {
	wrong := func(k *problems.Kind, params problems.Params) (any, finding) {
		if params == nil {
			params = problems.Params{}
		}
		return nil, finding{k, params}
	}
	switch {
	case platformTypes[f.typ]:
		if r.opts.Inputs == nil {
			return wrong(problems.PlatformInput, problems.Params{"type": f.typ})
		}
		out, err := r.opts.Inputs.ResolveInput(f.typ, f.def, v)
		switch {
		case errors.Is(err, ErrInputType):
			return wrong(problems.PlatformInput, problems.Params{"type": f.typ})
		case err != nil:
			return wrong(problems.InputRejected, problems.Params{"reason": err.Error()})
		}
		if what, bad := notJSON(out); what != "" {
			return wrong(problems.PlatformValueNotJSON, problems.Params{"what": what, "value": bad})
		}
		return out, finding{}
	case f.typ == "number":
		n, ok := toNumber(v)
		if !ok {
			return wrong(problems.InputNotNumber, problems.Params{"value": problems.Show(v)})
		}
		if lo, ok := toNumber(f.def["min"]); ok && n < lo {
			return wrong(problems.InputBelowMin, problems.Params{"value": formatNumber(n), "min": formatNumber(lo)})
		}
		if hi, ok := toNumber(f.def["max"]); ok && n > hi {
			return wrong(problems.InputAboveMax, problems.Params{"value": formatNumber(n), "max": formatNumber(hi)})
		}
		return number(n), finding{}
	case f.typ == "boolean":
		switch v := v.(type) {
		case bool:
			return v, finding{}
		case string:
			switch strings.ToLower(strings.TrimSpace(v)) {
			case "true":
				return true, finding{}
			case "false":
				return false, finding{}
			}
		}
		return wrong(problems.InputNotBoolean, problems.Params{"value": problems.Show(v)})
	case f.typ == "list":
		if _, ok := v.([]any); !ok {
			return wrong(problems.InputNotList, problems.Params{"value": problems.Show(v)})
		}
		return v, finding{}
	case f.typ == "multi-dropdown":
		list, ok := v.([]any)
		if !ok {
			return wrong(problems.InputNotOptionList, problems.Params{"value": problems.Show(v)})
		}
		for _, item := range list {
			if bad := checkOption(f, item); bad.kind != nil {
				return nil, bad
			}
		}
		return v, finding{}
	case f.typ == "dropdown":
		if bad := checkOption(f, v); bad.kind != nil {
			return nil, bad
		}
		return v, finding{}
	}
	// Text: string, password, editor, color-picker and the like.
	switch v := v.(type) {
	case string:
		return v, finding{}
	case bool:
		return strconv.FormatBool(v), finding{}
	case map[string]any, []any:
		return wrong(problems.InputNotText, problems.Params{"value": problems.Show(v)})
	}
	if n, ok := toNumber(v); ok {
		return formatNumber(n), finding{}
	}
	return v, finding{}
}

// checkOption checks that v is one of a dropdown's options, when they are
// written in the workflow rather than computed.
func checkOption(f *field, v any) finding {
	options := optionValues(f.def["options"])
	if len(options) == 0 || hasTemplate(f.def["options"]) {
		return finding{}
	}
	for _, o := range options {
		if looseEqual(o, v) {
			return finding{}
		}
	}
	shown := make([]string, len(options))
	for i, o := range options {
		shown[i] = problems.Show(o)
	}
	return finding{problems.InputNotOption, problems.Params{"value": problems.Show(v), "options": problems.List(shown)}}
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
		for _, group := range jsonvalue.Sorted(o) {
			out = append(out, optionValues(group)...)
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
