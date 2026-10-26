package main

import (
	"encoding/json/jsontext"
	"errors"
	"io"
	"maps"
	"math"
	"math/big"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/parallelworks/meridian/internal/jsonvalue"
	"github.com/parallelworks/meridian/problems"
)

// An inputSet is one -input, path=value: the names of the path, joined
// with dots, and the value's text, which its problems never repeat, as it
// can be a password.
type inputSet struct {
	path  []string
	value string
}

// parseInputSets reads each -input as path=value, where a path is input
// names joined with dots.
func parseInputSets(flags []string) ([]inputSet, error) {
	sets := make([]inputSet, 0, len(flags))
	for i, flag := range flags {
		path, value, ok := strings.Cut(flag, "=")
		if !ok {
			return nil, problems.InputFlagUnsplit.Problem(problems.Params{"position": i + 1})
		}
		names := strings.Split(path, ".")
		if slices.Contains(names, "") {
			return nil, problems.InputPathInvalid.Problem(problems.Params{"path": path})
		}
		sets = append(sets, inputSet{path: names, value: value})
	}
	return sets, nil
}

// setInputs sets each of sets in values, in order, converted by the type
// defs, the workflow's input definitions, give its input, as ACTIVATE's pw
// workflows run --input does. A dropdown whose options an option-key
// picks by another input's value takes its value again once every value
// is set, unless a later -input sets it, or a group it is in.
func setInputs(values, defs map[string]any, sets []inputSet) error {
	var keyed []int
	for i, s := range sets {
		again, err := s.apply(values, defs, nil)
		if err != nil {
			return err
		}
		if again {
			keyed = append(keyed, i)
		}
	}
	for _, i := range keyed {
		if replacedLater(sets[i].path, sets[i+1:]) {
			continue
		}
		if _, err := sets[i].apply(values, defs, values); err != nil {
			return err
		}
	}
	return nil
}

// replacedLater reports whether one of later sets the input at path, or a
// group it is in.
func replacedLater(path []string, later []inputSet) bool {
	return slices.ContainsFunc(later, func(s inputSet) bool {
		return len(s.path) <= len(path) && slices.Equal(s.path, path[:len(s.path)])
	})
}

// apply sets s's input in values, by its type in defs; root, if it isn't
// nil, holds every value set, which a dropdown's option-key picks its
// options by. keyed says that the input is such a dropdown, read before
// every value was set.
func (s inputSet) apply(values, defs, root map[string]any) (keyed bool, err error) {
	for i, name := range s.path {
		last := i == len(s.path)-1
		def, siblings, defined := findInput(defs, name)
		if !defined {
			// An input the workflow doesn't define is given its text.
			if last {
				values[name] = s.value
				return false, nil
			}
			if values, err = s.object(values, i); err != nil {
				return false, err
			}
			defs = nil
			continue
		}
		kind, fields := classifyInput(siblings, def)
		switch {
		case kind == inputFlattened:
			return false, s.notSettable(i, def, "flattened")
		case last:
			options, dependent := dropdownOptions(def, root)
			v, err := s.convert(def, options, i)
			if err != nil {
				return false, err
			}
			values[name] = v
			return dependent && root == nil, nil
		case kind == inputList:
			return false, s.notSettable(i, def, "rows")
		case kind != inputNested:
			return false, s.notSettable(i, def, "leaf")
		}
		if values, err = s.object(values, i); err != nil {
			return false, err
		}
		defs = fields
	}
	return false, nil
}

// object returns the mapping in values at the path's i'th name, making it
// if there is none.
func (s inputSet) object(values map[string]any, i int) (map[string]any, error) {
	name := s.path[i]
	switch v := values[name].(type) {
	case nil:
		m := map[string]any{}
		values[name] = m
		return m, nil
	case map[string]any:
		return v, nil
	}
	return nil, problems.InputPathConflict.Problem(problems.Params{"path": strings.Join(s.path[:i+1], ".")})
}

// params returns the params of a problem with s, at the input at the
// path's i'th name.
func (s inputSet) params(i int, def map[string]any) problems.Params {
	return problems.Params{"path": strings.Join(s.path, "."), "input": strings.Join(s.path[:i+1], "."), "type": typeOf(def)}
}

func (s inputSet) notSettable(i int, def map[string]any, reason string) error {
	params := s.params(i, def)
	params["reason"], params["next"] = reason, ""
	if i+1 < len(s.path) {
		params["next"] = s.path[i+1]
	}
	return problems.InputFlagPathNotSettable.Problem(params)
}

// convert reads s's text as a value of def's type, a dropdown's among
// options: a boolean true or false, exactly; a number a JSON number; a
// structure JSON of its shape; and every type of text, and a dropdown
// whose options hold none the text reads as, the text as it is.
func (s inputSet) convert(def map[string]any, options any, i int) (any, error) {
	wrongType := func(reason string) error {
		params := s.params(i, def)
		params["reason"] = reason
		return problems.InputFlagWrongType.Problem(params)
	}
	switch typeOf(def) {
	case "boolean":
		if b, ok := parseBoolean(s.value); ok {
			return b, nil
		}
		return nil, wrongType("")
	case "number":
		n, err := parseNumber(s.value)
		if err != nil {
			return nil, wrongType(err.Error())
		}
		return n, nil
	case "dropdown":
		return choice(options, s.value), nil
	case "header":
		return nil, wrongType("")
	}
	shape, structured := structureOf(def)
	if !structured {
		return s.value, nil
	}
	v, reason, offset := parseJSON(s.value)
	if reason != "" {
		params := s.params(i, def)
		params["shape"], params["reason"], params["offset"] = shape, reason, offset
		return nil, problems.InputFlagNotJSON.Problem(params)
	}
	if !isShape(v, shape) {
		params := s.params(i, def)
		params["shape"], params["kind"] = shape, kindOf(v)
		return nil, problems.InputFlagWrongShape.Problem(params)
	}
	return v, nil
}

const (
	shapeArray  = "array"
	shapeObject = "object"
)

// structureOf says whether def's value is a structure, and which shape: an
// array or an object.
func structureOf(def map[string]any) (string, bool) {
	multi := def["multi"] == true
	switch typeOf(def) {
	case "list", "multi-dropdown":
		return shapeArray, true
	case "group", "compute-target":
		return shapeObject, true
	case "step":
		if multi {
			return shapeArray, true
		}
		return shapeObject, true
	case "compute-clusters", "compute-resources", "bucket":
		if multi {
			return shapeArray, true
		}
	}
	return "", false
}

func isShape(v any, shape string) bool {
	switch v.(type) {
	case []any:
		return shape == shapeArray
	case map[string]any:
		return shape == shapeObject
	}
	return false
}

// kindOf names the JSON type of a value parseJSON read.
func kindOf(v any) string {
	switch v.(type) {
	case bool:
		return "boolean"
	case int64, float64:
		return "number"
	case string:
		return "string"
	case []any:
		return "array"
	case map[string]any:
		return "object"
	}
	return "null"
}

func parseBoolean(s string) (bool, bool) {
	switch s {
	case "true":
		return true, true
	case "false":
		return false, true
	}
	return false, false
}

// jsonNumberGrammar is JSON's number grammar (RFC 8259), which has no 007,
// 0x10, +1, .5 or 1.
var jsonNumberGrammar = regexp.MustCompile(`^-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?$`)

// errTooLarge is parseNumber's error for a number too large for 64 bits,
// and errNotNumber for text that isn't a JSON number.
var (
	errTooLarge  = errors.New("too_large")
	errNotNumber = errors.New("not_number")
)

// parseNumber reads s as a JSON number, as the file of inputs reads one: an
// int64 when it is whole and fits, and a float64 otherwise.
func parseNumber(s string) (any, error) {
	if !jsonNumberGrammar.MatchString(s) {
		return nil, errNotNumber
	}
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		return n, nil
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil || math.IsInf(f, 0) {
		return nil, errTooLarge
	}
	return f, nil
}

// parseJSON reads s as one JSON value, with its numbers as parseNumber
// reads them, or says what is wrong with it: empty, extra, number, for a
// number too large to read, or syntax, at the byte offset.
func parseJSON(s string) (v any, reason string, offset int64) {
	if strings.TrimSpace(s) == "" {
		return nil, "empty", 0
	}
	// A key given twice has its last value, as ACTIVATE's reads it.
	dec := jsontext.NewDecoder(strings.NewReader(s), jsontext.AllowDuplicateNames(true))
	v, err := jsonvalue.Decode(dec, func(text string) (any, error) {
		n, err := parseNumber(text)
		if err != nil {
			return nil, errTooLarge
		}
		return n, nil
	})
	switch {
	case errors.Is(err, errTooLarge):
		return nil, "number", 0
	case err != nil:
		if se, ok := errors.AsType[*jsontext.SyntacticError](err); ok {
			return nil, "syntax", se.ByteOffset
		}
		return nil, "syntax", dec.InputOffset()
	}
	if _, err := dec.ReadToken(); !errors.Is(err, io.EOF) {
		return nil, "extra", 0
	}
	return v, "", 0
}

// dropdownOptions returns a dropdown's options: the list its option-key
// picks by an input's value in root, or every list when root doesn't
// have it, and whether an option-key picks them.
func dropdownOptions(def, root map[string]any) (options any, dependent bool) {
	options = def["options"]
	key, _ := def["option-key"].(string)
	byKey, isMap := options.(map[string]any)
	if key == "" || !isMap {
		return options, false
	}
	if value, ok := keyValue(key, root); ok {
		if list, ok := byKey[value].([]any); ok {
			return list, true
		}
	}
	return options, true
}

var inputReference = regexp.MustCompile(`^\$\{\{\s*inputs\.([A-Za-z0-9_-]+(?:\.[A-Za-z0-9_-]+)*)\s*\}\}$`)

// keyValue returns the text of the value of the input an option-key reads,
// from root.
func keyValue(key string, root map[string]any) (string, bool) {
	match := inputReference.FindStringSubmatch(key)
	if match == nil || root == nil {
		return "", false
	}
	var v any = root
	for name := range strings.SplitSeq(match[1], ".") {
		m, ok := v.(map[string]any)
		if !ok {
			return "", false
		}
		v = m[name]
	}
	switch v := v.(type) {
	case string:
		return v, true
	case bool:
		return strconv.FormatBool(v), true
	case int64:
		return strconv.FormatInt(v, 10), true
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64), true
	}
	return "", false
}

// choice returns the value of the dropdown option text names: the text,
// unless no option's value is that text and an option's value is a number
// or a boolean the text reads as, as a run form sends one.
func choice(options any, text string) any {
	values := optionValues(options)
	if slices.Contains(values, any(text)) {
		return text
	}
	for _, v := range values {
		if b, ok := v.(bool); ok {
			if t, ok := parseBoolean(text); ok && t == b {
				return v
			}
			continue
		}
		option, ok := exactNumber(v)
		if !ok || !jsonNumberGrammar.MatchString(text) {
			continue
		}
		if given, ok := new(big.Float).SetPrec(numberPrecision).SetString(text); ok && given.Cmp(option) == 0 {
			return v
		}
	}
	return text
}

// numberPrecision holds every whole number an option can be, so two that
// differ never compare equal.
const numberPrecision = 256

// optionValues lists a dropdown's option values, from its list or every
// option-key's list, {category, options} groups among them.
func optionValues(options any) []any {
	var lists [][]any
	if list, ok := options.([]any); ok {
		lists = append(lists, list)
	} else if byKey, ok := options.(map[string]any); ok {
		for _, key := range slices.Sorted(maps.Keys(byKey)) {
			if list, ok := byKey[key].([]any); ok {
				lists = append(lists, list)
			}
		}
	}
	var values []any
	for len(lists) > 0 {
		list := lists[0]
		lists = lists[1:]
		for _, option := range list {
			m, isObject := option.(map[string]any)
			if grouped, isGroup := m["options"].([]any); isObject && isGroup {
				lists = append(lists, grouped)
				continue
			}
			if isObject {
				option = m["value"]
			}
			values = append(values, option)
		}
	}
	return values
}

// exactNumber reads an option's number exactly.
func exactNumber(v any) (*big.Float, bool) {
	f := new(big.Float).SetPrec(numberPrecision)
	switch n := v.(type) {
	case int:
		return f.SetInt64(int64(n)), true
	case int64:
		return f.SetInt64(n), true
	case uint64:
		return f.SetUint64(n), true
	case float64:
		if math.IsNaN(n) || math.IsInf(n, 0) {
			return nil, false
		}
		return f.SetFloat64(n), true
	}
	return nil, false
}

type inputKind int

const (
	inputLeaf inputKind = iota
	// inputFlattened is a wizard step that flattens: its inputs are beside
	// it.
	inputFlattened
	// inputNested is a group, or a wizard step that doesn't flatten: a
	// mapping under its name.
	inputNested
	// inputList is a list or a repeated wizard step: a list of rows.
	inputList
)

// classifyInput says how a run reads the values in def, an input among
// siblings, and the definitions of the inputs in it. A group's are under
// its name: format 1 has no flatten for a group, which the workflow's
// definitions don't hold once loaded.
func classifyInput(siblings, def map[string]any) (inputKind, map[string]any) {
	switch typeOf(def) {
	case "group":
		items, ok := def["items"].(map[string]any)
		if !ok {
			return inputLeaf, nil
		}
		return inputNested, items
	case "step":
		options, ok := def["options"].(map[string]any)
		switch {
		case !ok:
			return inputLeaf, nil
		case def["multi"] == true:
			return inputList, options
		case wizardFlattens(siblings):
			return inputFlattened, options
		}
		return inputNested, options
	case "list":
		return inputList, nil
	}
	return inputLeaf, nil
}

// wizardFlattens reads $meta.wizard.flatten, which is true unless it is
// false.
func wizardFlattens(defs map[string]any) bool {
	meta, _ := defs["$meta"].(map[string]any)
	wizard, _ := meta["wizard"].(map[string]any)
	return wizard["flatten"] != false
}

// findInput finds name's definition in defs, or in a wizard step there
// that flattens, and the definitions beside it.
func findInput(defs map[string]any, name string) (def, siblings map[string]any, ok bool) {
	if name == "$meta" {
		return nil, nil, false
	}
	if def, ok := defs[name].(map[string]any); ok {
		return def, defs, true
	}
	for _, key := range slices.Sorted(maps.Keys(defs)) {
		inner, ok := defs[key].(map[string]any)
		if key == "$meta" || !ok {
			continue
		}
		if kind, fields := classifyInput(defs, inner); kind == inputFlattened {
			if def, siblings, ok := findInput(fields, name); ok {
				return def, siblings, true
			}
		}
	}
	return nil, nil, false
}

func typeOf(def map[string]any) string {
	t, _ := def["type"].(string)
	return t
}
