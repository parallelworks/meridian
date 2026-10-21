package expr_test

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/parallelworks/meridian/expr"
	"github.com/parallelworks/meridian/problems"
)

// templateLimit is the language's limit on what one template builds, and
// groupLimit Meridian's on what the templates of a group build together.
// workLimit and groupWork are the same limits on the work they do.
const (
	templateLimit = 8 << 20
	groupLimit    = 64 << 20
	workLimit     = 32 << 20
	groupWork     = 256 << 20
)

// longest holds the x's that xs returns, made once, since the tests of the
// work limit read strings of up to 32 MiB.
var longest = sync.OnceValue(func() string { return strings.Repeat("x", workLimit+1) })

func xs(n int) string { return longest()[:n] }

func tooLarge(err error) bool {
	ee, ok := errors.AsType[*expr.EvalError](err)
	return ok && ee.Kind == problems.ExpressionTooLarge
}

func tooMuchWork(err error) bool {
	ee, ok := errors.AsType[*expr.EvalError](err)
	return ok && ee.Kind == problems.ExpressionTooMuchWork
}

func overBudget(err error) bool {
	_, ok := errors.AsType[*expr.BudgetError](err)
	return ok
}

func inputs(values map[string]any) expr.Scope {
	return expr.Scope{Contexts: map[string]any{"inputs": values}}
}

// Each operation that builds a value counts the value's size against its
// template's limit, and nothing else counts. Each case's inputs make its
// template build n bytes: it passes at the limit, and fails a byte over.
func TestBuildLimit(t *testing.T) {
	keys := map[string]any{}
	for i := range 1000 {
		keys[strconv.Itoa(i)] = xs(1000)
	}
	for _, tc := range []struct {
		name     string
		template string
		// inputs returns the inputs with which the template builds n bytes.
		inputs func(n int) map[string]any
	}{
		{
			// Each value's text counts, and the text around it doesn't.
			"template text", "(${{ inputs.a }}, ${{ inputs.b }})",
			func(n int) map[string]any { return map[string]any{"a": xs(n / 2), "b": xs(n - n/2)} },
		},
		{
			"+", "${{ inputs.a + inputs.b }}",
			func(n int) map[string]any { return map[string]any{"a": xs(n - 1), "b": "y"} },
		},
		{
			"+ with a number", "${{ inputs.a + 1 }}",
			func(n int) map[string]any { return map[string]any{"a": xs(n - 1)} },
		},
		{
			// The string, once: two a's, a -, and b.
			"format", "${{ format('{0}{0}-{1}', inputs.a, inputs.b) }}",
			func(n int) map[string]any { return map[string]any{"a": xs(n / 4), "b": xs(n - 2*(n/4) - 1)} },
		},
		{
			"join", "${{ join(inputs.l, '-') }}",
			func(n int) map[string]any { return map[string]any{"l": []any{xs(n / 2), xs(n - n/2 - 1)}} },
		},
		{
			"the join operator", "${{ inputs.l join '-' }}",
			func(n int) map[string]any { return map[string]any{"l": []any{xs(n / 2), xs(n - n/2 - 1)}} },
		},
		{
			// The string's bytes, and 8 for each of its two parts.
			"split", "${{ inputs.s split ',' }}",
			func(n int) map[string]any { return map[string]any{"s": xs(n-17) + ","} },
		},
		{
			// A part for each character: a of one byte and b of two count
			// 9a + 10b.
			"split by ''", "${{ inputs.s split '' }}",
			func(n int) map[string]any {
				b := n % 9
				return map[string]any{"s": xs((n-10*b)/9) + strings.Repeat("é", b)}
			},
		},
		{
			// An empty string splits into no parts by ''.
			"split of an empty string by ''", "${{ length(inputs.s + '') + length('' split '') }}",
			func(n int) map[string]any { return map[string]any{"s": xs(n)} },
		},
		{
			// An empty string splits into one part by anything else.
			"split of an empty string", "${{ length(inputs.s + '') + length('' split ',') }}",
			func(n int) map[string]any { return map[string]any{"s": xs(n - 8)} },
		},
		{
			// Indented: [, a line break, two spaces, the string in quotes, a
			// line break and ].
			"toJSON", "${{ toJSON(inputs.l) }}",
			func(n int) map[string]any { return map[string]any{"l": []any{xs(n - 8)}} },
		},
		{
			// A list of one string: 8 for the item, and the string's bytes.
			"fromJSON", "${{ fromJSON(inputs.j) }}",
			func(n int) map[string]any { return map[string]any{"j": `["` + xs(n-8) + `"]`} },
		},
		{
			"upper", "${{ upper inputs.s }}",
			func(n int) map[string]any { return map[string]any{"s": xs(n)} },
		},
		{
			"lower", "${{ lower inputs.s }}",
			func(n int) map[string]any { return map[string]any{"s": xs(n)} },
		},
		{
			// 16 for each number.
			"range", "${{ length(range 10000) + length(inputs.s + '') }}",
			func(n int) map[string]any { return map[string]any{"s": xs(n - 16*10000)} },
		},
		{
			"the range operator", "${{ length(5 range 10005) + length(inputs.s + '') }}",
			func(n int) map[string]any { return map[string]any{"s": xs(n - 16*10000)} },
		},
		{
			// The slice operand 0:(end) builds 0: and end's text.
			"slice operand", "${{ 'abc' get 0:(inputs.s) }}",
			func(n int) map[string]any { return map[string]any{"s": xs(n - 2)} },
		},
		{
			// 8 for the item, and its bytes, though it was there already.
			"map", "${{ map(inputs.l, x => x) }}",
			func(n int) map[string]any { return map[string]any{"l": []any{xs(n - 8)}} },
		},
		{
			// What the body makes for each of 1,000 nulls, "!", and what
			// map makes of it, 8 for the item and its byte.
			"map of what its lambda makes", "${{ length(map(inputs.l, x => x + '!')) + length(inputs.s + '') }}",
			func(n int) map[string]any { return map[string]any{"l": make([]any, 1000), "s": xs(n - 10*1000)} },
		},
		{
			// 8 for the item filter keeps, and its bytes, and nothing for
			// the item it doesn't keep.
			"filter", "${{ filter(inputs.l, x => x != 'y') }}",
			func(n int) map[string]any { return map[string]any{"l": []any{xs(n - 8), "y"}} },
		},
		{
			// A mapping's values, without their keys.
			"filter of a mapping", "${{ filter(inputs.m, x => true) }}",
			func(n int) map[string]any { return map[string]any{"m": map[string]any{"key": xs(n - 8)}} },
		},
		{
			"keys, values and trim", "${{ length(inputs.s + '') + length(keys inputs.m) + length(values inputs.m) + length(trim inputs.t) }}",
			func(n int) map[string]any { return map[string]any{"s": xs(n), "m": keys, "t": " " + xs(1<<20) + " "} },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tmpl, err := expr.ParseTemplate(tc.template)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := tmpl.Eval(inputs(tc.inputs(templateLimit))); err != nil {
				t.Errorf("at the limit: %v", err)
			}
			if _, err := tmpl.Eval(inputs(tc.inputs(templateLimit + 1))); !tooLarge(err) {
				t.Errorf("a byte over the limit: %v, want %s", err, problems.ExpressionTooLarge.Code())
			}
		})
	}
}

// join and split count what they build before building it, so a long
// separator fails without being repeated.
func TestBuildLimitCountsFirst(t *testing.T) {
	scope := inputs(map[string]any{"s": xs(1 << 20)})
	for _, src := range []string{"(range 10000) join inputs.s", "join(range(10000), inputs.s)", "(inputs.s + inputs.s) split ''"} {
		if _, err := mustParse(t, src).Eval(scope); !tooLarge(err) {
			t.Errorf("%s: %v, want %s", src, err, problems.ExpressionTooLarge.Code())
		}
	}
}

// A template doesn't count what a template it reads builds, such as an
// input's default or a key beside it: each has its own count.
func TestReadTemplatesCountOnTheirOwn(t *testing.T) {
	w := &expr.Workflow{
		Root: map[string]any{
			"on": map[string]any{"execute": map[string]any{"inputs": map[string]any{
				"a": map[string]any{"type": "string", "default": "${{ inputs.s + inputs.s }}"},
			}}},
			"b": "${{ inputs.s + inputs.s }}",
		},
		Definitions: []string{"on", "execute", "inputs"},
	}
	scope := inputs(map[string]any{"s": xs(5 << 19)})
	// Each template read builds 5 MiB, and each reader 7.5 MiB.
	for _, src := range []string{"inputs.a.default + inputs.s", ".b + inputs.s"} {
		got, err := mustParse(t, src).Eval(w.Scope([]string{"c"}, scope))
		if s, _ := got.(string); err != nil || len(s) != 15<<19 {
			t.Errorf("%s: %d bytes, %v; want 7.5 MiB", src, len(s), err)
		}
	}
}

// The templates of a group, which share a budget, build at most 64 MiB
// together, even though each is within the language's limit.
func TestGroupBudget(t *testing.T) {
	root := map[string]any{"byte": "${{ 'x' + '' }}"}
	reads := make([]string, 8)
	for i := range reads {
		root[fmt.Sprintf("v%d", i)] = "${{ inputs.s + inputs.s }}"
		// ! reads nothing of what it negates, so the reader does little
		// work.
		reads[i] = fmt.Sprintf("!.v%d", i)
	}
	w := &expr.Workflow{Root: root}
	scope := inputs(map[string]any{"s": xs(templateLimit / 2)})
	// Eight templates that each build 8 MiB, the most one can, build the
	// group's 64 MiB.
	all := strings.Join(reads, " + ")
	got, err := mustParse(t, all).Eval(w.Scope([]string{"x"}, scope))
	if err != nil || got != 0.0 {
		t.Errorf("eight templates of 8 MiB: %v, %v; want 0", got, err)
	}
	_, err = mustParse(t, all+" + !.byte").Eval(w.Scope([]string{"x"}, scope))
	if !overBudget(err) || !strings.Contains(err.Error(), "64 MiB") {
		t.Errorf("a byte more: %v, want %s", err, problems.ExpressionsBuildTooMuch.Code())
	}
	// A scope without a budget makes each evaluation a group of its own.
	for range 9 {
		if _, err := mustParse(t, "inputs.s + inputs.s").Eval(scope); err != nil {
			t.Fatalf("a template on its own: %v", err)
		}
	}
}

// Each part of an expression counts toward its template's work, as
// docs/expressions.md says under Limits: 8 for each step, the size of each
// value an operation reads, and what the template builds.
func TestWork(t *testing.T) {
	scope := expr.Scope{
		Contexts: map[string]any{
			"inputs": map[string]any{
				"a": map[string]any{"b": 1.0},
				"s": "abc",
				"t": "12",
				"l": []any{map[string]any{"a": 1.0}, map[string]any{"a": 2.0}, map[string]any{}},
				"w": []any{"ab", "c"},
			},
			"vars": map[string]any{},
		},
		Status: &expr.Status{},
		Index:  []int{1},
	}
	for _, tc := range []struct {
		src  string
		work int
	}{
		// Each operand, operator and call is a step, and parentheses that
		// only group are none. A number with its sign is one operand, and
		// a - with a space after it an operator.
		{"1", 8},
		{"((1))", 8},
		{"'abc'", 8},
		{"word", 8},
		{"-5", 8},
		{"- 5", 16},
		{"1 + 2", 24},
		{"!!true", 24},
		{"success()", 8},
		{"true ? 1 : 2", 24},
		// What isn't evaluated counts nothing.
		{"false ? 'abc' == 'abc' : 2", 24},
		{"false && 'abc' == 'abc'", 16},
		{"true || 'abc' == 'abc'", 16},
		{"1 ?? 'abc' == 'abc'", 16},
		{"case(false, 'abc' == 'abc', true, 2, 3)", 32},
		// Each name or index of a path is a step, whether or not the path
		// leads anywhere, and so is a path after a computed value.
		{"inputs.a.b", 24},
		{"inputs['a'].b", 24},
		{"inputs.l[index].a", 32},
		{"inputs.missing.b.c", 32},
		{"vars.x", 16},
		{"(inputs.a).b", 32},
		// A .* is a step, and so is each item it visits, from which the
		// rest of the path is followed.
		{"inputs.l.*.a", 72},
		{"inputs.l.*", 48},
		{"inputs.missing.*.a", 24},
		{"(inputs.l).*.a", 80},
		// Going through a mapping reads its keys, 8 for each and its
		// bytes, and through a string the string, which holds the items as
		// JSON.
		{"inputs.a.*", 41},
		{"inputs.l.*.*", 106},
		{"(inputs.s).*", 35},
		// Each run of a lambda is a step, and its body counts each time.
		{"filter(inputs.l, x => false)", 72},
		{"filter(inputs.l, x => length(filter(inputs.w, y => false)))", 240},
		{"map(inputs.l, x => x.a)", 144},
		{"map('[1]', x => x)", 51},
		{"filter(inputs.a, x => false)", 49},
		// An operation that reads values counts their sizes.
		{"'abc' == 'de'", 29},
		{"'abc' !== 'de'", 29},
		{"'abc' < 'de'", 29},
		{"1 == '1'", 33},
		{"contains('abc', 'b')", 28},
		{"'abc' !in 'b'", 28},
		{"'abc' startsWith 'a'", 28},
		{"endsWith('abc', 'c')", 28},
		{"length('abc')", 19},
		{"length(inputs.l)", 24},
		{"trim ' a '", 19},
		{"upper 'ab'", 20},
		{"lower 5", 25},
		{"keys inputs.a", 41},
		{"values inputs.a", 41},
		{"toJSON(1)", 25},
		{"fromJSON('[1]')", 35},
		{"fromJSON(1)", 24},
		{"format('{0}', 'ab')", 31},
		{"join(inputs.w)", 48},
		{"inputs.w join '-'", 56},
		{"join('ab', '-')", 27},
		{"'a,b' split ','", 47},
		{"[1, 2]", 40},
		{`{"k": "v"}`, 18},
		// A name, an index or get that reads into a string reads it, and
		// get reads a key that is text.
		{"inputs.s[0]", 27},
		{"inputs.s get 0", 35},
		{"inputs.a get 'b'", 33},
		// Converting text to a number reads it.
		{"inputs.t * 1", 34},
		{"'12' - 1", 26},
		{"-inputs.s", 27},
		{"abs inputs.s", 27},
		{"range '3'", 65},
		{"'1' range 3", 57},
		// A path through lists and mappings, get of them, arithmetic on
		// numbers and ! read nothing.
		{"inputs.l", 16},
		{"inputs.l get 1", 32},
		{"inputs.a.b - 1", 40},
		{"true + 1", 24},
		{"!inputs.l", 24},
		// What a template builds counts as work too, the slice operands'
		// text among it.
		{"'a' + 'b'", 26},
		{"range 3", 64},
		{"0:('ab')", 20},
		{"(1):(2)", 27},
	} {
		if _, got, err := expr.Work(mustParse(t, tc.src), scope); err != nil || got != tc.work {
			t.Errorf("%s: %d, %v; want %d", tc.src, got, err, tc.work)
		}
	}
}

// A template does at most 32 MiB of work. Each case's inputs make its
// template do n bytes of work: it passes at the limit, and fails a byte
// over.
func TestWorkLimit(t *testing.T) {
	both := func(n int) map[string]any { return map[string]any{"a": xs(n - 41), "b": "y"} }
	// 1,000,000 items of 16 each, and the text of a value [], 2. The
	// template's second expression reads the rest: ==, inputs.s, '' and
	// the text false, 37, and the string's bytes.
	steps := func(n int) map[string]any {
		return map[string]any{"l": make([]any, 1000000), "s": xs(n - 63 - 16*1000000)}
	}
	for _, tc := range []struct {
		name     string
		template string
		// inputs returns the inputs with which the template does n bytes
		// of work.
		inputs func(n int) map[string]any
	}{
		// 8, two references of 16, and the bytes of both.
		{"==", "${{ inputs.a == inputs.b }}", both},
		{"contains", "${{ contains(inputs.a, inputs.b) }}", both},
		{"startsWith", "${{ inputs.a startsWith inputs.b }}", both},
		{
			// 8, a reference of 16, and the string's bytes.
			"length", "${{ length(inputs.a) }}",
			func(n int) map[string]any { return map[string]any{"a": xs(n - 24)} },
		},
		{
			// 8, a reference of 16, and the mapping: 8 for its entry, its
			// key and its value.
			"keys", "${{ keys inputs.m }}",
			func(n int) map[string]any { return map[string]any{"m": map[string]any{"k": xs(n - 33)}} },
		},
		{
			// inputs.l.*.a is 24, and each item it visits 16.
			".*", "${{ inputs.l.*.a }}${{ inputs.s == '' }}", steps,
		},
		{
			// filter and inputs.l are 24, and each run 16.
			"lambda runs", "${{ filter(inputs.l, x => false) }}${{ inputs.s == '' }}", steps,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tmpl, err := expr.ParseTemplate(tc.template)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := tmpl.Eval(inputs(tc.inputs(workLimit))); err != nil {
				t.Errorf("at the limit: %v", err)
			}
			if _, err := tmpl.Eval(inputs(tc.inputs(workLimit + 1))); !tooMuchWork(err) {
				t.Errorf("a byte over the limit: %v, want %s", err, problems.ExpressionTooMuchWork.Code())
			}
		})
	}

	// The error is at the operation that passes the limit.
	_, err := mustParse(t, "inputs.a == inputs.b").Eval(inputs(both(workLimit + 1)))
	ee, _ := errors.AsType[*expr.EvalError](err)
	if want := "The expression does more than 32 MiB of work."; !tooMuchWork(err) || ee.Offset != 9 || err.Error() != want {
		t.Errorf("a byte over: %#v, want %q at 9", err, want)
	}
}

// The templates of a group, which share a budget, do at most 256 MiB of
// work together, even though each is within the language's limit.
func TestGroupWorkBudget(t *testing.T) {
	// 8, two references of 16, and 16 MiB less 20 read twice: 32 MiB.
	values := map[string]any{"h": xs(workLimit/2 - 20)}
	heavy := "inputs.h == inputs.h"
	isOver := func(err error) bool {
		be, ok := errors.AsType[*expr.BudgetError](err)
		return ok && be.Kind == problems.ExpressionsTooMuchWork && be.Limit == groupWork>>20
	}

	// Eight templates that each do 32 MiB, the most one can, do the
	// group's 256 MiB, and a step more is too much.
	group := inputs(values).Budgeted()
	for i := range 8 {
		if _, err := mustParse(t, heavy).Eval(group); err != nil {
			t.Fatalf("template %d of 32 MiB: %v", i+1, err)
		}
	}
	_, err := mustParse(t, "1").Eval(group)
	if want := "The expressions evaluated together here do more than 256 MiB of work between them."; !isOver(err) || err.Error() != want {
		t.Errorf("a step more: %#v, want %q", err, want)
	}

	// A template that reads others shares their group: seven of 32 MiB
	// and the reader's 160 pass, and an eighth doesn't.
	root := map[string]any{}
	reads := make([]string, 8)
	for i := range reads {
		root[fmt.Sprintf("v%d", i)] = "${{ " + heavy + " }}"
		reads[i] = fmt.Sprintf(".v%d", i)
	}
	w := &expr.Workflow{Root: root}
	if _, err := mustParse(t, strings.Join(reads[:7], " + ")).Eval(w.Scope([]string{"x"}, inputs(values))); err != nil {
		t.Errorf("a template that reads seven of 32 MiB: %v", err)
	}
	if _, err := mustParse(t, strings.Join(reads, " + ")).Eval(w.Scope([]string{"x"}, inputs(values))); !isOver(err) {
		t.Errorf("a template that reads eight of 32 MiB: %v, want %s", err, problems.ExpressionsTooMuchWork.Code())
	}

	// A scope without a budget makes each evaluation a group of its own.
	for range 9 {
		if _, err := mustParse(t, heavy).Eval(inputs(values)); err != nil {
			t.Fatalf("a template on its own: %v", err)
		}
	}
}

// A clock is a test's clock. Each read moves it on by the next of steps,
// or by the last once there is one left, and the test can move it on
// between reads too.
type clock struct {
	now   time.Time
	steps []time.Duration
}

// every returns a clock that each read moves on by step.
func every(step time.Duration) *clock { return &clock{steps: []time.Duration{step}} }

func (c *clock) read() time.Time {
	c.now = c.now.Add(c.steps[0])
	if len(c.steps) > 1 {
		c.steps = c.steps[1:]
	}
	return c.now
}

func tooSlow(err error) bool {
	be, ok := errors.AsType[*expr.BudgetError](err)
	return ok && be.Kind == problems.ExpressionsTooSlow && be.Limit == 10
}

// The templates of a group spend at most 10 seconds evaluating, together.
// The clock is read when an evaluation starts and when it stops, and every
// 256 steps the group takes: each node it evaluates, and each run of a
// lambda's body. Each read but a stop's checks the time.
func TestGroupTimeBudget(t *testing.T) {
	eval := func(src string, scope expr.Scope) error {
		t.Helper()
		_, err := mustParse(t, src).Eval(scope)
		return err
	}
	lists := inputs(map[string]any{"l126": make([]any, 126), "l127": make([]any, 127)})
	// steps returns an expression of n steps: format, its template and n-2
	// arguments.
	steps := func(n int) string { return "format(''" + strings.Repeat(", 1", n-2) + ")" }

	// 11 seconds a read: the 256th step reads the clock, and the 255th
	// doesn't.
	if err := eval(steps(255), expr.WithClock(lists, every(11*time.Second).read)); err != nil {
		t.Errorf("255 steps: %v", err)
	}
	err := eval(steps(256), expr.WithClock(lists, every(11*time.Second).read))
	if want := "The expressions evaluated together here took more than 10 seconds between them."; !tooSlow(err) || err.Error() != want {
		t.Errorf("256 steps: %v, want %q", err, want)
	}
	// Each run of a lambda is a step: map, inputs.l, and a run and x for
	// each of 126 or 127 items are 254 or 256 steps.
	if err := eval("map(inputs.l126, x => x)", expr.WithClock(lists, every(11*time.Second).read)); err != nil {
		t.Errorf("126 runs: %v", err)
	}
	if err := eval("map(inputs.l127, x => x)", expr.WithClock(lists, every(11*time.Second).read)); !tooSlow(err) {
		t.Errorf("127 runs: %v, want %s", err, problems.ExpressionsTooSlow.Code())
	}

	// The group shares its time. A template of 256 steps takes 8 seconds,
	// 4 to its 256th step and 4 to its stop, which passes on its own, and a
	// second one in the group passes 10 seconds at its 256th step. The time
	// between two of the group's evaluations, an hour here, doesn't count.
	c := every(4 * time.Second)
	group := expr.WithClock(lists, c.read)
	if err := eval(steps(256), group); err != nil {
		t.Errorf("one template of 8 seconds: %v", err)
	}
	c.now = c.now.Add(time.Hour)
	if err := eval(steps(256), group); !tooSlow(err) {
		t.Errorf("two templates of 8 seconds: %v, want %s", err, problems.ExpressionsTooSlow.Code())
	}
	if err := eval(steps(256), expr.WithClock(lists, every(4*time.Second).read)); err != nil {
		t.Errorf("a template of 8 seconds in a group of its own: %v", err)
	}

	// A template that another reads takes its steps in the group's count,
	// and its time counts from when the reader started: the reader's +,
	// length and .a, and .a's 253 steps, are the group's 256th step, 10 or
	// 11 seconds after the reader started.
	w := &expr.Workflow{Root: map[string]any{"a": "${{ " + steps(253) + " }}"}}
	for _, tc := range []struct {
		after time.Duration
		ok    bool
	}{{10 * time.Second, true}, {11 * time.Second, false}} {
		c := &clock{steps: []time.Duration{time.Second, tc.after}}
		if err := eval("length(.a) + 1", w.Scope([]string{"x"}, expr.WithClock(lists, c.read))); tc.ok && err != nil || !tc.ok && !tooSlow(err) {
			t.Errorf("the 256th step %v after the reader started: %v", tc.after, err)
		}
	}
}
