package format_test

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/parallelworks/meridian/format"
	"github.com/parallelworks/meridian/problems"
)

func TestLocate(t *testing.T) {
	doc, diags := format.Load("workflow.yaml", []byte(`jobs:
  main:
    steps:
      - run: echo ${{ inputs.x }}
      - run: 'echo ${{ inputs.y }}'
      - run: "echo é ${{ inputs.z }}"
      - run: |
          echo one
            echo ${{ inputs.w }}
      - run: >
          echo ${{ inputs.v }}
      - run: "a\tb ${{ inputs.u }}"
`))
	if diags.HasErrors() {
		t.Fatal(diags)
	}
	for _, tc := range []struct {
		pointer string
		offset  int
		want    problems.Position
	}{
		{"/jobs/main/steps/0/run", 5, problems.Position{Line: 4, Column: 19}},
		{"/jobs/main/steps/1/run", 5, problems.Position{Line: 5, Column: 20}},
		{"/jobs/main/steps/2/run", 8, problems.Position{Line: 6, Column: 22}},
		{"/jobs/main/steps/3/run", 16, problems.Position{Line: 9, Column: 18}},
		{"/jobs/main/steps/3/run", 0, problems.Position{Line: 8, Column: 11}},
		// A folded block, and a string with an escape, give where it starts.
		{"/jobs/main/steps/4/run", 5, problems.Position{Line: 10, Column: 14}},
		{"/jobs/main/steps/5/run", 5, problems.Position{Line: 12, Column: 14}},
		// An offset outside the string, too.
		{"/jobs/main/steps/0/run", 99, problems.Position{Line: 4, Column: 14}},
	} {
		if got := doc.Locate(tc.pointer, tc.offset); got != tc.want {
			t.Errorf("Locate(%s, %d) = %v, want %v", tc.pointer, tc.offset, got, tc.want)
		}
	}
}

// A Locator finds each place in a string where Locate does, in whatever
// order it is asked for them.
func TestLocator(t *testing.T) {
	doc, diags := format.Load("workflow.yaml", []byte(`jobs:
  main:
    steps:
      - run: a ${{ x }} é ${{ y }} 日本 ${{ z }}
      - run: 'é ${{ x }}'
      - run: |
          é ${{ x }}
            日本 ${{ y }}

          ${{ z }} done
      - run: >
          folded ${{ x }}
`))
	if diags.HasErrors() {
		t.Fatal(diags)
	}
	var inOrder []int
	for offset := -1; offset < 50; offset++ {
		inOrder = append(inOrder, offset)
	}
	shuffled := slices.Clone(inOrder)
	rand.New(rand.NewPCG(1, 2)).Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
	for _, pointer := range []string{
		"/jobs/main/steps/0/run", "/jobs/main/steps/1/run", "/jobs/main/steps/2/run",
		"/jobs/main/steps/3/run", "/jobs/main/steps/9/run",
	} {
		for _, offsets := range [][]int{inOrder, shuffled} {
			l := doc.Locator(pointer)
			for _, offset := range offsets {
				// Locate finds each place with a Locator of its own.
				if got, want := l.Locate(offset), doc.Locate(pointer, offset); got != want {
					t.Errorf("Locator(%s).Locate(%d) = %v, want %v", pointer, offset, got, want)
				}
			}
		}
	}
	// The block's lines are where the file has them.
	if got, want := doc.Locate("/jobs/main/steps/2/run", 21), (problems.Position{Line: 8, Column: 16}); got != want {
		t.Errorf("Locate in a block = %v, want %v", got, want)
	}
}

// oneLine returns a workflow whose n steps are on one line, with strings in
// each style Locate is exact for, and with text that isn't ASCII if wide is
// set; and each string's pointer, the runes before it on the line, and its
// value.
func oneLine(n int, wide bool) (string, []lineString) {
	var b strings.Builder
	runes := 0
	write := func(s string) {
		b.WriteString(s)
		runes += utf8.RuneCountInString(s)
	}
	write("jobs: {main: {steps: [")
	var out []lineString
	for i := range n {
		if i > 0 {
			write(", ")
		}
		write("{run: ")
		value := fmt.Sprintf("echo %d ${{ a.b }}", i)
		if wide {
			value = fmt.Sprintf("é %d ${{ a.b }} 日本", i)
		}
		s := lineString{pointer: fmt.Sprintf("/jobs/main/steps/%d/run", i), value: value, runes: runes}
		switch i % 3 {
		case 0:
			// A plain scalar can't hold braces in a flow mapping.
			s.value = strings.ReplaceAll(strings.ReplaceAll(value, "${{", "$(("), "}}", "))")
			write(s.value)
		case 1:
			s.runes++
			write("'" + value + "'")
		default:
			s.runes++
			write(`"` + value + `"`)
		}
		out = append(out, s)
		write("}")
	}
	write("]}}\n")
	return b.String(), out
}

type lineString struct {
	pointer string
	runes   int
	value   string
}

// Each of many strings on one line is found where it is, before and after
// text that isn't ASCII, in order and not.
func TestLocateOnOneLine(t *testing.T) {
	for _, wide := range []bool{false, true} {
		workflow, strs := oneLine(200, wide)
		doc, diags := format.Load("workflow.yaml", []byte(workflow))
		if diags.HasErrors() {
			t.Fatal(diags)
		}
		order := rand.New(rand.NewPCG(1, 2)).Perm(len(strs))
		for _, i := range append(order, order...) {
			s := strs[i]
			for _, offset := range []int{0, 3, len(s.value)} {
				want := problems.Position{Line: 1, Column: 1 + s.runes + utf8.RuneCountInString(s.value[:offset])}
				if got := doc.Locate(s.pointer, offset); got != want {
					t.Errorf("Locate(%s, %d) = %v, want %v", s.pointer, offset, got, want)
				}
			}
		}
	}
}

// Strings on one line, as many as a file can hold, each with a place to
// find.
func BenchmarkLocateOnOneLine(b *testing.B) {
	for _, wide := range []bool{false, true} {
		b.Run(map[bool]string{false: "ascii", true: "wide"}[wide], func(b *testing.B) {
			n := 100
			for w, _ := oneLine(n+100, wide); len(w) <= format.DefaultMaxSize; w, _ = oneLine(n+100, wide) {
				n += 100
			}
			workflow, strs := oneLine(n, wide)
			doc, diags := format.Load("workflow.yaml", []byte(workflow))
			if diags.HasErrors() {
				b.Fatal(diags)
			}
			b.SetBytes(int64(len(workflow)))
			for b.Loop() {
				for _, s := range strs {
					doc.Locate(s.pointer, 3)
				}
			}
		})
	}
}

func TestReportPosition(t *testing.T) {
	doc, diags := format.Load("workflow.yaml", []byte(`jobs:
  main:
    steps:
      - run: echo
`))
	if diags.HasErrors() {
		t.Fatal(diags)
	}
	for _, tc := range []struct {
		pointer string
		want    problems.Position
	}{
		// A mapping is named by its key, and anything else is where it
		// starts.
		{"/jobs/main", problems.Position{Line: 2, Column: 3}},
		{"/jobs/main/steps", problems.Position{Line: 4, Column: 7}},
		{"/jobs/main/steps/0/run", problems.Position{Line: 4, Column: 14}},
		// An item of a list has no key.
		{"/jobs/main/steps/0", problems.Position{Line: 4, Column: 9}},
		// Nothing is there.
		{"/jobs/main/steps/00", problems.Position{}},
		{"jobs", problems.Position{}},
	} {
		if got := doc.ReportPosition(tc.pointer); got != tc.want {
			t.Errorf("ReportPosition(%s) = %v, want %v", tc.pointer, got, tc.want)
		}
	}
}
