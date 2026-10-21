package format_test

import (
	"math/rand/v2"
	"slices"
	"testing"

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
