package format_test

import (
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
