package format_test

import (
	"testing"

	"github.com/parallelworks/meridian/format"
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
		want    format.Position
	}{
		{"/jobs/main/steps/0/run", 5, format.Position{Line: 4, Column: 19}},
		{"/jobs/main/steps/1/run", 5, format.Position{Line: 5, Column: 20}},
		{"/jobs/main/steps/2/run", 8, format.Position{Line: 6, Column: 22}},
		{"/jobs/main/steps/3/run", 16, format.Position{Line: 9, Column: 18}},
		{"/jobs/main/steps/3/run", 0, format.Position{Line: 8, Column: 11}},
		// A folded block, and a string with an escape, give where it starts.
		{"/jobs/main/steps/4/run", 5, format.Position{Line: 10, Column: 14}},
		{"/jobs/main/steps/5/run", 5, format.Position{Line: 12, Column: 14}},
		// An offset outside the string, too.
		{"/jobs/main/steps/0/run", 99, format.Position{Line: 4, Column: 14}},
	} {
		if got := doc.Locate(tc.pointer, tc.offset); got != tc.want {
			t.Errorf("Locate(%s, %d) = %v, want %v", tc.pointer, tc.offset, got, tc.want)
		}
	}
}
