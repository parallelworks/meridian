package plan

import (
	"cmp"
	"fmt"
	"slices"
	"strings"
)

// A stepPin is a pin of a step of a workflow the run calls, a Call or a
// CheckoutPin, by where the step is (at): the digest of the text of the
// workflow whose step it is, and the step's JSON Pointer in that text. A
// plan holds each kind in that order, each step once, so that a child
// run's Create finds its steps' pins by them (findPin).
type stepPin interface {
	at() (caller, step string)
}

func (c Call) at() (caller, step string)        { return c.Caller, c.Step }
func (c CheckoutPin) at() (caller, step string) { return c.Caller, c.Step }

// compareAt orders the steps at caller and step: by caller, and then by
// step.
func compareAt(caller1, step1, caller2, step2 string) int {
	return cmp.Or(strings.Compare(caller1, caller2), strings.Compare(step1, step2))
}

// checkPins checks that pins are in the order of their steps, each once
// (stepPin): what names their kind in its errors, as call or checkout.
func checkPins[P stepPin](pins []P, what string) error {
	for i := 1; i < len(pins); i++ {
		ac, as := pins[i-1].at()
		bc, bs := pins[i].at()
		switch c := compareAt(ac, as, bc, bs); {
		case c == 0:
			return fmt.Errorf("the %s of %s's %s is in the plan twice", what, bc, bs)
		case c > 0:
			return fmt.Errorf("the %s of %s's %s comes after %s's %s", what, bc, bs, ac, as)
		}
	}
	return nil
}

// findPin returns the pin, among pins, in their order (checkPins), of the
// step at the JSON Pointer step of the workflow of the digest caller.
func findPin[P stepPin](pins []P, caller, step string) (P, bool) {
	i, ok := slices.BinarySearchFunc(pins, [2]string{caller, step}, func(p P, k [2]string) int {
		c, s := p.at()
		return compareAt(c, s, k[0], k[1])
	})
	if !ok {
		var none P
		return none, false
	}
	return pins[i], true
}
