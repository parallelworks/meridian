package plan

import (
	"math"
	"math/big"
	"regexp"
	"strconv"
	"strings"
)

// Reading the values of what a phase evaluates, as evaluating gives them:
// JSON values, with whole numbers as int64. Each returns false for a value
// it doesn't read. Those that are exported read the quantities a job asks
// for as Ready does, for whatever else reads them, such as an execution
// group's offer.

// durationPattern is a duration as format 1 writes one: whole seconds, or a
// whole number of seconds, minutes, hours or days.
var durationPattern = regexp.MustCompile(`^([0-9]+)([smhd]?)$`)

// durationUnits are a duration's units, in seconds.
var durationUnits = map[string]int64{"": 1, "s": 1, "m": 60, "h": 60 * 60, "d": 24 * 60 * 60}

// ParseDuration reads a duration in seconds, as a timeout: is written: a
// whole number of seconds, or text of one, alone or with s, m, h or d after
// it, such as 300 or 10m. Null and "" are none, 0.
func ParseDuration(v any) (int64, bool) {
	v = wide(v)
	switch v := v.(type) {
	case nil:
		return 0, true
	case int64:
		if v < 0 {
			return 0, false
		}
		return v, true
	case float64:
		if v < 0 || v != math.Trunc(v) || v >= 1<<63 {
			return 0, false
		}
		return int64(v), true
	case string:
		if v == "" {
			return 0, true
		}
		m := durationPattern.FindStringSubmatch(v)
		if m == nil {
			return 0, false
		}
		n, err := strconv.ParseInt(m[1], 10, 64)
		unit := durationUnits[m[2]]
		if err != nil || n > math.MaxInt64/unit {
			return 0, false
		}
		return n * unit, true
	}
	return 0, false
}

// readCount reads how many times something may run again, as format 1
// reads it: a number, or text that reads as one, with a fraction rounded
// down and a negative number 0.
func readCount(v any) (int, bool) {
	v = wide(v)
	var n float64
	switch v := v.(type) {
	case int64:
		n = float64(v)
	case float64:
		n = v
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
		if err != nil {
			return 0, false
		}
		n = f
	default:
		return 0, false
	}
	switch {
	case math.IsNaN(n):
		return 0, false
	case n <= 0:
		return 0, true
	case n >= math.MaxInt32:
		return math.MaxInt32, true
	}
	return int(n), true
}

// readBool reads a value that turns something on or off, as format 1 reads
// ignore-errors: true or false, text that reads as one, or a number, which
// is true unless it is 0. Anything else is def.
func readBool(v any, def bool) bool {
	v = wide(v)
	switch v := v.(type) {
	case bool:
		return v
	case string:
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	case int64:
		return v != 0
	case float64:
		return v != 0 && !math.IsNaN(v)
	}
	return def
}

// wide returns v with an int, as Go values given to a Value can hold, as
// the int64 a plan's numbers are.
func wide(v any) any {
	if n, ok := v.(int); ok {
		return int64(n)
	}
	return v
}

// cpuPattern is a number of CPUs: cores, or thousandths of one with m.
var cpuPattern = regexp.MustCompile(`^([0-9]+(?:\.[0-9]+)?)(m?)$`)

// memoryPattern is an amount of memory: bytes, or a number with a unit.
var memoryPattern = regexp.MustCompile(`^([0-9]+(?:\.[0-9]+)?)(k|M|G|T|P|E|Ki|Mi|Gi|Ti|Pi|Ei)?$`)

// memoryUnits are the units of memory, in bytes.
var memoryUnits = map[string]*big.Int{
	"":  big.NewInt(1),
	"k": pow(1000, 1), "M": pow(1000, 2), "G": pow(1000, 3), "T": pow(1000, 4), "P": pow(1000, 5), "E": pow(1000, 6),
	"Ki": pow(1024, 1), "Mi": pow(1024, 2), "Gi": pow(1024, 3), "Ti": pow(1024, 4), "Pi": pow(1024, 5), "Ei": pow(1024, 6),
}

func pow(base, exp int64) *big.Int {
	return new(big.Int).Exp(big.NewInt(base), big.NewInt(exp), nil)
}

// ParseCPU reads a number of CPUs in thousandths of a core, as resources:
// cpu: is written: a number of cores, as a number or text, such as 4 or
// 0.5, or text of a whole number of thousandths with m, such as 500m. A
// fraction of a thousandth isn't one. Null and "" are none.
func ParseCPU(v any) (int64, bool) {
	v = wide(v)
	switch v := v.(type) {
	case nil:
		return 0, true
	case int64:
		return scaled(strconv.FormatInt(v, 10), big.NewInt(1000))
	case float64:
		if v < 0 || math.IsNaN(v) || math.IsInf(v, 0) {
			return 0, false
		}
		return scaled(strconv.FormatFloat(v, 'f', -1, 64), big.NewInt(1000))
	case string:
		if v == "" {
			return 0, true
		}
		m := cpuPattern.FindStringSubmatch(v)
		if m == nil {
			return 0, false
		}
		if m[2] == "m" {
			return scaled(m[1], big.NewInt(1))
		}
		return scaled(m[1], big.NewInt(1000))
	}
	return 0, false
}

// ParseMemory reads an amount of memory in bytes, as resources: memory: is
// written: a whole number of bytes, or text of a number with a unit, k, M,
// G, T, P or E for powers of 1000 and Ki, Mi, Gi, Ti, Pi or Ei for powers
// of 1024, such as 8Gi, which must come to whole bytes. Null and "" are
// none.
func ParseMemory(v any) (int64, bool) {
	v = wide(v)
	switch v := v.(type) {
	case nil:
		return 0, true
	case int64:
		if v < 0 {
			return 0, false
		}
		return v, true
	case float64:
		if v < 0 || math.IsNaN(v) || math.IsInf(v, 0) {
			return 0, false
		}
		return scaled(strconv.FormatFloat(v, 'f', -1, 64), big.NewInt(1))
	case string:
		if v == "" {
			return 0, true
		}
		m := memoryPattern.FindStringSubmatch(v)
		if m == nil {
			return 0, false
		}
		return scaled(m[1], memoryUnits[m[2]])
	}
	return 0, false
}

// ParseGPU reads a number of GPUs, as resources: gpu: is written: a whole
// number, or text of one. Null and "" are none.
func ParseGPU(v any) (int, bool) {
	v = wide(v)
	var n int64
	switch v := v.(type) {
	case nil:
		return 0, true
	case int64:
		n = v
	case float64:
		if v != math.Trunc(v) || v < 0 || v > math.MaxInt32 {
			return 0, false
		}
		n = int64(v)
	case string:
		if v == "" {
			return 0, true
		}
		var err error
		if n, err = strconv.ParseInt(v, 10, 64); err != nil {
			return 0, false
		}
	default:
		return 0, false
	}
	if n < 0 || n > math.MaxInt32 {
		return 0, false
	}
	return int(n), true
}

// scaled returns decimal, a non-negative number written in digits with an
// optional fraction, times unit, if that is a whole number that an int64
// holds.
func scaled(decimal string, unit *big.Int) (int64, bool) {
	whole, frac, _ := strings.Cut(decimal, ".")
	n, ok := new(big.Int).SetString(whole+frac, 10)
	if !ok || n.Sign() < 0 {
		return 0, false
	}
	n.Mul(n, unit)
	div := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(len(frac))), nil)
	q, r := new(big.Int).QuoRem(n, div, new(big.Int))
	if r.Sign() != 0 || !q.IsInt64() {
		return 0, false
	}
	return q.Int64(), true
}
