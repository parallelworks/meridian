package local

import (
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
	"fmt"
	"math"
	"math/big"
	"strings"
	"time"
)

// parseCPU reads a number of CPUs as a workflow's resources: writes it, in
// cores, as 4 or 0.5, or in thousandths of one, as 500m, and returns it in
// thousandths.
func parseCPU(s string) (int64, error) {
	text, scale := s, int64(1000)
	if rest, ok := strings.CutSuffix(s, "m"); ok {
		text, scale = rest, 1
	}
	n, err := scaled(text, scale)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("%q isn't a number of CPUs greater than 0, such as 4, 0.5 or 500m", s)
	}
	return n, nil
}

// memoryUnits are the suffixes of a quantity of memory, as Kubernetes
// writes one, and what each multiplies by.
var memoryUnits = []struct {
	suffix string
	scale  int64
}{
	{"Ki", 1 << 10},
	{"Mi", 1 << 20},
	{"Gi", 1 << 30},
	{"Ti", 1 << 40},
	{"Pi", 1 << 50},
	{"Ei", 1 << 60},
	{"k", 1e3},
	{"M", 1e6},
	{"G", 1e9},
	{"T", 1e12},
	{"P", 1e15},
	{"E", 1e18},
}

// parseMemory reads a quantity of memory, in bytes, or with a suffix, as
// 8Gi or 512M, and returns it in bytes.
func parseMemory(s string) (int64, error) {
	text, scale := s, int64(1)
	for _, u := range memoryUnits {
		if rest, ok := strings.CutSuffix(s, u.suffix); ok {
			text, scale = rest, u.scale
			break
		}
	}
	n, err := scaled(text, scale)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("%q isn't a quantity of memory greater than 0, such as 8Gi, 512Mi or 1073741824", s)
	}
	return n, nil
}

// scaled reads text, a decimal number, and returns it times scale, which
// must be a whole number that fits an int64.
func scaled(text string, scale int64) (int64, error) {
	if text == "" || strings.ContainsAny(text, "eE+_") || strings.HasPrefix(text, ".") || strings.HasSuffix(text, ".") {
		return 0, fmt.Errorf("%q isn't a decimal number", text)
	}
	r, ok := new(big.Rat).SetString(text)
	if !ok {
		return 0, fmt.Errorf("%q isn't a decimal number", text)
	}
	r.Mul(r, new(big.Rat).SetInt64(scale))
	if !r.IsInt() || !r.Num().IsInt64() {
		return 0, fmt.Errorf("%q isn't a whole number of units", text)
	}
	return r.Num().Int64(), nil
}

// parseSeconds reads a duration, as 24h or 90m, which must be a whole
// number of seconds greater than 0, and returns it in seconds.
func parseSeconds(s string) (int64, error) {
	d, err := time.ParseDuration(s)
	if err != nil || d <= 0 || d%time.Second != 0 || d/time.Second > math.MaxInt32 {
		return 0, fmt.Errorf("%q isn't a duration of whole seconds greater than 0, such as 24h, 90m or 30s", s)
	}
	return int64(d / time.Second), nil
}

// settingsJSON encodes a member's settings, as YAML decoded them, as the
// JSON its backend's driver decodes, with keys in order.
func settingsJSON(settings map[string]any) (jsontext.Value, error) {
	data, err := jsonv2.Marshal(settings, jsonv2.Deterministic(true))
	if err != nil {
		return nil, err
	}
	return jsontext.Value(data), nil
}
