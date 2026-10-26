package local

import (
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
	"math"

	"github.com/parallelworks/meridian/plan"
)

// parseCPU reads a number of CPUs as a workflow's resources: writes it, in
// cores, as 4 or 0.5, or in thousandths of one, as 500m, and returns it in
// thousandths, which must be more than 0.
func parseCPU(s string) (int64, bool) {
	n, ok := plan.ParseCPU(s)
	return n, ok && n > 0
}

// parseMemory reads a quantity of memory as a workflow's resources: writes
// it, in bytes, or with a suffix as Kubernetes writes one, as 8Gi or 512M,
// and returns it in bytes, which must be more than 0.
func parseMemory(s string) (int64, bool) {
	n, ok := plan.ParseMemory(s)
	return n, ok && n > 0
}

// parseGPU reads a number of GPUs as a workflow's resources: writes it, a
// whole number.
func parseGPU(s string) (int, bool) {
	if s == "" {
		return 0, false
	}
	return plan.ParseGPU(s)
}

// parseSeconds reads a duration as a workflow's timeout: writes it, a
// whole number of seconds, or of minutes, hours or days, as 3600, 90m or
// 1d, which must be more than 0, and returns it in seconds. Placement
// compares it with jobs' timeouts, so the two are read alike.
func parseSeconds(s string) (int64, bool) {
	n, ok := plan.ParseDuration(s)
	return n, ok && n > 0 && n <= math.MaxInt32
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
