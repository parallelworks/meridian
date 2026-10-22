package local

import (
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
	"fmt"
	"math"
	"time"

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
