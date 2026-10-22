package plan_test

import (
	"errors"
	"testing"

	"github.com/parallelworks/meridian/plan"
)

// TestReadyJobEncoding checks that a ready job reads back as it was
// written, its expressions and a number a float64 can't hold among it, and
// that reading one is as strict as reading a plan.
func TestReadyJobEncoding(t *testing.T) {
	run := mustValue(t, "echo ${{ matrix.n }}")
	j := &plan.ReadyJob{
		Key: "build#2", Name: "build", Matrix: plan.DataOf(map[string]any{"n": int64(1<<60 + 1)}),
		Target:    plan.Target{Inline: &plan.Inline{Name: "ssh", Backend: "ssh", Isolation: "host", SSH: plan.DataOf(map[string]any{"remoteHost": "h", "disconnect-timeout": int64(300)})}},
		Resources: plan.Resources{MilliCPU: 500, Memory: 8 << 30},
		Retry:     &plan.ReadyJobRetry{Reasons: []plan.Reason{plan.ReasonWorkerLost}, MaxRetries: 3, IntervalSeconds: 60},
		Steps:     []plan.Step{{Run: run, Path: "/jobs/build/steps/0"}}, Path: "/jobs/build",
	}
	data, err := plan.EncodeReadyJob(j)
	if err != nil {
		t.Fatal(err)
	}
	back, err := plan.DecodeReadyJob(plan.Version, data)
	if err != nil {
		t.Fatal(err)
	}
	if got := back.Matrix.Get().(map[string]any)["n"]; got != int64(1<<60+1) {
		t.Errorf("matrix value read back as %v (%T)", got, got)
	}
	again, err := plan.EncodeReadyJob(back)
	if err != nil {
		t.Fatal(err)
	}
	if string(again) != string(data) {
		t.Errorf("read back as\n%s\nnot\n%s", again, data)
	}

	if _, err := plan.DecodeReadyJob(plan.Version+1, data); !errors.Is(err, plan.ErrVersion) {
		t.Errorf("another version: got %v, want ErrVersion", err)
	}
	for _, bad := range []string{
		`{"key":"a","name":"a","steps":[],"path":"/jobs/a","isolation":"none"}`,
		`{"key":"a","key":"b","name":"a","steps":[],"path":"/jobs/a"}`,
	} {
		if _, err := plan.DecodeReadyJob(plan.Version, []byte(bad)); err == nil {
			t.Errorf("read %s", bad)
		}
	}
}
