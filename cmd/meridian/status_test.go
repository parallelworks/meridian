package main

import (
	jsonv2 "encoding/json/v2"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/parallelworks/hopper/driver/hoppersqlite"

	"github.com/parallelworks/meridian/cmd/meridian/internal/local"
)

// failWorkflow is a workflow of one job, whose step fails.
const failWorkflow = "jobs:\n  main:\n    steps:\n      - run: exit 3\n"

// jsonLines decodes the JSON lines out, each an object.
func jsonLines(t *testing.T, out string) []map[string]any {
	t.Helper()
	var lines []map[string]any
	for line := range strings.Lines(out) {
		var l map[string]any
		if err := jsonv2.Unmarshal([]byte(line), &l); err != nil {
			t.Fatalf("%v: %q", err, line)
		}
		lines = append(lines, l)
	}
	return lines
}

// TestStatus checks what meridian status shows: with no store, no runs;
// the store's runs, the newest first, each as it is, created, running or
// ended; one run, and its jobs, in each of those states, with how it ended;
// each as text, and as JSON lines; and a run the store hasn't.
func TestStatus(t *testing.T) {
	parallel(t)
	env := runEnv(t)
	if r := commandTest(t, env, "status"); r.code != exitOK || r.stdout != "no runs\n" || r.stderr != "" {
		t.Errorf("meridian status without a store = %+v", r)
	}
	if r := commandTest(t, env, "status", "--format", "json"); r.code != exitOK || r.stdout != "" || r.stderr != "" {
		t.Errorf("meridian status --format json without a store = %+v", r)
	}
	if r := commandTest(t, env, "status", "1"); r.code != exitError || !strings.Contains(r.stderr, "(run_not_found)") || r.stdout != "" {
		t.Errorf("meridian status 1 without a store = %+v", r)
	}

	// Run 1 succeeds, run 2 fails, run 3 is stored but not started, and run
	// 4 runs until it is released.
	if r := runTest(t, env, nil, writeFile(t, "w.yaml", helloWorkflow)); r.code != exitOK {
		t.Fatalf("meridian run = %+v", r)
	}
	if r := runTest(t, env, nil, writeFile(t, "w.yaml", failWorkflow)); r.code != exitInvalid {
		t.Fatalf("meridian run of a step that fails = %+v", r)
	}
	created, createdDir := createdRun(t, env, writeFile(t, "w.yaml", helloWorkflow))
	if created != "3" {
		t.Fatalf("the run created is %s", created)
	}
	workflow, release := waitingWorkflow(t)
	running := startCommand(t, env, nil, "run", workflow)
	untilSaid(t, running, "main step 0 started")

	r := commandTest(t, env, "status")
	when := `\d{4}-\d\d-\d\d \d\d:\d\d:\d\d`
	list := regexp.MustCompile(`^RUN +STATE +OUTCOME +CREATED +WORKFLOW
4 +running +` + when + ` +w\.yaml
3 +created +` + when + ` +w\.yaml
2 +ended +failure +` + when + ` +w\.yaml
1 +ended +success +` + when + ` +w\.yaml
$`)
	if r.code != exitOK || !list.MatchString(r.stdout) || r.stderr != "" {
		t.Errorf("meridian status = %+v", r)
	}
	if r := commandTest(t, env, "status", "-n", "2"); r.code != exitOK || strings.Count(r.stdout, "\n") != 3 || !strings.Contains(r.stdout, "\n3 ") {
		t.Errorf("meridian status -n 2 = %+v", r)
	}
	if r := commandTest(t, env, "status", "-n", "0"); r.code != exitError || !strings.Contains(r.stderr, "(flag_value_invalid)") {
		t.Errorf("meridian status -n 0 = %+v", r)
	}

	r = commandTest(t, env, "status", "3")
	one := regexp.MustCompile(`^run 3: created
workflow: w\.yaml
run directory: ` + regexp.QuoteMeta(createdDir) + `
created ` + when + `

JOB +STATE +OUTCOME +ATTEMPTS +WHERE
main +waiting +0 *
$`)
	if r.code != exitOK || !one.MatchString(r.stdout) || r.stderr != "" {
		t.Errorf("meridian status of a run created = %+v", r)
	}
	r = commandTest(t, env, "status", "4")
	one = regexp.MustCompile(`^run 4: running
workflow: w\.yaml
run directory: \S+
created ` + when + `, started ` + when + `

JOB +STATE +OUTCOME +ATTEMPTS +WHERE
main +running +1 +local/\S+
$`)
	if r.code != exitOK || !one.MatchString(r.stdout) || r.stderr != "" {
		t.Errorf("meridian status of a run running = %+v", r)
	}
	r = commandTest(t, env, "status", "--format", "json", "4")
	lines := jsonLines(t, r.stdout)
	if r.code != exitOK || len(lines) != 2 {
		t.Fatalf("meridian status --format json 4 = %+v", r)
	}
	if run := lines[0]; run["version"] != float64(local.ProgressVersion) || run["kind"] != "run" || run["run"] != "4" || run["state"] != "running" || run["created"] == nil || run["started"] == nil || run["ended"] != nil {
		t.Errorf("meridian status --format json 4 showed the run as %v", run)
	}
	if job := lines[1]; job["kind"] != "job" || job["run"] != "4" || job["job"] != "main" || job["state"] != "running" || job["attempts"] != float64(1) || !strings.HasPrefix(job["member"].(string), "local/") {
		t.Errorf("meridian status --format json 4 showed the job as %v", job)
	}

	release()
	if r := <-running.done; r.code != exitOK {
		t.Fatalf("meridian run of the run released = %+v", r)
	}
	for id, want := range map[string]string{"4": "success", "2": "failure"} {
		r = commandTest(t, env, "status", id)
		one = regexp.MustCompile(`^run ` + id + `: ended, ` + want + `
workflow: w\.yaml
run directory: \S+
created ` + when + `, started ` + when + `, ended ` + when + `

JOB +STATE +OUTCOME +ATTEMPTS +WHERE
main +ended +` + want + ` +1 +local/\S+
$`)
		if r.code != exitOK || !one.MatchString(r.stdout) || r.stderr != "" {
			t.Errorf("meridian status of a run that ended, %s = %+v", want, r)
		}
	}
	r = commandTest(t, env, "status", "--format", "json")
	lines = jsonLines(t, r.stdout)
	if r.code != exitOK || len(lines) != 4 || lines[0]["run"] != "4" || lines[0]["outcome"] != "success" || lines[3]["run"] != "1" {
		t.Errorf("meridian status --format json = %+v", r)
	}
	if r := commandTest(t, env, "status", "9"); r.code != exitError || !strings.Contains(r.stderr, "(run_not_found)") || r.stdout != "" {
		t.Errorf("meridian status of a run the store hasn't = %+v", r)
	}
}

// TestStatusOlderStore checks that meridian status and logs, which only
// read the store, refuse one whose schema an earlier release, which may
// still be running a workflow with it, left at an earlier version, and
// leave it as it is, rather than bring it up to date.
func TestStatusOlderStore(t *testing.T) {
	parallel(t)
	env := runEnv(t)
	if r := runTest(t, env, nil, writeFile(t, "w.yaml", helloWorkflow)); r.code != exitOK {
		t.Fatalf("meridian run = %+v", r)
	}
	d, err := hoppersqlite.OpenContext(t.Context(), filepath.Join(env[local.EnvStateDir], local.DatabaseFile), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	version := func() int {
		t.Helper()
		var v int
		if err := d.DB().QueryRowContext(t.Context(), "SELECT max(version) FROM meridian_schema").Scan(&v); err != nil {
			t.Fatal(err)
		}
		return v
	}
	latest := version()
	if _, err := d.DB().ExecContext(t.Context(), "DELETE FROM meridian_schema WHERE version = ?", latest); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"status"}, {"status", "1"}, {"logs", "1"}} {
		r := commandTest(t, env, args...)
		if r.code != exitError || !strings.Contains(r.stderr, "(database_too_old)") || r.stdout != "" {
			t.Errorf("meridian %s of an earlier release's store = %+v", strings.Join(args, " "), r)
		}
		if v := version(); v != latest-1 {
			t.Errorf("meridian %s brought the store's schema to version %d", strings.Join(args, " "), v)
		}
	}
}
