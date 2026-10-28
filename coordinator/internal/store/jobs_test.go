package store

import (
	"errors"
	"reflect"
	"testing"

	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
)

func TestJobs(t *testing.T) {
	s := open(t)
	id := createRun(t, s)
	inTx(t, s, func(tx *Tx) error {
		for i, key := range []string{"test#0", "test#12"} {
			if err := tx.InsertJob(NewJob{Run: id, Name: "test", Key: key, SupervisorID: fakeJobID(10 + i)}); err != nil {
				return err
			}
		}
		for _, j := range []NewJob{
			{Run: id, Name: "test", Key: "test#01", SupervisorID: fakeJobID(20)},
			{Run: id, Name: "test", Key: "test#-1", SupervisorID: fakeJobID(20)},
			{Run: id, Name: "test", Key: "build", SupervisorID: fakeJobID(20)},
			{Run: id, Name: "test", Key: "test#", SupervisorID: fakeJobID(20)},
			{Run: id, Name: "", Key: "", SupervisorID: fakeJobID(20)},
			{Run: id, Name: "deploy", Key: "deploy"},
		} {
			if err := tx.InsertJob(j); err == nil {
				t.Errorf("InsertJob(%+v) succeeded", j)
			}
		}
		if err := tx.InsertJob(NewJob{Run: id, Name: "build", Key: "build", SupervisorID: fakeJobID(20)}); err == nil {
			t.Error("InsertJob stored a job twice")
		}
		if err := tx.InsertJob(NewJob{Run: "9", Name: "a", Key: "a", SupervisorID: fakeJobID(20)}); !errors.Is(err, ErrNotFound) {
			t.Errorf("InsertJob of no run = %v, want ErrNotFound", err)
		}
		return nil
	})
	inTx(t, s, func(tx *Tx) error {
		jobs, err := tx.Jobs(id)
		if err != nil {
			return err
		}
		var got []string
		for _, j := range jobs {
			got = append(got, j.Key)
			if j.Run != id || j.Fence != 0 || j.Attempts != 0 || j.Outcome != "" || j.Result != nil || j.CreatedAt.IsZero() {
				t.Errorf("job = %+v", j)
			}
		}
		if want := []string{"build", "test", "test#0", "test#12"}; !reflect.DeepEqual(got, want) {
			t.Errorf("Jobs = %v, want %v", got, want)
		}
		j, err := tx.Job(id, "test#12")
		if err != nil {
			return err
		}
		if j.Name != "test" || j.Instance != 12 || j.SupervisorID != fakeJobID(11) {
			t.Errorf("job = %+v", j)
		}
		if j, err := tx.Job(id, "build"); err != nil || j.Instance != -1 || j.SupervisorID != fakeJobID(1) {
			t.Errorf("Job(build) = %+v, %v", j, err)
		}
		if _, err := tx.Job(id, "deploy"); !errors.Is(err, ErrNotFound) {
			t.Errorf("Job of no job = %v, want ErrNotFound", err)
		}
		if _, err := tx.Jobs("9"); !errors.Is(err, ErrNotFound) {
			t.Errorf("Jobs of no run = %v, want ErrNotFound", err)
		}
		return nil
	})
	checkIntegrity(t, s)
}

func TestEndJob(t *testing.T) {
	s := open(t)
	id := createRun(t, s)
	whole := plan.JobResult{
		Outcome: plan.OutcomeSuccess,
		Outputs: map[string]string{"version": "1.2", "token": "s3cret"},
		Secrets: []string{"token"},
		Steps:   map[string]map[string]string{"compile": {"path": "/out"}},
	}
	_, state := whole.Split()
	inTx(t, s, func(tx *Tx) error {
		if err := tx.EndJob(id, "build", JobEnd{Result: whole}); err == nil {
			t.Error("EndJob stored a private output")
		}
		if err := tx.EndJob(id, "build", JobEnd{Result: plan.JobResult{Outcome: "done"}}); err == nil {
			t.Error("EndJob stored an outcome that isn't one")
		}
		if err := tx.EndJob(id, "deploy", JobEnd{Result: state}); !errors.Is(err, ErrNotFound) {
			t.Errorf("EndJob of no job = %v, want ErrNotFound", err)
		}
		a, _, err := tx.NewAttempt(NewAttempt{Run: id, Job: "build", LeaseSeconds: 60, HeartbeatSeconds: 10})
		if err != nil {
			return err
		}
		if err := tx.EndJob(id, "build", JobEnd{Result: state}); !errors.Is(err, ErrState) {
			t.Errorf("EndJob with an attempt that hasn't ended = %v, want ErrState", err)
		}
		if err := tx.EndAttempt(a.ID, AttemptEnd{Kind: EndLost, Reason: plan.ReasonWorkerLost}); err != nil {
			return err
		}
		return tx.EndJob(id, "build", JobEnd{Result: state, Reason: plan.ReasonWorkerLost})
	})
	inTx(t, s, func(tx *Tx) error {
		j, err := tx.Job(id, "build")
		if err != nil {
			return err
		}
		if j.Outcome != plan.OutcomeSuccess || j.Reason != plan.ReasonWorkerLost || j.EndedAt.IsZero() || !reflect.DeepEqual(j.Result, &state) {
			t.Errorf("an ended job is %+v, its result %+v; want %+v", j, j.Result, state)
		}
		if err := tx.EndJob(id, "build", JobEnd{Result: state}); !errors.Is(err, ErrState) {
			t.Errorf("EndJob again = %v, want ErrState", err)
		}
		if _, _, err := tx.NewAttempt(NewAttempt{Run: id, Job: "build", LeaseSeconds: 60, HeartbeatSeconds: 10}); !errors.Is(err, ErrState) {
			t.Errorf("NewAttempt of an ended job = %v, want ErrState", err)
		}
		return nil
	})
}

// TestNoteJob checks that the warnings Ready gave a job are kept with it,
// each once however often it is readied, and that its end keeps them,
// before its own problems.
func TestNoteJob(t *testing.T) {
	s := open(t)
	id := createRun(t, s)
	empty := problems.JobSSHHostEmpty.At("", problems.Position{}, "/jobs/build/ssh", nil)
	ended := problems.PlacementImpossible.At("", problems.Position{}, "", problems.Params{"job": "build"})
	inTx(t, s, func(tx *Tx) error {
		for range 2 {
			if err := tx.NoteJob(id, "build", problems.Diagnostics{empty}); err != nil {
				return err
			}
		}
		if err := tx.NoteJob(id, "deploy", problems.Diagnostics{empty}); !errors.Is(err, ErrNotFound) {
			t.Errorf("NoteJob of no job = %v, want ErrNotFound", err)
		}
		j, err := tx.Job(id, "build")
		if err != nil {
			return err
		}
		if got := codesOf(j.Diagnostics); !reflect.DeepEqual(got, []string{"job_ssh_host_empty /jobs/build/ssh"}) {
			t.Errorf("the job's problems are %v", got)
		}
		return tx.EndJob(id, "build", JobEnd{Result: plan.JobResult{Outcome: plan.OutcomeFailure}, Diagnostics: problems.Diagnostics{ended}})
	})
	inTx(t, s, func(tx *Tx) error {
		j, err := tx.Job(id, "build")
		if err != nil {
			return err
		}
		if got, want := codesOf(j.Diagnostics), []string{"job_ssh_host_empty /jobs/build/ssh", "placement_impossible "}; !reflect.DeepEqual(got, want) {
			t.Errorf("the ended job's problems are %v, want %v", got, want)
		}
		if err := tx.NoteJob(id, "build", problems.Diagnostics{empty}); !errors.Is(err, ErrState) {
			t.Errorf("NoteJob of an ended job = %v, want ErrState", err)
		}
		return nil
	})
}

// codesOf returns each of diags as its code and path.
func codesOf(diags problems.Diagnostics) []string {
	var out []string
	for _, d := range diags {
		out = append(out, string(d.Code)+" "+d.Path())
	}
	return out
}

// TestMalformedResult checks that a job's result the store holds is read
// as strictly as a worker's report: a member this release doesn't know,
// which a newer release wrote, is ErrMalformed, not dropped.
func TestMalformedResult(t *testing.T) {
	s := open(t)
	id := createRun(t, s)
	for _, result := range []string{
		`{"outcome":"success","shiny":true}`,
		`{"outcome":"done"}`,
		`{"outputs":{}}`,
		`{"outcome":"success","outcome":"failure"}`,
		`not JSON`,
	} {
		inTx(t, s, func(tx *Tx) error {
			_, err := tx.tx.ExecContext(tx.ctx, "UPDATE meridian_jobs SET outcome = 'success', result = ? WHERE key = 'build'", []byte(result))
			return err
		})
		inTx(t, s, func(tx *Tx) error {
			if _, err := tx.Job(id, "build"); !errors.Is(err, ErrMalformed) {
				t.Errorf("Job with the result %s = %v, want ErrMalformed", result, err)
			}
			if _, err := tx.Jobs(id); !errors.Is(err, ErrMalformed) {
				t.Errorf("Jobs with the result %s = %v, want ErrMalformed", result, err)
			}
			return nil
		})
	}
}
