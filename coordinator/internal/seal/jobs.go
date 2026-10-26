package seal

import (
	"context"
	jsonv2 "encoding/json/v2"
	"fmt"

	"github.com/parallelworks/meridian/plan"
)

// A JobPart is what the store holds of a job's part of a run's secret
// record: the part, sealed (SealJob), and its generation, which the store
// keeps apart from it.
type JobPart struct {
	Generation uint64
	Sealed     []byte
}

// SealJob seals rec, what the run runID's secret record holds of the job,
// or the matrix instance, whose key is job (plan.Record.Jobs), as a part of
// its own (SealPart, PurposeJobRecord), of generation gen, under a key
// that the run's data key, which Keys wrapped as wrapped (Sealed.Key),
// derives. A job's end so writes its part alone, in the time its own size
// takes, rather than seal the run's whole record again.
func (s *Sealer) SealJob(ctx context.Context, runID, job string, gen uint64, wrapped []byte, rec plan.JobRecord) ([]byte, error) {
	id := PartID{Purpose: PurposeJobRecord, ID: job, Generation: gen}
	if err := checkPartID(runID, id); err != nil {
		return nil, err
	}
	data, err := jsonv2.Marshal(rec, jsonv2.Deterministic(true))
	if err != nil {
		// What the encoder says can quote a value in the record.
		return nil, fmt.Errorf("seal: run %s's secret record of job %q doesn't encode", runID, job)
	}
	defer clear(data)
	return s.SealPart(ctx, runID, wrapped, id, data)
}

// OpenJobs opens parts, the jobs' parts of the run runID's secret record
// by their keys (SealJob), with one unwrap of the run's data key, which
// Keys wrapped as wrapped, and decodes them. A part that doesn't open as
// the latest of its job, run and store is record_unreadable, as a whole
// record is (Open), and so are its other errors.
func (s *Sealer) OpenJobs(ctx context.Context, runID string, wrapped []byte, parts map[string]JobPart) (map[string]plan.JobRecord, error) {
	sealed := make([]SealedPart, 0, len(parts))
	for job, p := range parts {
		sealed = append(sealed, SealedPart{Purpose: PurposeJobRecord, ID: job, Generation: p.Generation, Sealed: p.Sealed})
	}
	opened, err := s.OpenParts(ctx, runID, wrapped, sealed)
	if err != nil {
		return nil, err
	}
	defer clearAll(opened)
	out := make(map[string]plan.JobRecord, len(parts))
	for i, p := range sealed {
		var rec plan.JobRecord
		if err := jsonv2.Unmarshal(opened[i], &rec, jsonv2.RejectUnknownMembers(true)); err != nil {
			// What the decoder says can quote a value in the record.
			return nil, fmt.Errorf("%w: run %s's secret record of job %q doesn't decode", plan.ErrMalformed, runID, p.ID)
		}
		out[p.ID] = rec
	}
	return out, nil
}
