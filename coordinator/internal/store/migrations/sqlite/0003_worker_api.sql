-- What the worker API keeps of an attempt, beside what 0001 keeps.

-- Where the attempt was placed: its group, its member, the backend and its
-- own name for the placement, unless where its job runs is private
-- (plan.ReadyJob.PrivateFields); and the digest of the worker binary the
-- placement started, as sha256: and its hex, which the worker's claim must
-- give (workerapi.Worker.Digest), and which every placement recorded has,
-- a private one's too, so that it says the attempt is placed.
ALTER TABLE meridian_attempts ADD COLUMN grp TEXT;
ALTER TABLE meridian_attempts ADD COLUMN member TEXT;
ALTER TABLE meridian_attempts ADD COLUMN backend TEXT;
ALTER TABLE meridian_attempts ADD COLUMN placement_id TEXT;
ALTER TABLE meridian_attempts ADD COLUMN placed_digest TEXT;

-- Where an attempt is placed when where its job runs is private: its
-- group, member and backend, and the backend's name for the placement,
-- sealed under the run's data key, which only the coordinator opens; grp,
-- member, backend and placement_id are then NULL. A problem
-- such an attempt ends with is stored masked, since it can name the
-- member.
ALTER TABLE meridian_attempts ADD COLUMN private_placement BLOB;

-- What the coordinator asks of the attempt's worker (workerapi.Control),
-- whose version grows each time it asks something more.
ALTER TABLE meridian_attempts ADD COLUMN control_version INTEGER NOT NULL DEFAULT 0;
ALTER TABLE meridian_attempts ADD COLUMN control BLOB;

-- How the attempt ended: report is the worker's report of its end
-- (workerapi.Complete), with only the part of its result that run state
-- holds (plan.JobResult.Split), as the worker API encodes it; problem is
-- what failed before a step started, as a problem's JSON.
ALTER TABLE meridian_attempts ADD COLUMN report BLOB;
ALTER TABLE meridian_attempts ADD COLUMN problem BLOB;

-- The attempt's step events, as its worker reported them, by their
-- sequence numbers, from 1, without a gap: their outputs are masked, as
-- the worker masks its logs.
CREATE TABLE meridian_events (
  attempt_id TEXT NOT NULL REFERENCES meridian_attempts (id),
  seq INTEGER NOT NULL,
  body BLOB NOT NULL,
  stored_at INTEGER NOT NULL,
  PRIMARY KEY (attempt_id, seq)
) STRICT, WITHOUT ROWID;

-- What the attempt's reports take: how many events it has, and their
-- bytes, and the bytes of its logs, which the worker API's quotas read
-- without counting them each time.
ALTER TABLE meridian_attempts ADD COLUMN events_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE meridian_attempts ADD COLUMN events_bytes INTEGER NOT NULL DEFAULT 0;
ALTER TABLE meridian_attempts ADD COLUMN log_bytes INTEGER NOT NULL DEFAULT 0;

-- What a run's secret record holds of a job, or of a matrix instance, that
-- ended with private outputs (plan.Record.Jobs): sealed as a part of its
-- own, under a key that the run's data key (meridian_records.wrapped_key)
-- derives, so that a job's end writes its part alone. Its generation is the job's record_gen,
-- kept apart from it, as the run's is from the record. It is deleted, and
-- erased, with the run's record.
ALTER TABLE meridian_jobs ADD COLUMN record_gen INTEGER NOT NULL DEFAULT 0;
CREATE TABLE meridian_job_records (
  run_id INTEGER NOT NULL,
  job_key TEXT NOT NULL,
  sealed BLOB NOT NULL,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY (run_id, job_key),
  FOREIGN KEY (run_id, job_key) REFERENCES meridian_jobs (run_id, key)
) STRICT, WITHOUT ROWID;
