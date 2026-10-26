-- What the worker API keeps of an attempt, beside what 0001 keeps.

-- Where the attempt was placed: its group and member, unless where its job
-- runs is private (plan.ReadyJob.PrivateFields), the backend and its own
-- name for the placement, and the digest of the worker binary the
-- placement started, as sha256: and its hex, which the worker's claim must
-- give (workerapi.Worker.Digest), or NULL where its backend can't say.
ALTER TABLE meridian_attempts ADD COLUMN grp TEXT;
ALTER TABLE meridian_attempts ADD COLUMN member TEXT;
ALTER TABLE meridian_attempts ADD COLUMN backend TEXT;
ALTER TABLE meridian_attempts ADD COLUMN placement_id TEXT;
ALTER TABLE meridian_attempts ADD COLUMN placed_digest TEXT;

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
