-- What a matrix job's supervisor keeps of its instances.

-- expansion is the matrix job's instances as run state holds them, once
-- its supervisor has expanded its matrix at Ready (plan.Expansion.Split):
-- one Start expanded is in the run's start state. turn_at is when an
-- instance was given its turn to be placed, under its strategy's
-- max-parallel: (at once without one), and fail_fast_at when a failure of
-- one of a matrix job's instances stopped the rest, as its strategy's
-- fail-fast: asks.
ALTER TABLE meridian_jobs ADD COLUMN expansion BLOB;
ALTER TABLE meridian_jobs ADD COLUMN turn_at INTEGER;
ALTER TABLE meridian_jobs ADD COLUMN fail_fast_at INTEGER;

-- A job other jobs need any instance of (needs: <job>:any), by its key, and
-- the hopper job that waits for one, which the jobs that need it wait for:
-- once it has, instance_key is the instance they read, chosen at
-- chosen_at, or NULL for the job itself, as for one that never expanded;
-- and ended_keys the instances that had ended by then, a JSON list of
-- their keys, whose outputs they read.
CREATE TABLE meridian_anys (
  run_id INTEGER NOT NULL REFERENCES meridian_runs (id),
  job_key TEXT NOT NULL,
  marker_id TEXT NOT NULL,
  chosen_at INTEGER,
  instance_key TEXT,
  ended_keys BLOB,
  PRIMARY KEY (run_id, job_key)
) STRICT, WITHOUT ROWID;
