-- A run's events: each change to the run, to one of its jobs, attempts or
-- steps, that whoever follows the run sees, added in the transaction that
-- makes the change. seq numbers them, from 1, in the order of the
-- transactions that added them: the run's events_seq, which each grows by
-- one, is the last's. What an event is about is the row it names, as it is
-- now: the run's, its job's (job_key), its attempt's (attempt_id), or its
-- step's, the attempt's event (meridian_events) numbered step_seq. at is
-- when it was added, by the store's clock.
ALTER TABLE meridian_runs ADD COLUMN events_seq INTEGER NOT NULL DEFAULT 0;
CREATE TABLE meridian_run_events (
  run_id INTEGER NOT NULL REFERENCES meridian_runs (id),
  seq INTEGER NOT NULL,
  kind TEXT NOT NULL,
  job_key TEXT,
  attempt_id TEXT,
  step_seq INTEGER,
  at INTEGER NOT NULL,
  PRIMARY KEY (run_id, seq)
) STRICT, WITHOUT ROWID;
