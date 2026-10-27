-- A child run's link to the try of the step that called it (store.Link):
-- the run whose step it was (parent_run), its job's key (parent_job), the
-- attempt that ran the step (parent_attempt), the step's index in its job
-- (parent_step) and its try (parent_try), from 0; and the run at the top of
-- its tree (root_run), and how far below it the run is (depth), from 1. A
-- run no step called has none of them, and depth 0. A step's try has one
-- child at most, which a repeated ask finds again.
ALTER TABLE meridian_runs ADD COLUMN parent_run INTEGER REFERENCES meridian_runs (id);
ALTER TABLE meridian_runs ADD COLUMN parent_job TEXT;
ALTER TABLE meridian_runs ADD COLUMN parent_attempt TEXT REFERENCES meridian_attempts (id);
ALTER TABLE meridian_runs ADD COLUMN parent_step INTEGER;
ALTER TABLE meridian_runs ADD COLUMN parent_try INTEGER;
ALTER TABLE meridian_runs ADD COLUMN root_run INTEGER REFERENCES meridian_runs (id);
ALTER TABLE meridian_runs ADD COLUMN depth INTEGER NOT NULL DEFAULT 0;

CREATE UNIQUE INDEX meridian_runs_child ON meridian_runs (parent_attempt, parent_step, parent_try)
WHERE parent_attempt IS NOT NULL;

CREATE INDEX meridian_runs_tree ON meridian_runs (root_run, state) WHERE root_run IS NOT NULL;

-- The run's finisher, a hopper job, which waits for the runs of its tree
-- to end before it ends a root run, and which the end of one wakes then
-- (waited_at, once it waits); none for a run stored before.
ALTER TABLE meridian_runs ADD COLUMN finisher_id TEXT;
ALTER TABLE meridian_runs ADD COLUMN waited_at INTEGER;
