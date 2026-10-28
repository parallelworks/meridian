-- A step run of a step's cleanup: (parent_cleanup), which a step's try's
-- own step run, of the same step and try, isn't. Each of a step's tries
-- has one child at most, and its cleanup one more.
ALTER TABLE meridian_runs ADD COLUMN parent_cleanup INTEGER NOT NULL DEFAULT 0;

DROP INDEX meridian_runs_child;
CREATE UNIQUE INDEX meridian_runs_child ON meridian_runs (parent_attempt, parent_step, parent_try, parent_cleanup)
WHERE parent_attempt IS NOT NULL;
