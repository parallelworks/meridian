-- A step run (docs/design.md, Steps on another host): a child run of a
-- step's try that runs that step on another host than its job's. It has no
-- plan of its own, but names its caller's, the plan of the run its link
-- names (parent_run), by its digest (plan_digest), and the job and the step
-- in it (parent_job, parent_step): its plan column is empty, and Tx.Run
-- reads its caller's.
ALTER TABLE meridian_runs ADD COLUMN step_run INTEGER NOT NULL DEFAULT 0;
