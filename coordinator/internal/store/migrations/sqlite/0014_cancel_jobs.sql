-- When a step of the job's run, through parallelworks/cancel-jobs,
-- cancelled the job, and which step, as /jobs/<its job's key>/steps/<its
-- index>, as in /jobs/build#1/steps/2: NULL for a job no step cancelled.
-- Each of the job's attempts tells its worker to cancel its job from then
-- on, and a job that hasn't started never starts.
ALTER TABLE meridian_jobs ADD COLUMN cancelled_at INTEGER;
ALTER TABLE meridian_jobs ADD COLUMN cancelled_by TEXT;
