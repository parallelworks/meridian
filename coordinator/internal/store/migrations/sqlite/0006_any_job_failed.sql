-- When the first job of the run to fail ended, which every attempt of the
-- run tells its worker from then on, for the steps whose early-cancel: is
-- any-job-failed (workerapi.Control.AnyJobFailed).
ALTER TABLE meridian_runs ADD COLUMN any_job_failed_at INTEGER;
