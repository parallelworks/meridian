-- The run's stop record: when it was stopped early, and why, cancelled or
-- timeout, which a person's cancel, or the run's own timeout:, stores once,
-- and which never changes after.
ALTER TABLE meridian_runs ADD COLUMN stopped_at INTEGER;
ALTER TABLE meridian_runs ADD COLUMN stop_reason TEXT;
