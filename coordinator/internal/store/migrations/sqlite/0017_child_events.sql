-- The child run a run's child.created or child.ended event is about: one
-- that a try of a step of the run's called (child_run); none for any other
-- event.
ALTER TABLE meridian_run_events ADD COLUMN child_run INTEGER REFERENCES meridian_runs (id);
