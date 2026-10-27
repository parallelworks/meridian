-- What a job's supervisor keeps as it readies and places the job.

-- What the job was first readied with, pinned then, so that Ready, run
-- again at each claim of an attempt, gives the same job: when (ready_at),
-- whether the run had been stopped, by a cancel or by its timeout
-- (readied_stopped), and for each need on any instance of a matrix job,
-- the instance that met it (any_choices, a JSON object of instance keys by
-- job name). wake grows each time something the supervisor waits for
-- happens, as an attempt's claim or end, which the supervisor reads again
-- before it waits. place_tries counts the times in a row that a backend
-- couldn't answer as the job was placed. diagnostics are the job's
-- problems, as Ready's or placement's, which never show a value made from a
-- variable or a password.
ALTER TABLE meridian_jobs ADD COLUMN ready_at INTEGER;
ALTER TABLE meridian_jobs ADD COLUMN readied_stopped INTEGER;
ALTER TABLE meridian_jobs ADD COLUMN any_choices BLOB;
ALTER TABLE meridian_jobs ADD COLUMN wake INTEGER NOT NULL DEFAULT 0;
ALTER TABLE meridian_jobs ADD COLUMN place_tries INTEGER NOT NULL DEFAULT 0;
ALTER TABLE meridian_jobs ADD COLUMN diagnostics BLOB;

-- The generation of an attempt's private place (private_placement, which
-- 0003 adds), from 1, kept apart from it, as a job's record_gen is from
-- its part: its group, member and backend are sealed as the first, before
-- the backend is asked, so that a supervisor that ends before it records
-- the placement finds the member again, and the next adds the backend's ID
-- for the placement, as the placement is recorded. 0 for a place in the
-- clear.
ALTER TABLE meridian_attempts ADD COLUMN placement_gen INTEGER NOT NULL DEFAULT 0;
