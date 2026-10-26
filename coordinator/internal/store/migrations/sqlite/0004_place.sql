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

-- Where an attempt is placed when where its job runs is private: its
-- group, member and backend, and then the backend's placement, sealed under
-- the run's data key, which only the coordinator opens. Where it isn't
-- private, grp, member and backend hold them. Either is recorded before the
-- backend is asked, so that a supervisor that ends before it records the
-- placement finds it again.
ALTER TABLE meridian_attempts ADD COLUMN private_placement BLOB;
