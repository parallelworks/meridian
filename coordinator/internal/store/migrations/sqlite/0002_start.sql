-- What a run's Start gave that run state holds, which whoever can see the
-- run can read (compile.EncodeStartState), stored once Start has kept its
-- part; how many times the coordinator tried to start the run and
-- couldn't, as when a resolver couldn't answer; and the run's problems,
-- Start's and the coordinator's own, as a list of diagnostics, which never
-- show a value made from a variable or a password.
ALTER TABLE meridian_runs ADD COLUMN start_state BLOB;
ALTER TABLE meridian_runs ADD COLUMN start_tries INTEGER NOT NULL DEFAULT 0;
ALTER TABLE meridian_runs ADD COLUMN diagnostics BLOB;
