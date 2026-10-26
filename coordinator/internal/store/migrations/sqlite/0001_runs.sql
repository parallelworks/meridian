-- Meridian's own tables, beside hopper's in the same database: what hopper
-- doesn't model of a run. Times are microseconds since the Unix epoch, by
-- the store's clock, which is hopper's. The foreign keys say what refers to
-- what; hopper's connections turn SQLite's enforcement of them off, so the
-- store keeps them itself, and its tests check them (PRAGMA
-- foreign_key_check).

-- The store's own values: its ID (store_id), random, made when the store
-- is first opened, which each run's secret record is bound to with the
-- run's ID; and the number of the last run it made (last_run), which only
-- grows.
CREATE TABLE meridian_store (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
) STRICT, WITHOUT ROWID;

INSERT INTO meridian_store (key, value) VALUES ('last_run', '0');

-- A run, by its number in the store, which is its ID. The plan is
-- plan.Encode's, with its version and digest beside it, so that a
-- coordinator can tell whether it reads the run without decoding it; it
-- never changes. name, location and release are the plan's, for listing
-- runs. workflow_id is the run's hopper workflow. record_gen is the
-- generation of the run's secret record, which the record is sealed with:
-- 0 before it has one, 1 once it is stored, and one more each time it is
-- sealed again. It is kept here, apart from the sealed record, and changes
-- in the transaction that changes the record, so that an older record of
-- the run put back in its place doesn't open.
CREATE TABLE meridian_runs (
  id INTEGER PRIMARY KEY,
  created_at INTEGER NOT NULL,
  plan_version INTEGER NOT NULL,
  plan_digest TEXT NOT NULL,
  plan BLOB NOT NULL,
  name TEXT NOT NULL,
  location TEXT NOT NULL,
  release TEXT NOT NULL,
  default_group TEXT NOT NULL,
  run_dir TEXT NOT NULL,
  workflow_id TEXT NOT NULL,
  record_gen INTEGER NOT NULL DEFAULT 0,
  state TEXT NOT NULL,
  outcome TEXT,
  reason TEXT,
  started_at INTEGER,
  ended_at INTEGER
) STRICT;

CREATE TRIGGER meridian_runs_plan_fixed
BEFORE UPDATE OF id, created_at, plan_version, plan_digest, plan ON meridian_runs
BEGIN
  SELECT RAISE(ABORT, 'meridian: a run''s plan never changes');
END;

-- A run's secret record, sealed under a data key of its own, which
-- wrapped_key holds as Keys wrapped it. It is sealed again as parts are
-- added, under the same key, with the next generation (the run's
-- record_gen), and deleted when the run ends. The sealed bytes say which
-- version of the sealing they are.
CREATE TABLE meridian_records (
  run_id INTEGER PRIMARY KEY REFERENCES meridian_runs (id),
  wrapped_key BLOB NOT NULL,
  sealed BLOB NOT NULL,
  updated_at INTEGER NOT NULL
) STRICT;

CREATE TRIGGER meridian_records_key_fixed
BEFORE UPDATE OF run_id, wrapped_key ON meridian_records
BEGIN
  SELECT RAISE(ABORT, 'meridian: a secret record''s data key never changes');
END;

-- A job of a run, or an instance of a matrix job (instance is its index),
-- by its key: the job's name, or the name, # and the index, as build#2.
-- supervisor_id is the hopper job that supervises it. fence grows with each
-- attempt, which holds the job while its own fence is the job's; attempts
-- counts them, and attempts_started those that started a step. Once the
-- job has ended, outcome is set, and result holds how it ended, as run
-- state holds it (plan.JobResult.Split), encoded as the worker API encodes
-- it.
CREATE TABLE meridian_jobs (
  run_id INTEGER NOT NULL REFERENCES meridian_runs (id),
  key TEXT NOT NULL,
  name TEXT NOT NULL,
  instance INTEGER,
  supervisor_id TEXT NOT NULL,
  fence INTEGER NOT NULL DEFAULT 0,
  attempts INTEGER NOT NULL DEFAULT 0,
  attempts_started INTEGER NOT NULL DEFAULT 0,
  created_at INTEGER NOT NULL,
  outcome TEXT,
  reason TEXT,
  result BLOB,
  ended_at INTEGER,
  PRIMARY KEY (run_id, key)
) STRICT, WITHOUT ROWID;

-- An attempt of a job, and the record of it: a placement of its worker,
-- which claims it with its bootstrap token and its own credential, both
-- kept only as SHA-256 digests. Its lease lasts lease_seconds after
-- renewed_at, the claim's or the last heartbeat's. It holds its job while
-- it hasn't ended and its fence is the job's. A job has one attempt that
-- hasn't ended at most.
CREATE TABLE meridian_attempts (
  id TEXT PRIMARY KEY,
  run_id INTEGER NOT NULL,
  job_key TEXT NOT NULL,
  number INTEGER NOT NULL,
  fence INTEGER NOT NULL,
  state TEXT NOT NULL,
  token_hash BLOB NOT NULL,
  credential_hash BLOB,
  self_fencing INTEGER NOT NULL,
  lease_seconds INTEGER NOT NULL,
  heartbeat_seconds INTEGER NOT NULL,
  worker_version TEXT,
  worker_digest TEXT,
  created_at INTEGER NOT NULL,
  claimed_at INTEGER,
  renewed_at INTEGER,
  step_started_at INTEGER,
  end_kind TEXT,
  outcome TEXT,
  reason TEXT,
  complete_digest BLOB,
  ended_at INTEGER,
  FOREIGN KEY (run_id, job_key) REFERENCES meridian_jobs (run_id, key)
) STRICT, WITHOUT ROWID;

CREATE UNIQUE INDEX meridian_attempts_token ON meridian_attempts (token_hash);
CREATE UNIQUE INDEX meridian_attempts_credential ON meridian_attempts (credential_hash);
CREATE UNIQUE INDEX meridian_attempts_fence ON meridian_attempts (run_id, job_key, fence);
CREATE UNIQUE INDEX meridian_attempts_number ON meridian_attempts (run_id, job_key, number);
CREATE UNIQUE INDEX meridian_attempts_live ON meridian_attempts (run_id, job_key) WHERE state <> 'ended';

-- An endpoint that workers reach the coordinator through, such as the
-- local socket, and who holds it, under a file lock beside the database,
-- since when. From grace_from, when it was taken, or when its holder's
-- host woke, the coordinator acts on no lapsed lease for a grace period.
CREATE TABLE meridian_endpoints (
  name TEXT PRIMARY KEY,
  holder TEXT NOT NULL,
  taken_at INTEGER NOT NULL,
  grace_from INTEGER NOT NULL
) STRICT, WITHOUT ROWID;
