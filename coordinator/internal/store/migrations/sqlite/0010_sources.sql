-- The texts of the workflows a run's tree pins (compile.Created.Texts), by
-- the digests of their texts, which a child run is created from, without a
-- fetch, as the run was created with them: each kept once, whatever runs
-- pin it. Only the bytes: where a run found a text is its plan's
-- (plan.Source.Location), and another run that pins the same bytes, from
-- another place, learns nothing of it here.
CREATE TABLE meridian_sources (
  digest TEXT PRIMARY KEY,
  text BLOB NOT NULL
) STRICT, WITHOUT ROWID;

-- Which texts each run pins: a run reads only those.
CREATE TABLE meridian_run_sources (
  run_id INTEGER NOT NULL REFERENCES meridian_runs (id),
  digest TEXT NOT NULL REFERENCES meridian_sources (digest),
  PRIMARY KEY (run_id, digest)
) STRICT, WITHOUT ROWID;

CREATE INDEX meridian_run_sources_digest ON meridian_run_sources (digest);
