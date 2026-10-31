-- A session a run's sessions: made (compile.Sessions.Make), by its ID,
-- random. root_id is the run at the top of the tree that made it, which
-- owns it, and whose end ends it; run_id is the run whose sessions: made
-- it, the root, or a run a step called, and key its key there; name is its
-- name, which sessions.<key> gives, unique among the store's sessions that
-- haven't ended. type is tunnel or link, and redirect whether its
-- redirect: is true. label is the session's, random, made once, at whose
-- host name, <label>.localhost, it is served, the same each time; salt is
-- the session's, which its secret is made of, and which a reset replaces.
--
-- data is what the session is, and where it points, as JSON: its
-- definition, as the run's sessions: gave it, and where the last
-- update-session pointed it, as the session host read it. It is in the
-- clear, or, once what gave it read a variable or a password, sealed as a
-- part of the root's secrets (data_sealed), of generation data_gen, which
-- is kept apart from it, and is sealed from then on. port is the port of
-- 127.0.0.1 the session is served at, once it points somewhere, and
-- port_given whether the step gave it, which a session served again keeps;
-- serve_problem is why the process that serves the session can't, as it
-- last said, a problem's JSON, or NULL, which a step that points it again
-- clears. ended_at is set as its root ends, which erases its data.
CREATE TABLE meridian_sessions (
  id TEXT PRIMARY KEY,
  root_id INTEGER NOT NULL REFERENCES meridian_runs (id),
  run_id INTEGER NOT NULL REFERENCES meridian_runs (id),
  key TEXT NOT NULL,
  name TEXT NOT NULL,
  type TEXT NOT NULL,
  redirect INTEGER NOT NULL DEFAULT 0,
  label TEXT NOT NULL,
  salt BLOB NOT NULL,
  data TEXT,
  data_sealed BLOB,
  data_gen INTEGER NOT NULL DEFAULT 0,
  port INTEGER,
  port_given INTEGER NOT NULL DEFAULT 0,
  serve_problem BLOB,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  ended_at INTEGER
) STRICT;

CREATE UNIQUE INDEX meridian_sessions_made ON meridian_sessions (run_id, key);
CREATE UNIQUE INDEX meridian_sessions_live_name ON meridian_sessions (name) WHERE ended_at IS NULL;
CREATE INDEX meridian_sessions_root ON meridian_sessions (root_id);

-- A session's one-time code, which a link to it holds, by the session and
-- its keyed hash (sessionproxy.Keyring.CodeMAC), of the session's ID and
-- the code, which is all that is kept of it, until it is used, it expires
-- (expires_at), or the session is reset or ends.
CREATE TABLE meridian_session_codes (
  session_id TEXT NOT NULL REFERENCES meridian_sessions (id),
  mac BLOB NOT NULL,
  expires_at INTEGER NOT NULL,
  PRIMARY KEY (session_id, mac)
) STRICT, WITHOUT ROWID;

CREATE INDEX meridian_session_codes_expiry ON meridian_session_codes (expires_at);
