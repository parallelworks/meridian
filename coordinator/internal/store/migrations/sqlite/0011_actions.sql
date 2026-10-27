-- The answers the coordinator gave the built-in actions that attempts'
-- steps asked it for (workerapi.PathAction), by the attempt, and the
-- step's index and try: the action is performed once, and a repeat of
-- its request is given the same answer. digest is the SHA-256 of the
-- whole request, as the worker API encodes it: its step, try, with: and
-- outlivesCancel. It is kept, never the request itself, whose with: can
-- hold a secret, and a request for the same try with another digest is
-- refused. answer is the answer, as the worker API encodes it
-- (workerapi.ActionAnswer). A row outlives its attempt, as an attempt's
-- events do: the store runs without foreign keys enforced, so nothing
-- cascades, and whatever deletes attempts deletes their rows here too.
CREATE TABLE meridian_actions (
  attempt_id TEXT NOT NULL REFERENCES meridian_attempts (id),
  step INTEGER NOT NULL,
  try INTEGER NOT NULL,
  digest BLOB NOT NULL,
  answer BLOB NOT NULL,
  stored_at INTEGER NOT NULL,
  PRIMARY KEY (attempt_id, step, try)
) STRICT, WITHOUT ROWID;
