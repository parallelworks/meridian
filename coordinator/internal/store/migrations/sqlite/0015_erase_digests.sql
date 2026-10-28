-- The store keeps nothing made from what can hold a secret that whoever
-- holds the database could test a guess at.
--
-- An attempt's end can hold one, as an output, and complete_digest held a
-- plain SHA-256 of it. The store no longer keeps a digest of an end: these
-- are erased here, and again each time the store opens and each time a
-- run ends (Tx.eraseDigests), since a release before this one, still
-- running on the database, writes them. The column stays for that release,
-- and the index holds only the rows that still have one.
UPDATE meridian_attempts SET complete_digest = NULL WHERE complete_digest IS NOT NULL;
CREATE INDEX meridian_attempts_end_digest ON meridian_attempts (id) WHERE complete_digest IS NOT NULL;

-- An action's request holds its step's with:, and its digest was a plain
-- SHA-256 too. From this release it is an HMAC-SHA256 under a key that the
-- run's data key derives (seal.Sealer.Digest), which keyed marks. A row a
-- release before this one writes leaves keyed 0, and its plain digest is
-- overwritten with zeros here and by each erase after; a request of a row
-- whose digest isn't keyed can't be checked, and is taken for a repeat.
ALTER TABLE meridian_actions ADD COLUMN keyed INTEGER NOT NULL DEFAULT 0;
UPDATE meridian_actions SET digest = x'0000000000000000000000000000000000000000000000000000000000000000'
  WHERE keyed = 0 AND digest <> x'0000000000000000000000000000000000000000000000000000000000000000';
CREATE INDEX meridian_actions_plain_digest ON meridian_actions (attempt_id)
  WHERE keyed = 0 AND digest <> x'0000000000000000000000000000000000000000000000000000000000000000';
