-- The key of the member an attempt was placed on, where its backend's
-- workers reach the coordinator through a listener on the member, as the
-- ssh backend's do (backend.Forwarder.MemberKey): a keyed hash, never a
-- host in the clear. The worker API holds the attempt's requests to the
-- listener of that member, and a replica that takes over finds by it the
-- members whose listeners it holds again. NULL for any other attempt.
ALTER TABLE meridian_attempts ADD COLUMN member_key TEXT;
-- The run's directory on the member, where its driver chose it
-- (backend.Placement.RunDir), which the attempt's claim gives in place of
-- the run's. NULL for the run's own, and for a private placement, whose
-- place holds it sealed.
ALTER TABLE meridian_attempts ADD COLUMN run_dir TEXT;
CREATE INDEX meridian_attempts_member_key ON meridian_attempts (member_key) WHERE member_key IS NOT NULL AND state <> 'ended';
