-- A session's port_given is gone: update-session's localPort, which it
-- said the step gave, is ignored, as format 1 ignores it, so that every
-- session is served at a port the system picks.
ALTER TABLE meridian_sessions DROP COLUMN port_given;
