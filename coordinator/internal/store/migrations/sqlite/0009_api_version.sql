-- The version of the worker API an attempt was claimed at, which its worker
-- and every replica that serves it speak until it ends
-- (workerapi.Claim.APIVersion): NULL until it is claimed.
ALTER TABLE meridian_attempts ADD COLUMN api_version INTEGER;
