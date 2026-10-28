-- When its backend's check of the attempt's worker first gave a problem,
-- a cause that won't pass by itself, since the backend last decided, or
-- the attempt's lease was last renewed, by the store's clock
-- (Tx.CheckProblem): the coordinator loses the attempt once that is long
-- enough ago (coordinator.Timing.CheckGrace). NULL otherwise.
ALTER TABLE meridian_attempts ADD COLUMN problem_since INTEGER;
