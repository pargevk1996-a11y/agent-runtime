-- Migration 0003: cooperative cancellation flag on runs.
--
-- The control plane requests cancellation by setting this flag (a plain UPDATE
-- under RLS, no lease required). The scheduler — the lease holder — observes it
-- each loop and drives the run to a cancelled terminal state.

ALTER TABLE runs ADD COLUMN cancel_requested boolean NOT NULL DEFAULT false;
