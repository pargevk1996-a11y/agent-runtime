-- Migration 0001: event store.
--
-- The event log is the runtime's single source of truth. This creates the
-- append-only `events` table, range-partitioned by month, with Row-Level
-- Security enforcing hard multi-tenant isolation.
--
-- Preconditions (established by bootstrap / infra, not this file): the roles
-- ar_admin (schema owner, runs this migration, bypasses RLS) and ar_app
-- (runtime role, subject to RLS) already exist.

-- Partition key is the month of the run's creation, derived application-side
-- from the UUIDv7 run_id. It is identical for every event of a run, so a run's
-- events always live in a single partition and (run_id, seq) is unique in
-- practice. Retention = dropping whole monthly partitions.
CREATE TABLE events (
    partition_key   date        NOT NULL,
    run_id          uuid        NOT NULL,
    seq             bigint      NOT NULL,
    event_id        uuid        NOT NULL,
    tenant_id       uuid        NOT NULL,
    event_type      text        NOT NULL,
    payload_version integer     NOT NULL,
    payload         jsonb       NOT NULL,
    occurred_at     timestamptz NOT NULL,
    recorded_at     timestamptz NOT NULL DEFAULT now(),
    causation_id    uuid,
    correlation_id  uuid,
    CONSTRAINT events_seq_positive CHECK (seq >= 1),
    CONSTRAINT events_payload_version_positive CHECK (payload_version >= 1),
    -- Partition column must be part of every unique constraint on a partitioned
    -- table; (run_id, seq) uniqueness holds because a run stays in one partition.
    PRIMARY KEY (partition_key, run_id, seq)
) PARTITION BY RANGE (partition_key);

-- Read path is always (partition_key known from run_id, run_id, seq) which the
-- primary key already serves; this index supports tenant-scoped scans.
CREATE INDEX events_tenant_idx ON events (tenant_id, run_id, seq);

-- Row-Level Security: a row is visible/insertable only under a matching tenant
-- context. The two-arg current_setting returns NULL when unset, so a missing
-- tenant GUC denies all rows rather than raising.
ALTER TABLE events ENABLE ROW LEVEL SECURITY;

CREATE POLICY events_tenant_isolation ON events
    USING (tenant_id = current_setting('app.tenant_id', true)::uuid)
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true)::uuid);

GRANT SELECT, INSERT ON events TO ar_app;

-- Creates a monthly partition if absent. Called ahead of time by ops/maintenance
-- (and by the pre-creation loop below); there is deliberately no DEFAULT
-- partition, so an insert for an unprovisioned month fails loudly.
CREATE OR REPLACE FUNCTION ensure_events_partition(month_start date)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    part_name  text := 'events_' || to_char(month_start, 'YYYY_MM');
    next_month date := (month_start + interval '1 month')::date;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = part_name) THEN
        EXECUTE format(
            'CREATE TABLE %I PARTITION OF events FOR VALUES FROM (%L) TO (%L)',
            part_name, month_start, next_month
        );
    END IF;
END;
$$;

-- Pre-create the current month plus a year ahead so normal operation never hits
-- a missing partition.
DO $$
DECLARE
    base_month date := date_trunc('month', now())::date;
    i          integer;
BEGIN
    FOR i IN 0..12 LOOP
        PERFORM ensure_events_partition((base_month + (i || ' months')::interval)::date);
    END LOOP;
END;
$$;
