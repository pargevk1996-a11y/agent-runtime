-- Migration 0002: run projection and checkpoints.
--
-- `runs` is a derived projection of the event log: it indexes run status for
-- queries and is the coordination point for lease/fencing (single-writer). It is
-- always reconstructible from events. `run_snapshots` caches folded RunState for
-- fast recovery. Both are partitioned by the same monthly key as events, so a
-- run, its snapshots, and its events drop together on retention.

CREATE TABLE runs (
    partition_key    date        NOT NULL,
    run_id           uuid        NOT NULL,
    tenant_id        uuid        NOT NULL,
    status           text        NOT NULL,
    last_seq         bigint      NOT NULL,
    lease_owner      text,
    lease_expires_at timestamptz,
    fencing_token    bigint      NOT NULL DEFAULT 0,
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (partition_key, run_id)
) PARTITION BY RANGE (partition_key);

CREATE INDEX runs_tenant_status_idx ON runs (tenant_id, status);

ALTER TABLE runs ENABLE ROW LEVEL SECURITY;

CREATE POLICY runs_tenant_isolation ON runs
    USING (tenant_id = current_setting('app.tenant_id', true)::uuid)
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true)::uuid);

GRANT SELECT, INSERT, UPDATE ON runs TO ar_app;

CREATE TABLE run_snapshots (
    partition_key date        NOT NULL,
    run_id        uuid        NOT NULL,
    tenant_id     uuid        NOT NULL,
    at_seq        bigint      NOT NULL,
    state         jsonb       NOT NULL,
    created_at    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (partition_key, run_id, at_seq)
) PARTITION BY RANGE (partition_key);

ALTER TABLE run_snapshots ENABLE ROW LEVEL SECURITY;

CREATE POLICY run_snapshots_tenant_isolation ON run_snapshots
    USING (tenant_id = current_setting('app.tenant_id', true)::uuid)
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true)::uuid);

GRANT SELECT, INSERT, DELETE ON run_snapshots TO ar_app;

-- Generic monthly-partition helper for any range-partitioned parent. There is
-- deliberately no DEFAULT partition, so an insert for an unprovisioned month
-- fails loudly.
CREATE OR REPLACE FUNCTION ensure_month_partition(parent text, month_start date)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    part_name  text := parent || '_' || to_char(month_start, 'YYYY_MM');
    next_month date := (month_start + interval '1 month')::date;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = part_name) THEN
        EXECUTE format(
            'CREATE TABLE %I PARTITION OF %I FOR VALUES FROM (%L) TO (%L)',
            part_name, parent, month_start, next_month
        );
    END IF;
END;
$$;

DO $$
DECLARE
    base_month date := date_trunc('month', now())::date;
    i          integer;
    m          date;
BEGIN
    FOR i IN 0..12 LOOP
        m := (base_month + (i || ' months')::interval)::date;
        PERFORM ensure_month_partition('runs', m);
        PERFORM ensure_month_partition('run_snapshots', m);
    END LOOP;
END;
$$;
