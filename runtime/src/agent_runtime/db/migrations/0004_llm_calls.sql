-- Migration 0004: LLM cost ledger.
--
-- An append-only record of every LLM call — tokens, latency, dollar cost —
-- attributed to tenant, run, and (optionally) node. Kept out of the event log:
-- the call's *result* lives in the node output, while dollar cost is derived
-- accounting best aggregated by SQL over this table. Partitioned by the same
-- monthly key as events so it drops together on retention.

CREATE TABLE llm_calls (
    partition_key date           NOT NULL,
    id            uuid           NOT NULL,
    tenant_id     uuid           NOT NULL,
    run_id        uuid           NOT NULL,
    node_id       uuid,
    provider      text           NOT NULL,
    model         text           NOT NULL,
    input_tokens  bigint         NOT NULL,
    output_tokens bigint         NOT NULL,
    cost_usd      numeric(20, 10) NOT NULL,
    latency_ms    integer        NOT NULL,
    created_at    timestamptz    NOT NULL DEFAULT now(),
    PRIMARY KEY (partition_key, id)
) PARTITION BY RANGE (partition_key);

CREATE INDEX llm_calls_run_idx ON llm_calls (run_id, node_id);
CREATE INDEX llm_calls_tenant_idx ON llm_calls (tenant_id);

ALTER TABLE llm_calls ENABLE ROW LEVEL SECURITY;

CREATE POLICY llm_calls_tenant_isolation ON llm_calls
    USING (tenant_id = current_setting('app.tenant_id', true)::uuid)
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true)::uuid);

GRANT SELECT, INSERT ON llm_calls TO ar_app;

DO $$
DECLARE
    base_month date := date_trunc('month', now())::date;
    i          integer;
BEGIN
    FOR i IN 0..12 LOOP
        PERFORM ensure_month_partition('llm_calls', (base_month + (i || ' months')::interval)::date);
    END LOOP;
END;
$$;
