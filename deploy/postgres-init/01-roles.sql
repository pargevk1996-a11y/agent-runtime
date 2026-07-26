-- Runs once on first Postgres init (as the ar_admin superuser).
-- Creates the runtime app role; schema privileges are granted by migrations.
-- ar_admin (POSTGRES_USER) owns the schema and bypasses RLS; ar_app is subject
-- to RLS and is what the API/worker connect as at runtime.

CREATE ROLE ar_app LOGIN PASSWORD 'devpass';
