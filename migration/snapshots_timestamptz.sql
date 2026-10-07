-- Mark availability_snapshots timestamps as UTC and make NYC the database's local time.
--
-- Snapshots are stored as naive UTC; trips are naive America/New_York.
-- After this, DATE()/EXTRACT() on captured_at return NYC dates/hours and line up with trips.
--
-- The ALTERs are metadata-only (no table rewrite) on PG12+ because the session
-- timezone is UTC and there is no USING clause, so existing values are kept as-is
-- and read as UTC. Order matters: run the ALTERs BEFORE changing the DB timezone.

BEGIN;
SET LOCAL timezone = 'UTC';
ALTER TABLE availability_snapshots
    ALTER COLUMN captured_at   TYPE TIMESTAMPTZ,
    ALTER COLUMN last_reported TYPE TIMESTAMPTZ;
COMMIT;

-- Applies to new connections (reconnect psql / restart the API afterwards).
ALTER DATABASE citibike SET timezone = 'America/New_York';

-- Sanity check (new session): latest snapshot should show a -05 offset in winter
-- SELECT captured_at FROM availability_snapshots ORDER BY captured_at DESC LIMIT 1;
