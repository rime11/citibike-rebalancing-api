-- Mark trips timestamps as America/New_York so they compare correctly with availability_snapshots.
--
-- Trip CSVs from S3 are naive NYC local time. Converting to TIMESTAMPTZ stores them as absolute
-- instants (UTC internally), same as snapshots. With the database timezone set to America/New_York
-- (see snapshots_timestamptz.sql), DATE()/EXTRACT() still return NYC dates/hours.
--
-- This rewrites the trips table. duration_seconds is a generated column that depends on
-- started_at/ended_at, so it has to be dropped and re-added around the type change.
-- Times in the repeated 1-2am hour when DST ends are ambiguous; Postgres picks one reading.

BEGIN;
ALTER TABLE trips DROP COLUMN duration_seconds;

ALTER TABLE trips
    ALTER COLUMN started_at TYPE TIMESTAMPTZ USING started_at AT TIME ZONE 'America/New_York',
    ALTER COLUMN ended_at   TYPE TIMESTAMPTZ USING ended_at   AT TIME ZONE 'America/New_York';

ALTER TABLE trips
    ADD COLUMN duration_seconds INTEGER
    GENERATED ALWAYS AS ( (EXTRACT(EPOCH FROM (ended_at - started_at)))::INTEGER ) STORED;
COMMIT;

-- Sanity check (new session, DB timezone = America/New_York): a trip's started_at should print
-- with a -04/-05 offset and the same wall-clock time as in the source CSV.
-- SELECT ride_id, started_at FROM trips ORDER BY started_at DESC LIMIT 1;
