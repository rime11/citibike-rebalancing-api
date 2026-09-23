CREATE TABLE "stations" (
  "station_id" varchar(50) PRIMARY KEY,
  "name" varchar(200) NOT NULL,
  "short_name" varchar(20),
  "latitude" decimal(9,6) NOT NULL,
  "longitude" decimal(9,6) NOT NULL,
  "capacity" integer
);

CREATE TABLE "availability_snapshots" (
  "snapshot_id" SERIAL PRIMARY KEY,
  "Station_id" varchar(50) NOT NULL,
  "captured_at" timestamp NOT NULL,
  "last_reported" timestamp,
  "num_bikes_available" integer NOT NULL,
  "num_ebikes_available" integer,
  "num_docks_available" integer NOT NULL,
  "num_bikes_disabled" integer,
  "num_docks_disabled" integer,
  "is_installed" boolean,
  "is_renting" boolean,
  "is_returning" boolean
);

CREATE TABLE "trips" (
  "ride_id" varchar(50) PRIMARY KEY,
  "rideable_type" varchar(20),
  "started_at" timestamp NOT NULL,
  "ended_at" timestamp NOT NULL,
  "start_station_id" varchar(50),
  "end_station_id" varchar(50),
  "start_station_name" varchar(200),
  "end_station_name" varchar(200),
  "start_lat" decimal(9,6),
  "start_lng" decimal(9,6),
  "end_lat" decimal(9,6),
  "end_lng" decimal(9,6),
  "member_casual" varchar(10),
  "duration_seconds" integer
);

CREATE TABLE "daily_station_metrics" (
  "metric_id" SERIAL PRIMARY KEY,
  "station_id" varchar(50) NOT NULL,
  "summary_date" date NOT NULL,
  "trips_started" integer DEFAULT 0,
  "trips_ended" integer DEFAULT 0,
  "net_flow" integer,
  "avg_bikes_available" decimal(5,2),
  "min_bikes_available" integer,
  "max_bikes_available" integer,
  "pct_time_empty" decimal(5,2),
  "pct_time_full" decimal(5,2)
);

CREATE TABLE "hourly_patterns" (
  "pattern_id" SERIAL PRIMARY KEY,
  "station_id" varchar(50) NOT NULL,
  "day_of_week" integer,
  "hour_of_day" integer,
  "avg_trips_started" decimal(6,2),
  "avg_trips_ended" decimal(6,2),
  "avg_net_flow" decimal(6,2),
  "avg_bikes_available" decimal(5,2)
);

CREATE TABLE "station_pairs" (
  "pair_id" SERIAL PRIMARY KEY,
  "start_station_id" varchar(50) NOT NULL,
  "end_station_id" varchar(50) NOT NULL,
  "trip_count" integer NOT NULL,
  "avg_duration_seconds" integer,
  "member_trips" integer DEFAULT 0,
  "casual_trips" integer DEFAULT 0,
  "last_computed" timestamp
);

CREATE TABLE "rebalancing_flags" (
  "flag_id" SERIAL PRIMARY KEY,
  "station_id" varchar(50) NOT NULL,
  "flag_type" varchar(30) NOT NULL,
  "severity" varchar(10),
  "detected_at" timestamp,
  "resolved_at" timestamp,
  "notes" text
);

CREATE TABLE "station_status_changes" (
  "change_id" SERIAL PRIMARY KEY,
  "station_id" varchar(50) NOT NULL,
  "change_type" varchar(30) NOT NULL,
  "detected_at" timestamp NOT NULL,
  "value_before" integer,
  "value_after" integer,
  "snapshot_id" bigint
);

CREATE UNIQUE INDEX ON "daily_station_metrics" ("station_id", "summary_date");

CREATE UNIQUE INDEX ON "hourly_patterns" ("station_id", "day_of_week", "hour_of_day");

CREATE UNIQUE INDEX ON "station_pairs" ("start_station_id", "end_station_id");

CREATE INDEX "idx_changes_station_time" ON "station_status_changes" ("station_id", "detected_at");

CREATE INDEX "idx_changes_type" ON "station_status_changes" ("change_type", "detected_at");

COMMENT ON COLUMN "station_status_changes"."change_type" IS 'became_empty | became_full | recovered_from_empty | recovered_from_full | went_offline | came_online';

COMMENT ON COLUMN "station_status_changes"."value_before" IS 'bike or dock count; NULL for offline/online events';

ALTER TABLE "availability_snapshots" ADD FOREIGN KEY ("Station_id") REFERENCES "stations" ("station_id") DEFERRABLE INITIALLY IMMEDIATE;

ALTER TABLE "daily_station_metrics" ADD FOREIGN KEY ("station_id") REFERENCES "stations" ("station_id") DEFERRABLE INITIALLY IMMEDIATE;

ALTER TABLE "hourly_patterns" ADD FOREIGN KEY ("station_id") REFERENCES "stations" ("station_id") DEFERRABLE INITIALLY IMMEDIATE;

ALTER TABLE "station_pairs" ADD FOREIGN KEY ("start_station_id") REFERENCES "stations" ("station_id") DEFERRABLE INITIALLY IMMEDIATE;

ALTER TABLE "station_pairs" ADD FOREIGN KEY ("end_station_id") REFERENCES "stations" ("station_id") DEFERRABLE INITIALLY IMMEDIATE;

ALTER TABLE "rebalancing_flags" ADD FOREIGN KEY ("station_id") REFERENCES "stations" ("station_id") DEFERRABLE INITIALLY IMMEDIATE;

ALTER TABLE "station_status_changes" ADD FOREIGN KEY ("station_id") REFERENCES "stations" ("station_id") DEFERRABLE INITIALLY IMMEDIATE;

ALTER TABLE "station_status_changes" ADD FOREIGN KEY ("snapshot_id") REFERENCES "availability_snapshots" ("snapshot_id") DEFERRABLE INITIALLY IMMEDIATE;
