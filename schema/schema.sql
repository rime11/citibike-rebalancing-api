--
-- PostgreSQL database dump
--

\restrict uDTwPaAxeBH7aH3pyNZsm4f7bKeYlNRMIrpvHp74QCwNb5vdrvutMtSTXDMALZa

-- Dumped from database version 16.15 (Ubuntu 16.15-0ubuntu0.24.04.1)
-- Dumped by pg_dump version 16.15 (Ubuntu 16.15-0ubuntu0.24.04.1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: availability_snapshots; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.availability_snapshots (
    snapshot_id integer NOT NULL,
    station_id character varying(50) NOT NULL,
    captured_at timestamp with time zone NOT NULL,
    last_reported timestamp with time zone,
    num_bikes_available integer NOT NULL,
    num_ebikes_available integer,
    num_docks_available integer NOT NULL,
    num_bikes_disabled integer,
    num_docks_disabled integer,
    is_installed boolean,
    is_renting boolean,
    is_returning boolean
);


--
-- Name: availability_snapshots_snapshot_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.availability_snapshots_snapshot_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: availability_snapshots_snapshot_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.availability_snapshots_snapshot_id_seq OWNED BY public.availability_snapshots.snapshot_id;


--
-- Name: daily_station_metrics; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.daily_station_metrics (
    metric_id integer NOT NULL,
    station_id character varying(50) NOT NULL,
    summary_date date NOT NULL,
    trips_started integer,
    trips_ended integer,
    net_flow integer GENERATED ALWAYS AS ((trips_ended - trips_started)) STORED,
    avg_bikes_available numeric(5,2),
    min_bikes_available integer,
    max_bikes_available integer,
    pct_time_empty numeric(5,2),
    pct_time_full numeric(5,2),
    computed_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT daily_station_metrics_pct_time_empty_check CHECK (((pct_time_empty >= (0)::numeric) AND (pct_time_empty <= (100)::numeric))),
    CONSTRAINT daily_station_metrics_pct_time_full_check CHECK (((pct_time_full >= (0)::numeric) AND (pct_time_full <= (100)::numeric)))
);


--
-- Name: daily_station_metrics_metric_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.daily_station_metrics_metric_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: daily_station_metrics_metric_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.daily_station_metrics_metric_id_seq OWNED BY public.daily_station_metrics.metric_id;


--
-- Name: hourly_patterns; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.hourly_patterns (
    pattern_id integer NOT NULL,
    station_id character varying(50) NOT NULL,
    day_of_week integer NOT NULL,
    hour_of_day integer NOT NULL,
    is_weekday boolean GENERATED ALWAYS AS (((day_of_week >= 1) AND (day_of_week <= 5))) STORED,
    avg_trips_started numeric(6,2),
    avg_trips_ended numeric(6,2),
    avg_net_flow numeric(6,2),
    avg_bikes_available numeric(5,2),
    num_days_sampled integer NOT NULL,
    computed_at timestamp without time zone DEFAULT LOCALTIMESTAMP NOT NULL,
    CONSTRAINT hourly_patterns_day_of_week_check CHECK (((day_of_week >= 0) AND (day_of_week <= 6))),
    CONSTRAINT hourly_patterns_hour_of_day_check CHECK (((hour_of_day >= 0) AND (hour_of_day <= 23)))
);


--
-- Name: hourly_patterns_pattern_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.hourly_patterns_pattern_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: hourly_patterns_pattern_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.hourly_patterns_pattern_id_seq OWNED BY public.hourly_patterns.pattern_id;


--
-- Name: rebalancing_flags; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.rebalancing_flags (
    flag_id integer NOT NULL,
    station_id character varying(50) NOT NULL,
    flag_type character varying(30) NOT NULL,
    severity character varying(10) NOT NULL,
    metric_value numeric(7,2) NOT NULL,
    median_outage_minutes numeric(7,1),
    outage_count integer,
    window_label character varying(40) NOT NULL,
    computed_at timestamp without time zone DEFAULT LOCALTIMESTAMP NOT NULL,
    CONSTRAINT rebalancing_flags_flag_type_check CHECK (((flag_type)::text = ANY ((ARRAY['chronic_empty'::character varying, 'chronic_full'::character varying, 'imbalance_toward_empty'::character varying, 'imbalance_toward_full'::character varying])::text[]))),
    CONSTRAINT rebalancing_flags_severity_check CHECK (((severity)::text = ANY ((ARRAY['low'::character varying, 'medium'::character varying, 'high'::character varying])::text[]))),
    CONSTRAINT rebalancing_flags_window_label_check CHECK (((window_label)::text = ANY ((ARRAY['weekday_am_rush'::character varying, 'weekday_pm_rush'::character varying])::text[])))
);


--
-- Name: rebalancing_flags_flag_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.rebalancing_flags_flag_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: rebalancing_flags_flag_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.rebalancing_flags_flag_id_seq OWNED BY public.rebalancing_flags.flag_id;


--
-- Name: station_status_changes; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.station_status_changes (
    change_id integer NOT NULL,
    station_id character varying(50) NOT NULL,
    change_type character varying(30) NOT NULL,
    detected_at timestamp without time zone NOT NULL,
    value_before integer,
    value_after integer,
    snapshot_id integer,
    CONSTRAINT valid_change_type CHECK (((change_type)::text = ANY ((ARRAY['became_empty'::character varying, 'became_full'::character varying, 'recovered_from_empty'::character varying, 'recovered_from_full'::character varying, 'went_offline'::character varying, 'came_online'::character varying])::text[])))
);


--
-- Name: station_outages; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.station_outages AS
 WITH events AS (
         SELECT station_status_changes.station_id,
            station_status_changes.detected_at,
                CASE
                    WHEN ((station_status_changes.change_type)::text = ANY ((ARRAY['became_empty'::character varying, 'recovered_from_empty'::character varying])::text[])) THEN 'empty'::text
                    WHEN ((station_status_changes.change_type)::text = ANY ((ARRAY['became_full'::character varying, 'recovered_from_full'::character varying])::text[])) THEN 'full'::text
                    ELSE 'offline'::text
                END AS outage_type,
            ((station_status_changes.change_type)::text = ANY ((ARRAY['became_empty'::character varying, 'became_full'::character varying, 'went_offline'::character varying])::text[])) AS is_start
           FROM public.station_status_changes
        ), next_event AS (
         SELECT events.station_id,
            events.outage_type,
            events.is_start,
            events.detected_at,
            lead(events.is_start) OVER w AS next_is_start,
            lead(events.detected_at) OVER w AS next_start
           FROM events
          WINDOW w AS (PARTITION BY events.station_id, events.outage_type ORDER BY events.detected_at)
        ), episodes AS (
         SELECT next_event.station_id,
            next_event.outage_type,
            next_event.detected_at AS started_at,
            next_event.next_start AS ended_at,
            (next_event.next_is_start IS NULL) AS is_ongoing
           FROM next_event
          WHERE (next_event.is_start AND (next_event.next_is_start IS NOT TRUE))
        )
 SELECT station_id,
    outage_type,
    started_at,
    ended_at,
    is_ongoing,
    round((EXTRACT(epoch FROM (COALESCE(ended_at, LOCALTIMESTAMP) - started_at)) / 60.0), 1) AS minutes_out,
    ((outage_type <> 'offline'::text) AND (EXISTS ( SELECT 1
           FROM episodes o
          WHERE (((o.station_id)::text = (e.station_id)::text) AND (o.outage_type = 'offline'::text) AND (o.started_at < COALESCE(e.ended_at, LOCALTIMESTAMP)) AND (COALESCE(o.ended_at, LOCALTIMESTAMP) > e.started_at))))) AS overlaps_offline
   FROM episodes e;


--
-- Name: VIEW station_outages; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON VIEW public.station_outages IS 'One row per empty/full/offline episode, paired from station_status_changes. Ongoing episodes have ended_at NULL and minutes_out measured to now. overlaps_offline marks empty/full episodes contaminated by station downtime; exclude them from rebalancing metrics.';


--
-- Name: station_status_changes_change_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.station_status_changes_change_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: station_status_changes_change_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.station_status_changes_change_id_seq OWNED BY public.station_status_changes.change_id;


--
-- Name: stations; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.stations (
    station_id character varying(50) NOT NULL,
    station_name character varying(200) NOT NULL,
    short_name character varying(20),
    latitude numeric(9,6) NOT NULL,
    longitude numeric(9,6) NOT NULL,
    capacity integer,
    is_active boolean DEFAULT true,
    last_updated timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: technician_alerts; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.technician_alerts AS
 WITH latest AS (
         SELECT DISTINCT ON (availability_snapshots.station_id) availability_snapshots.station_id,
            availability_snapshots.captured_at,
            availability_snapshots.num_bikes_available AS bikes,
            availability_snapshots.num_docks_available AS docks,
            availability_snapshots.is_renting
           FROM public.availability_snapshots
          WHERE (availability_snapshots.captured_at > (( SELECT max(availability_snapshots_1.captured_at) AS max
                   FROM public.availability_snapshots availability_snapshots_1) - '00:30:00'::interval))
          ORDER BY availability_snapshots.station_id, availability_snapshots.captured_at DESC
        ), classified AS (
         SELECT l.station_id,
            l.captured_at,
            l.bikes,
            l.docks,
            l.is_renting,
            s.station_name AS name,
            s.short_name,
            s.capacity,
                CASE
                    WHEN (l.is_renting IS NOT TRUE) THEN NULL::text
                    WHEN (l.bikes = 0) THEN 'empty'::text
                    WHEN (l.docks = 0) THEN 'full'::text
                    WHEN ((l.bikes)::numeric <= GREATEST((2)::numeric, (0.10 * (s.capacity)::numeric))) THEN 'near_empty'::text
                    WHEN ((l.docks)::numeric <= GREATEST((2)::numeric, (0.10 * (s.capacity)::numeric))) THEN 'near_full'::text
                    ELSE NULL::text
                END AS status
           FROM (latest l
             JOIN public.stations s ON (((s.station_id)::text = (l.station_id)::text)))
        ), reputation AS (
         SELECT rebalancing_flags.station_id,
            max(rebalancing_flags.metric_value) FILTER (WHERE ((rebalancing_flags.flag_type)::text = 'chronic_empty'::text)) AS chronic_empty_pct,
            max(rebalancing_flags.metric_value) FILTER (WHERE ((rebalancing_flags.flag_type)::text = 'chronic_full'::text)) AS chronic_full_pct
           FROM public.rebalancing_flags
          GROUP BY rebalancing_flags.station_id
        )
 SELECT c.station_id,
    c.name,
    c.short_name,
    c.capacity,
    c.status,
    c.bikes,
    c.docks,
    c.captured_at AS as_of,
        CASE
            WHEN ((ev.change_type)::text = ('became_'::text || c.status)) THEN round((EXTRACT(epoch FROM (c.captured_at - (ev.detected_at)::timestamp with time zone)) / 60.0))
            ELSE NULL::numeric
        END AS minutes_in_status,
    r.chronic_empty_pct,
    r.chronic_full_pct
   FROM ((classified c
     LEFT JOIN LATERAL ( SELECT ch.change_type,
            ch.detected_at
           FROM public.station_status_changes ch
          WHERE (((ch.station_id)::text = (c.station_id)::text) AND ((ch.change_type)::text = ANY ((ARRAY['became_empty'::character varying, 'recovered_from_empty'::character varying, 'became_full'::character varying, 'recovered_from_full'::character varying])::text[])))
          ORDER BY ch.detected_at DESC
         LIMIT 1) ev ON ((c.status = ANY (ARRAY['empty'::text, 'full'::text]))))
     LEFT JOIN reputation r ON (((r.station_id)::text = (c.station_id)::text)))
  WHERE (c.status IS NOT NULL);


--
-- Name: trips; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.trips (
    ride_id character varying(50) NOT NULL,
    rideable_type character varying(20),
    started_at timestamp with time zone NOT NULL,
    ended_at timestamp with time zone NOT NULL,
    start_station_id character varying(50),
    end_station_id character varying(50),
    start_station_name character varying(200),
    end_station_name character varying(200),
    start_lat numeric(9,6),
    start_lng numeric(9,6),
    end_lat numeric(9,6),
    end_lng numeric(9,6),
    member_casual character varying(10),
    trip_id bigint NOT NULL,
    duration_seconds integer GENERATED ALWAYS AS ((EXTRACT(epoch FROM (ended_at - started_at)))::integer) STORED
);


--
-- Name: trips_trip_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.trips_trip_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: trips_trip_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.trips_trip_id_seq OWNED BY public.trips.trip_id;


--
-- Name: availability_snapshots snapshot_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.availability_snapshots ALTER COLUMN snapshot_id SET DEFAULT nextval('public.availability_snapshots_snapshot_id_seq'::regclass);


--
-- Name: daily_station_metrics metric_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.daily_station_metrics ALTER COLUMN metric_id SET DEFAULT nextval('public.daily_station_metrics_metric_id_seq'::regclass);


--
-- Name: hourly_patterns pattern_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.hourly_patterns ALTER COLUMN pattern_id SET DEFAULT nextval('public.hourly_patterns_pattern_id_seq'::regclass);


--
-- Name: rebalancing_flags flag_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.rebalancing_flags ALTER COLUMN flag_id SET DEFAULT nextval('public.rebalancing_flags_flag_id_seq'::regclass);


--
-- Name: station_status_changes change_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.station_status_changes ALTER COLUMN change_id SET DEFAULT nextval('public.station_status_changes_change_id_seq'::regclass);


--
-- Name: trips trip_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.trips ALTER COLUMN trip_id SET DEFAULT nextval('public.trips_trip_id_seq'::regclass);


--
-- Name: availability_snapshots availability_snapshots_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.availability_snapshots
    ADD CONSTRAINT availability_snapshots_pkey PRIMARY KEY (snapshot_id);


--
-- Name: daily_station_metrics daily_station_metrics_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.daily_station_metrics
    ADD CONSTRAINT daily_station_metrics_pkey PRIMARY KEY (metric_id);


--
-- Name: hourly_patterns hourly_patterns_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.hourly_patterns
    ADD CONSTRAINT hourly_patterns_pkey PRIMARY KEY (pattern_id);


--
-- Name: rebalancing_flags rebalancing_flags_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.rebalancing_flags
    ADD CONSTRAINT rebalancing_flags_pkey PRIMARY KEY (flag_id);


--
-- Name: station_status_changes station_status_changes_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.station_status_changes
    ADD CONSTRAINT station_status_changes_pkey PRIMARY KEY (change_id);


--
-- Name: stations stations_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stations
    ADD CONSTRAINT stations_pkey PRIMARY KEY (station_id);


--
-- Name: stations stations_short_name_unique; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stations
    ADD CONSTRAINT stations_short_name_unique UNIQUE (short_name);


--
-- Name: trips trips_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.trips
    ADD CONSTRAINT trips_pkey PRIMARY KEY (trip_id);


--
-- Name: station_status_changes uq_changes_event; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.station_status_changes
    ADD CONSTRAINT uq_changes_event UNIQUE (station_id, change_type, detected_at);


--
-- Name: daily_station_metrics uq_daily_station_date; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.daily_station_metrics
    ADD CONSTRAINT uq_daily_station_date UNIQUE (station_id, summary_date);


--
-- Name: rebalancing_flags uq_flags_station_type_window; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.rebalancing_flags
    ADD CONSTRAINT uq_flags_station_type_window UNIQUE (station_id, flag_type, window_label);


--
-- Name: hourly_patterns uq_hourly_station_dow_hour; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.hourly_patterns
    ADD CONSTRAINT uq_hourly_station_dow_hour UNIQUE (station_id, day_of_week, hour_of_day);


--
-- Name: trips uq_trips_ride_id; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.trips
    ADD CONSTRAINT uq_trips_ride_id UNIQUE (ride_id);


--
-- Name: idx_changes_station_time; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_changes_station_time ON public.station_status_changes USING btree (station_id, detected_at);


--
-- Name: idx_changes_type; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_changes_type ON public.station_status_changes USING btree (change_type, detected_at);


--
-- Name: idx_snapshots_captured; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_snapshots_captured ON public.availability_snapshots USING btree (captured_at);


--
-- Name: idx_snapshots_station_time; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_snapshots_station_time ON public.availability_snapshots USING btree (station_id, captured_at);


--
-- Name: daily_station_metrics daily_station_metrics_station_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.daily_station_metrics
    ADD CONSTRAINT daily_station_metrics_station_id_fkey FOREIGN KEY (station_id) REFERENCES public.stations(station_id) DEFERRABLE;


--
-- Name: hourly_patterns hourly_patterns_station_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.hourly_patterns
    ADD CONSTRAINT hourly_patterns_station_id_fkey FOREIGN KEY (station_id) REFERENCES public.stations(station_id) DEFERRABLE;


--
-- Name: rebalancing_flags rebalancing_flags_station_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.rebalancing_flags
    ADD CONSTRAINT rebalancing_flags_station_id_fkey FOREIGN KEY (station_id) REFERENCES public.stations(station_id) DEFERRABLE;


--
-- Name: station_status_changes station_status_changes_snapshot_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.station_status_changes
    ADD CONSTRAINT station_status_changes_snapshot_id_fkey FOREIGN KEY (snapshot_id) REFERENCES public.availability_snapshots(snapshot_id) DEFERRABLE;


--
-- Name: station_status_changes station_status_changes_station_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.station_status_changes
    ADD CONSTRAINT station_status_changes_station_id_fkey FOREIGN KEY (station_id) REFERENCES public.stations(station_id) DEFERRABLE;


--
-- Name: trips trips_end_station_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.trips
    ADD CONSTRAINT trips_end_station_fkey FOREIGN KEY (end_station_id) REFERENCES public.stations(short_name);


--
-- Name: trips trips_start_station_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.trips
    ADD CONSTRAINT trips_start_station_fkey FOREIGN KEY (start_station_id) REFERENCES public.stations(short_name);


--
-- PostgreSQL database dump complete
--

\unrestrict uDTwPaAxeBH7aH3pyNZsm4f7bKeYlNRMIrpvHp74QCwNb5vdrvutMtSTXDMALZa

