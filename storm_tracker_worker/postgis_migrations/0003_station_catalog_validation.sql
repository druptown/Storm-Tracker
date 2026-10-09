BEGIN;

CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE IF NOT EXISTS public.stv3_ground_station(
    provider_id text NOT NULL,
    station_id text NOT NULL,
    position geometry(Point,4326) NOT NULL,
    discovered_at timestamptz NOT NULL,
    last_seen_at timestamptz NOT NULL,
    quality_score double precision NOT NULL DEFAULT 1.0
        CHECK(quality_score BETWEEN 0.0 AND 1.0),
    operational boolean NOT NULL DEFAULT true,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(provider_id,station_id)
);

CREATE TABLE IF NOT EXISTS public.stv3_station_capability(
    provider_id text NOT NULL,
    station_id text NOT NULL,
    capability text NOT NULL,
    PRIMARY KEY(provider_id,station_id,capability),
    FOREIGN KEY(provider_id,station_id)
        REFERENCES public.stv3_ground_station(provider_id,station_id)
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS public.stv3_station_discovery_tile(
    provider_id text NOT NULL,
    tile_id text NOT NULL,
    cursor text,
    completed boolean NOT NULL,
    station_count bigint NOT NULL DEFAULT 0 CHECK(station_count >= 0),
    updated_at timestamptz NOT NULL,
    PRIMARY KEY(provider_id,tile_id)
);

CREATE TABLE IF NOT EXISTS public.stv3_target_passage(
    target_id text NOT NULL,
    evaluated_at timestamptz NOT NULL,
    cell_id text NOT NULL,
    provider_id text NOT NULL,
    entry_at timestamptz NOT NULL,
    exit_at timestamptz NOT NULL,
    method text NOT NULL,
    predicted_intensity_mm_h double precision,
    footprint geometry(Geometry,4326) NOT NULL,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY(target_id,evaluated_at,cell_id)
);

CREATE TABLE IF NOT EXISTS public.stv3_target_position(
    target_id text NOT NULL,
    observed_at timestamptz NOT NULL,
    position geometry(Point,4326) NOT NULL,
    accuracy_km double precision NOT NULL DEFAULT 0.0
        CHECK(accuracy_km >= 0.0),
    PRIMARY KEY(target_id,observed_at)
);

CREATE TABLE IF NOT EXISTS public.stv3_forecast_validation(
    validation_key text PRIMARY KEY,
    target_id text NOT NULL,
    evaluated_at timestamptz NOT NULL,
    cell_id text,
    provider_id text NOT NULL,
    station_provider_id text NOT NULL,
    station_id text NOT NULL,
    outcome text NOT NULL,
    entry_error_seconds double precision,
    exit_error_seconds double precision,
    predicted_intensity_mm_h double precision,
    observed_intensity_mm_h double precision,
    details jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY(station_provider_id,station_id)
        REFERENCES public.stv3_ground_station(provider_id,station_id)
        ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS public.stv3_replay_run(
    replay_id text PRIMARY KEY,
    started_at timestamptz NOT NULL,
    completed_at timestamptz,
    algorithm_version text NOT NULL,
    parameters jsonb NOT NULL,
    status text NOT NULL
);

CREATE TABLE IF NOT EXISTS public.stv3_replay_metric(
    replay_id text NOT NULL REFERENCES public.stv3_replay_run(replay_id)
        ON DELETE CASCADE,
    metric_name text NOT NULL,
    dimension jsonb NOT NULL DEFAULT '{}'::jsonb,
    metric_value double precision NOT NULL,
    sample_count bigint NOT NULL CHECK(sample_count >= 0),
    PRIMARY KEY(replay_id,metric_name,dimension)
);

CREATE INDEX IF NOT EXISTS stv3_ground_station_position_gist
    ON public.stv3_ground_station USING gist(position);
CREATE INDEX IF NOT EXISTS stv3_ground_station_provider_seen_idx
    ON public.stv3_ground_station(provider_id,last_seen_at DESC);
CREATE INDEX IF NOT EXISTS stv3_station_capability_lookup_idx
    ON public.stv3_station_capability(capability,provider_id,station_id);
CREATE INDEX IF NOT EXISTS stv3_station_discovery_updated_idx
    ON public.stv3_station_discovery_tile(provider_id,updated_at DESC);
CREATE INDEX IF NOT EXISTS stv3_target_passage_time_idx
    ON public.stv3_target_passage(target_id,entry_at,exit_at);
CREATE INDEX IF NOT EXISTS stv3_target_passage_footprint_gist
    ON public.stv3_target_passage USING gist(footprint);
CREATE INDEX IF NOT EXISTS stv3_target_position_position_gist
    ON public.stv3_target_position USING gist(position);
CREATE INDEX IF NOT EXISTS stv3_forecast_validation_time_idx
    ON public.stv3_forecast_validation(target_id,evaluated_at DESC);

GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE
    public.stv3_ground_station,
    public.stv3_station_capability,
    public.stv3_station_discovery_tile,
    public.stv3_target_position,
    public.stv3_target_passage,
    public.stv3_forecast_validation,
    public.stv3_replay_run,
    public.stv3_replay_metric
TO storm_ingest;

INSERT INTO stv3_meta.schema_migrations(migration_id,contract)
VALUES('0003_station_catalog_validation',
       'stv3-thin-station-catalog-validation-v1')
ON CONFLICT(migration_id) DO NOTHING;

COMMIT;
