BEGIN;

CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE IF NOT EXISTS public.stv3_observation(
    observation_key text PRIMARY KEY,
    provider_id text NOT NULL,
    product_id text NOT NULL,
    target_id text,
    kind text NOT NULL,
    value double precision NOT NULL,
    unit text NOT NULL,
    observed_at timestamptz NOT NULL,
    received_at timestamptz NOT NULL,
    quality text NOT NULL,
    metadata jsonb NOT NULL,
    geometry geometry(Geometry,4326) NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS public.stv3_fusion_cycle(
    cycle_key text PRIMARY KEY,
    evaluated_at timestamptz NOT NULL,
    targets jsonb NOT NULL,
    providers jsonb NOT NULL,
    rejected jsonb NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS public.stv3_storm_cell(
    cell_id text NOT NULL,
    observed_at timestamptz NOT NULL,
    provider_id text NOT NULL,
    precipitation_rate_mm_h double precision,
    reflectivity_dbz double precision,
    radar_level double precision,
    geometry geometry(Geometry,4326) NOT NULL,
    PRIMARY KEY(cell_id,observed_at)
);

CREATE TABLE IF NOT EXISTS public.stv3_track_point(
    cell_id text NOT NULL,
    observed_at timestamptz NOT NULL,
    velocity_east_kmh double precision NOT NULL,
    velocity_north_kmh double precision NOT NULL,
    speed_kmh double precision NOT NULL,
    heading_deg double precision,
    position geometry(Point,4326) NOT NULL,
    PRIMARY KEY(cell_id,observed_at)
);

CREATE TABLE IF NOT EXISTS public.stv3_target_forecast(
    target_id text NOT NULL,
    evaluated_at timestamptz NOT NULL,
    current_state text NOT NULL,
    forecast_state text NOT NULL,
    precipitation_rate_mm_h double precision,
    eta_minutes double precision,
    confidence double precision NOT NULL,
    reason_code text NOT NULL,
    contributors jsonb NOT NULL,
    PRIMARY KEY(target_id,evaluated_at)
);

CREATE INDEX IF NOT EXISTS stv3_observation_provider_time_idx
    ON public.stv3_observation(provider_id, observed_at DESC);
CREATE INDEX IF NOT EXISTS stv3_observation_geometry_gist
    ON public.stv3_observation USING gist(geometry);
CREATE INDEX IF NOT EXISTS stv3_fusion_cycle_evaluated_idx
    ON public.stv3_fusion_cycle(evaluated_at DESC);
CREATE INDEX IF NOT EXISTS stv3_storm_cell_observed_idx
    ON public.stv3_storm_cell(observed_at DESC);
CREATE INDEX IF NOT EXISTS stv3_storm_cell_geometry_gist
    ON public.stv3_storm_cell USING gist(geometry);
CREATE INDEX IF NOT EXISTS stv3_track_point_observed_idx
    ON public.stv3_track_point(observed_at DESC);
CREATE INDEX IF NOT EXISTS stv3_track_point_position_gist
    ON public.stv3_track_point USING gist(position);
CREATE INDEX IF NOT EXISTS stv3_target_forecast_target_time_idx
    ON public.stv3_target_forecast(target_id, evaluated_at DESC);

GRANT USAGE ON SCHEMA public TO storm_ingest;
GRANT SELECT, INSERT, UPDATE ON TABLE
    public.stv3_observation,
    public.stv3_fusion_cycle,
    public.stv3_storm_cell,
    public.stv3_track_point,
    public.stv3_target_forecast
TO storm_ingest;

INSERT INTO stv3_meta.schema_migrations(migration_id, contract)
VALUES('0002_thin_operational_results', 'stv3-thin-operational-results-v1')
ON CONFLICT(migration_id) DO NOTHING;

COMMIT;
