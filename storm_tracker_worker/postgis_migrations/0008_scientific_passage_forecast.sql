BEGIN;

CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE IF NOT EXISTS public.stv3_target_forecast_issue(
    forecast_issue_id text PRIMARY KEY,
    target_id text NOT NULL,
    issued_at timestamptz NOT NULL,
    algorithm_version text NOT NULL,
    policy_version text NOT NULL,
    region_revision_id text,
    input_fingerprints jsonb NOT NULL DEFAULT '[]'::jsonb,
    forecast_metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS public.stv3_target_passage_issue(
    passage_id text PRIMARY KEY,
    forecast_issue_id text NOT NULL
        REFERENCES public.stv3_target_forecast_issue(forecast_issue_id)
        ON DELETE CASCADE,
    target_id text NOT NULL,
    passage_system_id text NOT NULL,
    interval_ordinal integer NOT NULL CHECK(interval_ordinal >= 0),
    status text NOT NULL,
    entry_at timestamptz,
    exit_at timestamptz,
    entry_bracket_lower_at timestamptz,
    entry_bracket_upper_at timestamptz,
    exit_bracket_lower_at timestamptz,
    exit_bracket_upper_at timestamptz,
    left_censored boolean NOT NULL DEFAULT false,
    right_censored boolean NOT NULL DEFAULT false,
    coverage_censored boolean NOT NULL DEFAULT false,
    predicted_mean_intensity_mm_h double precision,
    predicted_peak_intensity_mm_h double precision,
    quantity text,
    unit text,
    footprint geometry(Geometry,4326) NOT NULL,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE(forecast_issue_id,passage_system_id,interval_ordinal)
);

CREATE INDEX IF NOT EXISTS stv3_target_forecast_issue_target_time_idx
    ON public.stv3_target_forecast_issue(target_id,issued_at DESC);
CREATE INDEX IF NOT EXISTS stv3_target_passage_issue_target_entry_idx
    ON public.stv3_target_passage_issue(target_id,entry_at DESC);
CREATE INDEX IF NOT EXISTS stv3_target_passage_issue_forecast_idx
    ON public.stv3_target_passage_issue(forecast_issue_id);
CREATE INDEX IF NOT EXISTS stv3_target_passage_issue_footprint_gist
    ON public.stv3_target_passage_issue USING gist(footprint);

GRANT SELECT, INSERT ON TABLE
    public.stv3_target_forecast_issue,
    public.stv3_target_passage_issue
TO storm_ingest;

INSERT INTO stv3_meta.schema_migrations(migration_id,contract)
VALUES('0008_scientific_passage_forecast',
       'stv3-scientific-passage-forecast-v1')
ON CONFLICT(migration_id) DO NOTHING;

COMMIT;
