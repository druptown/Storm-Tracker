BEGIN;

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE OR REPLACE FUNCTION public.stv3_observation_source_key(
    source_provider_id text,
    source_product_id text,
    source_kind text,
    source_observed_at timestamptz,
    source_geometry geometry
)
RETURNS text
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
STRICT
AS $$
    SELECT encode(
        digest(
            convert_to(
                source_provider_id || chr(31) ||
                source_product_id || chr(31) ||
                source_kind,
                'UTF8'
            ) ||
            float8send(extract(epoch FROM source_observed_at)::double precision) ||
            ST_AsEWKB(source_geometry),
            'sha256'
        ),
        'hex'
    )
$$;

-- Existing observations predate regional provenance and may contain one copy per
-- target. Keep one canonical source fact, but do not invent historical region links.
CREATE TEMPORARY TABLE stv3_observation_rekey ON COMMIT DROP AS
SELECT
    public.stv3_observation_source_key(
        provider_id,product_id,kind,observed_at,geometry
    ) AS canonical_key,
    provider_id,
    product_id,
    kind,
    value,
    unit,
    observed_at,
    received_at,
    quality,
    metadata,
    geometry,
    updated_at,
    row_number() OVER (
        PARTITION BY public.stv3_observation_source_key(
            provider_id,product_id,kind,observed_at,geometry
        )
        ORDER BY received_at DESC,updated_at DESC,observation_key
    ) AS canonical_rank
FROM public.stv3_observation;

TRUNCATE TABLE public.stv3_observation;

INSERT INTO public.stv3_observation(
    observation_key,provider_id,product_id,target_id,kind,value,unit,
    observed_at,received_at,quality,metadata,geometry,updated_at
)
SELECT
    canonical_key,provider_id,product_id,NULL,kind,value,unit,
    observed_at,received_at,quality,metadata,geometry,updated_at
FROM stv3_observation_rekey
WHERE canonical_rank=1;

CREATE TABLE IF NOT EXISTS public.stv3_region_instance(
    region_instance_id text PRIMARY KEY,
    region_id text NOT NULL,
    kind text NOT NULL,
    activated_at timestamptz NOT NULL,
    last_seen_at timestamptz NOT NULL,
    valid_until timestamptz,
    active boolean NOT NULL
);

CREATE INDEX IF NOT EXISTS stv3_region_instance_logical_time_idx
    ON public.stv3_region_instance(region_id,activated_at DESC);
CREATE UNIQUE INDEX IF NOT EXISTS stv3_region_instance_active_idx
    ON public.stv3_region_instance(region_id)
    WHERE valid_until IS NULL;

CREATE TABLE IF NOT EXISTS public.stv3_region_revision(
    region_revision_id text PRIMARY KEY,
    region_instance_id text NOT NULL
        REFERENCES public.stv3_region_instance(region_instance_id)
        ON DELETE CASCADE,
    center geometry(Point,4326) NOT NULL,
    observation_radius_km double precision NOT NULL,
    radar_analysis_radius_km double precision,
    valid_from timestamptz NOT NULL,
    last_seen_at timestamptz NOT NULL,
    valid_until timestamptz,
    active boolean NOT NULL
);

CREATE INDEX IF NOT EXISTS stv3_region_revision_instance_time_idx
    ON public.stv3_region_revision(region_instance_id,valid_from DESC);
CREATE UNIQUE INDEX IF NOT EXISTS stv3_region_revision_active_idx
    ON public.stv3_region_revision(region_instance_id)
    WHERE valid_until IS NULL;
CREATE INDEX IF NOT EXISTS stv3_region_revision_center_gist
    ON public.stv3_region_revision USING gist(center);

CREATE TABLE IF NOT EXISTS public.stv3_region_target(
    region_target_id bigserial PRIMARY KEY,
    target_id text NOT NULL,
    region_revision_id text NOT NULL
        REFERENCES public.stv3_region_revision(region_revision_id)
        ON DELETE CASCADE,
    position geometry(Point,4326) NOT NULL,
    location_accuracy_m double precision,
    position_observed_at timestamptz,
    valid_from timestamptz NOT NULL,
    valid_until timestamptz,
    active boolean NOT NULL DEFAULT true,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE UNIQUE INDEX IF NOT EXISTS stv3_region_target_active_target_idx
    ON public.stv3_region_target(target_id)
    WHERE valid_until IS NULL;
CREATE INDEX IF NOT EXISTS stv3_region_target_revision_time_idx
    ON public.stv3_region_target(region_revision_id,valid_from DESC);
CREATE INDEX IF NOT EXISTS stv3_region_target_position_gist
    ON public.stv3_region_target USING gist(position);

CREATE TABLE IF NOT EXISTS public.stv3_observation_region(
    observation_key text NOT NULL
        REFERENCES public.stv3_observation(observation_key)
        ON DELETE CASCADE,
    region_revision_id text NOT NULL
        REFERENCES public.stv3_region_revision(region_revision_id)
        ON DELETE CASCADE,
    first_seen_at timestamptz NOT NULL,
    last_seen_at timestamptz NOT NULL,
    PRIMARY KEY(observation_key,region_revision_id)
);

CREATE INDEX IF NOT EXISTS stv3_observation_region_revision_time_idx
    ON public.stv3_observation_region(region_revision_id,last_seen_at DESC);

CREATE TABLE IF NOT EXISTS public.stv3_station_measurement_region(
    provider_id text NOT NULL,
    station_id text NOT NULL,
    observed_at timestamptz NOT NULL,
    measurement_type text NOT NULL,
    region_revision_id text NOT NULL
        REFERENCES public.stv3_region_revision(region_revision_id)
        ON DELETE CASCADE,
    first_seen_at timestamptz NOT NULL,
    last_seen_at timestamptz NOT NULL,
    PRIMARY KEY(
        provider_id,station_id,observed_at,measurement_type,region_revision_id
    ),
    FOREIGN KEY(provider_id,station_id,observed_at,measurement_type)
        REFERENCES public.stv3_station_measurement(
            provider_id,station_id,observed_at,measurement_type
        ) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS stv3_station_measurement_region_revision_time_idx
    ON public.stv3_station_measurement_region(
        region_revision_id,observed_at DESC
    );

ALTER TABLE public.stv3_storm_cell
    ADD COLUMN IF NOT EXISTS region_revision_id text
        REFERENCES public.stv3_region_revision(region_revision_id)
        ON DELETE SET NULL;
ALTER TABLE public.stv3_track_point
    ADD COLUMN IF NOT EXISTS region_revision_id text
        REFERENCES public.stv3_region_revision(region_revision_id)
        ON DELETE SET NULL;
ALTER TABLE public.stv3_target_forecast
    ADD COLUMN IF NOT EXISTS region_revision_id text
        REFERENCES public.stv3_region_revision(region_revision_id)
        ON DELETE SET NULL;
ALTER TABLE public.stv3_target_passage
    ADD COLUMN IF NOT EXISTS region_revision_id text
        REFERENCES public.stv3_region_revision(region_revision_id)
        ON DELETE SET NULL;
ALTER TABLE public.stv3_forecast_validation
    ADD COLUMN IF NOT EXISTS region_revision_id text
        REFERENCES public.stv3_region_revision(region_revision_id)
        ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS stv3_storm_cell_region_time_idx
    ON public.stv3_storm_cell(region_revision_id,observed_at DESC);
CREATE INDEX IF NOT EXISTS stv3_track_point_region_time_idx
    ON public.stv3_track_point(region_revision_id,observed_at DESC);
CREATE INDEX IF NOT EXISTS stv3_target_forecast_region_time_idx
    ON public.stv3_target_forecast(region_revision_id,evaluated_at DESC);
CREATE INDEX IF NOT EXISTS stv3_target_passage_region_time_idx
    ON public.stv3_target_passage(region_revision_id,evaluated_at DESC);
CREATE INDEX IF NOT EXISTS stv3_forecast_validation_region_time_idx
    ON public.stv3_forecast_validation(region_revision_id,evaluated_at DESC);

CREATE TABLE IF NOT EXISTS public.stv3_retention_state(
    policy_id text PRIMARY KEY,
    last_run_at timestamptz,
    last_result jsonb NOT NULL DEFAULT '{}'::jsonb
);

INSERT INTO public.stv3_retention_state(policy_id)
VALUES('six_month_operational_history')
ON CONFLICT(policy_id) DO NOTHING;

CREATE OR REPLACE FUNCTION public.stv3_enforce_retention()
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog,public,stv3_ingest,stv3_geo,stv3_storm
AS $$
DECLARE
    reference_at timestamptz := clock_timestamp();
    cutoff_at timestamptz := reference_at - interval '6 months';
    claimed_policy text;
    deleted_count bigint;
    result jsonb := '{}'::jsonb;
BEGIN
    UPDATE public.stv3_retention_state
    SET last_run_at=reference_at
    WHERE policy_id='six_month_operational_history'
      AND (
          last_run_at IS NULL OR
          last_run_at <= reference_at - interval '1 day'
      )
    RETURNING policy_id INTO claimed_policy;

    IF claimed_policy IS NULL THEN
        RETURN jsonb_build_object('status','not_due');
    END IF;

    DELETE FROM public.stv3_forecast_validation WHERE evaluated_at < cutoff_at;
    DELETE FROM public.stv3_target_passage WHERE evaluated_at < cutoff_at;
    DELETE FROM public.stv3_target_forecast WHERE evaluated_at < cutoff_at;
    DELETE FROM public.stv3_target_position WHERE observed_at < cutoff_at;
    DELETE FROM public.stv3_track_point WHERE observed_at < cutoff_at;
    DELETE FROM public.stv3_storm_cell WHERE observed_at < cutoff_at;
    DELETE FROM public.stv3_fusion_cycle WHERE evaluated_at < cutoff_at;
    DELETE FROM public.stv3_station_measurement WHERE observed_at < cutoff_at;
    DELETE FROM public.stv3_observation WHERE observed_at < cutoff_at;
    DELETE FROM public.stv3_replay_run
    WHERE coalesce(completed_at,started_at) < cutoff_at;

    DELETE FROM stv3_storm.lightning_events WHERE observed_at < cutoff_at;
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    result := result || jsonb_build_object('lightning_events',deleted_count);

    DELETE FROM stv3_ingest.raw_captures AS capture
    WHERE capture.received_at < cutoff_at
      AND NOT EXISTS(
          SELECT 1 FROM stv3_storm.lightning_events AS event
          WHERE event.raw_capture_id=capture.capture_id
      );
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    result := result || jsonb_build_object('raw_captures',deleted_count);

    DELETE FROM stv3_geo.target_observations WHERE observed_at < cutoff_at;
    DELETE FROM public.stv3_region_target
    WHERE valid_until IS NOT NULL AND valid_until < cutoff_at;
    DELETE FROM public.stv3_region_instance
    WHERE valid_until IS NOT NULL AND valid_until < cutoff_at;

    result := result || jsonb_build_object(
        'status','applied',
        'cutoff_at',cutoff_at
    );
    UPDATE public.stv3_retention_state
    SET last_result=result
    WHERE policy_id='six_month_operational_history';
    RETURN result;
END
$$;

REVOKE ALL ON FUNCTION public.stv3_enforce_retention() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.stv3_enforce_retention() TO storm_ingest;
GRANT EXECUTE ON FUNCTION public.stv3_observation_source_key(
    text,text,text,timestamptz,geometry
) TO storm_ingest;
GRANT SELECT,INSERT,UPDATE ON TABLE public.stv3_observation_region TO storm_ingest;
GRANT SELECT,INSERT,UPDATE ON TABLE
    public.stv3_region_instance,
    public.stv3_region_revision,
    public.stv3_region_target,
    public.stv3_station_measurement_region
TO storm_ingest;
GRANT USAGE,SELECT ON SEQUENCE public.stv3_region_target_region_target_id_seq
TO storm_ingest;
GRANT SELECT ON TABLE public.stv3_retention_state TO storm_ingest;

INSERT INTO stv3_meta.schema_migrations(migration_id,contract)
VALUES(
    '0006_canonical_observation_retention',
    'stv3-canonical-observation-retention-v2'
)
ON CONFLICT(migration_id) DO NOTHING;

COMMIT;
