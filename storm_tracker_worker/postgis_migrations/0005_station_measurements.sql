BEGIN;

CREATE OR REPLACE FUNCTION public.stv3_quality_rank(value text)
RETURNS integer
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
    SELECT CASE lower(value)
        WHEN 'good' THEN 3
        WHEN 'suspect' THEN 2
        WHEN 'poor' THEN 1
        ELSE 0
    END
$$;

CREATE TABLE IF NOT EXISTS public.stv3_station_measurement(
    provider_id text NOT NULL,
    station_id text NOT NULL,
    observed_at timestamptz NOT NULL,
    measurement_type text NOT NULL,
    product_id text NOT NULL,
    value double precision NOT NULL,
    unit text NOT NULL,
    received_at timestamptz NOT NULL,
    quality text NOT NULL,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(provider_id,station_id,observed_at,measurement_type),
    FOREIGN KEY(provider_id,station_id)
        REFERENCES public.stv3_ground_station(provider_id,station_id)
        ON DELETE RESTRICT
) PARTITION BY RANGE(observed_at);

DO $partitions$
DECLARE
    month_offset integer;
    start_at timestamptz;
    end_at timestamptz;
    partition_name text;
BEGIN
    FOR month_offset IN -6..1 LOOP
        start_at := date_trunc('month', CURRENT_TIMESTAMP)
            + make_interval(months => month_offset);
        end_at := start_at + interval '1 month';
        partition_name := format(
            'stv3_station_measurement_%s',
            to_char(start_at, 'YYYY_MM')
        );
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS public.%I '
            'PARTITION OF public.stv3_station_measurement '
            'FOR VALUES FROM (%L) TO (%L)',
            partition_name,
            start_at,
            end_at
        );
    END LOOP;
END
$partitions$;

CREATE INDEX IF NOT EXISTS stv3_station_measurement_station_time_idx
    ON public.stv3_station_measurement(
        provider_id,station_id,observed_at DESC
    );
CREATE INDEX IF NOT EXISTS stv3_station_measurement_type_time_idx
    ON public.stv3_station_measurement(
        measurement_type,observed_at DESC
    );

CREATE TABLE IF NOT EXISTS public.stv3_station_measurement_backfill_state(
    migration_id text PRIMARY KEY,
    source_cursor text,
    copied_count bigint NOT NULL DEFAULT 0 CHECK(copied_count >= 0),
    rejected_count bigint NOT NULL DEFAULT 0 CHECK(rejected_count >= 0),
    status text NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

INSERT INTO public.stv3_station_measurement_backfill_state(
    migration_id,source_cursor,copied_count,rejected_count,status
) VALUES(
    'legacy_observation_v1',NULL,0,0,'pending'
)
ON CONFLICT(migration_id) DO NOTHING;

GRANT SELECT, INSERT, UPDATE ON TABLE
    public.stv3_station_measurement,
    public.stv3_station_measurement_backfill_state
TO storm_ingest;
GRANT EXECUTE ON FUNCTION public.stv3_quality_rank(text) TO storm_ingest;

INSERT INTO stv3_meta.schema_migrations(migration_id,contract)
VALUES(
    '0005_station_measurements',
    'stv3-station-measurements-v1'
)
ON CONFLICT(migration_id) DO NOTHING;

COMMIT;
