BEGIN;

CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_topology;

CREATE SCHEMA IF NOT EXISTS stv3_meta;
CREATE SCHEMA IF NOT EXISTS stv3_ingest;
CREATE SCHEMA IF NOT EXISTS stv3_geo;
CREATE SCHEMA IF NOT EXISTS stv3_storm;
CREATE SCHEMA IF NOT EXISTS stv3_calibration;

CREATE TABLE IF NOT EXISTS stv3_meta.schema_migrations(
    migration_id text PRIMARY KEY,
    contract text NOT NULL,
    applied_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS stv3_ingest.raw_captures(
    capture_id varchar(96) PRIMARY KEY,
    source_id varchar(128) NOT NULL,
    provider varchar(128) NOT NULL,
    product varchar(256) NOT NULL,
    capture_receipt_sha256 char(64) NOT NULL
        CHECK(capture_receipt_sha256 ~ '^[0-9a-f]{64}$'),
    manifest_sha256 char(64) NOT NULL
        CHECK(manifest_sha256 ~ '^[0-9a-f]{64}$'),
    product_timestamp timestamptz,
    received_at timestamptz NOT NULL,
    raw_store_created_at timestamptz NOT NULL,
    worker_indexed_at timestamptz NOT NULL,
    member_count smallint NOT NULL CHECK(member_count > 0),
    coverage geometry(MultiPolygon,4326),
    projector_contract text NOT NULL,
    projector_version integer NOT NULL CHECK(projector_version > 0),
    projected_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

COMMENT ON TABLE stv3_ingest.raw_captures IS
    'Payload-free projection of exact raw-artifact provenance. Raw TIFF/HDF/GRIB bytes remain in the worker raw artifact store.';

CREATE INDEX IF NOT EXISTS raw_captures_source_received_idx
    ON stv3_ingest.raw_captures(source_id,received_at DESC);
CREATE INDEX IF NOT EXISTS raw_captures_provider_received_idx
    ON stv3_ingest.raw_captures(provider,received_at DESC);
CREATE INDEX IF NOT EXISTS raw_captures_received_brin
    ON stv3_ingest.raw_captures USING brin(received_at);
CREATE INDEX IF NOT EXISTS raw_captures_coverage_gist
    ON stv3_ingest.raw_captures USING gist(coverage);

CREATE TABLE IF NOT EXISTS stv3_geo.target_observations(
    observation_sha256 char(64) PRIMARY KEY
        CHECK(observation_sha256 ~ '^[0-9a-f]{64}$'),
    target_id varchar(128) NOT NULL,
    source varchar(32) NOT NULL
        CHECK(source IN ('life360','synthetic','operator')),
    observed_at timestamptz NOT NULL,
    received_at timestamptz NOT NULL,
    position geometry(Point,4326) NOT NULL,
    accuracy_m real CHECK(accuracy_m IS NULL OR accuracy_m >= 0),
    desired_state_sha256 char(64)
        CHECK(desired_state_sha256 IS NULL OR
              desired_state_sha256 ~ '^[0-9a-f]{64}$'),
    inserted_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX IF NOT EXISTS target_observations_target_time_idx
    ON stv3_geo.target_observations(target_id,observed_at DESC);
CREATE INDEX IF NOT EXISTS target_observations_time_brin
    ON stv3_geo.target_observations USING brin(observed_at);
CREATE INDEX IF NOT EXISTS target_observations_position_gist
    ON stv3_geo.target_observations USING gist(position);

CREATE TABLE IF NOT EXISTS stv3_storm.lightning_events(
    event_sha256 char(64) PRIMARY KEY
        CHECK(event_sha256 ~ '^[0-9a-f]{64}$'),
    provider varchar(64) NOT NULL,
    provider_event_id varchar(256),
    observed_at timestamptz NOT NULL,
    received_at timestamptz NOT NULL,
    position geometry(Point,4326) NOT NULL,
    amplitude_ka real,
    polarity smallint CHECK(polarity IS NULL OR polarity IN (-1,0,1)),
    raw_capture_id varchar(96) REFERENCES
        stv3_ingest.raw_captures(capture_id) ON DELETE RESTRICT,
    quality jsonb NOT NULL DEFAULT '{}'::jsonb
        CHECK(jsonb_typeof(quality)='object'),
    inserted_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

COMMENT ON TABLE stv3_storm.lightning_events IS
    'High-cadence point-event store. Ingest remains disabled until the provider authority and retention terms are explicitly admitted.';

CREATE UNIQUE INDEX IF NOT EXISTS lightning_provider_event_unique
    ON stv3_storm.lightning_events(provider,provider_event_id)
    WHERE provider_event_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS lightning_events_time_brin
    ON stv3_storm.lightning_events USING brin(observed_at);
CREATE INDEX IF NOT EXISTS lightning_events_position_gist
    ON stv3_storm.lightning_events USING gist(position);

INSERT INTO stv3_meta.schema_migrations(migration_id,contract)
VALUES('0001_storm_tracker_spatial_foundation',
       'stv3-postgis-spatial-foundation-v1')
ON CONFLICT(migration_id) DO NOTHING;

COMMIT;
