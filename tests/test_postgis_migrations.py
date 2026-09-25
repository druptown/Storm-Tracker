from pathlib import Path


def test_regional_radar_migration_is_compact_and_registered():
    sql = Path(
        "storm_tracker_worker/postgis_migrations/0004_regional_radar_fusion.sql"
    ).read_text(encoding="utf-8")
    normalized = " ".join(sql.split()).lower()

    assert "add column if not exists regional_radar jsonb" in normalized
    assert "0004_regional_radar_fusion" in sql
    assert "raster" not in normalized
    assert "bytea" not in normalized


def test_station_measurement_migration_is_target_independent_and_partitioned():
    sql = Path(
        "storm_tracker_worker/postgis_migrations/0005_station_measurements.sql"
    ).read_text(encoding="utf-8")
    normalized = " ".join(sql.lower().split())

    assert "partition by range(observed_at)" in normalized
    assert (
        "primary key(provider_id,station_id,observed_at,measurement_type)"
        in normalized
    )
    assert (
        "references public.stv3_ground_station(provider_id,station_id)"
        in normalized
    )
    station_table = normalized.split(
        "create table if not exists public.stv3_station_measurement(",
        1,
    )[1].split("create table", 1)[0]
    assert "target_id" not in station_table


def test_station_measurement_migration_has_bounded_partitions_and_backfill_state():
    sql = Path(
        "storm_tracker_worker/postgis_migrations/0005_station_measurements.sql"
    ).read_text(encoding="utf-8")
    normalized = " ".join(sql.lower().split())

    assert "for month_offset in -6..1 loop" in normalized
    assert "stv3_station_measurement_backfill_state" in normalized
    assert "source_cursor text" in normalized
    assert "copied_count bigint" in normalized
    assert "rejected_count bigint" in normalized
    assert "0005_station_measurements" in sql


def test_canonical_observation_migration_links_regions_and_bounds_history():
    sql = Path(
        "storm_tracker_worker/postgis_migrations/0006_canonical_observation_retention.sql"
    ).read_text(encoding="utf-8")
    normalized = " ".join(sql.lower().split())

    assert "create table if not exists public.stv3_region_instance" in normalized
    assert "create table if not exists public.stv3_region_revision" in normalized
    assert "region_revision_id text not null" in normalized
    assert "create table if not exists public.stv3_region_target" in normalized
    assert "region_target_id bigserial primary key" in normalized
    assert "valid_from timestamptz not null" in normalized
    assert "valid_until timestamptz" in normalized
    assert "where valid_until is null" in normalized
    assert "create table if not exists public.stv3_observation_region" in normalized
    assert "primary key(observation_key,region_revision_id)" in normalized
    assert (
        "create table if not exists public.stv3_station_measurement_region"
        in normalized
    )
    assert "stv3_observation_target" not in normalized
    assert "create or replace function public.stv3_observation_source_key" in normalized
    identity_function = normalized.split(
        "create or replace function public.stv3_observation_source_key", 1
    )[1].split("create", 1)[0]
    assert "target_id" not in identity_function
    assert "interval '6 months'" in normalized
    assert "delete from public.stv3_observation" in normalized
    assert "delete from public.stv3_station_measurement" in normalized
    assert "delete from public.stv3_ground_station" not in normalized
    assert "delete from public.stv3_station_capability" not in normalized
    assert "delete from public.stv3_station_discovery_tile" not in normalized
    assert "delete from public.stv3_region_instance" in normalized
    assert "0006_canonical_observation_retention" in sql
