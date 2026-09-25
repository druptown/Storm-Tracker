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
