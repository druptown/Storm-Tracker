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
