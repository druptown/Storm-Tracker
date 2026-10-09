BEGIN;

ALTER TABLE public.stv3_fusion_cycle
    ADD COLUMN IF NOT EXISTS regional_radar jsonb NOT NULL DEFAULT '[]'::jsonb;

INSERT INTO stv3_meta.schema_migrations(migration_id, contract)
VALUES(
    '0004_regional_radar_fusion',
    'stv3-regional-radar-fusion-summary-v1'
)
ON CONFLICT(migration_id) DO NOTHING;

COMMIT;
