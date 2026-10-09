BEGIN;

ALTER TABLE public.stv3_track_point
    ADD COLUMN IF NOT EXISTS motion_metadata jsonb
        NOT NULL DEFAULT '{}'::jsonb;

CREATE INDEX IF NOT EXISTS stv3_track_point_motion_status_idx
    ON public.stv3_track_point((motion_metadata->>'motion_status'),observed_at DESC);

INSERT INTO stv3_meta.schema_migrations(migration_id,contract)
VALUES('0007_motion_evidence','stv3-motion-evidence-v1')
ON CONFLICT(migration_id) DO NOTHING;

COMMIT;
