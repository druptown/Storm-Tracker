-- User-authorized rolling calendar month. Apply as existing storm_admin.
-- Reference catalogue/configuration/code artifacts are not weather history.
INSERT INTO public.stv3_retention_state(policy_id)
 VALUES('one_month_weather_history') ON CONFLICT DO NOTHING;
CREATE OR REPLACE FUNCTION public.stv3_enforce_retention()
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER
SET search_path=pg_catalog,public,stv3_ingest,stv3_geo,stv3_storm AS $$
DECLARE
 reference_at timestamptz := clock_timestamp();
 cutoff_at timestamptz := reference_at - interval '1 month';
 claimed_policy text;
BEGIN
 UPDATE public.stv3_retention_state SET last_run_at=reference_at
 WHERE policy_id='one_month_weather_history'
 AND (last_run_at IS NULL OR last_run_at <= reference_at - interval '5 minutes')
 RETURNING policy_id INTO claimed_policy;
 IF claimed_policy IS NULL THEN RETURN jsonb_build_object('status','not_due'); END IF;

 DELETE FROM public.stv3_raw_issue_field_v1 AS link USING public.stv3_raw_cycle_v1 AS cycle
 WHERE link.cycle_id=cycle.cycle_id AND cycle.evaluated_at < cutoff_at;
 DELETE FROM public.stv3_raw_cycle_field_v1 AS link USING public.stv3_raw_cycle_v1 AS cycle
 WHERE link.cycle_id=cycle.cycle_id AND cycle.evaluated_at < cutoff_at;
 DELETE FROM public.stv3_raw_cycle_field_source_v1 AS link USING public.stv3_raw_cycle_v1 AS cycle
 WHERE link.cycle_id=cycle.cycle_id AND cycle.evaluated_at < cutoff_at;
 DELETE FROM public.stv3_raw_cycle_v1 WHERE evaluated_at < cutoff_at;

 -- A fresh binding cannot conceal expired necessary input. Record that loss
 -- before removing its source/field links, rather than leave false completeness.
 UPDATE public.stv3_raw_cycle_v1 AS cycle SET complete=false,
 gaps=CASE WHEN gaps ? 'archive_input_expired' THEN gaps ELSE gaps || '["archive_input_expired"]'::jsonb END
 WHERE EXISTS(SELECT 1 FROM public.stv3_raw_cycle_field_v1 AS link
 JOIN public.stv3_raw_field_v1 AS field ON field.field_id=link.field_id
 WHERE link.cycle_id=cycle.cycle_id AND
 (field.observed_at < cutoff_at OR
 EXISTS(SELECT 1 FROM public.stv3_raw_cycle_field_source_v1 AS source
 JOIN public.stv3_raw_capture_v1 AS capture ON capture.capture_id=source.capture_id
 WHERE source.field_id=field.field_id AND source.cycle_id=cycle.cycle_id
 AND capture.received_at < cutoff_at)));
 DELETE FROM public.stv3_raw_issue_field_v1 AS link USING public.stv3_raw_field_v1 AS field
 WHERE link.field_id=field.field_id AND field.observed_at < cutoff_at;
 DELETE FROM public.stv3_raw_cycle_field_v1 AS link USING public.stv3_raw_field_v1 AS field
 WHERE link.field_id=field.field_id AND field.observed_at < cutoff_at;
 DELETE FROM public.stv3_raw_cycle_field_source_v1 AS link USING public.stv3_raw_field_v1 AS field
 WHERE link.field_id=field.field_id AND field.observed_at < cutoff_at;
 DELETE FROM public.stv3_raw_cycle_field_source_v1 AS link USING public.stv3_raw_capture_v1 AS capture
 WHERE link.capture_id=capture.capture_id AND capture.received_at < cutoff_at;
 DELETE FROM public.stv3_raw_field_source_v1 AS source USING public.stv3_raw_field_v1 AS field
 WHERE source.field_id=field.field_id AND field.observed_at < cutoff_at;
 DELETE FROM public.stv3_raw_field_source_v1 AS source USING public.stv3_raw_capture_v1 AS capture
 WHERE source.capture_id=capture.capture_id AND capture.received_at < cutoff_at;
 DELETE FROM public.stv3_raw_field_v1 WHERE observed_at < cutoff_at;
 DELETE FROM public.stv3_raw_capture_v1 WHERE received_at < cutoff_at;
 DELETE FROM public.stv3_raw_payload_v1 AS payload WHERE
 NOT EXISTS(SELECT 1 FROM public.stv3_raw_capture_v1 AS capture WHERE capture.payload_sha256=payload.sha256)
 AND NOT EXISTS(SELECT 1 FROM public.stv3_raw_field_v1 AS field,
 jsonb_each(field.array_payload_refs) AS array_ref
 WHERE array_ref.value->>'payload_sha256'=payload.sha256);

 DELETE FROM public.stv3_forecast_validation WHERE evaluated_at < cutoff_at;
 DELETE FROM public.stv3_target_passage WHERE evaluated_at < cutoff_at;
 DELETE FROM public.stv3_target_forecast WHERE evaluated_at < cutoff_at;
 DELETE FROM public.stv3_target_forecast_issue WHERE issued_at < cutoff_at;
 DELETE FROM public.stv3_target_position WHERE observed_at < cutoff_at;
 DELETE FROM public.stv3_track_point WHERE observed_at < cutoff_at;
 DELETE FROM public.stv3_storm_cell WHERE observed_at < cutoff_at;
 DELETE FROM public.stv3_fusion_cycle WHERE evaluated_at < cutoff_at;
 DELETE FROM public.stv3_station_measurement WHERE observed_at < cutoff_at;
 DELETE FROM public.stv3_observation WHERE observed_at < cutoff_at;
 DELETE FROM public.stv3_replay_run WHERE coalesce(completed_at,started_at) < cutoff_at;
 DELETE FROM stv3_storm.lightning_events WHERE observed_at < cutoff_at;
 DELETE FROM stv3_ingest.raw_captures AS capture WHERE capture.received_at < cutoff_at
 AND NOT EXISTS(SELECT 1 FROM stv3_storm.lightning_events AS event WHERE event.raw_capture_id=capture.capture_id);
 DELETE FROM stv3_geo.target_observations WHERE observed_at < cutoff_at;
 DELETE FROM public.stv3_region_target WHERE valid_until IS NOT NULL AND valid_until < cutoff_at;
 DELETE FROM public.stv3_region_instance WHERE valid_until IS NOT NULL AND valid_until < cutoff_at;
 UPDATE public.stv3_retention_state SET last_result=jsonb_build_object('status','applied','cutoff_at',cutoff_at)
 WHERE policy_id='one_month_weather_history';
 RETURN jsonb_build_object('status','applied','cutoff_at',cutoff_at);
END $$;
REVOKE ALL ON FUNCTION public.stv3_enforce_retention() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.stv3_enforce_retention() TO storm_ingest;
INSERT INTO stv3_meta.schema_migrations(migration_id,contract)
 VALUES('0010_one_month_weather_retention','stv3-one-calendar-month-v1') ON CONFLICT DO NOTHING;
