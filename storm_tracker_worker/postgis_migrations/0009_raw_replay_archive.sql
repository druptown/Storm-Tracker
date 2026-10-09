CREATE TABLE IF NOT EXISTS public.stv3_raw_payload_v1(
 sha256 text PRIMARY KEY CHECK(length(sha256)=64), payload bytea NOT NULL,
 byte_count bigint NOT NULL CHECK(byte_count=octet_length(payload)),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp());
CREATE TABLE IF NOT EXISTS public.stv3_raw_capture_v1(
 capture_id text PRIMARY KEY, payload_sha256 text REFERENCES public.stv3_raw_payload_v1,
 provider_id text NOT NULL,product_id text NOT NULL,requested_at timestamptz NOT NULL,
 received_at timestamptz NOT NULL,product_valid_at timestamptz,
 safe_context jsonb NOT NULL,disposition text NOT NULL,reason_code text);
CREATE INDEX IF NOT EXISTS stv3_raw_capture_v1_received ON public.stv3_raw_capture_v1(received_at);
CREATE TABLE IF NOT EXISTS public.stv3_raw_field_v1(
 field_id text PRIMARY KEY,metadata jsonb NOT NULL,array_payload_refs jsonb NOT NULL,
 observed_at timestamptz NOT NULL);
CREATE TABLE IF NOT EXISTS public.stv3_raw_field_source_v1(
 field_id text REFERENCES public.stv3_raw_field_v1,capture_id text REFERENCES public.stv3_raw_capture_v1,
 PRIMARY KEY(field_id,capture_id));
CREATE TABLE IF NOT EXISTS public.stv3_raw_cycle_v1(
 cycle_id text PRIMARY KEY,evaluated_at timestamptz NOT NULL,code_revision text NOT NULL,
 policy jsonb NOT NULL,complete boolean NOT NULL,gaps jsonb NOT NULL);
CREATE INDEX IF NOT EXISTS stv3_raw_cycle_v1_time ON public.stv3_raw_cycle_v1(evaluated_at);
CREATE TABLE IF NOT EXISTS public.stv3_raw_cycle_field_v1(
 cycle_id text REFERENCES public.stv3_raw_cycle_v1,region_revision_id text NOT NULL,
 field_id text REFERENCES public.stv3_raw_field_v1,role text NOT NULL,
 PRIMARY KEY(cycle_id,region_revision_id,field_id,role));
CREATE TABLE IF NOT EXISTS public.stv3_raw_issue_field_v1(
 forecast_issue_id text NOT NULL,cycle_id text REFERENCES public.stv3_raw_cycle_v1,
 region_revision_id text NOT NULL,field_id text REFERENCES public.stv3_raw_field_v1,
 PRIMARY KEY(forecast_issue_id,cycle_id,region_revision_id,field_id));
CREATE TABLE IF NOT EXISTS public.stv3_raw_cycle_field_source_v1(
 cycle_id text REFERENCES public.stv3_raw_cycle_v1,
 field_id text REFERENCES public.stv3_raw_field_v1,
 capture_id text REFERENCES public.stv3_raw_capture_v1,
 PRIMARY KEY(cycle_id,field_id,capture_id));
CREATE TABLE IF NOT EXISTS public.stv3_raw_code_revision_v1(
 code_revision text PRIMARY KEY,source_tree_digest text NOT NULL,worker_archive_sha256 text NOT NULL,
 integration_archive_sha256 text NOT NULL,safe_artifact_reference jsonb NOT NULL);
GRANT SELECT,INSERT ON TABLE public.stv3_raw_payload_v1,public.stv3_raw_capture_v1,
 public.stv3_raw_field_v1,public.stv3_raw_field_source_v1,public.stv3_raw_cycle_v1,
 public.stv3_raw_cycle_field_v1,public.stv3_raw_issue_field_v1,public.stv3_raw_code_revision_v1,
 public.stv3_raw_cycle_field_source_v1 TO storm_ingest;
INSERT INTO stv3_meta.schema_migrations(migration_id,contract)
 VALUES('0009_raw_replay_archive','stv3-raw-replay-archive-v1') ON CONFLICT DO NOTHING;
