#!/usr/bin/with-contenv bashio
set -e

export STV3_MQTT_HOST
export STV3_MQTT_PORT
export STV3_MQTT_USERNAME
export STV3_MQTT_PASSWORD
export STV3_MQTT_TLS
export STV3_QUEUE_SIZE
export STV3_COMPUTE_PROFILE
export STV3_CPU_THREADS
export STV3_WORKER_NICE
export STV3_CPU_LIMIT_PERCENT
export STV3_HEARTBEAT_SECONDS
export STV3_PROVIDER_CYCLE_SECONDS
export STV3_LOG_LEVEL
export STV3_NETATMO_CLIENT_ID
export STV3_NETATMO_CLIENT_SECRET
export STV3_NETATMO_REFRESH_TOKEN
export STV3_NETATMO_TOKEN_STATE
export STV3_AWN_APPLICATION_KEY
export STV3_AWN_API_KEY
export STV3_AWN_RAW_RETENTION_AUTHORITY
export STV3_SYNOPTIC_TOKEN
export STV3_SYNOPTIC_RAW_RETENTION_AUTHORITY
export STV3_RAINVIEWER_CACHE_ATTRIBUTION_AUTHORITY
export STV3_KMI_APPV4_RAW_RETENTION_AUTHORITY
export STV3_OPERA_RAW_RETENTION_AUTHORITY
export STV3_BLITZORTUNG_RAW_RETENTION_AUTHORITY
export STV3_KNMI_API_KEY
export STV3_AEMET_API_KEY
export STV3_METEOFRANCE_API_TOKEN
export STV3_METEOFRANCE_APPLICATION_ID
export STV3_HSAF_USERNAME
export STV3_HSAF_PASSWORD
export STV3_EUMETSAT_CONSUMER_KEY
export STV3_EUMETSAT_CONSUMER_SECRET
export STV3_JOB_RECEIPT_DB
export STV3_WORKER_ONLY_ROOT

if [[ -n "${STV3_MQTT_HOST:-}" ]]; then
  # Standalone/NAS mode.  Compose or the operator owns every value; secrets
  # are never copied to arguments or logs.
  STV3_RUNNING_UNDER_HA="false"
  STV3_MQTT_PORT="${STV3_MQTT_PORT:-1883}"
  STV3_MQTT_USERNAME="${STV3_MQTT_USERNAME:-}"
  STV3_MQTT_PASSWORD="${STV3_MQTT_PASSWORD:-}"
  STV3_MQTT_TLS="${STV3_MQTT_TLS:-false}"
  STV3_QUEUE_SIZE="${STV3_QUEUE_SIZE:-16}"
  STV3_COMPUTE_PROFILE="${STV3_COMPUTE_PROFILE:-dedicated_nas}"
  STV3_WORKER_NICE="${STV3_WORKER_NICE:-15}"
  STV3_CPU_LIMIT_PERCENT="${STV3_CPU_LIMIT_PERCENT:-40}"
  STV3_HEARTBEAT_SECONDS="${STV3_HEARTBEAT_SECONDS:-10}"
  STV3_PROVIDER_CYCLE_SECONDS="${STV3_PROVIDER_CYCLE_SECONDS:-60}"
  STV3_LOG_LEVEL="${STV3_LOG_LEVEL:-INFO}"
  STV3_RUNTIME_MODE="${STV3_RUNTIME_MODE-legacy}"
  STV3_WORKER_FIRST_CONFIG_PATH="${STV3_WORKER_FIRST_CONFIG_PATH-}"
  STV3_WORKER_FIRST_CONFIG_SHA256="${STV3_WORKER_FIRST_CONFIG_SHA256-}"
  STV3_WORKER_FIRST_AUTHORITY_SHA256="${STV3_WORKER_FIRST_AUTHORITY_SHA256-}"
  STV3_WORKER_ONLY_ROOT="${STV3_WORKER_ONLY_ROOT:-/share/storm_tracker_worker/runtime/worker_only_production_v1}"
  STV3_NETATMO_CLIENT_ID="${STV3_NETATMO_CLIENT_ID:-}"
  STV3_NETATMO_CLIENT_SECRET="${STV3_NETATMO_CLIENT_SECRET:-}"
  STV3_NETATMO_REFRESH_TOKEN="${STV3_NETATMO_REFRESH_TOKEN:-}"
  STV3_NETATMO_TOKEN_STATE="${STV3_NETATMO_TOKEN_STATE:-/share/storm_tracker_worker/private/netatmo_token_state.json}"
  STV3_AWN_APPLICATION_KEY="${STV3_AWN_APPLICATION_KEY:-}"
  STV3_AWN_API_KEY="${STV3_AWN_API_KEY:-}"
  STV3_AWN_RAW_RETENTION_AUTHORITY="${STV3_AWN_RAW_RETENTION_AUTHORITY:-}"
  STV3_SYNOPTIC_TOKEN="${STV3_SYNOPTIC_TOKEN:-}"
  STV3_SYNOPTIC_RAW_RETENTION_AUTHORITY="${STV3_SYNOPTIC_RAW_RETENTION_AUTHORITY:-}"
  STV3_RAINVIEWER_CACHE_ATTRIBUTION_AUTHORITY="${STV3_RAINVIEWER_CACHE_ATTRIBUTION_AUTHORITY:-}"
  STV3_KMI_APPV4_RAW_RETENTION_AUTHORITY="${STV3_KMI_APPV4_RAW_RETENTION_AUTHORITY:-}"
  STV3_OPERA_RAW_RETENTION_AUTHORITY="${STV3_OPERA_RAW_RETENTION_AUTHORITY:-}"
  STV3_BLITZORTUNG_RAW_RETENTION_AUTHORITY="${STV3_BLITZORTUNG_RAW_RETENTION_AUTHORITY:-}"
  STV3_KNMI_API_KEY="${STV3_KNMI_API_KEY:-}"
  STV3_AEMET_API_KEY="${STV3_AEMET_API_KEY:-}"
  STV3_METEOFRANCE_API_TOKEN="${STV3_METEOFRANCE_API_TOKEN:-}"
  STV3_METEOFRANCE_APPLICATION_ID="${STV3_METEOFRANCE_APPLICATION_ID:-}"
  STV3_HSAF_USERNAME="${STV3_HSAF_USERNAME:-}"
  STV3_HSAF_PASSWORD="${STV3_HSAF_PASSWORD:-}"
  STV3_EUMETSAT_CONSUMER_KEY="${STV3_EUMETSAT_CONSUMER_KEY:-}"
  STV3_EUMETSAT_CONSUMER_SECRET="${STV3_EUMETSAT_CONSUMER_SECRET:-}"
  STV3_JOB_RECEIPT_DB="${STV3_JOB_RECEIPT_DB:-/share/storm_tracker_worker/state/service_job_receipts.sqlite3}"
else
  # Home Assistant app mode.  Supervisor owns MQTT discovery and options.
  STV3_RUNNING_UNDER_HA="true"
  STV3_MQTT_HOST="$(bashio::services mqtt host)"
  STV3_MQTT_PORT="$(bashio::services mqtt port)"
  STV3_MQTT_USERNAME="$(bashio::services mqtt username)"
  STV3_MQTT_PASSWORD="$(bashio::services mqtt password)"
  STV3_MQTT_TLS="$(bashio::services mqtt ssl)"
  STV3_QUEUE_SIZE="$(bashio::config queue_size)"
  if bashio::config.has_value compute_profile; then
    STV3_COMPUTE_PROFILE="$(bashio::config compute_profile)"
  else
    STV3_COMPUTE_PROFILE="shared_ha"
  fi
  STV3_WORKER_NICE="$(bashio::config worker_nice)"
  if bashio::config.has_value cpu_limit_percent; then
    STV3_CPU_LIMIT_PERCENT="$(bashio::config cpu_limit_percent)"
  else
    STV3_CPU_LIMIT_PERCENT="40"
  fi
  STV3_HEARTBEAT_SECONDS="$(bashio::config heartbeat_seconds)"
  if bashio::config.has_value provider_cycle_seconds; then
    STV3_PROVIDER_CYCLE_SECONDS="$(
      bashio::config provider_cycle_seconds
    )"
  else
    STV3_PROVIDER_CYCLE_SECONDS="60"
  fi
  STV3_LOG_LEVEL="$(bashio::config log_level)"
  if bashio::config.has_value runtime_mode; then
    STV3_RUNTIME_MODE="$(bashio::config runtime_mode)"
  else
    STV3_RUNTIME_MODE="worker_only"
  fi
  if bashio::config.has_value worker_first_config_path; then
    STV3_WORKER_FIRST_CONFIG_PATH="$(bashio::config worker_first_config_path)"
  else
    STV3_WORKER_FIRST_CONFIG_PATH=""
  fi
  if bashio::config.has_value worker_first_config_sha256; then
    STV3_WORKER_FIRST_CONFIG_SHA256="$(bashio::config worker_first_config_sha256)"
  else
    STV3_WORKER_FIRST_CONFIG_SHA256=""
  fi
  if bashio::config.has_value worker_first_authority_sha256; then
    STV3_WORKER_FIRST_AUTHORITY_SHA256="$(bashio::config worker_first_authority_sha256)"
  else
    STV3_WORKER_FIRST_AUTHORITY_SHA256=""
  fi
  STV3_WORKER_ONLY_ROOT="${STV3_WORKER_ONLY_ROOT:-/data/storm_tracker_worker_runtime_production_v1}"
  STV3_NETATMO_CLIENT_ID="$(bashio::config netatmo_client_id)"
  STV3_NETATMO_CLIENT_SECRET="$(bashio::config netatmo_client_secret)"
  STV3_NETATMO_REFRESH_TOKEN="$(bashio::config netatmo_refresh_token)"
  STV3_NETATMO_TOKEN_STATE="${STV3_NETATMO_TOKEN_STATE:-/data/netatmo_token_state.json}"
  STV3_AWN_APPLICATION_KEY="$(bashio::config awn_application_key)"
  STV3_AWN_API_KEY="$(bashio::config awn_api_key)"
  STV3_SYNOPTIC_TOKEN="$(bashio::config synoptic_token)"
  if [[ "$(bashio::config awn_private_raw_retention_authorized)" == "true" ]]; then
    STV3_AWN_RAW_RETENTION_AUTHORITY="worker_authority.awn_private_raw_retention_v1"
  else
    STV3_AWN_RAW_RETENTION_AUTHORITY=""
  fi
  if [[ "$(bashio::config synoptic_raw_retention_terms_accepted)" == "true" ]]; then
    STV3_SYNOPTIC_RAW_RETENTION_AUTHORITY="worker_authority.synoptic_raw_retention_terms_v1"
  else
    STV3_SYNOPTIC_RAW_RETENTION_AUTHORITY=""
  fi
  if [[ "$(bashio::config rainviewer_cache_attribution_accepted)" == "true" ]]; then
    STV3_RAINVIEWER_CACHE_ATTRIBUTION_AUTHORITY="worker_authority.rainviewer_cache_attribution_v1"
  else
    STV3_RAINVIEWER_CACHE_ATTRIBUTION_AUTHORITY=""
  fi
  if [[ "$(bashio::config kmi_appv4_raw_retention_authorized)" == "true" ]]; then
    STV3_KMI_APPV4_RAW_RETENTION_AUTHORITY="worker_authority.kmi_appv4_raw_retention_v1"
  else
    STV3_KMI_APPV4_RAW_RETENTION_AUTHORITY=""
  fi
  if [[ "$(bashio::config opera_raw_retention_authorized)" == "true" ]]; then
    STV3_OPERA_RAW_RETENTION_AUTHORITY="worker_authority.opera_raw_retention_v1"
  else
    STV3_OPERA_RAW_RETENTION_AUTHORITY=""
  fi
  if [[ "$(bashio::config blitzortung_raw_retention_authorized)" == "true" ]]; then
    STV3_BLITZORTUNG_RAW_RETENTION_AUTHORITY="worker_authority.blitzortung_participant_raw_retention_v1"
  else
    STV3_BLITZORTUNG_RAW_RETENTION_AUTHORITY=""
  fi
  STV3_KNMI_API_KEY="$(bashio::config knmi_api_key)"
  STV3_AEMET_API_KEY="$(bashio::config aemet_api_key)"
  STV3_METEOFRANCE_API_TOKEN="$(bashio::config meteofrance_api_token)"
  STV3_METEOFRANCE_APPLICATION_ID="$(bashio::config meteofrance_application_id)"
  STV3_HSAF_USERNAME="$(bashio::config hsaf_username)"
  STV3_HSAF_PASSWORD="$(bashio::config hsaf_password)"
  STV3_EUMETSAT_CONSUMER_KEY="$(bashio::config eumetsat_consumer_key)"
  STV3_EUMETSAT_CONSUMER_SECRET="$(bashio::config eumetsat_consumer_secret)"
  STV3_JOB_RECEIPT_DB="${STV3_JOB_RECEIPT_DB:-/data/storm_tracker_worker_receipts.sqlite3}"

fi

# Materialize the same pinned worker-only authority in either deployment. The
# NAS root stays inside its one persistent mount; HA keeps the existing /data
# boundary. A partial manual pinset remains an error in the closed gate below.
if [[ "${STV3_RUNNING_UNDER_HA}" == "true" ]]; then
  case "${STV3_WORKER_ONLY_ROOT}" in
    /data/*) ;;
    *)
      bashio::log.fatal "Ongeldige worker-only opslagroot"
      exit 1
      ;;
  esac
else
  case "${STV3_WORKER_ONLY_ROOT}" in
    /share/storm_tracker_worker/*) ;;
    *)
      bashio::log.fatal "Ongeldige worker-only opslagroot"
      exit 1
      ;;
  esac
fi
if [[ "${STV3_RUNTIME_MODE}" == "worker_only" &&
  -z "${STV3_WORKER_FIRST_CONFIG_PATH}" &&
  -z "${STV3_WORKER_FIRST_CONFIG_SHA256}" &&
  -z "${STV3_WORKER_FIRST_AUTHORITY_SHA256}" ]]; then
  bootstrap_output="$(
    /opt/storm-tracker/bin/python \
      -m storm_tracker_worker.worker_only_bootstrap \
      --root "${STV3_WORKER_ONLY_ROOT}"
  )"
  IFS=$'\t' read -r \
    STV3_WORKER_FIRST_CONFIG_PATH \
    STV3_WORKER_FIRST_CONFIG_SHA256 \
    STV3_WORKER_FIRST_AUTHORITY_SHA256 <<< "${bootstrap_output}"
  unset bootstrap_output
fi
unset STV3_WORKER_ONLY_ROOT

# Host safety is an explicit profile. shared_ha protects the NUC at 25% and
# nice 15. dedicated_nas may use 50% and nice 10, while still keeping exactly
# two numerical threads and one complete physical core outside its CPU-set.
if [[ ! "${STV3_CPU_LIMIT_PERCENT}" =~ ^[0-9]+$ ||
  "${STV3_CPU_LIMIT_PERCENT}" -lt 25 ]]; then
  bashio::log.fatal "Ongeldige Storm-worker CPU-limiet"
  exit 1
fi
if [[ ! "${STV3_WORKER_NICE}" =~ ^[0-9]+$ ||
  "${STV3_WORKER_NICE}" -gt 19 ]]; then
  bashio::log.fatal "Ongeldige Storm-worker nice-waarde"
  exit 1
fi
case "${STV3_COMPUTE_PROFILE}" in
  shared_ha)
    if [[ "${STV3_CPU_LIMIT_PERCENT}" -gt 25 ]]; then
      bashio::log.warning \
        "shared_ha begrenst de Storm-worker tot 25% CPU"
      STV3_CPU_LIMIT_PERCENT="25"
    fi
    if [[ "${STV3_WORKER_NICE}" -lt 15 ]]; then
      bashio::log.warning \
        "shared_ha verlaagt de Storm-workerprioriteit naar nice 15"
      STV3_WORKER_NICE="15"
    fi
    ;;
  dedicated_nas)
    if [[ "${STV3_CPU_LIMIT_PERCENT}" -gt 50 ]]; then
      bashio::log.warning \
        "dedicated_nas begrenst de Storm-worker tot 50% CPU"
      STV3_CPU_LIMIT_PERCENT="50"
    fi
    if [[ "${STV3_WORKER_NICE}" -lt 10 ]]; then
      bashio::log.warning \
        "dedicated_nas bewaart HA-voorrang met nice 10"
      STV3_WORKER_NICE="10"
    fi
    ;;
  *)
    bashio::log.fatal "Ongeldig Storm-worker compute_profile"
    exit 1
    ;;
esac
export STV3_COMPUTE_PROFILE
export STV3_WORKER_NICE
export STV3_CPU_LIMIT_PERCENT

case "${STV3_RUNTIME_MODE}" in
  legacy)
    if [[ -n "${STV3_WORKER_FIRST_CONFIG_PATH}" ||
      -n "${STV3_WORKER_FIRST_CONFIG_SHA256}" ||
      -n "${STV3_WORKER_FIRST_AUTHORITY_SHA256}" ]]; then
      bashio::log.fatal "Ongeldige worker-runtimeconfiguratie"
      exit 1
    fi
    unset STV3_WORKER_FIRST_CONFIG_PATH
    unset STV3_WORKER_FIRST_CONFIG_SHA256
    unset STV3_WORKER_FIRST_AUTHORITY_SHA256
    unset STV3_BLITZORTUNG_RAW_RETENTION_AUTHORITY
    ;;
  worker_first_canary)
    if [[ -z "${STV3_WORKER_FIRST_CONFIG_PATH}" ||
      -z "${STV3_WORKER_FIRST_CONFIG_SHA256}" ||
      -z "${STV3_WORKER_FIRST_AUTHORITY_SHA256}" ]]; then
      bashio::log.fatal "Ongeldige worker-runtimeconfiguratie"
      exit 1
    fi
    export STV3_WORKER_FIRST_CONFIG_PATH
    export STV3_WORKER_FIRST_CONFIG_SHA256
    export STV3_WORKER_FIRST_AUTHORITY_SHA256
    # This canary admits exact DWD RADOLAN-RV only.  Credentials and mutable
    # legacy receipt paths for every other provider must not cross exec or be
    # inherited by its isolated child processes.
    unset STV3_NETATMO_CLIENT_ID
    unset STV3_NETATMO_CLIENT_SECRET
    unset STV3_NETATMO_REFRESH_TOKEN
    unset STV3_NETATMO_TOKEN_STATE
    unset STV3_AWN_APPLICATION_KEY
    unset STV3_AWN_API_KEY
    unset STV3_AWN_RAW_RETENTION_AUTHORITY
    unset STV3_SYNOPTIC_TOKEN
    unset STV3_SYNOPTIC_RAW_RETENTION_AUTHORITY
    unset STV3_RAINVIEWER_CACHE_ATTRIBUTION_AUTHORITY
    unset STV3_KMI_APPV4_RAW_RETENTION_AUTHORITY
    unset STV3_OPERA_RAW_RETENTION_AUTHORITY
    unset STV3_BLITZORTUNG_RAW_RETENTION_AUTHORITY
    unset STV3_KNMI_API_KEY
    unset STV3_AEMET_API_KEY
    unset STV3_METEOFRANCE_API_TOKEN
    unset STV3_METEOFRANCE_APPLICATION_ID
    unset STV3_HSAF_USERNAME
    unset STV3_HSAF_PASSWORD
    unset STV3_EUMETSAT_CONSUMER_KEY
    unset STV3_EUMETSAT_CONSUMER_SECRET
    unset STV3_JOB_RECEIPT_DB
    ;;
  raw_capture_shadow|worker_only)
    if [[ -z "${STV3_WORKER_FIRST_CONFIG_PATH}" ||
      -z "${STV3_WORKER_FIRST_CONFIG_SHA256}" ||
      -z "${STV3_WORKER_FIRST_AUTHORITY_SHA256}" ]]; then
      bashio::log.fatal "Ongeldige worker-runtimeconfiguratie"
      exit 1
    fi
    export STV3_WORKER_FIRST_CONFIG_PATH
    export STV3_WORKER_FIRST_CONFIG_SHA256
    export STV3_WORKER_FIRST_AUTHORITY_SHA256
    # Provider credentials stay worker-local.  The raw-capture runtime passes
    # only its explicit provider allowlist to each fresh acquisition child;
    # none of these values enters MQTT, a job payload or a diagnostic.
    unset STV3_JOB_RECEIPT_DB
    ;;
  *)
    bashio::log.fatal "Ongeldige worker-runtimeconfiguratie"
    exit 1
    ;;
esac
export STV3_RUNTIME_MODE

# This release owns exactly two numerical threads in every deployment mode.
STV3_CPU_THREADS="2"

bashio::log.info "Storm Tracker Worker start met profiel ${STV3_COMPUTE_PROFILE}, queue ${STV3_QUEUE_SIZE}, maximaal ${STV3_CPU_THREADS} CPU-threads, nice ${STV3_WORKER_NICE} en ${STV3_CPU_LIMIT_PERCENT}% totale CPU"
export OMP_NUM_THREADS="${STV3_CPU_THREADS}"
export OPENBLAS_NUM_THREADS="${STV3_CPU_THREADS}"
export MKL_NUM_THREADS="${STV3_CPU_THREADS}"
export NUMEXPR_NUM_THREADS="${STV3_CPU_THREADS}"

STV3_WORKER_CPU_SET="$(
  /opt/storm-tracker/bin/python -m storm_tracker_worker.cpu_topology
)"
if [[ -z "${STV3_WORKER_CPU_SET}" ]]; then
  bashio::log.fatal "Geen veilige worker-CPU-set gevonden"
  exit 1
fi
bashio::log.info \
  "Worker-CPU-set ${STV3_WORKER_CPU_SET}; minstens een fysieke core blijft vrij voor HA/Z2M"

exec nice -n "${STV3_WORKER_NICE}" \
  taskset --cpu-list "${STV3_WORKER_CPU_SET}" \
  /opt/storm-tracker/bin/python \
  -m storm_tracker_worker.cpu_limited_launcher \
  --limit "${STV3_CPU_LIMIT_PERCENT}" \
  -- /opt/storm-tracker/bin/python -m storm_tracker_worker.service
