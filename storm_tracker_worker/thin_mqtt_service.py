"""Direct MQTT shell for the thin operational runtime."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import threading
import time
from typing import Any

from . import __version__ as WORKER_VERSION
from .protocol import decode, encode, envelope, job_payload, topic
from .thin_operational_runtime import (
    MAX_GEOJSON_PAYLOAD_BYTES,
    ThinOperationalRuntime,
)
from .thin_blitzortung import ThinBlitzortungFeed
from .thin_satellite_adapters import (
    EumetsatLightningAdapter,
    GoesGlmAdapter,
    GoesRrqpeAdapter,
    HsafH40bAdapter,
    NoaaErrAdapter,
)
from .thin_ground_station_runtime import GroundStationRuntime
from .thin_postgis import ThinPostGISSettings, ThinPostGISWriter
from .thin_provider_adapters import (
    AemetRadarAdapter,
    AioHttpFetcher,
    BlitzortungAdapter,
    BlockedProviderAdapter,
    DpcRadarAdapter,
    DwdRadolanAdapter,
    GeoSphereNowcastAdapter,
    ItaliaMeteoDpcHistoricalCatalogAdapter,
    ItaliaMeteoIcon2iAdapter,
    KmiAwsAdapter,
    KmiRadarAdapter,
    KnmiRadarAdapter,
    MetarAdapter,
    MeteoFranceOAuthTokenProvider,
    MeteoFranceRadarAdapter,
    MeteoLuxValidationAdapter,
    MetOfficeRadarAdapter,
    NetatmoAdapter,
    NetatmoOAuthTokenProvider,
    OpenMeteoAdapter,
    OperaRadarAdapter,
    RainViewerRadarAdapter,
    WundergroundAdapter,
)
from .thin_provider_runtime import ThinProviderRuntime
from .thin_station_catalog import StationCatalogRepository
from .tracking_core.operational_snapshot import encode_control_envelope


_LOGGER = logging.getLogger(__name__)

WORKER_TECHNICAL_SOURCE_CONTRACT = (
    "stv3-worker-technical-diagnostics-source-v1"
)
WORKER_TECHNICAL_SOURCE_FIELDS = frozenset({
    "build_sha256",
    "cache_age_seconds",
    "cache_status",
    "capacity_alarm",
    "config_sha256",
    "contract",
    "current_version",
    "desired_state_sha256",
    "failed_jobs",
    "frame_age_seconds",
    "frame_status",
    "headroom_bytes",
    "headroom_percent",
    "next_version",
    "provider_age_seconds",
    "provider_status",
    "queue_depth",
    "replay_algorithm_version",
    "replay_contract_version",
    "retention_alarm",
    "storage_reason_codes",
    "terminal_jobs",
    "transition_reason_codes",
    "transition_status",
    "worker_observed_at",
    "worker_state",
    "worker_version",
})


class ThinMqttService:
    def __init__(
        self,
        *,
        client: Any,
        runtime: ThinOperationalRuntime,
        topic_root: str = "storm_tracker/v1",
        heartbeat_seconds: float = 10.0,
        cycle_seconds: float = 60.0,
        lightning_feed: Any | None = None,
    ) -> None:
        self.client = client
        self.runtime = runtime
        self.topic_root = topic_root.rstrip("/")
        self.heartbeat_seconds = max(2.0, float(heartbeat_seconds))
        self.cycle_seconds = max(2.0, float(cycle_seconds))
        self.lightning_feed = lightning_feed
        self._wake = asyncio.Event()
        self._stop = asyncio.Event()
        self._tasks: list[asyncio.Task] = []
        self._loop: asyncio.AbstractEventLoop | None = None
        self._heartbeat_stop = threading.Event()
        self._heartbeat_thread: threading.Thread | None = None
        self._job_results: dict[str, dict[str, Any]] = {}
        self._last_snapshot_revision = 0
        self._last_snapshot_published_at: float | None = None
        self._last_error: str | None = None
        self._last_cycle_summary: tuple[Any, ...] | None = None
        self._last_geojson_feature_count: int | None = None
        self._postgis_projection_enabled: bool | None = None
        self._postgis_projection_status: str | None = None
        self._postgis_projection_batch = 0
        self._postgis_projection_ready = 0
        self._postgis_projection_leased = 0
        self._postgis_projection_projected = 0
        self._postgis_projection_last_success_at: float | None = None
        self._postgis_projection_last_error_code: str | None = None
        self._provider_status_snapshot: dict[str, Any] | None = None

    def _technical_diagnostics_source(
        self,
        *,
        state: str,
        observed_at: float,
    ) -> dict[str, Any]:
        snapshot_age = (
            None
            if self._last_snapshot_published_at is None
            else max(0.0, observed_at - self._last_snapshot_published_at)
        )
        snapshot_status = (
            "unknown"
            if snapshot_age is None
            else "current" if snapshot_age <= 300.0 else "stale"
        )
        desired_state = self.runtime.desired_state_snapshot()
        desired_state_sha256 = (
            desired_state.get("state_sha256")
            if isinstance(desired_state, dict)
            and desired_state.get("status") == "configured"
            else None
        )
        source = {
            "contract": WORKER_TECHNICAL_SOURCE_CONTRACT,
            "worker_state": state,
            "worker_observed_at": observed_at,
            "worker_version": WORKER_VERSION,
            "build_sha256": None,
            "config_sha256": None,
            "desired_state_sha256": desired_state_sha256,
            "provider_age_seconds": snapshot_age,
            "provider_status": snapshot_status,
            "frame_age_seconds": snapshot_age,
            "frame_status": snapshot_status,
            "cache_age_seconds": snapshot_age,
            "cache_status": snapshot_status,
            "queue_depth": 0,
            "terminal_jobs": min(1_000_000_000, len(self._job_results)),
            "failed_jobs": 0 if self._last_error is None else 1,
            "retention_alarm": None,
            "capacity_alarm": None,
            "headroom_bytes": None,
            "headroom_percent": None,
            "storage_reason_codes": None,
            "transition_status": "stable",
            "current_version": WORKER_VERSION,
            "next_version": None,
            "transition_reason_codes": [],
            "replay_contract_version": None,
            "replay_algorithm_version": None,
        }
        if set(source) != WORKER_TECHNICAL_SOURCE_FIELDS:
            raise RuntimeError("thin worker technical source schema drift")
        return source

    def _heartbeat(self, state: str = "online") -> bytes:
        observed_at = time.time()
        snapshot_runtime: dict[str, Any] = {
            "snapshot_revision": self._last_snapshot_revision,
            "snapshot_transport_state": (
                "current" if self._last_snapshot_revision else "waiting"
            ),
        }
        if (
            isinstance(self._postgis_projection_enabled, bool)
            and self._postgis_projection_status
            in {"disabled", "waiting", "current", "degraded"}
        ):
            snapshot_runtime["postgis_projection"] = {
                "enabled": self._postgis_projection_enabled,
                "status": self._postgis_projection_status,
                "batch": self._postgis_projection_batch,
                "ready": self._postgis_projection_ready,
                "leased": self._postgis_projection_leased,
                "projected": self._postgis_projection_projected,
                "last_success_at": self._postgis_projection_last_success_at,
                "last_error_code": self._postgis_projection_last_error_code,
            }
        if self._provider_status_snapshot is not None:
            snapshot_runtime["provider_status_snapshot"] = (
                self._provider_status_snapshot
            )
        return encode(envelope(
            "worker_status",
            state=state,
            worker_version=WORKER_VERSION,
            runtime_mode="thin_operational",
            job_admission_open=state != "offline",
            queue_depth=0,
            failed_jobs=0 if self._last_error is None else 1,
            compute_profile=os.getenv("STV3_COMPUTE_PROFILE", "dedicated_nas_fast"),
            cpu_limit_percent=int(os.getenv("STV3_CPU_LIMIT_PERCENT", "200")),
            worker_nice=int(os.getenv("STV3_WORKER_NICE", "0")),
            cpu_threads=2,
            provider_cycle_seconds=self.cycle_seconds,
            operational_snapshot_runtime=snapshot_runtime,
            technical_diagnostics=self._technical_diagnostics_source(
                state=state,
                observed_at=observed_at,
            ),
        ))

    def _publish_heartbeat(self) -> None:
        self.client.publish(
            topic(self.topic_root, "worker/status"),
            self._heartbeat("degraded" if self._last_error else "online"),
            qos=1,
            retain=True,
        )

    def _run_heartbeat_thread(self) -> None:
        while not self._heartbeat_stop.wait(self.heartbeat_seconds):
            try:
                self._publish_heartbeat()
            except Exception:
                _LOGGER.exception("Publiceren van workerheartbeat mislukt")

    async def start(self) -> None:
        self._loop = asyncio.get_running_loop()
        self.client.on_message = self._on_message
        self.client.on_connect = self._on_connect
        self.client.will_set(
            topic(self.topic_root, "worker/status"),
            self._heartbeat("offline"),
            qos=1,
            retain=True,
        )
        if hasattr(self.client, "connect_async"):
            self.client.connect_async(
                os.getenv("STV3_MQTT_HOST", "core-mosquitto"),
                int(os.getenv("STV3_MQTT_PORT", "1883")),
                keepalive=60,
            )
        self.client.loop_start()
        self._heartbeat_stop.clear()
        self._heartbeat_thread = threading.Thread(
            target=self._run_heartbeat_thread,
            name="thin-heartbeat",
            daemon=True,
        )
        self._heartbeat_thread.start()
        if self.lightning_feed is not None:
            self.lightning_feed.update_targets(self.runtime.targets)
            self.lightning_feed.start()
        self.client.publish(
            topic(self.topic_root, "worker/status"),
            self._heartbeat("online"),
            qos=1,
            retain=True,
        )
        self.client.subscribe(topic(self.topic_root, "jobs/request"), qos=1)
        self._tasks = [
            asyncio.create_task(self._cycle_loop(), name="thin-cycle"),
        ]
        if self.runtime.has_state:
            self._wake.set()

    def _on_connect(
        self,
        client: Any,
        userdata: Any,
        flags: Any,
        reason_code: Any,
        properties: Any = None,
    ) -> None:
        try:
            successful = int(reason_code) == 0
        except (TypeError, ValueError):
            successful = str(reason_code).lower() in {"0", "success"}
        if successful:
            client.subscribe(topic(self.topic_root, "jobs/request"), qos=1)
            self._publish_heartbeat()

    async def stop(self) -> None:
        self._stop.set()
        self._wake.set()
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        close_runtime = getattr(self.runtime, "close", None)
        if callable(close_runtime):
            close_runtime()
        self._heartbeat_stop.set()
        if self._heartbeat_thread is not None:
            self._heartbeat_thread.join(timeout=2.0)
            self._heartbeat_thread = None
        if self.lightning_feed is not None:
            self.lightning_feed.stop()
        self.client.publish(
            topic(self.topic_root, "worker/status"),
            self._heartbeat("offline"),
            qos=1,
            retain=True,
        )
        self.client.loop_stop()
        self.client.disconnect()

    async def _cycle_loop(self) -> None:
        while not self._stop.is_set():
            if not self.runtime.has_state:
                self._wake.clear()
                await self._wake.wait()
                continue
            self._wake.clear()
            try:
                result = await self.runtime.run_cycle()
                payload = encode_control_envelope(result.snapshot)
                self._last_snapshot_revision = int(result.snapshot["revision"])
                self._last_error = None
                self._postgis_projection_enabled = (
                    result.postgis.status != "disabled"
                )
                self._postgis_projection_status = result.postgis.status
                self._postgis_projection_batch = self._last_snapshot_revision
                self._postgis_projection_ready = max(
                    0, int(result.postgis.queued_cycles)
                )
                self._postgis_projection_leased = 0
                self._postgis_projection_projected = sum(
                    max(0, int(value))
                    for value in result.postgis.written_rows.values()
                )
                if result.postgis.status == "current":
                    self._postgis_projection_last_success_at = time.time()
                self._postgis_projection_last_error_code = (
                    "postgis_projection_failed"
                    if result.postgis.last_error_code is not None
                    else None
                )
                provider_items = result.snapshot.get("providers", {}).get("items", [])
                self._provider_status_snapshot = {
                    "desired_state_sha256": result.snapshot[
                        "desired_state_sha256"
                    ],
                    "evaluated_at": result.snapshot["evaluated_at"],
                    "valid_until": result.snapshot["valid_until"],
                    "items": [dict(item) for item in provider_items],
                }
                provider_summary = tuple(
                    sorted(
                        (
                            str(item.get("provider_id")),
                            str(item.get("state")),
                            ",".join(str(code) for code in item.get("reason_codes", [])),
                        )
                        for item in provider_items
                        if isinstance(item, dict)
                    )
                )
                cycle_summary = (
                    len(result.fusion.observations),
                    provider_summary,
                    result.postgis.status,
                    result.postgis.last_error_code,
                )
                if cycle_summary != self._last_cycle_summary:
                    _LOGGER.info(
                        "Dunne cyclus: observaties=%d providers=%s postgis=%s fout=%s",
                        cycle_summary[0],
                        provider_summary,
                        result.postgis.status,
                        result.postgis.last_error_code,
                    )
                    self._last_cycle_summary = cycle_summary
                self.client.publish(
                    topic(self.topic_root, "operational/snapshot"),
                    payload,
                    qos=1,
                    retain=True,
                )
                self._last_snapshot_published_at = time.time()
                passages_payload = json.dumps({
                    "schema_version": 1,
                    "evaluated_at": result.nowcast.evaluated_at,
                    "items": [{
                        "target_id": item.target_id,
                        "cell_id": item.cell_id,
                        "provider_id": item.provider_id,
                        "entry_at": item.entry_at,
                        "exit_at": item.exit_at,
                        "method": item.method,
                        "predicted_intensity_mm_h": item.predicted_intensity_mm_h,
                        "fallback_reason": item.fallback_reason,
                        "horizon_clipped": item.horizon_clipped,
                    } for item in result.nowcast.target_passages[:256]],
                    "overflow_count": max(
                        0, len(result.nowcast.target_passages) - 256,
                    ),
                }, allow_nan=False, separators=(",", ":"), sort_keys=True).encode("utf-8")
                self.client.publish(
                    topic(self.topic_root, "operational/passages"),
                    passages_payload,
                    qos=1,
                    retain=True,
                )
                validation_counts: dict[str, int] = {}
                for validation in result.validations:
                    validation_counts[validation.outcome] = (
                        validation_counts.get(validation.outcome, 0) + 1
                    )
                validation_payload = json.dumps({
                    "schema_version": 1,
                    "evaluated_at": result.nowcast.evaluated_at,
                    "status": "current",
                    "new_count": len(result.validations),
                    "outcomes": validation_counts,
                }, allow_nan=False, separators=(",", ":"), sort_keys=True).encode("utf-8")
                self.client.publish(
                    topic(self.topic_root, "operational/validation"),
                    validation_payload,
                    qos=1,
                    retain=True,
                )
                geojson_payload = json.dumps(
                    self.runtime.geojson_collection(),
                    allow_nan=False,
                    ensure_ascii=False,
                    separators=(",", ":"),
                    sort_keys=True,
                ).encode("utf-8")
                if len(geojson_payload) > MAX_GEOJSON_PAYLOAD_BYTES:
                    raise ValueError("geojson_payload_too_large")
                self.client.publish(
                    topic(self.topic_root, "operational/geojson"),
                    geojson_payload,
                    qos=1,
                    retain=True,
                )
                geojson_feature_count = len(
                    self.runtime.geojson_collection().get("features", [])
                )
                if geojson_feature_count != self._last_geojson_feature_count:
                    _LOGGER.info(
                        "Dunne kaart gepubliceerd: features=%d bytes=%d",
                        geojson_feature_count,
                        len(geojson_payload),
                    )
                    self._last_geojson_feature_count = geojson_feature_count
                self._publish_heartbeat()
            except asyncio.CancelledError:
                raise
            except Exception as error:
                self._last_error = type(error).__name__
                _LOGGER.exception("Dunne operationele cyclus mislukt")
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=self.cycle_seconds)
            except TimeoutError:
                pass

    def _on_message(self, client: Any, userdata: Any, message: Any) -> None:
        if self._loop is None:
            return
        payload = bytes(message.payload)
        incoming_topic = str(message.topic)
        self._loop.call_soon_threadsafe(
            lambda: asyncio.create_task(self.handle_message(incoming_topic, payload))
        )

    async def handle_message(self, incoming_topic: str, payload: bytes | str) -> None:
        if incoming_topic != topic(self.topic_root, "jobs/request"):
            return
        request: dict[str, Any] | None = None
        try:
            request = decode(payload)
            if request.get("message_type") != "job_request":
                raise ValueError("invalid_message_type")
            job_id = str(request["job_id"])
            job_type = str(request["job_type"])
            if float(request["deadline"]) <= time.time():
                raise TimeoutError("deadline_exceeded")
            cached = self._job_results.get(job_id)
            if cached is not None:
                self._publish_job_status(cached)
                return
            values = job_payload(request)
            if job_type == "desired_state_claim":
                result = self.runtime.claim_authority(
                    str(values["producer_id"]), str(values["claim_id"])
                )
            elif job_type == "desired_state_snapshot":
                result = self.runtime.desired_state_snapshot()
            elif job_type == "desired_state_apply":
                result = self.runtime.apply_desired_state(values["desired_state"])
                if self.lightning_feed is not None:
                    self.lightning_feed.update_targets(self.runtime.targets)
                self._wake.set()
            elif job_type == "geojson_operational":
                result = {"collection": self.runtime.geojson_collection()}
            elif job_type == "ping":
                result = {"pong": True}
            else:
                raise ValueError("unknown_job_type")
            status = envelope(
                "job_status",
                worker_id="thin-operational",
                job_id=job_id,
                job_type=job_type,
                state="completed",
                result=result,
            )
        except Exception as error:
            job_id = str(request.get("job_id", "invalid")) if request else "invalid"
            job_type = str(request.get("job_type", "invalid")) if request else "invalid"
            status = envelope(
                "job_status",
                worker_id="thin-operational",
                job_id=job_id,
                job_type=job_type,
                state="failed",
                error=type(error).__name__,
            )
        if status["state"] == "completed":
            self._job_results[job_id] = status
            if len(self._job_results) > 256:
                self._job_results.pop(next(iter(self._job_results)))
        self._publish_job_status(status)

    def _publish_job_status(self, status: dict[str, Any]) -> None:
        self.client.publish(
            topic(self.topic_root, f"jobs/{status['job_id']}/status"),
            encode(status),
            qos=1,
            retain=False,
        )


def _build_provider_adapters(fetcher: Any) -> dict[str, Any]:
    blitzortung = BlitzortungAdapter()
    return {
        "aemet_radar": AemetRadarAdapter(),
        "meteofrance_radar": MeteoFranceRadarAdapter(
            access_token_provider=MeteoFranceOAuthTokenProvider(
                fetcher=fetcher,
            ).get_access_token,
        ),
        "dpc_radar": DpcRadarAdapter(),
        "italiameteo_icon2i": ItaliaMeteoIcon2iAdapter(),
        "geosphere": GeoSphereNowcastAdapter(),
        "italiameteo_dpc_catalog": ItaliaMeteoDpcHistoricalCatalogAdapter(),
        "metoffice_radar": MetOfficeRadarAdapter(),
        "meteolux": MeteoLuxValidationAdapter(),
        "kmi_aws": KmiAwsAdapter(),
        "kmi_appv4": KmiRadarAdapter(),
        "kmi_radqpe": BlockedProviderAdapter(),
        "opera_radar": OperaRadarAdapter(),
        "knmi_radar": KnmiRadarAdapter(),
        "dwd_radolan": DwdRadolanAdapter(),
        "open_meteo": OpenMeteoAdapter(),
        "metar": MetarAdapter(),
        "netatmo": NetatmoAdapter(
            # Fixed household reference station. An explicit app option still
            # overrides this default when the Supervisor exposes that field.
            station_id_for_summary=(
                os.getenv("STV3_NETATMO_STATION_ID")
                or "70:ee:50:29:59:56"
            ),
            access_token_provider=NetatmoOAuthTokenProvider(
                fetcher=fetcher,
            ).get_access_token,
        ),
        "wunderground": WundergroundAdapter(),
        "blitzortung": blitzortung,
        "eumetsat_li": EumetsatLightningAdapter(),
        "goes18_glm": GoesGlmAdapter(18),
        "goes19_glm": GoesGlmAdapter(19),
        "goes18_rrqpe": GoesRrqpeAdapter(18),
        "goes19_rrqpe": GoesRrqpeAdapter(19),
        "noaa_err": NoaaErrAdapter(),
        "hsaf_h40b": HsafH40bAdapter(),
        "rainviewer": RainViewerRadarAdapter(),
    }


def _build_runtime() -> tuple[ThinOperationalRuntime, BlitzortungAdapter]:
    fetcher = AioHttpFetcher()
    adapters = _build_provider_adapters(fetcher)
    blitzortung = adapters["blitzortung"]
    settings = ThinPostGISSettings.from_environment()
    station_runtime = None
    if settings is not None:
        import psycopg

        enabled_ground_providers = {"metar"}
        if all(os.getenv(name) for name in (
            "STV3_NETATMO_CLIENT_ID",
            "STV3_NETATMO_CLIENT_SECRET",
            "STV3_NETATMO_REFRESH_TOKEN",
        )):
            enabled_ground_providers.add("netatmo")
        if os.getenv("STV3_WUNDERGROUND_API_KEY"):
            enabled_ground_providers.add("wunderground")
        station_runtime = GroundStationRuntime(
            StationCatalogRepository(
                psycopg.connect,
                settings.connect_kwargs(),
            ),
            enabled_providers=enabled_ground_providers,
        )
    provider_runtime = ThinProviderRuntime(
        adapters=adapters,
        fetcher=fetcher,
        # NAS-HA is dedicated to Storm Tracker.  Start every provider that is
        # actually due in the same I/O wave; the adapter-level radar decode
        # semaphore still caps CPU-heavy decoding at two threads.
        max_parallel=len(adapters),
        timeout_seconds=45.0,
        retries=1,
        request_preparer=station_runtime,
    )
    writer = ThinPostGISWriter(settings) if settings is not None else None
    return (
        ThinOperationalRuntime(
            provider_runtime=provider_runtime,
            postgis_writer=writer,
        ),
        blitzortung,
    )


async def _async_main() -> None:
    import paho.mqtt.client as mqtt

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)
    username = os.getenv("STV3_MQTT_USERNAME", "")
    if username:
        client.username_pw_set(username, os.getenv("STV3_MQTT_PASSWORD", ""))
    if os.getenv("STV3_MQTT_TLS", "false").lower() in {"1", "true", "yes", "on"}:
        client.tls_set()
    runtime, blitzortung = _build_runtime()
    lightning_client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION1,
        protocol=mqtt.MQTTv311,
    )
    service = ThinMqttService(
        client=client,
        runtime=runtime,
        lightning_feed=ThinBlitzortungFeed(
            client=lightning_client,
            adapter=blitzortung,
        ),
        heartbeat_seconds=float(os.getenv("STV3_HEARTBEAT_SECONDS", "10")),
        cycle_seconds=float(os.getenv("STV3_PROVIDER_CYCLE_SECONDS", "60")),
    )
    await service.start()
    stopped = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(signum, stopped.set)
        except NotImplementedError:
            pass
    await stopped.wait()
    await service.stop()


def main() -> None:
    logging.basicConfig(
        level=getattr(logging, os.getenv("STV3_LOG_LEVEL", "INFO").upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    asyncio.run(_async_main())


if __name__ == "__main__":
    main()


__all__ = ["ThinMqttService", "main"]
