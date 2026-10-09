"""Behaviour and isolation proof for the unwired HA desired-state controller."""
from __future__ import annotations

import ast
import asyncio
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import sys
import types

import pytest


ROOT = Path(__file__).resolve().parents[1]
PACKAGE_ROOT = ROOT / "custom_components/storm_tracker_v3"
MODULE_PATH = PACKAGE_ROOT / "desired_state_controller.py"
NOW = 1_800_000_000.0


def _load_file(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def controller_module(monkeypatch):
    package_name = "stv3_desired_controller_test_package"
    package = types.ModuleType(package_name)
    package.__path__ = [str(PACKAGE_ROOT)]
    monkeypatch.setitem(sys.modules, package_name, package)

    homeassistant = types.ModuleType("homeassistant")
    core = types.ModuleType("homeassistant.core")
    core.HomeAssistant = object
    homeassistant.core = core
    monkeypatch.setitem(sys.modules, "homeassistant", homeassistant)
    monkeypatch.setitem(sys.modules, "homeassistant.core", core)

    builder = _load_file(
        f"{package_name}.desired_state_builder",
        PACKAGE_ROOT / "desired_state_builder.py",
    )
    monkeypatch.setitem(
        sys.modules, f"{package_name}.desired_state_builder", builder
    )

    bridge = types.ModuleType(f"{package_name}.worker_bridge")

    async def unused_requester(*_args, **_kwargs):
        raise AssertionError("test moet een fake requester injecteren")

    bridge.async_request_worker_job = unused_requester
    monkeypatch.setitem(sys.modules, f"{package_name}.worker_bridge", bridge)
    return _load_file(f"{package_name}.desired_state_controller", MODULE_PATH)


def _target(
    *,
    latitude: float = 51.04,
    longitude: float = 4.48,
    observed_at: float | None = NOW,
    available: bool = True,
    target_id: str = "device_tracker.wim",
) -> dict:
    return {
        "target_id": target_id,
        "display_name": "Wim",
        "kind": "device_tracker",
        "available": available,
        "latitude": latitude,
        "longitude": longitude,
        "position_observed_at": observed_at,
        "location_accuracy_m": 12.0,
        "observation_radius_km": 250.0,
        "country_code": "BE",
    }


def _settings(*, ground_radius_km: float = 20.0) -> dict:
    return {
        "ground_radius_km": ground_radius_km,
        "lightning_source_mode": "auto",
        "model_guidance_enabled": False,
        "region_sharing_distance_km": 150.0,
    }


def _unconfigured_head(*, producer: str | None = None, epoch: int | None = None):
    result = {
        "method": "worker_desired_state_store_v1",
        "status": "unconfigured",
    }
    if producer is not None:
        result.update(producer_id=producer, authority_epoch=epoch)
    return result


def _configured_head(
    state_sha256: str,
    *,
    producer: str = "old-ha",
    epoch: int = 4,
    generation: int = 9,
    target_count: int = 1,
) -> dict:
    return {
        "method": "worker_desired_state_store_v1",
        "status": "configured",
        "state_sha256": state_sha256,
        "producer_id": producer,
        "authority_epoch": epoch,
        "generation": generation,
        "issued_at": NOW - 1,
        "first_received_at": NOW - 1,
        "last_received_at": NOW - 1,
        "receipt_count": 1,
        "activated_at": NOW - 1,
        "target_count": target_count,
    }


class FakeRequester:
    def __init__(self, *, epoch: int = 10, head: dict | None = None) -> None:
        self.epoch = epoch
        self.head = copy.deepcopy(head or _unconfigured_head())
        self.calls: list[dict] = []
        self.claim_id: str | None = None
        self.offline = False
        self.apply_failures: list[BaseException] = []
        self.on_apply_failure = None
        self.block_job_type: str | None = None
        self.block_started = asyncio.Event()
        self.block_release = asyncio.Event()

    async def __call__(
        self,
        _hass,
        job_type,
        payload,
        *,
        timeout,
        job_id,
    ):
        self.calls.append({
            "job_type": job_type,
            "payload": copy.deepcopy(payload),
            "timeout": timeout,
            "job_id": job_id,
        })
        if self.offline:
            raise RuntimeError("Storm Tracker-worker is niet beschikbaar")
        if self.block_job_type == job_type:
            self.block_started.set()
            await self.block_release.wait()
            self.block_job_type = None
        if job_type == "desired_state_claim":
            status = "claimed" if self.claim_id is None else "claim_idempotent"
            self.claim_id = payload["claim_id"]
            if self.head["status"] == "unconfigured":
                self.head = _unconfigured_head(
                    producer=payload["producer_id"], epoch=self.epoch
                )
            return {
                "method": "worker_desired_state_store_v1",
                "status": status,
                "producer_id": payload["producer_id"],
                "authority_epoch": self.epoch,
                "claim_id": payload["claim_id"],
                "claimed_at": NOW - 2,
            }
        if job_type == "desired_state_snapshot":
            return copy.deepcopy(self.head)
        if job_type != "desired_state_apply":
            raise AssertionError(job_type)
        document = payload["desired_state"]
        canonical = json.dumps(
            document,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        state_sha = hashlib.sha256(canonical).hexdigest()
        if self.apply_failures:
            error = self.apply_failures.pop(0)
            if self.on_apply_failure is not None:
                self.on_apply_failure(document)
            raise error
        self.head = _configured_head(
            state_sha,
            producer=document["producer_id"],
            epoch=document["authority_epoch"],
            generation=document["generation"],
            target_count=len(document["targets"]),
        )
        return {
            "method": "worker_desired_state_store_v1",
            "status": "activated",
            "state_sha256": state_sha,
            "producer_id": document["producer_id"],
            "authority_epoch": document["authority_epoch"],
            "generation": document["generation"],
            "issued_at": document["issued_at"],
            "first_received_at": NOW,
            "last_received_at": NOW,
            "receipt_count": 1,
            "target_count": len(document["targets"]),
        }


def _controller(
    module,
    fake,
    *,
    debounce: float = 0.0,
    authority_callback=None,
):
    return module.DesiredStateController(
        object(),
        producer_id="homeassistant.storm-tracker",
        boot_id="boot-test-a",
        requester=fake,
        authority_callback=authority_callback,
        clock=lambda: NOW,
        debounce_seconds=debounce,
        request_timeout=5.0,
    )


def test_claim_snapshot_apply_order_and_new_authority_extends_existing_head(
    controller_module,
):
    async def run():
        old_sha = "a" * 64
        fake = FakeRequester(
            epoch=10,
            head=_configured_head(old_sha, producer="previous-ha", epoch=9),
        )
        controller = _controller(controller_module, fake)
        await controller.async_start(targets=[_target()], settings=_settings())

        assert [call["job_type"] for call in fake.calls] == [
            "desired_state_claim",
            "desired_state_snapshot",
            "desired_state_apply",
        ]
        document = fake.calls[-1]["payload"]["desired_state"]
        assert document["producer_id"] == "homeassistant.storm-tracker"
        assert document["authority_epoch"] == 10
        assert document["previous_state_sha256"] == old_sha
        assert document["generation"] == 1
        assert all(0 < len(call["job_id"]) <= 80 for call in fake.calls)
        assert controller.diagnostics()["state"] == "idle"
        assert controller.diagnostics()["dirty"] is False

    asyncio.run(run())


def test_duplicate_updates_are_coalesced_without_an_apply_backlog(controller_module):
    async def run():
        fake = FakeRequester()
        controller = _controller(controller_module, fake, debounce=0.01)
        await controller.async_start(targets=[_target()], settings=_settings())
        for _ in range(5):
            assert not await controller.async_update(
                targets=[_target()], settings=_settings()
            )
        await asyncio.sleep(0.02)
        await controller.async_flush()
        assert sum(
            call["job_type"] == "desired_state_apply" for call in fake.calls
        ) == 1
        assert controller.diagnostics()["counters"]["coalesced_updates"] == 5

    asyncio.run(run())


def test_life360_timestamp_accuracy_and_sub_kilometre_jitter_do_not_republish(
    controller_module,
):
    async def run():
        fake = FakeRequester()
        controller = _controller(controller_module, fake, debounce=0.0)
        await controller.async_start(targets=[_target()], settings=_settings())

        refreshed = _target(
            latitude=51.0405,
            longitude=4.4805,
            observed_at=NOW + 180,
        )
        refreshed["location_accuracy_m"] = 35.0
        assert not await controller.async_update(
            targets=[refreshed], settings=_settings()
        )
        await controller.async_flush()

        applies = [
            call for call in fake.calls if call["job_type"] == "desired_state_apply"
        ]
        assert len(applies) == 1
        assert controller.diagnostics()["counters"]["coalesced_updates"] == 1

    asyncio.run(run())


def test_stationary_life360_liveness_is_refreshed_before_worker_expiry(
    controller_module,
):
    async def run():
        fake = FakeRequester()
        now = [NOW]
        controller = controller_module.DesiredStateController(
            object(),
            producer_id="homeassistant.storm-tracker",
            boot_id="boot-test-a",
            requester=fake,
            clock=lambda: now[0],
            debounce_seconds=0,
            request_timeout=5.0,
        )
        await controller.async_start(targets=[_target()], settings=_settings())

        now[0] += controller_module.MAX_SEMANTIC_SILENCE_SECONDS - 1
        assert not await controller.async_update(
            targets=[_target(observed_at=now[0])], settings=_settings()
        )

        now[0] += 1
        assert await controller.async_update(
            targets=[_target(observed_at=now[0])], settings=_settings()
        )
        await controller.async_flush()

        applies = [
            call for call in fake.calls if call["job_type"] == "desired_state_apply"
        ]
        assert len(applies) == 2
        refreshed = applies[-1]["payload"]["desired_state"]
        assert refreshed["generation"] == 2
        assert refreshed["targets"][0]["position_observed_at"] == now[0]
        assert controller.diagnostics()["counters"]["lease_refreshes"] == 1

    asyncio.run(run())


def test_liveness_only_authority_refresh_preserves_current_snapshot_but_movement_does_not(
    controller_module,
):
    async def run():
        fake = FakeRequester()
        now = [NOW]
        authorities: list[tuple[str | None, bool]] = []

        def record_authority(
            value: str | None,
            *,
            preserve_current_snapshot: bool = False,
        ) -> None:
            authorities.append((value, preserve_current_snapshot))

        controller = controller_module.DesiredStateController(
            object(),
            producer_id="homeassistant.storm-tracker",
            boot_id="boot-test-a",
            requester=fake,
            authority_callback=record_authority,
            clock=lambda: now[0],
            debounce_seconds=0,
            request_timeout=5.0,
        )
        await controller.async_start(targets=[_target()], settings=_settings())
        authorities.clear()

        now[0] += controller_module.MAX_SEMANTIC_SILENCE_SECONDS
        assert await controller.async_update(
            targets=[_target(observed_at=now[0])], settings=_settings()
        )
        assert authorities == [(None, True)]

        await controller.async_flush()
        assert authorities[-1] == (controller.head_state_sha256, True)

        assert await controller.async_update(
            targets=[_target(latitude=51.06, observed_at=now[0] + 1)],
            settings=_settings(),
        )
        assert authorities[-1] == (None, False)

    asyncio.run(run())


def test_early_life360_update_schedules_lease_refresh_at_silence_boundary(
    controller_module,
    monkeypatch,
):
    """A fresh early observation must not leave the worker lease to expire."""

    async def run():
        monkeypatch.setattr(
            controller_module,
            "MAX_SEMANTIC_SILENCE_SECONDS",
            0.02,
        )
        fake = FakeRequester()
        now = [NOW]
        controller = controller_module.DesiredStateController(
            object(),
            producer_id="homeassistant.storm-tracker",
            boot_id="boot-test-a",
            requester=fake,
            clock=lambda: now[0],
            debounce_seconds=0,
            request_timeout=5.0,
        )
        await controller.async_start(targets=[_target()], settings=_settings())

        now[0] += 0.001
        assert not await controller.async_update(
            targets=[_target(observed_at=now[0])],
            settings=_settings(),
        )

        now[0] = NOW + 0.02
        await asyncio.sleep(0.04)
        await controller.async_flush()

        applies = [
            call for call in fake.calls if call["job_type"] == "desired_state_apply"
        ]
        assert len(applies) == 2
        refreshed = applies[-1]["payload"]["desired_state"]
        assert refreshed["generation"] == 2
        assert refreshed["targets"][0]["position_observed_at"] == NOW + 0.001
        assert controller.diagnostics()["counters"]["lease_refreshes"] == 1
        await controller.async_stop()

    asyncio.run(run())


def test_life360_meaningful_movement_or_availability_change_republishes(
    controller_module,
):
    async def run():
        fake = FakeRequester()
        controller = _controller(controller_module, fake, debounce=0.0)
        await controller.async_start(targets=[_target()], settings=_settings())

        assert await controller.async_update(
            targets=[_target(latitude=51.06, observed_at=NOW + 180)],
            settings=_settings(),
        )
        await controller.async_flush()
        assert await controller.async_update(
            targets=[
                _target(
                    latitude=51.06,
                    observed_at=NOW + 360,
                    available=False,
                )
            ],
            settings=_settings(),
        )
        await controller.async_flush()

        applies = [
            call for call in fake.calls if call["job_type"] == "desired_state_apply"
        ]
        assert len(applies) == 3
        assert applies[1]["payload"]["desired_state"]["targets"][0][
            "latitude"
        ] == 51.06
        assert applies[2]["payload"]["desired_state"]["targets"][0][
            "available"
        ] is False

    asyncio.run(run())


def test_update_arriving_during_apply_is_serialised_as_one_follow_up(
    controller_module,
):
    async def run():
        fake = FakeRequester()
        fake.block_job_type = "desired_state_apply"
        controller = _controller(controller_module, fake, debounce=0.0)
        start = asyncio.create_task(
            controller.async_start(targets=[_target()], settings=_settings())
        )
        await asyncio.wait_for(fake.block_started.wait(), timeout=1)
        assert await controller.async_update(
            targets=[_target(latitude=51.05, observed_at=NOW + 1)],
            settings=_settings(),
        )
        fake.block_release.set()
        await asyncio.wait_for(start, timeout=1)

        applies = [
            call for call in fake.calls if call["job_type"] == "desired_state_apply"
        ]
        assert len(applies) == 2
        first = applies[0]["payload"]["desired_state"]
        second = applies[1]["payload"]["desired_state"]
        first_sha = hashlib.sha256(
            json.dumps(
                first, ensure_ascii=False, separators=(",", ":"), sort_keys=True
            ).encode("utf-8")
        ).hexdigest()
        assert second["previous_state_sha256"] == first_sha
        assert second["generation"] == 2
        assert second["targets"][0]["latitude"] == 51.05
        assert controller.diagnostics()["dirty"] is False

    asyncio.run(run())


def test_authority_callback_never_exposes_intermediate_obsolete_head(
    controller_module,
):
    async def run():
        fake = FakeRequester()
        fake.block_job_type = "desired_state_apply"
        authorities: list[str | None] = []
        controller = _controller(
            controller_module,
            fake,
            authority_callback=lambda value, **_kwargs: authorities.append(value),
        )
        start = asyncio.create_task(
            controller.async_start(targets=[_target()], settings=_settings())
        )
        await asyncio.wait_for(fake.block_started.wait(), timeout=1)
        assert await controller.async_update(
            targets=[_target(latitude=51.05, observed_at=NOW + 1)],
            settings=_settings(),
        )
        fake.block_release.set()
        await asyncio.wait_for(start, timeout=1)

        non_null = [value for value in authorities if value is not None]
        assert non_null == [controller.head_state_sha256]
        assert authorities[-1] == controller.head_state_sha256
        assert controller.diagnostics()["dirty"] is False

    asyncio.run(run())


def test_offline_keeps_one_dirty_snapshot_and_reconnect_retries(controller_module):
    async def run():
        fake = FakeRequester()
        fake.offline = True
        controller = _controller(controller_module, fake)
        await controller.async_start(targets=[_target()], settings=_settings())
        first_job_id = fake.calls[0]["job_id"]
        diagnostics = controller.diagnostics()
        assert diagnostics["state"] == "offline"
        assert diagnostics["dirty"] is True
        assert diagnostics["in_flight"] is False
        assert len(fake.calls) == 1

        fake.offline = False
        await controller.async_worker_reconnected()
        assert fake.calls[1]["job_type"] == "desired_state_claim"
        assert fake.calls[1]["job_id"] == first_job_id
        assert controller.diagnostics()["dirty"] is False
        assert controller.diagnostics()["counters"]["reconnects"] == 1

    asyncio.run(run())


def test_clean_reconnect_reproves_head_and_repairs_fresh_worker_store(
    controller_module,
):
    async def run():
        fake = FakeRequester()
        controller = _controller(controller_module, fake)
        await controller.async_start(targets=[_target()], settings=_settings())
        first_head = controller.head_state_sha256
        assert first_head is not None
        assert controller.diagnostics()["dirty"] is False

        await controller.async_worker_reconnected()
        assert [call["job_type"] for call in fake.calls[-2:]] == [
            "desired_state_claim",
            "desired_state_snapshot",
        ]
        assert len([
            call for call in fake.calls
            if call["job_type"] == "desired_state_apply"
        ]) == 1

        fake.epoch += 1
        fake.claim_id = None
        fake.head = _unconfigured_head()
        await controller.async_worker_reconnected()

        applies = [
            call for call in fake.calls
            if call["job_type"] == "desired_state_apply"
        ]
        assert len(applies) == 2
        repaired = applies[-1]["payload"]["desired_state"]
        assert repaired["previous_state_sha256"] is None
        assert repaired["generation"] == 1
        assert controller.head_state_sha256 != first_head
        assert controller.diagnostics()["dirty"] is False

    asyncio.run(run())


def test_uncertain_apply_reuses_identical_document_and_job_id_on_reconnect(
    controller_module,
):
    async def run():
        fake = FakeRequester()
        fake.apply_failures.append(TimeoutError("missing receipt"))
        controller = _controller(controller_module, fake)
        await controller.async_start(targets=[_target()], settings=_settings())
        assert controller.diagnostics()["dirty"] is True
        first_apply = next(
            call for call in fake.calls if call["job_type"] == "desired_state_apply"
        )

        await controller.async_worker_reconnected()
        applies = [
            call for call in fake.calls if call["job_type"] == "desired_state_apply"
        ]
        assert len(applies) == 2
        assert applies[1]["job_id"] == first_apply["job_id"]
        assert applies[1]["payload"] == first_apply["payload"]
        assert controller.diagnostics()["dirty"] is False

    asyncio.run(run())


def test_cas_conflict_refreshes_snapshot_and_rebuilds_once(controller_module):
    async def run():
        first_head = "a" * 64
        replacement_head = "b" * 64
        fake = FakeRequester(
            epoch=10,
            head=_configured_head(first_head, producer="previous-ha", epoch=9),
        )
        fake.apply_failures.append(
            RuntimeError("Workerjob failed: desired-state bouwt niet op de actieve SHA-256 voort")
        )

        def move_head(_document):
            fake.head = _configured_head(
                replacement_head, producer="concurrent-ha", epoch=8, generation=12
            )

        fake.on_apply_failure = move_head
        controller = _controller(controller_module, fake)
        await controller.async_start(targets=[_target()], settings=_settings())

        assert [call["job_type"] for call in fake.calls] == [
            "desired_state_claim",
            "desired_state_snapshot",
            "desired_state_apply",
            "desired_state_snapshot",
            "desired_state_apply",
        ]
        snapshots = [
            call for call in fake.calls if call["job_type"] == "desired_state_snapshot"
        ]
        assert snapshots[0]["job_id"] != snapshots[1]["job_id"]
        last = fake.calls[-1]["payload"]["desired_state"]
        assert last["previous_state_sha256"] == replacement_head
        assert last["authority_epoch"] == 10
        assert last["generation"] == 1
        assert controller.diagnostics()["counters"]["cas_refreshes"] == 1

    asyncio.run(run())


def test_raw_capture_conflict_code_is_recognized_as_cas(controller_module):
    assert controller_module._is_cas_conflict(RuntimeError(
        "Workerjob failed: raw_capture_desired_state_conflict"
    ))


def test_stale_positions_cannot_regress_and_unavailable_keeps_last_known_position(
    controller_module,
):
    async def run():
        fake = FakeRequester()
        controller = _controller(controller_module, fake)
        await controller.async_start(targets=[_target()], settings=_settings())
        await controller.async_update(
            targets=[_target(latitude=51.06, observed_at=NOW + 10)],
            settings=_settings(),
        )
        await controller.async_flush()

        assert not await controller.async_update(
            targets=[_target(latitude=49.0, observed_at=NOW + 5)],
            settings=_settings(),
        )
        assert await controller.async_update(
            targets=[
                _target(
                    latitude=0.0,
                    longitude=0.0,
                    observed_at=None,
                    available=False,
                )
            ],
            settings=_settings(),
        )
        await controller.async_flush()
        last = [
            call for call in fake.calls if call["job_type"] == "desired_state_apply"
        ][-1]["payload"]["desired_state"]["targets"][0]
        assert last["available"] is False
        assert last["latitude"] == 51.06
        assert last["longitude"] == 4.48
        assert last["position_observed_at"] == NOW + 10
        assert (
            controller.diagnostics()["counters"]["regressive_positions_filtered"]
            == 2
        )

    asyncio.run(run())


def test_stop_cancels_inflight_request_and_leaves_no_controller_task(
    controller_module,
):
    async def run():
        fake = FakeRequester()
        fake.block_job_type = "desired_state_claim"
        controller = _controller(controller_module, fake)
        start = asyncio.create_task(
            controller.async_start(targets=[_target()], settings=_settings())
        )
        await asyncio.wait_for(fake.block_started.wait(), timeout=1)
        await controller.async_stop()
        await asyncio.gather(start, return_exceptions=True)
        diagnostics = controller.diagnostics()
        assert diagnostics["state"] == "stopped"
        assert diagnostics["stopped"] is True
        assert diagnostics["in_flight"] is False
        assert diagnostics["counters"]["cancellations"] == 1
        await controller.async_unload()

    asyncio.run(run())


def test_diagnostics_are_fixed_size_and_never_retain_payload_or_error_text(
    controller_module,
):
    async def run():
        calls = []

        async def fail(*args, **kwargs):
            calls.append({"job_type": args[1], "job_id": kwargs["job_id"]})
            raise RuntimeError(
                "worker offline nabij 51.040000,4.480000 voor device_tracker.wim"
            )

        controller = controller_module.DesiredStateController(
            object(),
            producer_id="homeassistant.storm-tracker",
            boot_id="boot-test-a",
            requester=fail,
            clock=lambda: NOW,
            debounce_seconds=0,
        )
        await controller.async_start(targets=[_target()], settings=_settings())
        diagnostics = controller.diagnostics()
        encoded = json.dumps(diagnostics, sort_keys=True)
        assert len(encoded.encode("utf-8")) < 4096
        assert "51.04" not in encoded
        assert "4.48" not in encoded
        assert "device_tracker" not in encoded
        assert "targets" not in encoded
        assert diagnostics["last_error_code"] == "worker_offline"
        assert set(diagnostics["counters"]) == {
            "accepted_updates",
            "coalesced_updates",
            "lease_refreshes",
            "regressive_positions_filtered",
            "worker_requests",
            "claim_requests",
            "snapshot_requests",
            "apply_requests",
            "applied_states",
            "recovered_receipts",
            "cas_refreshes",
            "reconnects",
            "failures",
            "cancellations",
        }

    asyncio.run(run())


def test_65_targets_fail_before_any_worker_request(controller_module):
    async def run():
        fake = FakeRequester()
        controller = _controller(controller_module, fake)
        targets = [
            {
                **_target(target_id=f"device_tracker.t{index:02d}"),
                "display_name": f"T{index:02d}",
            }
            for index in range(65)
        ]
        with pytest.raises(ValueError, match="64|lijstgrens"):
            await controller.async_start(targets=targets, settings=_settings())
        assert fake.calls == []

    asyncio.run(run())


def test_ast_import_isolation_and_absence_of_registration_or_io_api():
    tree = ast.parse(MODULE_PATH.read_text(encoding="utf-8"))
    imported = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            imported.append(("." * node.level) + (node.module or ""))
    forbidden_roots = {
        "aiohttp",
        "custom_components",
        "mqtt",
        "pathlib",
        "requests",
        "socket",
        "sqlite3",
        "storm_tracker_worker",
        "urllib",
    }
    assert not {
        name.lstrip(".").split(".")[0]
        for name in imported
    } & forbidden_roots
    assert "homeassistant.core" in imported
    assert ".desired_state_builder" in imported
    assert ".worker_bridge" in imported
    assert not any(
        name.startswith("homeassistant.") and name != "homeassistant.core"
        for name in imported
    )
    calls = {
        node.func.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    }
    assert calls.isdisjoint({
        "open",
        "print",
        "async_listen",
        "async_track_state_change_event",
        "async_write_ha_state",
    })
