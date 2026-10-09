from __future__ import annotations

import ast
from dataclasses import replace
import hashlib
import json
from pathlib import Path
import subprocess
import sys
from typing import Any, Callable

import pytest

from storm_tracker_worker.offline_replay_acceptance import (
    ArtifactRole,
    BaselineId,
    ScienceStage,
)
import storm_tracker_worker.offline_replay_nowcast_runner as runner


ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "tests/fixtures/offline_replay_nowcast_baseline_input.json"


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")


def _fixture_document() -> dict[str, Any]:
    return json.loads(FIXTURE.read_text(encoding="utf-8"))


def _document_bytes(value: dict[str, Any]) -> bytes:
    return _canonical_bytes(value)


def _refresh_raw_manifest(document: dict[str, Any]) -> None:
    replay_input = document["offline_replay_input"]
    manifest_sha = hashlib.sha256(
        _canonical_bytes(replay_input["frames"])
    ).hexdigest()
    artifact = next(
        item
        for item in replay_input["artifacts"]
        if item["role"] == ArtifactRole.RAW_INPUT_MANIFEST.value
    )
    artifact["sha256"] = manifest_sha


def test_fixture_executes_every_pair_and_keeps_partial_authority_false() -> None:
    receipt = runner.run_nowcast_baseline_primary_repeat_file(FIXTURE)
    document = receipt.to_mapping()
    output = document["primary_output"]
    baseline_outputs = output["baseline_outputs"]
    eulerian = baseline_outputs[BaselineId.EULERIAN_PERSISTENCE.value]
    translation = baseline_outputs[BaselineId.TWO_FRAME_TRANSLATION.value]

    assert receipt.primary_repeat_passed is True
    assert receipt.stage_evidence.stage is ScienceStage.NOWCAST_BASELINES
    assert receipt.stage_evidence.completed is True
    assert receipt.stage_evidence.reproducible is True
    assert receipt.stage_evidence.frame_count == 20
    assert receipt.stage_evidence.event_ids == ("nowcast-event-a",)
    assert eulerian["forecast_count"] == 19
    assert translation["forecast_count"] == 19
    assert len(eulerian["forecasts"]) == 19
    assert len(translation["forecasts"]) == 19
    assert eulerian["forecasts"][0]["support_frame_ids"] == [
        "nowcast-frame-000",
        "nowcast-frame-001",
    ]
    assert eulerian["forecasts"][-1]["support_frame_ids"] == [
        "nowcast-frame-018",
        "nowcast-frame-019",
    ]

    final_eulerian = eulerian["forecasts"][-1]["forecast"]
    final_translation = translation["forecasts"][-1]
    assert final_eulerian["values"] == [[0.0, 10.0, 0.0]]
    assert final_eulerian["valid_mask"] == [[True, True, True]]
    assert final_translation["forecast"]["values"] == [[0.0, 0.0, 10.0]]
    assert final_translation["forecast"]["valid_mask"] == [
        [False, True, True]
    ]
    assert final_translation["motion"]["vx_m_s"] == pytest.approx(1000.0 / 300.0)
    assert final_translation["motion"]["vy_m_s"] == 0.0
    assert final_translation["motion"]["speed_kmh"] == pytest.approx(12.0)

    assert tuple(item.baseline_id for item in receipt.baseline_evidence) == (
        BaselineId.EULERIAN_PERSISTENCE,
        BaselineId.TWO_FRAME_TRANSLATION,
    )
    assert all(item.completed for item in receipt.baseline_evidence)
    assert all(item.reproducible for item in receipt.baseline_evidence)
    assert all(item.frame_count == 20 for item in receipt.baseline_evidence)
    assert receipt.remaining_baselines == runner.REMAINING_BASELINES
    assert receipt.blocking_reasons == tuple(
        sorted(
            f"baseline_not_executed:{item.value}"
            for item in runner.REMAINING_BASELINES
        )
    )
    assert receipt.real_meteorological_case is False
    assert receipt.offline_science_gate_passed is False
    assert receipt.meteorological_admission is False
    assert receipt.live_wiring_allowed is False
    assert receipt.dashboard_publication_allowed is False
    assert receipt.home_assistant_state_mutated is False


def test_fixture_is_content_addressed_and_repeat_receipt_is_stable() -> None:
    body = FIXTURE.read_bytes()
    first = runner.run_nowcast_baseline_primary_repeat(body)
    second = runner.run_nowcast_baseline_primary_repeat(body)

    assert first == second
    assert first.sha256 == second.sha256
    assert first.replay_pack_sha256 == hashlib.sha256(body).hexdigest()
    assert first.primary_output_sha256 == first.reproduction_output_sha256
    assert len(first.sha256) == 64
    assert (
        first.to_mapping()["nowcast_replay_receipt_sha256"]
        == first.sha256
    )


def test_runner_calls_real_nowcast_functions_for_both_full_executions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = {"derive": 0, "eulerian": 0, "translation": 0}

    def counted(
        name: str,
        function: Callable[..., Any],
    ) -> Callable[..., Any]:
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            calls[name] += 1
            return function(*args, **kwargs)

        return wrapper

    monkeypatch.setattr(
        runner,
        "derive_two_frame_constant_motion",
        counted("derive", runner.derive_two_frame_constant_motion),
    )
    monkeypatch.setattr(
        runner,
        "eulerian_persistence",
        counted("eulerian", runner.eulerian_persistence),
    )
    monkeypatch.setattr(
        runner,
        "lagrangian_constant_motion",
        counted("translation", runner.lagrangian_constant_motion),
    )

    receipt = runner.run_nowcast_baseline_primary_repeat(FIXTURE.read_bytes())

    assert receipt.primary_repeat_passed is True
    assert calls == {"derive": 38, "eulerian": 38, "translation": 38}


def test_tampered_payload_and_raw_manifest_fail_closed() -> None:
    payload_tamper = _fixture_document()
    payload_tamper["frames"][0]["payload_base64"] = "AAAA"
    with pytest.raises(ValueError, match="payload SHA"):
        runner.run_nowcast_baseline_primary_repeat(
            _document_bytes(payload_tamper)
        )

    manifest_tamper = _fixture_document()
    artifact = next(
        item
        for item in manifest_tamper["offline_replay_input"]["artifacts"]
        if item["role"] == ArtifactRole.RAW_INPUT_MANIFEST.value
    )
    artifact["sha256"] = "0" * 64
    with pytest.raises(ValueError, match="raw input manifest"):
        runner.run_nowcast_baseline_primary_repeat(
            _document_bytes(manifest_tamper)
        )


def test_duplicate_keys_bool_number_and_source_reuse_fail_closed() -> None:
    body = FIXTURE.read_bytes().lstrip()
    duplicate = b'{"contract_version":"forged",' + body[1:]
    with pytest.raises(ValueError, match="duplicate JSON key"):
        runner.run_nowcast_baseline_primary_repeat(duplicate)

    bool_number = _fixture_document()
    bool_number["lead_time_seconds"] = True
    with pytest.raises(TypeError, match="must be numeric"):
        runner.run_nowcast_baseline_primary_repeat(
            _document_bytes(bool_number)
        )

    reused_source = _fixture_document()
    sources = reused_source["baseline_preregistration_source_ids"]
    sources[BaselineId.TWO_FRAME_TRANSLATION.value] = sources[
        BaselineId.EULERIAN_PERSISTENCE.value
    ]
    with pytest.raises(ValueError, match="distinct source id"):
        runner.run_nowcast_baseline_primary_repeat(
            _document_bytes(reused_source)
        )


def test_minimum_pack_and_per_event_pair_coverage_are_enforced() -> None:
    too_short = _fixture_document()
    too_short["frames"] = too_short["frames"][:-1]
    replay_frames = too_short["offline_replay_input"]["frames"]
    too_short["offline_replay_input"]["frames"] = replay_frames[:-1]
    with pytest.raises(ValueError, match="at least twenty"):
        runner.run_nowcast_baseline_primary_repeat(_document_bytes(too_short))

    singleton_event = _fixture_document()
    replay_frames = singleton_event["offline_replay_input"]["frames"]
    replay_frames[-1]["event_id"] = "nowcast-event-z"
    _refresh_raw_manifest(singleton_event)
    with pytest.raises(ValueError, match="each event requires at least two"):
        runner.run_nowcast_baseline_primary_repeat(
            _document_bytes(singleton_event)
        )


def test_canonical_output_has_a_hard_byte_bound(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(runner, "MAXIMUM_CANONICAL_OUTPUT_BYTES", 1)

    with pytest.raises(
        runner.OfflineNowcastReplayError,
        match="output exceeds hard byte bound",
    ):
        runner.run_nowcast_baseline_primary_repeat(FIXTURE.read_bytes())


def test_receipt_cannot_be_upgraded_or_detached_from_evidence() -> None:
    receipt = runner.run_nowcast_baseline_primary_repeat(FIXTURE.read_bytes())

    with pytest.raises(ValueError, match="may never grant"):
        replace(receipt, live_wiring_allowed=True)
    with pytest.raises(ValueError, match="stage evidence provenance"):
        replace(
            receipt,
            stage_evidence=replace(
                receipt.stage_evidence,
                input_contract_sha256="0" * 64,
            ),
        )
    detached = replace(
        receipt.baseline_evidence[0],
        preregistration_source_id="forged-source",
    )
    with pytest.raises(ValueError, match="baseline evidence provenance"):
        replace(
            receipt,
            baseline_evidence=(detached, receipt.baseline_evidence[1]),
        )


def test_cli_emits_one_non_admitting_canonical_receipt() -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-I",
            "-B",
            str(ROOT / "scripts/run_offline_replay_nowcast_baselines.py"),
            "--replay-pack",
            str(FIXTURE),
            "--worker-package",
            str(ROOT / "storm_tracker_worker"),
        ],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=30,
    )

    assert result.stderr == ""
    assert result.stdout.endswith("\n")
    assert result.stdout.count("\n") == 1
    document = json.loads(result.stdout)
    canonical_line = _canonical_bytes(document).decode("ascii") + "\n"
    assert result.stdout == canonical_line
    assert document["primary_repeat_passed"] is True
    assert document["offline_science_gate_passed"] is False
    assert document["meteorological_admission"] is False
    assert document["live_wiring_allowed"] is False
    assert document["dashboard_publication_allowed"] is False
    assert document["home_assistant_state_mutated"] is False


def test_runner_is_inert_and_not_imported_by_live_runtime() -> None:
    module_path = ROOT / "storm_tracker_worker/offline_replay_nowcast_runner.py"
    tree = ast.parse(module_path.read_text(encoding="utf-8"))
    imported_roots = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_roots.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_roots.add(node.module.split(".")[0])
    assert imported_roots.isdisjoint(
        {"aiohttp", "homeassistant", "paho", "requests"}
    )

    for live_file in (
        ROOT / "storm_tracker_worker/service.py",
        ROOT / "storm_tracker_worker/tracking_core/__init__.py",
        ROOT / "custom_components/storm_tracker_v3/__init__.py",
        ROOT / "custom_components/storm_tracker_v3/sensor.py",
    ):
        assert "offline_replay_nowcast_runner" not in live_file.read_text(
            encoding="utf-8"
        )
