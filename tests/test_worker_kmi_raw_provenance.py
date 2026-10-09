from __future__ import annotations

import hashlib
import io
import json
from types import SimpleNamespace

from PIL import Image
import pytest

from storm_tracker_worker import artifacts, provider_radar
from storm_tracker_worker.raw_artifacts import (
    RawArtifactCapacityError,
    RawArtifactIntegrityError,
    RawArtifactLimits,
    RawArtifactStore,
)


FRAME_TIME = "2026-07-30T12:00:00+00:00"
URI_SECRET = "never-persist-this-query-secret"
FRAME_URI = (
    "https://app.meteo.be/rendered/animation/frame-001.png"
    f"?k={URI_SECRET}&token=also-secret"
)


def _forecast_bytes() -> bytes:
    return (
        "{\n"
        '  "animation": {"sequence": ['
        f'{{"time": "{FRAME_TIME}", "uri": "{FRAME_URI}"}}'
        "]},\n"
        '  "obs": {"ww": "light rain", "timestamp": 1785412800, "temp": 17.2}\n'
        "}\n"
    ).encode("utf-8")


def _png_bytes() -> bytes:
    stream = io.BytesIO()
    image = Image.new("RGBA", (3, 2), (0, 0, 0, 0))
    image.putdata([
        (0, 200, 100, 255),
        (0, 200, 100, 255),
        (0, 0, 0, 0),
        (0, 200, 100, 255),
        (0, 0, 0, 0),
        (0, 0, 0, 0),
    ])
    image.save(stream, format="PNG")
    return stream.getvalue()


def _response(payload: bytes, content_type: str, received_at: float):
    return provider_radar._KmiHttpResponse(
        payload=payload,
        content_type=content_type,
        received_at=received_at,
    )


def _job(job_id: str = "kmi-slot-001"):
    return SimpleNamespace(
        job_id=job_id,
        payload={"center_lat": 51.05, "center_lon": 4.40},
    )


def _install_responses(monkeypatch, responses):
    queue = iter(responses)
    calls: list[str] = []

    def download(url: str, *, maximum_bytes: int, headers: dict[str, str]):
        assert maximum_bytes > 0
        assert headers["User-Agent"] == "be.meteo.app"
        calls.append(url)
        return next(queue)

    monkeypatch.setattr(provider_radar, "_download_kmi_response", download)
    return calls


def _artifact_root(monkeypatch, tmp_path):
    root = tmp_path / "transfer"
    monkeypatch.setattr(artifacts, "ARTIFACT_ROOT", root)
    return root


def test_kmi_capture_is_stored_byte_exact_before_both_decoders(monkeypatch, tmp_path):
    forecast_payload = _forecast_bytes()
    image_payload = _png_bytes()
    store = RawArtifactStore(tmp_path / "raw")
    _artifact_root(monkeypatch, tmp_path)
    calls = _install_responses(monkeypatch, [
        _response(forecast_payload, "application/json", 1_800_000_000.25),
        _response(image_payload, "image/png", 1_800_000_001.50),
    ])

    real_json_loads = json.loads
    real_image_rgba = provider_radar._image_rgba
    decode_order: list[str] = []

    def checked_json_loads(value: str):
        stats = store.stats()
        assert stats["object_count"] == 1
        assert stats["receipt_count"] == 1
        assert stats["capture_count"] == 0
        objects = list((tmp_path / "raw" / "objects").glob("*.raw"))
        assert len(objects) == 1
        assert objects[0].read_bytes() == forecast_payload
        decode_order.append("forecast_json")
        return real_json_loads(value)

    def checked_image_rgba(value: bytes):
        stats = store.stats()
        assert stats["object_count"] == 2
        assert stats["receipt_count"] == 2
        assert stats["capture_count"] == 1
        assert value == image_payload
        assert image_payload in {
            path.read_bytes()
            for path in (tmp_path / "raw" / "objects").glob("*.raw")
        }
        decode_order.append("radar_png")
        return real_image_rgba(value)

    monkeypatch.setattr(
        provider_radar,
        "json",
        SimpleNamespace(loads=checked_json_loads),
    )
    monkeypatch.setattr(provider_radar, "_image_rgba", checked_image_rgba)

    metadata = provider_radar.kmi_fetch(_job(), raw_store=store)
    decoded = artifacts.read_json_artifact(metadata)

    assert decode_order == ["forecast_json", "radar_png"]
    assert len(calls) == 2
    assert decoded["frame_timestamp"] != 1_800_000_000.25
    assert decoded["raw_provenance"]["product_timestamp"] == decoded["frame_timestamp"]
    sources = decoded["raw_provenance"]["raw_sources"]
    assert store.read_bytes(sources["forecast_json"]["raw_receipt_id"]) == forecast_payload
    assert store.read_bytes(sources["radar_png"]["raw_receipt_id"]) == image_payload
    assert sources["forecast_json"]["raw_content_sha256"] == hashlib.sha256(
        forecast_payload
    ).hexdigest()
    assert sources["radar_png"]["raw_content_sha256"] == hashlib.sha256(
        image_payload
    ).hexdigest()
    assert sources["forecast_json"]["received_at"] == 1_800_000_000.25
    assert sources["radar_png"]["received_at"] == 1_800_000_001.50
    assert sources["forecast_json"]["parser_version"] == (
        provider_radar.KMI_FORECAST_PARSER_VERSION
    )
    assert sources["radar_png"]["parser_version"] == (
        provider_radar.KMI_IMAGE_PARSER_VERSION
    )
    assert all(
        source["collector_version"] == provider_radar.KMI_RAW_COLLECTOR_VERSION
        for source in sources.values()
    )

    nodes = [metadata, decoded, decoded["overlay"], *decoded["observations"]]
    for node in nodes:
        assert node["provenance_grade"] == "exact_raw"
        assert node["provenance_scope"] == "upstream_bytes_only"
        assert node["raw_provenance"]["raw_capture_id"] == (
            decoded["raw_provenance"]["raw_capture_id"]
        )
        assert set(node["raw_provenance"]["raw_sources"]) == {
            "forecast_json",
            "radar_png",
        }

    assert decoded["product"] == "kmi_appv4_visualisation"
    assert decoded["semantic_class"] == "categorical_rendered_image"
    assert decoded["quantitative_semantics_eligible"] is False
    assert decoded["operational_use"] == "visualisation_only"
    assert decoded["operational_admission_eligible"] is False
    assert decoded["admission_status"] == "uncertified_visualisation_only"
    assert decoded["legacy_decoder_quality_constant"] == 0.95
    assert decoded["legacy_decoder_quality_operational"] is False
    assert decoded["overlay"]["semantic_class"] == "categorical_rendered_image"
    assert decoded["overlay"]["operational_use"] == "visualisation_only"
    assert decoded["overlay"]["operational_admission_eligible"] is False
    assert decoded["overlay"]["runs"]
    assert decoded["observations"] == []
    assert metadata["observation_count"] == 0

    bridge = decoded["radar_cycle_bridge"]
    assert bridge["status"] == "not_bridged_linked_capture"
    assert bridge["required_representation"] == "raw_capture_set"
    assert bridge["raw_capture_id"] == decoded["raw_provenance"]["raw_capture_id"]
    assert bridge["single_receipt_complete_set"] is False
    assert bridge["exact_raw_artifacts_complete_in_v1"] is False
    assert set(bridge["member_receipt_ids"]) == {
        source["raw_receipt_id"] for source in sources.values()
    }

    persisted = json.dumps(
        {
            "metadata": metadata,
            "decoded": decoded,
            "receipts": {
                role: store.load_receipt(source["raw_receipt_id"]).to_metadata()
                for role, source in sources.items()
            },
        },
        sort_keys=True,
    ).lower()
    assert URI_SECRET.lower() not in persisted
    assert "also-secret" not in persisted
    assert provider_radar.KMI_SECRET.lower() not in persisted
    assert "?k=" not in persisted
    assert "radqpe" not in persisted
    assert "reflectivity" not in persisted
    assert "mm/h" not in persisted
    assert '"quality": 0.95' not in persisted
    assert "uri" not in decoded["diagnostics"]
    assert all("?" not in source["upstream_record_id"] for source in sources.values())
    assert URI_SECRET.encode("utf-8") not in (
        tmp_path / "raw" / "raw_artifacts.sqlite3"
    ).read_bytes()
    assert store.integrity_check()["healthy"] is True


def test_kmi_raw_capacity_failure_stops_before_json_or_image_decode(
    monkeypatch, tmp_path,
):
    forecast_payload = _forecast_bytes()
    store = RawArtifactStore(
        tmp_path / "raw",
        limits=RawArtifactLimits(max_object_bytes=8, max_total_bytes=16),
    )
    _artifact_root(monkeypatch, tmp_path)
    calls = _install_responses(monkeypatch, [
        _response(forecast_payload, "application/json", 1_800_000_000.25),
    ])
    monkeypatch.setattr(
        provider_radar,
        "json",
        SimpleNamespace(loads=lambda _value: pytest.fail("JSON decode uitgevoerd")),
    )
    monkeypatch.setattr(
        provider_radar,
        "_image_rgba",
        lambda _value: pytest.fail("beelddecode uitgevoerd"),
    )

    with pytest.raises(RawArtifactCapacityError):
        provider_radar.kmi_fetch(_job(), raw_store=store)

    assert len(calls) == 1
    assert store.stats()["object_count"] == 0
    assert store.stats()["receipt_count"] == 0
    assert store.stats()["capture_count"] == 0
    assert not (tmp_path / "transfer").exists()


class _FailFirstImageReceipt:
    def __init__(self, store: RawArtifactStore) -> None:
        self.store_backend = store
        self.failed = False

    def initialize(self):
        return self.store_backend.initialize()

    def store(self, **kwargs):
        if (
            kwargs["product"] == provider_radar.KMI_IMAGE_PRODUCT
            and not self.failed
        ):
            self.failed = True
            raise RawArtifactIntegrityError("simulated image-receipt failure")
        return self.store_backend.store(**kwargs)

    def commit_capture(self, **kwargs):
        return self.store_backend.commit_capture(**kwargs)


def test_kmi_crash_between_members_is_visible_and_retry_completes_capture(
    monkeypatch, tmp_path,
):
    forecast_payload = _forecast_bytes()
    image_payload = _png_bytes()
    store = RawArtifactStore(tmp_path / "raw")
    failing_store = _FailFirstImageReceipt(store)
    _artifact_root(monkeypatch, tmp_path)
    _install_responses(monkeypatch, [
        _response(forecast_payload, "application/json", 1_800_000_000.25),
        _response(image_payload, "image/png", 1_800_000_001.50),
        _response(forecast_payload, "application/json", 1_800_000_600.75),
        _response(image_payload, "image/png", 1_800_000_601.25),
    ])
    real_image_rgba = provider_radar._image_rgba
    image_decodes: list[bytes] = []

    def counted_image_rgba(value: bytes):
        image_decodes.append(value)
        return real_image_rgba(value)

    monkeypatch.setattr(provider_radar, "_image_rgba", counted_image_rgba)

    with pytest.raises(
        RawArtifactIntegrityError, match="simulated image-receipt failure"
    ):
        provider_radar.kmi_fetch(_job(), raw_store=failing_store)

    assert image_decodes == []
    assert store.stats()["object_count"] == 1
    assert store.stats()["receipt_count"] == 1
    assert store.stats()["capture_count"] == 0
    assert not (tmp_path / "transfer").exists()

    metadata = provider_radar.kmi_fetch(_job(), raw_store=store)
    decoded = artifacts.read_json_artifact(metadata)
    sources = decoded["raw_provenance"]["raw_sources"]

    assert image_decodes == [image_payload]
    assert store.stats()["object_count"] == 2
    assert store.stats()["receipt_count"] == 2
    assert store.stats()["capture_count"] == 1
    assert sources["forecast_json"]["received_at"] == 1_800_000_000.25
    assert sources["radar_png"]["received_at"] == 1_800_000_601.25
    assert store.integrity_check()["healthy"] is True


def test_kmi_same_job_retry_is_idempotent_and_next_slot_is_a_new_fetch(
    monkeypatch, tmp_path,
):
    forecast_payload = _forecast_bytes()
    image_payload = _png_bytes()
    store = RawArtifactStore(tmp_path / "raw")
    _artifact_root(monkeypatch, tmp_path)
    _install_responses(monkeypatch, [
        _response(forecast_payload, "application/json", 1_800_000_000.25),
        _response(image_payload, "image/png", 1_800_000_001.50),
        _response(forecast_payload, "application/json", 1_800_000_600.75),
        _response(image_payload, "image/png", 1_800_000_601.25),
        _response(forecast_payload, "application/json", 1_800_000_900.50),
        _response(image_payload, "image/png", 1_800_000_901.00),
    ])

    first = provider_radar.kmi_fetch(_job("kmi-slot-001"), raw_store=store)
    retry = provider_radar.kmi_fetch(_job("kmi-slot-001"), raw_store=store)
    next_slot = provider_radar.kmi_fetch(_job("kmi-slot-002"), raw_store=store)
    first_decoded = artifacts.read_json_artifact(first)
    retry_decoded = artifacts.read_json_artifact(retry)
    next_decoded = artifacts.read_json_artifact(next_slot)

    first_raw = first_decoded["raw_provenance"]
    retry_raw = retry_decoded["raw_provenance"]
    next_raw = next_decoded["raw_provenance"]
    assert retry_raw == first_raw
    assert retry["artifact_sha256"] == first["artifact_sha256"]
    assert retry_raw["raw_capture_id"] == first_raw["raw_capture_id"]
    assert next_raw["raw_capture_id"] != first_raw["raw_capture_id"]
    for role in ("forecast_json", "radar_png"):
        assert retry_raw["raw_sources"][role]["raw_receipt_id"] == (
            first_raw["raw_sources"][role]["raw_receipt_id"]
        )
        assert next_raw["raw_sources"][role]["raw_receipt_id"] != (
            first_raw["raw_sources"][role]["raw_receipt_id"]
        )
        assert next_raw["raw_sources"][role]["raw_content_sha256"] == (
            first_raw["raw_sources"][role]["raw_content_sha256"]
        )
    assert first_raw["raw_sources"]["forecast_json"]["received_at"] == (
        1_800_000_000.25
    )
    assert retry_raw["raw_sources"]["forecast_json"]["received_at"] == (
        1_800_000_000.25
    )
    assert next_raw["raw_sources"]["forecast_json"]["received_at"] == (
        1_800_000_900.50
    )
    assert store.stats()["object_count"] == 2
    assert store.stats()["receipt_count"] == 4
    assert store.stats()["capture_count"] == 2
    assert store.integrity_check()["healthy"] is True
