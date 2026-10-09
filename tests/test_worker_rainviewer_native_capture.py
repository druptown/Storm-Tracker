from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
import io
import json
import struct
import threading
import time
from types import SimpleNamespace
from urllib.parse import urlsplit
import zlib

import pytest

from storm_tracker_worker import rainviewer_native
from storm_tracker_worker.raw_artifacts import RawArtifactStore


FRAME_TIME = 1_800_000_000
MANIFEST_TIME = FRAME_TIME + 75
OPAQUE_FRAME_ID = "619a3539db0d"


def _chunk(kind: bytes, data: bytes) -> bytes:
    return (
        struct.pack(">I", len(data))
        + kind
        + data
        + struct.pack(">I", zlib.crc32(kind + data) & 0xFFFFFFFF)
    )


def _png(*, width: int = 256, height: int = 256, marker: bytes = b"x") -> bytes:
    ihdr = struct.pack(">IIBBBBB", width, height, 8, 6, 0, 0, 0)
    return (
        b"\x89PNG\r\n\x1a\n"
        + _chunk(b"IHDR", ihdr)
        + _chunk(b"IDAT", marker)
        + _chunk(b"IEND", b"")
    )


def _manifest(
    *,
    frame_time: int = FRAME_TIME,
    host: str = rainviewer_native.RAINVIEWER_TILE_HOST,
    path: str | None = None,
    version: str = "2.0",
) -> bytes:
    value = {
        "version": version,
        "generated": MANIFEST_TIME,
        "host": host,
        "radar": {
            "past": [
                {"time": frame_time - 600, "path": f"/v2/radar/{frame_time - 600}"},
                {
                    "time": frame_time,
                    "path": path if path is not None else f"/v2/radar/{frame_time}",
                },
            ]
        },
    }
    return json.dumps(value, separators=(",", ":")).encode()


def _live_shaped_manifest() -> bytes:
    value = {
        "version": "2.0",
        "generated": MANIFEST_TIME,
        "host": rainviewer_native.RAINVIEWER_TILE_HOST,
        "radar": {
            "past": [
                {"time": FRAME_TIME - 600, "path": "/v2/radar/6d4bfbea20d2"},
                {"time": FRAME_TIME, "path": f"/v2/radar/{OPAQUE_FRAME_ID}"},
            ],
            "nowcast": [],
        },
        "satellite": {"infrared": []},
    }
    return json.dumps(value, separators=(",", ":")).encode()


def _response(payload: bytes, content_type: str, received_at: float):
    return rainviewer_native.RainviewerHttpResponse(
        payload=payload,
        content_type=content_type,
        received_at=received_at,
    )


def _job(
    job_id: str = "rainviewer-native-slot-001",
    *,
    lat: float = 40.7128,
    lon: float = -74.0060,
    payload: dict | None = None,
):
    return SimpleNamespace(
        job_id=job_id,
        payload=(
            {"regions": [{"center_lat": lat, "center_lon": lon, "horizon_km": 350.0}]}
            if payload is None
            else payload
        ),
    )


def _install_downloads(monkeypatch, *, manifest: bytes | None = None):
    manifest_payload = _manifest() if manifest is None else manifest
    calls = {"manifest": 0, "tiles": []}

    def get_manifest(**_kwargs):
        calls["manifest"] += 1
        return _response(manifest_payload, "application/json", MANIFEST_TIME + 0.25)

    def get_tile(url: str, **_kwargs):
        calls["tiles"].append(url)
        marker = urlsplit(url).path.encode()
        return _response(_png(marker=marker), "image/png", MANIFEST_TIME + 1.0)

    monkeypatch.setattr(rainviewer_native, "_download_manifest", get_manifest)
    monkeypatch.setattr(rainviewer_native, "_download_tile", get_tile)
    return calls


def _capture(monkeypatch, tmp_path):
    calls = _install_downloads(monkeypatch)
    store = RawArtifactStore(tmp_path / "raw")
    result = rainviewer_native.capture_latest_rainviewer_region(
        _job(), raw_store=store
    )
    return store, result, calls


def test_candidate_role_contract_is_one_manifest_plus_two_complete_3x3_grids():
    roles = rainviewer_native.RAINVIEWER_CANDIDATE_REQUIRED_ROLES
    assert len(roles) == 19
    assert roles == tuple(sorted(set(roles)))
    assert roles.count("manifest") == 1
    assert sum(role.startswith("radar_") for role in roles) == 9
    assert sum(role.startswith("coverage_") for role in roles) == 9


def test_exact_manifest_is_durable_before_first_json_parse(monkeypatch, tmp_path):
    payload = _manifest()
    store = RawArtifactStore(tmp_path / "raw")
    _install_downloads(monkeypatch, manifest=payload)
    real_inspector = rainviewer_native._inspect_manifest
    inspected = []

    def inspect(exact: bytes):
        stats = store.stats()
        assert stats["receipt_count"] == 1
        assert stats["capture_count"] == 0
        objects = list((tmp_path / "raw" / "objects").glob("*.raw"))
        assert len(objects) == 1
        assert objects[0].read_bytes() == payload
        inspected.append(exact)
        return real_inspector(exact)

    monkeypatch.setattr(rainviewer_native, "_inspect_manifest", inspect)
    rainviewer_native.capture_latest_rainviewer_region(_job(), raw_store=store)
    assert inspected == [payload]


def test_each_exact_tile_is_durable_before_png_inspection(monkeypatch, tmp_path):
    store = RawArtifactStore(tmp_path / "raw")
    _install_downloads(monkeypatch)
    real_inspector = rainviewer_native._inspect_png
    receipt_counts = []

    def inspect(exact: bytes):
        stats = store.stats()
        assert stats["capture_count"] == 0
        receipt_counts.append(stats["receipt_count"])
        assert any(
            path.read_bytes() == exact
            for path in (tmp_path / "raw" / "objects").glob("*.raw")
        )
        return real_inspector(exact)

    monkeypatch.setattr(rainviewer_native, "_inspect_png", inspect)
    rainviewer_native.capture_latest_rainviewer_region(_job(), raw_store=store)
    assert receipt_counts == list(range(2, 20))
    assert store.stats()["capture_count"] == 1


def test_complete_capture_retains_manifest_radar_and_coverage_exact_bytes(
    monkeypatch, tmp_path
):
    store, result, calls = _capture(monkeypatch, tmp_path)
    assert calls["manifest"] == 1
    assert len(calls["tiles"]) == 18
    assert sum("/v2/radar/" in url for url in calls["tiles"]) == 9
    assert sum("/v2/coverage/0/" in url for url in calls["tiles"]) == 9
    assert all("/2/0_0.png" in url for url in calls["tiles"] if "/radar/" in url)

    raw = result["raw_provenance"]
    capture = store.load_capture(raw["raw_capture_id"])
    assert capture is not None
    assert tuple(member.role for member in capture.members) == (
        rainviewer_native.RAINVIEWER_CANDIDATE_REQUIRED_ROLES
    )
    assert len(raw["raw_sources"]) == 19
    assert store.read_bytes(raw["raw_sources"]["manifest"]["raw_receipt_id"]) == _manifest()
    for role, source in raw["raw_sources"].items():
        exact = store.read_bytes(source["raw_receipt_id"])
        assert exact
        if role != "manifest":
            assert exact.startswith(b"\x89PNG")


def test_result_cannot_claim_rain_dry_observations_or_quantitative_truth(
    monkeypatch, tmp_path
):
    _store, result, _calls = _capture(monkeypatch, tmp_path)
    assert result["declared_semantic_class"] == (
        "categorical_composite_reflectivity_visualisation"
    )
    assert result["timestamp_quality_state"] == (
        "composite_frame_generation_time_not_uniform_observation_time"
    )
    assert result["coverage_mask_captured"] is True
    assert result["radar_tile_count"] == 9
    assert result["coverage_tile_count"] == 9
    assert result["raw_member_count"] == 19
    assert result["captured_frame_count"] == 1
    assert result["motion_history_eligible"] is False
    assert result["request_horizon_km"] == 350.0
    assert result["guaranteed_capture_coverage_radius_km"] >= 350.0
    assert result["raw_provenance"]["request_region"] == {
        "center_lat": 40.7128,
        "center_lon": -74.006,
        "horizon_km": 350.0,
        "guaranteed_capture_coverage_radius_km": result[
            "guaranteed_capture_coverage_radius_km"
        ],
    }
    assert "id-" in result["raw_provenance"]["upstream_capture_id"]
    assert "lat40.71280000-lon-74.00600000-horizon350.000km" in (
        result["raw_provenance"]["upstream_capture_id"]
    )
    assert result["quantitative_semantics_eligible"] is False
    assert result["operational_admission_eligible"] is False
    assert result["weather_state"] == "unknown"
    assert result["decode_status"] == "not_decoded"
    assert "frame_timestamp" not in result
    assert "observations" not in result
    assert "observation_count" not in result
    assert "raining" not in json.dumps(result).lower()
    assert "dry" not in json.dumps(result).lower()


def test_payload_free_provenance_contains_no_url_path_or_secret_material(
    monkeypatch, tmp_path
):
    _store, result, _calls = _capture(monkeypatch, tmp_path)
    encoded = json.dumps(result["raw_provenance"], sort_keys=True).lower()
    assert "payload" not in encoded
    assert "artifact_path" not in encoded
    assert "authorization" not in encoded
    assert "token" not in encoded
    assert "password" not in encoded
    assert "credential" not in encoded
    assert "?" not in encoded


def test_complete_retry_reuses_all_durable_receipts_without_network(
    monkeypatch, tmp_path
):
    store, first, calls = _capture(monkeypatch, tmp_path)

    def forbidden(*_args, **_kwargs):
        pytest.fail("complete retry mag niet opnieuw downloaden")

    monkeypatch.setattr(rainviewer_native, "_download_manifest", forbidden)
    monkeypatch.setattr(rainviewer_native, "_download_tile", forbidden)
    second = rainviewer_native.capture_latest_rainviewer_region(
        _job(), raw_store=store
    )
    assert calls["manifest"] == 1
    assert len(calls["tiles"]) == 18
    assert second["raw_provenance"]["raw_capture_id"] == (
        first["raw_provenance"]["raw_capture_id"]
    )
    assert store.stats()["receipt_count"] == 19
    assert store.stats()["capture_count"] == 1


def test_partial_failure_leaves_receipts_but_no_false_complete_capture_and_resumes(
    monkeypatch, tmp_path
):
    manifest_payload = _manifest()
    store = RawArtifactStore(tmp_path / "raw")
    calls: list[str] = []
    failed_url: list[str] = []
    monkeypatch.setattr(
        rainviewer_native,
        "_download_manifest",
        lambda **_kwargs: _response(
            manifest_payload, "application/json", MANIFEST_TIME
        ),
    )

    def flaky(url: str, **_kwargs):
        calls.append(url)
        if len(calls) == 5:
            failed_url.append(url)
            raise TimeoutError("synthetic tile timeout")
        return _response(_png(marker=url.encode()), "image/png", MANIFEST_TIME + 1)

    monkeypatch.setattr(rainviewer_native, "_download_tile", flaky)
    with pytest.raises(TimeoutError, match="synthetic"):
        rainviewer_native.capture_latest_rainviewer_region(_job(), raw_store=store)
    assert store.stats()["receipt_count"] == 5  # manifest + four complete tiles
    assert store.stats()["capture_count"] == 0

    result = rainviewer_native.capture_latest_rainviewer_region(
        _job(), raw_store=store
    )
    assert calls.count(failed_url[0]) == 2
    assert len(calls) == 19  # eighteen successful URLs plus one failed attempt
    assert store.stats()["receipt_count"] == 19
    assert store.stats()["capture_count"] == 1
    assert result["capture_state"] == "captured_not_decoded"


def test_malformed_manifest_is_still_exactly_retained_but_never_completed(
    monkeypatch, tmp_path
):
    malformed = b'{"version":"2.0","version":"2.1"}'
    store = RawArtifactStore(tmp_path / "raw")
    _install_downloads(monkeypatch, manifest=malformed)
    with pytest.raises(ValueError, match="dubbele JSON"):
        rainviewer_native.capture_latest_rainviewer_region(_job(), raw_store=store)
    assert store.stats()["receipt_count"] == 1
    assert store.stats()["capture_count"] == 0
    receipt_id = RawArtifactStore._receipt_id(
        provider="rainviewer",
        product=rainviewer_native.RAINVIEWER_MANIFEST_PRODUCT,
        fetch_id="rainviewer-native-slot-001:manifest",
    )
    assert store.read_bytes(receipt_id) == malformed


def test_malformed_png_is_retained_but_cannot_complete_capture(monkeypatch, tmp_path):
    store = RawArtifactStore(tmp_path / "raw")
    _install_downloads(monkeypatch)
    malformed = b"not-a-png"
    monkeypatch.setattr(
        rainviewer_native,
        "_download_tile",
        lambda _url, **_kwargs: _response(
            malformed, "image/png", MANIFEST_TIME + 1
        ),
    )
    with pytest.raises(ValueError, match="geen geldige PNG"):
        rainviewer_native.capture_latest_rainviewer_region(_job(), raw_store=store)
    assert store.stats()["receipt_count"] == 2
    assert store.stats()["capture_count"] == 0
    objects = [path.read_bytes() for path in (tmp_path / "raw" / "objects").glob("*.raw")]
    assert malformed in objects


@pytest.mark.parametrize(
    "manifest, match",
    [
        (_manifest(host="https://evil.invalid"), "vaste tilehost"),
        (_manifest(path=f"/v2/radar/{FRAME_TIME - 1}"), "spreken elkaar tegen"),
        (_manifest(version="3.0"), "v2-API"),
    ],
)
def test_manifest_identity_drift_fails_closed_after_raw_retention(
    monkeypatch, tmp_path, manifest, match
):
    store = RawArtifactStore(tmp_path / "raw")
    _install_downloads(monkeypatch, manifest=manifest)
    with pytest.raises(ValueError, match=match):
        rainviewer_native.capture_latest_rainviewer_region(_job(), raw_store=store)
    assert store.stats()["receipt_count"] == 1
    assert store.stats()["capture_count"] == 0


@pytest.mark.parametrize(
    "job",
    [
        _job(payload={"regions": []}),
        _job(payload={"regions": [{"center_lat": 1, "center_lon": 2, "token": "x"}]}),
        _job(payload={"regions": [
            {"center_lat": 1, "center_lon": 2},
            {"center_lat": 3, "center_lon": 4},
        ]}),
        _job(lat=float("nan")),
        _job(lon=180.0),
        _job(job_id="bad job id"),
    ],
)
def test_unbounded_ambiguous_or_secret_bearing_jobs_fail_before_network(
    monkeypatch, tmp_path, job
):
    monkeypatch.setattr(
        rainviewer_native,
        "_download_manifest",
        lambda **_kwargs: pytest.fail(
            "ongeldig jobcontract mag geen netwerk gebruiken"
        ),
    )
    with pytest.raises(ValueError):
        rainviewer_native.capture_latest_rainviewer_region(
            job, raw_store=RawArtifactStore(tmp_path / "raw")
        )


def test_longitude_wrap_produces_nine_unique_tiles_at_dateline():
    center_x, center_y = rainviewer_native._center_tile(0.0, 179.99)
    coordinates = {
        rainviewer_native._tile_coordinates(center_x, center_y, dx, dy)
        for dx, dy in rainviewer_native._OFFSETS
    }
    assert len(coordinates) == 9
    assert {x for x, _y in coordinates} == {30, 31, 0}


def test_polar_edge_fails_explicitly_instead_of_requesting_invalid_tiles():
    with pytest.raises(ValueError, match="poolrand"):
        rainviewer_native._center_tile(85.0, 0.0)


def test_png_crc_dimensions_and_required_chunks_are_checked():
    valid = _png(marker=b"rainviewer")
    identity = rainviewer_native._inspect_png(valid)
    assert (identity.width, identity.height, identity.color_type) == (256, 256, 6)
    with pytest.raises(ValueError, match="afmetingen"):
        rainviewer_native._inspect_png(_png(width=512))
    corrupted = bytearray(valid)
    corrupted[-1] ^= 1
    with pytest.raises(ValueError, match="checksum"):
        rainviewer_native._inspect_png(bytes(corrupted))
    no_idat = (
        b"\x89PNG\r\n\x1a\n"
        + _chunk(b"IHDR", struct.pack(">IIBBBBB", 256, 256, 8, 6, 0, 0, 0))
        + _chunk(b"IEND", b"")
    )
    with pytest.raises(ValueError, match="beelddata"):
        rainviewer_native._inspect_png(no_idat)


class _FakeHttpResponse(io.BytesIO):
    def __init__(
        self,
        payload: bytes,
        *,
        url: str,
        status: int = 200,
        content_type: str = "application/json",
        content_encoding: str = "identity",
        declared_length: str | None = None,
    ):
        super().__init__(payload)
        self.status = status
        self._url = url
        self.headers = {
            "Content-Type": content_type,
            "Content-Encoding": content_encoding,
            "Content-Length": str(len(payload)) if declared_length is None else declared_length,
        }

    def geturl(self):
        return self._url

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False


@pytest.mark.parametrize(
    "response, match",
    [
        (_FakeHttpResponse(b"{}", url=rainviewer_native.RAINVIEWER_MANIFEST_URL, status=302), "HTTP 200"),
        (_FakeHttpResponse(b"{}", url="https://evil.invalid/x"), "onverwacht endpoint"),
        (_FakeHttpResponse(b"{}", url=rainviewer_native.RAINVIEWER_MANIFEST_URL, content_encoding="gzip"), "transportcompressie"),
        (_FakeHttpResponse(b"{}", url=rainviewer_native.RAINVIEWER_MANIFEST_URL, content_type="text/html"), "Content-Type"),
        (_FakeHttpResponse(b"{}", url=rainviewer_native.RAINVIEWER_MANIFEST_URL, declared_length="abc"), "Content-Length"),
        (_FakeHttpResponse(b"{}", url=rainviewer_native.RAINVIEWER_MANIFEST_URL, declared_length="9"), "payloadlengte"),
    ],
)
def test_http_contract_rejects_status_origin_encoding_type_and_length(response, match):
    with pytest.raises(ValueError, match=match):
        rainviewer_native._download_exact(
            rainviewer_native.RAINVIEWER_MANIFEST_URL,
            expected_host="api.rainviewer.com",
            expected_path="/public/weather-maps.json",
            maximum_bytes=1024,
            allowed_content_types=frozenset({"application/json"}),
            opener=lambda *_args, **_kwargs: response,
        )


def test_source_url_validator_rejects_redirect_style_and_ssrf_shapes():
    for value in (
        "http://api.rainviewer.com/public/weather-maps.json",
        "https://user@api.rainviewer.com/public/weather-maps.json",
        "https://api.rainviewer.com:444/public/weather-maps.json",
        "https://api.rainviewer.com/public/weather-maps.json?next=https://evil.invalid",
        "https://127.0.0.1/public/weather-maps.json",
    ):
        with pytest.raises(ValueError):
            rainviewer_native._safe_public_url(
                value,
                exact_host="api.rainviewer.com",
                exact_path="/public/weather-maps.json",
            )


def test_manifest_selection_uses_latest_generation_not_list_position():
    document = json.loads(_manifest())
    document["radar"]["past"].reverse()
    identity = rainviewer_native._inspect_manifest(
        json.dumps(document, separators=(",", ":")).encode()
    )
    assert identity.frame_generation_timestamp == FRAME_TIME
    assert identity.frame_path == f"/v2/radar/{FRAME_TIME}"


def test_current_live_shaped_opaque_frame_path_is_bound_without_becoming_time():
    identity = rainviewer_native._inspect_manifest(_live_shaped_manifest())
    assert identity.frame_generation_timestamp == FRAME_TIME
    assert identity.frame_path == f"/v2/radar/{OPAQUE_FRAME_ID}"


def test_opaque_frame_path_must_be_exact_lowercase_bounded_hex():
    document = json.loads(_live_shaped_manifest())
    document["radar"]["past"][-1]["path"] = "/v2/radar/ZZZZZZZZZZZZ"
    with pytest.raises(ValueError, match="v2-contract"):
        rainviewer_native._inspect_manifest(json.dumps(document).encode())


def test_manifest_frame_count_is_bounded():
    document = json.loads(_manifest())
    document["radar"]["past"] = [
        {"time": 1_700_000_000 + index, "path": f"/v2/radar/{1_700_000_000 + index}"}
        for index in range(rainviewer_native.MAX_MANIFEST_FRAMES + 1)
    ]
    with pytest.raises(ValueError, match="begrensde past"):
        rainviewer_native._inspect_manifest(json.dumps(document).encode())


@pytest.mark.parametrize(
    "generated, frame_time",
    [
        (FRAME_TIME, FRAME_TIME + 301),
        (FRAME_TIME + 3_601, FRAME_TIME),
        (9_999_999_999, FRAME_TIME),
    ],
)
def test_manifest_and_latest_frame_time_relation_is_bounded(generated, frame_time):
    document = json.loads(_manifest(frame_time=frame_time))
    document["generated"] = generated
    with pytest.raises(ValueError, match="tijd"):
        rainviewer_native._inspect_manifest(json.dumps(document).encode())


@pytest.mark.parametrize("drift", ["collector", "upstream", "content_type"])
def test_resume_rejects_drifted_manifest_receipt_contract(
    monkeypatch, tmp_path, drift
):
    store = RawArtifactStore(tmp_path / "raw")
    store.initialize()
    provenance = rainviewer_native._manifest_provenance()
    if drift == "collector":
        provenance["collector_version"] = "obsolete-rainviewer-contract"
    content_type = "text/plain" if drift == "content_type" else "application/json"
    upstream_id = (
        "wrong-stable-manifest-id"
        if drift == "upstream"
        else rainviewer_native._manifest_upstream_id(_manifest())
    )
    store.store(
        provider="rainviewer",
        product=rainviewer_native.RAINVIEWER_MANIFEST_PRODUCT,
        fetch_id="rainviewer-native-slot-001:manifest",
        content_type=content_type,
        received_at=MANIFEST_TIME,
        upstream_id=upstream_id,
        provenance=provenance,
        payload=_manifest(),
    )
    monkeypatch.setattr(
        rainviewer_native,
        "_download_manifest",
        lambda **_kwargs: pytest.fail(
            "bestaande afwijkende receipt mag geen nieuw netwerk maskeren"
        ),
    )
    with pytest.raises(ValueError, match="hervatte receipt"):
        rainviewer_native.capture_latest_rainviewer_region(_job(), raw_store=store)


@pytest.mark.parametrize("drift", ["upstream", "content_type", "provenance"])
def test_resume_rejects_drifted_tile_receipt_contract(
    monkeypatch, tmp_path, drift
):
    store = RawArtifactStore(tmp_path / "raw")
    store.initialize()
    manifest = _manifest()
    manifest_fetch = "rainviewer-native-slot-001:manifest"
    store.store(
        provider="rainviewer",
        product=rainviewer_native.RAINVIEWER_MANIFEST_PRODUCT,
        fetch_id=manifest_fetch,
        content_type="application/json",
        received_at=MANIFEST_TIME,
        upstream_id=rainviewer_native._manifest_upstream_id(manifest),
        provenance=rainviewer_native._manifest_provenance(),
        payload=manifest,
    )
    identity = rainviewer_native._inspect_manifest(manifest)
    center_x, center_y = rainviewer_native._center_tile(40.7128, -74.006)
    dx, dy = rainviewer_native._OFFSETS[0]
    x, y = rainviewer_native._tile_coordinates(center_x, center_y, dx, dy)
    role = rainviewer_native._tile_role("radar", dx, dy)
    provenance = rainviewer_native._tile_provenance(
        identity,
        kind="radar",
        x=x,
        y=y,
        center_lat=40.7128,
        center_lon=-74.006,
        horizon_km=350.0,
    )
    if drift == "provenance":
        provenance["smooth"] = 1
    store.store(
        provider="rainviewer",
        product=rainviewer_native.RAINVIEWER_RADAR_TILE_PRODUCT,
        fetch_id=f"rainviewer-native-slot-001:{role}",
        content_type="text/plain" if drift == "content_type" else "image/png",
        received_at=MANIFEST_TIME + 1,
        upstream_id=(
            "wrong-radar-tile-id"
            if drift == "upstream"
            else rainviewer_native._tile_upstream_id(
                identity, kind="radar", x=x, y=y
            )
        ),
        provenance=provenance,
        payload=_png(),
    )
    monkeypatch.setattr(
        rainviewer_native,
        "_download_manifest",
        lambda **_kwargs: pytest.fail("manifestreceipt moet lokaal hervatten"),
    )
    monkeypatch.setattr(
        rainviewer_native,
        "_download_tile",
        lambda *_args, **_kwargs: pytest.fail(
            "afwijkende tegelreceipt mag geen nieuw netwerk maskeren"
        ),
    )
    with pytest.raises(ValueError, match="hervatte receipt"):
        rainviewer_native.capture_latest_rainviewer_region(_job(), raw_store=store)


def test_fresh_jobs_can_store_successive_mutable_manifests(monkeypatch, tmp_path):
    first = _manifest()
    second_document = json.loads(_manifest(frame_time=FRAME_TIME + 600))
    second_document["generated"] = MANIFEST_TIME + 600
    second = json.dumps(second_document, separators=(",", ":")).encode()
    manifests = iter((first, second))
    manifest_calls = []
    tile_calls = []

    def get_manifest(**_kwargs):
        value = next(manifests)
        manifest_calls.append(value)
        return _response(value, "application/json", MANIFEST_TIME)

    def get_tile(url: str, **_kwargs):
        tile_calls.append(url)
        return _response(_png(marker=url.encode()), "image/png", MANIFEST_TIME + 1)

    monkeypatch.setattr(rainviewer_native, "_download_manifest", get_manifest)
    monkeypatch.setattr(rainviewer_native, "_download_tile", get_tile)
    store = RawArtifactStore(tmp_path / "raw")
    first_result = rainviewer_native.capture_latest_rainviewer_region(
        _job("rainviewer-fresh-job-a"), raw_store=store
    )
    second_result = rainviewer_native.capture_latest_rainviewer_region(
        _job("rainviewer-fresh-job-b"), raw_store=store
    )
    assert manifest_calls == [first, second]
    assert len(tile_calls) == 36
    assert store.stats()["receipt_count"] == 38
    assert store.stats()["capture_count"] == 2
    assert first_result["frame_generation_timestamp"] == FRAME_TIME
    assert second_result["frame_generation_timestamp"] == FRAME_TIME + 600
    assert (
        first_result["raw_provenance"]["upstream_capture_id"]
        != second_result["raw_provenance"]["upstream_capture_id"]
    )


def test_requested_horizon_must_fit_inside_complete_3x3_union(monkeypatch, tmp_path):
    monkeypatch.setattr(
        rainviewer_native,
        "_download_manifest",
        lambda **_kwargs: pytest.fail("ondekbare horizon mag geen netwerk starten"),
    )
    with pytest.raises(ValueError, match="dekt de gevraagde horizon niet"):
        rainviewer_native.capture_latest_rainviewer_region(
            _job(lat=82.0, lon=0.0, payload={"regions": [{
                "center_lat": 82.0,
                "center_lon": 0.0,
                "horizon_km": 1000.0,
            }]}),
            raw_store=RawArtifactStore(tmp_path / "raw"),
        )


def test_jobwide_deadline_stops_sequential_tile_monopoly(monkeypatch, tmp_path):
    clock = {"now": 0.0, "deadline": None, "tiles": 0}
    monkeypatch.setattr(rainviewer_native.time, "monotonic", lambda: clock["now"])

    def get_manifest(*, deadline):
        clock["deadline"] = deadline
        return _response(_manifest(), "application/json", MANIFEST_TIME)

    def get_tile(_url: str, *, deadline):
        assert deadline == clock["deadline"]
        clock["tiles"] += 1
        clock["now"] = deadline + 0.001
        return _response(_png(), "image/png", MANIFEST_TIME + 1)

    monkeypatch.setattr(rainviewer_native, "_download_manifest", get_manifest)
    monkeypatch.setattr(rainviewer_native, "_download_tile", get_tile)
    with pytest.raises(TimeoutError, match="jobbrede deadline"):
        rainviewer_native.capture_latest_rainviewer_region(
            _job(), raw_store=RawArtifactStore(tmp_path / "raw")
        )
    assert clock["tiles"] == 1


def test_same_zoom_tile_still_binds_exact_region_request(monkeypatch, tmp_path):
    store, result, _calls = _capture(monkeypatch, tmp_path)
    request = result["raw_provenance"]["request_region"]
    assert request["center_lat"] == 40.7128
    assert request["center_lon"] == -74.006
    assert request["horizon_km"] == 350.0


def test_result_is_stable_under_unrelated_input_copy(monkeypatch, tmp_path):
    job = _job()
    original = deepcopy(job.payload)
    _install_downloads(monkeypatch)
    rainviewer_native.capture_latest_rainviewer_region(
        job, raw_store=RawArtifactStore(tmp_path / "raw")
    )
    assert job.payload == original


def test_complete_retry_reopens_store_reads_exactly_nineteen_members_without_reparse(
    monkeypatch, tmp_path,
):
    store, first, _calls = _capture(monkeypatch, tmp_path)
    reopened = RawArtifactStore(store.root)
    reopened.initialize()
    real_read = reopened._read_verified  # noqa: SLF001 - counted authority read
    reads = []

    def counted(*args, **kwargs):
        reads.append(args[0])
        return real_read(*args, **kwargs)

    monkeypatch.setattr(reopened, "_read_verified", counted)
    monkeypatch.setattr(
        rainviewer_native,
        "_inspect_manifest",
        lambda *_args, **_kwargs: pytest.fail("manifest-successverdict moest reparse voorkomen"),
    )
    monkeypatch.setattr(
        rainviewer_native,
        "_inspect_png",
        lambda *_args, **_kwargs: pytest.fail("PNG-successverdict moest reparse voorkomen"),
    )
    monkeypatch.setattr(
        rainviewer_native,
        "_download_manifest",
        lambda **_kwargs: pytest.fail("complete retry mocht niet downloaden"),
    )
    monkeypatch.setattr(
        rainviewer_native,
        "_download_tile",
        lambda *_args, **_kwargs: pytest.fail("complete retry mocht niet downloaden"),
    )

    retry = rainviewer_native.capture_latest_rainviewer_region(_job(), raw_store=reopened)

    assert retry == first
    assert len(reads) == 19


def test_deterministic_manifest_rejection_survives_store_reopen(
    monkeypatch, tmp_path,
):
    store = RawArtifactStore(tmp_path / "raw")
    _install_downloads(monkeypatch, manifest=b'{"version":"1"}')
    job = _job("rainviewer-durable-rejection")
    with pytest.raises(ValueError, match="v2-API"):
        rainviewer_native.capture_latest_rainviewer_region(job, raw_store=store)

    monkeypatch.setattr(
        rainviewer_native,
        "_inspect_manifest",
        lambda *_args, **_kwargs: pytest.fail("duurzame afkeur mocht niet herinspecteren"),
    )
    monkeypatch.setattr(
        rainviewer_native,
        "_download_manifest",
        lambda **_kwargs: pytest.fail("receipt-retry mocht niet downloaden"),
    )
    with pytest.raises(ValueError, match="v2-API"):
        rainviewer_native.capture_latest_rainviewer_region(
            job,
            raw_store=RawArtifactStore(store.root),
        )


def test_rainviewer_inspection_timeout_is_never_persisted_as_rejection(
    monkeypatch, tmp_path,
):
    store = RawArtifactStore(tmp_path / "raw")
    _install_downloads(monkeypatch)
    job = _job("rainviewer-transient-timeout")
    real_inspector = rainviewer_native._inspect_manifest
    monkeypatch.setattr(
        rainviewer_native,
        "_inspect_manifest",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(TimeoutError("synthetic timeout")),
    )
    with pytest.raises(TimeoutError, match="synthetic"):
        rainviewer_native.capture_latest_rainviewer_region(job, raw_store=store)
    verdicts = store.root / "rainviewer_validation_verdicts"
    assert list(verdicts.glob("*.json")) == []

    monkeypatch.setattr(rainviewer_native, "_inspect_manifest", real_inspector)
    result = rainviewer_native.capture_latest_rainviewer_region(
        job,
        raw_store=RawArtifactStore(store.root),
    )
    assert result["raw_member_count"] == 19


def test_manifest_json_depth_budget_is_enforced():
    value = 1
    for _ in range(rainviewer_native.MAX_MANIFEST_JSON_DEPTH + 2):
        value = {"nested": value}
    with pytest.raises(ValueError, match="diep genest"):
        rainviewer_native._json_without_duplicates(json.dumps(value).encode())


def test_concurrent_same_job_is_serialized_to_one_upstream_capture(
    monkeypatch, tmp_path,
):
    calls = _install_downloads(monkeypatch)
    store = RawArtifactStore(tmp_path / "raw")
    job = _job("rainviewer-concurrent-slot")

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(
            lambda _index: rainviewer_native.capture_latest_rainviewer_region(
                job,
                raw_store=store,
            ),
            range(2),
        ))

    assert results[0] == results[1]
    assert calls["manifest"] == 1
    assert len(calls["tiles"]) == 18


def test_provider_capacity_file_lock_honours_absolute_deadline(tmp_path):
    store = RawArtifactStore(tmp_path / "raw")
    store.initialize()
    path = rainviewer_native._capture_capacity_lock_path(store)
    entered = threading.Event()
    release = threading.Event()

    def holder():
        with rainviewer_native._deadline_file_lock(
            path,
            deadline=time.monotonic() + 5.0,
        ):
            entered.set()
            assert release.wait(2.0)

    thread = threading.Thread(target=holder)
    thread.start()
    assert entered.wait(2.0)
    try:
        with pytest.raises(TimeoutError, match="jobbrede deadline"):
            with rainviewer_native._deadline_file_lock(
                path,
                deadline=time.monotonic() + 0.05,
            ):
                pytest.fail("tweede houder mocht de capacity-lock niet verkrijgen")
    finally:
        release.set()
        thread.join(timeout=3.0)
    assert not thread.is_alive()
