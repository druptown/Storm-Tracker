from __future__ import annotations

from contextlib import closing
from copy import deepcopy
import json
import sqlite3

import pytest

from storm_tracker_worker import radar_cycle_contract as contract
from storm_tracker_worker.radar_cycle_store import (
    AdmissionConflict,
    RadarCycleStore,
    RawCaptureSetMismatch,
)
from storm_tracker_worker.native_capture import (
    NativeCaptureMemberInput,
    capture_native_set,
)
from storm_tracker_worker.raw_artifacts import (
    RAW_ARTIFACT_CAPTURE_RECEIPT_SCHEMA,
    RAW_ARTIFACT_CAPTURE_SCHEMA,
    RAW_ARTIFACT_SCHEMA_VERSION,
    RawArtifactIntegrityError,
    RawArtifactStore,
)


PRODUCT_AT = 1_800_000_000.25
RECEIVED_AT = 1_800_000_011.75
EVALUATED_AT = 1_800_000_040.0


def _store_capture(store: RawArtifactStore, suffix: str = "a"):
    return capture_native_set(
        store,
        provider="kmi",
        capture_product="kmi_appv4_visualisation_capture",
        fetch_id=f"job:{suffix}",
        upstream_capture_id=f"forecast/capture-{suffix}",
        product_timestamp=PRODUCT_AT,
        collector="kmi-native-capture",
        collector_version="kmi-native-capture-v1",
        members=(
            NativeCaptureMemberInput(
                role="capture_manifest",
                product="kmi_capture_manifest",
                content_type="application/json",
                received_at=RECEIVED_AT - 2.0,
                upstream_id=f"kmi/{suffix}/capture-manifest.json",
                payload=(f'{{"capture":"{suffix}"}}').encode("ascii"),
                provenance={"semantic_class": "capture_manifest"},
            ),
            NativeCaptureMemberInput(
                role="forecast_json",
                product="kmi_forecast_json",
                content_type="application/json",
                received_at=RECEIVED_AT - 1.0,
                upstream_id=f"kmi/{suffix}/forecast.json",
                payload=(f'{{"forecast":"{suffix}"}}').encode("ascii"),
                provenance={"semantic_class": "forecast_manifest"},
            ),
            NativeCaptureMemberInput(
                role="radar_png",
                product="kmi_radar_png",
                content_type="image/png",
                received_at=RECEIVED_AT,
                upstream_id=f"kmi/{suffix}/radar.png",
                payload=b"\x89PNG\r\n\x1a\n" + suffix.encode("ascii"),
                provenance={"semantic_class": "rendered_radar_image"},
            ),
        ),
    ).capture


def _request(receipt) -> dict:
    return {
        "contract": contract.REQUEST_CONTRACT_V2,
        "requested_at": 1_800_000_020.0,
        "regions": [
            {
                "region_id": "home",
                "center_lat": 51.025,
                "center_lon": 4.477,
                "radius_km": 250.0,
            }
        ],
        "targets": [
            {
                "target_id": "tracker-home",
                "region_id": "home",
                "latitude": 51.025,
                "longitude": 4.477,
            }
        ],
        "inputs": [
            {
                "provider": receipt.provider,
                "product": receipt.product,
                "region_ids": ["home"],
                "capture_set": {
                    "capture_id": receipt.capture_id,
                    "fetch_id": receipt.fetch_id,
                    "upstream_capture_id": receipt.upstream_capture_id,
                    "manifest_sha256": receipt.manifest_sha256,
                    "product_timestamp": receipt.product_timestamp,
                    "members": [
                        {
                            "member_id": member.member_id,
                            "role": member.role,
                            "receipt_id": member.receipt_id,
                            "content_sha256": member.content_sha256,
                            "receipt_sha256": member.receipt_sha256,
                            "received_at": member.received_at,
                        }
                        for member in receipt.members
                    ],
                    "provenance": {
                        "grade": contract.RAW_CAPTURE_PROVENANCE_GRADE,
                        "complete": True,
                        "raw_store_schema_version": (
                            contract.RAW_ARTIFACT_STORE_SCHEMA_VERSION
                        ),
                        "capture_schema": (
                            contract.RAW_ARTIFACT_CAPTURE_SCHEMA
                        ),
                        "capture_receipt_schema": (
                            contract.RAW_ARTIFACT_CAPTURE_RECEIPT_SCHEMA
                        ),
                        "capture_receipt_sha256": (
                            receipt.capture_receipt_sha256
                        ),
                    },
                },
                "coverage": {
                    "status": "complete",
                    "bbox": [2.0, 49.0, 7.0, 52.0],
                },
            }
        ],
    }


def _v1_request() -> dict:
    return {
        "contract": contract.REQUEST_CONTRACT_V1,
        "requested_at": 1_800_000_020.0,
        "regions": [
            {
                "region_id": "home",
                "center_lat": 51.025,
                "center_lon": 4.477,
                "radius_km": 250.0,
            }
        ],
        "targets": [],
        "inputs": [],
    }


def _raw_store(tmp_path) -> RawArtifactStore:
    store = RawArtifactStore((tmp_path / "raw-artifacts").resolve())
    store.initialize()
    return store


def test_schema3_capture_receipt_view_is_real_stable_and_payload_free(
    tmp_path,
):
    raw_store = _raw_store(tmp_path)
    committed = _store_capture(raw_store)
    view = raw_store.load_capture_receipt(committed.capture_id)

    assert view == committed
    assert view.capture_id.startswith("capture-")
    assert view.raw_store_schema_version == 3
    assert view.capture_schema == "stv3-worker-raw-native-capture-v2"
    assert view.capture_receipt_schema == (
        "stv3-worker-raw-native-capture-receipt-v1"
    )
    assert len(view.capture_receipt_sha256) == 64
    assert view.complete is True
    assert contract.RAW_ARTIFACT_STORE_SCHEMA_VERSION == (
        RAW_ARTIFACT_SCHEMA_VERSION
    )
    assert contract.RAW_ARTIFACT_CAPTURE_SCHEMA == RAW_ARTIFACT_CAPTURE_SCHEMA
    assert contract.RAW_ARTIFACT_CAPTURE_RECEIPT_SCHEMA == (
        RAW_ARTIFACT_CAPTURE_RECEIPT_SCHEMA
    )
    for member in view.members:
        receipt = raw_store.load_receipt(member.receipt_id)
        assert member.member_id == member.role
        assert member.receipt_sha256 == receipt.receipt_sha256

    encoded = json.dumps(view.to_metadata(), sort_keys=True)
    assert "artifact_path" not in encoded
    assert "payload" not in encoded
    reopened = RawArtifactStore(raw_store.root)
    reopened.initialize()
    assert reopened.load_capture_receipt(view.capture_id) == view


def test_tampered_schema3_member_receipt_fails_before_cycle_facts(tmp_path):
    raw_store = _raw_store(tmp_path)
    capture = _store_capture(raw_store)
    request = _request(capture)
    with closing(sqlite3.connect(raw_store.catalog_path)) as database, database:
        database.execute(
            "UPDATE raw_artifact_receipts SET receipt_sha256=? "
            "WHERE receipt_id=?",
            ("f" * 64, capture.members[0].receipt_id),
        )
        database.commit()
    cycle_path = tmp_path / "tampered-cycle.sqlite3"
    store = RadarCycleStore(
        cycle_path,
        raw_artifact_store=raw_store,
    )

    with pytest.raises(RawArtifactIntegrityError, match="checksum"):
        store.admit("admission:tampered-raw", request)

    with closing(sqlite3.connect(cycle_path)) as database, database:
        assert database.execute(
            "SELECT count(*) FROM cycles"
        ).fetchone()[0] == 0


def test_v2_contract_is_canonical_payload_free_path_free_and_time_separated(
    tmp_path,
):
    raw_store = _raw_store(tmp_path)
    request = _request(_store_capture(raw_store))
    reversed_request = deepcopy(request)
    reversed_request["inputs"][0]["capture_set"]["members"].reverse()

    parsed = contract.parse_operational_radar_cycle_request(request)
    reversed_parsed = contract.parse_operational_radar_cycle_request(
        reversed_request
    )

    assert isinstance(parsed, contract.RadarCycleRequestV2)
    assert parsed.canonical_bytes == reversed_parsed.canonical_bytes
    assert parsed.cycle_id == reversed_parsed.cycle_id
    assert parsed.inputs[0].product_valid_at == PRODUCT_AT
    assert parsed.inputs[0].received_at == RECEIVED_AT
    assert parsed.inputs[0].product_valid_at != parsed.inputs[0].received_at
    wire = parsed.canonical_bytes.decode("ascii")
    assert "artifact_path" not in wire
    assert "payload" not in wire
    assert "PNG" not in wire


@pytest.mark.parametrize("mutation", ("missing", "extra", "swapped"))
def test_store_rejects_missing_extra_or_role_swapped_member(tmp_path, mutation):
    raw_store = _raw_store(tmp_path)
    request = _request(_store_capture(raw_store))
    members = request["inputs"][0]["capture_set"]["members"]
    if mutation == "missing":
        del members[1:]
    elif mutation == "extra":
        members.append({
            "member_id": "invented-member",
            "role": "invented_role",
            "receipt_id": "raw-" + "b" * 64,
            "content_sha256": "c" * 64,
            "receipt_sha256": "d" * 64,
            "received_at": RECEIVED_AT,
        })
    else:
        first = members[0]
        second = members[1]
        for key in (
            "receipt_id",
            "content_sha256",
            "receipt_sha256",
            "received_at",
        ):
            first[key], second[key] = second[key], first[key]

    store = RadarCycleStore(
        tmp_path / f"cycle-{mutation}.sqlite3",
        raw_artifact_store=raw_store,
        clock=lambda: EVALUATED_AT,
    )
    with pytest.raises(RawCaptureSetMismatch, match="exactly match"):
        store.admit(f"admission:{mutation}", request)


@pytest.mark.parametrize(
    "mutation",
    ("manifest_sha256", "product_timestamp", "member_received_at"),
)
def test_store_crosschecks_manifest_and_independent_product_receive_times(
    tmp_path,
    mutation,
):
    raw_store = _raw_store(tmp_path)
    request = _request(_store_capture(raw_store))
    capture = request["inputs"][0]["capture_set"]
    if mutation == "manifest_sha256":
        capture["manifest_sha256"] = "b" * 64
    elif mutation == "product_timestamp":
        capture["product_timestamp"] += 1.0
    else:
        capture["members"][0]["received_at"] += 1.0

    store = RadarCycleStore(
        tmp_path / f"time-{mutation}.sqlite3",
        raw_artifact_store=raw_store,
    )
    with pytest.raises(RawCaptureSetMismatch, match="exactly match"):
        store.admit(f"admission:{mutation}", request)


def test_v2_exact_retry_does_not_reverify_large_raw_set(tmp_path):
    raw_store = _raw_store(tmp_path)
    request = _request(_store_capture(raw_store))

    class CountingReader:
        def __init__(self):
            self.calls = 0

        def load_capture_receipt(self, capture_id):
            self.calls += 1
            return raw_store.load_capture_receipt(capture_id)

    reader = CountingReader()
    store = RadarCycleStore(
        tmp_path / "cycles.sqlite3",
        raw_artifact_store=reader,
        clock=lambda: EVALUATED_AT,
    )
    first = store.admit("admission:one", request)
    retry = store.admit("admission:one", deepcopy(request))

    assert reader.calls == 1
    assert retry["cycle_id"] == first["cycle_id"]
    assert retry["already_admitted"] is True
    assert retry["admission_created"] is False


def test_exact_identity_comparison_detects_forced_digest_collision(
    tmp_path,
    monkeypatch,
):
    raw_store = _raw_store(tmp_path)
    first = _request(_store_capture(raw_store, "a"))
    second = _request(_store_capture(raw_store, "b"))
    monkeypatch.setattr(contract, "_sha256_document", lambda _value: "d" * 64)
    store = RadarCycleStore(
        tmp_path / "collision.sqlite3",
        raw_artifact_store=raw_store,
        clock=lambda: EVALUATED_AT,
    )

    store.admit("admission:first", first)
    with pytest.raises(AdmissionConflict, match="digest collision"):
        store.admit("admission:second", second)


def test_v1_remains_readable_but_cannot_downgrade_operational_admission(
    tmp_path,
):
    raw = _v1_request()
    assert isinstance(
        contract.parse_radar_cycle_request(raw),
        contract.RadarCycleRequest,
    )
    store = RadarCycleStore(tmp_path / "compat.sqlite3")
    with pytest.raises(ValueError, match="requires contract v2"):
        store.admit("admission:downgrade", raw)

    result = store.admit_v1_compat("admission:legacy-replay", raw)
    assert result["request_contract"] == contract.REQUEST_CONTRACT_V1
    assert result["capture_set_count"] == 0


def test_empty_v2_cycle_remains_admissible_unknown_without_raw_reader(
    tmp_path,
):
    request = _v1_request()
    request["contract"] = contract.REQUEST_CONTRACT_V2
    store = RadarCycleStore(tmp_path / "empty-v2.sqlite3")

    result = store.admit("admission:empty-v2", request)

    assert result["request_contract"] == contract.REQUEST_CONTRACT_V2
    assert result["input_count"] == 0
    assert result["capture_set_count"] == 0
    assert result["regions"][0]["precipitation_state"] == "unknown"


def test_additive_capture_schema_migration_preserves_existing_v1_cycle(
    tmp_path,
):
    path = tmp_path / "legacy-v1.sqlite3"
    first_store = RadarCycleStore(path)
    admitted = first_store.admit_v1_compat("admission:legacy", _v1_request())
    with closing(sqlite3.connect(path)) as db, db:
        db.execute(
            "DELETE FROM manifest WHERE key='native_capture_schema_version'"
        )
        for table in (
            "cycle_native_capture_input_regions",
            "cycle_native_capture_inputs",
            "radar_native_capture_inputs",
            "radar_native_capture_members",
            "radar_native_capture_sets",
        ):
            db.execute(f"DROP TABLE {table}")
        db.commit()

    reopened = RadarCycleStore(path)
    snapshot = reopened.snapshot(admitted["cycle_id"])

    assert snapshot["request_contract"] == contract.REQUEST_CONTRACT_V1
    assert snapshot["capture_set_count"] == 0
    with closing(sqlite3.connect(path)) as db, db:
        tables = {
            row[0]
            for row in db.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        assert "radar_capture_sets" in tables
        assert "radar_native_capture_sets" in tables
        assert db.execute(
            "SELECT value FROM manifest "
            "WHERE key='capture_set_schema_version'"
        ).fetchone()[0] == "1"
        assert db.execute(
            "SELECT value FROM manifest "
            "WHERE key='native_capture_schema_version'"
        ).fetchone()[0] == "1"


def test_v2_snapshot_is_bounded_and_excludes_members_paths_and_bytes(tmp_path):
    raw_store = _raw_store(tmp_path)
    request = _request(_store_capture(raw_store))
    parsed = contract.parse_operational_radar_cycle_request(request)
    store_path = tmp_path / "snapshot.sqlite3"
    store = RadarCycleStore(
        store_path,
        raw_artifact_store=raw_store,
        clock=lambda: EVALUATED_AT,
    )
    admitted = store.admit("admission:snapshot", request)
    input_id = parsed.inputs[0].input_id
    store.commit_region(
        parsed.cycle_id,
        "home",
        commit_id="commit:home-wet",
        expected_revision=0,
        evaluation={
            "status": "available",
            "precipitation_state": "wet",
            "evaluated_at": EVALUATED_AT,
            "processor_version": "radar-decoder-v2",
            "reason_code": "processor_result",
            "evidence": {
                "input_ids": [input_id],
                "coverage_status": "complete",
                "freshness_status": "fresh",
                "freshness_max_age_seconds": 60.0,
                "outcome": "wet",
            },
        },
    )
    snapshot = store.snapshot(parsed.cycle_id)

    assert admitted["request_contract"] == contract.REQUEST_CONTRACT_V2
    assert snapshot["capture_set_count"] == 1
    assert snapshot["input_count"] == 1
    assert snapshot["regions"][0]["product_valid_at_min"] == PRODUCT_AT
    assert snapshot["regions"][0]["received_at_min"] == RECEIVED_AT
    encoded = json.dumps(snapshot, sort_keys=True).encode("ascii")
    assert len(encoded) <= 96 * 1024
    assert b"members" not in encoded
    assert b"receipt_id" not in encoded
    assert b"artifact_path" not in encoded
    assert b"payload" not in encoded

    with closing(sqlite3.connect(store_path)) as db, db:
        assert db.execute(
            "SELECT count(*) FROM radar_native_capture_members"
        ).fetchone()[0] == 3
        assert db.execute(
            "SELECT count(*) FROM cycle_native_capture_inputs"
        ).fetchone()[0] == 1


@pytest.mark.parametrize("bad_key", ("artifact_path", "payload"))
def test_v2_rejects_path_or_inline_byte_fields(tmp_path, bad_key):
    raw_store = _raw_store(tmp_path)
    request = _request(_store_capture(raw_store))
    request["inputs"][0]["capture_set"][bad_key] = "../escape"
    with pytest.raises(ValueError, match="keys do not match contract"):
        contract.parse_operational_radar_cycle_request(request)
