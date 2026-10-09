from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from copy import deepcopy
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import threading

import pytest

from storm_tracker_worker import radar_cycle_contract as contract
from storm_tracker_worker import radar_cycle_store as store_module
from storm_tracker_worker.radar_cycle_store import (
    AdmissionConflict,
    CommitConflict,
    RadarCycleStore as _OperationalRadarCycleStore,
    RevisionConflict,
    parse_region_evaluation,
)


NOW = 1_800_000_040.0


class RadarCycleStore(_OperationalRadarCycleStore):
    """Run this preserved v1 suite through the explicit compatibility path."""

    def admit(self, admission_id: str, request: object):
        return self.admit_v1_compat(admission_id, request)


def _decoded_input(
    suffix: str = "a",
    *,
    coverage_status: str = "incomplete",
    region_ids: list[str] | None = None,
) -> dict:
    bbox = [2.0, 39.0, 7.0, 54.0] if coverage_status != "unknown" else None
    return {
        "provider": "kmi",
        "product": "composite-rain-rate",
        "region_ids": region_ids or ["home"],
        "artifact": {
            "artifact_path": f"/share/storm_tracker_worker/{suffix}.json.gz",
            "artifact_sha256": suffix[0].lower() * 64,
            "artifact_bytes": 1234,
            "content_type": "application/json+gzip",
        },
        "provenance": {
            "grade": "decoded_legacy",
            "artifact_kind": "decoded_provider_result",
            "exact_raw_artifacts_complete": False,
            "parser_id": "kmi-decoder-v1",
            "upstream_record_id": "legacy:frame-001",
            "raw_artifact_receipt_id": None,
        },
        "product_valid_at": 1_800_000_000.25,
        "received_at": 1_800_000_007.75,
        "coverage": {"status": coverage_status, "bbox": bbox},
    }


def _raw_input(suffix: str = "b", *, coverage_status: str = "complete") -> dict:
    bbox = [-130.0, 20.0, 10.0, 60.0] if coverage_status != "unknown" else None
    return {
        "provider": "kmi",
        "product": "native-composite",
        "region_ids": ["home", "new-york"],
        "artifact": {
            "artifact_path": f"/share/storm_tracker_worker/{suffix}.h5",
            "artifact_sha256": suffix[0].lower() * 64,
            "artifact_bytes": 987654,
            "content_type": "application/x-hdf5",
        },
        "provenance": {
            "grade": "raw_native",
            "artifact_kind": "upstream_native_product",
            "exact_raw_artifacts_complete": True,
            "parser_id": None,
            "upstream_record_id": "native/archive/member-001.h5",
            "raw_artifact_receipt_id": "receipt:raw-001",
        },
        "product_valid_at": 1_800_000_030.5,
        "received_at": 1_800_000_010.125,
        "coverage": {"status": coverage_status, "bbox": bbox},
    }


def _request(*, inputs: list[dict] | None = None) -> dict:
    return {
        "contract": contract.REQUEST_CONTRACT,
        "requested_at": 1_800_000_020.875,
        "regions": [
            {
                "region_id": "new-york",
                "center_lat": 40.7128,
                "center_lon": -74.006,
                "radius_km": 250.0,
            },
            {
                "region_id": "home",
                "center_lat": 51.025,
                "center_lon": 4.477,
                "radius_km": 250.0,
            },
        ],
        "targets": [
            {
                "target_id": "tracker-new-york",
                "region_id": "new-york",
                "latitude": 40.7128,
                "longitude": -74.006,
            },
            {
                "target_id": "tracker-home",
                "region_id": "home",
                "latitude": 51.025,
                "longitude": 4.477,
            },
        ],
        "inputs": [_raw_input(), _decoded_input()] if inputs is None else inputs,
    }


def _canonical(value: object) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")


def _input_id(parsed: contract.RadarCycleRequest, product: str) -> str:
    return next(value.input_id for value in parsed.inputs if value.product == product)


def _evaluation(
    input_ids: list[str] | tuple[str, ...],
    *,
    status: str = "available",
    precipitation_state: str = "wet",
    evaluated_at: float = NOW,
    processor_version: str | None = "radar-decoder-v7",
    reason_code: str = "processor_result",
    coverage_status: str = "complete",
    freshness_status: str = "fresh",
    freshness_max_age_seconds: float | None = 60.0,
    outcome: str = "wet",
) -> dict:
    return {
        "status": status,
        "precipitation_state": precipitation_state,
        "evaluated_at": evaluated_at,
        "processor_version": processor_version,
        "reason_code": reason_code,
        "evidence": {
            "input_ids": list(input_ids),
            "coverage_status": coverage_status,
            "freshness_status": freshness_status,
            "freshness_max_age_seconds": freshness_max_age_seconds,
            "outcome": outcome,
        },
    }


def _error_evaluation(*, evaluated_at: float = NOW) -> dict:
    return {
        "status": "error",
        "precipitation_state": "unknown",
        "evaluated_at": evaluated_at,
        "processor_version": None,
        "reason_code": "processor_error",
        "evidence": None,
    }


def _counts(path: Path) -> dict[str, int]:
    with closing(sqlite3.connect(path)) as db, db:
        return {
            table: int(db.execute(f"SELECT count(*) FROM {table}").fetchone()[0])
            for table in (
                "cycles",
                "cycle_admissions",
                "cycle_regions",
                "cycle_targets",
                "radar_inputs",
                "cycle_inputs",
                "cycle_input_regions",
                "region_revisions",
                "region_commit_receipts",
            )
        }


def test_admission_persists_canonical_receipt_digests_and_all_opaque_facts(tmp_path):
    path = tmp_path / "radar-cycle.sqlite3"
    raw = _request()
    parsed = contract.parse_radar_cycle_request(raw)
    store = RadarCycleStore(path, clock=lambda: NOW)

    result = store.admit("receipt:cycle-001", raw)

    assert result["cycle_id"] == parsed.cycle_id
    assert result["request_sha256"] == parsed.request_sha256
    assert result["identity_sha256"] == parsed.identity_sha256
    assert result["regions_sha256"] == parsed.regions_sha256
    assert result["targets_sha256"] == parsed.targets_sha256
    assert result["inputs_sha256"] == parsed.inputs_sha256
    assert result["admission_created"] is True
    assert result["already_admitted"] is False
    assert [region["status"] for region in result["regions"]] == ["pending", "pending"]
    assert [region["evaluated_at"] for region in result["regions"]] == [None, None]
    assert [region["precipitation_state"] for region in result["regions"]] == [
        "unknown",
        "unknown",
    ]

    with closing(sqlite3.connect(path)) as db, db:
        db.row_factory = sqlite3.Row
        receipt = db.execute(
            "SELECT * FROM cycle_admissions WHERE admission_id=?",
            ("receipt:cycle-001",),
        ).fetchone()
        assert bytes(receipt["canonical_request"]) == parsed.canonical_bytes
        assert receipt["request_sha256"] == parsed.request_sha256
        assert receipt["request_bytes"] == len(parsed.canonical_bytes)
        assert receipt["requested_at"] == parsed.requested_at
        assert receipt["admitted_at"] == NOW

        rows = {
            row["product"]: row
            for row in db.execute("SELECT * FROM radar_inputs ORDER BY product")
        }
        decoded = rows["composite-rain-rate"]
        assert bytes(decoded["canonical_input"]) == _canonical(
            next(
                value.to_dict()
                for value in parsed.inputs
                if value.product == "composite-rain-rate"
            )
        )
        assert decoded["artifact_path"].endswith("a.json.gz")
        assert decoded["artifact_sha256"] == "a" * 64
        assert decoded["artifact_bytes"] == 1234
        assert decoded["content_type"] == "application/json+gzip"
        assert decoded["provenance_grade"] == "decoded_legacy"
        assert decoded["artifact_kind"] == "decoded_provider_result"
        assert decoded["exact_raw_artifacts_complete"] == 0
        assert decoded["parser_id"] == "kmi-decoder-v1"
        assert decoded["upstream_record_id"] == "legacy:frame-001"
        assert decoded["raw_artifact_receipt_id"] is None
        assert decoded["product_valid_at"] == 1_800_000_000.25
        assert decoded["received_at"] == 1_800_000_007.75
        assert decoded["coverage_status"] == "incomplete"

        native = rows["native-composite"]
        assert native["artifact_path"].endswith("b.h5")
        assert native["provenance_grade"] == "raw_native"
        assert native["artifact_kind"] == "upstream_native_product"
        assert native["exact_raw_artifacts_complete"] == 1
        assert native["parser_id"] is None
        assert native["upstream_record_id"] == "native/archive/member-001.h5"
        assert native["raw_artifact_receipt_id"] == "receipt:raw-001"
        assert native["product_valid_at"] != native["received_at"]

    encoded_result = _canonical(result)
    assert len(encoded_result) <= store_module.MAX_SNAPSHOT_BYTES
    forbidden = ("observations", "raster", "polygon", "cells", "runs", "artifact_path")
    assert all(name not in encoded_result.decode("ascii").lower() for name in forbidden)


def test_exact_retry_is_idempotent_but_reused_admission_with_changed_request_conflicts(
    tmp_path,
):
    path = tmp_path / "radar-cycle.sqlite3"
    store = RadarCycleStore(path, clock=lambda: NOW)
    raw = _request()
    first = store.admit("receipt:one", raw)
    before = _counts(path)

    retry = store.admit("receipt:one", deepcopy(raw))
    assert retry["cycle_id"] == first["cycle_id"]
    assert retry["already_admitted"] is True
    assert retry["admission_created"] is False
    assert _counts(path) == before

    later = deepcopy(raw)
    later["requested_at"] += 900.0
    assert contract.parse_radar_cycle_request(later).cycle_id == first["cycle_id"]
    with pytest.raises(AdmissionConflict, match="different request"):
        store.admit("receipt:one", later)

    changed = deepcopy(raw)
    changed["inputs"][0]["received_at"] += 1.0
    assert contract.parse_radar_cycle_request(changed).cycle_id != first["cycle_id"]
    with pytest.raises(AdmissionConflict, match="different request"):
        store.admit("receipt:one", changed)
    assert _counts(path) == before

    second = store.admit("receipt:two", later)
    assert second["cycle_id"] == first["cycle_id"]
    assert second["already_admitted"] is True
    assert second["admission_created"] is True
    counts = _counts(path)
    assert counts["cycles"] == 1
    assert counts["cycle_admissions"] == 2
    assert counts["radar_inputs"] == 2
    with closing(sqlite3.connect(path)) as db, db:
        digests = db.execute(
            "SELECT request_sha256 FROM cycle_admissions ORDER BY admission_id"
        ).fetchall()
    assert len({row[0] for row in digests}) == 2


def test_region_cas_failure_is_transactional_and_does_not_block_sibling_region(tmp_path):
    path = tmp_path / "radar-cycle.sqlite3"
    parsed = contract.parse_radar_cycle_request(_request())
    store = RadarCycleStore(path, clock=lambda: NOW + 1.0)
    store.admit("receipt:one", parsed)
    native_id = _input_id(parsed, "native-composite")

    home = store.commit_region(
        parsed.cycle_id,
        "home",
        commit_id="commit:home-wet",
        expected_revision=0,
        evaluation=_evaluation([native_id]),
    )
    assert home["revision"] == 1
    assert home["status"] == "available"
    assert home["precipitation_state"] == "wet"

    invalid_dry = _evaluation(
        [native_id],
        precipitation_state="dry",
        outcome="dry",
        evaluated_at=1_800_000_005.0,
    )
    with pytest.raises(ValueError, match="predate input receipt"):
        store.commit_region(
            parsed.cycle_id,
            "new-york",
            commit_id="commit:new-york-invalid-dry",
            expected_revision=0,
            evaluation=invalid_dry,
        )
    assert store.region_snapshot(parsed.cycle_id, "new-york")["revision"] == 0

    new_york = store.commit_region(
        parsed.cycle_id,
        "new-york",
        commit_id="commit:new-york-error",
        expected_revision=0,
        evaluation=_error_evaluation(),
    )
    assert new_york["revision"] == 1
    assert new_york["status"] == "error"
    assert new_york["precipitation_state"] == "unknown"

    with pytest.raises(RevisionConflict, match="current is 1"):
        store.commit_region(
            parsed.cycle_id,
            "home",
            commit_id="commit:home-stale-cas",
            expected_revision=0,
            evaluation=_error_evaluation(),
        )
    assert store.region_snapshot(parsed.cycle_id, "home")["revision"] == 1
    with closing(sqlite3.connect(path)) as db, db:
        revisions = dict(
            db.execute(
                "SELECT region_id,count(*) FROM region_revisions GROUP BY region_id"
            )
        )
    assert revisions == {"home": 2, "new-york": 2}


def test_region_commit_receipt_stores_canonical_operation_and_exact_digest(tmp_path):
    path = tmp_path / "commit-receipt.sqlite3"
    raw = _request(inputs=[_raw_input()])
    parsed = contract.parse_radar_cycle_request(raw)
    evaluation = _evaluation([parsed.inputs[0].input_id])
    store = RadarCycleStore(path, clock=lambda: NOW + 1.0)
    store.admit("receipt:one", raw)

    result = store.commit_region(
        parsed.cycle_id,
        "home",
        commit_id="commit:canonical-001",
        expected_revision=0,
        evaluation=evaluation,
    )
    operation = {
        "contract": store_module.REGION_COMMIT_CONTRACT,
        "cycle_id": parsed.cycle_id,
        "region_id": "home",
        "expected_revision": 0,
        "evaluation": parse_region_evaluation(evaluation).to_dict(),
    }
    canonical = _canonical(operation)
    digest = hashlib.sha256(canonical).hexdigest()

    assert result["method"] == "worker_radar_cycle_region_commit_v1"
    assert result["commit_id"] == "commit:canonical-001"
    assert result["operation_contract"] == store_module.REGION_COMMIT_CONTRACT
    assert result["operation_sha256"] == digest
    assert result["expected_revision"] == 0
    assert result["committed_revision"] == result["revision"] == 1
    assert result["committed_at"] == NOW + 1.0
    assert result["targets"] == [
        {
            "target_id": "tracker-home",
            "latitude": 51.025,
            "longitude": 4.477,
        }
    ]

    with closing(sqlite3.connect(path)) as db, db:
        db.row_factory = sqlite3.Row
        receipt = db.execute(
            "SELECT * FROM region_commit_receipts WHERE commit_id=?",
            ("commit:canonical-001",),
        ).fetchone()
    assert receipt["operation_sha256"] == digest
    assert bytes(receipt["canonical_operation"]) == canonical
    assert receipt["operation_bytes"] == len(canonical)
    assert receipt["cycle_id"] == parsed.cycle_id
    assert receipt["region_id"] == "home"
    assert receipt["expected_revision"] == 0
    assert receipt["committed_revision"] == 1
    assert receipt["committed_at"] == NOW + 1.0
    external = _canonical(result).decode("ascii").lower()
    assert all(
        forbidden not in external
        for forbidden in (
            "canonical_operation",
            "evidence_json",
            "artifact_path",
            "observations",
            "raster",
            "polygon",
            "cells",
        )
    )


def test_same_commit_id_exact_retry_returns_byte_equivalent_original_response(tmp_path):
    path = tmp_path / "exact-commit-retry.sqlite3"
    raw = _request(inputs=[_raw_input()])
    parsed = contract.parse_radar_cycle_request(raw)
    evaluation = _evaluation([parsed.inputs[0].input_id])
    store = RadarCycleStore(path, clock=lambda: NOW + 1.0)
    store.admit("receipt:one", raw)
    first = store.commit_region(
        parsed.cycle_id,
        "home",
        commit_id="commit:exact-001",
        expected_revision=0,
        evaluation=evaluation,
    )
    before = _counts(path)

    reordered = dict(reversed(list(evaluation.items())))
    retry = RadarCycleStore(path, clock=lambda: NOW + 999.0).commit_region(
        parsed.cycle_id,
        "home",
        commit_id="commit:exact-001",
        expected_revision=0,
        evaluation=reordered,
    )
    assert retry == first
    assert _canonical(retry) == _canonical(first)
    assert _counts(path) == before


def test_concurrent_same_commit_id_serializes_to_one_revision_and_one_receipt(tmp_path):
    path = tmp_path / "concurrent-commit-retry.sqlite3"
    raw = _request(inputs=[_raw_input()])
    parsed = contract.parse_radar_cycle_request(raw)
    evaluation = _evaluation([parsed.inputs[0].input_id])
    RadarCycleStore(path, clock=lambda: NOW).admit("receipt:one", raw)
    barrier = threading.Barrier(2)

    def commit_once() -> dict:
        barrier.wait(timeout=10)
        return RadarCycleStore(path, clock=lambda: NOW + 1.0).commit_region(
            parsed.cycle_id,
            "home",
            commit_id="commit:concurrent-001",
            expected_revision=0,
            evaluation=evaluation,
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(lambda _index: commit_once(), range(2)))

    assert results[0] == results[1]
    assert results[0]["revision"] == 1
    counts = _counts(path)
    assert counts["region_revisions"] == 3
    assert counts["region_commit_receipts"] == 1


def test_same_commit_id_with_changed_operation_conflicts_before_revision_cas(tmp_path):
    path = tmp_path / "commit-conflict.sqlite3"
    raw = _request(inputs=[_raw_input()])
    parsed = contract.parse_radar_cycle_request(raw)
    evaluation = _evaluation([parsed.inputs[0].input_id])
    store = RadarCycleStore(path, clock=lambda: NOW)
    store.admit("receipt:one", raw)
    store.commit_region(
        parsed.cycle_id,
        "home",
        commit_id="commit:bound-once",
        expected_revision=0,
        evaluation=evaluation,
    )
    before = _counts(path)

    changed = deepcopy(evaluation)
    changed["reason_code"] = "different_processor_result"
    with pytest.raises(CommitConflict, match="different region operation"):
        store.commit_region(
            parsed.cycle_id,
            "home",
            commit_id="commit:bound-once",
            expected_revision=0,
            evaluation=changed,
        )
    with pytest.raises(CommitConflict, match="different region operation"):
        store.commit_region(
            parsed.cycle_id,
            "new-york",
            commit_id="commit:bound-once",
            expected_revision=0,
            evaluation=evaluation,
        )
    assert _counts(path) == before
    assert store.region_snapshot(parsed.cycle_id, "home")["revision"] == 1
    assert store.region_snapshot(parsed.cycle_id, "new-york")["revision"] == 0


def test_late_retry_returns_original_revision_without_rewinding_newer_head(tmp_path):
    path = tmp_path / "late-retry.sqlite3"
    raw = _request(inputs=[_raw_input()])
    parsed = contract.parse_radar_cycle_request(raw)
    input_id = parsed.inputs[0].input_id
    first_evaluation = _evaluation([input_id], evaluated_at=NOW)
    second_evaluation = _evaluation(
        [input_id],
        evaluated_at=NOW + 1.0,
        reason_code="newer_processor_result",
    )
    store = RadarCycleStore(path, clock=lambda: NOW + 2.0)
    store.admit("receipt:one", raw)
    first = store.commit_region(
        parsed.cycle_id,
        "home",
        commit_id="commit:revision-001",
        expected_revision=0,
        evaluation=first_evaluation,
    )
    second = store.commit_region(
        parsed.cycle_id,
        "home",
        commit_id="commit:revision-002",
        expected_revision=1,
        evaluation=second_evaluation,
    )
    assert second["revision"] == 2

    late_retry = RadarCycleStore(path, clock=lambda: NOW + 500.0).commit_region(
        parsed.cycle_id,
        "home",
        commit_id="commit:revision-001",
        expected_revision=0,
        evaluation=first_evaluation,
    )
    assert late_retry == first
    assert late_retry["revision"] == 1
    head = store.region_snapshot(parsed.cycle_id, "home")
    assert head["revision"] == 2
    assert head["reason_code"] == "newer_processor_result"
    counts = _counts(path)
    assert counts["region_revisions"] == 4
    assert counts["region_commit_receipts"] == 2


@pytest.mark.parametrize("coverage_status", ["incomplete", "unknown"])
def test_incomplete_or_unknown_admitted_input_cannot_become_dry(
    tmp_path, coverage_status
):
    raw = _request(inputs=[_decoded_input(coverage_status=coverage_status)])
    parsed = contract.parse_radar_cycle_request(raw)
    store = RadarCycleStore(tmp_path / f"{coverage_status}.sqlite3", clock=lambda: NOW)
    store.admit("receipt:one", raw)
    input_id = parsed.inputs[0].input_id

    with pytest.raises(ValueError, match="complete coverage on every input"):
        store.commit_region(
            parsed.cycle_id,
            "home",
            commit_id=f"commit:dry-{coverage_status}",
            expected_revision=0,
            evaluation=_evaluation(
                [input_id], precipitation_state="dry", outcome="dry"
            ),
        )
    region = store.region_snapshot(parsed.cycle_id, "home")
    assert region["revision"] == 0
    assert region["precipitation_state"] == "unknown"


def test_no_admitted_inputs_remains_unknown_and_cannot_be_manufactured_into_dry(
    tmp_path,
):
    raw = _request(inputs=[])
    parsed = contract.parse_radar_cycle_request(raw)
    store = RadarCycleStore(tmp_path / "no-inputs.sqlite3", clock=lambda: NOW)
    snapshot = store.admit("receipt:empty", raw)
    assert snapshot["input_count"] == 0
    assert all(
        region["precipitation_state"] == "unknown"
        for region in snapshot["regions"]
    )

    invented_id = "radar-input:" + "a" * 64
    with pytest.raises(ValueError, match="outside this cycle region"):
        store.commit_region(
            parsed.cycle_id,
            "home",
            commit_id="commit:invented-dry",
            expected_revision=0,
            evaluation=_evaluation(
                [invented_id], precipitation_state="dry", outcome="dry"
            ),
        )
    assert store.region_snapshot(parsed.cycle_id, "home")["revision"] == 0


@pytest.mark.parametrize(
    "scope,mutator,match",
    [
        (
            "evidence",
            lambda value: value.update(
                freshness_status="stale", freshness_max_age_seconds=None
            ),
            "available status requires fresh processor evidence",
        ),
        (
            "evidence",
            lambda value: value.update(
                freshness_status="unknown", freshness_max_age_seconds=None
            ),
            "available status requires fresh processor evidence",
        ),
        (
            "evaluation",
            lambda value: value.update(processor_version=None),
            "available status requires processor evidence",
        ),
        (
            "evidence",
            lambda value: value.update(input_ids=[]),
            "wet or dry evidence requires at least one input",
        ),
    ],
)
def test_dry_requires_explicit_processor_freshness_and_nonempty_evidence(
    scope, mutator, match
):
    value = _evaluation(
        ["radar-input:" + "a" * 64],
        precipitation_state="dry",
        outcome="dry",
    )
    mutator(value if scope == "evaluation" else value["evidence"])
    with pytest.raises(ValueError, match=match):
        parse_region_evaluation(value)


def test_region_evidence_input_references_have_a_closed_hard_bound():
    input_ids = [f"radar-input:{index:064x}" for index in range(49)]
    with pytest.raises(ValueError, match="input_ids exceed bounds"):
        parse_region_evaluation(_evaluation(input_ids))

    duplicate = [input_ids[0], input_ids[0]]
    with pytest.raises(ValueError, match="contain duplicates"):
        parse_region_evaluation(_evaluation(duplicate))


def test_fresh_complete_processor_evidence_can_commit_dry_and_preserves_three_clocks(
    tmp_path,
):
    raw = _request(inputs=[_raw_input()])
    parsed = contract.parse_radar_cycle_request(raw)
    input_id = parsed.inputs[0].input_id
    store = RadarCycleStore(tmp_path / "dry.sqlite3", clock=lambda: NOW + 9.0)
    store.admit("receipt:one", raw)

    region = store.commit_region(
        parsed.cycle_id,
        "home",
        commit_id="commit:verified-dry",
        expected_revision=0,
        evaluation=_evaluation(
            [input_id], precipitation_state="dry", outcome="dry"
        ),
    )
    assert region["status"] == "available"
    assert region["precipitation_state"] == "dry"
    assert region["evaluated_at"] == NOW
    assert region["product_valid_at_min"] == 1_800_000_030.5
    assert region["product_valid_at_max"] == 1_800_000_030.5
    assert region["received_at_min"] == 1_800_000_010.125
    assert region["received_at_max"] == 1_800_000_010.125
    assert len(region["evidence_sha256"]) == 64


@pytest.mark.parametrize(
    "evaluated_at,maximum_age,match",
    [
        (1_800_000_005.0, 60.0, "predate input receipt"),
        (1_800_000_200.0, 60.0, "outside declared freshness window"),
    ],
)
def test_declared_fresh_evidence_is_verified_against_receipt_and_product_times(
    tmp_path, evaluated_at, maximum_age, match
):
    raw = _request(inputs=[_raw_input()])
    parsed = contract.parse_radar_cycle_request(raw)
    store = RadarCycleStore(tmp_path / f"fresh-{evaluated_at}.sqlite3", clock=lambda: NOW)
    store.admit("receipt:one", raw)

    with pytest.raises(ValueError, match=match):
        store.commit_region(
            parsed.cycle_id,
            "home",
            commit_id=f"commit:freshness-{evaluated_at}",
            expected_revision=0,
            evaluation=_evaluation(
                [parsed.inputs[0].input_id],
                precipitation_state="dry",
                outcome="dry",
                evaluated_at=evaluated_at,
                freshness_max_age_seconds=maximum_age,
            ),
        )
    assert store.region_snapshot(parsed.cycle_id, "home")["revision"] == 0


@pytest.mark.parametrize("status", ["unknown", "pending", "available", "stale", "error"])
def test_region_status_enum_is_closed(status):
    if status == "pending":
        value = {
            "status": "pending",
            "precipitation_state": "unknown",
            "evaluated_at": NOW,
            "processor_version": None,
            "reason_code": "queued",
            "evidence": None,
        }
    elif status == "stale":
        value = _evaluation(
            ["radar-input:" + "a" * 64],
            status="stale",
            precipitation_state="unknown",
            freshness_status="stale",
            freshness_max_age_seconds=None,
            outcome="unknown",
        )
    elif status == "available":
        value = _evaluation(["radar-input:" + "a" * 64])
    else:
        value = _error_evaluation()
        value["status"] = status
        value["reason_code"] = f"{status}_state"
    assert parse_region_evaluation(value).status.value == status

    invalid = deepcopy(value)
    invalid["status"] = "guess"
    with pytest.raises(ValueError, match="unsupported region status"):
        parse_region_evaluation(invalid)


def test_available_cannot_publish_stale_or_unknown_weather_as_current():
    input_id = "radar-input:" + "a" * 64
    stale = _evaluation(
        [input_id],
        freshness_status="stale",
        freshness_max_age_seconds=None,
    )
    with pytest.raises(ValueError, match="available status requires fresh"):
        parse_region_evaluation(stale)

    unknown = _evaluation(
        [input_id],
        precipitation_state="unknown",
        outcome="unknown",
    )
    with pytest.raises(ValueError, match="explicit wet or dry"):
        parse_region_evaluation(unknown)

    error_with_wet_claim = _evaluation(
        [input_id],
        status="error",
        precipitation_state="unknown",
    )
    with pytest.raises(ValueError, match="cannot expose wet or dry evidence"):
        parse_region_evaluation(error_with_wet_claim)


def test_fact_tables_reject_updates_and_deletes_while_region_history_is_retained(tmp_path):
    path = tmp_path / "append-only.sqlite3"
    parsed = contract.parse_radar_cycle_request(_request(inputs=[_raw_input()]))
    store = RadarCycleStore(path, clock=lambda: NOW)
    store.admit("receipt:one", parsed)
    store.commit_region(
        parsed.cycle_id,
        "home",
        commit_id="commit:append-only",
        expected_revision=0,
        evaluation=_evaluation([parsed.inputs[0].input_id]),
    )

    with closing(sqlite3.connect(path)) as db, db:
        for statement in (
            "UPDATE cycles SET contract='changed'",
            "DELETE FROM cycle_admissions",
            "UPDATE radar_inputs SET provider='changed'",
            "DELETE FROM region_revisions",
            "UPDATE region_commit_receipts SET committed_at=0",
            "DELETE FROM region_commit_receipts",
        ):
            with pytest.raises(sqlite3.IntegrityError, match="append-only table"):
                db.execute(statement)
            db.rollback()
        rows = db.execute(
            "SELECT revision,status FROM region_revisions "
            "WHERE cycle_id=? AND region_id='home' ORDER BY revision",
            (parsed.cycle_id,),
        ).fetchall()
    assert rows == [(0, "pending"), (1, "available")]


class _InjectedFailure(RuntimeError):
    pass


@pytest.mark.parametrize(
    "point",
    [
        "admit.after_inputs",
        "admit.after_cycle",
        "admit.after_admission",
        "admit.before_commit",
    ],
)
def test_admission_fault_points_rollback_every_cycle_fact(tmp_path, point):
    path = tmp_path / f"admit-{point}.sqlite3"

    def inject(actual: str) -> None:
        if actual == point:
            raise _InjectedFailure(point)

    with pytest.raises(_InjectedFailure, match=point):
        RadarCycleStore(path, clock=lambda: NOW, fault_injector=inject).admit(
            "receipt:one", _request()
        )

    RadarCycleStore(path).initialize()
    counts = _counts(path)
    assert all(value == 0 for value in counts.values())


@pytest.mark.parametrize(
    "point",
    [
        "region.after_revision",
        "region.after_head_cas",
        "region.after_receipt",
        "region.before_commit",
    ],
)
def test_region_fault_points_rollback_revision_and_leave_sibling_writable(tmp_path, point):
    path = tmp_path / f"region-{point}.sqlite3"
    parsed = contract.parse_radar_cycle_request(_request(inputs=[_raw_input()]))
    RadarCycleStore(path, clock=lambda: NOW).admit("receipt:one", parsed)

    def inject(actual: str) -> None:
        if actual == point:
            raise _InjectedFailure(point)

    store = RadarCycleStore(path, clock=lambda: NOW + 1.0, fault_injector=inject)
    with pytest.raises(_InjectedFailure, match=point):
        store.commit_region(
            parsed.cycle_id,
            "home",
            commit_id=f"commit:fault-{point}",
            expected_revision=0,
            evaluation=_evaluation([parsed.inputs[0].input_id]),
        )

    recovered = RadarCycleStore(path, clock=lambda: NOW + 2.0)
    assert recovered.region_snapshot(parsed.cycle_id, "home")["revision"] == 0
    sibling = recovered.commit_region(
        parsed.cycle_id,
        "new-york",
        commit_id=f"commit:sibling-{point}",
        expected_revision=0,
        evaluation=_evaluation([parsed.inputs[0].input_id]),
    )
    assert sibling["revision"] == 1
    with closing(sqlite3.connect(path)) as db, db:
        home_revisions = db.execute(
            "SELECT count(*) FROM region_revisions WHERE cycle_id=? AND region_id='home'",
            (parsed.cycle_id,),
        ).fetchone()[0]
    assert home_revisions == 1


def test_result_lost_after_sqlite_commit_recovers_through_exact_commit_receipt(tmp_path):
    path = tmp_path / "result-lost.sqlite3"
    raw = _request(inputs=[_raw_input()])
    parsed = contract.parse_radar_cycle_request(raw)
    evaluation = _evaluation([parsed.inputs[0].input_id])
    RadarCycleStore(path, clock=lambda: NOW).admit("receipt:one", raw)

    def lose_result(point: str) -> None:
        if point == "region.after_commit":
            raise _InjectedFailure("region.after_commit")

    with pytest.raises(_InjectedFailure, match="region.after_commit"):
        RadarCycleStore(
            path,
            clock=lambda: NOW + 1.0,
            fault_injector=lose_result,
        ).commit_region(
            parsed.cycle_id,
            "home",
            commit_id="commit:lost-result",
            expected_revision=0,
            evaluation=evaluation,
        )

    recovered = RadarCycleStore(path, clock=lambda: NOW + 999.0)
    assert recovered.region_snapshot(parsed.cycle_id, "home")["revision"] == 1
    before = _counts(path)
    retry = recovered.commit_region(
        parsed.cycle_id,
        "home",
        commit_id="commit:lost-result",
        expected_revision=0,
        evaluation=evaluation,
    )
    assert retry["revision"] == retry["committed_revision"] == 1
    assert retry["committed_at"] == NOW + 1.0
    assert _counts(path) == before
    assert before["region_commit_receipts"] == 1


def _power_cut(
    path: Path,
    operation: str,
    point: str,
    request: dict,
    evaluation: dict | None = None,
) -> subprocess.CompletedProcess:
    script = r'''
import json
import os
import sys
from storm_tracker_worker.radar_cycle_contract import parse_radar_cycle_request
from storm_tracker_worker.radar_cycle_store import RadarCycleStore

path, operation, point, raw_request, raw_evaluation = sys.argv[1:]
request = json.loads(raw_request)

def fault(actual):
    if actual == point:
        os._exit(73)

store = RadarCycleStore(path, clock=lambda: 1800000041.0, fault_injector=fault)
if operation == "admit":
    store.admit_v1_compat("receipt:cut", request)
elif operation == "region":
    parsed = parse_radar_cycle_request(request)
    store.commit_region(
        parsed.cycle_id,
        "home",
        commit_id="commit:power-cut",
        expected_revision=0,
        evaluation=json.loads(raw_evaluation),
    )
else:
    raise AssertionError(operation)
'''
    return subprocess.run(
        [
            sys.executable,
            "-c",
            script,
            str(path),
            operation,
            point,
            json.dumps(request, separators=(",", ":")),
            json.dumps(evaluation, separators=(",", ":")) if evaluation else "null",
        ],
        cwd=Path(__file__).resolve().parents[1],
        env=os.environ.copy(),
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )


def test_process_power_cut_during_admission_recovers_without_partial_cycle(tmp_path):
    path = tmp_path / "power-admit.sqlite3"
    completed = _power_cut(path, "admit", "admit.after_inputs", _request())
    assert completed.returncode == 73, (completed.stdout, completed.stderr)

    RadarCycleStore(path).initialize()
    assert all(value == 0 for value in _counts(path).values())


@pytest.mark.parametrize(
    "point",
    ["region.after_revision", "region.after_head_cas", "region.after_receipt"],
)
def test_process_power_cut_before_commit_recovers_old_consistent_head(tmp_path, point):
    path = tmp_path / f"power-region-{point}.sqlite3"
    raw = _request(inputs=[_raw_input()])
    parsed = contract.parse_radar_cycle_request(raw)
    store = RadarCycleStore(path, clock=lambda: NOW)
    store.admit("receipt:one", raw)
    evaluation = _evaluation([parsed.inputs[0].input_id])

    completed = _power_cut(path, "region", point, raw, evaluation)
    assert completed.returncode == 73, (completed.stdout, completed.stderr)

    recovered = RadarCycleStore(path)
    region = recovered.region_snapshot(parsed.cycle_id, "home")
    assert region["revision"] == 0
    assert region["status"] == "pending"
    with closing(sqlite3.connect(path)) as db, db:
        assert db.execute("PRAGMA quick_check").fetchone()[0] == "ok"
        assert db.execute(
            "SELECT count(*) FROM region_revisions WHERE cycle_id=? AND region_id='home'",
            (parsed.cycle_id,),
        ).fetchone()[0] == 1
        assert db.execute(
            "SELECT count(*) FROM region_commit_receipts"
        ).fetchone()[0] == 0


def test_process_power_cut_after_commit_uses_receipt_without_duplicate_revision(tmp_path):
    path = tmp_path / "power-region-after-commit.sqlite3"
    raw = _request(inputs=[_raw_input()])
    parsed = contract.parse_radar_cycle_request(raw)
    evaluation = _evaluation([parsed.inputs[0].input_id])
    RadarCycleStore(path, clock=lambda: NOW).admit("receipt:one", raw)

    completed = _power_cut(
        path, "region", "region.after_commit", raw, evaluation
    )
    assert completed.returncode == 73, (completed.stdout, completed.stderr)

    recovered = RadarCycleStore(path, clock=lambda: NOW + 999.0)
    retry = recovered.commit_region(
        parsed.cycle_id,
        "home",
        commit_id="commit:power-cut",
        expected_revision=0,
        evaluation=evaluation,
    )
    assert retry["revision"] == retry["committed_revision"] == 1
    assert retry["committed_at"] == 1_800_000_041.0
    counts = _counts(path)
    assert counts["region_revisions"] == 3
    assert counts["region_commit_receipts"] == 1


def test_store_enforces_database_and_history_bounds_without_mutating_state(
    tmp_path, monkeypatch
):
    with pytest.raises(ValueError, match="at least 1 MiB"):
        RadarCycleStore(tmp_path / "tiny.sqlite3", max_database_bytes=1024)

    path = tmp_path / "bounded.sqlite3"
    raw = _request(inputs=[_raw_input()])
    parsed = contract.parse_radar_cycle_request(raw)
    store = RadarCycleStore(path, clock=lambda: NOW)
    store.admit("receipt:one", raw)
    monkeypatch.setattr(store_module, "MAX_REVISIONS_PER_REGION", 0)
    with pytest.raises(ValueError, match="revision bound"):
        store.commit_region(
            parsed.cycle_id,
            "home",
            commit_id="commit:revision-bound",
            expected_revision=0,
            evaluation=_evaluation([parsed.inputs[0].input_id]),
        )
    assert store.region_snapshot(parsed.cycle_id, "home")["revision"] == 0


@pytest.mark.parametrize(
    "constant,match",
    [
        ("MAX_TOTAL_REGION_COMMITS", "receipt count bound"),
        ("MAX_COMMIT_RECEIPTS_PER_REGION", "per-region commit receipt count bound"),
    ],
)
def test_new_region_commit_receipt_bounds_fail_atomically(
    tmp_path, monkeypatch, constant, match
):
    path = tmp_path / f"{constant}.sqlite3"
    raw = _request(inputs=[_raw_input()])
    parsed = contract.parse_radar_cycle_request(raw)
    store = RadarCycleStore(path, clock=lambda: NOW)
    store.admit("receipt:one", raw)
    before = _counts(path)
    monkeypatch.setattr(store_module, constant, 0)

    with pytest.raises(ValueError, match=match):
        store.commit_region(
            parsed.cycle_id,
            "home",
            commit_id=f"commit:bounded-{constant}",
            expected_revision=0,
            evaluation=_evaluation([parsed.inputs[0].input_id]),
        )
    assert _counts(path) == before
    assert store.region_snapshot(parsed.cycle_id, "home")["revision"] == 0


def test_exact_commit_retry_bypasses_new_receipt_bounds(tmp_path, monkeypatch):
    path = tmp_path / "receipt-retry-at-bound.sqlite3"
    raw = _request(inputs=[_raw_input()])
    parsed = contract.parse_radar_cycle_request(raw)
    evaluation = _evaluation([parsed.inputs[0].input_id])
    store = RadarCycleStore(path, clock=lambda: NOW)
    store.admit("receipt:one", raw)
    first = store.commit_region(
        parsed.cycle_id,
        "home",
        commit_id="commit:before-bound",
        expected_revision=0,
        evaluation=evaluation,
    )
    before = _counts(path)
    monkeypatch.setattr(store_module, "MAX_TOTAL_REGION_COMMITS", 0)
    monkeypatch.setattr(store_module, "MAX_COMMIT_RECEIPTS_PER_REGION", 0)

    retry = store.commit_region(
        parsed.cycle_id,
        "home",
        commit_id="commit:before-bound",
        expected_revision=0,
        evaluation=evaluation,
    )
    assert retry == first
    assert _counts(path) == before


def test_region_operation_byte_bound_fails_before_any_commit_fact(tmp_path, monkeypatch):
    path = tmp_path / "operation-bound.sqlite3"
    raw = _request(inputs=[_raw_input()])
    parsed = contract.parse_radar_cycle_request(raw)
    store = RadarCycleStore(path, clock=lambda: NOW)
    store.admit("receipt:one", raw)
    before = _counts(path)
    monkeypatch.setattr(store_module, "MAX_REGION_OPERATION_BYTES", 1)

    with pytest.raises(ValueError, match="operation exceeds byte budget"):
        store.commit_region(
            parsed.cycle_id,
            "home",
            commit_id="commit:oversize-operation",
            expected_revision=0,
            evaluation=_evaluation([parsed.inputs[0].input_id]),
        )
    assert _counts(path) == before


def test_cycle_and_admission_count_bounds_fail_before_new_facts(tmp_path, monkeypatch):
    path = tmp_path / "admission-bounds.sqlite3"
    store = RadarCycleStore(path, clock=lambda: NOW)
    monkeypatch.setattr(store_module, "MAX_TOTAL_CYCLES", 0)
    with pytest.raises(ValueError, match="cycle count bound"):
        store.admit("receipt:one", _request())
    assert all(value == 0 for value in _counts(path).values())


@pytest.mark.parametrize(
    "constant,match",
    [
        ("MAX_TOTAL_ADMISSIONS", "admission count bound"),
        ("MAX_ADMISSIONS_PER_CYCLE", "per-cycle admission count bound"),
    ],
)
def test_admission_receipt_bounds_are_atomic(tmp_path, monkeypatch, constant, match):
    path = tmp_path / f"{constant}.sqlite3"
    raw = _request(inputs=[])
    store = RadarCycleStore(path, clock=lambda: NOW)
    store.admit("receipt:one", raw)
    before = _counts(path)
    monkeypatch.setattr(store_module, constant, 1)

    with pytest.raises(ValueError, match=match):
        store.admit("receipt:two", raw)
    assert _counts(path) == before


def test_sqlite_durability_foreign_keys_and_manifest_integrity(tmp_path):
    path = tmp_path / "integrity.sqlite3"
    store = RadarCycleStore(path)
    store.initialize()
    with closing(store._connect()) as db, db:
        assert db.execute("PRAGMA journal_mode").fetchone()[0].lower() == "wal"
        assert db.execute("PRAGMA synchronous").fetchone()[0] == 2
        assert db.execute("PRAGMA foreign_keys").fetchone()[0] == 1
        assert db.execute("PRAGMA quick_check").fetchone()[0] == "ok"
        assert db.execute("PRAGMA foreign_key_check").fetchone() is None
        with pytest.raises(sqlite3.IntegrityError, match="FOREIGN KEY"):
            db.execute(
                "INSERT INTO cycle_inputs VALUES('missing','missing','v1')"
            )
        db.rollback()

    with closing(sqlite3.connect(path)) as db, db:
        db.execute(
            "UPDATE manifest SET value='999' WHERE key='schema_version'"
        )
        db.commit()
    with pytest.raises(ValueError, match="incompatible manifest"):
        store.initialize()


class _TracedStore(RadarCycleStore):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.read_trace: list[str] = []

    def _connect_read_only(self) -> sqlite3.Connection:
        db = super()._connect_read_only()
        db.set_trace_callback(self.read_trace.append)
        return db


def test_snapshot_reads_only_cycle_heads_and_targets_not_history_or_input_payloads(tmp_path):
    path = tmp_path / "snapshot.sqlite3"
    raw = _request(inputs=[_raw_input()])
    parsed = contract.parse_radar_cycle_request(raw)
    store = _TracedStore(path, clock=lambda: NOW)
    store.admit("receipt:one", raw)
    store.read_trace.clear()
    for expected in range(3):
        store.commit_region(
            parsed.cycle_id,
            "home",
            commit_id=f"commit:snapshot-{expected}",
            expected_revision=expected,
            evaluation=_evaluation([parsed.inputs[0].input_id]),
        )
    reopened = _TracedStore(path, clock=lambda: NOW)
    snapshot = reopened.snapshot(parsed.cycle_id)
    select_trace = "\n".join(
        statement.lower()
        for statement in reopened.read_trace
        if statement.lstrip().upper().startswith("SELECT")
    )
    assert snapshot["region_count"] == 2
    assert len(snapshot["regions"]) == 2
    assert "region_revisions" not in select_trace
    assert "radar_inputs" not in select_trace
    assert "cycle_inputs" not in select_trace
    assert "cycle_admissions" not in select_trace
    assert "region_commit_receipts" not in select_trace
    assert select_trace.count("select") == 4
    assert "select key,value from manifest" in select_trace


def test_parse_request_is_the_single_store_admission_boundary(tmp_path, monkeypatch):
    calls: list[object] = []
    real_parser = store_module.parse_radar_cycle_request

    def spy(value: object):
        calls.append(value)
        return real_parser(value)

    monkeypatch.setattr(store_module, "parse_radar_cycle_request", spy)
    raw = _request(inputs=[])
    store = RadarCycleStore(tmp_path / "boundary.sqlite3", clock=lambda: NOW)
    store.admit("receipt:one", raw)
    assert calls == [raw]

    invalid = deepcopy(raw)
    invalid["observations"] = []
    with pytest.raises(ValueError, match="keys do not match contract"):
        store.admit("receipt:bad", invalid)
    assert calls == [raw, invalid]
    assert _counts(tmp_path / "boundary.sqlite3")["cycle_admissions"] == 1
