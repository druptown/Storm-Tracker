from __future__ import annotations

import hashlib
import json
from pathlib import Path
import subprocess
import sys
import time

import pytest

from storm_tracker_worker import netatmo_ground_decoder as decoder
from storm_tracker_worker import netatmo_native as native
from storm_tracker_worker.canonical_ground_evidence_store import (
    CanonicalGroundEvidenceStore,
)
from storm_tracker_worker.raw_artifacts import RawArtifactStore


GOLDEN = (
    Path(__file__).parent
    / "fixtures"
    / "netatmo_getpublicdata_ground_golden.json"
)


def _raw(tmp_path: Path) -> RawArtifactStore:
    store = RawArtifactStore((tmp_path / "raw").resolve())
    store.initialize()
    return store


def _capture(raw: RawArtifactStore, payload: bytes, *, job: str = "ground-test"):
    context = {
        "schema": native.NETATMO_QUERY_CONTEXT_SCHEMA,
        "method": "GET",
        "origin": native.NETATMO_ORIGIN,
        "path": native.NETATMO_PUBLIC_DATA_PATH,
        "query_identity": "ground-test-query",
        "query_kind": "local",
        "requested_center": {"latitude": 51.05, "longitude": 4.42},
        "requested_radius_km": 10.0,
        "wire_geometry": "bounding_box_not_geodesic_circle",
        "parameters": [],
        "coverage_status": "unknown_not_proven_by_station_count",
    }
    context_sha = native._query_context_sha256(context)
    content_sha = hashlib.sha256(payload).hexdigest()
    receipt = raw.store(
        provider="netatmo",
        product=native.NETATMO_JSON_PRODUCT,
        fetch_id=f"{job}:response_json",
        content_type="application/json",
        received_at=time.time() - 1.0,
        upstream_id=(
            "netatmo/getpublicdata"
            f"|query_sha256={context_sha}|content_sha256={content_sha}"
        ),
        provenance=native._native_provenance(context, transport_attempt=1),
        payload=payload,
    )
    return raw.commit_capture(
        provider="netatmo",
        product=native.NETATMO_CAPTURE_PRODUCT,
        fetch_id=f"{job}:capture",
        upstream_capture_id=(
            f"{native.NETATMO_CAPTURE_PRODUCT}"
            f"|query_sha256={context_sha}|content_sha256={content_sha}"
        ),
        product_timestamp=None,
        members={"response_json": receipt.receipt_id},
    )


def _ground(tmp_path: Path, raw: RawArtifactStore, **kwargs):
    return CanonicalGroundEvidenceStore(
        (tmp_path / "ground.sqlite3").resolve(),
        raw_artifact_reader=raw,
        **kwargs,
    )


def _decode(tmp_path: Path, payload: bytes, *, job: str = "ground-test"):
    raw = _raw(tmp_path)
    capture = _capture(raw, payload, job=job)
    ground = _ground(tmp_path, raw)
    result = decoder.decode_netatmo_ground_capture(
        capture.capture_id,
        raw_store=raw,
        ground_store=ground,
    )
    return raw, capture, ground, result


def _compact(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
    ).encode()


def test_realistic_golden_preserves_station_module_time_and_exact_lexemes(tmp_path):
    payload = GOLDEN.read_bytes()
    _raw_store, capture, _store, result = _decode(tmp_path, payload)

    assert result.station_count == 2
    assert result.module_count == 3
    assert result.fact_count == 7
    assert result.operational_admission_eligible is False
    assert result.ground_confirmation_eligible is False
    assert all(item.fact.capture_id == capture.capture_id for item in result.fact_receipts)

    facts = {item.fact.source_record_locator: item.fact for item in result.fact_receipts}
    soft = facts[
        "/body/0/measures/02:00:00:aa:bb:cc/rain_live"
    ]
    assert soft.measurement.exact_value == "0.015"
    assert soft.measurement.variable == "netatmo_native.rain_live"
    assert soft.measurement.unit == decoder.NETATMO_NATIVE_UNSPECIFIED_UNIT
    assert soft.observed_at == 1722000000.0
    assert soft.representative_interval_start == soft.observed_at
    assert soft.representative_interval_end == soft.observed_at
    assert soft.station.provider_station_id == "70:ee:50:29:59:56"
    assert soft.station.site_id == "netatmo-public:70:ee:50:29:59:56"
    assert soft.station.longitude_decimal == "4.4180"
    assert soft.station.latitude_decimal == "51.0470"
    assert soft.station.elevation_m_decimal == "6"

    accumulation_name_only = facts[
        "/body/0/measures/02:00:00:aa:bb:cc/rain_60min"
    ]
    assert accumulation_name_only.measurement.exact_value == "0.10"
    assert accumulation_name_only.representative_interval_start == 1722000000.0
    expected_record = b'"rain_60min": 0.10'
    assert accumulation_name_only.source_record_sha256 == hashlib.sha256(
        expected_record
    ).hexdigest()

    escaped_module = facts[
        "/body/1/measures/05:00:00:rain~0module~12/res/1721999900/0"
    ]
    assert escaped_module.measurement.exact_value == "1e-1"
    assert escaped_module.measurement.variable == "netatmo_native.type.rain"


def test_zero_missing_and_soft_are_distinct_without_wet_or_dry_derivation(tmp_path):
    _raw_store, _capture_receipt, _store, result = _decode(
        tmp_path, GOLDEN.read_bytes()
    )
    facts = {item.fact.source_record_locator: item.fact for item in result.fact_receipts}
    zero = facts["/body/1/measures/05:00:00:rain~0module~12/rain"]
    missing = facts[
        "/body/1/measures/05:00:00:rain~0module~12/rain_live"
    ]
    soft = facts[
        "/body/0/measures/02:00:00:aa:bb:cc/rain_live"
    ]
    assert (zero.measurement.state, zero.measurement.exact_value) == ("observed", "0")
    assert (missing.measurement.state, missing.measurement.exact_value) == (
        "missing",
        None,
    )
    assert (soft.measurement.state, soft.measurement.exact_value) == (
        "observed",
        "0.015",
    )
    metadata = result.to_metadata()
    assert metadata["wet_claim"] is False
    assert metadata["dry_claim"] is False
    assert metadata["coverage_claim"] is False
    assert metadata["ground_confirmation_eligible"] is False


def test_every_fact_exposes_uncertified_unit_and_temporal_support_qc(tmp_path):
    _raw_store, _capture_receipt, _store, result = _decode(
        tmp_path, GOLDEN.read_bytes()
    )
    for receipt in result.fact_receipts:
        fact = receipt.fact
        assert fact.measurement.unit == "netatmo_native_unit_unspecified"
        flags = {item.flag: item for item in fact.provider_qc}
        assert "netatmo.decoder.temporal_support_uncertified" in flags
        assert "record-time anchor" in (
            flags["netatmo.decoder.temporal_support_uncertified"].text or ""
        )
        assert "netatmo.decoder.unit_semantics_uncertified" in flags
        assert fact.representative_interval_start == fact.representative_interval_end
    # Names remain opaque upstream identities. No physical unit is assigned.
    assert {item.fact.measurement.unit for item in result.fact_receipts} == {
        "netatmo_native_unit_unspecified"
    }
    assert not any(
        item.fact.measurement.unit in {"mm", "mm/h"}
        for item in result.fact_receipts
    )


def test_exact_retry_returns_same_receipts_and_does_not_add_rows(tmp_path):
    raw = _raw(tmp_path)
    capture = _capture(raw, GOLDEN.read_bytes())
    ground = _ground(tmp_path, raw)
    first = decoder.decode_netatmo_ground_capture(
        capture.capture_id, raw_store=raw, ground_store=ground
    )
    second = decoder.decode_netatmo_ground_capture(
        capture.capture_id, raw_store=raw, ground_store=ground
    )
    assert [item.receipt_sha256 for item in second.fact_receipts] == [
        item.receipt_sha256 for item in first.fact_receipts
    ]
    assert ground.stats()["ground_facts"] == first.fact_count


def test_complete_decoder_batch_reverifies_raw_authority_once(tmp_path):
    raw = _raw(tmp_path)
    capture = _capture(raw, GOLDEN.read_bytes())

    class CountingAuthority:
        reads = 0

        def load_capture_receipt(self, capture_id):
            self.reads += 1
            return raw.load_capture_receipt(capture_id)

    authority = CountingAuthority()
    ground = CanonicalGroundEvidenceStore(
        (tmp_path / "ground.sqlite3").resolve(),
        raw_artifact_reader=authority,
    )
    result = decoder.decode_netatmo_ground_capture(
        capture.capture_id, raw_store=raw, ground_store=ground
    )
    assert result.fact_count == 7
    assert authority.reads == 1
    assert ground.stats()["ground_facts"] == 7


def test_stale_record_time_is_preserved_not_filtered_or_relabeled(tmp_path):
    payload = _compact({
        "status": "ok",
        "body": [{
            "_id": "stale",
            "place": {"location": [4.4, 51.0]},
            "measures": {"rain-module": {
                "rain_timeutc": 1,
                "rain_live": 0.001,
            }},
        }],
    })
    _raw_store, _capture_receipt, _store, result = _decode(tmp_path, payload)
    fact = result.fact_receipts[0].fact
    assert fact.observed_at == 1.0
    assert fact.measurement.exact_value == "0.001"
    assert not any("stale" in item.flag for item in fact.provider_qc)


@pytest.mark.parametrize(
    "payload,match",
    [
        (
            b'{"status":"ok","status":"ok","body":[]}',
            "dubbele objectkey",
        ),
        (
            b'{"status":"ok","body":[],"x":NaN}',
            "ongeldige waarde",
        ),
        (
            _compact({
                "status": "ok",
                "body": [{
                    "_id": "S",
                    "place": {"location": [4, 51]},
                    "measures": {"M": {"type": ["rain", "temperature"], "res": {"1": [0]}}},
                }],
            }),
            "arraylengtes",
        ),
        (
            _compact({
                "status": "ok",
                "body": [{
                    "_id": "S",
                    "place": {"location": [4, 51]},
                    "measures": {"M": {"rain_5min": 1, "rain_timeutc": 1}},
                }],
            }),
            "onbekende rain-variabele",
        ),
        (
            _compact({
                "status": "ok",
                "body": [{
                    "_id": "S",
                    "place": {"location": [4, 51]},
                    "measures": {"M": {"rain_live": 1}},
                }],
            }),
            "mist rain_timeutc",
        ),
        (
            _compact({
                "status": "ok",
                "body": [{
                    "_id": "S",
                    "place": {"location": [4, 51]},
                    "measures": {"M": {"type": ["rain"], "res": {"1.0": [1]}}},
                }],
            }),
            "ambigue tijdsemantiek",
        ),
        (
            b'{"status":"ok","body":[],"access_token":"secret"}',
            "authmetadata",
        ),
    ],
)
def test_schema_ambiguity_fails_before_any_fact(tmp_path, payload, match):
    raw = _raw(tmp_path)
    capture = _capture(raw, payload)
    ground = _ground(tmp_path, raw)
    with pytest.raises(decoder.NetatmoGroundDecoderContractError, match=match):
        decoder.decode_netatmo_ground_capture(
            capture.capture_id, raw_store=raw, ground_store=ground
        )
    assert ground.stats()["ground_facts"] == 0


def test_raw_tamper_is_rejected_before_json_or_ground_commit(tmp_path):
    raw = _raw(tmp_path)
    capture = _capture(raw, GOLDEN.read_bytes())
    receipt = raw.load_receipt(capture.members[0].receipt_id)
    assert receipt is not None
    artifact = Path(receipt.artifact_path)
    changed = bytearray(artifact.read_bytes())
    changed[-2] ^= 1
    artifact.write_bytes(bytes(changed))
    ground = _ground(tmp_path, raw)
    with pytest.raises(decoder.NetatmoGroundCaptureError, match="byte-exact"):
        decoder.decode_netatmo_ground_capture(
            capture.capture_id, raw_store=raw, ground_store=ground
        )
    assert ground.stats()["ground_facts"] == 0


def test_response_and_fact_counts_are_hard_bounded_before_commit(tmp_path, monkeypatch):
    raw = _raw(tmp_path)
    capture = _capture(raw, GOLDEN.read_bytes())
    ground = _ground(tmp_path, raw)
    monkeypatch.setattr(decoder, "MAX_DECODE_FACTS", 1)
    with pytest.raises(decoder.NetatmoGroundDecoderCapacityError, match="feitengrens"):
        decoder.decode_netatmo_ground_capture(
            capture.capture_id, raw_store=raw, ground_store=ground
        )
    assert ground.stats()["ground_facts"] == 0


def test_absolute_deadline_is_checked_before_and_after_raw_load(tmp_path, monkeypatch):
    raw = _raw(tmp_path)
    capture = _capture(raw, GOLDEN.read_bytes())
    ground = _ground(tmp_path, raw)
    with pytest.raises(decoder.NetatmoGroundDecoderDeadlineError):
        decoder.decode_netatmo_ground_capture(
            capture.capture_id,
            raw_store=raw,
            ground_store=ground,
            deadline_at=time.monotonic() - 1,
        )

    original = raw.load_capture_bundle_bytes

    def delayed(capture_id):
        time.sleep(0.03)
        return original(capture_id)

    monkeypatch.setattr(raw, "load_capture_bundle_bytes", delayed)
    with pytest.raises(decoder.NetatmoGroundDecoderDeadlineError):
        decoder.decode_netatmo_ground_capture(
            capture.capture_id,
            raw_store=raw,
            ground_store=ground,
            deadline_at=time.monotonic() + 0.01,
        )
    assert ground.stats()["ground_facts"] == 0


def test_committed_then_interrupted_batch_is_recovered_by_exact_retry(tmp_path):
    raw = _raw(tmp_path)
    capture = _capture(raw, GOLDEN.read_bytes())
    fired = False

    def fault(point: str) -> None:
        nonlocal fired
        if point == "commit_batch.after_commit" and not fired:
            fired = True
            raise RuntimeError("synthetic postcommit interruption")

    failing = _ground(tmp_path, raw, fault_injector=fault)
    with pytest.raises(RuntimeError, match="postcommit"):
        decoder.decode_netatmo_ground_capture(
            capture.capture_id, raw_store=raw, ground_store=failing
        )
    recovered = CanonicalGroundEvidenceStore(
        (tmp_path / "ground.sqlite3").resolve(), raw_artifact_reader=raw
    )
    result = decoder.decode_netatmo_ground_capture(
        capture.capture_id, raw_store=raw, ground_store=recovered
    )
    assert result.fact_count == 7
    assert recovered.stats()["ground_facts"] == 7


def test_candidate_is_process_replayable_and_remains_unwired(tmp_path):
    raw = _raw(tmp_path)
    capture = _capture(raw, GOLDEN.read_bytes())
    ground_path = (tmp_path / "ground.sqlite3").resolve()
    # Initialize before the independent process, then let the child perform the
    # full verified raw replay and canonical commits.
    CanonicalGroundEvidenceStore(ground_path, raw_artifact_reader=raw)
    source = (
        "from storm_tracker_worker.raw_artifacts import RawArtifactStore;"
        "from storm_tracker_worker.canonical_ground_evidence_store import CanonicalGroundEvidenceStore;"
        "from storm_tracker_worker.netatmo_ground_decoder import decode_netatmo_ground_capture;"
        "import sys;"
        "r=RawArtifactStore(sys.argv[1]);r.initialize();"
        "g=CanonicalGroundEvidenceStore(sys.argv[2],raw_artifact_reader=r);"
        "x=decode_netatmo_ground_capture(sys.argv[3],raw_store=r,ground_store=g);"
        "print(x.fact_count)"
    )
    completed = subprocess.run(
        [
            sys.executable,
            "-c",
            source,
            str(raw.root),
            str(ground_path),
            capture.capture_id,
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert completed.returncode == 0, completed.stderr
    assert completed.stdout.strip() == "7"
    assert "netatmo_ground_decoder" not in Path(
        "storm_tracker_worker/service.py"
    ).read_text(encoding="utf-8")
    assert "netatmo_ground_decoder" not in Path(
        "storm_tracker_worker/provider_capture_registry.py"
    ).read_text(encoding="utf-8")
