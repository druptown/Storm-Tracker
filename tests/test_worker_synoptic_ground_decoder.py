from __future__ import annotations

import ast
from dataclasses import replace
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import time

import pytest

from storm_tracker_worker import synoptic_ground_decoder as decoder
from storm_tracker_worker import synoptic_native as native
from storm_tracker_worker.canonical_ground_evidence_store import (
    CanonicalGroundEvidenceConflict,
    CanonicalGroundEvidenceStore,
)
from storm_tracker_worker.raw_artifacts import RawArtifactStore


GOLDEN = (
    Path(__file__).parent
    / "fixtures"
    / "synoptic_timeseries_ground_golden.json"
)


def _raw(tmp_path: Path) -> RawArtifactStore:
    store = RawArtifactStore((tmp_path / "raw").resolve())
    store.initialize()
    return store


def _context() -> dict[str, object]:
    return native._query_context(
        native.SynopticQuery(
            latitude=40.7128,
            longitude=-74.0060,
            radius_km=20.0,
            query_key="tracker:new-york",
            query_kind="local",
        )
    )


def _capture(
    raw: RawArtifactStore,
    payload: bytes,
    *,
    job: str = "synoptic-ground-test",
    context: dict[str, object] | None = None,
    member_role: str = "response_json",
):
    exact_context = _context() if context is None else context
    context_sha = native._query_context_sha256(exact_context)
    content_sha = hashlib.sha256(payload).hexdigest()
    receipt = raw.store(
        provider="synoptic",
        product=native.SYNOPTIC_JSON_PRODUCT,
        fetch_id=f"{job}:response_json:attempt:1",
        content_type="application/json",
        received_at=time.time() - 1.0,
        upstream_id=(
            "synoptic/stations/timeseries"
            f"|query_sha256={context_sha}"
            f"|request_attempt=1|content_sha256={content_sha}"
        ),
        provenance=native._native_provenance(
            exact_context, request_attempt=1
        ),
        payload=payload,
    )
    return raw.commit_capture(
        provider="synoptic",
        product=native.SYNOPTIC_CAPTURE_PRODUCT,
        fetch_id=f"{job}:capture",
        upstream_capture_id=(
            f"{native.SYNOPTIC_CAPTURE_PRODUCT}"
            f"|query_sha256={context_sha}|content_sha256={content_sha}"
        ),
        product_timestamp=None,
        members={member_role: receipt.receipt_id},
    )


def _ground(tmp_path: Path, raw: RawArtifactStore) -> CanonicalGroundEvidenceStore:
    return CanonicalGroundEvidenceStore(
        (tmp_path / "ground.sqlite3").resolve(),
        raw_artifact_reader=raw,
    )


def _decode(tmp_path: Path, payload: bytes = b""):
    exact_payload = GOLDEN.read_bytes() if not payload else payload
    raw = _raw(tmp_path)
    capture = _capture(raw, exact_payload)
    ground = _ground(tmp_path, raw)
    result = decoder.decode_synoptic_ground_capture(
        capture.capture_id,
        raw_store=raw,
        ground_store=ground,
    )
    return raw, capture, ground, result


def _document() -> dict[str, object]:
    return json.loads(GOLDEN.read_text(encoding="utf-8"))


def _compact(value: object) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")


def _facts_by_locator(result):
    return {
        item.fact.source_record_locator: item.fact
        for item in result.fact_receipts
    }


def test_golden_preserves_exact_station_sets_values_and_time_semantics(tmp_path):
    _raw_store, capture, _store, result = _decode(tmp_path)

    assert result.station_count == 1
    assert result.sensor_set_count == 4
    assert result.interval_fact_count == 6
    assert result.accumulated_fact_count == 6
    assert result.fact_count == 12
    assert result.operational_admission_eligible is False
    assert result.ground_confirmation_eligible is False
    assert all(item.fact.capture_id == capture.capture_id for item in result.fact_receipts)

    facts = _facts_by_locator(result)
    first = facts[
        "/STATION/0/OBSERVATIONS/precip_intervals_set_1d/0"
    ]
    second = facts[
        "/STATION/0/OBSERVATIONS/precip_intervals_set_1d/1"
    ]
    exponential = facts[
        "/STATION/0/OBSERVATIONS/precip_intervals_set_2d/2"
    ]
    accumulated = facts[
        "/STATION/0/OBSERVATIONS/precip_accumulated_set_1d/1"
    ]

    t0 = datetime(2026, 8, 2, 12, 0, tzinfo=timezone.utc).timestamp()
    t1 = datetime(2026, 8, 2, 12, 5, tzinfo=timezone.utc).timestamp()
    assert first.measurement.state == "unknown"
    assert first.measurement.exact_value is None
    assert first.representative_interval_start == t0
    assert first.representative_interval_end == t0
    first_flags = {item.flag: item for item in first.provider_qc}
    assert first_flags["synoptic.decoder.interval_start_unknown"].value == "0.125"

    assert second.measurement.state == "observed"
    assert second.measurement.exact_value == "0.000"
    assert second.representative_interval_start == t0
    assert second.representative_interval_end == t1
    assert second.observed_at == t1
    assert exponential.measurement.exact_value == "2e-2"

    assert accumulated.measurement.exact_value == "0.125"
    assert accumulated.representative_interval_start == t1
    assert accumulated.representative_interval_end == t1
    assert "request_window_cumulative" in accumulated.measurement.variable
    accumulated_flags = {item.flag: item for item in accumulated.provider_qc}
    assert "synoptic.decoder.request_window_start_unbound" in accumulated_flags

    assert first.station.provider_station_id == "KJFK"
    assert first.station.site_id == "synoptic:KJFK"
    assert first.station.network == "synoptic-mnet:1"
    assert first.station.latitude_decimal == "40.6392"
    assert first.station.longitude_decimal == "-73.7639"
    assert first.station.elevation_state == "unknown"
    assert first.station.elevation_m_decimal is None
    assert first.measurement.unit == "Millimeters"


def test_multiple_sets_are_all_independent_never_selected_merged_or_summed(tmp_path):
    _raw_store, _capture_receipt, _store, result = _decode(tmp_path)
    variables = {item.fact.measurement.variable for item in result.fact_receipts}
    assert variables == {
        "synoptic.native.precip_intervals_set_1d.between_consecutive_reports",
        "synoptic.native.precip_intervals_set_2d.between_consecutive_reports",
        "synoptic.native.precip_accumulated_set_1d.request_window_cumulative",
        "synoptic.native.precip_accumulated_set_2d.request_window_cumulative",
    }
    assert len([item for item in result.fact_receipts if "set_1d" in item.fact.measurement.variable]) == 6
    assert len([item for item in result.fact_receipts if "set_2d" in item.fact.measurement.variable]) == 6
    assert not any("combined" in variable or "selected" in variable for variable in variables)


def test_native_qc_and_provisional_status_do_not_invalidate_flagged_value(tmp_path):
    _raw_store, _capture_receipt, _store, result = _decode(tmp_path)
    fact = _facts_by_locator(result)[
        "/STATION/0/OBSERVATIONS/precip_intervals_set_1d/1"
    ]
    assert fact.measurement.state == "observed"
    assert fact.measurement.exact_value == "0.000"
    flags: dict[str, list] = {}
    for item in fact.provider_qc:
        flags.setdefault(item.flag, []).append(item)
    assert flags["synoptic.provider.provisional"][0].value == "true"
    assert flags["synoptic.qc_flagged"][0].value == "true"
    assert {item.value for item in flags["synoptic.qc.test_applied"]} == {
        "sl_range_check",
        "ma_range_check",
    }
    assert flags["synoptic.sensor_set"][0].value == "precip_intervals_set_1d"
    assert "derived_from" in (flags["synoptic.sensor_metadata"][0].value or "")


def test_official_station_level_qc_flag_boolean_is_preserved_for_every_set(tmp_path):
    document = _document()
    document["STATION"][0]["QC_FLAGGED"] = False
    _raw_store, _capture_receipt, _store, result = _decode(
        tmp_path, _compact(document)
    )
    assert result.fact_count == 12
    for receipt in result.fact_receipts:
        flags = [
            item
            for item in receipt.fact.provider_qc
            if item.flag == "synoptic.qc_flagged"
        ]
        assert len(flags) == 1
        assert flags[0].value == "false"
        assert receipt.fact.measurement.state in {"observed", "missing", "unknown"}


def test_first_interval_null_zero_and_missing_are_not_conflated(tmp_path):
    _raw_store, _capture_receipt, _store, result = _decode(tmp_path)
    facts = _facts_by_locator(result)
    first_null = facts[
        "/STATION/0/OBSERVATIONS/precip_intervals_set_2d/0"
    ]
    exact_zero = facts[
        "/STATION/0/OBSERVATIONS/precip_intervals_set_1d/1"
    ]
    later_null = facts[
        "/STATION/0/OBSERVATIONS/precip_intervals_set_1d/2"
    ]
    accumulated_null = facts[
        "/STATION/0/OBSERVATIONS/precip_accumulated_set_2d/0"
    ]
    assert (first_null.measurement.state, first_null.measurement.exact_value) == (
        "unknown",
        None,
    )
    assert (exact_zero.measurement.state, exact_zero.measurement.exact_value) == (
        "observed",
        "0.000",
    )
    assert (later_null.measurement.state, later_null.measurement.exact_value) == (
        "missing",
        None,
    )
    assert (
        accumulated_null.measurement.state,
        accumulated_null.measurement.exact_value,
    ) == ("missing", None)
    metadata = result.to_metadata()
    assert metadata["wet_claim"] is False
    assert metadata["dry_claim"] is False
    assert metadata["coverage_claim"] is False
    assert metadata["ground_confirmation_eligible"] is False


def test_exact_retry_is_idempotent_and_changed_same_observation_conflicts(tmp_path):
    raw = _raw(tmp_path)
    capture = _capture(raw, GOLDEN.read_bytes())
    ground = _ground(tmp_path, raw)
    first = decoder.decode_synoptic_ground_capture(
        capture.capture_id, raw_store=raw, ground_store=ground
    )
    second = decoder.decode_synoptic_ground_capture(
        capture.capture_id, raw_store=raw, ground_store=ground
    )
    assert [item.receipt_sha256 for item in second.fact_receipts] == [
        item.receipt_sha256 for item in first.fact_receipts
    ]
    assert ground.stats()["ground_facts"] == first.fact_count

    original = next(
        item.fact
        for item in first.fact_receipts
        if item.fact.measurement.state == "observed"
    )
    changed = replace(
        original,
        measurement=replace(original.measurement, exact_value="999"),
    )
    with pytest.raises(CanonicalGroundEvidenceConflict):
        ground.commit(changed)
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
    result = decoder.decode_synoptic_ground_capture(
        capture.capture_id, raw_store=raw, ground_store=ground
    )
    assert result.fact_count == 12
    assert authority.reads == 1
    assert ground.stats()["ground_facts"] == 12


def test_raw_tamper_and_missing_capture_fail_before_any_ground_fact(tmp_path):
    raw = _raw(tmp_path)
    capture = _capture(raw, GOLDEN.read_bytes())
    receipt = raw.load_receipt(capture.members[0].receipt_id)
    assert receipt is not None
    artifact = Path(receipt.artifact_path)
    changed = bytearray(artifact.read_bytes())
    changed[-2] ^= 1
    artifact.write_bytes(bytes(changed))
    ground = _ground(tmp_path, raw)
    with pytest.raises(decoder.SynopticGroundCaptureError, match="byte-exact"):
        decoder.decode_synoptic_ground_capture(
            capture.capture_id, raw_store=raw, ground_store=ground
        )
    assert ground.stats()["ground_facts"] == 0

    with pytest.raises(decoder.SynopticGroundCaptureError, match="incompleet"):
        decoder.decode_synoptic_ground_capture(
            "capture-" + "0" * 64,
            raw_store=raw,
            ground_store=ground,
        )
    assert ground.stats()["ground_facts"] == 0


def test_wrong_member_role_and_tampered_query_semantics_are_rejected(tmp_path):
    raw = _raw(tmp_path)
    wrong_role = _capture(
        raw,
        GOLDEN.read_bytes(),
        job="wrong-role",
        member_role="payload",
    )
    ground = _ground(tmp_path, raw)
    with pytest.raises(decoder.SynopticGroundCaptureError, match="complete"):
        decoder.decode_synoptic_ground_capture(
            wrong_role.capture_id, raw_store=raw, ground_store=ground
        )

    context = _context()
    context["parameters"] = [dict(item) for item in context["parameters"]]
    context["parameters"][3]["value"] = "0"
    tampered = _capture(
        raw,
        GOLDEN.read_bytes(),
        job="bad-context",
        context=context,
    )
    with pytest.raises(decoder.SynopticGroundCaptureError, match="informatiebehoud"):
        decoder.decode_synoptic_ground_capture(
            tampered.capture_id, raw_store=raw, ground_store=ground
        )
    assert ground.stats()["ground_facts"] == 0


@pytest.mark.parametrize("bad_value", [False, "", "0"])
def test_false_and_empty_or_text_values_never_become_zero_or_dry(tmp_path, bad_value):
    document = _document()
    document["STATION"][0]["OBSERVATIONS"]["precip_intervals_set_1d"][1] = bad_value
    raw = _raw(tmp_path)
    capture = _capture(raw, _compact(document))
    ground = _ground(tmp_path, raw)
    with pytest.raises(decoder.SynopticGroundDecoderContractError):
        decoder.decode_synoptic_ground_capture(
            capture.capture_id, raw_store=raw, ground_store=ground
        )
    assert ground.stats()["ground_facts"] == 0


def test_incomplete_multiset_and_nonconsecutive_time_axis_fail_closed(tmp_path):
    document = _document()
    del document["STATION"][0]["SENSOR_VARIABLES"]["precip_intervals"][
        "precip_intervals_set_2d"
    ]
    raw = _raw(tmp_path)
    capture = _capture(raw, _compact(document), job="missing-set")
    ground = _ground(tmp_path, raw)
    with pytest.raises(decoder.SynopticGroundDecoderContractError):
        decoder.decode_synoptic_ground_capture(
            capture.capture_id, raw_store=raw, ground_store=ground
        )
    assert ground.stats()["ground_facts"] == 0

    duplicate_time = _document()
    duplicate_time["STATION"][0]["OBSERVATIONS"]["date_time"][1] = (
        duplicate_time["STATION"][0]["OBSERVATIONS"]["date_time"][0]
    )
    capture = _capture(raw, _compact(duplicate_time), job="duplicate-time")
    with pytest.raises(
        decoder.SynopticGroundDecoderContractError, match="strikt oplopend"
    ):
        decoder.decode_synoptic_ground_capture(
            capture.capture_id, raw_store=raw, ground_store=ground
        )
    assert ground.stats()["ground_facts"] == 0


def test_fact_count_and_absolute_deadline_are_hard_bounded_before_commit(
    tmp_path, monkeypatch
):
    raw = _raw(tmp_path)
    capture = _capture(raw, GOLDEN.read_bytes())
    ground = _ground(tmp_path, raw)
    monkeypatch.setattr(decoder, "MAX_DECODE_FACTS", 1)
    with pytest.raises(decoder.SynopticGroundDecoderCapacityError, match="feitengrens"):
        decoder.decode_synoptic_ground_capture(
            capture.capture_id, raw_store=raw, ground_store=ground
        )
    assert ground.stats()["ground_facts"] == 0

    with pytest.raises(decoder.SynopticGroundDecoderDeadlineError):
        decoder.decode_synoptic_ground_capture(
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
    with pytest.raises(decoder.SynopticGroundDecoderDeadlineError):
        decoder.decode_synoptic_ground_capture(
            capture.capture_id,
            raw_store=raw,
            ground_store=ground,
            deadline_at=time.monotonic() + 0.01,
        )
    assert ground.stats()["ground_facts"] == 0


def test_decoder_remains_unwired_and_contains_no_network_or_weather_decision_path():
    path = Path(decoder.__file__)
    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source)
    imports = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, (ast.Import, ast.ImportFrom))
        for alias in node.names
    }
    assert not any(
        name.endswith("service")
        or "homeassistant" in name
        or "mqtt" in name
        or "urllib" in name
        or "requests" in name
        for name in imports
    )
    assert "decode_synoptic_ground_capture" not in Path(
        native.__file__
    ).read_text(encoding="utf-8")
    result_keys = decoder.SynopticGroundDecodeResult.to_metadata
    assert callable(result_keys)
