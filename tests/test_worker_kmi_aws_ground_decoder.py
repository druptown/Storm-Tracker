from __future__ import annotations

from types import SimpleNamespace

import pytest

from storm_tracker_worker.canonical_ground_evidence_store import (
    CanonicalGroundEvidenceStore,
)
from storm_tracker_worker import kmi_aws_ground_decoder as decoder
from storm_tracker_worker import kmi_opendata_raw as raw
from storm_tracker_worker.raw_artifacts import RawArtifactStore
from storm_tracker_worker.worker_only_ground_processing import (
    GROUND_WORKLOAD_IDS,
    decode_worker_only_ground_receipts,
)


RECEIVED_AT = 1_788_178_830.0
PAYLOAD = br'''{
  "type":"FeatureCollection",
  "numberReturned":2,
  "features":[
    {
      "type":"Feature",
      "id":"aws_10min.station-1",
      "geometry":{"type":"Point","coordinates":[4.36,50.84]},
      "properties":{
        "code":12345,
        "timestamp":"2026-08-31T13:20:00Z",
        "precip_quantity":0.40,
        "qc_flags":"{\"validated\":{\"PRECIP_QUANTITY\":true}}"
      }
    },
    {
      "type":"Feature",
      "id":"aws_10min.station-2",
      "geometry":{"type":"Point","coordinates":[3.074,51.28]},
      "properties":{
        "code":10101002,
        "timestamp":"2026-08-31T13:20:00Z",
        "precip_quantity":null,
        "qc_flags":"{\"validated\":{\"PRECIP_QUANTITY\":false}}"
      }
    }
  ]
}'''


class _Response:
    def __init__(self, payload: bytes, url: str) -> None:
        self._payload = payload
        self._offset = 0
        self._url = url
        self.status = 200
        self.headers = {
            "Content-Type": "application/json;charset=UTF-8",
            "Content-Length": str(len(payload)),
        }

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def geturl(self) -> str:
        return self._url

    def read(self, size: int) -> bytes:
        result = self._payload[self._offset : self._offset + size]
        self._offset += len(result)
        return result


def _capture(store: RawArtifactStore, payload: bytes, job_id: str):
    def opener(request, *, timeout):
        assert timeout <= raw.HTTP_IO_TIMEOUT_SECONDS
        return _Response(payload, request.full_url)

    result = raw.capture_kmi_aws_10min(
        SimpleNamespace(job_id=job_id, payload={}),
        raw_store=store,
        _opener=opener,
    )
    return result["raw_provenance"]["raw_capture_id"]


def _stores(tmp_path):
    raw_store = RawArtifactStore((tmp_path / "raw").resolve())
    raw_store.initialize()
    ground_store = CanonicalGroundEvidenceStore(
        (tmp_path / "ground.sqlite3").resolve(),
        raw_artifact_reader=raw_store,
        clock=lambda: RECEIVED_AT + 1.0,
    )
    return raw_store, ground_store


def test_kmi_decoder_commits_exact_ten_minute_mm_candidates(
    monkeypatch,
    tmp_path,
):
    monkeypatch.setattr(raw.time, "time", lambda: RECEIVED_AT)
    raw_store, ground_store = _stores(tmp_path)
    capture_id = _capture(raw_store, PAYLOAD, "kmi-ground-golden")

    result = decoder.decode_kmi_aws_ground_capture(
        capture_id,
        raw_store=raw_store,
        ground_store=ground_store,
    )

    assert result.feature_count == 2
    assert result.fact_count == 2
    first = result.fact_receipts[0].fact
    assert first.provider == "kmi"
    assert first.product == raw.AWS_10MIN_CAPTURE_PRODUCT
    assert first.station.provider_station_id == "12345"
    assert first.station.latitude_decimal == "50.84"
    assert first.station.longitude_decimal == "4.36"
    assert first.measurement.variable == "precip_quantity"
    assert first.measurement.state == "observed"
    assert first.measurement.representation == "decimal"
    assert first.measurement.exact_value == "0.40"
    assert first.measurement.unit == "mm"
    assert first.representative_interval_end - first.representative_interval_start == 600
    assert first.provider_qc[0].flag == "kmi.precip_quantity.validated"
    assert first.provider_qc[0].value == "true"
    assert result.operational_admission_eligible is False
    assert result.ground_confirmation_eligible is False

    second = result.fact_receipts[1].fact
    assert second.measurement.state == "missing"
    assert second.measurement.exact_value is None
    assert second.provider_qc[0].value == "false"

    retry = decoder.decode_kmi_aws_ground_capture(
        capture_id,
        raw_store=raw_store,
        ground_store=ground_store,
    )
    assert retry == result


def test_worker_only_router_includes_kmi_and_returns_canonical_receipts(
    monkeypatch,
    tmp_path,
):
    monkeypatch.setattr(raw.time, "time", lambda: RECEIVED_AT)
    raw_store, _ground_store = _stores(tmp_path)
    capture_id = _capture(raw_store, PAYLOAD, "kmi-ground-worker-router")

    receipts = decode_worker_only_ground_receipts(
        workload_id="ground.kmi.aws_10min",
        capture_id=capture_id,
        raw_store=raw_store,
        evidence_path=(tmp_path / "worker-ground.sqlite3").resolve(),
    )

    assert "ground.kmi.aws_10min" in GROUND_WORKLOAD_IDS
    assert len(receipts) == 2
    assert {item.fact.decoder_identity for item in receipts} == {
        decoder.KMI_AWS_GROUND_DECODER_IDENTITY
    }


def test_duplicate_feature_property_fails_closed(monkeypatch, tmp_path):
    monkeypatch.setattr(raw.time, "time", lambda: RECEIVED_AT)
    raw_store, ground_store = _stores(tmp_path)
    payload = PAYLOAD.replace(
        b'"precip_quantity":0.40,',
        b'"precip_quantity":0.40,"precip_quantity":9,',
        1,
    )
    capture_id = _capture(raw_store, payload, "kmi-ground-duplicate-key")

    with pytest.raises(
        decoder.KmiAwsGroundDecoderContractError,
        match="dubbele objectkey",
    ):
        decoder.decode_kmi_aws_ground_capture(
            capture_id,
            raw_store=raw_store,
            ground_store=ground_store,
        )
    assert ground_store.stats()["ground_facts"] == 0


def test_negative_precipitation_is_preserved_only_as_unknown(
    monkeypatch,
    tmp_path,
):
    monkeypatch.setattr(raw.time, "time", lambda: RECEIVED_AT)
    raw_store, ground_store = _stores(tmp_path)
    payload = PAYLOAD.replace(b'"precip_quantity":0.40', b'"precip_quantity":-1', 1)
    capture_id = _capture(raw_store, payload, "kmi-ground-negative")

    result = decoder.decode_kmi_aws_ground_capture(
        capture_id,
        raw_store=raw_store,
        ground_store=ground_store,
    )

    measurement = result.fact_receipts[0].fact.measurement
    assert measurement.state == "unknown"
    assert measurement.representation == "none"
    assert measurement.exact_value is None
