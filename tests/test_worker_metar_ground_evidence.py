from __future__ import annotations

import csv
from datetime import datetime, timezone
import gzip
import io
from pathlib import Path
import time

import pytest

from storm_tracker_worker.canonical_ground_evidence_store import (
    CanonicalGroundEvidenceStore,
    parse_ground_observation,
)
from storm_tracker_worker import metar_ground_evidence as decoder
from storm_tracker_worker import metar_native as native
from storm_tracker_worker.raw_artifacts import (
    RawArtifactIntegrityError,
    RawArtifactStore,
)


RECEIVED_AT = datetime(2026, 8, 2, 12, 1, tzinfo=timezone.utc).timestamp()
REAL_CACHE = Path(__file__).parents[1] / "metars_current.csv.gz"


def _row(**overrides: str) -> list[str]:
    values = [""] * 44
    defaults = {
        0: "METAR EBAW 021200Z AUTO 00000KT 9999 -RA SCT020 18/16 Q1012 RMK AO2",
        1: "EBAW",
        2: "2026-08-02T12:00:00Z",
        3: "51.1894",
        4: "4.4603",
        14: "TRUE",
        15: "TRUE",
        21: "-RA",
        42: "METAR",
        43: "12.30",
    }
    names = {name: index for index, name in enumerate(native.METAR_EXPECTED_HEADER)}
    # Duplicate cloud names are irrelevant to this decoder; named overrides
    # intentionally address only unique columns.
    for index, value in defaults.items():
        values[index] = value
    for name, value in overrides.items():
        values[names[name]] = value
    return values


def _gzip(rows: list[list[str]], *, header=None) -> bytes:
    stream = io.StringIO(newline="")
    writer = csv.writer(stream)
    writer.writerow(list(header or native.METAR_EXPECTED_HEADER))
    writer.writerows(rows)
    return gzip.compress(stream.getvalue().encode("utf-8"), mtime=0)


def _capture(tmp_path: Path, payload: bytes, *, received_at=RECEIVED_AT):
    raw = RawArtifactStore((tmp_path / "raw").resolve())
    raw.initialize()
    digest = __import__("hashlib").sha256(payload).hexdigest()
    receipt = raw.store(
        provider="metar",
        product=native.METAR_GZIP_PRODUCT,
        fetch_id="decoder-test:csv_gzip",
        content_type="application/gzip",
        received_at=received_at,
        upstream_id=f"aviationweather/metars.cache.csv.gz|content_sha256={digest}",
        provenance=native._native_provenance(),
        payload=payload,
    )
    capture = raw.commit_capture(
        provider="metar",
        product=native.METAR_CAPTURE_PRODUCT,
        fetch_id="decoder-test:capture",
        upstream_capture_id=(
            f"{native.METAR_CAPTURE_PRODUCT}|content_sha256={digest}"
        ),
        product_timestamp=None,
        members={"csv_gzip": receipt.receipt_id},
    )
    ground = CanonicalGroundEvidenceStore(
        (tmp_path / "ground.sqlite3").resolve(),
        raw_artifact_reader=raw,
        clock=lambda: max(time.time(), received_at) + 1_000.0,
    )
    return raw, capture, ground


def _decode(tmp_path: Path, rows: list[list[str]], *, received_at=RECEIVED_AT, **kwargs):
    raw, capture, ground = _capture(tmp_path, _gzip(rows), received_at=received_at)
    result = decoder.decode_metar_ground_capture(
        capture.capture_id,
        raw_store=raw,
        evidence_store=ground,
        **kwargs,
    )
    return result, raw, ground


def _facts(result, variable: str):
    return [receipt.fact for receipt in result.receipts if receipt.fact.measurement.variable == variable]


def _qc_values(fact, flag: str) -> list[str | None]:
    return [item.value for item in fact.provider_qc if item.flag == flag]


def test_ra_sn_br_and_vcsh_are_separate_categorical_facts(tmp_path):
    row = _row(wx_string="-RA SN BR VCSH")
    result, _, _ = _decode(tmp_path, [row])
    weather = _facts(result, "present_weather")

    assert [fact.measurement.exact_value for fact in weather] == ["-RA", "SN", "BR", "VCSH"]
    assert all(fact.measurement.state == "observed" for fact in weather)
    vcsh = weather[-1]
    assert _qc_values(vcsh, "metar.station_relation") == ["vicinity_not_at_station"]
    assert vcsh.measurement.variable == "present_weather"
    assert result.operational_admission_eligible is False


def test_empty_weather_is_missing_but_pwino_is_unknown_never_dry(tmp_path):
    plain = _row(
        station_id="KAAA",
        raw_text="METAR KAAA 021200Z AUTO 10SM CLR RMK AO2",
        wx_string="",
    )
    unavailable = _row(
        station_id="KBBB",
        raw_text="METAR KBBB 021200Z AUTO 10SM CLR RMK AO2 PWINO",
        wx_string="",
        present_weather_sensor_off="TRUE",
    )
    result, _, _ = _decode(tmp_path, [plain, unavailable])
    facts = _facts(result, "present_weather")

    assert [fact.measurement.state for fact in facts] == ["missing", "unknown"]
    assert _qc_values(facts[1], "metar.provider_flag") == ["AUTO", "AO2", "PWINO"]
    assert all(fact.measurement.exact_value is None for fact in facts)


def test_prrrr_trace_and_amount_are_retained_without_fake_hour(tmp_path):
    trace = _row(
        station_id="KAAA",
        raw_text="METAR KAAA 021200Z AUTO 10SM CLR RMK AO2 P0000",
        wx_string="",
        precip_in="0.00",
    )
    amount = _row(
        station_id="KBBB",
        raw_text="METAR KBBB 021200Z AUTO 10SM CLR RMK AO2 P0001",
        wx_string="",
        precip_in="0.01",
    )
    result, _, _ = _decode(tmp_path, [trace, amount])
    remarks = _facts(result, "metar_hourly_precipitation_remark")

    assert [fact.measurement.exact_value for fact in remarks] == ["P0000", "P0001"]
    assert _qc_values(remarks[0], "metar.prrrr_reported_amount_state") == ["trace"]
    assert _qc_values(remarks[1], "awc.csv.precip_in_lexeme") == ["0.01"]
    assert result.unresolved_prrrr_count == 2
    assert not _facts(result, "precipitation_accumulation")


@pytest.mark.parametrize(
    ("observed", "token", "column", "lexeme", "hours"),
    (
        ("2026-08-02T03:55:00Z", "60001", "pcp3hr_in", "0.01", 3),
        ("2026-08-02T06:55:00Z", "60002", "pcp6hr_in", "0.02", 6),
    ),
)
def test_6rrrr_only_decodes_when_report_hour_and_awc_column_agree(
    tmp_path, observed, token, column, lexeme, hours,
):
    row = _row(
        observation_time=observed,
        raw_text=f"METAR KAAA 020355Z AUTO 10SM CLR RMK AO2 {token}",
        wx_string="",
        **{column: lexeme},
    )
    result, _, _ = _decode(
        tmp_path,
        [row],
        received_at=(
            datetime.fromisoformat(observed.replace("Z", "+00:00")).timestamp()
            + 60
        ),
    )
    fact = _facts(result, "precipitation_accumulation")[0]

    assert fact.measurement.exact_value == lexeme
    assert fact.measurement.unit == "in"
    assert fact.representative_interval_end - fact.representative_interval_start == hours * 3600
    assert _qc_values(fact, "metar.6rrrr_interval_hours") == [str(hours)]


def test_6rrrr_in_speci_or_wrong_hour_is_preserved_but_not_guessed(tmp_path):
    row = _row(
        observation_time="2026-08-02T04:10:00Z",
        raw_text="SPECI KAAA 020410Z AUTO 10SM CLR RMK AO2 60003",
        wx_string="",
        metar_type="SPECI",
        pcp3hr_in="0.03",
    )
    result, _, _ = _decode(
        tmp_path,
        [row],
        received_at=datetime(2026, 8, 2, 4, 11, tzinfo=timezone.utc).timestamp(),
    )

    assert not _facts(result, "precipitation_accumulation")
    raw = _facts(result, "metar_3_or_6_hour_precipitation_remark")[0]
    assert raw.measurement.exact_value == "60003"
    assert _qc_values(raw, "metar.6rrrr_decode_state") == ["ambiguous_withheld"]


def test_7rrrr_has_exact_24_hour_interval(tmp_path):
    row = _row(
        raw_text="METAR KAAA 021200Z AUTO 10SM CLR RMK AO2 70025",
        wx_string="",
        pcp24hr_in="0.25",
    )
    result, _, _ = _decode(tmp_path, [row])
    fact = _facts(result, "precipitation_accumulation")[0]

    assert fact.measurement.exact_value == "0.25"
    assert fact.representative_interval_end - fact.representative_interval_start == 86400


def test_exact_retry_is_idempotent(tmp_path):
    raw, capture, ground = _capture(tmp_path, _gzip([_row()]))
    first = decoder.decode_metar_ground_capture(
        capture.capture_id, raw_store=raw, evidence_store=ground
    )
    second = decoder.decode_metar_ground_capture(
        capture.capture_id, raw_store=raw, evidence_store=ground
    )

    assert [r.fact_id for r in second.receipts] == [r.fact_id for r in first.receipts]
    assert ground.stats()["ground_facts"] == first.fact_count


def test_target_scopes_project_only_nearby_available_stations(tmp_path):
    antwerp = _row(
        station_id="EBAW",
        latitude="51.1894",
        longitude="4.4603",
    )
    frankfurt = _row(
        station_id="EDDF",
        latitude="50.0333",
        longitude="8.5706",
    )
    unknown = _row(
        station_id="XXXX",
        latitude="",
        longitude="",
    )
    scopes = (
        decoder.MetarGroundTargetScope(
            target_id="home",
            latitude=51.047,
            longitude=4.418,
            radius_km=30.0,
        ),
        decoder.MetarGroundTargetScope(
            target_id="test-frankfurt",
            latitude=50.0333,
            longitude=8.5706,
            radius_km=30.0,
            available=False,
        ),
    )

    result, _, _ = _decode(
        tmp_path,
        [antwerp, frankfurt, unknown],
        target_scopes=scopes,
    )

    assert result.csv_row_count == 3
    assert result.decoded_report_count == 1
    assert result.out_of_scope_report_count == 2
    assert {item.fact.station.provider_station_id for item in result.receipts} == {
        "EBAW"
    }


def test_multiple_target_scopes_form_a_union_without_truncating_raw(tmp_path):
    rows = [
        _row(station_id="EBAW", latitude="51.1894", longitude="4.4603"),
        _row(station_id="EDDF", latitude="50.0333", longitude="8.5706"),
        _row(station_id="KJFK", latitude="40.6399", longitude="-73.7787"),
    ]
    payload = _gzip(rows)
    raw, capture, ground = _capture(tmp_path, payload)
    result = decoder.decode_metar_ground_capture(
        capture.capture_id,
        raw_store=raw,
        evidence_store=ground,
        target_scopes=(
            decoder.MetarGroundTargetScope(
                target_id="home",
                latitude=51.047,
                longitude=4.418,
                radius_km=30.0,
            ),
            decoder.MetarGroundTargetScope(
                target_id="test-frankfurt",
                latitude=50.0333,
                longitude=8.5706,
                radius_km=30.0,
            ),
        ),
    )

    assert result.decoded_report_count == 2
    assert result.out_of_scope_report_count == 1
    assert {item.fact.station.provider_station_id for item in result.receipts} == {
        "EBAW",
        "EDDF",
    }
    assert raw.load_capture_bundle_bytes(capture.capture_id)[2]["csv_gzip"] == payload


def test_empty_target_scope_keeps_global_raw_but_emits_no_canonical_facts(
    tmp_path,
):
    payload = _gzip([_row()])
    raw, capture, ground = _capture(tmp_path, payload)

    result = decoder.decode_metar_ground_capture(
        capture.capture_id,
        raw_store=raw,
        evidence_store=ground,
        target_scopes=(),
    )

    assert result.csv_row_count == 1
    assert result.decoded_report_count == 0
    assert result.out_of_scope_report_count == 1
    assert result.fact_count == 0
    assert ground.stats()["ground_facts"] == 0
    assert raw.load_capture_bundle_bytes(capture.capture_id)[2]["csv_gzip"] == payload


def test_target_scopes_are_concrete_and_hard_bounded(tmp_path):
    raw, capture, ground = _capture(tmp_path, _gzip([_row()]))
    scope = decoder.MetarGroundTargetScope(
        target_id="home",
        latitude=51.047,
        longitude=4.418,
        radius_km=20.0,
    )
    with pytest.raises(TypeError, match="concrete tuple"):
        decoder.decode_metar_ground_capture(
            capture.capture_id,
            raw_store=raw,
            evidence_store=ground,
            target_scopes=[scope],
        )
    with pytest.raises(decoder.MetarGroundCapacityError, match="target-scope"):
        decoder.decode_metar_ground_capture(
            capture.capture_id,
            raw_store=raw,
            evidence_store=ground,
            target_scopes=(scope,) * 65,
        )


def test_complete_decoder_batch_reverifies_raw_authority_once(tmp_path):
    raw, capture, _ground = _capture(tmp_path, _gzip([_row()]))

    class CountingAuthority:
        reads = 0

        def load_capture_receipt(self, capture_id):
            self.reads += 1
            return raw.load_capture_receipt(capture_id)

    authority = CountingAuthority()
    ground = CanonicalGroundEvidenceStore(
        (tmp_path / "counted-ground.sqlite3").resolve(),
        raw_artifact_reader=authority,
        clock=lambda: max(float(capture.created_at), RECEIVED_AT) + 1_000.0,
    )
    result = decoder.decode_metar_ground_capture(
        capture.capture_id, raw_store=raw, evidence_store=ground
    )
    assert result.fact_count > 1
    assert authority.reads == 1
    assert ground.stats()["ground_facts"] == result.fact_count


def test_exact_duplicate_collapses_but_distinct_revisions_are_all_retained(tmp_path):
    first = _row()
    corrected = _row(
        raw_text="METAR EBAW 021200Z COR 00000KT 9999 BR SCT020 18/16 Q1012",
        wx_string="BR",
        corrected="TRUE",
    )
    result, _, _ = _decode(tmp_path, [first, first.copy(), corrected])

    assert result.exact_duplicate_row_count == 1
    assert result.revision_identity_count == 1
    weather = _facts(result, "present_weather")
    assert [fact.measurement.exact_value for fact in weather] == ["-RA", "BR"]
    assert [_qc_values(fact, "decoder.revision_group_size") for fact in weather] == [["2"], ["2"]]


def test_stale_and_future_tokens_are_explicit_unknown(tmp_path):
    stale = _row(
        station_id="KAAA",
        observation_time="2026-08-02T09:00:00Z",
        raw_text="METAR KAAA 020900Z RA",
        wx_string="RA",
    )
    future = _row(
        station_id="KBBB",
        observation_time="2026-08-02T12:10:00Z",
        raw_text="METAR KBBB 021210Z SN",
        wx_string="SN",
    )
    result, _, _ = _decode(tmp_path, [stale, future])
    facts = _facts(result, "present_weather")

    assert [fact.measurement.state for fact in facts] == ["unknown", "unknown"]
    assert [_qc_values(fact, "decoder.time_state") for fact in facts] == [["stale"], ["future"]]
    assert [_qc_values(fact, "awc.present_weather_token") for fact in facts] == [["RA"], ["SN"]]


def test_unknown_position_and_exact_elevation_state_are_preserved(tmp_path):
    row = _row(latitude="51.2", longitude="", elevation_m="6.00")
    result, _, _ = _decode(tmp_path, [row])
    fact = _facts(result, "present_weather")[0]

    assert fact.station.position_state == "unknown"
    assert fact.station.latitude_decimal is None
    assert fact.station.longitude_decimal is None
    assert fact.station.elevation_state == "known"
    assert fact.station.elevation_m_decimal == "6.00"
    assert _qc_values(fact, "awc.csv.latitude_lexeme") == ["51.2"]


@pytest.mark.parametrize(
    "payload",
    (
        gzip.compress(b'"unterminated\n', mtime=0),
        _gzip([_row()], header=native.METAR_EXPECTED_HEADER[:-1]),
        _gzip([_row()[:-1]]),
    ),
)
def test_malformed_quotes_header_or_shifted_rows_fail_before_commit(tmp_path, payload):
    raw, capture, ground = _capture(tmp_path, payload)
    with pytest.raises(decoder.MetarGroundStructureError):
        decoder.decode_metar_ground_capture(
            capture.capture_id, raw_store=raw, evidence_store=ground
        )
    assert ground.stats()["ground_facts"] == 0


def test_decompression_and_row_station_fact_bounds_fail_closed(tmp_path):
    payload = _gzip([_row(station_id="KAAA"), _row(station_id="KBBB")])
    raw, capture, ground = _capture(tmp_path, payload)
    with pytest.raises(decoder.MetarGroundCapacityError):
        decoder.decode_metar_ground_capture(
            capture.capture_id,
            raw_store=raw,
            evidence_store=ground,
            limits=decoder.MetarGroundDecodeLimits(max_stations=1),
        )
    assert ground.stats()["ground_facts"] == 0

    bomb_raw, bomb_capture, bomb_ground = _capture(
        tmp_path / "bomb", gzip.compress(b"A" * 10_000, mtime=0)
    )
    with pytest.raises(decoder.MetarGroundCapacityError):
        decoder.decode_metar_ground_capture(
            bomb_capture.capture_id,
            raw_store=bomb_raw,
            evidence_store=bomb_ground,
            limits=decoder.MetarGroundDecodeLimits(
                max_uncompressed_bytes=100,
                max_expansion_ratio=2.0,
            ),
        )


@pytest.mark.parametrize(
    ("limit_name", "limit_value", "rows"),
    (
        ("max_rows", 1, [_row(station_id="KAAA"), _row(station_id="KBBB")]),
        ("max_facts", 1, [_row()]),
        ("max_fact_bytes", 100, [_row()]),
    ),
)
def test_row_fact_and_aggregate_fact_byte_limits_precede_commit(
    tmp_path, limit_name, limit_value, rows,
):
    raw, capture, ground = _capture(tmp_path, _gzip(rows))
    limits = decoder.MetarGroundDecodeLimits(**{limit_name: limit_value})
    with pytest.raises(decoder.MetarGroundCapacityError):
        decoder.decode_metar_ground_capture(
            capture.capture_id,
            raw_store=raw,
            evidence_store=ground,
            limits=limits,
        )
    assert ground.stats()["ground_facts"] == 0


def test_compressed_byte_limit_is_explicit_capacity_failure(tmp_path):
    payload = _gzip([_row()])
    raw, capture, ground = _capture(tmp_path, payload)
    with pytest.raises(decoder.MetarGroundCapacityError):
        decoder.decode_metar_ground_capture(
            capture.capture_id,
            raw_store=raw,
            evidence_store=ground,
            limits=decoder.MetarGroundDecodeLimits(
                max_compressed_bytes=len(payload) - 1
            ),
        )
    assert ground.stats()["ground_facts"] == 0


def test_expired_absolute_deadline_reads_nothing_and_commits_nothing(tmp_path, monkeypatch):
    raw, capture, ground = _capture(tmp_path, _gzip([_row()]))
    monkeypatch.setattr(
        raw,
        "load_capture_bundle_bytes",
        lambda *_: pytest.fail("expired deadline must precede raw read"),
    )
    with pytest.raises(decoder.MetarGroundDeadlineError):
        decoder.decode_metar_ground_capture(
            capture.capture_id,
            raw_store=raw,
            evidence_store=ground,
            deadline_at=time.monotonic() - 1.0,
        )
    assert ground.stats()["ground_facts"] == 0


def test_default_deadline_budget_supports_the_low_priority_ha_worker():
    limits = decoder.MetarGroundDecodeLimits()

    assert decoder.DEFAULT_DECODE_SECONDS == 300.0
    assert limits.max_decode_seconds == 300.0
    with pytest.raises(ValueError):
        decoder.MetarGroundDecodeLimits(max_decode_seconds=300.001)


def test_raw_object_tamper_fails_at_verified_bundle_boundary(tmp_path):
    raw, capture, ground = _capture(tmp_path, _gzip([_row()]))
    receipt = raw.load_capture_bundle(capture.capture_id)[1]["csv_gzip"]
    path = Path(receipt.artifact_path)
    data = path.read_bytes()
    path.write_bytes(bytes([data[0] ^ 1]) + data[1:])

    with pytest.raises(RawArtifactIntegrityError):
        decoder.decode_metar_ground_capture(
            capture.capture_id, raw_store=raw, evidence_store=ground
        )
    assert ground.stats()["ground_facts"] == 0


@pytest.mark.skipif(not REAL_CACHE.is_file(), reason="local real AWC golden unavailable")
def test_real_awc_golden_builds_only_bounded_canonical_candidate_facts(tmp_path):
    # This optional worldwide golden validates every canonical fact through a
    # bounded in-memory batch writer; the canonical store's one-read atomic
    # batch contract is covered independently without making this fixture's
    # runtime depend on local SQLite durability.
    payload = REAL_CACHE.read_bytes()
    raw, capture, _ = _capture(
        tmp_path,
        payload,
        received_at=REAL_CACHE.stat().st_mtime,
    )

    class ValidatingWriter:
        def __init__(self):
            self.facts = []

        def commit_batch(self, facts, *, deadline_at=None):
            parsed = [
                parse_ground_observation(fact.to_mapping()) for fact in facts
            ]
            start = len(self.facts)
            receipts = tuple(
                type(
                    "Receipt",
                    (),
                    {"fact": fact, "fact_id": f"f-{start + index + 1}"},
                )()
                for index, fact in enumerate(parsed)
            )
            self.facts.extend(parsed)
            return receipts

    writer = ValidatingWriter()
    result = decoder.decode_metar_ground_capture(
        capture.capture_id,
        raw_store=raw,
        evidence_store=writer,
    )

    assert result.csv_row_count > 1_000
    assert result.decoded_report_count > 1_000
    assert result.fact_count == len(writer.facts)
    assert result.revision_identity_count >= 1
    assert all(f.operational_admission_eligible is False for f in writer.facts)
    assert any(f.measurement.variable == "present_weather" for f in writer.facts)


@pytest.mark.skipif(not REAL_CACHE.is_file(), reason="local real AWC golden unavailable")
def test_real_awc_golden_target_projection_is_small_and_keeps_global_raw(tmp_path):
    payload = REAL_CACHE.read_bytes()
    raw, capture, ground = _capture(
        tmp_path,
        payload,
        received_at=REAL_CACHE.stat().st_mtime,
    )

    result = decoder.decode_metar_ground_capture(
        capture.capture_id,
        raw_store=raw,
        evidence_store=ground,
        target_scopes=(
            decoder.MetarGroundTargetScope(
                target_id="home",
                latitude=51.047,
                longitude=4.418,
                radius_km=20.0,
            ),
            decoder.MetarGroundTargetScope(
                target_id="test-frankfurt",
                latitude=50.1109,
                longitude=8.6821,
                radius_km=20.0,
            ),
        ),
    )

    assert result.csv_row_count > 1_000
    assert 1 <= result.decoded_report_count <= 32
    assert result.out_of_scope_report_count > 1_000
    assert result.fact_count == ground.stats()["ground_facts"]
    assert raw.load_capture_bundle_bytes(capture.capture_id)[2]["csv_gzip"] == payload
    assert (tmp_path / "ground.sqlite3").stat().st_size < 4 * 1024 * 1024
