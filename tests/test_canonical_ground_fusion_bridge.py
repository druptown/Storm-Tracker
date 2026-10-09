"""Strict shadow tests for the canonical-ground-to-fusion boundary."""
from __future__ import annotations

from dataclasses import replace
import hashlib
import json
from pathlib import Path

import pytest

from storm_tracker_worker.tracking_core.engine import (
    canonical_ground_fusion_bridge as bridge_module,
)
from storm_tracker_worker.canonical_ground_evidence_store import (
    GROUND_EVIDENCE_RECEIPT_SCHEMA,
    CanonicalGroundEvidenceStore,
    GroundCaptureMemberBinding,
    GroundMeasurementValue,
    GroundObservationFact,
    GroundObservationReceipt,
    GroundStationSite,
    ProviderQcFact,
    ground_fact_id,
    ground_fact_sha256,
    ground_observation_key,
    parse_ground_observation,
)
from storm_tracker_worker.tracking_core.engine.canonical_ground_fusion_bridge import (
    GROUND_BRIDGE_ADMISSION_STATUS,
    HARD_MAX_BRIDGE_FACTS,
    GroundBridgeCapacityError,
    GroundBridgeDeadlineError,
    GroundBridgeIssueState,
    GroundSemanticsCertificate,
    GroundSemanticsProfile,
    _adapt_raw_reverified_ground_receipts,
    query_and_adapt_canonical_ground,
)
from storm_tracker_worker.tracking_core.engine.ground_fusion import (
    MeasurementSemantics,
    PhysicalSite,
    PhysicalSiteRegistry,
    PrecipitationState,
    StationAlias,
)


NOW = 1_800_000_000.0
CONFIG = "9" * 64
REVIEW = "8" * 64
MAPPING = "7" * 64


def _qc(flag: str, value: str | None) -> ProviderQcFact:
    return ProviderQcFact(flag=flag, value=value, text=None)


def _replace_qc_value(
    fact: GroundObservationFact,
    flag: str,
    value: str | None,
) -> GroundObservationFact:
    replaced = False
    provider_qc = []
    for item in fact.provider_qc:
        if item.flag == flag:
            provider_qc.append(replace(item, value=value))
            replaced = True
        else:
            provider_qc.append(item)
    assert replaced, flag
    return parse_ground_observation(replace(
        fact,
        provider_qc=tuple(provider_qc),
    ).to_mapping())


def _common_metar_qc(*extra: ProviderQcFact) -> tuple[ProviderQcFact, ...]:
    return (
        _qc("decoder.time_state", "current"),
        _qc("awc.csv.maintenance_indicator_on", "FALSE"),
        _qc("awc.csv.no_signal", "FALSE"),
        _qc("awc.csv.present_weather_sensor_off", "FALSE"),
        *extra,
    )


def _fact(
    *,
    provider: str,
    product: str,
    decoder_identity: str,
    decoder_version: str,
    station_id: str,
    variable: str,
    state: str,
    representation: str,
    exact_value: str | None,
    unit: str,
    start: float,
    end: float,
    qc: tuple[ProviderQcFact, ...],
    latitude: str = "40.7500",
    longitude: str = "-73.9800",
) -> GroundObservationFact:
    member = GroundCaptureMemberBinding(
        member_id="response",
        role="response",
        receipt_id="raw-receipt-response",
        content_sha256="1" * 64,
        receipt_sha256="2" * 64,
        received_at=NOW + 10.0,
    )
    return parse_ground_observation(GroundObservationFact(
        capture_id="capture-" + "3" * 64,
        capture_receipt_sha256="4" * 64,
        members=(member,),
        source_member_role="response",
        source_member_content_sha256=member.content_sha256,
        source_member_receipt_sha256=member.receipt_sha256,
        source_record_locator=f"/station/{station_id}/{variable}",
        source_record_sha256="5" * 64,
        decoder_identity=decoder_identity,
        decoder_version=decoder_version,
        config_sha256=CONFIG,
        provider=provider,
        product=product,
        station=GroundStationSite(
            provider_station_id=station_id,
            site_id=f"{provider}:{station_id}",
            network=f"{provider}-test",
            station_name=None,
            position_state="known",
            latitude_decimal=latitude,
            longitude_decimal=longitude,
            elevation_state="unknown",
            elevation_m_decimal=None,
        ),
        observed_at=end,
        representative_interval_start=start,
        representative_interval_end=end,
        received_at=NOW + 10.0,
        ingested_at=NOW + 11.0,
        measurement=GroundMeasurementValue(
            variable=variable,
            state=state,  # type: ignore[arg-type]
            representation=representation,  # type: ignore[arg-type]
            exact_value=exact_value,
            unit=unit,
            censor_limit=None,
        ),
        provider_qc=qc,
    ).to_mapping())


def _receipt(fact: GroundObservationFact) -> GroundObservationReceipt:
    fact_id = ground_fact_id(fact)
    observation_key = ground_observation_key(fact)
    fact_sha256 = ground_fact_sha256(fact)
    committed_at = NOW + 12.0
    capture_manifest_sha256 = "a" * 64
    document = {
        "capture_manifest_sha256": capture_manifest_sha256,
        "capture_receipt_sha256": fact.capture_receipt_sha256,
        "committed_at": committed_at,
        "fact_id": fact_id,
        "fact_sha256": fact_sha256,
        "observation_key": observation_key,
        "schema": GROUND_EVIDENCE_RECEIPT_SCHEMA,
    }
    receipt_sha256 = hashlib.sha256(json.dumps(
        document,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")).hexdigest()
    return GroundObservationReceipt(
        fact_id=fact_id,
        observation_key=observation_key,
        fact_sha256=fact_sha256,
        receipt_sha256=receipt_sha256,
        capture_manifest_sha256=capture_manifest_sha256,
        committed_at=committed_at,
        fact=fact,
    )


def _registry(
    provider: str,
    station_id: str,
    *,
    mapping_fingerprint: str | None = MAPPING,
    registry_latitude: float = 40.75,
    catalog_fingerprint: str | None = "b" * 64,
) -> PhysicalSiteRegistry:
    return PhysicalSiteRegistry(
        sites=(PhysicalSite(
            physical_site_id=f"physical:{provider}:{station_id}",
            latitude=registry_latitude,
            longitude=-73.98,
            independence_group=f"independent:{station_id}",
            aliases=(StationAlias(
                provider_id=provider,
                station_id=station_id,
                mapping_fingerprint=mapping_fingerprint,
            ),),
        ),),
        registry_version="ground-bridge-test-v1",
        catalog_fingerprint=catalog_fingerprint,
    )


def _certificate(
    fact: GroundObservationFact,
    profile: GroundSemanticsProfile,
    *,
    config_sha256: str = CONFIG,
) -> GroundSemanticsCertificate:
    return GroundSemanticsCertificate(
        certificate_id=f"test:{profile.value}",
        certificate_version="1",
        provider=fact.provider,
        product=fact.product,
        decoder_identity=fact.decoder_identity,
        decoder_version=fact.decoder_version,
        config_sha256=config_sha256,
        profile=profile,
        primary_sources=("https://example.test/provider-specification",),
        review_evidence_sha256=REVIEW,
    )


def _synoptic(
    value: str,
    *,
    qc_flagged: str = "false",
    start: float = NOW - 300.0,
    unit: str = "Millimeters",
    variable: str = (
        "synoptic.native.precip_intervals_set_1d."
        "between_consecutive_reports"
    ),
) -> GroundObservationFact:
    sensor_set = variable.split(".")[2]
    return _fact(
        provider="synoptic",
        product="synoptic_timeseries_query_capture",
        decoder_identity="synoptic_timeseries_ground_evidence",
        decoder_version="synoptic-ground-evidence-shadow-v1",
        station_id="KNYC",
        variable=variable,
        state="observed",
        representation="decimal",
        exact_value=value,
        unit=unit,
        start=start,
        end=NOW,
        qc=(
            _qc("synoptic.provider.provisional", "true"),
            _qc("synoptic.sensor_set", sensor_set),
            _qc(
                "synoptic.decoder.temporal_semantics",
                "between_consecutive_provider_observations",
            ),
            _qc("synoptic.qc_flagged", qc_flagged),
        ),
    )


def _metar_weather(
    token: str,
    *,
    station_id: str = "KNYC",
    extra_qc: tuple[ProviderQcFact, ...] = (),
) -> GroundObservationFact:
    return _fact(
        provider="metar",
        product="aviationweather_current_metars_capture",
        decoder_identity="aviationweather_metar_canonical_ground",
        decoder_version="metar-canonical-ground-v1",
        station_id=station_id,
        variable="present_weather",
        state="observed",
        representation="text",
        exact_value=token,
        unit="metar_code",
        start=NOW,
        end=NOW,
        qc=_common_metar_qc(
            _qc("awc.present_weather_token", token),
            _qc("metar.present_weather_token_profile", "recognized"),
            *extra_qc,
        ),
    )


def test_synoptic_positive_and_exact_zero_retain_interval_semantics():
    wet = _synoptic("0.125")
    dry = _synoptic("0.000")
    # A different record locator gives this second value its own immutable ID.
    dry = replace(dry, source_record_locator=dry.source_record_locator + ":zero")
    dry = parse_ground_observation(dry.to_mapping())
    certificates = (
        _certificate(wet, GroundSemanticsProfile.SYNOPTIC_INTERVAL_ACCUMULATION_V1),
    )

    batch = _adapt_raw_reverified_ground_receipts(
        (_receipt(wet), _receipt(dry)),
        certificates=certificates,
        site_registry=_registry("synoptic", "KNYC"),
    )

    assert batch.issues == ()
    assert [item.precipitation_state for item in batch.observations] == [
        PrecipitationState.PRESENT,
        PrecipitationState.ABSENT,
    ]
    assert all(
        item.measurement_semantics is MeasurementSemantics.INTERVAL_ACCUMULATION
        and item.interval_start_at == NOW - 300.0
        and item.interval_end_at == NOW
        and item.measurement_unit == "mm"
        for item in batch.observations
    )
    assert [item.amount for item in batch.observations] == [0.125, 0.0]
    assert batch.operational_admission_eligible is False
    assert batch.admission_status == GROUND_BRIDGE_ADMISSION_STATUS


@pytest.mark.parametrize(
    ("qc_flagged", "expected_code"),
    (("true", "synoptic_provider_qc_not_clean"), ("null", "synoptic_provider_qc_not_clean")),
)
def test_synoptic_flagged_or_ambiguous_qc_is_retained_but_cannot_vote(
    qc_flagged,
    expected_code,
):
    fact = _synoptic("4.2", qc_flagged=qc_flagged)
    batch = _adapt_raw_reverified_ground_receipts(
        (_receipt(fact),),
        certificates=(
            _certificate(fact, GroundSemanticsProfile.SYNOPTIC_INTERVAL_ACCUMULATION_V1),
        ),
        site_registry=_registry("synoptic", "KNYC"),
    )

    assert batch.observations == ()
    assert batch.issues[0].code == expected_code
    assert batch.issues[0].state is GroundBridgeIssueState.WITHHELD


@pytest.mark.parametrize(
    ("fact", "code"),
    (
        (_synoptic("1.0", start=NOW), "synoptic_interval_support_missing"),
        (
            _synoptic(
                "1.0",
                variable=(
                    "synoptic.native.precip_accumulated_set_1d."
                    "request_window_cumulative"
                ),
            ),
            "provider_variable_semantics_uncertified",
        ),
        (_synoptic("1.0", unit="mm/h"), "synoptic_interval_not_quantitative"),
    ),
)
def test_synoptic_incomplete_cumulative_or_wrong_unit_never_becomes_weather(
    fact,
    code,
):
    profile = GroundSemanticsProfile.SYNOPTIC_INTERVAL_ACCUMULATION_V1
    batch = _adapt_raw_reverified_ground_receipts(
        (_receipt(fact),),
        certificates=(_certificate(fact, profile),),
        site_registry=_registry("synoptic", "KNYC"),
    )
    assert batch.observations == ()
    assert batch.issues[0].code == code


def test_synoptic_variable_must_bind_the_exact_independent_sensor_set():
    fact = _replace_qc_value(
        _synoptic("0.2"),
        "synoptic.sensor_set",
        "precip_intervals_set_2d",
    )
    batch = _adapt_raw_reverified_ground_receipts(
        (_receipt(fact),),
        certificates=(
            _certificate(fact, GroundSemanticsProfile.SYNOPTIC_INTERVAL_ACCUMULATION_V1),
        ),
        site_registry=_registry("synoptic", "KNYC"),
    )

    assert batch.observations == ()
    assert batch.issues[0].code == "synoptic_sensor_set_binding_mismatch"


@pytest.mark.parametrize("token", ("-RA", "+TSRA", "FZDZ", "SN", "UP"))
def test_metar_local_precipitation_tokens_are_categorical_positive_evidence(token):
    fact = _metar_weather(token)
    batch = _adapt_raw_reverified_ground_receipts(
        (_receipt(fact),),
        certificates=(
            _certificate(fact, GroundSemanticsProfile.METAR_PRESENT_WEATHER_V1),
        ),
        site_registry=_registry("metar", "KNYC"),
    )
    observation = batch.observations[0]
    assert batch.issues == ()
    assert observation.precipitation_state is PrecipitationState.PRESENT
    assert observation.measurement_semantics is MeasurementSemantics.INSTANTANEOUS_STATE
    assert observation.amount is None
    assert observation.measurement_unit is None


@pytest.mark.parametrize(
    ("token", "extra_qc", "code"),
    (
        ("BR", (), "metar_non_precipitation_weather_not_dry"),
        ("VCRA", (_qc("metar.station_relation", "vicinity_not_at_station"),), "metar_vicinity_not_at_station"),
        ("VCSH", (_qc("metar.station_relation", "vicinity_not_at_station"),), "metar_vicinity_not_at_station"),
    ),
)
def test_metar_nonprecipitation_and_vicinity_are_never_false_local_dry_or_wet(
    token,
    extra_qc,
    code,
):
    fact = _metar_weather(token, extra_qc=extra_qc)
    batch = _adapt_raw_reverified_ground_receipts(
        (_receipt(fact),),
        certificates=(
            _certificate(fact, GroundSemanticsProfile.METAR_PRESENT_WEATHER_V1),
        ),
        site_registry=_registry("metar", "KNYC"),
    )
    assert batch.observations == ()
    assert batch.issues[0].code == code


def test_metar_sensor_failure_flag_withholds_even_an_explicit_rain_token():
    fact = _metar_weather("RA", extra_qc=(_qc("metar.provider_flag", "PWINO"),))
    batch = _adapt_raw_reverified_ground_receipts(
        (_receipt(fact),),
        certificates=(
            _certificate(fact, GroundSemanticsProfile.METAR_PRESENT_WEATHER_V1),
        ),
        site_registry=_registry("metar", "KNYC"),
    )
    assert batch.observations == ()
    assert batch.issues[0].code == "metar_sensor_or_maintenance_flag"


def test_metar_ambiguous_sensor_status_and_token_binding_fail_closed():
    fact = _replace_qc_value(_metar_weather("RA"), "awc.csv.no_signal", "MAYBE")
    certificate = _certificate(
        fact,
        GroundSemanticsProfile.METAR_PRESENT_WEATHER_V1,
    )
    ambiguous = _adapt_raw_reverified_ground_receipts(
        (_receipt(fact),),
        certificates=(certificate,),
        site_registry=_registry("metar", "KNYC"),
    )
    mismatched = _replace_qc_value(_metar_weather("RA"), "awc.present_weather_token", "SN")
    mismatch_batch = _adapt_raw_reverified_ground_receipts(
        (_receipt(mismatched),),
        certificates=(
            _certificate(
                mismatched,
                GroundSemanticsProfile.METAR_PRESENT_WEATHER_V1,
            ),
        ),
        site_registry=_registry("metar", "KNYC"),
    )

    assert ambiguous.issues[0].code == "metar_sensor_status_ambiguous"
    assert mismatch_batch.issues[0].code == "metar_weather_token_binding_mismatch"


def test_positive_metar_accumulation_retains_true_interval_and_native_unit():
    fact = _fact(
        provider="metar",
        product="aviationweather_current_metars_capture",
        decoder_identity="aviationweather_metar_canonical_ground",
        decoder_version="metar-canonical-ground-v1",
        station_id="KNYC",
        variable="precipitation_accumulation",
        state="observed",
        representation="decimal",
        exact_value="0.25",
        unit="in",
        start=NOW - 6 * 3600.0,
        end=NOW,
        qc=_common_metar_qc(
            _qc("metar.6rrrr_interval_hours", "6"),
            _qc("metar.6rrrr_token", "60025"),
            _qc("awc.csv.accumulation_lexeme", "0.25"),
            _qc("metar.6rrrr_zero_semantics", "not_applicable"),
        ),
    )
    batch = _adapt_raw_reverified_ground_receipts(
        (_receipt(fact),),
        certificates=(
            _certificate(fact, GroundSemanticsProfile.METAR_INTERVAL_ACCUMULATION_V1),
        ),
        site_registry=_registry("metar", "KNYC"),
    )
    observation = batch.observations[0]
    assert observation.precipitation_state is PrecipitationState.PRESENT
    assert observation.amount == 0.25
    assert observation.measurement_unit == "in"
    assert observation.interval_start_at == NOW - 6 * 3600.0


def test_metar_accumulation_qc_must_exactly_bind_interval_token_and_amount():
    fact = _fact(
        provider="metar",
        product="aviationweather_current_metars_capture",
        decoder_identity="aviationweather_metar_canonical_ground",
        decoder_version="metar-canonical-ground-v1",
        station_id="KNYC",
        variable="precipitation_accumulation",
        state="observed",
        representation="decimal",
        exact_value="0.25",
        unit="in",
        start=NOW - 6 * 3600.0,
        end=NOW,
        qc=_common_metar_qc(
            _qc("metar.6rrrr_interval_hours", "3"),
            _qc("metar.6rrrr_token", "60025"),
            _qc("awc.csv.accumulation_lexeme", "0.25"),
            _qc("metar.6rrrr_zero_semantics", "not_applicable"),
        ),
    )
    batch = _adapt_raw_reverified_ground_receipts(
        (_receipt(fact),),
        certificates=(
            _certificate(fact, GroundSemanticsProfile.METAR_INTERVAL_ACCUMULATION_V1),
        ),
        site_registry=_registry("metar", "KNYC"),
    )

    assert batch.observations == ()
    assert batch.issues[0].code == "metar_accumulation_interval_qc_invalid"


def test_netatmo_opaque_rain_name_and_number_remain_uncertified():
    fact = _fact(
        provider="netatmo",
        product="netatmo_getpublicdata_capture",
        decoder_identity="netatmo_getpublicdata_ground_evidence",
        decoder_version="netatmo-ground-evidence-shadow-v1",
        station_id="station-public",
        variable="netatmo_native.rain_live",
        state="observed",
        representation="decimal",
        exact_value="12.5",
        unit="netatmo_native_unit_unspecified",
        start=NOW,
        end=NOW,
        qc=(
            _qc("netatmo.decoder.temporal_support_uncertified", None),
            _qc("netatmo.decoder.unit_semantics_uncertified", None),
        ),
    )
    batch = _adapt_raw_reverified_ground_receipts(
        (_receipt(fact),),
        certificates=(),
        site_registry=_registry("netatmo", "station-public"),
    )
    assert batch.observations == ()
    assert batch.issues[0].code == "netatmo_semantics_uncertified"


def test_netatmo_cannot_be_enabled_by_a_known_looking_variable_or_certificate():
    fact = _fact(
        provider="netatmo",
        product="netatmo_getpublicdata_capture",
        decoder_identity="netatmo_getpublicdata_ground_evidence",
        decoder_version="netatmo-ground-evidence-shadow-v1",
        station_id="station-public",
        variable="present_weather",
        state="observed",
        representation="text",
        exact_value="RA",
        unit="metar_code",
        start=NOW,
        end=NOW,
        qc=(_qc("metar.present_weather_token_profile", "recognized"),),
    )
    batch = _adapt_raw_reverified_ground_receipts(
        (_receipt(fact),),
        certificates=(
            _certificate(fact, GroundSemanticsProfile.METAR_PRESENT_WEATHER_V1),
        ),
        site_registry=_registry("netatmo", "station-public"),
    )

    assert batch.observations == ()
    assert batch.issues[0].code == "netatmo_semantics_uncertified"


def test_certificate_binding_is_exact_for_decoder_configuration():
    fact = _synoptic("0.2")
    batch = _adapt_raw_reverified_ground_receipts(
        (_receipt(fact),),
        certificates=(
            _certificate(
                fact,
                GroundSemanticsProfile.SYNOPTIC_INTERVAL_ACCUMULATION_V1,
                config_sha256="c" * 64,
            ),
        ),
        site_registry=_registry("synoptic", "KNYC"),
    )
    assert batch.observations == ()
    assert batch.issues[0].code == "semantics_certificate_missing"


def test_site_identity_requires_exact_alias_and_mapping_proof_not_proximity():
    fact = _synoptic("0.2")
    certificate = _certificate(
        fact,
        GroundSemanticsProfile.SYNOPTIC_INTERVAL_ACCUMULATION_V1,
    )
    nearby_wrong_alias = _registry("synoptic", "KOTHER")
    unproven = _registry("synoptic", "KNYC", mapping_fingerprint=None)

    nearby = _adapt_raw_reverified_ground_receipts(
        (_receipt(fact),), certificates=(certificate,), site_registry=nearby_wrong_alias
    )
    missing_proof = _adapt_raw_reverified_ground_receipts(
        (_receipt(fact),), certificates=(certificate,), site_registry=unproven
    )

    assert nearby.issues[0].code == "physical_site_alias_unverified"
    assert missing_proof.issues[0].code == "physical_site_mapping_proof_missing"
    assert nearby.observations == missing_proof.observations == ()


def test_site_registry_requires_both_alias_and_catalog_proof():
    fact = _synoptic("0.2")
    batch = _adapt_raw_reverified_ground_receipts(
        (_receipt(fact),),
        certificates=(
            _certificate(fact, GroundSemanticsProfile.SYNOPTIC_INTERVAL_ACCUMULATION_V1),
        ),
        site_registry=_registry(
            "synoptic",
            "KNYC",
            catalog_fingerprint=None,
        ),
    )

    assert batch.observations == ()
    assert batch.issues[0].code == "physical_site_catalog_proof_missing"


def test_receipt_identity_tamper_is_explicitly_withheld():
    fact = _synoptic("0.2")
    receipt = replace(_receipt(fact), fact_id="ground-fact-" + "0" * 64)
    batch = _adapt_raw_reverified_ground_receipts(
        (receipt,),
        certificates=(
            _certificate(fact, GroundSemanticsProfile.SYNOPTIC_INTERVAL_ACCUMULATION_V1),
        ),
        site_registry=_registry("synoptic", "KNYC"),
    )
    assert batch.observations == ()
    assert batch.issues[0].code == "receipt_fact_id_mismatch"


def test_receipt_digest_tamper_is_retained_with_exact_claimed_hashes():
    fact = _synoptic("0.2")
    receipt = replace(_receipt(fact), receipt_sha256="0" * 64)
    batch = _adapt_raw_reverified_ground_receipts(
        (receipt,),
        certificates=(
            _certificate(fact, GroundSemanticsProfile.SYNOPTIC_INTERVAL_ACCUMULATION_V1),
        ),
        site_registry=_registry("synoptic", "KNYC"),
    )

    issue = batch.issues[0]
    assert issue.code == "receipt_sha256_mismatch"
    assert issue.fact_sha256 == receipt.fact_sha256
    assert issue.receipt_sha256 == "0" * 64


def test_tampered_same_claimed_id_is_not_hidden_as_a_duplicate():
    fact = _synoptic("0.2")
    valid = _receipt(fact)
    altered_fact = parse_ground_observation(replace(
        fact,
        source_record_locator=fact.source_record_locator + ":altered",
    ).to_mapping())
    tampered = replace(_receipt(altered_fact), fact_id=valid.fact_id)
    batch = _adapt_raw_reverified_ground_receipts(
        (valid, tampered),
        certificates=(
            _certificate(fact, GroundSemanticsProfile.SYNOPTIC_INTERVAL_ACCUMULATION_V1),
        ),
        site_registry=_registry("synoptic", "KNYC"),
    )

    assert len(batch.observations) == 1
    assert batch.issues[0].state is GroundBridgeIssueState.WITHHELD
    assert batch.issues[0].code == "receipt_fact_id_mismatch"


def test_canonical_contract_blocks_ambiguous_source_role_before_bridge():
    fact = _synoptic("0.2")
    second = replace(
        fact.members[0],
        member_id="response-copy",
        receipt_id="raw-receipt-response-copy",
        content_sha256="c" * 64,
        receipt_sha256="d" * 64,
    )
    with pytest.raises(ValueError, match="member_id must equal"):
        parse_ground_observation(replace(
            fact,
            members=(fact.members[0], second),
        ).to_mapping())


def test_duplicate_receipt_is_not_a_second_ground_vote():
    fact = _synoptic("0.2")
    receipt = _receipt(fact)
    batch = _adapt_raw_reverified_ground_receipts(
        (receipt, receipt),
        certificates=(
            _certificate(fact, GroundSemanticsProfile.SYNOPTIC_INTERVAL_ACCUMULATION_V1),
        ),
        site_registry=_registry("synoptic", "KNYC"),
    )
    assert len(batch.observations) == 1
    assert batch.issues[0].state is GroundBridgeIssueState.DUPLICATE_INPUT
    assert batch.source_fact_count == 2


class _Reader:
    def __init__(self, receipts):
        self.receipts = receipts
        self.kwargs = None

    def query(self, **kwargs):
        self.kwargs = kwargs
        return self.receipts


def test_store_query_path_forwards_bounds_and_deadline_before_adaptation(monkeypatch):
    fact = _synoptic("0.2")
    receipt = _receipt(fact)
    store = object.__new__(CanonicalGroundEvidenceStore)
    captured = {}

    def query(_self, **kwargs):
        captured.update(kwargs)
        return (receipt,)

    monkeypatch.setattr(CanonicalGroundEvidenceStore, "query", query)
    clock = lambda: 10.0
    batch = query_and_adapt_canonical_ground(
        store,
        certificates=(
            _certificate(fact, GroundSemanticsProfile.SYNOPTIC_INTERVAL_ACCUMULATION_V1),
        ),
        site_registry=_registry("synoptic", "KNYC"),
        provider="synoptic",
        observed_from=NOW - 600.0,
        observed_through=NOW,
        limit=25,
        deadline_at=20.0,
        monotonic=clock,
    )
    assert len(batch.observations) == 1
    assert captured == {
        "provider": "synoptic",
        "provider_station_id": None,
        "site_id": None,
        "variable": None,
        "observed_from": NOW - 600.0,
        "observed_through": NOW,
        "limit": 25,
        "deadline_at": 20.0,
    }


def test_public_entry_rejects_protocol_fakes_that_cannot_prove_raw_reverification():
    fact = _synoptic("0.2")
    with pytest.raises(TypeError, match="concrete CanonicalGroundEvidenceStore"):
        query_and_adapt_canonical_ground(
            _Reader((_receipt(fact),)),  # type: ignore[arg-type]
            certificates=(
                _certificate(
                    fact,
                    GroundSemanticsProfile.SYNOPTIC_INTERVAL_ACCUMULATION_V1,
                ),
            ),
            site_registry=_registry("synoptic", "KNYC"),
        )


def test_alias_projection_never_calls_proximity_or_per_fact_registry_resolution(
    monkeypatch,
):
    fact = _synoptic("0.2")

    def forbidden(*_args, **_kwargs):
        raise AssertionError("per-fact registry resolution was called")

    monkeypatch.setattr(PhysicalSiteRegistry, "resolve_with_alias", forbidden)
    batch = _adapt_raw_reverified_ground_receipts(
        (_receipt(fact),),
        certificates=(
            _certificate(fact, GroundSemanticsProfile.SYNOPTIC_INTERVAL_ACCUMULATION_V1),
        ),
        site_registry=_registry("synoptic", "KNYC"),
    )

    assert len(batch.observations) == 1


def test_capacity_deadline_and_certificate_source_contracts_fail_closed():
    with pytest.raises(GroundBridgeCapacityError):
        _adapt_raw_reverified_ground_receipts(
            (None,) * (HARD_MAX_BRIDGE_FACTS + 1),  # type: ignore[arg-type]
            certificates=(),
            site_registry=PhysicalSiteRegistry(sites=(), registry_version="empty"),
        )
    with pytest.raises(GroundBridgeDeadlineError):
        _adapt_raw_reverified_ground_receipts(
            (),
            certificates=(),
            site_registry=PhysicalSiteRegistry(sites=(), registry_version="empty"),
            deadline_at=10.0,
            monotonic=lambda: 10.0,
        )
    with pytest.raises(ValueError, match="monotonic clock"):
        _adapt_raw_reverified_ground_receipts(
            (),
            certificates=(),
            site_registry=PhysicalSiteRegistry(sites=(), registry_version="empty"),
            monotonic=lambda: float("nan"),
        )
    fact = _synoptic("0.2")
    with pytest.raises(ValueError, match="HTTPS"):
        replace(
            _certificate(fact, GroundSemanticsProfile.SYNOPTIC_INTERVAL_ACCUMULATION_V1),
            primary_sources=("http://example.test/not-authoritative",),
        )


def test_candidate_fingerprint_is_deterministic_and_all_provenance_is_bound():
    fact = _synoptic("0.2")
    receipt = _receipt(fact)
    certificate = _certificate(
        fact, GroundSemanticsProfile.SYNOPTIC_INTERVAL_ACCUMULATION_V1
    )
    registry = _registry("synoptic", "KNYC")
    first = _adapt_raw_reverified_ground_receipts(
        (receipt,), certificates=(certificate,), site_registry=registry
    )
    second = _adapt_raw_reverified_ground_receipts(
        (receipt,), certificates=(certificate,), site_registry=registry
    )
    assert first.fingerprint == second.fingerprint
    adapted = first.facts[0]
    assert adapted.fact_id in adapted.observation.quality_flags[0]
    assert adapted.certificate_fingerprint == certificate.fingerprint
    assert adapted.mapping_fingerprint == MAPPING
    assert adapted.observation.provenance.raw_content_sha256 == "1" * 64
    assert adapted.observation.provenance.raw_artifact_receipt_id == (
        "raw-receipt-response"
    )


def test_bridge_is_worker_only_shadow_code_and_has_no_live_wiring():
    root = Path(__file__).resolve().parents[1]
    service = (root / "storm_tracker_worker/service.py").read_text(encoding="utf-8")
    integration = (
        root / "custom_components/storm_tracker_v3/__init__.py"
    ).read_text(encoding="utf-8")
    symbol = "canonical_ground_fusion_bridge"
    assert symbol not in service
    assert symbol not in integration
    assert "_adapt_raw_reverified_ground_receipts" not in bridge_module.__all__
    assert "query_and_adapt_canonical_ground" in bridge_module.__all__
