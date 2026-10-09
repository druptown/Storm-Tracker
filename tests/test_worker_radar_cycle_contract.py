from __future__ import annotations

from copy import deepcopy
import json

import pytest

from storm_tracker_worker import radar_cycle_contract as contract


def _decoded_input(
    suffix: str = "a",
    *,
    provider: str = "kmi",
    region_ids: list[str] | None = None,
) -> dict:
    return {
        "provider": provider,
        "product": "composite-rain-rate",
        "region_ids": region_ids or ["home"],
        "artifact": {
            "artifact_path": f"/share/storm_tracker_worker/{suffix}.json.gz",
            "artifact_sha256": (suffix[0].lower() if suffix[0].lower() in "abcdef" else "a") * 64,
            "artifact_bytes": 1234,
            "content_type": "application/json+gzip",
        },
        "provenance": {
            "grade": "decoded_legacy",
            "artifact_kind": "decoded_provider_result",
            "exact_raw_artifacts_complete": False,
            "parser_id": "kmi-decoder-v1",
            "upstream_record_id": None,
            "raw_artifact_receipt_id": None,
        },
        "product_valid_at": 1_800_000_000.25,
        "received_at": 1_800_000_007.75,
        "coverage": {
            "status": "incomplete",
            "bbox": [2.0, 49.0, 6.0, 53.0],
        },
    }


def _raw_input(suffix: str = "b", *, provider: str = "kmi") -> dict:
    return {
        "provider": provider,
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
        # A future valid time is legal and remains distinct from receipt time.
        "product_valid_at": 1_800_000_030.5,
        "received_at": 1_800_000_010.125,
        "coverage": {
            "status": "complete",
            "bbox": [-130.0, 20.0, 10.0, 60.0],
        },
    }


def _request() -> dict:
    return {
        "contract": contract.REQUEST_CONTRACT,
        "requested_at": 1_800_000_020.875,
        # Deliberately non-canonical input order.
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
        "inputs": [_raw_input(), _decoded_input()],
    }


def test_golden_canonical_request_ids_and_provenance_are_stable():
    parsed = contract.parse_radar_cycle_request(_request())

    assert parsed.contract == "stv3-radar-cycle-request-v1"
    assert [region.region_id for region in parsed.regions] == ["home", "new-york"]
    assert [target.target_id for target in parsed.targets] == [
        "tracker-home", "tracker-new-york"
    ]
    assert [value.provider for value in parsed.inputs] == ["kmi", "kmi"]
    assert {value.provenance.grade.value for value in parsed.inputs} == {
        "decoded_legacy", "raw_native"
    }
    assert parsed.to_dict()["inputs"][0]["product_valid_at"] in {
        1_800_000_000.25, 1_800_000_030.5
    }
    assert parsed.request_sha256 == (
        "3332cfe2cefec7e4f7d45802f1963205055a9c9663bd48a5ca2b46f7e051456f"
    )
    assert parsed.identity_sha256 == (
        "09adc15811f5f6bae46b3909a57c4e72dc963a29b82450cbaaa5518af859fbee"
    )
    assert parsed.cycle_id == (
        "radar-cycle:"
        "09adc15811f5f6bae46b3909a57c4e72dc963a29b82450cbaaa5518af859fbee"
    )
    assert parsed.regions_sha256 == (
        "1b46925d4211d3738f663a2cb77512456919f828a8d9abc1d4f78aaa6de8f400"
    )
    assert parsed.targets_sha256 == (
        "adf0550ceb2925e8e8dbd3d14a2127dfc111bb6eab18b84b265f26d1e913dcd3"
    )
    assert parsed.inputs_sha256 == (
        "0d419468155fbdc304c20217d8f149c8d985ef959fc4c49917b407b25fc99709"
    )
    assert contract.canonical_radar_cycle_request(_request()) == parsed.canonical_bytes
    assert contract.radar_cycle_request_digest(_request()) == parsed.request_sha256


def test_collection_order_is_canonical_and_requested_at_is_not_cycle_identity():
    first_raw = _request()
    second_raw = deepcopy(first_raw)
    second_raw["regions"].reverse()
    second_raw["targets"].reverse()
    second_raw["inputs"].reverse()
    first = contract.parse_radar_cycle_request(first_raw)
    second = contract.parse_radar_cycle_request(second_raw)
    assert first.canonical_bytes == second.canonical_bytes
    assert first.digests() == second.digests()

    later_raw = deepcopy(first_raw)
    later_raw["requested_at"] += 900.125
    later = contract.parse_radar_cycle_request(later_raw)
    assert later.requested_at == first.requested_at + 900.125
    assert later.request_sha256 != first.request_sha256
    assert later.cycle_id == first.cycle_id
    assert later.identity_sha256 == first.identity_sha256


def test_product_and_receipt_clocks_remain_independent_exact_facts():
    raw = _request()
    raw["inputs"] = [_raw_input()]
    raw["inputs"][0]["product_valid_at"] = 9_000_000_000.125
    raw["inputs"][0]["received_at"] = 10.25
    parsed = contract.parse_radar_cycle_request(raw)
    value = parsed.inputs[0]
    assert value.product_valid_at == 9_000_000_000.125
    assert value.received_at == 10.25

    changed = deepcopy(raw)
    changed["inputs"][0]["received_at"] = 10.5
    assert contract.parse_radar_cycle_request(changed).cycle_id != parsed.cycle_id
    changed = deepcopy(raw)
    changed["inputs"][0]["product_valid_at"] += 1.0
    assert contract.parse_radar_cycle_request(changed).cycle_id != parsed.cycle_id


@pytest.mark.parametrize("status,bbox", [
    ("unknown", None),
    ("unknown", [2.0, 49.0, 6.0, 53.0]),
    ("incomplete", None),
    ("incomplete", [2.0, 49.0, 6.0, 53.0]),
    ("complete", [2.0, 49.0, 6.0, 53.0]),
])
def test_empty_legacy_artifact_is_unknown_and_never_synthetic_dry(status, bbox):
    raw = _request()
    legacy = _decoded_input()
    legacy["coverage"] = {"status": status, "bbox": bbox}
    raw["inputs"] = [legacy]
    value = contract.parse_radar_cycle_request(raw).inputs[0]
    assert value.provenance.grade is contract.ProvenanceGrade.DECODED_LEGACY
    assert value.empty_precipitation_state is contract.PrecipitationState.UNKNOWN
    assert value.empty_precipitation_state is not contract.PrecipitationState.DRY


def test_no_provider_inputs_is_explicitly_unknown_not_a_dry_cycle():
    raw = _request()
    raw["inputs"] = []
    parsed = contract.parse_radar_cycle_request(raw)
    assert parsed.inputs == ()
    assert (
        parsed.undecoded_precipitation_state
        is contract.PrecipitationState.UNKNOWN
    )


def test_dateline_crossing_coverage_is_valid_geography_not_globalized():
    raw = _request()
    value = _decoded_input()
    value["coverage"] = {
        "status": "complete",
        "bbox": [170.0, -10.0, -170.0, 10.0],
    }
    raw["inputs"] = [value]
    coverage = contract.parse_radar_cycle_request(raw).inputs[0].coverage
    assert coverage.bbox == (170.0, -10.0, -170.0, 10.0)
    assert coverage.crosses_antimeridian is True


def _many_regions(count: int) -> list[dict]:
    return [
        {
            "region_id": f"region-{index:02d}",
            "center_lat": -80.0 + index,
            "center_lon": -170.0 + index,
            "radius_km": 1.0 + index,
        }
        for index in range(count)
    ]


def _many_targets(count: int, region_id: str = "home") -> list[dict]:
    return [
        {
            "target_id": f"target-{index:02d}",
            "region_id": region_id,
            "latitude": 0.0,
            "longitude": float(index % 60),
        }
        for index in range(count)
    ]


def _many_inputs(count: int, *, long_paths: bool = False) -> list[dict]:
    values = []
    alphabet = "0123456789abcdef"
    for index in range(count):
        value = _decoded_input(
            f"artifact-{index:02d}", provider="rainviewer"
        )
        value["artifact"]["artifact_sha256"] = (
            alphabet[index % len(alphabet)] * 62 + f"{index:02x}"
        )
        if long_paths:
            value["artifact"]["artifact_path"] = (
                "/share/storm_tracker_worker/"
                + f"{index:02d}-"
                + "x" * 1950
                + ".json.gz"
            )
        values.append(value)
    return values


def test_exact_structural_count_boundaries_and_same_provider_inputs():
    raw = _request()
    raw["regions"] = _many_regions(contract.MAX_REGIONS)
    raw["targets"] = _many_targets(contract.MAX_TARGETS, "region-00")
    raw["inputs"] = _many_inputs(contract.MAX_INPUTS)
    for value in raw["inputs"]:
        value["region_ids"] = ["region-00"]
    parsed = contract.parse_radar_cycle_request(raw)
    assert len(parsed.regions) == 16
    assert len(parsed.targets) == 64
    assert len(parsed.inputs) == 48
    assert {value.provider for value in parsed.inputs} == {"rainviewer"}

    too_many = deepcopy(raw)
    too_many["regions"] = _many_regions(contract.MAX_REGIONS + 1)
    with pytest.raises(ValueError, match="maximum count 16"):
        contract.parse_radar_cycle_request(too_many)
    too_many = deepcopy(raw)
    too_many["targets"] = _many_targets(contract.MAX_TARGETS + 1, "region-00")
    with pytest.raises(ValueError, match="maximum count 64"):
        contract.parse_radar_cycle_request(too_many)
    too_many = deepcopy(raw)
    too_many["inputs"].append(_decoded_input("extra"))
    with pytest.raises(ValueError, match="maximum count 48"):
        contract.parse_radar_cycle_request(too_many)


def test_encoded_and_canonical_requests_have_independent_96kib_bounds():
    compact = json.dumps(_request(), separators=(",", ":")).encode()
    oversized_transport = compact + b" " * contract.MAX_REQUEST_BYTES
    with pytest.raises(ValueError, match="encoded.*96 KiB"):
        contract.parse_radar_cycle_request(oversized_transport)

    raw = _request()
    raw["inputs"] = _many_inputs(contract.MAX_INPUTS, long_paths=True)
    with pytest.raises(ValueError, match="canonical.*96 KiB"):
        contract.parse_radar_cycle_request(raw)


@pytest.mark.parametrize("path,value", [
    (("requested_at",), float("nan")),
    (("requested_at",), float("inf")),
    (("requested_at",), -1.0),
    (("requested_at",), True),
    (("regions", 0, "center_lat"), 91.0),
    (("regions", 0, "center_lat"), float("nan")),
    (("regions", 0, "center_lon"), 181.0),
    (("regions", 0, "radius_km"), 0.0),
    (("regions", 0, "radius_km"), float("inf")),
    (("targets", 0, "latitude"), -91.0),
    (("targets", 0, "longitude"), float("nan")),
    (("inputs", 0, "product_valid_at"), float("inf")),
    (("inputs", 0, "received_at"), -0.1),
    (("inputs", 0, "received_at"), False),
])
def test_nonfinite_or_invalid_geography_and_times_fail_closed(path, value):
    raw = _request()
    target = raw
    for part in path[:-1]:
        target = target[part]
    target[path[-1]] = value
    with pytest.raises((TypeError, ValueError)):
        contract.parse_radar_cycle_request(raw)


@pytest.mark.parametrize("mutation,match", [
    (
        lambda value: value["provenance"].update(
            artifact_kind="upstream_native_product"
        ),
        "decoded_legacy requires",
    ),
    (
        lambda value: value["provenance"].update(
            exact_raw_artifacts_complete=True
        ),
        "cannot claim complete",
    ),
    (
        lambda value: value["provenance"].update(
            raw_artifact_receipt_id="receipt:forged"
        ),
        "cannot claim a raw artifact receipt",
    ),
])
def test_decoded_legacy_cannot_masquerade_as_native_raw(mutation, match):
    raw = _request()
    value = _decoded_input()
    mutation(value)
    raw["inputs"] = [value]
    with pytest.raises(ValueError, match=match):
        contract.parse_radar_cycle_request(raw)


@pytest.mark.parametrize("mutation,match", [
    (
        lambda value: value["provenance"].update(
            artifact_kind="decoded_provider_result"
        ),
        "raw_native requires upstream",
    ),
    (
        lambda value: value["provenance"].update(
            exact_raw_artifacts_complete=False
        ),
        "requires complete exact raw",
    ),
    (
        lambda value: value["provenance"].update(raw_artifact_receipt_id=None),
        "requires raw_artifact_receipt_id",
    ),
    (
        lambda value: value["provenance"].update(parser_id="parser-v1"),
        "parser_id must remain unknown",
    ),
])
def test_raw_native_requires_consistent_explicit_provenance(mutation, match):
    raw = _request()
    value = _raw_input()
    mutation(value)
    raw["inputs"] = [value]
    with pytest.raises(ValueError, match=match):
        contract.parse_radar_cycle_request(raw)


@pytest.mark.parametrize("forbidden", [
    "observations", "cells", "runs", "raster", "speed_kmh", "intensity",
])
def test_request_never_embeds_or_caps_decoded_meteorological_values(forbidden):
    raw = _request()
    raw["inputs"][0][forbidden] = 1_000_000_000.0
    with pytest.raises(ValueError, match="extra"):
        contract.parse_radar_cycle_request(raw)


def test_duplicate_and_dangling_identities_fail_but_provider_repetition_does_not():
    raw = _request()
    assert len(contract.parse_radar_cycle_request(raw).inputs) == 2

    duplicate = deepcopy(raw)
    duplicate["regions"][1]["region_id"] = duplicate["regions"][0]["region_id"]
    with pytest.raises(ValueError, match="region_id must be unique"):
        contract.parse_radar_cycle_request(duplicate)
    duplicate = deepcopy(raw)
    duplicate["targets"][1]["target_id"] = duplicate["targets"][0]["target_id"]
    with pytest.raises(ValueError, match="target_id must be globally unique"):
        contract.parse_radar_cycle_request(duplicate)
    duplicate = deepcopy(raw)
    duplicate["inputs"] = [duplicate["inputs"][0], duplicate["inputs"][0]]
    with pytest.raises(ValueError, match="identical radar inputs"):
        contract.parse_radar_cycle_request(duplicate)
    conflicting = deepcopy(raw)
    conflicting["inputs"][1]["artifact"] = deepcopy(
        conflicting["inputs"][0]["artifact"]
    )
    with pytest.raises(ValueError, match="artifact handle has conflicting"):
        contract.parse_radar_cycle_request(conflicting)
    dangling = deepcopy(raw)
    dangling["targets"][0]["region_id"] = "missing-region"
    with pytest.raises(ValueError, match="target references unknown"):
        contract.parse_radar_cycle_request(dangling)
    dangling = deepcopy(raw)
    dangling["inputs"][0]["region_ids"] = ["missing-region"]
    with pytest.raises(ValueError, match="input references unknown"):
        contract.parse_radar_cycle_request(dangling)


def test_storage_region_id_is_caller_stable_and_never_coordinate_derived():
    first_raw = _request()
    second_raw = deepcopy(first_raw)
    second_raw["regions"][1]["center_lat"] = 52.0
    second_raw["regions"][1]["center_lon"] = 5.0
    first = contract.parse_radar_cycle_request(first_raw)
    second = contract.parse_radar_cycle_request(second_raw)
    assert first.regions[0].storage_region_id == "home"
    assert second.regions[0].storage_region_id == "home"
    assert first.cycle_id != second.cycle_id

    unstable = deepcopy(first_raw)
    unstable["regions"][1]["region_id"] = "engine@51.0,4.0,250"
    with pytest.raises(ValueError, match="region_id"):
        contract.parse_radar_cycle_request(unstable)
    wrong_shape = deepcopy(first_raw)
    wrong_shape["regions"][1]["region_key"] = "engine@51.0,4.0,250"
    with pytest.raises(ValueError, match="extra"):
        contract.parse_radar_cycle_request(wrong_shape)


@pytest.mark.parametrize("mutation", [
    lambda value: value["artifact"].update(artifact_path="../escape.json.gz"),
    lambda value: value["artifact"].update(
        artifact_path="/share/../escape.json.gz"
    ),
    lambda value: value["artifact"].update(
        artifact_path="https://example.test/artifact"
    ),
    lambda value: value["artifact"].update(artifact_sha256="A" * 64),
    lambda value: value["artifact"].update(artifact_bytes=True),
    lambda value: value["artifact"].update(artifact_bytes=0),
    lambda value: value["artifact"].update(
        content_type="application/json; charset=utf-8"
    ),
])
def test_artifact_handle_adversarial_inputs_fail(mutation):
    raw = _request()
    value = _decoded_input()
    mutation(value)
    raw["inputs"] = [value]
    with pytest.raises((TypeError, ValueError)):
        contract.parse_radar_cycle_request(raw)


def test_json_parser_rejects_duplicates_nonfinite_constants_and_nonobjects():
    raw = _request()
    encoded = json.dumps(raw, separators=(",", ":"))
    duplicate = encoded.replace(
        '"requested_at":1800000020.875',
        '"requested_at":1800000020.875,"requested_at":1800000020.875',
        1,
    )
    with pytest.raises(ValueError, match="duplicate JSON key"):
        contract.parse_radar_cycle_request(duplicate)
    nonfinite = encoded.replace("1800000020.875", "NaN", 1)
    with pytest.raises(ValueError, match="non-finite JSON"):
        contract.parse_radar_cycle_request(nonfinite)
    with pytest.raises(ValueError, match="UTF-8"):
        contract.parse_radar_cycle_request(b"\xff")
    with pytest.raises(TypeError, match="root must be an object"):
        contract.parse_radar_cycle_request("[]")
    invalid_keys = _request()
    invalid_keys[1] = "not-a-JSON-key"
    with pytest.raises(TypeError, match="keys must be strings"):
        contract.parse_radar_cycle_request(invalid_keys)


def test_complete_coverage_requires_bbox_and_bbox_coordinates_are_finite():
    raw = _request()
    value = _decoded_input()
    value["coverage"] = {"status": "complete", "bbox": None}
    raw["inputs"] = [value]
    with pytest.raises(ValueError, match="complete coverage requires"):
        contract.parse_radar_cycle_request(raw)

    value["coverage"] = {
        "status": "incomplete",
        "bbox": [2.0, 49.0, float("inf"), 53.0],
    }
    with pytest.raises(ValueError, match="finite"):
        contract.parse_radar_cycle_request(raw)
