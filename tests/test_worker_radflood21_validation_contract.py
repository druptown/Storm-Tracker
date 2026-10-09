from __future__ import annotations

from copy import deepcopy
import hashlib
import inspect
import json
from pathlib import Path

import pytest

from storm_tracker_worker import radflood21_validation_contract as contract


def _manifest_mapping() -> dict:
    path = Path(contract.__file__).with_name(contract.MANIFEST_FILENAME)
    return json.loads(path.read_text(encoding="utf-8"))


def _zenodo_snapshot() -> dict:
    manifest = _manifest_mapping()
    record = manifest["record"]
    return {
        "id": record["record_id"],
        "conceptrecid": str(record["concept_record_id"]),
        "doi": record["doi"],
        "conceptdoi": record["concept_doi"],
        "revision": record["revision"],
        "created": record["created_at"],
        "updated": record["updated_at"],
        "metadata": {
            "title": record["title"],
            "publication_date": record["publication_date"],
            "access_right": record["access_right"],
            "license": {"id": record["license_id"]},
            "resource_type": {
                "type": record["resource_type"],
                "title": "Dataset",
            },
            "creators": deepcopy(record["creators"]),
        },
        "files": [
            {
                "key": artifact["name"],
                "size": artifact["size_bytes"],
                "checksum": (
                    f"{artifact['checksum_algorithm']}:"
                    f"{artifact['checksum_hex']}"
                ),
            }
            for artifact in manifest["artifacts"]
        ],
    }


def test_pinned_manifest_loads_and_exposes_exact_official_identity():
    manifest = contract.load_official_manifest()

    assert manifest.record_id == 7_740_059
    assert manifest.concept_record_id == 7_740_058
    assert manifest.doi == "10.5281/zenodo.7740059"
    assert manifest.concept_doi == "10.5281/zenodo.7740058"
    assert manifest.revision == 8
    assert manifest.license_id == "cc-by-4.0"
    assert manifest.canonical_sha256 == (
        "f70b360db33e2d0aac5a052183d97a42a89ee3b9dc1a6916b9125ca53aefa741"
    )


def test_pinned_archive_and_visualisation_have_exact_size_and_md5():
    manifest = contract.load_official_manifest()
    by_name = {artifact.name: artifact for artifact in manifest.artifacts}

    archive = by_name["radflood21.zip"]
    assert archive.role == "validation_corpus_archive"
    assert archive.size_bytes == 2_642_551_002
    assert archive.zenodo_checksum == (
        "md5:2cb3660a9a2b81fcc3fb98d103984e12"
    )
    video = by_name["radflood21.mp4"]
    assert video.role == "visualisation_only"
    assert video.size_bytes == 45_375_862
    assert video.zenodo_checksum == (
        "md5:ab87ef2df8bf0008a39dd6b791168ea2"
    )


def test_manifest_can_validate_archive_identity_but_never_decode_or_admit():
    manifest = contract.load_official_manifest()

    assert manifest.archive_identity_validation_eligible is True
    assert manifest.decoder_eligible is False
    assert manifest.live_provider_eligible is False
    assert manifest.operational_admission_eligible is False
    assert manifest.open_decoder_gates == contract.OPEN_DECODER_GATES
    assert len(manifest.open_decoder_gates) == 9


@pytest.mark.parametrize(
    ("path", "replacement"),
    (
        (("record", "record_id"), 7740060),
        (("record", "doi"), "10.5281/zenodo.7740060"),
        (("record", "license_id"), "cc0-1.0"),
        (("record", "revision"), 9),
        (("artifacts", 0, "size_bytes"), 2_642_551_001),
        (("artifacts", 0, "checksum_hex"), "0" * 32),
        (("artifacts", 1, "name"), "renamed.mp4"),
        (("decoder_gate", "decoder_eligible"), True),
        (("product", "spatial_resolution_metres"), 500),
    ),
)
def test_synthetic_manifest_drift_hard_fails(path, replacement):
    value = _manifest_mapping()
    target = value
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = replacement

    with pytest.raises(contract.Radflood21ValidationError):
        contract.validate_manifest_mapping(value)


def test_synthetic_manifest_missing_checksum_hard_fails():
    value = _manifest_mapping()
    del value["artifacts"][0]["checksum_hex"]

    with pytest.raises(contract.Radflood21ValidationError):
        contract.validate_manifest_mapping(value)


def test_manifest_duplicate_json_key_is_rejected(tmp_path):
    path = tmp_path / contract.MANIFEST_FILENAME
    path.write_text(
        '{"schema_version":"one","schema_version":"two"}',
        encoding="utf-8",
    )

    with pytest.raises(contract.Radflood21ValidationError, match="dubbele"):
        contract.load_official_manifest(path)


def test_exact_synthetic_zenodo_snapshot_mapping_and_bytes_validate():
    snapshot = _zenodo_snapshot()

    from_mapping = contract.validate_zenodo_record_snapshot(snapshot)
    payload = json.dumps(snapshot, ensure_ascii=False).encode("utf-8")
    from_bytes = contract.validate_zenodo_record_snapshot(payload)

    assert from_mapping.record_id == contract.ZENODO_RECORD_ID
    assert from_mapping.revision == 8
    assert from_mapping.artifact_count == 2
    assert from_mapping.matches_pinned_manifest is True
    assert from_mapping.metadata_sha256 == from_bytes.metadata_sha256
    assert from_mapping.to_dict()["decoder_eligible"] is False


@pytest.mark.parametrize(
    ("path", "replacement"),
    (
        (("id",), 7740058),
        (("conceptrecid",), 7740058),
        (("doi",), "10.5281/zenodo.7740058"),
        (("revision",), 7),
        (("updated",), "2023-10-17T12:06:58+00:00"),
        (("metadata", "title"), "RADFLOOD21"),
        (("metadata", "access_right"), "restricted"),
        (("metadata", "license", "id"), "cc0-1.0"),
        (("metadata", "resource_type", "type"), "other"),
        (("metadata", "creators", 0, "affiliation"), "Unknown"),
        (("files", 0, "size"), 1),
        (("files", 0, "checksum"), "md5:" + "0" * 32),
    ),
)
def test_synthetic_zenodo_metadata_or_checksum_drift_hard_fails(
    path,
    replacement,
):
    value = _zenodo_snapshot()
    target = value
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = replacement

    with pytest.raises(contract.Radflood21ValidationError):
        contract.validate_zenodo_record_snapshot(value)


@pytest.mark.parametrize(
    "path",
    (
        ("conceptdoi",),
        ("metadata", "publication_date"),
        ("metadata", "license"),
        ("files", 0, "checksum"),
    ),
)
def test_synthetic_zenodo_missing_metadata_hard_fails(path):
    value = _zenodo_snapshot()
    target = value
    for key in path[:-1]:
        target = target[key]
    del target[path[-1]]

    with pytest.raises(contract.Radflood21ValidationError):
        contract.validate_zenodo_record_snapshot(value)


def test_zenodo_extra_or_duplicate_file_hard_fails():
    value = _zenodo_snapshot()
    value["files"].append(deepcopy(value["files"][0]))

    with pytest.raises(contract.Radflood21ValidationError):
        contract.validate_zenodo_record_snapshot(value)


def test_local_synthetic_artifact_is_streamed_and_receipted(tmp_path):
    payload = b"synthetic-radflood21-integrity-fixture" * 100
    path = tmp_path / "radflood21-test.bin"
    path.write_bytes(payload)
    artifact = contract.Radflood21Artifact(
        name=path.name,
        role="synthetic_validation_only",
        size_bytes=len(payload),
        checksum_algorithm="md5",
        checksum_hex=hashlib.md5(
            payload,
            usedforsecurity=False,
        ).hexdigest(),
    )

    receipt = contract.validate_local_artifact(
        path,
        artifact,
        chunk_bytes=17,
    )

    assert receipt.size_bytes == len(payload)
    assert receipt.official_md5 == artifact.checksum_hex
    assert receipt.local_sha256 == hashlib.sha256(payload).hexdigest()
    assert receipt.to_dict()["upstream_checksum_matched"] is True
    assert receipt.to_dict()["decoder_eligible"] is False


def test_local_artifact_size_name_and_checksum_fail_closed(tmp_path):
    path = tmp_path / "radflood21-test.bin"
    path.write_bytes(b"payload")
    correct_md5 = hashlib.md5(
        b"payload",
        usedforsecurity=False,
    ).hexdigest()

    with pytest.raises(contract.Radflood21ValidationError, match="naam"):
        contract.validate_local_artifact(
            path,
            contract.Radflood21Artifact(
                name="other.bin",
                role="synthetic_validation_only",
                size_bytes=7,
                checksum_algorithm="md5",
                checksum_hex=correct_md5,
            ),
        )
    with pytest.raises(contract.Radflood21ValidationError, match="grootte"):
        contract.validate_local_artifact(
            path,
            contract.Radflood21Artifact(
                name=path.name,
                role="synthetic_validation_only",
                size_bytes=8,
                checksum_algorithm="md5",
                checksum_hex=correct_md5,
            ),
        )
    with pytest.raises(contract.Radflood21ValidationError, match="MD5"):
        contract.validate_local_artifact(
            path,
            contract.Radflood21Artifact(
                name=path.name,
                role="synthetic_validation_only",
                size_bytes=7,
                checksum_algorithm="md5",
                checksum_hex="0" * 32,
            ),
        )


def test_local_artifact_replacement_between_stat_and_open_fails_closed(
    tmp_path,
    monkeypatch,
):
    original = b"first-proof-bytes"
    replacement = b"other-proof-bytes"
    assert len(original) == len(replacement)
    path = tmp_path / "radflood21-test.bin"
    alternate = tmp_path / "alternate.bin"
    path.write_bytes(original)
    alternate.write_bytes(replacement)
    artifact = contract.Radflood21Artifact(
        name=path.name,
        role="synthetic_validation_only",
        size_bytes=len(original),
        checksum_algorithm="md5",
        checksum_hex=hashlib.md5(
            original,
            usedforsecurity=False,
        ).hexdigest(),
    )
    real_open = contract.os.open
    swapped = False

    def replace_then_open(candidate, flags):
        nonlocal swapped
        if Path(candidate) == path and not swapped:
            swapped = True
            alternate.replace(path)
        return real_open(candidate, flags)

    monkeypatch.setattr(contract.os, "open", replace_then_open)

    with pytest.raises(contract.Radflood21ValidationError, match="openen"):
        contract.validate_local_artifact(path, artifact)


def test_mapping_snapshot_obeys_same_byte_bound_as_raw_snapshot():
    snapshot = _zenodo_snapshot()
    snapshot["oversized"] = "x" * contract.MAX_ZENODO_SNAPSHOT_BYTES

    with pytest.raises(contract.Radflood21ValidationError, match="bytecontract"):
        contract.validate_zenodo_record_snapshot(snapshot)


def test_official_archive_stub_fails_on_size_without_reading_2_6_gb(tmp_path):
    path = tmp_path / contract.CORPUS_ARCHIVE_NAME
    path.write_bytes(b"not-the-corpus")

    with pytest.raises(contract.Radflood21ValidationError, match="grootte"):
        contract.validate_official_corpus_archive(path)


@pytest.mark.parametrize("chunk_bytes", (0, -1, True, 16 * 1024 * 1024 + 1))
def test_file_validator_chunk_contract_is_bounded(tmp_path, chunk_bytes):
    path = tmp_path / "radflood21-test.bin"
    path.write_bytes(b"x")
    artifact = contract.Radflood21Artifact(
        name=path.name,
        role="synthetic_validation_only",
        size_bytes=1,
        checksum_algorithm="md5",
        checksum_hex=hashlib.md5(b"x", usedforsecurity=False).hexdigest(),
    )

    with pytest.raises(ValueError):
        contract.validate_local_artifact(
            path,
            artifact,
            chunk_bytes=chunk_bytes,
        )


def test_official_end_stamped_accumulation_support_is_exact():
    five_minutes = contract.accumulation_support(
        "2021-07-14T12:05:00Z",
        300,
    )
    hourly = contract.accumulation_support(
        "2021-07-14T13:00:00Z",
        3_600,
    )

    assert five_minutes.start == "2021-07-14T12:00:00Z"
    assert five_minutes.end == "2021-07-14T12:05:00Z"
    assert five_minutes.timestamp_role == "end_of_accumulation_period"
    assert hourly.start == "2021-07-14T12:00:00Z"
    assert hourly.end == "2021-07-14T13:00:00Z"


@pytest.mark.parametrize(
    ("timestamp", "duration"),
    (
        ("2021-07-14T12:05:00", 300),
        ("2021-07-14T12:05:00+00:00", 300),
        ("not-a-time", 300),
        ("2021-07-14T12:05:00Z", 600),
        ("2021-07-14T12:05:00Z", True),
    ),
)
def test_accumulation_support_rejects_unproven_time_or_duration(
    timestamp,
    duration,
):
    with pytest.raises(contract.Radflood21ValidationError):
        contract.accumulation_support(timestamp, duration)


def test_module_is_unwired_and_has_no_network_archive_or_hdf5_decoder():
    source = inspect.getsource(contract)
    forbidden = (
        "urllib.request",
        "import requests",
        "import aiohttp",
        "import httpx",
        "import zipfile",
        "import h5py",
        "provider_radar",
    )
    assert not any(token in source for token in forbidden)

    worker_root = Path(contract.__file__).resolve().parent
    for candidate in worker_root.glob("*.py"):
        if candidate.name in {
            "radflood21_validation_contract.py",
            "worker_cutover_manifest.py",
        }:
            continue
        assert "radflood21_validation_contract" not in candidate.read_text(
            encoding="utf-8",
            errors="strict",
        )


def test_documentation_contains_every_open_gate_and_primary_source():
    root = Path(__file__).resolve().parents[1]
    text = (
        root / "docs" / "radflood21_validation_contract.md"
    ).read_text(encoding="utf-8")
    manifest = _manifest_mapping()

    for gate in contract.OPEN_DECODER_GATES:
        assert gate in text
    for source in manifest["primary_sources"].values():
        assert str(source) in text
    assert "2.642.551.002" in text
    assert "downloadt de 2,6 GB ZIP niet" in text
    assert "geen live provider" in " ".join(text.split())
