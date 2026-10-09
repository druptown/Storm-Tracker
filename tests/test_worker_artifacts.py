from __future__ import annotations

import gzip
import hashlib
import json
from pathlib import Path

import pytest

from storm_tracker_worker import artifacts


def test_json_artifact_is_atomic_bounded_and_verifiable(tmp_path, monkeypatch):
    monkeypatch.setattr(artifacts, "ARTIFACT_ROOT", tmp_path)
    metadata = artifacts.write_json_artifact(
        "job-123",
        {"provider": "dwd_radolan", "observations": [{"lat": 51.0}]},
    )
    target = tmp_path / Path(metadata["artifact_path"]).name
    payload = target.read_bytes()
    assert metadata["artifact_path"] == str(target)
    assert metadata["artifact_sha256"] == hashlib.sha256(payload).hexdigest()
    assert target.name == f"job-123-{metadata['artifact_sha256']}.json.gz"
    assert json.loads(gzip.decompress(payload))["provider"] == "dwd_radolan"
    assert not list(tmp_path.glob(".job-123.*"))


def test_same_job_with_new_bytes_keeps_first_artifact_valid(
    tmp_path, monkeypatch,
):
    monkeypatch.setattr(artifacts, "ARTIFACT_ROOT", tmp_path)
    first = artifacts.write_json_artifact("same-job", {"sequence": 1})
    first_path = Path(first["artifact_path"])
    first_bytes = first_path.read_bytes()

    second = artifacts.write_json_artifact("same-job", {"sequence": 2})
    second_path = Path(second["artifact_path"])

    assert first_path != second_path
    assert first_path.read_bytes() == first_bytes
    assert artifacts.read_json_artifact(first) == {"sequence": 1}
    assert artifacts.read_json_artifact(second) == {"sequence": 2}


def test_identical_artifact_is_reused_without_overwriting(tmp_path, monkeypatch):
    monkeypatch.setattr(artifacts, "ARTIFACT_ROOT", tmp_path)
    first = artifacts.write_json_artifact("same-job", {"sequence": 1})
    first_path = Path(first["artifact_path"])
    first_bytes = first_path.read_bytes()

    repeated = artifacts.write_json_artifact("same-job", {"sequence": 1})

    assert repeated == first
    assert first_path.read_bytes() == first_bytes
    assert not list(tmp_path.glob(".same-job.*"))


def test_existing_content_path_with_other_bytes_is_never_overwritten(
    tmp_path, monkeypatch,
):
    monkeypatch.setattr(artifacts, "ARTIFACT_ROOT", tmp_path)
    value = {"sequence": 1}
    metadata = artifacts.write_json_artifact("same-job", value)
    target = Path(metadata["artifact_path"])
    target.write_bytes(b"corrupt-or-colliding-content")

    with pytest.raises(ValueError, match="andere bytes"):
        artifacts.write_json_artifact("same-job", value)

    assert target.read_bytes() == b"corrupt-or-colliding-content"


def test_json_artifact_rejects_unsafe_job_id(tmp_path, monkeypatch):
    monkeypatch.setattr(artifacts, "ARTIFACT_ROOT", tmp_path)
    with pytest.raises(ValueError, match="taak-ID"):
        artifacts.write_json_artifact("../escape", {"ok": True})
