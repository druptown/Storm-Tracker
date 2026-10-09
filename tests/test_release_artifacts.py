"""Release archive naming, byte identity and fail-closed evidence proofs."""
from __future__ import annotations

import ast
from contextlib import contextmanager
import hashlib
import inspect
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import textwrap
import zipfile

import pytest

import release_install_bound_inputs as bound_inputs
import storm_tracker_worker.release_artifacts as release_artifacts_module
from descriptor_relative_authority import DirectoryAuthority, RelativePath
from storm_tracker_worker.release_artifacts import (
    RELEASE_EVIDENCE_CONTRACT,
    ReleaseArtifactError,
    build_release_candidate,
    inspect_source_tree,
    verify_release_candidate_evidence,
    verify_release_candidate_evidence_bound_inputs,
)


ROOT = Path(__file__).resolve().parents[1]


def _sources(tmp_path: Path) -> tuple[Path, Path]:
    integration = tmp_path / "source" / "storm_tracker_v3"
    worker = tmp_path / "worker-source" / "storm_tracker_worker"
    (integration / "engine").mkdir(parents=True)
    (worker / "nested").mkdir(parents=True)
    (integration / "manifest.json").write_text(json.dumps({
        "domain": "storm_tracker_v3",
        "name": "Storm Tracker V3",
        "version": "9.8.7",
    }), encoding="utf-8")
    (integration / "__init__.py").write_text(
        'STORE = Factory(build_id="integration-9.8.7-worker-6.5.4")\n',
        encoding="utf-8",
    )
    (integration / "engine" / "model.py").write_text(
        "MODEL = 'deterministic'\n", encoding="utf-8"
    )
    (worker / "config.yaml").write_text(
        'name: "Storm Tracker Worker"\nversion: "6.5.4"\n', encoding="utf-8"
    )
    (worker / "__init__.py").write_text(
        '__version__ = "6.5.4"\n', encoding="utf-8"
    )
    (worker / "run.sh").write_text("#!/bin/sh\nexec python -m service\n", encoding="utf-8")
    (worker / "service.py").write_text("def main():\n    return 0\n", encoding="utf-8")
    (worker / "requirements.txt").write_text("paho-mqtt==2.1.0\n", encoding="utf-8")
    (worker / "nested" / "contract.json").write_text(
        '{"admission":false}\n', encoding="utf-8"
    )
    return integration, worker


def _build(tmp_path: Path, name: str = "out"):
    integration, worker = _sources(tmp_path)
    output = tmp_path / name
    output.mkdir()
    return build_release_candidate(
        integration_root=integration,
        worker_root=worker,
        integration_version="9.8.7",
        worker_version="6.5.4",
        output_directory=output,
    )


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _bound_registry(
    result,
    *,
    keys: tuple[str, ...] = ("evidence", "integration", "worker"),
) -> bound_inputs.BoundInputRegistry:
    paths = {
        "evidence": result.evidence_path,
        "integration": result.integration_archive,
        "worker": result.worker_archive,
    }
    with DirectoryAuthority.open_absolute(result.evidence_path.parent) as authority:
        declarations: dict[str, bound_inputs.BoundInputDeclaration] = {}
        for key in keys:
            raw = paths[key].read_bytes()
            relative = RelativePath.parse(paths[key].name)
            with authority.open_file(relative) as source:
                identity = source.identity()
            declarations[key] = bound_inputs.BoundInputDeclaration(
                relative=relative,
                identity=identity,
                size=len(raw),
                sha256=hashlib.sha256(raw).hexdigest(),
                maximum_bytes=len(raw),
            )
        return bound_inputs.BoundInputRegistry.seal(
            authority,
            declarations,
            expected_keys=keys,
        )


class _RecordingBoundSource:
    def __init__(self, registry: bound_inputs.BoundInputRegistry) -> None:
        self.registry = registry
        self.open_calls: list[str] = []
        self.active: list[str] = []

    @property
    def keys(self) -> tuple[str, ...]:
        return self.registry.keys

    def require_exact_keys(self, expected_keys) -> None:
        self.registry.require_exact_keys(expected_keys)

    def metadata(self, key: str):
        return self.registry.metadata(key)

    @contextmanager
    def open_stream(self, key: str):
        self.open_calls.append(key)
        self.active.append(key)
        try:
            with self.registry.open_stream(key) as stream:
                yield stream
        finally:
            assert self.active.pop() == key


def test_build_is_deterministic_named_and_fully_reverified(tmp_path: Path):
    integration, worker = _sources(tmp_path)
    first_out = tmp_path / "first"
    second_out = tmp_path / "second"
    first_out.mkdir()
    second_out.mkdir()
    arguments = {
        "integration_root": integration,
        "worker_root": worker,
        "integration_version": "9.8.7",
        "worker_version": "6.5.4",
    }
    first = build_release_candidate(output_directory=first_out, **arguments)
    second = build_release_candidate(output_directory=second_out, **arguments)

    assert first.integration_archive.name == "storm_tracker_v3_v9.8.7_deploy.zip"
    assert first.worker_archive.name == "storm_tracker_worker_v6.5.4_deploy.tar.gz"
    assert first.integration_archive.read_bytes() == second.integration_archive.read_bytes()
    assert first.worker_archive.read_bytes() == second.worker_archive.read_bytes()
    assert first.evidence_path.read_bytes() == second.evidence_path.read_bytes()
    assert first.integration_sha256 == _sha(first.integration_archive)
    assert first.worker_sha256 == _sha(first.worker_archive)

    evidence = verify_release_candidate_evidence(first.evidence_path)
    assert evidence["contract"] == RELEASE_EVIDENCE_CONTRACT
    assert evidence["deployment_performed"] is False
    assert evidence["evidence_sha256"] == first.evidence_sha256
    assert evidence["source_trees"]["integration"]["file_count"] == 3
    assert evidence["source_trees"]["worker"]["file_count"] == 6

    with zipfile.ZipFile(first.integration_archive) as archive:
        assert archive.namelist()[0] == "storm_tracker_v3/"
        assert "storm_tracker_v3/manifest.json" in archive.namelist()
        assert all("\\" not in name and ".." not in name for name in archive.namelist())
    with tarfile.open(first.worker_archive, "r:gz") as archive:
        members = archive.getmembers()
        assert members[0].name == "storm_tracker_worker"
        run = archive.getmember("storm_tracker_worker/run.sh")
        assert run.mode == 0o755
        assert all(member.uid == 0 and member.gid == 0 and member.mtime == 0 for member in members)


def test_worker_canary_activation_and_packaging_files_enter_archive(
    tmp_path: Path,
) -> None:
    integration, worker = _sources(tmp_path)
    additions = {
        ".dockerignore": ".env\n**/__pycache__/\n",
        ".env.example": "STV3_RUNTIME_MODE=legacy\n",
        "compose.yaml": "services: {}\n",
        "worker_first_service_activation.py": "MODE = 'legacy'\n",
        "worker_first_service_composition.py": "CANARY = False\n",
    }
    for relative, content in additions.items():
        (worker / relative).write_text(content, encoding="utf-8")
    output = tmp_path / "canary-output"
    output.mkdir()

    result = build_release_candidate(
        integration_root=integration,
        worker_root=worker,
        integration_version="9.8.7",
        worker_version="6.5.4",
        output_directory=output,
    )

    expected = {
        "storm_tracker_worker/.dockerignore",
        "storm_tracker_worker/.env.example",
        "storm_tracker_worker/compose.yaml",
        "storm_tracker_worker/config.yaml",
        "storm_tracker_worker/run.sh",
        "storm_tracker_worker/worker_first_service_activation.py",
        "storm_tracker_worker/worker_first_service_composition.py",
    }
    with tarfile.open(result.worker_archive, "r:gz") as archive:
        names = {member.name for member in archive.getmembers()}
    assert expected <= names
    evidence = verify_release_candidate_evidence(result.evidence_path)
    paths = {
        item["path"]
        for item in evidence["source_trees"]["worker"]["files"]
    }
    assert {name.removeprefix("storm_tracker_worker/") for name in expected} <= paths


def test_postgis_station_storage_files_enter_worker_archive(tmp_path: Path):
    integration, worker = _sources(tmp_path)
    required = (
        "thin_station_measurement.py",
        "thin_postgis_station_migration.py",
        "manage_postgis_station_storage.py",
        "postgis_migrations/0005_station_measurements.sql",
    )
    for relative in required:
        source = ROOT / "storm_tracker_worker" / relative
        target = worker / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)
    output = tmp_path / "postgis-output"
    output.mkdir()

    result = build_release_candidate(
        integration_root=integration,
        worker_root=worker,
        integration_version="9.8.7",
        worker_version="6.5.4",
        output_directory=output,
    )

    with tarfile.open(result.worker_archive, "r:gz") as archive:
        names = {member.name for member in archive.getmembers()}
    assert {
        f"storm_tracker_worker/{relative}"
        for relative in required
    } <= names


def test_builder_refuses_existing_output_and_never_overwrites(tmp_path: Path):
    result = _build(tmp_path)
    before = result.integration_archive.read_bytes()
    integration = tmp_path / "source" / "storm_tracker_v3"
    worker = tmp_path / "worker-source" / "storm_tracker_worker"
    with pytest.raises(ReleaseArtifactError, match="bestaat al"):
        build_release_candidate(
            integration_root=integration,
            worker_root=worker,
            integration_version="9.8.7",
            worker_version="6.5.4",
            output_directory=result.integration_archive.parent,
        )
    assert result.integration_archive.read_bytes() == before


@pytest.mark.parametrize("target", ["integration", "worker"])
def test_requested_version_must_equal_internal_version(tmp_path: Path, target: str):
    integration, worker = _sources(tmp_path)
    output = tmp_path / "out"
    output.mkdir()
    arguments = {
        "integration_root": integration,
        "worker_root": worker,
        "integration_version": "9.8.7",
        "worker_version": "6.5.4",
        "output_directory": output,
    }
    arguments[f"{target}_version"] = "1.0.0"
    with pytest.raises(ReleaseArtifactError, match="versie"):
        build_release_candidate(**arguments)
    assert tuple(output.iterdir()) == ()


def test_worker_config_and_python_version_cannot_drift(tmp_path: Path):
    integration, worker = _sources(tmp_path)
    (worker / "__init__.py").write_text('__version__ = "6.5.3"\n', encoding="utf-8")
    output = tmp_path / "out"
    output.mkdir()
    with pytest.raises(ReleaseArtifactError, match="__version__"):
        build_release_candidate(
            integration_root=integration,
            worker_root=worker,
            integration_version="9.8.7",
            worker_version="6.5.4",
            output_directory=output,
        )
    assert tuple(output.iterdir()) == ()


@pytest.mark.parametrize(
    "integration_init",
    [
        'STORE = Factory(build_id="integration-9.8.7-worker-6.5.3")\n',
        "STORE = Factory()\n",
        (
            'BUILD_ID = "integration-9.8.7-worker-6.5.4"\n'
            "STORE = Factory(build_id=BUILD_ID)\n"
        ),
        (
            'FIRST = Factory(build_id="integration-9.8.7-worker-6.5.4")\n'
            'SECOND = Factory(build_id="integration-9.8.7-worker-6.5.4")\n'
        ),
    ],
    ids=("stale", "missing", "nonliteral", "duplicate"),
)
def test_integration_build_id_must_be_unique_literal_and_current(
    tmp_path: Path,
    integration_init: str,
):
    integration, worker = _sources(tmp_path)
    (integration / "__init__.py").write_text(
        integration_init,
        encoding="utf-8",
    )
    output = tmp_path / "out"
    output.mkdir()
    with pytest.raises(ReleaseArtifactError, match="build_id"):
        build_release_candidate(
            integration_root=integration,
            worker_root=worker,
            integration_version="9.8.7",
            worker_version="6.5.4",
            output_directory=output,
        )
    assert tuple(output.iterdir()) == ()


@pytest.mark.parametrize(
    ("relative", "content", "reason"),
    [
        ("__pycache__/bad.pyc", b"cache", "verboden pakketmap"),
        ("state.sqlite3", b"sqlite", "verboden pakketbestand"),
        ("secret.txt", b"-----BEGIN PRIVATE KEY-----\nabc", "private key"),
        ("old_deploy.tar.gz", b"archive", "verboden pakketbestand"),
    ],
)
def test_source_inventory_rejects_cache_state_keys_and_nested_archives(
    tmp_path: Path,
    relative: str,
    content: bytes,
    reason: str,
):
    integration, _worker = _sources(tmp_path)
    path = integration / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    with pytest.raises(ReleaseArtifactError, match=reason):
        inspect_source_tree(integration, expected_root_name="storm_tracker_v3")


def test_source_inventory_still_rejects_a_real_private_key_payload(tmp_path: Path):
    integration, _worker = _sources(tmp_path)
    (integration / "credential.txt").write_bytes(
        b"-----BEGIN PRIVATE KEY-----\nnot-a-real-secret\n"
        b"-----END PRIVATE KEY-----\n"
    )

    with pytest.raises(ReleaseArtifactError, match="private key"):
        inspect_source_tree(integration, expected_root_name="storm_tracker_v3")


def test_source_inventory_rejects_archive_member_count_before_build(
    tmp_path: Path,
    monkeypatch,
) -> None:
    integration, _worker = _sources(tmp_path)
    monkeypatch.setattr(release_artifacts_module, "MAX_ARCHIVE_MEMBER_COUNT", 3)

    with pytest.raises(ReleaseArtifactError, match="archiefmembergrens"):
        inspect_source_tree(
            integration,
            expected_root_name="storm_tracker_v3",
        )


def test_nested_ascii_source_tree_is_in_the_fixed_ustar_profile(tmp_path: Path):
    _integration, worker = _sources(tmp_path)

    tree = inspect_source_tree(worker, expected_root_name="storm_tracker_worker")

    assert "nested/contract.json" in {item.relative_path for item in tree.files}
    writer_source = inspect.getsource(release_artifacts_module._write_tar_gz)
    assert "format=tarfile.USTAR_FORMAT" in writer_source
    assert "tarfile.PAX_FORMAT" not in writer_source


@pytest.mark.parametrize("source_name", ("integration", "worker"))
@pytest.mark.parametrize("bad_name", ("niet-ascii-é.py", f"{'x' * 101}.py"))
def test_ascii_ustar_source_gate_precedes_every_staging_write(
    tmp_path: Path,
    monkeypatch,
    source_name: str,
    bad_name: str,
) -> None:
    integration, worker = _sources(tmp_path)
    selected = integration if source_name == "integration" else worker
    (selected / bad_name).write_bytes(b"bounded\n")
    output = tmp_path / "out"
    output.mkdir()

    def staging_write_would_be_too_late(*_args, **_kwargs):
        raise AssertionError("staging path allocated before USTAR source gate")

    monkeypatch.setattr(
        release_artifacts_module,
        "_staging_path",
        staging_write_would_be_too_late,
    )
    with pytest.raises(ReleaseArtifactError, match="ASCII USTAR-profiel"):
        build_release_candidate(
            integration_root=integration,
            worker_root=worker,
            integration_version="9.8.7",
            worker_version="6.5.4",
            output_directory=output,
        )
    assert tuple(output.iterdir()) == ()


def test_symlink_is_rejected_when_supported(tmp_path: Path):
    integration, _worker = _sources(tmp_path)
    link = integration / "linked.py"
    try:
        os.symlink(integration / "__init__.py", link)
    except (OSError, NotImplementedError):
        pytest.skip("symlinks zijn niet beschikbaar voor deze Windows-user")
    with pytest.raises(ReleaseArtifactError, match="symlink|regulier"):
        inspect_source_tree(integration, expected_root_name="storm_tracker_v3")


def test_archive_or_evidence_tamper_fails_closed(tmp_path: Path):
    result = _build(tmp_path)
    result.worker_archive.write_bytes(result.worker_archive.read_bytes() + b"tamper")
    with pytest.raises(ReleaseArtifactError, match="hash|grootte"):
        verify_release_candidate_evidence(result.evidence_path)

    document = json.loads(result.evidence_path.read_text(encoding="utf-8"))
    document["unexpected"] = True
    result.evidence_path.write_text(json.dumps(document), encoding="utf-8")
    with pytest.raises(ReleaseArtifactError, match="schema"):
        verify_release_candidate_evidence(result.evidence_path)


def test_zip_crc_failure_is_a_fixed_release_domain_error(tmp_path: Path):
    result = _build(tmp_path)
    document = json.loads(result.evidence_path.read_text(encoding="utf-8"))
    tree = release_artifacts_module._tree_from_mapping(
        document["source_trees"]["integration"]
    )
    corrupted = tmp_path / "crc-corrupted.zip"
    raw = bytearray(result.integration_archive.read_bytes())
    with zipfile.ZipFile(result.integration_archive, "r") as archive:
        info = next(item for item in archive.infolist() if not item.is_dir())
    central_offset = raw.find(b"PK\x01\x02")
    target_name = info.filename.encode("utf-8")
    while central_offset >= 0:
        name_bytes = int.from_bytes(
            raw[central_offset + 28:central_offset + 30], "little"
        )
        extra_bytes = int.from_bytes(
            raw[central_offset + 30:central_offset + 32], "little"
        )
        comment_bytes = int.from_bytes(
            raw[central_offset + 32:central_offset + 34], "little"
        )
        name = bytes(raw[central_offset + 46:central_offset + 46 + name_bytes])
        if name == target_name:
            claimed_crc = int.from_bytes(
                raw[central_offset + 16:central_offset + 20], "little"
            )
            raw[central_offset + 16:central_offset + 20] = (
                claimed_crc ^ 0x1
            ).to_bytes(4, "little")
            break
        central_offset += 46 + name_bytes + extra_bytes + comment_bytes
        if raw[central_offset:central_offset + 4] != b"PK\x01\x02":
            central_offset = -1
    assert central_offset >= 0
    corrupted.write_bytes(raw)

    with pytest.raises(
        ReleaseArtifactError,
        match="integratiearchief bevat gewijzigde of verboden bytes",
    ) as raised:
        release_artifacts_module._verify_zip(corrupted, tree)
    assert "CRC" not in str(raised.value)


@pytest.mark.parametrize(
    ("verifier_name", "archive_error", "expected_message"),
    (
        (
            "_verify_zip",
            zipfile.BadZipFile("private ZIP detail"),
            "integratiearchief bevat gewijzigde of verboden bytes",
        ),
        (
            "_verify_tar",
            tarfile.ReadError("private TAR detail"),
            "workerarchief bevat gewijzigde of verboden bytes",
        ),
    ),
)
def test_archive_open_and_read_errors_remain_fixed_release_errors(
    tmp_path: Path,
    monkeypatch,
    verifier_name: str,
    archive_error: Exception,
    expected_message: str,
):
    result = _build(tmp_path)
    document = json.loads(result.evidence_path.read_text(encoding="utf-8"))
    kind = "integration" if verifier_name == "_verify_zip" else "worker"
    tree = release_artifacts_module._tree_from_mapping(
        document["source_trees"][kind]
    )
    archive_path = (
        result.integration_archive if kind == "integration" else result.worker_archive
    )
    archive_module = zipfile if kind == "integration" else tarfile
    opener_name = "ZipFile" if kind == "integration" else "open"

    def fail_open(*_args, **_kwargs):
        raise archive_error

    monkeypatch.setattr(archive_module, opener_name, fail_open)
    with pytest.raises(ReleaseArtifactError, match=expected_message) as raised:
        getattr(release_artifacts_module, verifier_name)(archive_path, tree)
    assert "private" not in str(raised.value)


def test_archive_verifiers_delegate_bodies_to_the_shared_stream_primitive(
    tmp_path: Path,
    monkeypatch,
):
    result = _build(tmp_path)
    document = json.loads(result.evidence_path.read_text(encoding="utf-8"))
    integration_tree = release_artifacts_module._tree_from_mapping(
        document["source_trees"]["integration"]
    )
    worker_tree = release_artifacts_module._tree_from_mapping(
        document["source_trees"]["worker"]
    )
    original = release_artifacts_module.stream_archive_member
    calls: list[tuple[int, int]] = []

    def record_call(source, **kwargs):
        calls.append((kwargs["expected_size"], kwargs["maximum_size"]))
        return original(source, **kwargs)

    monkeypatch.setattr(release_artifacts_module, "stream_archive_member", record_call)
    release_artifacts_module._verify_zip(result.integration_archive, integration_tree)
    release_artifacts_module._verify_tar(result.worker_archive, worker_tree)
    expected_sizes = [
        item.size for item in (*integration_tree.files, *worker_tree.files)
    ]
    assert [size for size, _maximum in calls] == expected_sizes
    assert all(
        maximum == release_artifacts_module.MAX_SOURCE_FILE_BYTES
        for _size, maximum in calls
    )


def test_archive_verifiers_preflight_the_same_descriptor_before_stdlib():
    zip_source = inspect.getsource(release_artifacts_module._verify_zip)
    zip_payload = inspect.getsource(release_artifacts_module._verify_zip_payload)
    tar_source = inspect.getsource(release_artifacts_module._verify_tar)
    tar_payload = inspect.getsource(release_artifacts_module._verify_tar_payload)
    assert "validated_zip_file(" in zip_source
    assert "zipfile.ZipFile(archive_source" in zip_payload
    assert "zipfile.ZipFile(path" not in zip_source
    assert "validated_tar_gz_file(" in tar_source
    assert "tarfile.open(fileobj=archive_source" in tar_payload
    assert "tarfile.open(path" not in tar_source


def test_bound_verifier_uses_one_stream_per_key_for_all_verification_stages(
    tmp_path: Path,
    monkeypatch,
):
    result = _build(tmp_path)
    registry = _bound_registry(result)
    source = _RecordingBoundSource(registry)
    observed: dict[str, list[tuple[str, int]]] = {
        "evidence": [],
        "integration": [],
        "worker": [],
    }

    def wrap(name, function):
        def recorder(stream, *args, **kwargs):
            key = source.active[-1]
            observed[key].append((name, id(stream)))
            return function(stream, *args, **kwargs)

        return recorder

    def record_decode(value):
        assert source.active == ["evidence"]
        observed["evidence"].append(("json", id(value)))
        return original_decode(value)

    original_hash = release_artifacts_module._hash_bound_stream
    original_zip_header = release_artifacts_module.validate_zip_header_stream
    original_tar_header = release_artifacts_module.validate_tar_gz_header_stream
    original_zip_payload = release_artifacts_module._verify_zip_payload
    original_tar_payload = release_artifacts_module._verify_tar_payload
    original_decode = release_artifacts_module._decode_bound_evidence
    monkeypatch.setattr(
        release_artifacts_module,
        "_hash_bound_stream",
        wrap("hash", original_hash),
    )
    monkeypatch.setattr(
        release_artifacts_module,
        "validate_zip_header_stream",
        wrap("header", original_zip_header),
    )
    monkeypatch.setattr(
        release_artifacts_module,
        "validate_tar_gz_header_stream",
        wrap("header", original_tar_header),
    )
    monkeypatch.setattr(
        release_artifacts_module,
        "_verify_zip_payload",
        wrap("stdlib", original_zip_payload),
    )
    monkeypatch.setattr(
        release_artifacts_module,
        "_verify_tar_payload",
        wrap("stdlib", original_tar_payload),
    )
    monkeypatch.setattr(
        release_artifacts_module,
        "_decode_bound_evidence",
        record_decode,
    )
    try:
        document = verify_release_candidate_evidence_bound_inputs(source)
        assert document["contract"] == RELEASE_EVIDENCE_CONTRACT
        assert source.open_calls == ["evidence", "integration", "worker"]
        assert [name for name, _identity in observed["evidence"]] == [
            "hash", "json"
        ]
        for key in ("integration", "worker"):
            stages = observed[key]
            assert [name for name, _identity in stages] == [
                "hash", "header", "stdlib"
            ]
            assert len({identity for _name, identity in stages}) == 1
    finally:
        registry.close()


def test_bound_verifier_rejects_nonexact_keys_before_opening_a_stream(
    tmp_path: Path,
):
    result = _build(tmp_path)
    registry = _bound_registry(result, keys=("evidence", "integration"))
    source = _RecordingBoundSource(registry)
    try:
        with pytest.raises(ReleaseArtifactError, match="sleutelset"):
            verify_release_candidate_evidence_bound_inputs(source)
        assert source.open_calls == []
    finally:
        registry.close()


@pytest.mark.parametrize(
    "raw",
    (
        b'{"duplicate":1,"duplicate":2}',
        b'{"floating":1.5}',
        b'{"constant":NaN}',
        json.dumps({"k" * 257: 1}).encode(),
        json.dumps({"value": "x" * (1024 * 1024 + 1)}).encode(),
        b'{"integer":2147483649}',
        b'{"deep":' + b"[" * 65 + b"0" + b"]" * 65 + b"}",
    ),
    ids=(
        "duplicate-key",
        "float",
        "nonfinite",
        "oversized-key",
        "oversized-string",
        "oversized-integer",
        "excess-depth",
    ),
)
def test_bound_evidence_parser_rejects_adversarial_json(raw: bytes):
    with pytest.raises(ReleaseArtifactError):
        release_artifacts_module._decode_bound_evidence(raw)


def test_bound_verifier_entrypoint_has_no_path_or_sibling_reconstruction():
    source = textwrap.dedent(inspect.getsource(
        release_artifacts_module.verify_release_candidate_evidence_bound_inputs
    ))
    parsed = ast.parse(source)
    assert not any(
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id in {"Path", "open"}
        for node in ast.walk(parsed)
    )
    assert not any(
        isinstance(node, ast.Attribute) and node.attr in {"parent", "resolve"}
        for node in ast.walk(parsed)
    )
    assert "open_stream(kind)" in source
    assert "directory" not in source


def test_cli_builds_pair_and_reports_no_deployment(tmp_path: Path):
    integration, worker = _sources(tmp_path)
    output = tmp_path / "cli-output"
    output.mkdir()
    completed = subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts" / "build_release_candidate.py"),
            "--integration-root", str(integration),
            "--worker-root", str(worker),
            "--integration-version", "9.8.7",
            "--worker-version", "6.5.4",
            "--output-directory", str(output),
        ],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    report = json.loads(completed.stdout)
    assert report["deployment_performed"] is False
    assert Path(report["integration_archive"]).name.endswith("_deploy.zip")
    assert Path(report["worker_archive"]).name.endswith("_deploy.tar.gz")
    verify_release_candidate_evidence(Path(report["evidence"]))
