"""Fail-closed reset preview, preservation and exact execution boundaries."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import subprocess
import sys

import pytest

from scripts import reset_storm_tracker_runtime_history as reset


ROOT = Path(__file__).resolve().parents[1]


def _relative(path: Path, root: Path) -> str:
    return PurePosixPath(*path.relative_to(root).parts).as_posix()


def _layout(tmp_path: Path) -> tuple[Path, Path]:
    root = tmp_path / "live" / "storm_tracker_runtime"
    target = root / "runtime" / "raw_captures"
    (target / "kmi").mkdir(parents=True)
    (target / "kmi" / "aws_10min.bin").write_bytes(b"exact-kmi-response")
    (target / "public_provider_payload.tar.gz").write_bytes(b"raw-upstream-archive")
    (target / "capture_metadata.json").write_text(
        '{"semantic_status":"unknown"}\n',
        encoding="utf-8",
    )
    return root.resolve(), target.resolve()


def _config_document(
    root: Path,
    targets: list[dict[str, str]],
    *,
    operation_id: str = "reset-runtime-history-20260814",
) -> dict[str, object]:
    return {
        "contract": reset.CONFIG_CONTRACT,
        "operation_id": operation_id,
        "preservation_policy": reset.PRESERVATION_POLICY,
        "roots": [{
            "absolute_path": str(root),
            "root_id": "worker_runtime",
            "scope_assertion": reset.ROOT_SCOPE_ASSERTION,
        }],
        "targets": targets,
    }


def _target_record(
    root: Path,
    target: Path,
    *,
    target_id: str = "raw_capture_store",
    category: str = "meteorological_raw_capture_payloads",
    expected_kind: str = "directory",
) -> dict[str, str]:
    return {
        "category": category,
        "data_assertion": reset.TARGET_DATA_ASSERTION,
        "expected_kind": expected_kind,
        "relative_path": _relative(target, root),
        "root_id": "worker_runtime",
        "target_id": target_id,
    }


def _write_config(tmp_path: Path, document: dict[str, object]) -> Path:
    path = tmp_path / "explicit-reset-config.json"
    path.write_text(
        json.dumps(document, ensure_ascii=True, sort_keys=True),
        encoding="utf-8",
    )
    return path.resolve()


def _preview(tmp_path: Path) -> tuple[Path, Path, dict[str, object], dict[str, object]]:
    root, target = _layout(tmp_path)
    config = _config_document(root, [_target_record(root, target)])
    config_path = _write_config(tmp_path, config)
    manifest_path = (tmp_path / "sealed-reset-manifest.json").resolve()
    result = reset.build_reset_manifest(
        config_path=config_path,
        manifest_output=manifest_path,
    )
    return root, target, result, json.loads(manifest_path.read_text(encoding="utf-8"))


def test_preview_writes_canonical_manifest_and_never_deletes(tmp_path: Path):
    root, target, result, manifest = _preview(tmp_path)
    manifest_path = Path(str(result["manifest_path"]))
    raw = manifest_path.read_bytes()

    assert target.is_dir()
    assert (target / "kmi" / "aws_10min.bin").read_bytes() == b"exact-kmi-response"
    assert result["destructive_action_performed"] is False
    assert result["target_count"] == 1
    assert result["entry_count"] == 5
    assert result["manifest_sha256"] == hashlib.sha256(raw).hexdigest()
    assert raw == reset._canonical_json(manifest)
    assert manifest["contract"] == reset.MANIFEST_CONTRACT
    assert manifest["action"] == reset.RESET_ACTION
    assert manifest["destructive_action_performed"] is False
    assert manifest["worker_stop_required"] is True
    assert manifest["preservation"] == {
        "domains": list(reset.PRESERVED_DOMAINS),
        "policy": reset.PRESERVATION_POLICY,
    }
    assert manifest["roots"][0]["canonical_path"] == str(root)
    scheduled = {
        entry["relative_path"] for entry in manifest["targets"][0]["entries"]
    }
    assert _relative(target, root) in scheduled
    assert "." not in scheduled


@pytest.mark.parametrize(
    "protected_relative",
    (
        ".storage/core.config_entries",
        "recorder/home-assistant_v2.db",
        "credentials/provider.json",
        "runtime/provider_token_state.json",
        "control/desired_state.json",
        "authority/provider-retention.json",
        "rollback/storm_tracker_worker_pre_rollback_20260814.tar.gz",
        "release/storm_tracker_release_evidence.json",
        "configuration.yaml",
        "package/config.yaml",
        "package/worker.py",
    ),
)
def test_preview_refuses_every_preserved_domain_inside_a_target(
    tmp_path: Path,
    protected_relative: str,
):
    root, target = _layout(tmp_path)
    protected = target.joinpath(*PurePosixPath(protected_relative).parts)
    protected.parent.mkdir(parents=True, exist_ok=True)
    protected.write_bytes(b"must-survive")
    config = _config_document(root, [_target_record(root, target)])
    config_path = _write_config(tmp_path, config)
    manifest_path = (tmp_path / "must-not-exist.json").resolve()

    with pytest.raises(reset.ResetSafetyError, match="beschermde grens"):
        reset.build_reset_manifest(
            config_path=config_path,
            manifest_output=manifest_path,
        )

    assert protected.read_bytes() == b"must-survive"
    assert not manifest_path.exists()


@pytest.mark.parametrize(
    "protected_ancestor",
    (
        ".storage",
        "recorder",
        "credentials",
        "control",
        "desired_state",
        "authority",
        "rollback",
        "release",
        "evidence",
    ),
)
def test_protected_ancestor_cannot_be_a_deletion_authority_root(
    tmp_path: Path,
    protected_ancestor: str,
):
    root = tmp_path / protected_ancestor / "storm_tracker_runtime"
    target = root / "runtime" / "raw_captures"
    target.mkdir(parents=True)
    payload = target / "provider-response.bin"
    payload.write_bytes(b"must-survive")
    config_path = _write_config(
        tmp_path,
        _config_document(
            root.resolve(),
            [_target_record(root.resolve(), target.resolve())],
        ),
    )
    manifest_path = (tmp_path / "must-not-exist.json").resolve()

    with pytest.raises(reset.ResetSafetyError, match="beschermde grens"):
        reset.build_reset_manifest(
            config_path=config_path,
            manifest_output=manifest_path,
        )

    assert payload.read_bytes() == b"must-survive"
    assert not manifest_path.exists()


def test_correct_execution_deletes_only_inventory_and_preserves_siblings(
    tmp_path: Path,
):
    root, target, preview, _manifest = _preview(tmp_path)
    preserved = {
        root / "control" / "desired_state.json": b"desired-state",
        root / "credentials" / "provider_token_state.json": b"token-state",
        root / "rollback" / "worker_pre_rollback.tar.gz": b"rollback",
        root / "release" / "candidate_evidence.json": b"release-evidence",
        tmp_path / "home-assistant_v2.db": b"ha-recorder",
        tmp_path / "configuration.yaml": b"ha-config",
    }
    for path, payload in preserved.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)
    evidence_path = (tmp_path / "reset-execution-evidence.json").resolve()

    result = reset.execute_reset_manifest(
        manifest_path=Path(str(preview["manifest_path"])),
        manifest_sha256=str(preview["manifest_sha256"]),
        confirmation=reset.EXECUTION_CONFIRMATION,
        execution_evidence_output=evidence_path,
    )

    assert result["destructive_action_performed"] is True
    assert result["deleted_entry_count"] == preview["entry_count"]
    assert not target.exists()
    assert root.is_dir()
    for path, payload in preserved.items():
        assert path.read_bytes() == payload
    assert Path(str(preview["manifest_path"])).is_file()
    evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
    assert evidence["contract"] == reset.EXECUTION_EVIDENCE_CONTRACT
    assert evidence["completed"] is True
    assert evidence["manifest_sha256"] == preview["manifest_sha256"]
    assert result["execution_evidence_sha256"] == hashlib.sha256(
        evidence_path.read_bytes()
    ).hexdigest()


@pytest.mark.parametrize(
    ("hash_transform", "confirmation", "reason"),
    (
        (lambda _value: "0" * 64, reset.EXECUTION_CONFIRMATION, "komt niet exact"),
        (lambda value: value, "YES", "bevestiging ontbreekt"),
    ),
    ids=("wrong-manifest-hash", "wrong-confirmation"),
)
def test_execution_gate_fails_before_any_delete_or_evidence_creation(
    tmp_path: Path,
    hash_transform,
    confirmation: str,
    reason: str,
):
    _root, target, preview, _manifest = _preview(tmp_path)
    evidence_path = (tmp_path / "must-not-exist-evidence.json").resolve()

    with pytest.raises(reset.ResetSafetyError, match=reason):
        reset.execute_reset_manifest(
            manifest_path=Path(str(preview["manifest_path"])),
            manifest_sha256=hash_transform(str(preview["manifest_sha256"])),
            confirmation=confirmation,
            execution_evidence_output=evidence_path,
        )

    assert target.is_dir()
    assert not evidence_path.exists()


def test_inventory_drift_fails_before_first_delete(tmp_path: Path):
    _root, target, preview, _manifest = _preview(tmp_path)
    original = target / "kmi" / "aws_10min.bin"
    late = target / "late-provider-response.bin"
    late.write_bytes(b"arrived-after-preview")
    evidence_path = (tmp_path / "drift-evidence.json").resolve()

    with pytest.raises(reset.ResetSafetyError, match="inventaris wijkt af"):
        reset.execute_reset_manifest(
            manifest_path=Path(str(preview["manifest_path"])),
            manifest_sha256=str(preview["manifest_sha256"]),
            confirmation=reset.EXECUTION_CONFIRMATION,
            execution_evidence_output=evidence_path,
        )

    assert original.is_file()
    assert late.read_bytes() == b"arrived-after-preview"
    assert not evidence_path.exists()


def test_rehashed_manifest_cannot_redirect_a_target(tmp_path: Path):
    root, target, preview, manifest = _preview(tmp_path)
    protected = root / "control" / "desired_state.json"
    protected.parent.mkdir(parents=True)
    protected.write_bytes(b"preserve")
    manifest["targets"][0]["canonical_path"] = str(protected)
    tampered_path = (tmp_path / "attacker-rehashed-manifest.json").resolve()
    tampered_raw = reset._canonical_json(manifest)
    tampered_path.write_bytes(tampered_raw)

    with pytest.raises(reset.ResetSafetyError, match="canonicalisatie"):
        reset.execute_reset_manifest(
            manifest_path=tampered_path,
            manifest_sha256=hashlib.sha256(tampered_raw).hexdigest(),
            confirmation=reset.EXECUTION_CONFIRMATION,
            execution_evidence_output=(tmp_path / "tamper-evidence.json").resolve(),
        )

    assert target.is_dir()
    assert protected.read_bytes() == b"preserve"
    assert Path(str(preview["manifest_path"])).is_file()


def test_rehashed_manifest_cannot_move_authority_below_protected_ancestor(
    tmp_path: Path,
):
    _root, target, preview, manifest = _preview(tmp_path)
    protected_root = tmp_path / ".storage" / "storm_tracker_runtime"
    protected_root.mkdir(parents=True)
    manifest["roots"][0]["canonical_path"] = str(protected_root.resolve())
    tampered_path = (tmp_path / "attacker-protected-root-manifest.json").resolve()
    tampered_raw = reset._canonical_json(manifest)
    tampered_path.write_bytes(tampered_raw)
    evidence_path = (tmp_path / "must-not-exist-evidence.json").resolve()

    with pytest.raises(reset.ResetSafetyError, match="beschermde grens"):
        reset.execute_reset_manifest(
            manifest_path=tampered_path,
            manifest_sha256=hashlib.sha256(tampered_raw).hexdigest(),
            confirmation=reset.EXECUTION_CONFIRMATION,
            execution_evidence_output=evidence_path,
        )

    assert target.is_dir()
    assert not evidence_path.exists()
    assert Path(str(preview["manifest_path"])).is_file()


@pytest.mark.parametrize("relative", (".", "../raw", "raw/*", "raw/[0]"))
def test_relative_target_rejects_root_parent_and_glob_syntax(
    tmp_path: Path,
    relative: str,
):
    root, target = _layout(tmp_path)
    record = _target_record(root, target)
    record["relative_path"] = relative
    config_path = _write_config(tmp_path, _config_document(root, [record]))

    with pytest.raises(reset.ResetSafetyError):
        reset.build_reset_manifest(
            config_path=config_path,
            manifest_output=(tmp_path / "unsafe-manifest.json").resolve(),
        )

    assert target.is_dir()


def test_root_and_output_paths_must_be_explicit_absolute_and_external(
    tmp_path: Path,
):
    root, target = _layout(tmp_path)
    config = _config_document(root, [_target_record(root, target)])
    config["roots"][0]["absolute_path"] = "relative/runtime"
    config_path = _write_config(tmp_path, config)
    with pytest.raises(reset.ResetSafetyError, match="expliciet absoluut"):
        reset.build_reset_manifest(
            config_path=config_path,
            manifest_output=(tmp_path / "manifest.json").resolve(),
        )

    safe_config = _config_document(root, [_target_record(root, target)])
    config_path.write_text(json.dumps(safe_config), encoding="utf-8")
    with pytest.raises(reset.ResetSafetyError, match="buiten iedere"):
        reset.build_reset_manifest(
            config_path=config_path,
            manifest_output=(root / "manifest.json").resolve(),
        )

    assert target.is_dir()


def test_overlapping_targets_are_refused(tmp_path: Path):
    root, target = _layout(tmp_path)
    nested = target / "kmi"
    config = _config_document(root, [
        _target_record(root, target, target_id="all_raw"),
        _target_record(root, nested, target_id="kmi_raw"),
    ])
    config_path = _write_config(tmp_path, config)

    with pytest.raises(reset.ResetSafetyError, match="mogen niet overlappen"):
        reset.build_reset_manifest(
            config_path=config_path,
            manifest_output=(tmp_path / "overlap-manifest.json").resolve(),
        )

    assert target.is_dir()


def test_reparse_or_symlink_entry_is_refused_when_supported(tmp_path: Path):
    root, target = _layout(tmp_path)
    outside = tmp_path / "outside-provider-state"
    outside.mkdir()
    (outside / "keep.bin").write_bytes(b"keep")
    link = target / "linked-outside"
    try:
        os.symlink(outside, link, target_is_directory=True)
    except (NotImplementedError, OSError):
        pytest.skip("deze Windows-user kan geen testsymlink maken")
    config_path = _write_config(
        tmp_path,
        _config_document(root, [_target_record(root, target)]),
    )

    with pytest.raises(reset.ResetSafetyError, match="reparsepunt of link"):
        reset.build_reset_manifest(
            config_path=config_path,
            manifest_output=(tmp_path / "link-manifest.json").resolve(),
        )

    assert (outside / "keep.bin").read_bytes() == b"keep"


def test_duplicate_json_keys_and_unknown_schema_keys_fail_closed(tmp_path: Path):
    root, target = _layout(tmp_path)
    duplicate = (tmp_path / "duplicate-config.json").resolve()
    duplicate.write_text(
        '{"contract":"a","contract":"b"}',
        encoding="utf-8",
    )
    with pytest.raises(reset.ResetSafetyError, match="dubbele JSON-sleutel"):
        reset.build_reset_manifest(
            config_path=duplicate,
            manifest_output=(tmp_path / "duplicate-manifest.json").resolve(),
        )

    config = _config_document(root, [_target_record(root, target)])
    config["unexpected"] = True
    config_path = _write_config(tmp_path, config)
    with pytest.raises(reset.ResetSafetyError, match="exacte sleutelset"):
        reset.build_reset_manifest(
            config_path=config_path,
            manifest_output=(tmp_path / "unknown-manifest.json").resolve(),
        )


def test_cli_defaults_to_preview_and_requires_separate_execute_flag(tmp_path: Path):
    root, target = _layout(tmp_path)
    config_path = _write_config(
        tmp_path,
        _config_document(root, [_target_record(root, target)]),
    )
    manifest_path = (tmp_path / "cli-manifest.json").resolve()
    completed = subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts" / "reset_storm_tracker_runtime_history.py"),
            "--config",
            str(config_path),
            "--manifest-output",
            str(manifest_path),
        ],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    report = json.loads(completed.stdout)

    assert report["destructive_action_performed"] is False
    assert manifest_path.is_file()
    assert target.is_dir()

    refused = subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts" / "reset_storm_tracker_runtime_history.py"),
            "--execute-manifest",
            str(manifest_path),
            "--manifest-sha256",
            report["manifest_sha256"],
            "--confirm",
            reset.EXECUTION_CONFIRMATION,
            "--execution-evidence-output",
            str((tmp_path / "cli-evidence.json").resolve()),
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert refused.returncode == 2
    assert target.is_dir()


def test_source_has_no_glob_or_recursive_delete_shortcut():
    source = (
        ROOT / "scripts" / "reset_storm_tracker_runtime_history.py"
    ).read_text(encoding="utf-8")
    assert ".glob(" not in source
    assert ".rglob(" not in source
    assert "shutil.rmtree" not in source
    assert "os.walk(" not in source
    assert "target.unlink" not in source
    assert "target.rmdir" not in source
    assert "os.scandir(candidate)" in source
    assert "os.unlink(path)" in source
    assert "os.rmdir(path)" in source
