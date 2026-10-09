"""Portable RAW requirements and fail-closed target diagnostic contracts."""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import replace
from functools import lru_cache
import hashlib
import io
import json
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace

import pytest

import descriptor_relative_authority as authority
from storm_tracker_worker import acquisition_evidence_lifecycle
from storm_tracker_worker import raw_capture_activation_artifacts as materializer
from storm_tracker_worker.release_artifacts import (
    build_release_candidate,
    inspect_source_tree,
)
import storm_tracker_worker.raw_capture_release_requirements as subject


ROOT = Path(__file__).resolve().parents[1]
OPTIONS = (
    ROOT
    / "release_inputs"
    / "supervisor_options"
    / subject.SANITIZED_OPTIONS_FILENAME
)
INTEGRATION_VERSION = "1.13.2"
WORKER_VERSION = "1.16.2"
TARGET_BYTES = 40_000_000_000


@pytest.fixture(autouse=True)
def _portable_acquisition_directory_fsync(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        acquisition_evidence_lifecycle.AcquisitionEvidenceLifecycleStore,
        "_default_directory_fsync",
        staticmethod(lambda _path: None),
    )


def _sha(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def _canonical(value: object) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")


def _sources(
    base: Path,
    *,
    integration_version: str = INTEGRATION_VERSION,
    worker_version: str = WORKER_VERSION,
) -> tuple[Path, Path]:
    integration = base / "source" / "storm_tracker_v3"
    worker = base / "worker" / "storm_tracker_worker"
    integration.mkdir(parents=True)
    worker.mkdir(parents=True)
    (integration / "manifest.json").write_text(json.dumps({
        "domain": "storm_tracker_v3",
        "name": "Storm Tracker V3",
        "version": integration_version,
    }), encoding="utf-8")
    (integration / "__init__.py").write_text(
        "STORE = Factory(build_id="
        f"\"integration-{integration_version}-worker-{worker_version}\")\n",
        encoding="utf-8",
    )
    for relative in subject.FROZEN_WORKER_PACKAGE_MEMBER_PATHS:
        destination = worker / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(
            (ROOT / "storm_tracker_worker" / relative).read_bytes()
        )
    (worker / "config.yaml").write_text(
        f'name: "Storm Tracker Worker"\nversion: "{worker_version}"\n',
        encoding="utf-8",
    )
    (worker / "__init__.py").write_text(
        f'__version__ = "{worker_version}"\n',
        encoding="utf-8",
    )
    (worker / "run.sh").write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    return integration, worker


@lru_cache(maxsize=1)
def _pinset() -> subject.RawCapturePortablePinset:
    return subject.derive_frozen_raw_capture_release_pinset(
        materializer_path=ROOT / subject.MATERIALIZER_MEMBER_PATH,
    )


def _build(
    base: Path,
    name: str,
    *,
    integration_version: str = INTEGRATION_VERSION,
    worker_version: str = WORKER_VERSION,
):
    integration, worker = _sources(
        base / name,
        integration_version=integration_version,
        worker_version=worker_version,
    )
    output = base / f"{name}-out"
    output.mkdir()
    core = build_release_candidate(
        integration_root=integration,
        worker_root=worker,
        integration_version=integration_version,
        worker_version=worker_version,
        output_directory=output,
    )
    raw = subject.build_raw_capture_release_requirements(
        release=core,
        pinset=_pinset(),
        sanitized_options_snapshot=OPTIONS,
    )
    return integration, worker, core, raw


class _MemoryBoundSource:
    def __init__(self, bodies: dict[str, bytes]) -> None:
        self._bodies = dict(bodies)

    @property
    def keys(self) -> tuple[str, ...]:
        return tuple(sorted(self._bodies))

    def require_exact_keys(self, expected_keys) -> None:
        if tuple(sorted(expected_keys)) != self.keys:
            raise ValueError("unexpected keys")

    def metadata(self, key: str):
        body = self._bodies[key]
        return SimpleNamespace(size=len(body), sha256=_sha(body))

    @contextmanager
    def open_stream(self, key: str):
        yield io.BytesIO(self._bodies[key])


class _OneShotBoundSource(_MemoryBoundSource):
    def __init__(self, bodies: dict[str, bytes]) -> None:
        super().__init__(bodies)
        self.metadata_calls = {key: 0 for key in bodies}
        self.stream_calls = {key: 0 for key in bodies}

    def metadata(self, key: str):
        self.metadata_calls[key] += 1
        if self.metadata_calls[key] != 1:
            raise AssertionError(f"metadata read more than once: {key}")
        return super().metadata(key)

    @contextmanager
    def open_stream(self, key: str):
        self.stream_calls[key] += 1
        if self.stream_calls[key] != 1:
            raise AssertionError(f"stream opened more than once: {key}")
        with super().open_stream(key) as stream:
            yield stream


def _requirements_bodies(core, raw) -> dict[str, bytes]:
    return {
        "evidence": core.evidence_path.read_bytes(),
        "integration": core.integration_archive.read_bytes(),
        "options": OPTIONS.read_bytes(),
        "requirements": raw.requirements_path.read_bytes(),
        "worker": core.worker_archive.read_bytes(),
    }


def _materialize(base: Path, *, target_bytes: int = TARGET_BYTES):
    persistent = base / "persistent"
    persistent.mkdir(parents=True)
    output = persistent / "activation"
    return materializer.build_raw_capture_shadow_artifacts(
        persistent_base_root=persistent,
        output_directory=output,
        capture_window_hours=subject.FROZEN_CAPTURE_WINDOW_HOURS,
        target_raw_capacity_bytes=target_bytes,
        blitzortung_enabled=False,
        operator_host_operational_free_reserve_bytes=(
            subject.FROZEN_HOST_OPERATIONAL_FREE_RESERVE_BYTES
        ),
        expected_catalog_sha256=subject.FROZEN_CATALOG_SHA256,
        expected_activation_matrix_sha256=(
            subject.FROZEN_ACTIVATION_MATRIX_SHA256
        ),
    )


def _target_bound_bodies(core, raw, built, attestation: Path) -> dict[str, bytes]:
    manifest_body = built.manifest_path.read_bytes()
    manifest = json.loads(manifest_body)
    role_bodies = {
        item["role"]: (built.output_directory / item["path"]).read_bytes()
        for item in manifest["artifacts"]
    }
    return {
        **_requirements_bodies(core, raw),
        "attestation": attestation.read_bytes(),
        "target_authority": role_bodies["worker_first_authority"],
        "target_capacity": role_bodies["capacity_profile"],
        "target_config": role_bodies["raw_capture_config"],
        "target_lock": role_bodies["ownership_lock_anchor"],
        "target_manifest": manifest_body,
    }


def _quartet_names() -> tuple[str, str, str, str]:
    return (
        f"storm_tracker_v3_v{INTEGRATION_VERSION}_deploy.zip",
        f"storm_tracker_worker_v{WORKER_VERSION}_deploy.tar.gz",
        (
            f"storm_tracker_release_v{INTEGRATION_VERSION}_worker_v"
            f"{WORKER_VERSION}_evidence.json"
        ),
        (
            f"storm_tracker_raw_capture_release_v{INTEGRATION_VERSION}"
            f"_worker_v{WORKER_VERSION}_requirements.json"
        ),
    )


def _assert_zero_quartet(output: Path) -> None:
    if output.exists():
        assert list(output.iterdir()) == []
    assert not list(output.parent.glob(".stv3-raw-quartet-*"))


def _build_quartet(integration: Path, worker: Path, output: Path):
    return subject.build_raw_capture_release_quartet(
        integration_root=integration,
        worker_root=worker,
        integration_version=INTEGRATION_VERSION,
        worker_version=WORKER_VERSION,
        output_directory=output,
        pinset=_pinset(),
        sanitized_options_snapshot=OPTIONS,
    )


def test_requirements_are_fourth_deterministic_commit_marker(tmp_path: Path) -> None:
    _i1, _w1, first_core, first = _build(tmp_path, "first")
    _i2, _w2, second_core, second = _build(tmp_path, "second")

    assert first.requirements_path.name == (
        "storm_tracker_raw_capture_release_v1.13.2_worker_v1.16.2_"
        "requirements.json"
    )
    assert first_core.integration_archive.read_bytes() == (
        second_core.integration_archive.read_bytes()
    )
    assert first_core.worker_archive.read_bytes() == (
        second_core.worker_archive.read_bytes()
    )
    assert first_core.evidence_path.read_bytes() == (
        second_core.evidence_path.read_bytes()
    )
    assert first.requirements_path.read_bytes() == second.requirements_path.read_bytes()
    document = subject.verify_raw_capture_release_requirements(
        first.requirements_path,
        sanitized_options_snapshot=OPTIONS,
    )
    assert document["target_materialization_required"] is True
    assert document["target_activation_manifest_sha256"] is None
    assert document["deployment_bundle_complete"] is False


def test_requirements_are_portable_and_bind_complete_worker_membership(
    tmp_path: Path,
) -> None:
    _integration, _worker, _core, raw = _build(tmp_path, "portable")
    body = raw.requirements_path.read_bytes()
    document = json.loads(body)

    assert str(tmp_path).encode() not in body
    assert b"output_directory" not in body
    assert b"persistent_base_root" not in body
    inventory = document["worker_package_inventory"]
    assert inventory["member_count"] == len(
        subject.FROZEN_WORKER_PACKAGE_MEMBER_PATHS
    )
    assert inventory[
        "complete_frozen_worker_package_membership_content_bound"
    ] is True
    assert inventory["runtime_import_closure_verified"] is False
    assert inventory["dynamic_import_executability_proven"] is False
    by_path = {item["member_path"]: item for item in inventory["members"]}
    assert by_path["storm_tracker_worker/service.py"]["sha256"] == (
        subject.FROZEN_WORKER_SERVICE_SHA256
    )
    capacity = document["pinset"]["capacity"]
    assert capacity["derivation_sentinel_target_bytes"] == (1 << 63) - 1
    assert capacity["target_capacity_claimed"] is False
    assert capacity["minimum_required_same_filesystem_bytes"] == 20_594_032_640
    assert document["contract"] == "stv3-raw-capture-release-requirements-v2"
    assert document["target_profile"] == {
        "blitzortung_enabled": False,
        "canonical_absolute_path_selected_at_target": True,
        "contract": "stv3-raw-capture-target-materialization-profile-v2",
        "direct_child_output_required": True,
        "event_capacity_claimed": False,
        "host_identity_bound": False,
        "host_operational_free_reserve_bytes": 2_147_483_648,
        "path_semantics_selected_at_target": True,
        "reparse_or_symlink_allowed": False,
        "same_filesystem_layout_required": True,
        "selected_profile": "cadence_only",
    }
    assert document["materializer_arguments"] == {
        "blitzortung_enabled": False,
        "capture_window_hours": 1,
        "expected_activation_matrix_sha256": (
            subject.FROZEN_ACTIVATION_MATRIX_SHA256
        ),
        "expected_catalog_sha256": subject.FROZEN_CATALOG_SHA256,
        "operator_host_operational_free_reserve_bytes": 2_147_483_648,
    }
    assert document["pinset"]["retention"] == {
        "blitzortung_enabled": False,
        "capacity_policy": (
            "config_bound_optional_event_envelope_reserved_fail_closed_v2"
        ),
        "contract": "stv3-worker-raw-capture-retention-guard-v2",
        "event_capacity_claimed": False,
        "guard_configured_persistent_max_bytes": 17_901_289_472,
        "host_operational_free_reserve_bytes": 2_147_483_648,
        "selected_profile": "cadence_only",
        "service_configured_persistent_max_bytes": 17_632_854_016,
        "source_sha256": subject.FROZEN_RETENTION_GUARD_SHA256,
    }
    for claim in (
        "dashboard_output",
        "deployment_bundle_complete",
        "deployment_performed",
        "live_target_verified",
        "meteorological_admission",
        "meteorological_output",
        "production_enable_ready",
        "target_activation_ready",
        "target_materialization_performed",
    ):
        assert document[claim] is False
    assert document["target_live_verification_required"] is True


def test_frozen_membership_matches_actual_cache_filtered_worker_source(
    tmp_path: Path,
) -> None:
    source = ROOT / "storm_tracker_worker"
    clean = tmp_path / "storm_tracker_worker"
    clean.mkdir()
    for candidate in sorted(source.rglob("*")):
        relative = candidate.relative_to(source)
        if "__pycache__" in relative.parts or candidate.suffix in {".pyc", ".pyo"}:
            continue
        if candidate.is_dir():
            continue
        assert candidate.is_file()
        assert not candidate.is_symlink()
        destination = clean / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(candidate.read_bytes())
    inspected = inspect_source_tree(
        clean,
        expected_root_name="storm_tracker_worker",
    )

    assert tuple(item.relative_path for item in inspected.files) == tuple(
        sorted(subject.FROZEN_WORKER_PACKAGE_MEMBER_PATHS)
    )


def test_bound_requirements_verifier_preserves_v1_three_key_subview(
    tmp_path: Path,
) -> None:
    _integration, _worker, core, raw = _build(tmp_path, "bound")
    source = _OneShotBoundSource(_requirements_bodies(core, raw))
    document = subject.verify_raw_capture_release_requirements_bound_inputs(
        source
    )
    assert document["requirements_sha256"] == raw.requirements_sha256
    assert set(source.metadata_calls.values()) == {1}
    assert set(source.stream_calls.values()) == {1}


def test_requirements_v2_preserves_core_release_evidence_v1(
    tmp_path: Path,
) -> None:
    _integration, _worker, core, raw = _build(tmp_path, "core-v1")

    document = subject.verify_raw_capture_release_requirements_bound_inputs(
        _MemoryBoundSource(_requirements_bodies(core, raw))
    )

    assert document["contract"] == subject.RAW_RELEASE_REQUIREMENTS_CONTRACT
    assert document["release_evidence"]["contract"] == (
        subject.RELEASE_EVIDENCE_CONTRACT
    )


@pytest.mark.parametrize(
    "section,field,value",
    (
        ("target_profile", "selected_profile", "event_enabled"),
        ("target_profile", "event_capacity_claimed", True),
        ("materializer_arguments", "blitzortung_enabled", True),
        (
            "materializer_arguments",
            "operator_host_operational_free_reserve_bytes",
            2_147_483_647,
        ),
        (
            "retention",
            "guard_configured_persistent_max_bytes",
            18_001_952_769,
        ),
    ),
)
def test_requirements_v2_rejects_self_consistent_profile_or_retention_drift(
    tmp_path: Path,
    section: str,
    field: str,
    value: object,
) -> None:
    _integration, _worker, core, raw = _build(tmp_path, f"v2-drift-{field}")
    bodies = _requirements_bodies(core, raw)
    forged = json.loads(bodies["requirements"])
    if section == "retention":
        forged["pinset"][section][field] = value
    else:
        forged[section][field] = value
    unsigned = dict(forged)
    unsigned.pop("requirements_sha256")
    forged["requirements_sha256"] = _sha(_canonical(unsigned))
    bodies["requirements"] = _canonical(forged) + b"\n"

    with pytest.raises(subject.RawCaptureReleaseRequirementsError):
        subject.verify_raw_capture_release_requirements_bound_inputs(
            _MemoryBoundSource(bodies)
        )


def test_requirements_reject_pin_drift_and_missing_runtime_member(
    tmp_path: Path,
) -> None:
    integration, worker = _sources(tmp_path / "missing")
    (worker / "worker_first_runtime.py").unlink()
    output = tmp_path / "missing-out"
    output.mkdir()
    core = build_release_candidate(
        integration_root=integration,
        worker_root=worker,
        integration_version=INTEGRATION_VERSION,
        worker_version=WORKER_VERSION,
        output_directory=output,
    )
    with pytest.raises(
        subject.RawCaptureReleaseRequirementsError,
        match="uniquely bind|package membership",
    ):
        subject.build_raw_capture_release_requirements(
            release=core,
            pinset=_pinset(),
            sanitized_options_snapshot=OPTIONS,
        )
    with pytest.raises(
        subject.RawCaptureReleaseRequirementsError,
        match="frozen RAW release pin differs",
    ):
        subject.build_raw_capture_release_requirements(
            release=core,
            pinset=replace(_pinset(), catalog_sha256="0" * 64),
            sanitized_options_snapshot=OPTIONS,
        )


def test_requirements_reject_snapshot_rename(tmp_path: Path) -> None:
    _integration, _worker, core, _raw = _build(tmp_path, "snapshot")
    renamed = tmp_path / "renamed-options.json"
    renamed.write_bytes(OPTIONS.read_bytes())
    with pytest.raises(
        subject.RawCaptureReleaseRequirementsError,
        match="filename differs",
    ):
        subject.build_raw_capture_release_requirements(
            release=core,
            pinset=_pinset(),
            sanitized_options_snapshot=renamed,
        )


def test_quartet_companion_failure_leaves_no_final_files(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    integration, worker = _sources(tmp_path / "quartet-fail")
    output = tmp_path / "quartet-fail-output"

    def fail_requirements(**_kwargs):
        raise subject.RawCaptureReleaseRequirementsError("injected companion")

    monkeypatch.setattr(subject, "build_raw_capture_release_requirements", fail_requirements)
    with pytest.raises(subject.RawCaptureReleaseRequirementsError):
        _build_quartet(integration, worker, output)
    assert not output.exists()
    _assert_zero_quartet(output)


def test_quartet_creates_absent_exact_output_child(tmp_path: Path) -> None:
    integration, worker = _sources(tmp_path / "quartet-create")
    output = tmp_path / "quartet-create-output"

    result = _build_quartet(integration, worker, output)

    assert output.is_dir()
    assert tuple(sorted(path.name for path in output.iterdir())) == tuple(
        sorted(_quartet_names())
    )
    assert result.requirements.requirements_path.parent == output


@pytest.mark.parametrize("failure_index", range(4))
def test_quartet_publish_failure_rolls_back_all_exact_owned_finals(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure_index: int,
) -> None:
    integration, worker = _sources(tmp_path / f"quartet-publish-{failure_index}")
    output = tmp_path / f"quartet-publish-{failure_index}-output"
    output.mkdir()
    original = authority.DirectoryAuthority.publish_noreplace
    target = f"{output.name}/{_quartet_names()[failure_index]}"

    def fail_selected(self, source, destination, *, expected_source=None):
        if str(destination).replace("\\", "/") == target:
            raise authority.AuthorityIOError("injected prepublication failure")
        return original(
            self,
            source,
            destination,
            expected_source=expected_source,
        )

    monkeypatch.setattr(
        authority.DirectoryAuthority,
        "publish_noreplace",
        fail_selected,
    )
    with pytest.raises(subject.RawCaptureReleaseRequirementsError):
        _build_quartet(integration, worker, output)
    _assert_zero_quartet(output)


@pytest.mark.parametrize("failure_index", range(4))
def test_quartet_postpublication_failure_rolls_back_all_finals(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure_index: int,
) -> None:
    integration, worker = _sources(tmp_path / f"quartet-postlink-{failure_index}")
    output = tmp_path / f"quartet-postlink-{failure_index}-output"
    output.mkdir()
    original = authority.DirectoryAuthority.publish_noreplace
    target = f"{output.name}/{_quartet_names()[failure_index]}"

    def fail_after_link(self, source, destination, *, expected_source=None):
        backend = original(
            self,
            source,
            destination,
            expected_source=expected_source,
        )
        if str(destination).replace("\\", "/") == target:
            raise authority.AuthorityPostPublicationError(
                "injected postpublication failure",
                backend=backend,
                published_identity=self.entry_identity(destination),
                source=authority.RelativePath.parse(source),
                destination=authority.RelativePath.parse(destination),
            )
        return backend

    monkeypatch.setattr(
        authority.DirectoryAuthority,
        "publish_noreplace",
        fail_after_link,
    )
    with pytest.raises(subject.RawCaptureReleaseRequirementsError):
        _build_quartet(integration, worker, output)
    _assert_zero_quartet(output)


@pytest.mark.parametrize("failure_index", range(4))
def test_quartet_postread_failure_rolls_back_all_finals(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure_index: int,
) -> None:
    integration, worker = _sources(tmp_path / f"quartet-postread-{failure_index}")
    output = tmp_path / f"quartet-postread-{failure_index}-output"
    output.mkdir()
    original = authority.DirectoryAuthority.read_file
    target = f"{output.name}/{_quartet_names()[failure_index]}"

    def wrong_selected(self, relative, *, maximum_bytes):
        if str(relative).replace("\\", "/") == target:
            return b"wrong"
        return original(self, relative, maximum_bytes=maximum_bytes)

    monkeypatch.setattr(
        authority.DirectoryAuthority,
        "read_file",
        wrong_selected,
    )
    with pytest.raises(subject.RawCaptureReleaseRequirementsError):
        _build_quartet(integration, worker, output)
    _assert_zero_quartet(output)


@pytest.mark.parametrize("scope", ("output", "parent"))
def test_quartet_fsync_failure_rolls_back_all_finals(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    scope: str,
) -> None:
    integration, worker = _sources(tmp_path / f"quartet-fsync-{scope}")
    output = tmp_path / f"quartet-fsync-{scope}-output"
    output.mkdir()
    with authority.DirectoryAuthority.open_absolute(output) as pinned_output:
        output_identity = pinned_output.identity()
    with authority.DirectoryAuthority.open_absolute(output.parent) as pinned_parent:
        parent_identity = pinned_parent.identity()
    selected = output_identity if scope == "output" else parent_identity
    original = authority.DirectoryAuthority.fsync
    failed = False

    def fail_selected(self):
        nonlocal failed
        if not failed and self.identity().same_object(selected):
            failed = True
            raise authority.AuthorityIOError("injected quartet fsync")
        return original(self)

    monkeypatch.setattr(authority.DirectoryAuthority, "fsync", fail_selected)
    with pytest.raises(subject.RawCaptureReleaseRequirementsError):
        _build_quartet(integration, worker, output)
    assert failed is True
    _assert_zero_quartet(output)


@pytest.mark.parametrize("preexisting_output", (False, True))
def test_quartet_root_authority_close_failure_is_transactional(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    preexisting_output: bool,
) -> None:
    label = "existing" if preexisting_output else "absent"
    integration, worker = _sources(tmp_path / f"quartet-close-{label}")
    output = tmp_path / f"quartet-close-{label}-output"
    if preexisting_output:
        output.mkdir()
    with authority.DirectoryAuthority.open_absolute(output.parent) as pinned_parent:
        parent_identity = pinned_parent.identity()
    original = authority.DirectoryAuthority.close
    injected = False
    parent_close_calls = 0

    def fail_committing_root_close(self):
        nonlocal injected, parent_close_calls
        is_parent = (
            not self.closed
            and self.identity().same_object(parent_identity)
        )
        if is_parent:
            parent_close_calls += 1
        if (
            is_parent
            and not injected
            and all((output / name).exists() for name in _quartet_names())
        ):
            injected = True
            original(self)
            raise authority.AuthorityIOError("injected root close failure")
        return original(self)

    monkeypatch.setattr(
        authority.DirectoryAuthority,
        "close",
        fail_committing_root_close,
    )
    with pytest.raises(subject.RawCaptureReleaseRequirementsError):
        _build_quartet(integration, worker, output)

    assert injected is True
    assert parent_close_calls >= 2
    if preexisting_output:
        assert output.is_dir()
    else:
        assert not output.exists()
    _assert_zero_quartet(output)


def test_quartet_foreign_swap_is_never_removed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    integration, worker = _sources(tmp_path / "quartet-foreign-swap")
    output = tmp_path / "quartet-foreign-swap-output"
    output.mkdir()
    target_name = _quartet_names()[1]
    target_relative = f"{output.name}/{target_name}"
    original = authority.DirectoryAuthority.read_file
    swapped = False

    def swap_selected(self, relative, *, maximum_bytes):
        nonlocal swapped
        if not swapped and str(relative).replace("\\", "/") == target_relative:
            swapped = True
            destination = output / target_name
            destination.unlink()
            destination.write_bytes(b"foreign")
            raise authority.AuthorityIOError("injected foreign swap")
        return original(self, relative, maximum_bytes=maximum_bytes)

    monkeypatch.setattr(
        authority.DirectoryAuthority,
        "read_file",
        swap_selected,
    )
    with pytest.raises(
        subject.RawCaptureReleaseRequirementsError,
        match="rollback was incomplete",
    ):
        _build_quartet(integration, worker, output)
    assert swapped is True
    assert (output / target_name).read_bytes() == b"foreign"
    assert tuple(path.name for path in output.iterdir()) == (target_name,)
    assert not list(output.parent.glob(".stv3-raw-quartet-*"))


def test_marker_publication_rolls_back_postread_and_preserves_racing_destination(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    destination = tmp_path / "marker.json"
    original_read = authority.DirectoryAuthority.read_file

    def wrong_final_read(self, relative, *, maximum_bytes):
        if str(relative) == destination.name:
            return b"wrong"
        return original_read(self, relative, maximum_bytes=maximum_bytes)

    monkeypatch.setattr(authority.DirectoryAuthority, "read_file", wrong_final_read)
    with pytest.raises(subject.RawCaptureReleaseRequirementsError):
        subject._publish_canonical_document(destination, {"value": 1})
    assert not destination.exists()

    monkeypatch.setattr(authority.DirectoryAuthority, "read_file", original_read)
    original_publish = authority.DirectoryAuthority.publish_noreplace

    def race_destination(self, source, target, *, expected_source=None):
        with self.create_file(target) as attacker:
            attacker.write_all(b"attacker")
            attacker.flush()
        return original_publish(
            self,
            source,
            target,
            expected_source=expected_source,
        )

    monkeypatch.setattr(
        authority.DirectoryAuthority,
        "publish_noreplace",
        race_destination,
    )
    with pytest.raises(subject.RawCaptureReleaseRequirementsError):
        subject._publish_canonical_document(destination, {"value": 2})
    assert destination.read_bytes() == b"attacker"


def test_descriptor_read_detects_parent_reanchor_change(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = tmp_path / "payload"
    target.write_bytes(b"payload")
    original = authority.DirectoryAuthority.reanchor
    calls = 0

    def fail_second(self, path=None):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise authority.AuthorityViolation("injected parent swap")
        return original(self, path)

    monkeypatch.setattr(authority.DirectoryAuthority, "reanchor", fail_second)
    with pytest.raises(
        subject.RawCaptureReleaseRequirementsError,
        match="cannot be read safely",
    ):
        subject._read_file(target, maximum_bytes=32, label="race seam")


def test_descriptor_read_detects_same_identity_metadata_change(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = tmp_path / "same-size-payload"
    target.write_bytes(b"payload")
    target_identity = target.stat()
    original = subject._stat_witness
    observations = 0

    def drift(metadata):
        nonlocal observations
        witness = original(metadata)
        if (
            metadata.st_dev == target_identity.st_dev
            and metadata.st_ino == target_identity.st_ino
        ):
            observations += 1
            if observations == 2:
                return (*witness[:-1], witness[-1] + 1)
        return witness

    monkeypatch.setattr(subject, "_stat_witness", drift)
    with pytest.raises(
        subject.RawCaptureReleaseRequirementsError,
        match="changed while read",
    ):
        subject._read_file(target, maximum_bytes=32, label="same-size race seam")


def test_real_target_attestation_build_and_live_verify(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _integration, _worker, _core, raw = _build(tmp_path, "live")
    built = _materialize(tmp_path / "target")
    evidence = tmp_path / "retained-evidence"
    evidence.mkdir()
    monkeypatch.setattr(subject, "_observe_available_bytes", lambda _path: TARGET_BYTES)

    attestation = subject.build_raw_capture_target_activation_attestation(
        requirements_path=raw.requirements_path,
        sanitized_options_snapshot=OPTIONS,
        target_activation_manifest=built.manifest_path,
        output_directory=evidence,
    )
    verified = subject.verify_raw_capture_target_activation_attestation(
        attestation.attestation_path,
        requirements_path=raw.requirements_path,
        sanitized_options_snapshot=OPTIONS,
        target_activation_manifest=built.manifest_path,
    )

    assert verified["deployment_bundle_complete"] is False
    assert verified["target_live_verification_required"] is True
    assert verified["trusted_target_attestation_unavailable"] is True
    assert verified["capacity_proof"]["requirement_satisfied"] is False
    assert "observed_available_bytes" not in verified["capacity_proof"]
    proof = verified["filesystem_proof"]
    assert proof["same_filesystem_layout_verified"] is False
    assert proof["reparse_or_symlink_absent"] is False
    assert proof["target_artifacts_reverified"] is False
    assert proof["diagnostic_observation_only"] is True
    assert proof["evidence_output_directory"] == str(evidence)
    assert raw.requirements_file_sha256 in attestation.attestation_path.name
    assert built.manifest_sha256 in attestation.attestation_path.name


def test_target_attestation_filename_binds_each_full_release_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _i1, _w1, _core1, raw1 = _build(
        tmp_path,
        "identity-a",
        integration_version="1.13.2",
    )
    _i2, _w2, _core2, raw2 = _build(
        tmp_path,
        "identity-b",
        integration_version="1.13.3",
    )
    built = _materialize(tmp_path / "identity-target")
    evidence = tmp_path / "identity-evidence"
    evidence.mkdir()
    monkeypatch.setattr(subject, "_observe_available_bytes", lambda _path: TARGET_BYTES)

    first = subject.build_raw_capture_target_activation_attestation(
        requirements_path=raw1.requirements_path,
        sanitized_options_snapshot=OPTIONS,
        target_activation_manifest=built.manifest_path,
        output_directory=evidence,
    )
    second = subject.build_raw_capture_target_activation_attestation(
        requirements_path=raw2.requirements_path,
        sanitized_options_snapshot=OPTIONS,
        target_activation_manifest=built.manifest_path,
        output_directory=evidence,
    )

    assert first.attestation_path != second.attestation_path
    assert raw1.requirements_file_sha256 in first.attestation_path.name
    assert raw2.requirements_file_sha256 in second.attestation_path.name
    assert built.manifest_sha256 in first.attestation_path.name
    assert built.manifest_sha256 in second.attestation_path.name


def test_target_attestation_fsync_failure_removes_exact_owned_marker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _integration, _worker, _core, raw = _build(tmp_path, "target-fsync")
    built = _materialize(tmp_path / "target-fsync-materialized")
    evidence = tmp_path / "target-fsync-evidence"
    evidence.mkdir()
    monkeypatch.setattr(subject, "_observe_available_bytes", lambda _path: TARGET_BYTES)

    def fail_fsync(_self) -> None:
        raise authority.AuthorityIOError("injected target attestation fsync")

    monkeypatch.setattr(authority.DirectoryAuthority, "fsync", fail_fsync)
    with pytest.raises(subject.RawCaptureReleaseRequirementsError):
        subject.build_raw_capture_target_activation_attestation(
            requirements_path=raw.requirements_path,
            sanitized_options_snapshot=OPTIONS,
            target_activation_manifest=built.manifest_path,
            output_directory=evidence,
        )
    assert list(evidence.iterdir()) == []


def test_target_attestation_rejects_overlap_and_cross_device(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _integration, _worker, _core, raw = _build(tmp_path, "live-negative")
    built = _materialize(tmp_path / "target-negative")
    monkeypatch.setattr(subject, "_observe_available_bytes", lambda _path: TARGET_BYTES)

    with pytest.raises(
        subject.RawCaptureReleaseRequirementsError,
        match="overlaps runtime deletion roots",
    ):
        subject.build_raw_capture_target_activation_attestation(
            requirements_path=raw.requirements_path,
            sanitized_options_snapshot=OPTIONS,
            target_activation_manifest=built.manifest_path,
            output_directory=built.output_directory / "artifacts",
        )

    evidence = tmp_path / "external-evidence"
    evidence.mkdir()
    real_device = subject._observe_filesystem_device

    def split_device(path: Path) -> int:
        value = real_device(path)
        return value + 1 if path.name == "cadence-artifacts" else value

    monkeypatch.setattr(subject, "_observe_filesystem_device", split_device)
    with pytest.raises(
        subject.RawCaptureReleaseRequirementsError,
        match="one filesystem device",
    ):
        subject.build_raw_capture_target_activation_attestation(
            requirements_path=raw.requirements_path,
            sanitized_options_snapshot=OPTIONS,
            target_activation_manifest=built.manifest_path,
            output_directory=evidence,
        )


def test_target_attestation_rejects_runtime_reparse(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _integration, _worker, _core, raw = _build(tmp_path, "target-path-negative")
    built = _materialize(tmp_path / "target-path-negative-materialized")
    evidence = tmp_path / "target-path-negative-evidence"
    evidence.mkdir()
    monkeypatch.setattr(subject, "_observe_available_bytes", lambda _path: TARGET_BYTES)

    runtime = built.output_directory / "state" / "raw" / "cadence-artifacts"
    original = runtime.with_name("cadence-artifacts-original")
    runtime.rename(original)
    try:
        runtime.symlink_to(original, target_is_directory=True)
    except OSError as error:
        original.rename(runtime)
        pytest.skip(f"directory symlink unavailable: {error}")
    with pytest.raises(subject.RawCaptureReleaseRequirementsError):
        subject.build_raw_capture_target_activation_attestation(
            requirements_path=raw.requirements_path,
            sanitized_options_snapshot=OPTIONS,
            target_activation_manifest=built.manifest_path,
            output_directory=evidence,
        )


def test_target_attestation_rejects_capacity_derivation_sentinel(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _integration, _worker, _core, raw = _build(tmp_path, "target-sentinel")
    evidence = tmp_path / "target-sentinel-evidence"
    evidence.mkdir()
    monkeypatch.setattr(subject, "_observe_available_bytes", lambda _path: TARGET_BYTES)
    sentinel = _materialize(
        tmp_path / "target-sentinel-materialized",
        target_bytes=subject.MAX_SIGNED_INT64,
    )
    with pytest.raises(
        subject.RawCaptureReleaseRequirementsError,
        match="target declared capacity",
    ):
        subject.build_raw_capture_target_activation_attestation(
            requirements_path=raw.requirements_path,
            sanitized_options_snapshot=OPTIONS,
            target_activation_manifest=sentinel.manifest_path,
            output_directory=evidence,
        )


def test_forged_self_consistent_bytes_composite_never_claims_live_complete(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _integration, _worker, core, raw = _build(tmp_path, "offline")
    built = _materialize(tmp_path / "offline-target")
    evidence = tmp_path / "offline-evidence"
    evidence.mkdir()
    monkeypatch.setattr(subject, "_observe_available_bytes", lambda _path: TARGET_BYTES)
    attestation = subject.build_raw_capture_target_activation_attestation(
        requirements_path=raw.requirements_path,
        sanitized_options_snapshot=OPTIONS,
        target_activation_manifest=built.manifest_path,
        output_directory=evidence,
    )
    forged = json.loads(attestation.attestation_path.read_bytes())
    forged["filesystem_proof"]["diagnostic_filesystem_device"] += 1
    unsigned = dict(forged)
    unsigned.pop("attestation_sha256")
    forged["attestation_sha256"] = _sha(_canonical(unsigned))
    bodies = _target_bound_bodies(core, raw, built, attestation.attestation_path)
    bodies["attestation"] = _canonical(forged) + b"\n"

    source = _OneShotBoundSource(bodies)
    result = subject.verify_raw_capture_target_activation_composite_bound_inputs(
        source
    )

    assert result["target_attestation_bytes_bound"] is True
    assert result["deployment_bundle_complete"] is False
    assert result["target_live_verification_required"] is True
    assert result["trusted_target_attestation_unavailable"] is True
    assert result["attestation"]["deployment_bundle_complete"] is False
    assert result["attestation"]["capacity_proof"][
        "requirement_satisfied"
    ] is False
    assert set(source.metadata_calls.values()) == {1}
    assert set(source.stream_calls.values()) == {1}


def test_cli_raw_mode_publishes_fourth_marker_and_default_v1_stays_three(
    tmp_path: Path,
) -> None:
    integration, worker = _sources(tmp_path / "cli")
    default_output = tmp_path / "default-output"
    raw_output = tmp_path / "raw-output"
    default_output.mkdir()
    base = [
        sys.executable,
        "-I",
        "-B",
        str(ROOT / "scripts" / "build_release_candidate.py"),
        "--integration-root", str(integration),
        "--worker-root", str(worker),
        "--integration-version", INTEGRATION_VERSION,
        "--worker-version", WORKER_VERSION,
    ]
    default = subprocess.run(
        [*base, "--output-directory", str(default_output)],
        check=True,
        capture_output=True,
        text=True,
    )
    raw_process = subprocess.run(
        [
            *base,
            "--output-directory", str(raw_output),
            "--raw-capture-requirements",
            "--sanitized-options-snapshot", str(OPTIONS),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    default_report = json.loads(default.stdout)
    raw_report = json.loads(raw_process.stdout)

    assert set(default_report) == {
        "deployment_performed",
        "evidence",
        "evidence_sha256",
        "integration_archive",
        "integration_sha256",
        "worker_archive",
        "worker_sha256",
    }
    assert len(list(default_output.iterdir())) == 3
    assert len(list(raw_output.iterdir())) == 4
    assert raw_report["target_materialization_required"] is True
    assert raw_report["deployment_bundle_complete"] is False
    assert raw_report["raw_capture_requirements_contract"] == (
        "stv3-raw-capture-release-requirements-v2"
    )
    assert raw_report["selected_target_profile"] == "cadence_only"
    for claim in (
        "dashboard_output",
        "deployment_performed",
        "live_target_verified",
        "meteorological_admission",
        "meteorological_output",
        "production_enable_ready",
        "target_activation_ready",
    ):
        assert raw_report[claim] is False
    assert (default_output / Path(default_report["integration_archive"]).name).read_bytes() == (
        raw_output / Path(raw_report["integration_archive"]).name
    ).read_bytes()
    assert (default_output / Path(default_report["worker_archive"]).name).read_bytes() == (
        raw_output / Path(raw_report["worker_archive"]).name
    ).read_bytes()
    assert (default_output / Path(default_report["evidence"]).name).read_bytes() == (
        raw_output / Path(raw_report["evidence"]).name
    ).read_bytes()
