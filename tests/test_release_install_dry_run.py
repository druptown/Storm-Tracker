"""Strictly local release materialization, switch and rollback proofs."""
from __future__ import annotations

import ast
import copy
from contextlib import contextmanager
import errno
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import re
import shutil
import stat
import subprocess
import sys
import tarfile
from types import SimpleNamespace
import zipfile

import pytest

import release_install_dry_run as dry_run_module


def test_phase_b_authority_context_keeps_acceptance_blockers_open(tmp_path):
    run_root = tmp_path / "phase-a-run"
    lease = dry_run_module._acquire_process_lease(run_root)
    context = None
    try:
        context = dry_run_module._open_authority_run_context(
            run_root,
            existing=False,
            process_lease=lease,
        )
        evidence = context.evidence()
        assert context.lexical_root == run_root
        assert context.reprove().same_object(context.root_identity)
        assert evidence["descriptor_relative_mutations_phase_a"] is True
        assert evidence["recovery_reads_descriptor_relative"] is True
        assert evidence["recovery_inventory_descriptor_relative"] is True
        assert evidence["descriptor_relative_owned_reads_phase_b"] is True
        assert evidence["bound_archive_processing_phase_b"] is False
        assert evidence["bound_archive_route_count"] == 0
        assert evidence["bound_archive_routes"] == []
        assert evidence["simulator_integration_proven"] is False
        assert evidence["local_acceptance_blockers_closed"] == []
        assert evidence["local_acceptance_blockers"] == list(
            dry_run_module.LOCAL_ACCEPTANCE_BLOCKERS
        )
        assert evidence["operation_kind"] == "execute"
        assert evidence["bound_archive_routes_expected"] == sorted(
            dry_run_module._EXECUTE_BOUND_ARCHIVE_ROUTES
        )
        assert evidence["bound_archive_routes_satisfied"] is False
        atomic = evidence["atomic_capability"]
        assert atomic["simulator_integration_proven"] is evidence[
            "simulator_integration_proven"
        ]
        assert atomic["local_acceptance_blockers_closed"] == evidence[
            "local_acceptance_blockers_closed"
        ]
        assert atomic["allowed_root_bootstrap_proven"] is evidence[
            "allowed_root_bootstrap_proven"
        ]
        assert atomic["authority_root_identity_pinned"] is True
        assert dry_run_module.LOCAL_ACCEPTANCE_BLOCKERS == (
            "ancestor_and_final_output_path_race_exclusion_not_proven",
            "descriptor_relative_output_authority_not_proven",
        )
    finally:
        if context is not None:
            context.close()
        lease.release()


def test_phase_a_write_and_generation_publish_are_descriptor_relative(tmp_path):
    run_root = tmp_path / "phase-a-publish"
    lease = dry_run_module._acquire_process_lease(run_root)
    context = None
    try:
        context = dry_run_module._open_authority_run_context(
            run_root,
            existing=False,
            process_lease=lease,
        )
        dry_run_module._mkdir(run_root / "journal", run_root, context)
        dry_run_module._write_exclusive(
            run_root / "journal" / "pending.json",
            b"{}\n",
            mode=0o644,
            run_root=run_root,
            authority_context=context,
        )
        assert context.output.read(
            dry_run_module.RelativePath.parse("journal/pending.json"),
            maximum_bytes=16,
        ) == b"{}\n"

        dry_run_module._mkdir(run_root / "stage", run_root, context)
        dry_run_module._mkdir(run_root / "install", run_root, context)
        dry_run_module._mkdir(
            run_root / "stage" / "generation",
            run_root,
            context,
        )
        dry_run_module._rename(
            run_root / "stage" / "generation",
            run_root / "install" / "generation",
            run_root,
            authority_context=context,
        )
        assert context.root.entry_identity(
            dry_run_module.RelativePath.parse("install/generation")
        ).kind == "directory"
        assert context.atomic.generation_directory_publications == 1
    finally:
        if context is not None:
            context.close()
        lease.release()


def test_phase_a_atomic_postproof_failure_classifies_persistent_generation(
    tmp_path,
    monkeypatch,
):
    run_root = tmp_path / "phase-a-postproof"
    lease = dry_run_module._acquire_process_lease(run_root)
    context = None
    try:
        context = dry_run_module._open_authority_run_context(
            run_root,
            existing=False,
            process_lease=lease,
        )
        dry_run_module._mkdir(run_root / "stage", run_root, context)
        dry_run_module._mkdir(run_root / "install", run_root, context)
        dry_run_module._mkdir(
            run_root / "stage" / "generation",
            run_root,
            context,
        )

        def publish_then_fail(
            adapter,
            source,
            destination,
            *,
            expected_source,
            kind,
        ):
            assert adapter is context.atomic
            adapter.authority.publish_noreplace(
                source,
                destination,
                expected_source=expected_source,
            )
            raise dry_run_module.AtomicNoReplaceError("injected postproof failure")

        monkeypatch.setattr(
            dry_run_module.DescriptorRelativeAtomicAdapter,
            "publish",
            publish_then_fail,
        )
        with pytest.raises(DryRunError, match="published but postproof failed"):
            dry_run_module._rename(
                run_root / "stage" / "generation",
                run_root / "install" / "generation",
                run_root,
                authority_context=context,
            )

        assert not context.root.exists(
            dry_run_module.RelativePath.parse("stage/generation")
        )
        assert context.root.entry_identity(
            dry_run_module.RelativePath.parse("install/generation")
        ).kind == "directory"
        assert context.atomic.generation_directory_publications == 0
    finally:
        if context is not None:
            context.close()
        lease.release()


def test_phase_a_source_contains_no_lexical_output_open_after_context():
    source = Path(dry_run_module.__file__).read_text(encoding="utf-8")
    boundaries = (
        ("def _mkdir(", "def _mkdir_parents("),
        ("def _write_exclusive(", "def _write_member_exclusive("),
        ("def _write_member_exclusive(", "def _write_json_exclusive("),
        ("def _append_journal(", "def _event_names("),
        ("def _rename(", "def _raise_classified_atomic_failure("),
    )
    zones = tuple(
        source[source.index(start):source.index(end)]
        for start, end in boundaries
    )
    for zone in zones:
        assert "os.open(" not in zone
        assert "tempfile.TemporaryFile" not in zone
    assert ".mkdir(" not in zones[0].replace(
        "authority_context.output.mkdir(",
        "",
    )
    assert "write_bytes_noreplace(" in zones[1]
    assert "stream_noreplace(" in zones[2]
    assert "authority_context.atomic.publish(" in zones[3]
    assert "authority_context.atomic.publish(" in zones[4]


def test_phase_b_runtime_has_no_legacy_path_archive_routes():
    source = Path(dry_run_module.__file__).read_text(encoding="utf-8")
    module = ast.parse(source)
    definitions = {
        node.name for node in ast.walk(module) if isinstance(node, ast.FunctionDef)
    }
    assert definitions.isdisjoint({
        "_strict_candidate_zip",
        "_strict_candidate_tar",
        "_legacy_zip_tree",
        "_legacy_tar_tree",
        "_extract_zip",
        "_extract_tar",
    })
    assert "validated_zip_file" not in source
    assert "validated_tar_gz_file" not in source
    assert "zipfile.ZipFile" not in source
    assert "tarfile.open" not in source

    lexical_helpers = {
        "_candidate_archive_names_from_spooled_evidence",
        "_load_options_snapshot",
        "_sync_tree",
        "_fingerprint_tree",
    }
    runtime_roots = {
        "execute_release_install_rollback_dry_run",
        "_execute_release_install_rollback_dry_run_under_lease",
        "_execute_release_install_rollback_with_authority",
        "recover_release_install_dry_run",
        "_recover_release_install_dry_run_under_lease",
        "_upgrade_then_rollback",
        "_restore_baseline",
        "_restore_component_baseline",
        "_assert_candidate_active",
        "_assert_generation_inventory",
        "_assert_recovery_phase_layout",
        "_integration_identity",
        "_worker_identity",
    }
    functions = {
        node.name: node for node in module.body if isinstance(node, ast.FunctionDef)
    }
    for name in runtime_roots:
        calls = {
            node.func.id
            for node in ast.walk(functions[name])
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
        }
        assert calls.isdisjoint(lexical_helpers)


def test_phase_b_tree_converters_reject_forged_and_reordered_seals():
    records = (
        dry_run_module.FileRecord("a-b", 1, "0" * 64, 0o644),
        dry_run_module.FileRecord("a/x", 1, "1" * 64, 0o644),
    )
    sealed = dry_run_module.TreeRecord.create("root", records)
    forged_order = dry_run_module.TreeRecord(
        sealed.root_name,
        tuple(reversed(sealed.files)),
        sealed.directories,
        sealed.total_bytes,
        sealed.content_sha256,
    )
    forged_size = dry_run_module.TreeRecord(
        sealed.root_name,
        sealed.files,
        sealed.directories,
        sealed.total_bytes + 1,
        sealed.content_sha256,
    )
    forged_type = dry_run_module.TreeRecord(
        sealed.root_name,
        (object(),),
        sealed.directories,
        sealed.total_bytes,
        sealed.content_sha256,
    )
    for forged in (forged_order, forged_size, forged_type):
        with pytest.raises(DryRunError, match="seal conversion"):
            dry_run_module._tree_expectation(forged)
        with pytest.raises(DryRunError, match="seal conversion"):
            dry_run_module._bound_archive_tree(forged)


def test_phase_b_identity_size_cap_fails_before_owned_read():
    calls = []

    class Reads:
        def read_bounded(self, *_args, **_kwargs):
            calls.append("read")
            raise AssertionError("oversized identity reached the reader")

    context = SimpleNamespace(
        reads=Reads(),
        relative=lambda value: dry_run_module.RelativePath((Path(value).name,)),
    )
    record = dry_run_module.FileRecord(
        "manifest.json",
        dry_run_module.MAX_INPUT_METADATA_BYTES + 1,
        "0" * 64,
        0o644,
    )
    tree = dry_run_module.TreeRecord(
        "root",
        (record,),
        (),
        record.size,
        "0" * 64,
    )
    with pytest.raises(DryRunError, match="identity byte bound"):
        dry_run_module._read_identity_bytes(
            Path("root"),
            tree,
            "manifest.json",
            "manifest",
            context,
        )
    assert calls == []


def _phase_b_bound_archive_fixture(run_root, context):
    spool = run_root / "spool" / "archive.bin"
    value = b"sealed archive bytes"
    dry_run_module._write_exclusive(
        spool,
        value,
        mode=0o400,
        run_root=run_root,
        authority_context=context,
    )
    identity = context.root.entry_identity(context.relative(spool))
    witness = dry_run_module.FileWitness(
        path=spool,
        device=identity.volume,
        inode=int(identity.file_id, 16),
        size=len(value),
        modified_ns=0,
        sha256=hashlib.sha256(value).hexdigest(),
    )
    registry = dry_run_module._bound_output_archive(
        context, "cleanup-fixture", witness
    )
    bodies = {"one.txt": b"one", "two.txt": b"two"}
    tree = dry_run_module.TreeRecord.create(
        "root",
        tuple(
            dry_run_module.FileRecord(
                name,
                len(body),
                hashlib.sha256(body).hexdigest(),
                0o644,
            )
            for name, body in bodies.items()
        ),
    )
    return registry, tree, bodies


def test_phase_b_bound_archive_rejects_same_bytes_replacement_before_seal(
    tmp_path,
):
    with _authority_run_root(tmp_path / "archive-witness-swap") as (
        run_root,
        context,
    ):
        spool = run_root / "archive.bin"
        value = b"same bytes"
        dry_run_module._write_exclusive(
            spool,
            value,
            mode=0o400,
            run_root=run_root,
            authority_context=context,
        )
        relative = context.relative(spool)
        original = context.root.entry_identity(relative)
        witness = dry_run_module.FileWitness(
            path=spool,
            device=original.volume,
            inode=int(original.file_id, 16),
            size=len(value),
            modified_ns=0,
            sha256=hashlib.sha256(value).hexdigest(),
        )
        context.root.remove_owned_tree(
            relative,
            expected_root=original,
            maximum_entries=1,
        )
        dry_run_module._write_exclusive(
            spool,
            value,
            mode=0o400,
            run_root=run_root,
            authority_context=context,
        )
        with pytest.raises(DryRunError, match="identity differs from its witness"):
            dry_run_module._bound_output_archive(
                context, "same-bytes-replacement", witness
            )
        assert "same-bytes-replacement" not in context.bound_outputs


def test_phase_b_worker_compose_identity_is_bound_to_tree_hash(
    tmp_path,
):
    with _authority_run_root(tmp_path / "worker-compose-seal") as (
        run_root,
        context,
    ):
        root = run_root / "storm_tracker_worker"
        dry_run_module._mkdir(root, run_root, context)
        files = {
            "config.yaml": _config("1.16.0", candidate=True).encode("utf-8"),
            "__init__.py": b'__version__ = "1.16.0"\n',
            "compose.yaml": b"services:\n  worker:\n    image: one\n",
            "run.sh": b"#!/bin/sh\n",
            "service.py": b"VALUE = 1\n",
        }
        records = []
        for name, value in files.items():
            mode = 0o755 if name.endswith(".sh") else 0o644
            dry_run_module._write_exclusive(
                root / name,
                value,
                mode=mode,
                run_root=run_root,
                authority_context=context,
            )
            records.append(dry_run_module.FileRecord(
                name,
                len(value),
                hashlib.sha256(value).hexdigest(),
                mode,
            ))
        tree = dry_run_module.TreeRecord.create(
            "storm_tracker_worker", records
        )
        replacement = files["compose.yaml"].replace(b"one", b"two")
        assert len(replacement) == len(files["compose.yaml"])
        (root / "compose.yaml").write_bytes(replacement)
        with pytest.raises(DryRunError, match="compose.yaml could not be read"):
            dry_run_module._worker_identity(
                root,
                "1.16.0",
                tree=tree,
                authority_context=context,
            )


def test_phase_b_extract_failure_removes_identity_owned_partial_target(
    tmp_path,
    monkeypatch,
):
    with _authority_run_root(tmp_path / "extract-cleanup") as (run_root, context):
        registry, tree, bodies = _phase_b_bound_archive_fixture(run_root, context)

        def fail_on_second(_source, _key, bound_tree, sink, **_kwargs):
            first, second = bound_tree.files
            sink(first, io.BytesIO(bodies[first.path]))
            assert (run_root / "target" / first.path).is_file()
            raise dry_run_module.BoundArchiveError(
                f"second member failed: {second.path}"
            )

        monkeypatch.setattr(
            dry_run_module, "extract_zip_to_sink", fail_on_second
        )
        with pytest.raises(DryRunError, match="extraction failed closed"):
            dry_run_module._extract_bound_archive(
                registry,
                tree,
                run_root / "target",
                run_root,
                context,
                archive_format="zip",
                mode_policy="candidate",
                route="candidate_integration_extract",
            )
        assert not context.root.exists(
            dry_run_module.RelativePath.parse("target")
        )


def test_phase_b_extract_postcreate_mkdir_failure_removes_owned_target(
    tmp_path,
    monkeypatch,
):
    with _authority_run_root(tmp_path / "extract-postcreate-cleanup") as (
        run_root,
        context,
    ):
        registry, tree, bodies = _phase_b_bound_archive_fixture(run_root, context)

        def invoke_first(_source, _key, bound_tree, sink, **_kwargs):
            first = bound_tree.files[0]
            sink(first, io.BytesIO(bodies[first.path]))
            raise AssertionError("post-create failure did not propagate")

        def postcreate_failure(output, relative, *, mode=0o755):
            created = context.root.create_directory(relative, mode=mode)
            try:
                identity = created.identity()
            finally:
                created.close()
            raise dry_run_module.OutputAuthorityPostCreateError(
                created_identity=identity,
                relative=relative,
            )

        monkeypatch.setattr(
            dry_run_module.ReleaseInstallOutputAuthority,
            "mkdir",
            postcreate_failure,
        )
        monkeypatch.setattr(
            dry_run_module, "extract_zip_to_sink", invoke_first
        )
        with pytest.raises(DryRunError, match="post-create proof failed"):
            dry_run_module._extract_bound_archive(
                registry,
                tree,
                run_root / "target",
                run_root,
                context,
                archive_format="zip",
                mode_policy="candidate",
                route="candidate_integration_extract",
            )
        assert not context.root.exists(
            dry_run_module.RelativePath.parse("target")
        )


def test_phase_b_extract_postcreate_already_absent_preserves_primary_error(
    tmp_path,
    monkeypatch,
):
    with _authority_run_root(tmp_path / "extract-postcreate-absent") as (
        run_root,
        context,
    ):
        registry, tree, bodies = _phase_b_bound_archive_fixture(run_root, context)

        def invoke_first(_source, _key, bound_tree, sink, **_kwargs):
            first = bound_tree.files[0]
            sink(first, io.BytesIO(bodies[first.path]))
            raise AssertionError("post-create failure did not propagate")

        def cleaned_then_reported(output, relative, *, mode=0o755):
            created = context.root.create_directory(relative, mode=mode)
            try:
                identity = created.identity()
            finally:
                created.close()
            context.root.remove_owned_tree(
                relative,
                expected_root=identity,
                maximum_entries=1,
            )
            raise dry_run_module.OutputAuthorityPostCreateError(
                created_identity=identity,
                relative=relative,
            )

        monkeypatch.setattr(
            dry_run_module.ReleaseInstallOutputAuthority,
            "mkdir",
            cleaned_then_reported,
        )
        monkeypatch.setattr(
            dry_run_module, "extract_zip_to_sink", invoke_first
        )
        with pytest.raises(
            DryRunError, match="target post-create proof failed"
        ) as captured:
            dry_run_module._extract_bound_archive(
                registry,
                tree,
                run_root / "target",
                run_root,
                context,
                archive_format="zip",
                mode_policy="candidate",
                route="candidate_integration_extract",
            )
        assert "cleanup failed closed" not in str(captured.value)
        assert not context.root.exists(
            dry_run_module.RelativePath.parse("target")
        )


def test_phase_b_extract_postcreate_unknown_identity_fails_without_cleanup_claim(
    tmp_path,
    monkeypatch,
):
    with _authority_run_root(tmp_path / "extract-postcreate-unknown") as (
        run_root,
        context,
    ):
        registry, tree, bodies = _phase_b_bound_archive_fixture(run_root, context)

        def invoke_first(_source, _key, bound_tree, sink, **_kwargs):
            first = bound_tree.files[0]
            sink(first, io.BytesIO(bodies[first.path]))
            raise AssertionError("post-create failure did not propagate")

        def unknown_identity(output, relative, *, mode=0o755):
            created = context.root.create_directory(relative, mode=mode)
            created.close()
            raise dry_run_module.OutputAuthorityPostCreateError(
                created_identity=None,
                relative=relative,
            )

        monkeypatch.setattr(
            dry_run_module.ReleaseInstallOutputAuthority,
            "mkdir",
            unknown_identity,
        )
        monkeypatch.setattr(
            dry_run_module, "extract_zip_to_sink", invoke_first
        )
        with pytest.raises(
            DryRunError, match="target post-create proof failed"
        ):
            dry_run_module._extract_bound_archive(
                registry,
                tree,
                run_root / "target",
                run_root,
                context,
                archive_format="zip",
                mode_policy="candidate",
                route="candidate_integration_extract",
            )
        assert context.root.entry_identity(
            dry_run_module.RelativePath.parse("target")
        ).kind == "directory"
        assert "candidate_integration_extract" not in context.bound_archive_routes
        assert context.evidence()["local_acceptance_blockers_closed"] == []


def test_phase_b_extract_sync_failure_removes_identity_owned_target(
    tmp_path,
    monkeypatch,
):
    with _authority_run_root(tmp_path / "extract-sync-cleanup") as (
        run_root,
        context,
    ):
        registry, tree, bodies = _phase_b_bound_archive_fixture(run_root, context)

        def deliver(_source, _key, bound_tree, sink, **_kwargs):
            for record in bound_tree.files:
                sink(record, io.BytesIO(bodies[record.path]))
            return SimpleNamespace(
                file_count=len(bound_tree.files),
                total_bytes=bound_tree.total_bytes,
            )

        monkeypatch.setattr(dry_run_module, "extract_zip_to_sink", deliver)
        monkeypatch.setattr(
            dry_run_module,
            "_sync_tree_authority",
            lambda *_args: (_ for _ in ()).throw(DryRunError("sync failed")),
        )
        with pytest.raises(DryRunError, match="sync failed"):
            dry_run_module._extract_bound_archive(
                registry,
                tree,
                run_root / "target",
                run_root,
                context,
                archive_format="zip",
                mode_policy="candidate",
                route="candidate_integration_extract",
            )
        assert not context.root.exists(
            dry_run_module.RelativePath.parse("target")
        )


def test_phase_b_extract_cleanup_never_deletes_replacement_identity(
    tmp_path,
    monkeypatch,
):
    with _authority_run_root(tmp_path / "extract-replacement") as (
        run_root,
        context,
    ):
        registry, tree, bodies = _phase_b_bound_archive_fixture(run_root, context)

        def replace_then_fail(_source, _key, bound_tree, sink, **_kwargs):
            first = bound_tree.files[0]
            sink(first, io.BytesIO(bodies[first.path]))
            target = run_root / "target"
            original = context.root.entry_identity(context.relative(target))
            context.root.remove_owned_tree(
                context.relative(target),
                expected_root=original,
                maximum_entries=dry_run_module.MAX_ARCHIVE_MEMBER_COUNT + 1,
            )
            context.output.mkdir(context.relative(target), mode=0o755)
            raise dry_run_module.BoundArchiveError("SECRET_MATERIAL")

        monkeypatch.setattr(
            dry_run_module, "extract_zip_to_sink", replace_then_fail
        )
        with pytest.raises(
            DryRunError, match="target cleanup failed closed"
        ) as captured:
            dry_run_module._extract_bound_archive(
                registry,
                tree,
                run_root / "target",
                run_root,
                context,
                archive_format="zip",
                mode_policy="candidate",
                route="candidate_integration_extract",
            )
        assert context.root.entry_identity(
            dry_run_module.RelativePath.parse("target")
        ).kind == "directory"
        group = captured.value.__cause__
        assert isinstance(group, BaseExceptionGroup)
        assert len(group.exceptions) == 2
        assert "SECRET_MATERIAL" not in str(captured.value)


from scripts import dry_run_release_install as dry_run_cli
from release_install_dry_run import (
    DRY_RUN_CONTRACT,
    FAULT_POINTS,
    OPTIONS_CONTRACT,
    PROCESS_LEASE_CONTRACT,
    RECOVERY_FAULT_POINTS,
    DryRunError,
    LegacyRollbackPolicy,
    execute_release_install_rollback_dry_run,
    recover_release_install_dry_run,
)
from storm_tracker_worker.release_artifacts import build_release_candidate


ROOT = Path(__file__).resolve().parents[1]
CRASH_HELPER = ROOT / "tests" / "helpers" / "release_install_dry_run_crash_child.py"
RAW_CAPTURE_SANITIZED_OPTIONS = (
    ROOT
    / "release_inputs"
    / "supervisor_options"
    / "storm_tracker_worker_raw_capture_shadow_pre_target_v1.json"
)


@contextmanager
def _authority_run_root(run_root: Path):
    """Hold the process lease and phase-A authority for a focused helper test."""

    lease = dry_run_module._acquire_process_lease(run_root)
    context = None
    try:
        context = dry_run_module._open_authority_run_context(
            run_root,
            existing=False,
            process_lease=lease,
        )
        yield context.lexical_root, context
    finally:
        if context is not None:
            context.close()
        lease.release()

_EXPECTED_EXECUTION_JOURNALS = (
    "journal.initialized",
    "journal.upgrade_prepared",
    "journal.worker_baseline_backed_up",
    "journal.worker_candidate_activated",
    "journal.integration_baseline_backed_up",
    "journal.integration_candidate_activated",
    "journal.upgrade_commit_decided",
    "journal.upgrade_committed",
    "journal.rollback_decided",
    "journal.rollback_completed",
)
_EXPECTED_EXECUTION_RENAMES = (
    "rename.upgrade_worker_backup",
    "rename.upgrade_worker_activate",
    "rename.upgrade_integration_backup",
    "rename.upgrade_integration_activate",
    "rename.rollback_integration_retain",
    "rename.rollback_integration_restore",
    "rename.rollback_worker_retain",
    "rename.rollback_worker_restore",
)
_EXPECTED_RECOVERY_JOURNALS = (
    "journal.rollback_recovered",
    "journal.upgrade_recovered",
    "journal.upgrade_recovered_to_baseline",
)
_EXPECTED_RECOVERY_RENAMES = (
    "rename.recovery_integration_retain",
    "rename.recovery_integration_restore",
    "rename.recovery_worker_retain",
    "rename.recovery_worker_restore",
)
EXPECTED_EXECUTION_FAULT_POINTS = frozenset({
    *(f"{prefix}.after_pending_fsync_before_publish" for prefix in _EXPECTED_EXECUTION_JOURNALS),
    *(f"{prefix}.after_publish_before_directory_sync" for prefix in _EXPECTED_EXECUTION_JOURNALS),
    *(f"{prefix}.after_directory_sync" for prefix in _EXPECTED_EXECUTION_JOURNALS),
    *(f"{prefix}.before_rename" for prefix in _EXPECTED_EXECUTION_RENAMES),
    *(
        f"{prefix}.after_rename_before_source_directory_sync"
        for prefix in _EXPECTED_EXECUTION_RENAMES
    ),
    *(f"{prefix}.after_source_directory_sync" for prefix in _EXPECTED_EXECUTION_RENAMES),
    *(f"{prefix}.after_destination_directory_sync" for prefix in _EXPECTED_EXECUTION_RENAMES),
    "upgrade.before_commit_decision",
})
EXPECTED_RECOVERY_FAULT_POINTS = frozenset({
    *(f"{prefix}.after_pending_fsync_before_publish" for prefix in _EXPECTED_RECOVERY_JOURNALS),
    *(f"{prefix}.after_publish_before_directory_sync" for prefix in _EXPECTED_RECOVERY_JOURNALS),
    *(f"{prefix}.after_directory_sync" for prefix in _EXPECTED_RECOVERY_JOURNALS),
    *(f"{prefix}.before_rename" for prefix in _EXPECTED_RECOVERY_RENAMES),
    *(
        f"{prefix}.after_rename_before_source_directory_sync"
        for prefix in _EXPECTED_RECOVERY_RENAMES
    ),
    *(f"{prefix}.after_source_directory_sync" for prefix in _EXPECTED_RECOVERY_RENAMES),
    *(f"{prefix}.after_destination_directory_sync" for prefix in _EXPECTED_RECOVERY_RENAMES),
})


def _canonical(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _config(
    version: str,
    *,
    candidate: bool,
    schema_drift: bool = False,
) -> str:
    common = [
        ("queue_size", "16", '"int(1,16)"'),
        ("worker_nice", "15" if candidate else "0", '"int(0,19)"'),
        ("heartbeat_seconds", "10", '"int(2,60)"'),
        ("log_level", '"INFO"', '"list(DEBUG|INFO|WARNING|ERROR)"'),
        ("netatmo_client_id", '""', '"str?"'),
        ("netatmo_client_secret", '""', '"password?"'),
        ("netatmo_refresh_token", '""', '"password?"'),
        ("synoptic_token", '""', '"password?"'),
    ]
    legacy_only = [
        ("cpu_threads", "2", '"int(1,6)"'),
        ("reserve_ha_core", "true", '"bool"'),
    ]
    candidate_only = [
        (
            "runtime_mode",
            '"worker_only"',
            '"list(legacy|worker_first_canary|raw_capture_shadow|worker_only)"',
        ),
        ("worker_first_config_path", '""', '"str?"'),
        ("worker_first_config_sha256", '""', '"str?"'),
        ("worker_first_authority_sha256", '""', '"str?"'),
        ("synoptic_raw_retention_terms_accepted", "false", '"bool"'),
        ("rainviewer_cache_attribution_accepted", "false", '"bool"'),
        ("kmi_appv4_raw_retention_authorized", "false", '"bool"'),
        ("opera_raw_retention_authorized", "false", '"bool"'),
        ("blitzortung_raw_retention_authorized", "false", '"bool"'),
        ("knmi_api_key", '""', '"password?"'),
        ("aemet_api_key", '""', '"password?"'),
        ("meteofrance_api_token", '""', '"password?"'),
        ("meteofrance_application_id", '""', '"password?"'),
        ("hsaf_username", '""', '"str?"'),
        ("hsaf_password", '""', '"password?"'),
        ("eumetsat_consumer_key", '""', '"password?"'),
        ("eumetsat_consumer_secret", '""', '"password?"'),
    ]
    entries = common + (candidate_only if candidate else legacy_only)
    option_lines = [f"  {name}: {default}" for name, default, _schema in entries]
    schema_entries = entries[:-1] if schema_drift else entries
    schema_lines = [f"  {name}: {schema}" for name, _default, schema in schema_entries]
    return "\n".join([
        'name: "Storm Tracker Worker"',
        f'version: "{version}"',
        'slug: "storm_tracker_worker"',
        "options:",
        *option_lines,
        "schema:",
        *schema_lines,
        "",
    ])


def _candidate(base: Path, *, schema_drift: bool = False):
    integration = base / "candidate-source" / "storm_tracker_v3"
    worker = base / "candidate-worker-source" / "storm_tracker_worker"
    integration.mkdir(parents=True)
    worker.mkdir(parents=True)
    (integration / "manifest.json").write_text(json.dumps({
        "domain": "storm_tracker_v3",
        "name": "Storm Tracker V3",
        "version": "1.13.0",
    }), encoding="utf-8")
    (integration / "__init__.py").write_text(
        'STORE = Factory(build_id="integration-1.13.0-worker-1.16.0")\n',
        encoding="utf-8",
    )
    (integration / "model.py").write_text("VALUE = 113\n", encoding="utf-8")
    (worker / "config.yaml").write_text(
        _config("1.16.0", candidate=True, schema_drift=schema_drift),
        encoding="utf-8",
    )
    (worker / "__init__.py").write_text('__version__ = "1.16.0"\n', encoding="utf-8")
    (worker / "compose.yaml").write_text(
        "services:\n  worker:\n    image: local\n",
        encoding="utf-8",
    )
    (worker / "run.sh").write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    (worker / "service.py").write_text("VALUE = 116\n", encoding="utf-8")
    output = base / "candidate"
    output.mkdir()
    return build_release_candidate(
        integration_root=integration,
        worker_root=worker,
        integration_version="1.13.0",
        worker_version="1.16.0",
        output_directory=output,
    )


def _zip_info(name: str, mode: int, *, directory: bool) -> zipfile.ZipInfo:
    info = zipfile.ZipInfo(name, (2026, 7, 29, 12, 0, 0))
    info.create_system = 3
    info.external_attr = ((stat.S_IFDIR if directory else stat.S_IFREG) | mode) << 16
    if directory:
        info.external_attr |= 0x10
    info.compress_type = zipfile.ZIP_STORED if directory else zipfile.ZIP_DEFLATED
    return info


def _legacy_integration(
    base: Path,
    *,
    forbidden_directory: bool = False,
    extra_member: str | None = None,
    empty_directories: tuple[str, ...] = (),
) -> Path:
    path = base / "storm_tracker_v3_v1.11.0_deploy.zip"
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr(_zip_info("storm_tracker_v3/", 0o777, directory=True), b"")
        if forbidden_directory:
            archive.writestr(
                _zip_info("storm_tracker_v3/tests/", 0o777, directory=True), b""
            )
        for relative in empty_directories:
            archive.writestr(
                _zip_info(
                    f"storm_tracker_v3/{relative}/", 0o777, directory=True
                ),
                b"",
            )
        manifest = json.dumps({
            "domain": "storm_tracker_v3",
            "name": "Storm Tracker V3",
            "version": "1.11.0",
        }).encode("utf-8")
        archive.writestr(
            _zip_info("storm_tracker_v3/manifest.json", 0o666, directory=False),
            manifest,
        )
        archive.writestr(
            _zip_info("storm_tracker_v3/__init__.py", 0o666, directory=False),
            b"VALUE = 111\n",
        )
        if extra_member is not None:
            archive.writestr(
                _zip_info(extra_member, 0o666, directory=False), b"unsafe\n"
            )
    return path


def _tar_info(name: str, *, directory: bool, size: int = 0) -> tarfile.TarInfo:
    info = tarfile.TarInfo(name)
    info.type = tarfile.DIRTYPE if directory else tarfile.REGTYPE
    info.size = 0 if directory else size
    info.mode = 0o777 if directory else 0o666
    info.uid = 0
    info.gid = 0
    info.mtime = 1_722_252_800
    return info


def _legacy_worker(
    base: Path,
    *,
    empty_directories: tuple[str, ...] = (),
) -> Path:
    path = base / "storm_tracker_worker_v1.14.0_deploy.tar.gz"
    files = {
        "config.yaml": _config("1.14.0", candidate=False).encode("utf-8"),
        "__init__.py": b'__version__ = "1.14.0"\n',
        "compose.yaml": b"services:\n  worker:\n    image: local\n",
        "run.sh": b"#!/bin/sh\nexit 0\n",
        "service.py": b"VALUE = 114\n",
    }
    with tarfile.open(path, "w:gz") as archive:
        archive.addfile(_tar_info("storm_tracker_worker", directory=True))
        for relative in empty_directories:
            archive.addfile(_tar_info(
                f"storm_tracker_worker/{relative}", directory=True
            ))
        for name, data in files.items():
            archive.addfile(
                _tar_info(f"storm_tracker_worker/{name}", directory=False, size=len(data)),
                io.BytesIO(data),
            )
    return path


def _options(base: Path) -> Path:
    path = base / "sanitized-options.json"
    path.write_text(json.dumps({
        "contract": OPTIONS_CONTRACT,
        "options": {
            "cpu_threads": 2,
            "heartbeat_seconds": 10,
            "log_level": "INFO",
            "queue_size": 16,
            "reserve_ha_core": True,
            "runtime_mode": "legacy",
            "blitzortung_raw_retention_authorized": False,
            "kmi_appv4_raw_retention_authorized": False,
            "opera_raw_retention_authorized": False,
            "rainviewer_cache_attribution_accepted": False,
            "synoptic_raw_retention_terms_accepted": False,
            "worker_first_authority_sha256": "",
            "worker_first_config_path": "",
            "worker_first_config_sha256": "",
            "worker_nice": 15,
        },
        "secret_presence": {
            "aemet_api_key": False,
            "eumetsat_consumer_key": False,
            "eumetsat_consumer_secret": False,
            "hsaf_password": False,
            "hsaf_username": False,
            "knmi_api_key": False,
            "meteofrance_api_token": False,
            "meteofrance_application_id": False,
            "netatmo_client_id": False,
            "netatmo_client_secret": False,
            "netatmo_refresh_token": False,
            "synoptic_token": False,
        },
    }, sort_keys=True), encoding="utf-8")
    return path


def _inputs(base: Path, *, schema_drift: bool = False):
    candidate = _candidate(base, schema_drift=schema_drift)
    rollback_integration = _legacy_integration(base)
    rollback_worker = _legacy_worker(base)
    options = _options(base)
    policy = LegacyRollbackPolicy(
        integration_version="1.11.0",
        integration_sha256=_sha(rollback_integration),
        worker_version="1.14.0",
        worker_sha256=_sha(rollback_worker),
    )
    return candidate, rollback_integration, rollback_worker, options, policy


@pytest.fixture(scope="module")
def release_inputs(tmp_path_factory):
    return _inputs(tmp_path_factory.mktemp("release-dry-run-inputs"))


def _run(inputs, run_root: Path, **extra):
    candidate, rollback_integration, rollback_worker, options, policy = inputs
    return execute_release_install_rollback_dry_run(
        candidate_evidence=candidate.evidence_path,
        rollback_integration_archive=rollback_integration,
        rollback_worker_archive=rollback_worker,
        options_snapshot=options,
        run_root=run_root,
        rollback_policy=policy,
        **extra,
    )


def _expected_recovery(point: str) -> str:
    if point.startswith("journal.upgrade_commit_decided."):
        return (
            "baseline"
            if point.endswith("after_pending_fsync_before_publish")
            else "candidate"
        )
    if point.startswith("journal.upgrade_committed."):
        return "candidate"
    if point == "journal.rollback_decided.after_pending_fsync_before_publish":
        return "candidate"
    if point.startswith(("journal.rollback_decided.", "journal.rollback_completed.")):
        return "baseline"
    if point.startswith("rename.rollback_"):
        return "baseline"
    return "baseline"


def test_local_upgrade_and_rollback_include_exact_supervisor_options_roundtrip(
    tmp_path: Path,
    release_inputs,
):
    before = [_sha(path) for path in (
        release_inputs[0].evidence_path,
        release_inputs[0].integration_archive,
        release_inputs[0].worker_archive,
        release_inputs[1],
        release_inputs[2],
        release_inputs[3],
    )]
    result = _run(release_inputs, tmp_path / "run")
    report = result.report
    assert report["contract"] == DRY_RUN_CONTRACT
    assert report["simulation_completed"] is True
    assert report["local_simulation_passed"] is True
    expected_local = []
    expected_external = [
        "actual_target_installation_not_exercised",
        "data_and_schema_rollback_not_validated",
        "hostile_same_user_output_races_not_excluded",
        "human_deploy_approval_not_granted",
        "runtime_service_stop_and_start_not_exercised",
        "supervisor_build_and_cache_not_exercised",
        "target_filesystem_power_loss_not_exercised",
    ]
    expected_supervisor = []
    expected_namespaces = {
        "external_installation": expected_external,
        "local_simulation": expected_local,
        "supervisor_installation": expected_supervisor,
    }
    assert report["blocker_namespaces"] == expected_namespaces
    assert report["local_acceptance_blockers"] == expected_local
    assert report["local_acceptance_blockers_closed"] == sorted(
        dry_run_module.LOCAL_ACCEPTANCE_BLOCKERS
    )
    assert report["supervisor_installation_blockers"] == expected_supervisor
    assert report["unresolved_external_gates"] == expected_external
    assert report["installation_preflight_blockers"] == sorted(
        f"{namespace}:{code}"
        for namespace, codes in expected_namespaces.items()
        for code in codes
    )
    assert report["real_installation_ready"] is False
    assert report["supervisor_options_roundtrip_proven"] is True
    compatibility = report["options_compatibility"]
    assert compatibility["migration_proven"] is True
    assert compatibility["activation_option_values_reported"] is False
    assert compatibility["activation_option_names"] == [
        "runtime_mode",
        "worker_first_config_path",
        "worker_first_config_sha256",
        "worker_first_authority_sha256",
    ]
    assert compatibility["removed_baseline_schema_names"] == [
        "cpu_threads",
        "reserve_ha_core",
    ]
    assert report["baseline"]["worker"]["identity"][
        "activation_options_contract_exact"
    ] is False
    assert report["candidate"]["worker"]["identity"][
        "activation_options_contract_exact"
    ] is True
    assert report["baseline"]["worker"]["identity"][
        "raw_retention_options_contract_exact"
    ] is False
    assert report["candidate"]["worker"]["identity"][
        "raw_retention_options_contract_exact"
    ] is True
    assert report["deployment_performed"] is False
    assert report["network_performed"] is False
    assert report["live_paths_touched"] is False
    assert report["component_atomic"] is True
    assert report["pair_atomic"] is False
    lease_report = report["exclusive_process_lease"]
    assert report["exclusive_process_lease_proven"] is True
    assert lease_report["contract"] == PROCESS_LEASE_CONTRACT
    assert lease_report["acquisition"] == "non_blocking"
    assert lease_report["held_before_run_root_mutation"] is True
    assert lease_report["os_released_after_abrupt_exit"] is True
    assert lease_report["descriptor_inheritable"] is False
    assert lease_report["backend"] in {"posix_flock", "windows_msvcrt"}
    assert report["candidate_worker_gzip_header_canonicality_proven"] is True
    assert report["legacy_worker_noncanonical_gzip_header_hash_bound"] is True
    assert report["directory_durability_proven"] is False
    assert report["spooled_inputs_only_after_capture"] is True
    assert report["input_sources_reproved_after_simulation"] is True
    assert report["legacy_empty_directory_canonicality_proven"] is True
    assert len(report["input_spools"]) == 6
    for spool in report["input_spools"].values():
        assert spool["exclusive_creation"] is True
        assert spool["fsynced_before_use"] is True
        assert spool["physical_read_only"] is (os.name != "nt")
        assert spool["single_link"] is True
        assert spool["source_sha256"] == spool["spool_sha256"]
    assert report["input_spools_exclusive_fsynced_read_only"] is (
        os.name != "nt"
    )
    assert report["input_spools_exclusive_fsynced_sealed"] is True
    assert report["destination_absence_prechecked"] is True
    assert report["atomic_noreplace_publication_proven"] is True
    atomic_noreplace = report["atomic_noreplace_publication"]
    assert atomic_noreplace["adapter_contract"] == (
        "stv3-descriptor-relative-atomic-adapter-v1"
    )
    assert atomic_noreplace["contract"] == (
        "stv3-open-directory-authority-atomic-probe-v1"
    )
    assert atomic_noreplace["descriptor_relative_probe"] is True
    assert atomic_noreplace["path_reopen_used"] is False
    assert atomic_noreplace["path_authority_scope"] == (
        "already_open_directory_authority_handle"
    )
    assert atomic_noreplace["expected_source_identity_required"] is True
    assert atomic_noreplace[
        "root_identity_reproved_before_and_after_publication"
    ] is True
    assert atomic_noreplace[
        "source_consumption_and_destination_identity_reproved"
    ] is True
    assert atomic_noreplace["generation_directory_publications"] == 8
    assert atomic_noreplace["journal_pending_to_final_publications"] == 10
    assert atomic_noreplace["publication_scopes"] == [
        "generation_directory",
        "journal_pending_to_final",
    ]
    assert atomic_noreplace["allowed_root_bootstrap_proven"] is True
    assert atomic_noreplace[
        "ancestor_and_final_output_path_race_exclusion_proven"
    ] is True
    assert atomic_noreplace["descriptor_relative_output_authority_proven"] is True
    assert atomic_noreplace["simulator_integration_proven"] is True
    assert atomic_noreplace["local_acceptance_blockers_closed"] == sorted(
        dry_run_module.LOCAL_ACCEPTANCE_BLOCKERS
    )
    output_authority = report["output_authority"]
    assert output_authority["simulator_integration_proven"] is True
    assert output_authority["local_acceptance_blockers_closed"] == sorted(
        dry_run_module.LOCAL_ACCEPTANCE_BLOCKERS
    )
    assert output_authority["local_acceptance_blockers"] == []
    assert output_authority["bound_archive_processing_phase_b"] is True
    assert output_authority["bound_archive_route_count"] == 8
    assert output_authority["bound_archive_routes"] == sorted({
        "candidate_integration_validate",
        "candidate_integration_extract",
        "candidate_worker_validate",
        "candidate_worker_extract",
        "rollback_integration_inspect",
        "rollback_integration_extract",
        "rollback_worker_inspect",
        "rollback_worker_extract",
    })
    bound_archive_processing = output_authority["bound_archive_processing"]
    assert bound_archive_processing["archive_paths_accepted"] is False
    assert bound_archive_processing["processing_required"] is True
    assert bound_archive_processing["route_contract_satisfied"] is True
    assert bound_archive_processing["integration"] is True
    assert bound_archive_processing[
        "simulator_route_integration_proven"
    ] is True
    assert bound_archive_processing["simulator_integration_proven"] is True
    assert bound_archive_processing["local_acceptance_blockers_closed"] == sorted(
        dry_run_module.LOCAL_ACCEPTANCE_BLOCKERS
    )
    path_scope = output_authority["path_race_scope"]
    assert path_scope["contract"] == "stv3-local-output-path-race-scope-v1"
    assert path_scope[
        "local_ancestor_and_final_output_path_race_exclusion_proven"
    ] is True
    assert all(path_scope["local"].values())
    assert path_scope["hostile_same_user_in_local_scope"] is False
    assert path_scope["external"] == {
        "hostile_same_user_output_races_excluded": False,
    }
    assert "hostile_same_user_output_races_not_excluded" in report[
        "unresolved_external_gates"
    ]
    assert report["bounded_resource_policy"]["archive_member_count"] == 20_000
    assert report["bounded_resource_policy"]["archive_member_chunk_bytes"] == 1024 * 1024
    assert report["streaming_archive_member_processing_proven"] is True
    assert report["bounded_streaming_archive_header_parsing_proven"] is True
    assert report["bounded_streaming_archive_header_parsing"] == {
        "archive_extensions_fail_closed": [
            "gnu_long_name_or_link",
            "pax",
            "zip64",
        ],
        "bounded_metadata_before_stdlib_parse": True,
        "declared_member_and_total_bytes_bounded": True,
        "exact_index_tree_binding": ["count", "kind", "path", "size"],
        "same_descriptor_for_preflight_and_stdlib": True,
        "stdlib_archive_reads_preflighted": 8,
    }
    assert report["streaming_archive_member_processing"] == {
        "actual_size_reproved": True,
        "archive_validation_precedes_destination_creation": True,
        "chunk_bytes": 1024 * 1024,
        "contract": "stv3-archive-member-stream-v1",
        "destination_io_failure_cleanup_proven": True,
        "forbidden_marker_overlap_reproved": True,
        "member_sha256_reproved": True,
        "overrun_probe_bytes": 1,
        "partial_writes_handled": True,
        "whole_member_materialization": False,
    }
    assert report["baseline"]["integration"]["observed_legacy_modes"] == [0o666, 0o777]
    assert report["baseline"]["worker"]["observed_legacy_modes"] == [0o666, 0o777]
    assert json.loads(
        (tmp_path / "run" / "install" / "integration.active" / "manifest.json")
        .read_text(encoding="utf-8")
    )["version"] == "1.11.0"
    assert json.loads(
        (tmp_path / "run" / "retained" / "integration.candidate" / "manifest.json")
        .read_text(encoding="utf-8")
    )["version"] == "1.13.0"
    assert result.report_sha256 == _sha(result.report_path)
    after = [_sha(path) for path in (
        release_inputs[0].evidence_path,
        release_inputs[0].integration_archive,
        release_inputs[0].worker_archive,
        release_inputs[1],
        release_inputs[2],
        release_inputs[3],
    )]
    assert after == before


def _exclusive_lease_holder(run_root: Path, exit_mode: str) -> subprocess.Popen[str]:
    environment = dict(os.environ)
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    environment["PYTHONPATH"] = str(ROOT)
    child = subprocess.Popen(
        [
            sys.executable,
            str(CRASH_HELPER),
            "hold-lease",
            str(run_root),
            exit_mode,
        ],
        cwd=ROOT,
        env=environment,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    assert child.stdout is not None
    assert child.stdout.readline() == "lease-held\n"
    return child


def _finish_exclusive_lease_holder(
    child: subprocess.Popen[str], *, expected_returncode: int
) -> None:
    stdout, stderr = child.communicate("release\n", timeout=10)
    assert stdout == ""
    assert child.returncode == expected_returncode, stderr


def test_phase_b_all_eight_archive_routes_reuse_four_bound_registries(
    tmp_path: Path,
    release_inputs,
    monkeypatch,
):
    run_root = tmp_path / "phase-b-routes"
    calls = []
    names = (
        "validate_candidate_zip",
        "validate_candidate_tar_gz",
        "inspect_legacy_zip",
        "inspect_legacy_tar_gz",
        "extract_zip_to_sink",
        "extract_tar_gz_to_sink",
    )

    def wrapped(name, original):
        def invoke(*args, **kwargs):
            source = args[0]
            source.require_exact_keys(("archive",))
            calls.append((name, id(source), dict(kwargs)))
            if name.startswith(("validate_", "inspect_")):
                assert not any(
                    (run_root / child).exists()
                    for child in ("journal", "stage", "install", "backup", "retained")
                )
            return original(*args, **kwargs)
        return invoke

    for name in names:
        monkeypatch.setattr(
            dry_run_module,
            name,
            wrapped(name, getattr(dry_run_module, name)),
        )
    result = _run(release_inputs, run_root)
    assert result.report["output_authority"]["bound_archive_route_count"] == 8
    assert [item[0] for item in calls] == [
        "validate_candidate_zip",
        "validate_candidate_tar_gz",
        "inspect_legacy_zip",
        "inspect_legacy_tar_gz",
        "extract_zip_to_sink",
        "extract_tar_gz_to_sink",
        "extract_zip_to_sink",
        "extract_tar_gz_to_sink",
    ]
    source_ids = [item[1] for item in calls]
    assert len(set(source_ids[:4])) == 4
    assert source_ids[0] == source_ids[6]
    assert source_ids[1] == source_ids[7]
    assert source_ids[2] == source_ids[4]
    assert source_ids[3] == source_ids[5]
    assert calls[4][2]["mode_policy"] == "legacy"
    assert calls[5][2]["mode_policy"] == "legacy"
    assert calls[6][2]["mode_policy"] == "candidate"
    assert calls[7][2]["mode_policy"] == "candidate"
    assert calls[3][2]["exact_archive_sha256"] == release_inputs[4].worker_sha256
    assert calls[5][2]["exact_legacy_sha256"] == release_inputs[4].worker_sha256


def test_phase_b_recovery_reuses_inventory_snapshot_until_a_rename(
    tmp_path: Path,
    release_inputs,
    monkeypatch,
):
    run_root = tmp_path / "phase-b-recovery-inventory"
    _run(release_inputs, run_root)
    original = dry_run_module.OwnedOutputReadAuthority.generation_inventory
    calls = []

    def counted(self, *args, **kwargs):
        calls.append(tuple(sorted(kwargs)))
        return original(self, *args, **kwargs)

    monkeypatch.setattr(
        dry_run_module.OwnedOutputReadAuthority,
        "generation_inventory",
        counted,
    )
    recovered = recover_release_install_dry_run(run_root)
    assert recovered["outcome"] == "baseline"
    assert len(calls) == 1
    output = recovered["output_authority"]
    assert output["operation_kind"] == "recover"
    assert output["bound_archive_processing_required"] is False
    assert output["bound_archive_route_count"] == 0
    assert output["bound_archive_route_count_expected"] == 0
    assert output["bound_archive_routes_expected"] == []
    assert output["bound_archive_routes_satisfied"] is True
    assert output["simulator_integration_proven"] is True
    assert output["local_acceptance_blockers"] == []
    assert output["local_acceptance_blockers_closed"] == sorted(
        dry_run_module.LOCAL_ACCEPTANCE_BLOCKERS
    )
    assert output["atomic_capability"]["simulator_integration_proven"] is True
    assert output["atomic_capability"][
        "local_acceptance_blockers_closed"
    ] == output["local_acceptance_blockers_closed"]
    assert output["atomic_capability"][
        "absolute_ancestor_resolution_no_reparse_proven"
    ] is True
    bound_archive_processing = output["bound_archive_processing"]
    assert bound_archive_processing["archive_paths_accepted"] is False
    assert bound_archive_processing["processing_required"] is False
    assert bound_archive_processing["route_contract_satisfied"] is True
    assert bound_archive_processing["integration"] is False
    assert bound_archive_processing[
        "simulator_route_integration_proven"
    ] is False
    assert bound_archive_processing["simulator_integration_proven"] is False
    assert bound_archive_processing["local_acceptance_blockers_closed"] == []


@pytest.mark.parametrize(
    "seam",
    ("bootstrap", "lease", "owned_reads", "atomic", "postpublication"),
)
def test_phase_c_evidence_reopens_blockers_when_a_required_subproof_is_false(
    tmp_path,
    monkeypatch,
    seam,
):
    run_root = tmp_path / f"phase-c-evidence-false-{seam}"
    with _authority_run_root(run_root) as (_root, context):
        context.operation_kind = "recover"
        context.expected_bound_archive_routes = 0
        if seam == "bootstrap":
            monkeypatch.setattr(
                dry_run_module.DirectoryAuthority,
                "absolute_ancestor_resolution_no_reparse_proven",
                property(lambda self: False),
            )
        elif seam == "lease":
            original = dry_run_module._ExclusiveProcessLease.evidence

            def false_lease(lease):
                evidence = original(lease)
                evidence["descriptor_relative_anchor"] = False
                return evidence

            monkeypatch.setattr(
                dry_run_module._ExclusiveProcessLease, "evidence", false_lease
            )
        elif seam == "owned_reads":
            original = dry_run_module.owned_output_read_evidence

            def false_reads():
                evidence = original()
                evidence["descriptor_relative_owned_reads"] = False
                return evidence

            monkeypatch.setattr(
                dry_run_module, "owned_output_read_evidence", false_reads
            )
        elif seam == "atomic":
            monkeypatch.setattr(
                dry_run_module.OpenAuthorityAtomicCapability,
                "proven",
                property(lambda self: False),
            )
        else:
            original = dry_run_module.output_authority_evidence

            def false_postpublication():
                evidence = original()
                evidence["postpublication_final_cleanup_proven"] = False
                evidence[
                    "postcreate_and_postpublication_outcomes_classified"
                ] = False
                return evidence

            monkeypatch.setattr(
                dry_run_module, "output_authority_evidence", false_postpublication
            )
        evidence = context.evidence()
        assert evidence["simulator_integration_proven"] is False
        assert evidence["local_acceptance_blockers"] == list(
            dry_run_module.LOCAL_ACCEPTANCE_BLOCKERS
        )
        assert evidence["local_acceptance_blockers_closed"] == []


def test_phase_c_evidence_rejects_unknown_or_inconsistent_route_profile(
    tmp_path,
):
    run_root = tmp_path / "phase-c-route-profile"
    with _authority_run_root(run_root) as (_root, context):
        context.operation_kind = "unknown"
        with pytest.raises(DryRunError, match="operation kind is invalid"):
            context.evidence()
        context.operation_kind = "recover"
        context.expected_bound_archive_routes = 8
        with pytest.raises(DryRunError, match="route expectation differs"):
            context.evidence()


def test_phase_b_recovery_rescans_inventory_once_after_renames(
    tmp_path: Path,
    release_inputs,
    monkeypatch,
):
    run_root = tmp_path / "phase-b-recovery-rescan"

    class InjectedFailure(RuntimeError):
        pass

    def fail(point):
        if point == "upgrade.before_commit_decision":
            raise InjectedFailure(point)

    with pytest.raises(InjectedFailure, match="upgrade.before_commit_decision"):
        _run(release_inputs, run_root, fault_injector=fail)
    original = dry_run_module.OwnedOutputReadAuthority.generation_inventory
    calls = []

    def counted(self, *args, **kwargs):
        calls.append(tuple(sorted(kwargs)))
        return original(self, *args, **kwargs)

    monkeypatch.setattr(
        dry_run_module.OwnedOutputReadAuthority,
        "generation_inventory",
        counted,
    )
    recovered = recover_release_install_dry_run(run_root)
    assert recovered["outcome"] == "baseline"
    assert len(calls) == 2


def test_phase_b_runtime_owned_zones_ban_lexical_output_reads():
    source = Path(dry_run_module.__file__).read_text(encoding="utf-8")
    module = ast.parse(source)
    functions = {
        node.name: node for node in module.body if isinstance(node, ast.FunctionDef)
    }
    runtime = {
        "_execute_release_install_rollback_with_authority",
        "_recover_release_install_dry_run_under_lease",
        "_upgrade_then_rollback",
        "_load_state",
        "_read_journal",
        "_assert_generation_inventory",
        "_assert_recovery_phase_layout",
        "_integration_identity",
        "_worker_identity",
        "_owned_fingerprint_tree",
    }
    forbidden_attributes = {
        "read_bytes",
        "read_text",
        "glob",
        "iterdir",
        "lstat",
        "walk",
    }
    for name in runtime:
        attributes = {
            node.attr for node in ast.walk(functions[name])
            if isinstance(node, ast.Attribute)
            and not (
                node.attr == "walk"
                and isinstance(node.value, ast.Name)
                and node.value.id == "ast"
            )
        }
        assert attributes.isdisjoint(forbidden_attributes)


def test_phase_b_recovery_ignores_poisoned_lexical_output_path_reads(
    tmp_path: Path,
    release_inputs,
    monkeypatch,
):
    run_root = tmp_path / "phase-b-path-poison"
    _run(release_inputs, run_root)
    original_read_bytes = Path.read_bytes
    original_read_text = Path.read_text
    original_lstat = Path.lstat
    original_glob = Path.glob
    original_iterdir = Path.iterdir
    original_open_context = dry_run_module._open_authority_run_context
    active = False

    def owned(value):
        try:
            Path(value).relative_to(run_root)
            return True
        except ValueError:
            return False

    def poison(name, original):
        def call(path, *args, **kwargs):
            if active and owned(path):
                raise AssertionError(f"lexical owned-output {name} used")
            return original(path, *args, **kwargs)
        return call

    def open_then_poison(*args, **kwargs):
        nonlocal active
        context = original_open_context(*args, **kwargs)
        active = True
        return context

    monkeypatch.setattr(Path, "read_bytes", poison("read_bytes", original_read_bytes))
    monkeypatch.setattr(Path, "read_text", poison("read_text", original_read_text))
    monkeypatch.setattr(Path, "lstat", poison("lstat", original_lstat))
    monkeypatch.setattr(Path, "glob", poison("glob", original_glob))
    monkeypatch.setattr(Path, "iterdir", poison("iterdir", original_iterdir))
    monkeypatch.setattr(
        dry_run_module, "_open_authority_run_context", open_then_poison
    )
    recovered = recover_release_install_dry_run(run_root)
    assert recovered["outcome"] == "baseline"


def test_phase_b_execute_ignores_poisoned_lexical_output_path_reads(
    tmp_path: Path,
    release_inputs,
    monkeypatch,
):
    run_root = tmp_path / "phase-b-execute-path-poison"
    originals = {
        "read_bytes": Path.read_bytes,
        "read_text": Path.read_text,
        "lstat": Path.lstat,
        "glob": Path.glob,
        "iterdir": Path.iterdir,
    }
    original_open_context = dry_run_module._open_authority_run_context
    active = False

    def owned(value):
        try:
            Path(value).relative_to(run_root)
            return True
        except ValueError:
            return False

    def poison(name, original):
        def call(path, *args, **kwargs):
            if active and owned(path):
                raise AssertionError(f"lexical owned-output {name} used")
            return original(path, *args, **kwargs)
        return call

    def open_then_poison(*args, **kwargs):
        nonlocal active
        context = original_open_context(*args, **kwargs)
        active = True
        return context

    for name, original in originals.items():
        monkeypatch.setattr(Path, name, poison(name, original))
    monkeypatch.setattr(
        dry_run_module, "_open_authority_run_context", open_then_poison
    )
    result = _run(release_inputs, run_root)
    assert result.report["simulation_completed"] is True


def test_exclusive_process_lease_contender_precedes_new_run_root_mutation(
    tmp_path: Path,
    release_inputs,
):
    run_root = tmp_path / "lease-run"
    child = _exclusive_lease_holder(run_root, "clean")
    try:
        with pytest.raises(DryRunError, match="process lease is already held"):
            _run(release_inputs, run_root)
        assert not run_root.exists()
        assert child.poll() is None
    finally:
        _finish_exclusive_lease_holder(child, expected_returncode=0)

    result = _run(release_inputs, run_root)
    assert result.report["exclusive_process_lease_proven"] is True


def test_exclusive_process_lease_recovers_after_abrupt_owner_exit(
    tmp_path: Path,
    release_inputs,
):
    run_root = tmp_path / "abrupt-lease-run"
    _run(release_inputs, run_root)
    state_before = (run_root / "dry_run_state.json").read_bytes()
    journal_before = tuple(
        path.read_bytes() for path in sorted((run_root / "journal").glob("*.json"))
    )

    child = _exclusive_lease_holder(run_root, "abrupt")
    try:
        with pytest.raises(DryRunError, match="process lease is already held"):
            recover_release_install_dry_run(run_root)
        assert (run_root / "dry_run_state.json").read_bytes() == state_before
        assert tuple(
            path.read_bytes()
            for path in sorted((run_root / "journal").glob("*.json"))
        ) == journal_before
        assert child.poll() is None
    finally:
        _finish_exclusive_lease_holder(child, expected_returncode=73)

    recovered = recover_release_install_dry_run(run_root)
    assert recovered["exclusive_process_lease"]["contract"] == PROCESS_LEASE_CONTRACT
    assert recovered["outcome"] == "baseline"
    assert recovered["output_authority"]["bound_archive_processing_phase_b"] is False
    assert recovered["output_authority"]["bound_archive_route_count"] == 0
    assert recovered["output_authority"]["bound_archive_routes"] == []


def test_exclusive_process_lease_releases_after_regular_completion(tmp_path: Path):
    run_root = tmp_path / "failed-lease-run"
    first = dry_run_module._acquire_process_lease(run_root)
    try:
        with pytest.raises(DryRunError, match="process lease is already held"):
            dry_run_module._acquire_process_lease(run_root)
    finally:
        first.release()
    second = dry_run_module._acquire_process_lease(run_root)
    second.assert_held()
    second.release()


def test_phase_c_lease_and_run_context_share_one_allowed_root_lineage(
    tmp_path: Path,
    monkeypatch,
):
    run_root = tmp_path / "phase-c-one-bootstrap"
    original = dry_run_module.DirectoryAuthority.open_absolute.__func__
    opened = []

    def counted(cls, path, *, same_filesystem=True):
        authority = original(cls, path, same_filesystem=same_filesystem)
        opened.append((Path(path), authority.identity()))
        return authority

    monkeypatch.setattr(
        dry_run_module.DirectoryAuthority,
        "open_absolute",
        classmethod(counted),
    )
    lease = dry_run_module._acquire_process_lease(run_root)
    context = None
    try:
        assert len(opened) == 1
        context = dry_run_module._open_authority_run_context(
            run_root,
            existing=False,
            process_lease=lease,
        )
        assert len(opened) == 1
        assert context.allowed_root.identity().same_object(
            lease.allowed_identity
        )
        assert context.allowed_root.identity().backend == lease.allowed_identity.backend
        context.close()
        context = None
        lease.assert_held()
    finally:
        if context is not None:
            context.close()
        lease.release()


def test_phase_c_lease_assertion_binds_anchor_name_identity_and_bytes(
    tmp_path,
    monkeypatch,
):
    run_root = tmp_path / "phase-c-anchor-identity"
    lease = dry_run_module._acquire_process_lease(run_root)
    try:
        original = dry_run_module.DirectoryAuthority.entry_identity
        replacement = dry_run_module.NodeIdentity(
            backend=lease.anchor_identity.backend,
            volume=lease.anchor_identity.volume,
            file_id=f"{lease.anchor_identity.file_id}-replacement",
            kind="regular_file",
            link_count=1,
            size=len(lease.anchor_bytes),
        )

        def replaced(authority, relative):
            if (
                authority is lease.parent
                and dry_run_module.RelativePath.parse(relative).leaf
                == lease.binding.anchor_leaf
            ):
                return replacement
            return original(authority, relative)

        monkeypatch.setattr(
            dry_run_module.DirectoryAuthority, "entry_identity", replaced
        )
        with pytest.raises(DryRunError, match="process lease identity was lost"):
            lease.assert_held()
        monkeypatch.setattr(
            dry_run_module.DirectoryAuthority, "entry_identity", original
        )

        assert lease.anchor is not None
        with lease.anchor.duplicate_binary("r+b") as stream:
            stream.seek(0)
            stream.write(b"X")
            stream.flush()
            os.fsync(stream.fileno())
        with pytest.raises(DryRunError, match="process lease identity was lost"):
            lease.assert_held()
    finally:
        lease.release()


@pytest.mark.parametrize(
    "failure_point",
    ("create", "write", "flush", "parent_fsync", "lock", "assert"),
)
def test_phase_c_failed_new_lease_cleans_only_its_exact_anchor(
    tmp_path: Path,
    monkeypatch,
    failure_point: str,
):
    run_root = tmp_path / f"phase-c-lease-failure-{failure_point}"
    binding = dry_run_module._process_lease_binding(run_root)

    if failure_point == "create":
        monkeypatch.setattr(
            dry_run_module.DirectoryAuthority,
            "create_file",
            lambda *args, **kwargs: (_ for _ in ()).throw(
                dry_run_module.AuthorityError("injected create failure")
            ),
        )
    elif failure_point == "write":
        monkeypatch.setattr(
            dry_run_module.BoundFile,
            "write_all",
            lambda *args, **kwargs: (_ for _ in ()).throw(
                dry_run_module.AuthorityError("injected write failure")
            ),
        )
    elif failure_point == "flush":
        original = dry_run_module.BoundFile.flush
        calls = 0

        def fail_first_flush(self):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise dry_run_module.AuthorityError("injected flush failure")
            return original(self)

        monkeypatch.setattr(dry_run_module.BoundFile, "flush", fail_first_flush)
    elif failure_point == "parent_fsync":
        original = dry_run_module.DirectoryAuthority.fsync
        calls = 0

        def fail_first_fsync(self):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise dry_run_module.AuthorityError("injected parent fsync failure")
            return original(self)

        monkeypatch.setattr(
            dry_run_module.DirectoryAuthority, "fsync", fail_first_fsync
        )
    elif failure_point == "lock":
        calls = 0
        original_lock = dry_run_module._lock_process_lease_descriptor

        def fail_then_cleanup_lock(descriptor):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise OSError(errno.EIO, "injected")
            return original_lock(descriptor)
        monkeypatch.setattr(
            dry_run_module,
            "_lock_process_lease_descriptor",
            fail_then_cleanup_lock,
        )
    else:
        monkeypatch.setattr(
            dry_run_module._ExclusiveProcessLease,
            "assert_held",
            lambda self: (_ for _ in ()).throw(
                DryRunError("injected lease assertion failure")
            ),
        )

    with pytest.raises(DryRunError):
        dry_run_module._acquire_process_lease(run_root)
    if os.name == "nt" and failure_point != "create":
        # Windows bound handles deliberately deny delete sharing.  Once the
        # final lease name exists, cleanup therefore preserves that exact
        # inode fail-closed instead of closing the lock and racing an unlink.
        assert binding.anchor_path.is_file()
        monkeypatch.undo()
        recovered = dry_run_module._acquire_process_lease(run_root)
        try:
            assert recovered.anchor.read_bounded(
                len(binding.anchor_bytes)
            ) == binding.anchor_bytes
        finally:
            recovered.release()
        assert binding.anchor_path.read_bytes() == binding.anchor_bytes
    else:
        assert not binding.anchor_path.exists()


def test_phase_c_busy_contender_prevents_created_anchor_unlink(
    tmp_path,
    monkeypatch,
):
    run_root = tmp_path / "phase-c-created-anchor-contender"
    binding = dry_run_module._process_lease_binding(run_root)
    original_lock = dry_run_module._lock_process_lease_descriptor
    remove_calls = []
    lock_calls = 0

    def busy_lock(descriptor):
        nonlocal lock_calls
        lock_calls += 1
        raise BlockingIOError(errno.EACCES, "simulated contender owns inode")

    original_remove = dry_run_module.DirectoryAuthority.remove_owned_tree

    def record_remove(self, *args, **kwargs):
        remove_calls.append(args[0])
        return original_remove(self, *args, **kwargs)

    monkeypatch.setattr(
        dry_run_module, "_lock_process_lease_descriptor", busy_lock
    )
    monkeypatch.setattr(
        dry_run_module.DirectoryAuthority, "remove_owned_tree", record_remove
    )
    with pytest.raises(DryRunError, match="process lease is already held"):
        dry_run_module._acquire_process_lease(run_root)
    assert lock_calls >= 2
    assert remove_calls == []
    assert binding.anchor_path.is_file()
    identity = binding.anchor_path.stat()

    monkeypatch.setattr(
        dry_run_module, "_lock_process_lease_descriptor", original_lock
    )
    lease = dry_run_module._acquire_process_lease(run_root)
    try:
        after = binding.anchor_path.stat()
        assert (after.st_dev, after.st_ino) == (identity.st_dev, identity.st_ino)
        lease.assert_held()
    finally:
        lease.release()


def test_phase_c_busy_preexisting_stale_anchor_is_not_repaired_before_lock(
    tmp_path,
    monkeypatch,
):
    run_root = tmp_path / "phase-c-busy-stale-anchor"
    binding = dry_run_module._process_lease_binding(run_root)
    stale = b"stale-before-lock"
    binding.anchor_path.write_bytes(stale)
    before = binding.anchor_path.stat()

    monkeypatch.setattr(
        dry_run_module,
        "_lock_process_lease_descriptor",
        lambda descriptor: (_ for _ in ()).throw(
            BlockingIOError(errno.EACCES, "simulated contender owns inode")
        ),
    )
    with pytest.raises(DryRunError, match="process lease is already held"):
        dry_run_module._acquire_process_lease(run_root)
    after = binding.anchor_path.stat()
    assert (after.st_dev, after.st_ino) == (before.st_dev, before.st_ino)
    assert binding.anchor_path.read_bytes() == stale


@pytest.mark.parametrize(
    "stale",
    (b"short", b"longer-than-canonical" * 32),
)
def test_phase_c_locked_stale_anchor_is_rewritten_and_truncated(
    tmp_path,
    stale,
):
    run_root = tmp_path / f"phase-c-stale-anchor-{len(stale)}"
    binding = dry_run_module._process_lease_binding(run_root)
    binding.anchor_path.write_bytes(stale)
    before = binding.anchor_path.stat()

    lease = dry_run_module._acquire_process_lease(run_root)
    try:
        during = binding.anchor_path.stat()
        assert (during.st_dev, during.st_ino) == (before.st_dev, before.st_ino)
        assert lease.anchor.read_bounded(
            len(binding.anchor_bytes)
        ) == binding.anchor_bytes
        assert lease.anchor_identity.size == len(binding.anchor_bytes)
        assert lease.evidence()["anchor_namespace"] == (
            ".stv3-release-install-<run_root_sha256>.lease"
        )
        assert lease.evidence()[
            "divergent_unlocked_reserved_anchor_repaired_under_lock"
        ] is True
        assert lease.evidence()["anchor_identity"]["size"] == len(
            binding.anchor_bytes
        )
        lease.assert_held()
    finally:
        lease.release()
    assert binding.anchor_path.read_bytes() == binding.anchor_bytes
    assert binding.anchor_path.stat().st_size == len(binding.anchor_bytes)


def test_phase_c_stale_anchor_repair_failure_unlocks_without_unlink(
    tmp_path,
    monkeypatch,
):
    run_root = tmp_path / "phase-c-stale-repair-failure"
    binding = dry_run_module._process_lease_binding(run_root)
    stale = b"stale-repair-failure"
    binding.anchor_path.write_bytes(stale)
    original_repair = dry_run_module._repair_process_lease_anchor_while_locked
    state = {"locked": False, "repair_while_locked": False}
    original_lock = dry_run_module._lock_process_lease_descriptor
    original_unlock = dry_run_module._unlock_process_lease_descriptor

    def tracked_lock(descriptor):
        original_lock(descriptor)
        state["locked"] = True

    def tracked_repair(**kwargs):
        state["repair_while_locked"] = state["locked"]
        raise DryRunError("injected stale repair failure")

    def tracked_unlock(descriptor):
        assert state["locked"] is True
        original_unlock(descriptor)
        state["locked"] = False

    monkeypatch.setattr(
        dry_run_module, "_lock_process_lease_descriptor", tracked_lock
    )
    monkeypatch.setattr(
        dry_run_module, "_unlock_process_lease_descriptor", tracked_unlock
    )
    monkeypatch.setattr(
        dry_run_module,
        "_repair_process_lease_anchor_while_locked",
        tracked_repair,
    )
    with pytest.raises(DryRunError, match="injected stale repair failure"):
        dry_run_module._acquire_process_lease(run_root)
    assert state == {"locked": False, "repair_while_locked": True}
    assert binding.anchor_path.read_bytes() == stale
    monkeypatch.setattr(
        dry_run_module,
        "_repair_process_lease_anchor_while_locked",
        original_repair,
    )


@pytest.mark.skipif(
    os.name == "nt",
    reason="Windows preserves the exact non-delete-shared lease anchor",
)
def test_phase_c_postlock_failure_removes_anchor_before_unlock(
    tmp_path,
    monkeypatch,
):
    run_root = tmp_path / "phase-c-postlock-cleanup-order"
    state = {"locked": False}
    original_lock = dry_run_module._lock_process_lease_descriptor
    original_unlock = dry_run_module._unlock_process_lease_descriptor
    original_remove = dry_run_module.DirectoryAuthority.remove_owned_tree

    def tracked_lock(descriptor):
        original_lock(descriptor)
        state["locked"] = True

    def tracked_unlock(descriptor):
        assert state["removed"] is True
        original_unlock(descriptor)
        state["locked"] = False

    def tracked_remove(self, *args, **kwargs):
        assert state["locked"] is True
        state["removed"] = True
        return original_remove(self, *args, **kwargs)

    state["removed"] = False
    monkeypatch.setattr(
        dry_run_module, "_lock_process_lease_descriptor", tracked_lock
    )
    monkeypatch.setattr(
        dry_run_module, "_unlock_process_lease_descriptor", tracked_unlock
    )
    monkeypatch.setattr(
        dry_run_module.DirectoryAuthority, "remove_owned_tree", tracked_remove
    )
    monkeypatch.setattr(
        dry_run_module._ExclusiveProcessLease,
        "assert_held",
        lambda self: (_ for _ in ()).throw(DryRunError("post-lock failure")),
    )
    with pytest.raises(DryRunError, match="post-lock failure"):
        dry_run_module._acquire_process_lease(run_root)
    assert state == {"locked": False, "removed": True}


@pytest.mark.skipif(os.name != "nt", reason="Windows subprocess lease race")
def test_phase_c_real_locked_anchor_is_not_unlinked_or_replaced(
    tmp_path,
):
    run_root = tmp_path / "phase-c-real-locked-anchor"
    binding = dry_run_module._process_lease_binding(run_root)
    binding.anchor_path.write_bytes(binding.anchor_bytes)
    before = binding.anchor_path.stat()
    child = _exclusive_lease_holder(run_root, "clean")
    try:
        with pytest.raises(DryRunError, match="process lease is already held"):
            dry_run_module._acquire_process_lease(run_root)
        during = binding.anchor_path.stat()
        assert (during.st_dev, during.st_ino) == (before.st_dev, before.st_ino)
        with pytest.raises(DryRunError, match="process lease is already held"):
            dry_run_module._acquire_process_lease(run_root)
    finally:
        _finish_exclusive_lease_holder(child, expected_returncode=0)
    lease = dry_run_module._acquire_process_lease(run_root)
    lease.release()


@pytest.mark.skipif(os.name == "nt", reason="POSIX descriptor mode proof")
def test_phase_c_failed_fchmod_cleans_new_lease_anchor(tmp_path, monkeypatch):
    run_root = tmp_path / "phase-c-fchmod-failure"
    binding = dry_run_module._process_lease_binding(run_root)
    monkeypatch.setattr(
        os,
        "fchmod",
        lambda *args, **kwargs: (_ for _ in ()).throw(OSError(errno.EIO, "injected")),
    )
    with pytest.raises(DryRunError, match="process lease acquisition failed"):
        dry_run_module._acquire_process_lease(run_root)
    assert not binding.anchor_path.exists()


def test_phase_c_postcreate_error_reopens_exact_anchor_before_cleanup(
    tmp_path,
    monkeypatch,
):
    run_root = tmp_path / "phase-c-postcreate-anchor"
    binding = dry_run_module._process_lease_binding(run_root)
    original_create = dry_run_module.DirectoryAuthority.create_file

    def create_then_report(authority, relative, *, mode=0o600):
        bound = original_create(authority, relative, mode=mode)
        identity = bound.identity()
        bound.close()
        raise dry_run_module.AuthorityPostCreateError(
            "injected postcreate",
            created_identity=identity,
        )

    monkeypatch.setattr(
        dry_run_module.DirectoryAuthority, "create_file", create_then_report
    )
    with pytest.raises(DryRunError, match="process lease acquisition failed"):
        dry_run_module._acquire_process_lease(run_root)
    if os.name == "nt":
        assert binding.anchor_path.is_file()
        monkeypatch.undo()
        recovered = dry_run_module._acquire_process_lease(run_root)
        recovered.release()
        assert binding.anchor_path.read_bytes() == binding.anchor_bytes
    else:
        assert not binding.anchor_path.exists()


def test_phase_c_postcreate_replacement_is_never_deleted(tmp_path, monkeypatch):
    run_root = tmp_path / "phase-c-postcreate-replacement"
    binding = dry_run_module._process_lease_binding(run_root)
    original_create = dry_run_module.DirectoryAuthority.create_file
    replacement_bytes = b"replacement"

    def replace_then_report(authority, relative, *, mode=0o600):
        bound = original_create(authority, relative, mode=mode)
        created_identity = bound.identity()
        bound.close()
        path = binding.anchor_path
        path.unlink()
        path.write_bytes(replacement_bytes)
        raise dry_run_module.AuthorityPostCreateError(
            "injected postcreate",
            created_identity=created_identity,
        )

    monkeypatch.setattr(
        dry_run_module.DirectoryAuthority, "create_file", replace_then_report
    )
    with pytest.raises(
        DryRunError, match="process lease acquisition cleanup failed"
    ):
        dry_run_module._acquire_process_lease(run_root)
    assert binding.anchor_path.read_bytes() == replacement_bytes


def test_phase_c_lease_cleanup_failure_is_fixed_and_keeps_replacement_safe(
    tmp_path: Path,
    monkeypatch,
):
    run_root = tmp_path / "phase-c-cleanup-failure"
    binding = dry_run_module._process_lease_binding(run_root)
    monkeypatch.setattr(
        dry_run_module.BoundFile,
        "write_all",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            dry_run_module.AuthorityError("secret injected primary")
        ),
    )
    original_close = dry_run_module.DirectoryAuthority.close

    def close_then_report(authority):
        original_close(authority)
        raise dry_run_module.AuthorityError("secret injected cleanup")

    monkeypatch.setattr(
        dry_run_module.DirectoryAuthority,
        "close",
        close_then_report,
    )
    with pytest.raises(
        DryRunError, match="process lease acquisition cleanup failed"
    ) as captured:
        dry_run_module._acquire_process_lease(run_root)
    assert "secret" not in str(captured.value)
    assert isinstance(captured.value.__cause__, BaseExceptionGroup)
    assert os.path.normcase(str(binding.anchor_path)) not in (
        dry_run_module._PROCESS_LEASE_OWNED_PATHS
    )


def test_phase_c_unlock_failure_with_successful_close_releases_process_claim(
    tmp_path,
    monkeypatch,
):
    run_root = tmp_path / "phase-c-release-failure"
    lease = dry_run_module._acquire_process_lease(run_root)
    original_unlock = dry_run_module._unlock_process_lease_descriptor
    monkeypatch.setattr(
        dry_run_module,
        "_unlock_process_lease_descriptor",
        lambda descriptor: (_ for _ in ()).throw(OSError(errno.EIO, "injected")),
    )
    with pytest.raises(DryRunError, match="process lease release failed"):
        lease.release()
    monkeypatch.setattr(
        dry_run_module,
        "_unlock_process_lease_descriptor",
        original_unlock,
    )
    second = dry_run_module._acquire_process_lease(run_root)
    second.release()


def test_phase_c_unlock_and_stream_close_failure_retain_process_claim(
    tmp_path,
    monkeypatch,
):
    run_root = tmp_path / "phase-c-release-double-failure"
    lease = dry_run_module._acquire_process_lease(run_root)

    class FailingCloseStream:
        def __init__(self, wrapped):
            self.wrapped = wrapped

        @property
        def closed(self):
            return self.wrapped.closed

        def fileno(self):
            return self.wrapped.fileno()

        def close(self):
            raise OSError(errno.EIO, "close")

    underlying = lease.lock_stream
    lease.lock_stream = FailingCloseStream(underlying)
    monkeypatch.setattr(
        dry_run_module,
        "_unlock_process_lease_descriptor",
        lambda descriptor: (_ for _ in ()).throw(OSError(errno.EIO, "unlock")),
    )
    with pytest.raises(DryRunError, match="process lease release failed"):
        lease.release()
    with pytest.raises(DryRunError, match="process lease is already held"):
        dry_run_module._acquire_process_lease(run_root)
    underlying.close()


def test_phase_c_nonlock_close_failure_reports_but_releases_process_claim(
    tmp_path,
    monkeypatch,
):
    run_root = tmp_path / "phase-c-parent-close-failure"
    lease = dry_run_module._acquire_process_lease(run_root)
    original_close = dry_run_module.DirectoryAuthority.close
    target_parent = lease.parent

    def fail_parent(authority):
        if authority is target_parent:
            original_close(authority)
            raise dry_run_module.AuthorityError("injected parent close")
        return original_close(authority)

    monkeypatch.setattr(dry_run_module.DirectoryAuthority, "close", fail_parent)
    with pytest.raises(DryRunError, match="process lease release failed"):
        lease.release()
    monkeypatch.setattr(
        dry_run_module.DirectoryAuthority, "close", original_close
    )
    second = dry_run_module._acquire_process_lease(run_root)
    second.release()


def test_phase_c_preexisting_lease_anchor_is_never_removed_on_lock_failure(
    tmp_path: Path,
    monkeypatch,
):
    run_root = tmp_path / "phase-c-preexisting-anchor"
    binding = dry_run_module._process_lease_binding(run_root)
    first = dry_run_module._acquire_process_lease(run_root)
    anchor_path = first.anchor_path
    anchor_bytes = first.anchor.read_bounded(len(binding.anchor_bytes))
    assert anchor_bytes == binding.anchor_bytes
    first.release()
    identity = anchor_path.stat()
    monkeypatch.setattr(
        dry_run_module,
        "_lock_process_lease_descriptor",
        lambda descriptor: (_ for _ in ()).throw(OSError(errno.EIO, "injected")),
    )
    with pytest.raises(DryRunError, match="process lease acquisition failed"):
        dry_run_module._acquire_process_lease(run_root)
    after = anchor_path.stat()
    assert anchor_path.read_bytes() == anchor_bytes
    assert (after.st_dev, after.st_ino) == (identity.st_dev, identity.st_ino)


def test_phase_c_context_bootstrap_ignores_owned_lexical_path_poison(
    tmp_path: Path,
    monkeypatch,
):
    run_root = tmp_path / "phase-c-lexical-poison"
    lease = dry_run_module._acquire_process_lease(run_root)
    context = None
    try:
        monkeypatch.setattr(
            dry_run_module,
            "_allowed_write_roots",
            lambda: (_ for _ in ()).throw(AssertionError("lexical roots reopened")),
        )
        monkeypatch.setattr(
            dry_run_module,
            "_plain_existing_directory",
            lambda *args, **kwargs: (_ for _ in ()).throw(
                AssertionError("lexical directory reopened")
            ),
        )
        for name in ("lstat", "resolve", "is_symlink"):
            monkeypatch.setattr(
                Path,
                name,
                lambda *args, _name=name, **kwargs: (_ for _ in ()).throw(
                    AssertionError(f"owned lexical {_name} used")
                ),
            )
        context = dry_run_module._open_authority_run_context(
            run_root,
            existing=False,
            process_lease=lease,
        )
        lease.assert_held()
        context.reprove()
    finally:
        if context is not None:
            context.close()
        lease.release()


def test_phase_c_lease_and_context_ast_ban_owned_lexical_reopen():
    source = Path(dry_run_module.__file__).read_text(encoding="utf-8")
    module = ast.parse(source)
    functions = {
        node.name: node for node in module.body if isinstance(node, ast.FunctionDef)
    }
    acquire_calls = {
        (
            node.func.value.id if isinstance(node.func.value, ast.Name) else "",
            node.func.attr,
        )
        for node in ast.walk(functions["_acquire_process_lease"])
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
    }
    assert ("os", "open") not in acquire_calls
    assert ("os", "chmod") not in acquire_calls
    assert ("os", "fsync") not in acquire_calls
    context_source = ast.get_source_segment(
        source, functions["_open_authority_run_context"]
    )
    assert "DirectoryAuthority.open_absolute" not in context_source
    assert "_allowed_write_roots" not in context_source
    assert "_plain_existing_directory" not in context_source
    assert ".lstat(" not in context_source


@pytest.mark.parametrize("route", ("write", "member", "spool"))
def test_phase_c_all_output_routes_prove_typed_postpublication_cleanup_absence(
    tmp_path: Path,
    monkeypatch,
    route: str,
):
    run_root = tmp_path / f"phase-c-postpublication-{route}"
    with _authority_run_root(run_root) as (_root, context):
        if route == "write":
            destination = run_root / "write.json"
            relative = context.relative(destination)

            def cleaned(self, received, *args, **kwargs):
                assert received == relative
                raise dry_run_module.OutputAuthorityPostPublicationError(
                    cleanup_succeeded=True,
                    destination=relative,
                    published_identity=context.root_identity,
                    stream=SimpleNamespace(),
                )

            monkeypatch.setattr(
                dry_run_module.ReleaseInstallOutputAuthority,
                "write_bytes_noreplace",
                cleaned,
            )
            invoke = lambda: dry_run_module._write_exclusive(
                destination,
                b"{}\n",
                mode=0o644,
                run_root=run_root,
                authority_context=context,
            )
        elif route == "member":
            destination = run_root / "member.bin"
            relative = context.relative(destination)
            payload = b"member"
            record = dry_run_module.FileRecord(
                path="member.bin",
                size=len(payload),
                sha256=hashlib.sha256(payload).hexdigest(),
                mode=0o644,
            )

            def cleaned(self, received, *args, **kwargs):
                assert received == relative
                raise dry_run_module.OutputAuthorityPostPublicationError(
                    cleanup_succeeded=True,
                    destination=relative,
                    published_identity=context.root_identity,
                    stream=SimpleNamespace(),
                )

            monkeypatch.setattr(
                dry_run_module.ReleaseInstallOutputAuthority,
                "stream_noreplace",
                cleaned,
            )
            invoke = lambda: dry_run_module._write_member_exclusive(
                destination,
                io.BytesIO(payload),
                record=record,
                run_root=run_root,
                authority_context=context,
                label="archive member",
            )
        else:
            source = tmp_path / "phase-c-source.bin"
            source.write_bytes(b"spool")
            destination = run_root / "spool" / "source.bin"
            relative = context.relative(destination)

            def cleaned(self, received, *args, **kwargs):
                assert received == relative
                raise dry_run_module.OutputAuthorityPostPublicationError(
                    cleanup_succeeded=True,
                    destination=relative,
                    published_identity=context.root_identity,
                    stream=SimpleNamespace(),
                )

            monkeypatch.setattr(
                dry_run_module.ReleaseInstallOutputAuthority,
                "stream_noreplace",
                cleaned,
            )
            invoke = lambda: dry_run_module._spool_input(
                source,
                destination,
                label="phase-c source",
                maximum_bytes=64,
                run_root=run_root,
                authority_context=context,
                fault_injector=None,
                fault_prefix="input.phase_c",
            )

        with pytest.raises(
            DryRunError, match="failed closed after exact post-publication cleanup"
        ):
            invoke()
        assert not context.root.exists(relative)
        context.reprove()


def test_phase_c_failed_postpublication_cleanup_preserves_replacement(
    tmp_path: Path,
    monkeypatch,
):
    run_root = tmp_path / "phase-c-postpublication-replacement"
    with _authority_run_root(run_root) as (_root, context):
        destination = run_root / "replacement.json"
        relative = context.relative(destination)
        replacement = context.output.write_bytes_noreplace(
            relative,
            b"replacement",
            mode=0o644,
            forbidden_markers=(),
        )
        published = dry_run_module.NodeIdentity(
            backend=replacement.identity.backend,
            volume=replacement.identity.volume,
            file_id=f"{replacement.identity.file_id}-published",
            kind="regular_file",
            link_count=1,
            size=3,
        )

        def failed_cleanup(self, received, *args, **kwargs):
            assert received == relative
            raise dry_run_module.OutputAuthorityPostPublicationError(
                cleanup_succeeded=False,
                destination=relative,
                published_identity=published,
                stream=SimpleNamespace(),
            )

        monkeypatch.setattr(
            dry_run_module.ReleaseInstallOutputAuthority,
            "write_bytes_noreplace",
            failed_cleanup,
        )
        with pytest.raises(DryRunError, match="replacement was preserved"):
            dry_run_module._write_exclusive(
                destination,
                b"new",
                mode=0o644,
                run_root=run_root,
                authority_context=context,
            )
        observed = context.root.entry_identity(relative)
        assert observed.same_object(replacement.identity)
        assert context.output.read(relative, maximum_bytes=32) == b"replacement"


def test_cli_emits_exact_blocker_namespaces_and_aliases(
    tmp_path: Path,
    monkeypatch,
    capsys,
):
    namespaces = {
        "external_installation": ["human_deploy_approval_not_granted"],
        "local_simulation": [
            "descriptor_relative_output_authority_not_proven"
        ],
        "supervisor_installation": ["supervisor_options_roundtrip_not_proven"],
    }
    flattened = sorted(
        f"{namespace}:{code}"
        for namespace, codes in namespaces.items()
        for code in codes
    )
    report_path = tmp_path / "report.json"
    report = {
        "blocker_namespaces": namespaces,
        "installation_preflight_blockers": flattened,
        "local_acceptance_blockers": namespaces["local_simulation"],
        "local_acceptance_blockers_closed": [
            "ancestor_and_final_output_path_race_exclusion_not_proven"
        ],
        "local_simulation_passed": False,
        "simulation_completed": True,
        "atomic_noreplace_publication_proven": True,
        "supervisor_installation_blockers": namespaces["supervisor_installation"],
        "supervisor_options_roundtrip_proven": False,
        "unresolved_external_gates": namespaces["external_installation"],
    }
    monkeypatch.setattr(
        dry_run_cli,
        "_arguments",
        lambda: SimpleNamespace(
            candidate_evidence=tmp_path / "evidence.json",
            rollback_integration_archive=tmp_path / "integration.zip",
            rollback_worker_archive=tmp_path / "worker.tar.gz",
            sanitized_options_snapshot=tmp_path / "options.json",
            run_root=tmp_path / "run",
        ),
    )
    monkeypatch.setattr(
        dry_run_cli,
        "execute_release_install_rollback_dry_run",
        lambda **_arguments: SimpleNamespace(
            report=report,
            report_path=report_path,
            report_sha256="0" * 64,
        ),
    )
    assert dry_run_cli.main() == 0
    emitted = json.loads(capsys.readouterr().out)
    assert emitted == {
        "atomic_noreplace_publication_proven": True,
        "blocker_namespaces": namespaces,
        "deployment_performed": False,
        "destination_absence_prechecked": True,
        "installation_preflight_blockers": flattened,
        "live_paths_touched": False,
        "local_acceptance_blockers": namespaces["local_simulation"],
        "local_acceptance_blockers_closed": [
            "ancestor_and_final_output_path_race_exclusion_not_proven"
        ],
        "local_simulation_passed": False,
        "network_performed": False,
        "real_installation_ready": False,
        "report": str(report_path),
        "report_sha256": "0" * 64,
        "simulation_completed": True,
        "supervisor_installation_blockers": namespaces["supervisor_installation"],
        "supervisor_options_roundtrip_proven": False,
        "unresolved_external_gates": namespaces["external_installation"],
    }


@pytest.mark.parametrize("point", sorted(EXPECTED_EXECUTION_FAULT_POINTS))
def test_every_journal_rename_and_directory_sync_boundary_recovers_idempotently(
    tmp_path: Path,
    release_inputs,
    point: str,
):
    class InjectedFailure(RuntimeError):
        pass

    def fail(actual: str) -> None:
        if actual == point:
            raise InjectedFailure(actual)

    run_root = tmp_path / "run"
    with pytest.raises(InjectedFailure, match=re.escape(point)):
        _run(release_inputs, run_root, fault_injector=fail)
    first = recover_release_install_dry_run(run_root)
    second = recover_release_install_dry_run(run_root)
    assert first["outcome"] == _expected_recovery(point)
    assert second["outcome"] == first["outcome"]
    assert second["deployment_performed"] is False
    assert second["network_performed"] is False
    assert second["live_paths_touched"] is False


def _assert_active_generation(run_root: Path, outcome: str) -> None:
    state = json.loads((run_root / "dry_run_state.json").read_text(encoding="utf-8"))
    generation_name = "baseline" if outcome == "baseline" else "candidate"
    generation = state[generation_name]
    for component in ("integration", "worker"):
        root = run_root / "install" / f"{component}.active"
        expected_files = {
            item["path"]: item for item in generation[component]["files"]
        }
        actual_files = {
            path.relative_to(root).as_posix(): path
            for path in root.rglob("*")
            if path.is_file()
        }
        assert set(actual_files) == set(expected_files)
        for relative, expected in expected_files.items():
            path = actual_files[relative]
            assert path.stat().st_size == expected["size"]
            assert _sha(path) == expected["sha256"]
    integration_manifest = json.loads(
        (run_root / "install" / "integration.active" / "manifest.json")
        .read_text(encoding="utf-8")
    )
    worker_config = (
        run_root / "install" / "worker.active" / "config.yaml"
    ).read_text(encoding="utf-8")
    expected_versions = (
        ("1.11.0", "1.14.0")
        if outcome == "baseline"
        else ("1.13.0", "1.16.0")
    )
    assert integration_manifest["version"] == expected_versions[0]
    assert f'version: "{expected_versions[1]}"' in worker_config.splitlines()


@pytest.mark.parametrize("point", sorted(EXPECTED_EXECUTION_FAULT_POINTS))
def test_abrupt_process_boundaries_recover_one_coherent_pair(
    tmp_path: Path,
    release_inputs,
    point: str,
):
    candidate, rollback_integration, rollback_worker, options, policy = release_inputs
    run_root = tmp_path / "run"
    environment = dict(os.environ)
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    environment["PYTHONPATH"] = str(ROOT)
    completed = subprocess.run(
        [
            sys.executable,
            str(CRASH_HELPER),
            str(candidate.evidence_path),
            str(rollback_integration),
            str(rollback_worker),
            str(options),
            str(run_root),
            point,
            policy.integration_version,
            policy.integration_sha256,
            policy.worker_version,
            policy.worker_sha256,
        ],
        cwd=ROOT,
        env=environment,
        capture_output=True,
        timeout=30,
    )
    assert completed.returncode == 73, completed.stderr.decode(errors="replace")
    first = recover_release_install_dry_run(run_root)
    _assert_active_generation(run_root, first["outcome"])
    second = recover_release_install_dry_run(run_root)
    _assert_active_generation(run_root, second["outcome"])
    assert first["outcome"] == _expected_recovery(point)
    assert second == first


class _RecoverySeedFailure(RuntimeError):
    pass


def _recovery_seed_point(recovery_point: str) -> str:
    if recovery_point.startswith("journal.upgrade_recovered."):
        return "journal.upgrade_committed.after_pending_fsync_before_publish"
    if recovery_point.startswith("journal.rollback_recovered."):
        return "rename.rollback_integration_retain.before_rename"
    return "upgrade.before_commit_decision"


def _seed_recovery(inputs, run_root: Path, recovery_point: str) -> None:
    seed_point = _recovery_seed_point(recovery_point)

    def stop(actual: str) -> None:
        if actual == seed_point:
            raise _RecoverySeedFailure(actual)

    with pytest.raises(_RecoverySeedFailure, match=re.escape(seed_point)):
        _run(inputs, run_root, fault_injector=stop)


@pytest.mark.parametrize("point", sorted(EXPECTED_RECOVERY_FAULT_POINTS))
def test_every_recovery_boundary_has_exception_and_abrupt_idempotent_coverage(
    tmp_path: Path,
    release_inputs,
    point: str,
):
    class InjectedRecoveryFailure(RuntimeError):
        pass

    def fail(actual: str) -> None:
        if actual == point:
            raise InjectedRecoveryFailure(actual)

    expected_outcome = (
        "candidate" if point.startswith("journal.upgrade_recovered.") else "baseline"
    )
    exception_root = tmp_path / "exception-run"
    _seed_recovery(release_inputs, exception_root, point)
    with pytest.raises(InjectedRecoveryFailure, match=re.escape(point)):
        recover_release_install_dry_run(exception_root, fault_injector=fail)
    first = recover_release_install_dry_run(exception_root)
    _assert_active_generation(exception_root, first["outcome"])
    second = recover_release_install_dry_run(exception_root)
    _assert_active_generation(exception_root, second["outcome"])
    assert first["outcome"] == expected_outcome
    assert second == first

    abrupt_root = tmp_path / "abrupt-run"
    _seed_recovery(release_inputs, abrupt_root, point)
    environment = dict(os.environ)
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    environment["PYTHONPATH"] = str(ROOT)
    completed = subprocess.run(
        [sys.executable, str(CRASH_HELPER), "recover", str(abrupt_root), point],
        cwd=ROOT,
        env=environment,
        capture_output=True,
        timeout=30,
    )
    assert completed.returncode == 73, completed.stderr.decode(errors="replace")
    abrupt_first = recover_release_install_dry_run(abrupt_root)
    _assert_active_generation(abrupt_root, abrupt_first["outcome"])
    abrupt_second = recover_release_install_dry_run(abrupt_root)
    _assert_active_generation(abrupt_root, abrupt_second["outcome"])
    assert abrupt_first["outcome"] == expected_outcome
    assert abrupt_second == abrupt_first


def test_existing_or_outside_run_root_is_rejected_without_mutation(
    tmp_path: Path,
    release_inputs,
):
    existing = tmp_path / "existing"
    existing.mkdir()
    with pytest.raises(DryRunError, match="new and absent"):
        _run(release_inputs, existing)
    assert tuple(existing.iterdir()) == ()

    outside = ROOT / f"forbidden-release-dry-run-{tmp_path.name}"
    assert not outside.exists()
    with pytest.raises(DryRunError, match="outside temp"):
        _run(release_inputs, outside)
    assert not outside.exists()


def test_empty_forbidden_legacy_directory_leaves_only_owned_spools(
    tmp_path: Path,
    monkeypatch,
):
    candidate = _candidate(tmp_path)
    rollback_integration = _legacy_integration(tmp_path, forbidden_directory=True)
    rollback_worker = _legacy_worker(tmp_path)
    options = _options(tmp_path)
    policy = LegacyRollbackPolicy(
        "1.11.0", _sha(rollback_integration), "1.14.0", _sha(rollback_worker)
    )
    run_root = tmp_path / "run"
    routes = []
    original_record = dry_run_module._record_bound_archive_route

    def record_route(context, route):
        routes.append(route)
        return original_record(context, route)

    monkeypatch.setattr(
        dry_run_module, "_record_bound_archive_route", record_route
    )
    with pytest.raises(DryRunError, match="forbidden directory"):
        execute_release_install_rollback_dry_run(
            candidate_evidence=candidate.evidence_path,
            rollback_integration_archive=rollback_integration,
            rollback_worker_archive=rollback_worker,
            options_snapshot=options,
            run_root=run_root,
            rollback_policy=policy,
        )
    assert run_root.exists()
    assert (run_root / "spool").is_dir()
    assert not (run_root / "dry_run_state.json").exists()
    assert "rollback_integration_inspect" not in routes
    assert not any(
        (run_root / name).exists()
        for name in ("journal", "stage", "install", "backup", "retained")
    )


def _inputs_with_legacy_empty_directories(base: Path):
    candidate = _candidate(base)
    rollback_integration = _legacy_integration(
        base,
        empty_directories=("durable", "durable/empty"),
    )
    rollback_worker = _legacy_worker(
        base,
        empty_directories=("runtime", "runtime/empty"),
    )
    options = _options(base)
    policy = LegacyRollbackPolicy(
        "1.11.0", _sha(rollback_integration), "1.14.0", _sha(rollback_worker)
    )
    return candidate, rollback_integration, rollback_worker, options, policy


def _relative_directory_set(root: Path) -> set[str]:
    return {
        path.relative_to(root).as_posix()
        for path in root.rglob("*")
        if path.is_dir()
    }


def test_legacy_empty_directories_are_sealed_materialized_and_fingerprinted(
    tmp_path: Path,
):
    inputs = _inputs_with_legacy_empty_directories(tmp_path)
    run_root = tmp_path / "run"
    result = _run(inputs, run_root)
    state = json.loads((run_root / "dry_run_state.json").read_text(encoding="utf-8"))

    expected = {
        "integration": ["durable", "durable/empty"],
        "worker": ["runtime", "runtime/empty"],
    }
    active_roots = {
        "integration": run_root / "install" / "integration.active",
        "worker": run_root / "install" / "worker.active",
    }
    for component, directories in expected.items():
        assert state["baseline"][component]["directories"] == directories
        assert state["baseline"][component]["directory_count"] == len(directories)
        assert _relative_directory_set(active_roots[component]) == set(directories)
        fingerprint = result.report["baseline"][component]["tree"]
        assert fingerprint["directory_count"] == len(directories)
        assert fingerprint["empty_directories"] == [directories[-1]]
        assert fingerprint["explicit_empty_directories_proven"] is True

    integration_tree = dry_run_module.TreeRecord.from_mapping(
        state["baseline"]["integration"]
    )
    (active_roots["integration"] / "durable" / "empty").rmdir()
    with pytest.raises(DryRunError, match="directory set differs"):
        dry_run_module._fingerprint_tree(active_roots["integration"], integration_tree)


def test_legacy_empty_directories_survive_interrupted_upgrade_recovery(tmp_path: Path):
    inputs = _inputs_with_legacy_empty_directories(tmp_path)
    run_root = tmp_path / "run"

    class InjectedFailure(RuntimeError):
        pass

    def fail(point: str) -> None:
        if point == "upgrade.before_commit_decision":
            raise InjectedFailure(point)

    with pytest.raises(InjectedFailure, match="upgrade.before_commit_decision"):
        _run(inputs, run_root, fault_injector=fail)
    recovered = recover_release_install_dry_run(run_root)
    assert recovered["outcome"] == "baseline"
    assert _relative_directory_set(
        run_root / "install" / "integration.active"
    ) == {"durable", "durable/empty"}
    assert _relative_directory_set(
        run_root / "install" / "worker.active"
    ) == {"runtime", "runtime/empty"}
    assert recover_release_install_dry_run(run_root) == recovered


def test_tree_seal_binds_directory_set_and_rejects_ambiguous_paths():
    file_record = dry_run_module.FileRecord("file.txt", 1, "0" * 64, 0o644)
    without_empty = dry_run_module.TreeRecord.create("root", (file_record,))
    with_empty = dry_run_module.TreeRecord.create(
        "root", (file_record,), ("durable", "durable/empty")
    )
    assert without_empty.content_sha256 != with_empty.content_sha256
    assert with_empty.directories == ("durable", "durable/empty")

    tampered = with_empty.to_mapping()
    tampered["directories"] = ["durable"]
    tampered["directory_count"] = 1
    with pytest.raises(DryRunError, match="hash, count or size differs"):
        dry_run_module.TreeRecord.from_mapping(tampered)
    with pytest.raises(DryRunError, match="both a file and a directory"):
        dry_run_module.TreeRecord.create("root", (file_record,), ("file.txt",))
    with pytest.raises(DryRunError, match="duplicate directory"):
        dry_run_module.TreeRecord.create("root", (file_record,), ("empty", "empty"))
    with pytest.raises(DryRunError, match="sequence of paths"):
        dry_run_module.TreeRecord.create("root", (file_record,), "empty")


@pytest.mark.parametrize(
    ("body", "record_size", "record_sha256"),
    (
        (b"abcd", 3, hashlib.sha256(b"abc").hexdigest()),
        (b"abc", 3, "0" * 64),
        (
            b"-----BEGIN PRIVATE KEY-----\nsecret",
            len(b"-----BEGIN PRIVATE KEY-----\nsecret"),
            hashlib.sha256(b"-----BEGIN PRIVATE KEY-----\nsecret").hexdigest(),
        ),
    ),
    ids=("overrun", "hash", "private-marker"),
)
def test_archive_validation_failure_never_creates_destination(
    tmp_path: Path,
    body: bytes,
    record_size: int,
    record_sha256: str,
):
    with _authority_run_root(tmp_path / "run") as (run_root, context):
        destination = run_root / "member.bin"
        record = dry_run_module.FileRecord(
            "member.bin", record_size, record_sha256, 0o644
        )
        with pytest.raises(DryRunError, match="failed closed before publication"):
            dry_run_module._write_member_exclusive(
                destination,
                io.BytesIO(body),
                record=record,
                run_root=run_root,
                authority_context=context,
                label="adversarial archive",
            )
        assert not destination.exists()


def test_worker_options_and_schema_must_match_exactly(tmp_path: Path):
    inputs = _inputs(tmp_path, schema_drift=True)
    run_root = tmp_path / "run"
    with pytest.raises(DryRunError, match="options and schema keysets"):
        _run(inputs, run_root)
    assert run_root.exists()
    assert not (run_root / "dry_run_state.json").exists()


def _rewrite_candidate_worker_mode(candidate) -> None:
    source_path = candidate.worker_archive
    records: list[tuple[tarfile.TarInfo, bytes | None]] = []
    with tarfile.open(source_path, "r:gz") as source:
        for original in source.getmembers():
            member = copy.copy(original)
            data = None
            if member.isfile():
                stream = source.extractfile(original)
                assert stream is not None
                data = stream.read()
            if member.name.endswith("/run.sh"):
                member.mode = 0o644
            records.append((member, data))
    output = io.BytesIO()
    with gzip.GzipFile(filename="", mode="wb", fileobj=output, mtime=0) as compressed:
        with tarfile.open(fileobj=compressed, mode="w", format=tarfile.PAX_FORMAT) as target:
            for member, data in records:
                target.addfile(member, None if data is None else io.BytesIO(data))
    source_path.write_bytes(output.getvalue())
    document = json.loads(candidate.evidence_path.read_text(encoding="utf-8"))
    document["archives"]["worker"]["bytes"] = source_path.stat().st_size
    document["archives"]["worker"]["sha256"] = _sha(source_path)
    unsigned = dict(document)
    del unsigned["evidence_sha256"]
    document["evidence_sha256"] = hashlib.sha256(_canonical(unsigned)).hexdigest()
    candidate.evidence_path.write_bytes(_canonical(document) + b"\n")


def test_self_consistent_evidence_cannot_hide_candidate_mode_drift(tmp_path: Path):
    inputs = _inputs(tmp_path)
    _rewrite_candidate_worker_mode(inputs[0])
    run_root = tmp_path / "run"
    with pytest.raises(DryRunError, match="bound archive validation failed closed"):
        _run(inputs, run_root)
    assert run_root.exists()
    assert (run_root / "spool").is_dir()
    assert not (run_root / "dry_run_state.json").exists()


def test_symlinked_run_root_parent_is_rejected_lexically(
    tmp_path: Path,
    release_inputs,
):
    real_parent = tmp_path / "real-parent"
    real_parent.mkdir()
    indirect_parent = tmp_path / "indirect-parent"
    try:
        indirect_parent.symlink_to(real_parent, target_is_directory=True)
    except OSError as error:
        pytest.skip(f"directory symlinks unavailable: {error}")
    with pytest.raises(DryRunError, match="symlink, junction or reparse"):
        _run(release_inputs, indirect_parent / "run")
    assert not (real_parent / "run").exists()


def test_symlinked_input_parent_is_rejected_before_input_open(tmp_path: Path):
    inputs = _inputs(tmp_path / "real")
    indirect = tmp_path / "indirect"
    try:
        indirect.symlink_to(inputs[0].evidence_path.parent, target_is_directory=True)
    except OSError as error:
        pytest.skip(f"directory symlinks unavailable: {error}")
    with pytest.raises(DryRunError, match="symlink, junction or reparse"):
        execute_release_install_rollback_dry_run(
            candidate_evidence=indirect / inputs[0].evidence_path.name,
            rollback_integration_archive=inputs[1],
            rollback_worker_archive=inputs[2],
            options_snapshot=inputs[3],
            run_root=tmp_path / "run",
            rollback_policy=inputs[4],
        )
    assert (tmp_path / "run").is_dir()
    assert tuple((tmp_path / "run").iterdir()) == ()


def test_input_path_swap_during_open_spool_is_detected(tmp_path: Path):
    inputs = _inputs(tmp_path)
    source = inputs[0].worker_archive
    original_sha256 = _sha(source)
    replacement = source.with_name("replacement-worker.tar.gz")
    replacement.write_bytes(b"replacement\n")
    swapped = False

    def swap(point: str) -> None:
        nonlocal swapped
        if point != "input.candidate_worker.after_source_open":
            return
        try:
            os.replace(replacement, source)
        except PermissionError as error:
            pytest.skip(f"open-file replacement unavailable: {error}")
        swapped = True

    run_root = tmp_path / "run"
    with pytest.raises(
        DryRunError,
        match="changed while spooling|could not be bound and spooled",
    ):
        _run(inputs, run_root, fault_injector=swap)
    assert swapped is True
    spool = run_root / "spool" / "candidate" / source.name
    assert not spool.exists()
    assert _sha(source) != original_sha256


def test_all_verification_after_capture_uses_only_spooled_bytes(tmp_path: Path):
    inputs = _inputs(tmp_path)
    source = inputs[0].worker_archive
    original_sha256 = _sha(source)
    replacement = source.with_name("replacement-after-spool.tar.gz")
    replacement.write_bytes(b"replacement-after-spool\n")

    def swap(point: str) -> None:
        if point == "inputs.after_spooled":
            os.replace(replacement, source)

    run_root = tmp_path / "run"
    with pytest.raises(DryRunError, match="candidate worker archive changed"):
        _run(inputs, run_root, fault_injector=swap)
    spool = run_root / "spool" / "candidate" / source.name
    assert _sha(spool) == original_sha256
    assert _sha(source) != original_sha256
    journal = sorted((run_root / "journal").glob("*.json"))
    assert json.loads(journal[-1].read_text(encoding="utf-8"))["event"] == "rollback_completed"


def test_duplicate_json_keys_are_rejected_before_semantic_verification(tmp_path: Path):
    inputs = _inputs(tmp_path)
    evidence = inputs[0].evidence_path
    source = evidence.read_text(encoding="utf-8")
    closing = source.rfind("}")
    evidence.write_text(
        source[:closing] + ',"integration_version":"1.13.0"' + source[closing:],
        encoding="utf-8",
    )
    with pytest.raises(DryRunError, match="duplicate JSON key"):
        _run(inputs, tmp_path / "run")


def test_duplicate_options_json_key_is_rejected(tmp_path: Path):
    inputs = _inputs(tmp_path)
    source = inputs[3].read_text(encoding="utf-8")
    inputs[3].write_text(
        source.replace('"cpu_threads": 2', '"cpu_threads": 2, "cpu_threads": 2'),
        encoding="utf-8",
    )
    with pytest.raises(DryRunError, match="duplicate JSON key"):
        _run(inputs, tmp_path / "run")


@pytest.mark.parametrize(
    ("name", "value"),
    (
        ("cpu_threads", True),
        ("cpu_threads", 3),
        ("reserve_ha_core", 1),
        ("queue_size", 17),
        ("log_level", "TRACE"),
        ("postgis_port", 0),
        ("postgis_projection_enabled", 1),
        ("postgis_sslmode", "prefer"),
        ("opera_raw_retention_authorized", 1),
    ),
)
def test_sanitized_option_types_ranges_and_enums_are_exact(
    tmp_path: Path,
    name: str,
    value: object,
):
    inputs = _inputs(tmp_path)
    document = json.loads(inputs[3].read_text(encoding="utf-8"))
    document["options"][name] = value
    inputs[3].write_text(json.dumps(document), encoding="utf-8")
    with pytest.raises(DryRunError, match=f"invalid for {name}"):
        _run(inputs, tmp_path / "run")


def test_worker_first_canary_options_roundtrip_without_report_value_leakage(
    tmp_path: Path,
):
    inputs = _inputs(tmp_path)
    document = json.loads(inputs[3].read_text(encoding="utf-8"))
    private_config_path = "/share/private/stv3-worker-first-canary.json"
    private_config_pin = "a" * 64
    private_authority_pin = "b" * 64
    document["options"].update({
        "runtime_mode": "worker_first_canary",
        "worker_first_config_path": private_config_path,
        "worker_first_config_sha256": private_config_pin,
        "worker_first_authority_sha256": private_authority_pin,
    })
    inputs[3].write_text(json.dumps(document, sort_keys=True), encoding="utf-8")

    result = _run(inputs, tmp_path / "run")

    assert result.report["supervisor_options_roundtrip_proven"] is True
    assert result.report["supervisor_installation_blockers"] == []
    report_text = result.report_path.read_text(encoding="utf-8")
    for private_value in (
        private_config_path,
        private_config_pin,
        private_authority_pin,
    ):
        assert private_value not in report_text


def test_worker_only_snapshot_roundtrips_with_bootstrap_pins_empty(
    tmp_path: Path,
):
    inputs = _inputs(tmp_path)
    document = json.loads(inputs[3].read_text(encoding="utf-8"))
    document["options"]["runtime_mode"] = "worker_only"
    inputs[3].write_text(json.dumps(document, sort_keys=True), encoding="utf-8")

    result = _run(inputs, tmp_path / "run")

    assert result.report["supervisor_options_roundtrip_proven"] is True
    assert result.report["supervisor_installation_blockers"] == []
    compatibility = result.report["options_compatibility"]
    assert compatibility["raw_capture_shadow_selected"] is False
    assert compatibility["raw_capture_target_activation_pins_bound"] is False


def test_raw_capture_shadow_pre_target_snapshot_roundtrips_but_stays_blocked(
    tmp_path: Path,
):
    inputs = _inputs(tmp_path)
    document = json.loads(inputs[3].read_text(encoding="utf-8"))
    document["options"]["runtime_mode"] = "raw_capture_shadow"
    inputs[3].write_text(json.dumps(document, sort_keys=True), encoding="utf-8")

    result = _run(inputs, tmp_path / "run")

    assert result.report["supervisor_options_roundtrip_proven"] is True
    assert result.report["supervisor_installation_blockers"] == [
        "raw_capture_target_activation_pins_absent"
    ]
    compatibility = result.report["options_compatibility"]
    assert compatibility["raw_capture_shadow_selected"] is True
    assert compatibility["raw_capture_target_activation_pins_bound"] is False
    assert compatibility["raw_capture_retention_option_values_reported"] is False
    assert compatibility["raw_capture_retention_option_names"] == [
        "synoptic_raw_retention_terms_accepted",
        "rainviewer_cache_attribution_accepted",
        "kmi_appv4_raw_retention_authorized",
        "opera_raw_retention_authorized",
        "blitzortung_raw_retention_authorized",
    ]
    assert {
        "raw_capture_target_activation_attestation_not_exercised",
        "raw_capture_target_materialization_not_exercised",
    }.issubset(result.report["unresolved_external_gates"])


def test_raw_capture_shadow_bound_pins_roundtrip_without_report_value_leakage(
    tmp_path: Path,
):
    inputs = _inputs(tmp_path)
    document = json.loads(inputs[3].read_text(encoding="utf-8"))
    private_config_path = "/share/private/stv3-raw-capture-shadow.json"
    private_config_pin = "c" * 64
    private_authority_pin = "d" * 64
    document["options"].update({
        "runtime_mode": "raw_capture_shadow",
        "worker_first_config_path": private_config_path,
        "worker_first_config_sha256": private_config_pin,
        "worker_first_authority_sha256": private_authority_pin,
        "kmi_appv4_raw_retention_authorized": True,
        "opera_raw_retention_authorized": True,
    })
    inputs[3].write_text(json.dumps(document, sort_keys=True), encoding="utf-8")

    result = _run(inputs, tmp_path / "run")

    assert result.report["supervisor_options_roundtrip_proven"] is True
    assert result.report["supervisor_installation_blockers"] == []
    compatibility = result.report["options_compatibility"]
    assert compatibility["raw_capture_shadow_selected"] is True
    assert compatibility["raw_capture_target_activation_pins_bound"] is True
    report_text = result.report_path.read_text(encoding="utf-8")
    for private_value in (
        private_config_path,
        private_config_pin,
        private_authority_pin,
    ):
        assert private_value not in report_text


@pytest.mark.parametrize(
    ("updates", "error_pattern"),
    (
        ({"runtime_mode": "canary"}, "invalid for runtime_mode"),
        (
            {
                "runtime_mode": "worker_first_canary",
                "worker_first_config_path": "relative/config.json",
                "worker_first_config_sha256": "a" * 64,
                "worker_first_authority_sha256": "b" * 64,
            },
            "worker-first config path is invalid",
        ),
        (
            {"worker_first_config_sha256": "A" * 64},
            "invalid for worker_first_config_sha256",
        ),
        (
            {"worker_first_authority_sha256": "b" * 64},
            "legacy activation options must leave all worker-first pins empty",
        ),
    ),
)
def test_worker_first_activation_snapshot_fails_closed_without_value_leakage(
    tmp_path: Path,
    updates: dict[str, object],
    error_pattern: str,
):
    inputs = _inputs(tmp_path)
    document = json.loads(inputs[3].read_text(encoding="utf-8"))
    document["options"].update(updates)
    inputs[3].write_text(json.dumps(document, sort_keys=True), encoding="utf-8")

    with pytest.raises(DryRunError, match=error_pattern) as raised:
        _run(inputs, tmp_path / "run")
    error_text = str(raised.value)
    for value in updates.values():
        if isinstance(value, str) and value:
            assert value not in error_text


def test_incomplete_worker_first_activation_snapshot_is_rejected(tmp_path: Path):
    inputs = _inputs(tmp_path)
    document = json.loads(inputs[3].read_text(encoding="utf-8"))
    del document["options"]["worker_first_authority_sha256"]
    inputs[3].write_text(json.dumps(document, sort_keys=True), encoding="utf-8")

    with pytest.raises(DryRunError, match="activation option group is incomplete"):
        _run(inputs, tmp_path / "run")


@pytest.mark.parametrize(
    ("old", "new"),
    (
        ('  runtime_mode: "worker_only"', '  runtime_mode: "worker_first_canary"'),
        (
            '  worker_first_config_path: "str?"',
            '  worker_first_config_path: "password?"',
        ),
    ),
)
def test_candidate_worker_first_config_and_schema_declarations_are_exact(
    tmp_path: Path,
    old: str,
    new: str,
):
    source = _config("1.16.0", candidate=True)
    assert source.count(old) == 1
    files = {
        "config.yaml": source.replace(old, new).encode("utf-8"),
        "__init__.py": b'__version__ = "1.16.0"\n',
        "compose.yaml": b"services:\n  worker:\n    image: local\n",
        "run.sh": b"#!/bin/sh\n",
        "service.py": b"VALUE = 1\n",
    }
    with _authority_run_root(tmp_path / "worker-identity-run") as (
        run_root,
        context,
    ):
        root = run_root / "storm_tracker_worker"
        dry_run_module._mkdir(root, run_root, context)
        records = []
        for name, value in files.items():
            mode = 0o755 if name.endswith(".sh") else 0o644
            dry_run_module._write_exclusive(
                root / name,
                value,
                mode=mode,
                run_root=run_root,
                authority_context=context,
            )
            records.append(
                dry_run_module.FileRecord(
                    name,
                    len(value),
                    hashlib.sha256(value).hexdigest(),
                    mode,
                )
            )
        tree = dry_run_module.TreeRecord.create(
            "storm_tracker_worker", records
        )
        with pytest.raises(
            DryRunError,
            match="worker-first activation options or schema declarations differ",
        ):
            dry_run_module._worker_identity(
                root,
                "1.16.0",
                tree=tree,
                authority_context=context,
                require_worker_first_activation_contract=True,
            )


@pytest.mark.parametrize(
    ("old", "new"),
    (
        (
            "  opera_raw_retention_authorized: false",
            "  opera_raw_retention_authorized: true",
        ),
        (
            '  blitzortung_raw_retention_authorized: "bool"',
            '  blitzortung_raw_retention_authorized: "str?"',
        ),
    ),
)
def test_candidate_raw_retention_declarations_are_exact(
    tmp_path: Path,
    old: str,
    new: str,
):
    source = _config("1.16.0", candidate=True)
    assert source.count(old) == 1
    files = {
        "config.yaml": source.replace(old, new).encode("utf-8"),
        "__init__.py": b'__version__ = "1.16.0"\n',
        "compose.yaml": b"services:\n  worker:\n    image: local\n",
        "run.sh": b"#!/bin/sh\n",
        "service.py": b"VALUE = 1\n",
    }
    with _authority_run_root(tmp_path / "worker-identity-run") as (
        run_root,
        context,
    ):
        root = run_root / "storm_tracker_worker"
        dry_run_module._mkdir(root, run_root, context)
        records = []
        for name, value in files.items():
            mode = 0o755 if name.endswith(".sh") else 0o644
            dry_run_module._write_exclusive(
                root / name,
                value,
                mode=mode,
                run_root=run_root,
                authority_context=context,
            )
            records.append(dry_run_module.FileRecord(
                name,
                len(value),
                hashlib.sha256(value).hexdigest(),
                mode,
            ))
        tree = dry_run_module.TreeRecord.create("storm_tracker_worker", records)
        with pytest.raises(
            DryRunError,
            match="worker RAW-retention options or schema declarations differ",
        ):
            dry_run_module._worker_identity(
                root,
                "1.16.0",
                tree=tree,
                authority_context=context,
                require_worker_first_activation_contract=True,
            )


def test_committed_raw_capture_sanitized_snapshot_is_exact_and_secret_free():
    document = json.loads(RAW_CAPTURE_SANITIZED_OPTIONS.read_text(encoding="utf-8"))
    config = (ROOT / "storm_tracker_worker" / "config.yaml").read_text(
        encoding="utf-8"
    )
    candidate_schema_names = {
        name
        for name, _value in dry_run_module._yaml_section_entries(config, "schema")
    }
    documented_names = set(document["options"]) | set(document["secret_presence"])

    assert document["contract"] == OPTIONS_CONTRACT
    assert documented_names == candidate_schema_names | {
        "cpu_threads",
        "reserve_ha_core",
    }
    assert document["options"]["runtime_mode"] == "raw_capture_shadow"
    assert all(
        document["options"][name] is False
        for name in dry_run_module._RAW_RETENTION_OPTIONS
    )
    assert all(value is False for value in document["secret_presence"].values())
    _witness, parsed = dry_run_module._load_options_snapshot(
        RAW_CAPTURE_SANITIZED_OPTIONS
    )
    assert parsed["raw_capture_shadow_selected"] is True
    assert parsed["raw_capture_target_activation_pins_bound"] is False


def test_options_and_secret_presence_keysets_are_disjoint(tmp_path: Path):
    inputs = _inputs(tmp_path)
    document = json.loads(inputs[3].read_text(encoding="utf-8"))
    document["secret_presence"]["queue_size"] = False
    inputs[3].write_text(json.dumps(document), encoding="utf-8")
    with pytest.raises(DryRunError, match="must be disjoint"):
        _run(inputs, tmp_path / "run")


@pytest.mark.parametrize(
    "member_name",
    (
        "storm_tracker_v3/CON.txt",
        "storm_tracker_v3/cache:data",
        "storm_tracker_v3/trailing.",
        "storm_tracker_v3/secret.pem",
        "storm_tracker_v3/" + "/".join(["deep"] * 33) + "/file.py",
    ),
)
def test_cross_platform_archive_path_caps_are_fail_closed(
    tmp_path: Path,
    member_name: str,
):
    candidate = _candidate(tmp_path)
    rollback_integration = _legacy_integration(tmp_path, extra_member=member_name)
    rollback_worker = _legacy_worker(tmp_path)
    options = _options(tmp_path)
    policy = LegacyRollbackPolicy(
        "1.11.0", _sha(rollback_integration), "1.14.0", _sha(rollback_worker)
    )
    inputs = candidate, rollback_integration, rollback_worker, options, policy
    with pytest.raises(
        DryRunError,
        match=(
            "bound archive validation failed closed|cross-platform canonical|"
            "escapes|forbidden file"
        ),
    ):
        _run(inputs, tmp_path / "run")
    assert not any(
        (tmp_path / "run" / name).exists()
        for name in ("journal", "stage", "install", "backup", "retained")
    )


def test_journal_transition_grammar_rejects_hash_valid_extra_event(tmp_path: Path):
    inputs = _inputs(tmp_path)
    run_root = tmp_path / "run"
    _run(inputs, run_root)
    last = json.loads(
        (run_root / "journal" / "000010.json").read_text(encoding="utf-8")
    )
    unsigned = {
        "contract": last["contract"],
        "event": "upgrade_committed",
        "payload": {},
        "previous_sha256": last["event_sha256"],
        "sequence": 11,
    }
    forged = {**unsigned, "event_sha256": hashlib.sha256(_canonical(unsigned)).hexdigest()}
    (run_root / "journal" / "000011.json").write_bytes(_canonical(forged) + b"\n")
    with pytest.raises(DryRunError, match="transition grammar"):
        recover_release_install_dry_run(run_root)


@pytest.mark.parametrize("corruption", ("missing", "duplicate", "swapped"))
def test_recovery_requires_one_sealed_generation_of_each_kind(
    tmp_path: Path,
    corruption: str,
):
    inputs = _inputs(tmp_path)
    run_root = tmp_path / "run"
    _run(inputs, run_root)
    active = run_root / "install" / "integration.active"
    retained = run_root / "retained" / "integration.candidate"
    if corruption == "missing":
        os.rename(retained, run_root / "lost-integration-candidate")
    elif corruption == "duplicate":
        shutil.copytree(retained, run_root / "stage" / "integration.candidate")
    else:
        temporary = run_root / "swap-integration"
        os.rename(active, temporary)
        os.rename(retained, active)
        os.rename(temporary, retained)
    with pytest.raises(
        DryRunError,
        match=(
            "transaction generation inventory failed authority proof|"
            "phase-safe layouts|journal phase|placement is invalid"
        ),
    ):
        recover_release_install_dry_run(run_root)


@pytest.mark.parametrize("invalid_slot", ("stage", "retained"))
def test_commit_recovery_rejects_baseline_in_candidate_only_slots(
    tmp_path: Path,
    invalid_slot: str,
):
    inputs = _inputs(tmp_path)
    run_root = tmp_path / "run"
    seed_point = "journal.upgrade_committed.after_pending_fsync_before_publish"

    def stop(actual: str) -> None:
        if actual == seed_point:
            raise _RecoverySeedFailure(actual)

    with pytest.raises(_RecoverySeedFailure, match=re.escape(seed_point)):
        _run(inputs, run_root, fault_injector=stop)
    baseline = run_root / "backup" / "integration.baseline"
    destination = (
        run_root / invalid_slot / "integration.candidate"
    )
    os.rename(baseline, destination)
    with pytest.raises(
        DryRunError,
        match=(
            "transaction generation inventory failed authority proof|"
            "phase-safe layouts"
        ),
    ):
        recover_release_install_dry_run(run_root)


@pytest.mark.parametrize(
    "token",
    ("NaN", "Infinity", "-Infinity", "1e999", "-1e999", "1.0"),
)
def test_strict_json_rejects_every_nonfinite_or_floating_number(token: str):
    with pytest.raises(DryRunError, match="non-finite|floating"):
        dry_run_module._strict_json_bytes(token.encode("ascii"), "boundary JSON")


def test_json_metadata_byte_boundary_is_exact(monkeypatch):
    monkeypatch.setattr(dry_run_module, "MAX_INPUT_METADATA_BYTES", 4)
    assert dry_run_module._strict_json_bytes(b"{}  ", "boundary JSON") == {}
    with pytest.raises(DryRunError, match="JSON byte bound"):
        dry_run_module._strict_json_bytes(b"{}   ", "boundary JSON")


def test_json_structure_and_integer_boundaries_are_exact(monkeypatch):
    monkeypatch.setattr(dry_run_module, "MAX_INPUT_METADATA_BYTES", 1024)
    monkeypatch.setattr(dry_run_module, "MAX_JSON_DEPTH", 2)
    assert dry_run_module._strict_json_bytes(b"[[]]", "boundary JSON") == [[]]
    with pytest.raises(DryRunError, match="depth bound"):
        dry_run_module._strict_json_bytes(b"[[[]]]", "boundary JSON")

    monkeypatch.setattr(dry_run_module, "MAX_JSON_DEPTH", 20)
    monkeypatch.setattr(dry_run_module, "MAX_JSON_NODES", 3)
    assert dry_run_module._strict_json_bytes(b"[0,0]", "boundary JSON") == [0, 0]
    with pytest.raises(DryRunError, match="node bound"):
        dry_run_module._strict_json_bytes(b"[0,0,0]", "boundary JSON")

    monkeypatch.setattr(dry_run_module, "MAX_JSON_NODES", 100)
    monkeypatch.setattr(dry_run_module, "MAX_JSON_STRING_BYTES", 3)
    assert dry_run_module._strict_json_bytes(b'"abc"', "boundary JSON") == "abc"
    with pytest.raises(DryRunError, match="string bound"):
        dry_run_module._strict_json_bytes(b'"abcd"', "boundary JSON")

    monkeypatch.setattr(dry_run_module, "MAX_JSON_STRING_BYTES", 100)
    monkeypatch.setattr(dry_run_module, "MAX_JSON_KEY_BYTES", 3)
    assert dry_run_module._strict_json_bytes(b'{"abc":0}', "boundary JSON") == {"abc": 0}
    with pytest.raises(DryRunError, match="key bound"):
        dry_run_module._strict_json_bytes(b'{"abcd":0}', "boundary JSON")

    monkeypatch.setattr(dry_run_module, "MAX_JSON_KEY_BYTES", 100)
    monkeypatch.setattr(dry_run_module, "MAX_JSON_INTEGER", 10)
    assert dry_run_module._strict_json_bytes(b"10", "boundary JSON") == 10
    assert dry_run_module._strict_json_bytes(b"-10", "boundary JSON") == -10
    with pytest.raises(DryRunError, match="out-of-range JSON integer"):
        dry_run_module._strict_json_bytes(b"11", "boundary JSON")


def test_archive_path_byte_part_and_depth_boundaries_are_exact(monkeypatch):
    monkeypatch.setattr(dry_run_module, "MAX_ARCHIVE_PATH_BYTES", 5)
    monkeypatch.setattr(dry_run_module, "MAX_ARCHIVE_PART_BYTES", 3)
    monkeypatch.setattr(dry_run_module, "MAX_ARCHIVE_PATH_DEPTH", 3)
    assert dry_run_module._safe_relative_path("a/b/c").as_posix() == "a/b/c"
    with pytest.raises(DryRunError, match="invalid path"):
        dry_run_module._safe_relative_path("a/b/cd")
    with pytest.raises(DryRunError, match="cross-platform canonical"):
        dry_run_module._safe_relative_path("abcd")
    monkeypatch.setattr(dry_run_module, "MAX_ARCHIVE_PATH_BYTES", 100)
    with pytest.raises(DryRunError, match="escapes"):
        dry_run_module._safe_relative_path("a/b/c/d")


def test_tree_file_count_file_bytes_and_total_boundaries_are_exact(monkeypatch):
    raw = {"mode": 0o644, "path": "a", "sha256": "0" * 64, "size": 3}
    monkeypatch.setattr(dry_run_module, "MAX_FILE_BYTES", 3)
    assert dry_run_module.FileRecord.from_mapping(raw).size == 3
    with pytest.raises(DryRunError, match="tree file record is invalid"):
        dry_run_module.FileRecord.from_mapping({**raw, "size": 4})

    records = tuple(
        dry_run_module.FileRecord(chr(ord("a") + index), 1, "0" * 64, 0o644)
        for index in range(3)
    )
    monkeypatch.setattr(dry_run_module, "MAX_FILE_COUNT", 2)
    assert len(dry_run_module.TreeRecord.create("root", records[:2]).files) == 2
    with pytest.raises(DryRunError, match="file count"):
        dry_run_module.TreeRecord.create("root", records)

    monkeypatch.setattr(dry_run_module, "MAX_FILE_COUNT", 3)
    monkeypatch.setattr(dry_run_module, "MAX_TOTAL_BYTES", 3)
    total_exact = (
        dry_run_module.FileRecord("a", 1, "0" * 64, 0o644),
        dry_run_module.FileRecord("b", 2, "0" * 64, 0o644),
    )
    assert dry_run_module.TreeRecord.create("root", total_exact).total_bytes == 3
    with pytest.raises(DryRunError, match="total byte bound"):
        dry_run_module.TreeRecord.create(
            "root",
            (total_exact[0], dry_run_module.FileRecord("b", 3, "0" * 64, 0o644)),
        )


def test_spool_and_option_count_boundaries_are_exact(tmp_path: Path, monkeypatch):
    exact_source = tmp_path / "exact.bin"
    exact_source.write_bytes(b"abc")
    oversized = tmp_path / "oversized.bin"
    oversized.write_bytes(b"abcd")
    with _authority_run_root(tmp_path / "run") as (run_root, context):
        original, spool = dry_run_module._spool_input(
            exact_source,
            run_root / "spool" / "exact.bin",
            label="boundary input",
            maximum_bytes=3,
            run_root=run_root,
            authority_context=context,
            fault_injector=None,
            fault_prefix="input.boundary",
        )
        assert original.size == spool.size == 3
        with pytest.raises(
            DryRunError,
            match="outside its bound input contract|spool byte bound",
        ):
            dry_run_module._spool_input(
                oversized,
                run_root / "spool" / "oversized.bin",
                label="oversized boundary input",
                maximum_bytes=3,
                run_root=run_root,
                authority_context=context,
                fault_injector=None,
                fault_prefix="input.boundary_oversized",
            )

    monkeypatch.setattr(dry_run_module, "MAX_OPTIONS", 2)
    monkeypatch.setattr(dry_run_module, "MAX_SECRET_OPTIONS", 1)
    exact_options = tmp_path / "exact-options.json"
    exact_options.write_text(json.dumps({
        "contract": OPTIONS_CONTRACT,
        "options": {"cpu_threads": 2, "reserve_ha_core": True},
        "secret_presence": {"synoptic_token": False},
    }), encoding="utf-8")
    dry_run_module._load_options_snapshot(exact_options)
    too_many = tmp_path / "too-many-options.json"
    too_many.write_text(json.dumps({
        "contract": OPTIONS_CONTRACT,
        "options": {
            "cpu_threads": 2,
            "queue_size": 1,
            "reserve_ha_core": True,
        },
        "secret_presence": {"synoptic_token": False},
    }), encoding="utf-8")
    with pytest.raises(DryRunError, match="key-count bound"):
        dry_run_module._load_options_snapshot(too_many)
    too_many_secrets = tmp_path / "too-many-secrets.json"
    too_many_secrets.write_text(json.dumps({
        "contract": OPTIONS_CONTRACT,
        "options": {"cpu_threads": 2, "reserve_ha_core": True},
        "secret_presence": {
            "synoptic_token": False,
            "netatmo_client_secret": False,
        },
    }), encoding="utf-8")
    with pytest.raises(DryRunError, match="key-count bound"):
        dry_run_module._load_options_snapshot(too_many_secrets)


def test_spool_rejects_swap_between_lexical_check_and_authority_open(
    tmp_path: Path,
    monkeypatch,
):
    source = tmp_path / "source.bin"
    source.write_bytes(b"bound bytes")
    decoy = tmp_path / "decoy.bin"
    decoy.write_bytes(b"lexical stat from another object")
    original_plain_existing = dry_run_module._plain_existing_file

    with _authority_run_root(tmp_path / "run") as (run_root, context):
        def swapped_lexical_stat(path, label):
            resolved, details = original_plain_existing(path, label)
            if resolved == source:
                return resolved, decoy.stat()
            return resolved, details

        monkeypatch.setattr(
            dry_run_module,
            "_plain_existing_file",
            swapped_lexical_stat,
        )
        with pytest.raises(DryRunError, match="changed before it was captured"):
            dry_run_module._spool_input(
                source,
                run_root / "spool" / "source.bin",
                label="identity seam input",
                maximum_bytes=64,
                run_root=run_root,
                authority_context=context,
                fault_injector=None,
                fault_prefix="input.identity_seam",
            )
        assert not context.root.exists(
            dry_run_module.RelativePath.parse("spool/source.bin")
        )


def test_injected_destinations_are_never_overwritten_after_preflight(tmp_path: Path):
    inputs = _inputs(tmp_path)
    run_root = tmp_path / "tree-run"
    sentinel = b"do-not-overwrite\n"

    def create_tree_destination(point: str) -> None:
        if point == "rename.upgrade_worker_backup.before_rename":
            destination = run_root / "backup" / "worker.baseline"
            destination.mkdir()
            (destination / "sentinel").write_bytes(sentinel)

    with pytest.raises(
        DryRunError,
        match="observed to exist|destination already exists",
    ):
        _run(inputs, run_root, fault_injector=create_tree_destination)
    assert (run_root / "backup" / "worker.baseline" / "sentinel").read_bytes() == sentinel
    assert (run_root / "install" / "worker.active").is_dir()

    journal_root = tmp_path / "journal-run"

    def create_journal_destination(point: str) -> None:
        if point == "journal.initialized.after_pending_fsync_before_publish":
            (journal_root / "journal" / "000001.json").write_bytes(sentinel)

    with pytest.raises(
        DryRunError,
        match="observed to exist|destination already exists",
    ):
        _run(inputs, journal_root, fault_injector=create_journal_destination)
    assert (journal_root / "journal" / "000001.json").read_bytes() == sentinel


def test_all_transaction_publications_use_descriptor_relative_adapter(
    tmp_path: Path,
    monkeypatch,
):
    inputs = _inputs(tmp_path)
    observed_source_kinds: list[str] = []
    original = dry_run_module.DescriptorRelativeAtomicAdapter.publish

    def observe(self, source, destination, *, expected_source, kind):
        observed_source_kinds.append(expected_source.kind)
        return original(
            self,
            source,
            destination,
            expected_source=expected_source,
            kind=kind,
        )

    monkeypatch.setattr(
        dry_run_module.DescriptorRelativeAtomicAdapter,
        "publish",
        observe,
    )
    report = _run(inputs, tmp_path / "run").report

    assert observed_source_kinds.count("regular_file") == 10
    assert observed_source_kinds.count("directory") == 8
    assert report["atomic_noreplace_publication"][
        "journal_pending_to_final_publications"
    ] == 10
    assert report["atomic_noreplace_publication"][
        "generation_directory_publications"
    ] == 8


def test_open_authority_probe_fails_before_new_run_root_mutation(
    tmp_path: Path,
    monkeypatch,
):
    inputs = _inputs(tmp_path)
    run_root = tmp_path / "run"

    def fail_probe(_authority, _expected_identity):
        raise dry_run_module.AtomicNoReplaceError("injected unsupported backend")

    monkeypatch.setattr(dry_run_module, "probe_open_authority", fail_probe)
    with pytest.raises(
        DryRunError,
        match="authority bootstrap failed closed",
    ):
        _run(inputs, run_root)

    assert not run_root.exists()


def test_local_tooling_has_no_live_transport_or_destructive_extract_shortcut():
    assert set(FAULT_POINTS) == EXPECTED_EXECUTION_FAULT_POINTS
    assert len(FAULT_POINTS) == len(set(FAULT_POINTS)) == 63
    assert set(RECOVERY_FAULT_POINTS) == EXPECTED_RECOVERY_FAULT_POINTS
    assert len(RECOVERY_FAULT_POINTS) == len(set(RECOVERY_FAULT_POINTS)) == 25
    sources = "\n".join(path.read_text(encoding="utf-8") for path in (
        ROOT / "release_install_dry_run.py",
        ROOT / "scripts" / "dry_run_release_install.py",
    )).lower()
    for forbidden in (
        "paramiko", "import socket", "extractall", "rmtree(", ".unlink(",
        "os.remove(", "rm -rf", "/addons/", "/homeassistant/",
    ):
        assert forbidden not in sources
    assert re.search(r"\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}\b", sources) is None
    simulator_source = (ROOT / "release_install_dry_run.py").read_text(
        encoding="utf-8"
    )
    assert "os.rename(" not in simulator_source
    assert "authority_context.atomic.publish(" in simulator_source
