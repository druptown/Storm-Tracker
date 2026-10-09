from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

import atomic_noreplace_publication as legacy
import descriptor_relative_atomic_adapter as adapter
import descriptor_relative_authority as authority


def _write(
    root: authority.DirectoryAuthority,
    name: str | authority.RelativePath,
    value: bytes,
) -> authority.NodeIdentity:
    with root.create_file(name) as bound:
        bound.write_all(value)
        bound.flush()
        return bound.identity()


def _adapter(
    root: authority.DirectoryAuthority,
) -> adapter.DescriptorRelativeAtomicAdapter:
    root_identity = root.identity()
    capability = adapter.probe_open_authority(root, root_identity)
    return adapter.DescriptorRelativeAtomicAdapter(
        authority=root,
        root_identity=root_identity,
        capability=capability,
    )


def _names(root: authority.DirectoryAuthority) -> tuple[str, ...]:
    return tuple(entry.name for entry in root.list_entries(maximum_entries=64))


def test_open_authority_probe_is_bound_pathless_and_cleans(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    with authority.DirectoryAuthority.open_absolute(tmp_path) as root:
        root_identity = root.identity()
        before = _names(root)

        def forbid_path_reopen(*_args, **_kwargs):
            raise AssertionError("probe reopened a filesystem path")

        monkeypatch.setattr(
            authority.DirectoryAuthority,
            "open_absolute",
            classmethod(forbid_path_reopen),
        )
        capability = adapter.probe_open_authority(root, root_identity)

        assert _names(root) == before
        assert root.identity().same_object(root_identity)

    evidence = capability.to_mapping()
    assert capability.proven is True
    assert capability.authority_backend == root_identity.backend
    assert evidence["descriptor_relative_create_read_list"] is True
    assert evidence["file_publication_succeeded"] is True
    assert evidence["file_conflict_rejected"] is True
    assert evidence["directory_publication_succeeded"] is True
    assert evidence["directory_conflict_rejected"] is True
    assert evidence["listing_reproved"] is True
    assert evidence["opened_root_identity_bound"] is True
    assert evidence["path_reopen_used"] is False
    assert evidence["cleanup_completed"] is True


def test_file_and_nonempty_directory_publish_without_path_reopen(tmp_path: Path):
    with authority.DirectoryAuthority.open_absolute(tmp_path) as root:
        publication = _adapter(root)
        file_identity = _write(root, "pending", b"journal")

        file_backend = publication.publish(
            authority.RelativePath.parse("pending"),
            authority.RelativePath.parse("final"),
            expected_source=file_identity,
            kind="journal_pending_to_final",
        )

        with root.create_directory("generation-stage") as generation:
            generation_identity = generation.identity()
            _write(generation, "sentinel", b"generation")
        directory_backend = publication.publish(
            authority.RelativePath.parse("generation-stage"),
            authority.RelativePath.parse("generation-active"),
            expected_source=generation_identity,
            kind="generation_directory",
        )

        assert file_backend == publication.capability.publication_backend
        assert directory_backend == publication.capability.publication_backend
        assert not root.exists("pending")
        assert root.read_file("final", maximum_bytes=16) == b"journal"
        assert not root.exists("generation-stage")
        with root.open_directory("generation-active") as active:
            assert active.read_file("sentinel", maximum_bytes=16) == b"generation"
        assert publication.journal_publications == 1
        assert publication.generation_directory_publications == 1


def test_file_conflict_preserves_both_identities_and_counter(tmp_path: Path):
    with authority.DirectoryAuthority.open_absolute(tmp_path) as root:
        publication = _adapter(root)
        source_identity = _write(root, "source", b"source")
        destination_identity = _write(root, "destination", b"destination")

        with pytest.raises(legacy.AtomicNoReplaceConflict):
            publication.publish(
                authority.RelativePath.parse("source"),
                authority.RelativePath.parse("destination"),
                expected_source=source_identity,
                kind="journal_pending_to_final",
            )

        assert root.entry_identity("source").same_object(source_identity)
        assert root.entry_identity("destination").same_object(destination_identity)
        assert root.read_file("source", maximum_bytes=16) == b"source"
        assert root.read_file("destination", maximum_bytes=16) == b"destination"
        assert publication.journal_publications == 0


def test_nonempty_directory_conflict_preserves_both_identities(tmp_path: Path):
    with authority.DirectoryAuthority.open_absolute(tmp_path) as root:
        publication = _adapter(root)
        with root.create_directory("source") as source:
            source_identity = source.identity()
            _write(source, "payload", b"source")
        with root.create_directory("destination") as destination:
            destination_identity = destination.identity()
            _write(destination, "payload", b"destination")

        with pytest.raises(legacy.AtomicNoReplaceConflict):
            publication.publish(
                authority.RelativePath.parse("source"),
                authority.RelativePath.parse("destination"),
                expected_source=source_identity,
                kind="generation_directory",
            )

        assert root.entry_identity("source").same_object(source_identity)
        assert root.entry_identity("destination").same_object(destination_identity)
        with root.open_directory("source") as source:
            assert source.read_file("payload", maximum_bytes=16) == b"source"
        with root.open_directory("destination") as destination:
            assert destination.read_file("payload", maximum_bytes=16) == b"destination"
        assert publication.generation_directory_publications == 0


def test_contender_conflict_after_absence_still_reproves_source(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    with authority.DirectoryAuthority.open_absolute(tmp_path) as root:
        publication = _adapter(root)
        source_identity = _write(root, "source", b"source")

        def contender(
            self: authority.DirectoryAuthority,
            _source,
            destination,
            *,
            expected_source=None,
        ):
            del expected_source
            _write(self, destination, b"contender")
            raise authority.AuthorityConflict("injected contender")

        monkeypatch.setattr(
            authority.DirectoryAuthority,
            "publish_noreplace",
            contender,
        )
        with pytest.raises(legacy.AtomicNoReplaceConflict):
            publication.publish(
                authority.RelativePath.parse("source"),
                authority.RelativePath.parse("destination"),
                expected_source=source_identity,
                kind="journal_pending_to_final",
            )

        assert root.entry_identity("source").same_object(source_identity)
        assert root.read_file("destination", maximum_bytes=16) == b"contender"
        assert publication.journal_publications == 0


def test_false_success_without_source_consumption_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    with authority.DirectoryAuthority.open_absolute(tmp_path) as root:
        publication = _adapter(root)
        source_identity = _write(root, "source", b"source")

        def false_success(self, source, destination, *, expected_source=None):
            del self, source, destination, expected_source
            return publication.capability.publication_backend

        monkeypatch.setattr(
            authority.DirectoryAuthority,
            "publish_noreplace",
            false_success,
        )
        with pytest.raises(legacy.AtomicNoReplaceFailure, match="source_not_consumed"):
            publication.publish(
                authority.RelativePath.parse("source"),
                authority.RelativePath.parse("destination"),
                expected_source=source_identity,
                kind="journal_pending_to_final",
            )

        assert root.entry_identity("source").same_object(source_identity)
        assert not root.exists("destination")
        assert publication.journal_publications == 0


def test_false_conflict_after_consumption_is_not_reported_as_conflict(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    with authority.DirectoryAuthority.open_absolute(tmp_path) as root:
        publication = _adapter(root)
        source_identity = _write(root, "source", b"source")
        original_publish = authority.DirectoryAuthority.publish_noreplace

        def false_conflict(self, source, destination, *, expected_source=None):
            original_publish(
                self,
                source,
                destination,
                expected_source=expected_source,
            )
            raise authority.AuthorityConflict("injected false conflict")

        monkeypatch.setattr(
            authority.DirectoryAuthority,
            "publish_noreplace",
            false_conflict,
        )
        with pytest.raises(legacy.AtomicNoReplaceFailure):
            publication.publish(
                authority.RelativePath.parse("source"),
                authority.RelativePath.parse("destination"),
                expected_source=source_identity,
                kind="journal_pending_to_final",
            )

        assert not root.exists("source")
        assert root.entry_identity("destination").same_object(source_identity)
        assert publication.journal_publications == 0


def test_stale_source_identity_fails_without_publication(tmp_path: Path):
    with authority.DirectoryAuthority.open_absolute(tmp_path) as root:
        publication = _adapter(root)
        _write(root, "source", b"source")
        other = _write(root, "other", b"other")

        with pytest.raises(legacy.AtomicNoReplaceFailure, match="identity_changed"):
            publication.publish(
                authority.RelativePath.parse("source"),
                authority.RelativePath.parse("destination"),
                expected_source=other,
                kind="journal_pending_to_final",
            )

        assert root.exists("source")
        assert not root.exists("destination")


def test_root_identity_is_reproved_after_successful_primitive(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    with authority.DirectoryAuthority.open_absolute(tmp_path) as root:
        publication = _adapter(root)
        source_identity = _write(root, "source", b"source")
        original_publish = authority.DirectoryAuthority.publish_noreplace
        original_identity = authority.DirectoryAuthority.identity
        completed = False

        def publish_then_flip(self, source, destination, *, expected_source=None):
            nonlocal completed
            backend = original_publish(
                self,
                source,
                destination,
                expected_source=expected_source,
            )
            completed = True
            return backend

        def identity_after_flip(self):
            actual = original_identity(self)
            if self is root and completed:
                return replace(actual, file_id="00" * 16)
            return actual

        monkeypatch.setattr(
            authority.DirectoryAuthority,
            "publish_noreplace",
            publish_then_flip,
        )
        monkeypatch.setattr(
            authority.DirectoryAuthority,
            "identity",
            identity_after_flip,
        )
        with pytest.raises(legacy.AtomicNoReplaceFailure, match="root_after.*changed"):
            publication.publish(
                authority.RelativePath.parse("source"),
                authority.RelativePath.parse("destination"),
                expected_source=source_identity,
                kind="journal_pending_to_final",
            )

        assert publication.journal_publications == 0


def test_unproven_wrong_backend_or_wrong_root_capability_is_rejected(
    tmp_path: Path,
):
    with authority.DirectoryAuthority.open_absolute(tmp_path) as root:
        root_identity = root.identity()
        capability = adapter.probe_open_authority(root, root_identity)

        with pytest.raises(legacy.AtomicNoReplaceUnavailable, match="not_proven"):
            adapter.DescriptorRelativeAtomicAdapter(
                root,
                root_identity,
                replace(capability, file_conflict_preserved=False),
            )

        other_backend = f"{root.backend}_other"
        backend_capability = replace(
            capability,
            authority_backend=other_backend,
            root_identity=replace(root_identity, backend=other_backend),
        )
        assert backend_capability.proven is True
        with pytest.raises(legacy.AtomicNoReplaceUnavailable, match="backend_mismatch"):
            adapter.DescriptorRelativeAtomicAdapter(
                root,
                root_identity,
                backend_capability,
            )

        wrong_root = replace(root_identity, file_id="11" * 16)
        with pytest.raises(legacy.AtomicNoReplaceUnavailable, match="root_mismatch"):
            adapter.DescriptorRelativeAtomicAdapter(
                root,
                root_identity,
                replace(capability, root_identity=wrong_root),
            )


def test_kind_and_source_backend_must_match_expected_source(tmp_path: Path):
    with authority.DirectoryAuthority.open_absolute(tmp_path) as root:
        publication = _adapter(root)
        file_identity = _write(root, "file", b"file")
        with root.create_directory("directory") as directory:
            directory_identity = directory.identity()

        with pytest.raises(legacy.AtomicNoReplaceFailure, match="kind_invalid"):
            publication.publish(
                authority.RelativePath.parse("file"),
                authority.RelativePath.parse("file-destination"),
                expected_source=file_identity,
                kind="generation_directory",
            )
        with pytest.raises(legacy.AtomicNoReplaceFailure, match="kind_invalid"):
            publication.publish(
                authority.RelativePath.parse("directory"),
                authority.RelativePath.parse("directory-destination"),
                expected_source=directory_identity,
                kind="journal_pending_to_final",
            )
        with pytest.raises(legacy.AtomicNoReplaceFailure, match="backend_mismatch"):
            publication.publish(
                authority.RelativePath.parse("file"),
                authority.RelativePath.parse("file-destination"),
                expected_source=replace(file_identity, backend="wrong_backend"),
                kind="journal_pending_to_final",
            )


def test_adapter_rejects_path_objects_before_delegate(tmp_path: Path):
    with authority.DirectoryAuthority.open_absolute(tmp_path) as root:
        publication = _adapter(root)
        source_identity = _write(root, "source", b"source")
        with pytest.raises(TypeError, match="RelativePath"):
            publication.publish(  # type: ignore[arg-type]
                Path("source"),
                Path("destination"),
                expected_source=source_identity,
                kind="journal_pending_to_final",
            )
        assert root.exists("source")


def test_probe_name_collision_is_preserved(tmp_path: Path, monkeypatch):
    with authority.DirectoryAuthority.open_absolute(tmp_path) as root:
        collision = ".stv3-open-authority-atomic-collision"
        with root.create_directory(collision) as existing:
            _write(existing, "sentinel", b"owned-by-contender")
        monkeypatch.setattr(
            adapter.uuid,
            "uuid4",
            lambda: SimpleNamespace(hex="collision"),
        )

        with pytest.raises(legacy.AtomicNoReplaceConflict):
            adapter.probe_open_authority(root, root.identity())

        with root.open_directory(collision) as existing:
            assert existing.read_file("sentinel", maximum_bytes=64) == (
                b"owned-by-contender"
            )


def test_probe_midflight_failure_closes_children_and_removes_owned_tree(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    with authority.DirectoryAuthority.open_absolute(tmp_path) as root:
        before = _names(root)
        original_write = adapter._write_probe_file

        def fail_in_nested_directory(probe, relative, value):
            if str(relative) == "sentinel":
                raise legacy.AtomicNoReplaceFailure("injected nested failure")
            return original_write(probe, relative, value)

        monkeypatch.setattr(adapter, "_write_probe_file", fail_in_nested_directory)
        with pytest.raises(legacy.AtomicNoReplaceFailure, match="nested failure"):
            adapter.probe_open_authority(root, root.identity())

        assert _names(root) == before


def test_probe_listing_failure_removes_owned_tree_and_preserves_root(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    with authority.DirectoryAuthority.open_absolute(tmp_path) as root:
        before_names = _names(root)
        before_identity = root.identity()
        original_list = authority.DirectoryAuthority.list_entries
        injected = False

        def fail_once(self, *args, **kwargs):
            nonlocal injected
            if not injected:
                injected = True
                raise authority.AuthorityIOError("injected list failure")
            return original_list(self, *args, **kwargs)

        monkeypatch.setattr(
            authority.DirectoryAuthority,
            "list_entries",
            fail_once,
        )
        with pytest.raises(legacy.AtomicNoReplaceFailure, match="listing_failed"):
            adapter.probe_open_authority(root, before_identity)

        assert _names(root) == before_names
        assert root.identity().same_object(before_identity)


def test_probe_fsync_failure_removes_owned_tree_and_preserves_root(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    with authority.DirectoryAuthority.open_absolute(tmp_path) as root:
        before_names = _names(root)
        before_identity = root.identity()
        original_fsync = authority.DirectoryAuthority.fsync
        injected = False

        def fail_once(self):
            nonlocal injected
            if not injected:
                injected = True
                raise authority.AuthorityIOError("injected fsync failure")
            return original_fsync(self)

        monkeypatch.setattr(
            authority.DirectoryAuthority,
            "fsync",
            fail_once,
        )
        with pytest.raises(
            legacy.AtomicNoReplaceFailure,
            match="directory_fsync_failed",
        ):
            adapter.probe_open_authority(root, before_identity)

        assert _names(root) == before_names
        assert root.identity().same_object(before_identity)


def test_probe_cleanup_failure_never_returns_positive_capability(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    with authority.DirectoryAuthority.open_absolute(tmp_path) as root:
        before = _names(root)
        original_remove = authority.DirectoryAuthority.remove_owned_tree

        def remove_then_fail(self, *args, **kwargs):
            original_remove(self, *args, **kwargs)
            raise authority.AuthorityIOError("injected cleanup failure")

        monkeypatch.setattr(
            authority.DirectoryAuthority,
            "remove_owned_tree",
            remove_then_fail,
        )
        with pytest.raises(legacy.AtomicNoReplaceFailure, match="cleanup_failed"):
            adapter.probe_open_authority(root, root.identity())

        assert _names(root) == before


def test_probe_and_adapter_reject_wrong_root_identity(tmp_path: Path):
    with authority.DirectoryAuthority.open_absolute(tmp_path) as root:
        actual = root.identity()
        wrong = replace(actual, file_id="22" * 16)
        with pytest.raises(legacy.AtomicNoReplaceFailure, match="root_before_changed"):
            adapter.probe_open_authority(root, wrong)

        capability = adapter.probe_open_authority(root, actual)
        with pytest.raises(legacy.AtomicNoReplaceFailure, match="root_before_bind_changed"):
            adapter.DescriptorRelativeAtomicAdapter(root, wrong, capability)


def test_evidence_is_counter_compatible_and_claims_no_blocker(tmp_path: Path):
    with authority.DirectoryAuthority.open_absolute(tmp_path) as root:
        publication = _adapter(root)
        evidence = publication.evidence()

    assert evidence["adapter_contract"] == (
        adapter.DESCRIPTOR_RELATIVE_ATOMIC_ADAPTER_CONTRACT
    )
    assert evidence["journal_pending_to_final_publications"] == 0
    assert evidence["generation_directory_publications"] == 0
    assert evidence["path_reopen_used"] is False
    assert evidence["simulator_integration_proven"] is False
    assert evidence["descriptor_relative_output_authority_proven"] is False
    assert evidence["ancestor_and_final_output_path_race_exclusion_proven"] is False
    assert evidence["local_acceptance_blockers_closed"] == []


def test_module_has_no_path_api_or_legacy_path_publication_call():
    source = Path(adapter.__file__).read_text(encoding="utf-8")

    assert "from pathlib import Path" not in source
    assert "probe_atomic_noreplace" not in source
    assert "publish_atomic_noreplace" not in source
    assert ".open_absolute(" not in source
    assert ".publish_noreplace(" in source
    assert "RelativePath" in source
