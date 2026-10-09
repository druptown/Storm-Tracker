from __future__ import annotations

import ctypes
import errno
import os
from pathlib import Path
import platform

import pytest

import atomic_noreplace_publication as publication


def _write(path: Path, value: bytes) -> None:
    path.write_bytes(value)


def test_real_filesystem_probe_proves_success_and_conflict(tmp_path: Path):
    capability = publication.probe_atomic_noreplace(tmp_path)

    assert capability.proven is True
    assert capability.destination_conflict_rejected is True
    assert capability.directory_destination_conflict_rejected is True
    assert capability.directory_source_consumed_on_success is True
    assert capability.destructive_probe_completed is True
    assert capability.same_filesystem is True
    assert capability.source_consumed_on_success is True
    assert capability.to_mapping() == {
        "backend": capability.backend,
        "contract": publication.ATOMIC_NOREPLACE_CONTRACT,
        "destination_conflict_rejected": True,
        "directory_destination_conflict_rejected": True,
        "directory_source_consumed_on_success": True,
        "destructive_probe_completed": True,
        "fallback_used": False,
        "no_replace_atomicity_proven": True,
        "path_authority_scope": (
            "path_based_windows_api"
            if os.name == "nt"
            else "parent_directory_descriptors"
        ),
        "same_filesystem": True,
        "source_consumed_on_success": True,
    }
    assert tuple(tmp_path.iterdir()) == ()


def test_existing_destination_is_unchanged_and_source_remains(tmp_path: Path):
    source = tmp_path / "source"
    destination = tmp_path / "destination"
    _write(source, b"source")
    _write(destination, b"destination")

    with pytest.raises(publication.AtomicNoReplaceConflict):
        publication.publish_atomic_noreplace(source, destination)

    assert source.read_bytes() == b"source"
    assert destination.read_bytes() == b"destination"


def test_success_consumes_source_and_publishes_exact_bytes(tmp_path: Path):
    source = tmp_path / "source"
    destination = tmp_path / "destination"
    _write(source, b"source")

    backend = publication.publish_atomic_noreplace(source, destination)

    assert backend in {
        "linux_renameat2_noreplace",
        "darwin_renameatx_np_excl",
        "windows_movefileex_no_replace",
    }
    assert not source.exists()
    assert destination.read_bytes() == b"source"


def test_directory_success_consumes_source_and_publishes_tree(tmp_path: Path):
    source = tmp_path / "source"
    destination = tmp_path / "destination"
    source.mkdir()
    (source / "sentinel").write_bytes(b"directory-source")

    publication.publish_atomic_noreplace(source, destination)

    assert not source.exists()
    assert destination.is_dir()
    assert (destination / "sentinel").read_bytes() == b"directory-source"


def test_existing_directory_destination_is_unchanged_and_source_remains(
    tmp_path: Path,
):
    source = tmp_path / "source"
    destination = tmp_path / "destination"
    source.mkdir()
    destination.mkdir()
    (source / "source-sentinel").write_bytes(b"source")
    (destination / "destination-sentinel").write_bytes(b"destination")

    with pytest.raises(publication.AtomicNoReplaceConflict):
        publication.publish_atomic_noreplace(source, destination)

    assert (source / "source-sentinel").read_bytes() == b"source"
    assert (
        destination / "destination-sentinel"
    ).read_bytes() == b"destination"


def test_unsupported_platform_has_no_path_based_fallback(
    tmp_path: Path,
    monkeypatch,
):
    if os.name == "nt":
        pytest.skip("POSIX-only backend selection test")
    source = tmp_path / "source"
    destination = tmp_path / "destination"
    _write(source, b"source")
    monkeypatch.setattr(platform, "system", lambda: "FreeBSD")

    with pytest.raises(
        publication.AtomicNoReplaceUnavailable,
        match="platform_unsupported",
    ):
        publication.publish_atomic_noreplace(source, destination)

    assert source.read_bytes() == b"source"
    assert not destination.exists()


@pytest.mark.parametrize(
    "error_code",
    sorted(publication._UNSUPPORTED_ERRNOS),
)
def test_posix_unsupported_errors_fail_closed(error_code: int):
    with pytest.raises(publication.AtomicNoReplaceUnavailable):
        publication._classify_posix_error(error_code, "test")


def test_posix_conflict_is_distinct_from_unavailability():
    with pytest.raises(publication.AtomicNoReplaceConflict):
        publication._classify_posix_error(errno.EEXIST, "test")


def test_posix_unexpected_error_is_not_downgraded_to_unavailable():
    with pytest.raises(publication.AtomicNoReplaceFailure):
        publication._classify_posix_error(errno.EIO, "test")


def test_source_or_destination_parent_must_be_plain(tmp_path: Path):
    real = tmp_path / "real"
    real.mkdir()
    indirect = tmp_path / "indirect"
    try:
        indirect.symlink_to(real, target_is_directory=True)
    except OSError as error:
        pytest.skip(f"directory symlink unavailable: {error}")
    source = indirect / "source"
    destination = tmp_path / "destination"
    _write(real / "source", b"source")

    with pytest.raises(
        publication.AtomicNoReplaceFailure,
        match="not_plain_directory",
    ):
        publication.publish_atomic_noreplace(source, destination)

    assert (real / "source").read_bytes() == b"source"
    assert not destination.exists()


def test_probe_cleanup_failure_cannot_return_positive_capability(
    tmp_path: Path,
    monkeypatch,
):
    original_unlink = Path.unlink
    calls = 0

    def fail_first_cleanup(path: Path, *args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise PermissionError("injected cleanup failure")
        return original_unlink(path, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", fail_first_cleanup)
    with pytest.raises(
        publication.AtomicNoReplaceFailure,
        match="probe_cleanup_failed",
    ):
        publication.probe_atomic_noreplace(tmp_path)


def test_probe_uuid_collision_never_overwrites_or_removes_existing_file(
    tmp_path: Path,
    monkeypatch,
):
    probe_id = "fixed-collision"
    collision = tmp_path / f".atomic-noreplace-{probe_id}.source"
    collision.write_bytes(b"preexisting-sentinel")
    monkeypatch.setattr(
        publication.uuid,
        "uuid4",
        lambda: type("FixedUuid", (), {"hex": probe_id})(),
    )

    with pytest.raises(
        publication.AtomicNoReplaceFailure,
        match="probe_name_collision",
    ):
        publication.probe_atomic_noreplace(tmp_path)

    assert collision.read_bytes() == b"preexisting-sentinel"
    assert tuple(tmp_path.iterdir()) == (collision,)


def test_relative_paths_are_rejected_before_any_mutation(tmp_path: Path):
    with pytest.raises(
        publication.AtomicNoReplaceFailure,
        match="publication_leaf_invalid",
    ):
        publication.publish_atomic_noreplace(
            Path("relative-source"),
            tmp_path / "destination",
        )
    assert not (tmp_path / "destination").exists()


def test_source_and_destination_must_be_leaf_paths(tmp_path: Path):
    filesystem_root = Path(tmp_path.anchor)
    assert filesystem_root.name == ""
    with pytest.raises(
        publication.AtomicNoReplaceFailure,
        match="publication_leaf_invalid",
    ):
        publication.publish_atomic_noreplace(
            filesystem_root,
            tmp_path / "destination",
        )


def test_module_does_not_offer_unsafe_fallbacks():
    source = Path(publication.__file__).read_text(encoding="utf-8")
    forbidden = (
        "os.rename(",
        "os.replace(",
        "shutil.move(",
        "copyfile(",
        "os.link(",
    )
    for token in forbidden:
        assert token not in source
    assert "RENAME_NOREPLACE" in source
    assert "RENAME_EXCL" in source
    assert "MoveFileExW" in source
    assert "MOVEFILE_COPY_ALLOWED" in source
    assert "MOVEFILE_REPLACE_EXISTING" in source


def test_windows_error_sets_are_disjoint():
    assert publication._WINDOWS_CONFLICT_ERRORS.isdisjoint(
        publication._WINDOWS_UNSUPPORTED_ERRORS
    )
    assert publication.ERROR_ALREADY_EXISTS in publication._WINDOWS_CONFLICT_ERRORS
    assert publication.ERROR_NOT_SUPPORTED in publication._WINDOWS_UNSUPPORTED_ERRORS
