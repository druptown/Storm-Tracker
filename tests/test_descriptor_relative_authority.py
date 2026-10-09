from __future__ import annotations

from dataclasses import replace
import errno
import os
from pathlib import Path

import pytest

import descriptor_relative_authority as authority


def _write(
    root: authority.DirectoryAuthority,
    relative: str,
    value: bytes,
) -> authority.NodeIdentity:
    with root.create_file(relative) as bound:
        bound.write_all(value)
        bound.flush()
        return bound.identity()


def test_relative_path_is_bounded_and_portable():
    parsed = authority.RelativePath.parse("one/two")

    assert parsed.parts == ("one", "two")
    assert parsed.parent.parts == ("one",)
    assert parsed.leaf == "two"
    assert str(parsed.child("three")) == "one/two/three"
    assert authority.RelativePath.parse("").parts == ()


@pytest.mark.parametrize(
    "value",
    (
        "/absolute",
        "..",
        ".",
        "one//two",
        "one\\two",
        "C:drive",
        "trailing.",
        "trailing ",
        "NUL.txt",
        "COM1",
        "control\x01",
    ),
)
def test_relative_path_rejects_ambiguous_or_escaping_names(value: str):
    with pytest.raises(authority.AuthorityViolation):
        authority.RelativePath.parse(value)


def test_relative_path_consumes_only_bounded_iterable():
    consumed = 0

    def values():
        nonlocal consumed
        while True:
            consumed += 1
            yield "part"

    with pytest.raises(authority.AuthorityLimitExceeded):
        authority.RelativePath.parse(values())

    assert consumed == authority.MAX_RELATIVE_DEPTH + 1


@pytest.mark.parametrize(
    ("value", "expected"),
    (
        (r"C:\one\two", "\\??\\C:\\one\\two"),
        ("c:\\", "\\??\\C:\\"),
        (r"\\server\share\one", "\\??\\UNC\\server\\share\\one"),
    ),
)
def test_windows_absolute_path_translation_is_canonical(value: str, expected: str):
    assert authority._win_absolute_nt_path(value) == expected


@pytest.mark.parametrize(
    "value",
    (
        r"\root-relative",
        r"C:drive-relative",
        r"\\?\C:\extended",
        r"\\.\C:\device",
        r"\??\C:\already-nt",
        r"\Device\HarddiskVolume1\device",
        r"C:\one\..\escape",
        r"C:\one\.\ambiguous",
        r"C:\one:stream",
        r"C:\NUL",
        "C:\\trailing.",
        "C:\\trailing ",
        "C:\\control\x01",
        r"\\server",
        "\\\\server\\share\\",
        r"C:\one\\two",
        r"C:/mixed/separators",
    ),
)
def test_windows_absolute_path_translation_rejects_ambiguous_names(value: str):
    with pytest.raises(authority.AuthorityViolation):
        authority._win_absolute_nt_path(value)


def test_windows_absolute_path_translation_rejects_ushort_overflow():
    oversized = "C:\\" + "\\".join(["x" * 200] * 200)

    with pytest.raises(
        authority.AuthorityLimitExceeded,
        match="windows_absolute_path_too_long",
    ):
        authority._win_absolute_nt_path(oversized)


def test_open_create_read_identity_and_duplicate_stream(tmp_path: Path):
    with authority.DirectoryAuthority.open_absolute(tmp_path) as root:
        with root.create_directory("nested") as nested:
            with nested.create_file("payload") as bound:
                bound.write_all(b"payload")
                bound.flush()
                written = bound.identity()
                assert written.kind == "regular_file"
                assert written.size == 7
                with bound.duplicate_binary() as stream:
                    assert stream.read() == b"payload"

            assert nested.read_file("payload", maximum_bytes=7) == b"payload"
            reopened = nested.entry_identity("payload")
            assert reopened.same_object(written)
            assert reopened.to_mapping()["file_id"]

        assert root.entry_identity("nested").kind == "directory"


def test_exclusive_create_rejects_existing_file_and_directory(tmp_path: Path):
    with authority.DirectoryAuthority.open_absolute(tmp_path) as root:
        _write(root, "existing-file", b"original")
        with pytest.raises(authority.AuthorityConflict):
            root.create_file("existing-file")
        assert root.read_file("existing-file", maximum_bytes=32) == b"original"

        child = root.create_directory("existing-directory")
        child.close()
        with pytest.raises(authority.AuthorityConflict):
            root.create_directory("existing-directory")


def test_read_is_bounded_before_returning_bytes(tmp_path: Path):
    with authority.DirectoryAuthority.open_absolute(tmp_path) as root:
        _write(root, "payload", b"12345")

        with pytest.raises(
            authority.AuthorityLimitExceeded,
            match="read_limit_exceeded",
        ):
            root.read_file("payload", maximum_bytes=4)

        assert root.read_file("payload", maximum_bytes=5) == b"12345"


def test_hardlinked_regular_file_is_rejected(tmp_path: Path):
    with authority.DirectoryAuthority.open_absolute(tmp_path) as root:
        _write(root, "source", b"payload")
        try:
            os.link(tmp_path / "source", tmp_path / "alias")
        except OSError as error:
            pytest.skip(f"hard links unavailable: {error}")

        with pytest.raises(
            authority.AuthorityViolation,
            match="hardlink_alias_forbidden",
        ):
            root.open_file("source")
        with pytest.raises(
            authority.AuthorityViolation,
            match="hardlink_alias_forbidden",
        ):
            root.entry_identity("alias")
        with pytest.raises(
            authority.AuthorityViolation,
            match="hardlink_alias_forbidden",
        ):
            root.publish_noreplace("source", "published")
        assert root.exists("published") is False


def test_duplicate_stream_failure_closes_owned_duplicate(
    tmp_path: Path,
    monkeypatch,
):
    with authority.DirectoryAuthority.open_absolute(tmp_path) as root:
        _write(root, "payload", b"payload")
        with root.open_file("payload") as bound:
            if os.name == "nt":
                import msvcrt

                closed: list[int] = []
                fake_duplicate = 0x12345
                with monkeypatch.context() as patcher:
                    patcher.setattr(
                        authority,
                        "_win_duplicate",
                        lambda _handle: fake_duplicate,
                    )
                    patcher.setattr(
                        authority,
                        "_win_close",
                        lambda handle: closed.append(handle),
                    )
                    patcher.setattr(
                        msvcrt,
                        "open_osfhandle",
                        lambda *_args: (_ for _ in ()).throw(OSError("injected")),
                    )
                    with pytest.raises(authority.AuthorityIOError):
                        bound.duplicate_binary()
                assert closed == [fake_duplicate]
            else:
                duplicates: list[int] = []
                original_dup = os.dup

                def recording_dup(descriptor: int) -> int:
                    duplicate = original_dup(descriptor)
                    duplicates.append(duplicate)
                    return duplicate

                with monkeypatch.context() as patcher:
                    patcher.setattr(os, "dup", recording_dup)
                    patcher.setattr(
                        os,
                        "fdopen",
                        lambda *_args, **_kwargs: (_ for _ in ()).throw(
                            OSError("injected")
                        ),
                    )
                    with pytest.raises(authority.AuthorityIOError):
                        bound.duplicate_binary()
                assert len(duplicates) == 1
                with pytest.raises(OSError) as captured:
                    os.fstat(duplicates[0])
                assert captured.value.errno == errno.EBADF


def test_directory_listing_is_bounded_sorted_and_identity_backed(tmp_path: Path):
    with authority.DirectoryAuthority.open_absolute(tmp_path) as root:
        _write(root, "z-file", b"z")
        directory = root.create_directory("a-directory")
        directory.close()

        entries = root.list_entries(maximum_entries=2)

        assert [entry.name for entry in entries] == ["a-directory", "z-file"]
        assert [entry.identity.kind for entry in entries] == [
            "directory",
            "regular_file",
        ]
        with pytest.raises(authority.AuthorityLimitExceeded):
            root.list_entries(maximum_entries=1)


def test_symlink_or_reparse_point_is_rejected(tmp_path: Path):
    real = tmp_path / "real"
    real.mkdir()
    indirect = tmp_path / "indirect"
    try:
        indirect.symlink_to(real, target_is_directory=True)
    except OSError as error:
        pytest.skip(f"link creation unavailable: {error}")

    with authority.DirectoryAuthority.open_absolute(tmp_path) as root:
        with pytest.raises(authority.AuthorityViolation):
            root.open_directory("indirect")
        with pytest.raises(authority.AuthorityViolation):
            root.entry_identity("indirect")


def test_file_publish_noreplace_success_and_conflict(tmp_path: Path):
    with authority.DirectoryAuthority.open_absolute(tmp_path) as root:
        source_identity = _write(root, "source", b"source")

        backend = root.publish_noreplace(
            "source", "destination", expected_source=source_identity
        )

        assert "noreplace" in backend
        assert root.exists("source") is False
        assert root.read_file("destination", maximum_bytes=16) == b"source"

        _write(root, "conflict-source", b"conflict")
        with pytest.raises(authority.AuthorityConflict):
            root.publish_noreplace("conflict-source", "destination")
        assert root.read_file("conflict-source", maximum_bytes=16) == b"conflict"
        assert root.read_file("destination", maximum_bytes=16) == b"source"


def test_publish_rejects_stale_expected_source_identity(tmp_path: Path):
    with authority.DirectoryAuthority.open_absolute(tmp_path) as root:
        first = _write(root, "first", b"first")
        second = _write(root, "second", b"second")

        with pytest.raises(
            authority.AuthorityViolation,
            match="publication_source_identity_changed",
        ):
            root.publish_noreplace(
                "first", "destination", expected_source=second
            )

        assert root.entry_identity("first").same_object(first)
        assert root.exists("destination") is False


def test_nonempty_directory_publish_noreplace_success_and_conflict(
    tmp_path: Path,
):
    with authority.DirectoryAuthority.open_absolute(tmp_path) as root:
        source = root.create_directory("source")
        source_identity = source.identity()
        _write(source, "sentinel", b"source")
        source.close()

        root.publish_noreplace(
            "source", "destination", expected_source=source_identity
        )
        assert root.exists("source") is False
        with root.open_directory("destination") as destination:
            assert destination.read_file("sentinel", maximum_bytes=16) == b"source"

        conflict_source = root.create_directory("conflict-source")
        _write(conflict_source, "source", b"source")
        conflict_source.close()
        conflict_destination = root.create_directory("conflict-destination")
        _write(conflict_destination, "destination", b"destination")
        conflict_destination.close()

        with pytest.raises(authority.AuthorityConflict):
            root.publish_noreplace("conflict-source", "conflict-destination")

        with root.open_directory("conflict-source") as surviving_source:
            assert surviving_source.read_file("source", maximum_bytes=16) == b"source"
        with root.open_directory("conflict-destination") as surviving_destination:
            assert (
                surviving_destination.read_file("destination", maximum_bytes=16)
                == b"destination"
            )


def test_reanchor_compares_stable_identity(tmp_path: Path):
    with authority.DirectoryAuthority.open_absolute(tmp_path) as root:
        initial = root.identity()
        reopened = root.reanchor()

        assert reopened.same_object(initial)


def test_ancestor_pin_is_descriptor_bound_or_rename_blocked(tmp_path: Path):
    with authority.DirectoryAuthority.open_absolute(tmp_path) as root:
        pinned = root.create_directory("pinned")
        pinned_identity = pinned.identity()
        if os.name == "nt":
            with pytest.raises(
                authority.AuthorityViolation,
                match="pinned_against_delete",
            ):
                root.publish_noreplace(
                    "pinned", "moved", expected_source=pinned_identity
                )
            pinned.close()
            root.publish_noreplace(
                "pinned", "moved", expected_source=pinned_identity
            )
            with root.open_directory("moved") as moved:
                _write(moved, "inside", b"windows-pin")
        else:
            root.publish_noreplace(
                "pinned", "moved", expected_source=pinned_identity
            )
            _write(pinned, "inside", b"posix-pin")
            pinned.close()
            with root.open_directory("moved") as moved:
                assert moved.read_file("inside", maximum_bytes=16) == b"posix-pin"


def test_remove_owned_tree_requires_exact_root_identity(tmp_path: Path):
    with authority.DirectoryAuthority.open_absolute(tmp_path) as root:
        owned = root.create_directory("owned")
        expected = owned.identity()
        _write(owned, "payload", b"payload")
        owned.close()
        wrong = replace(expected, file_id="00" * 16)

        with pytest.raises(authority.AuthorityViolation, match="identity_changed"):
            root.remove_owned_tree("owned", expected_root=wrong)

        assert root.exists("owned") is True
        root.remove_owned_tree("owned", expected_root=expected)
        assert root.exists("owned") is False


@pytest.mark.skipif(
    not Path("/proc/self/fd").is_dir(),
    reason="requires procfs descriptor accounting",
)
def test_post_create_directory_failure_removes_leaf_and_closes_descriptor(
    tmp_path: Path,
    monkeypatch,
):
    with authority.DirectoryAuthority.open_absolute(tmp_path) as root:
        before = len(tuple(Path("/proc/self/fd").iterdir()))
        original = authority.DirectoryAuthority._check_volume
        calls = 0

        def fail_once(
            self: authority.DirectoryAuthority,
            identity: authority.NodeIdentity,
        ) -> None:
            nonlocal calls
            calls += 1
            if calls == 1:
                raise authority.AuthorityViolation("injected_post_create")
            original(self, identity)

        with monkeypatch.context() as patcher:
            patcher.setattr(
                authority.DirectoryAuthority,
                "_check_volume",
                fail_once,
            )
            with pytest.raises(
                authority.AuthorityViolation,
                match="injected_post_create",
            ):
                root.create_directory("partial-directory")

        after = len(tuple(Path("/proc/self/fd").iterdir()))
        assert after == before
        assert root.exists("partial-directory") is False


def test_post_create_file_failure_removes_leaf(tmp_path: Path, monkeypatch):
    with authority.DirectoryAuthority.open_absolute(tmp_path) as root:
        original = authority.DirectoryAuthority._check_volume
        calls = 0

        def fail_once(
            self: authority.DirectoryAuthority,
            identity: authority.NodeIdentity,
        ) -> None:
            nonlocal calls
            calls += 1
            if calls == 1:
                raise authority.AuthorityViolation("injected_post_create")
            original(self, identity)

        with monkeypatch.context() as patcher:
            patcher.setattr(
                authority.DirectoryAuthority,
                "_check_volume",
                fail_once,
            )
            with pytest.raises(
                authority.AuthorityViolation,
                match="injected_post_create",
            ):
                root.create_file("partial-file")

        assert root.exists("partial-file") is False


@pytest.mark.parametrize("operation", ("directory", "file", "open"))
def test_returned_child_is_closed_when_parent_close_fails(
    tmp_path: Path, monkeypatch, operation: str
):
    original_close = authority.DirectoryAuthority.close
    original_file_close = authority.BoundFile.close
    closed_children: list[object] = []
    fail_next_parent = False

    def failing_close(self):
        nonlocal fail_next_parent
        if fail_next_parent and self is not root:
            fail_next_parent = False
            original_close(self)
            raise authority.AuthorityIOError("injected-parent-close")
        result = original_close(self)
        if self is not root:
            closed_children.append(self)
        return result

    def recording_file_close(self):
        result = original_file_close(self)
        closed_children.append(self)
        return result

    with authority.DirectoryAuthority.open_absolute(tmp_path) as root:
        if operation == "open":
            _write(root, "existing", b"value")
        monkeypatch.setattr(authority.DirectoryAuthority, "close", failing_close)
        monkeypatch.setattr(authority.BoundFile, "close", recording_file_close)
        if operation == "directory":
            fail_next_parent = True
            call = lambda: root.create_directory("created")
        elif operation == "file":
            fail_next_parent = True
            call = lambda: root.create_file("created")
        else:
            fail_next_parent = True
            call = lambda: root.open_file("existing")

        with pytest.raises(authority.AuthorityError, match="cleanup|parent-close"):
            call()

        assert closed_children
        assert all(getattr(item, "closed") for item in closed_children)
        if operation in ("directory", "file"):
            assert not root.exists("created")
        else:
            assert root.read_file("existing", maximum_bytes=5) == b"value"


def test_publish_close_failure_is_typed_postpublication(tmp_path: Path, monkeypatch):
    original_close = authority.DirectoryAuthority.close
    fail_once = True

    def failing_close(self):
        nonlocal fail_once
        original_close(self)
        if fail_once and self is not root:
            fail_once = False
            raise authority.AuthorityIOError("injected-publication-close")

    with authority.DirectoryAuthority.open_absolute(tmp_path) as root:
        expected = _write(root, "source", b"sealed")
        monkeypatch.setattr(authority.DirectoryAuthority, "close", failing_close)
        with pytest.raises(
            authority.AuthorityPostPublicationError,
            match="authority_published_but_cleanup_failed",
        ) as captured:
            root.publish_noreplace("source", "destination", expected_source=expected)

        assert captured.value.published_identity.same_object(expected)
        assert captured.value.source == authority.RelativePath.parse("source")
        assert captured.value.destination == authority.RelativePath.parse("destination")
        assert root.read_file("destination", maximum_bytes=6) == b"sealed"
        assert not root.exists("source")


def test_closed_handles_fail_closed(tmp_path: Path):
    root = authority.DirectoryAuthority.open_absolute(tmp_path)
    root.close()

    with pytest.raises(authority.AuthorityViolation, match="closed"):
        root.identity()


def test_real_capability_probe_is_clean_and_does_not_claim_integration(
    tmp_path: Path,
):
    capability = authority.probe_descriptor_relative_authority(tmp_path)

    assert capability.primitive_proven is True
    assert capability.backend in {
        "linux_openat2",
        "posix_openat",
        "windows_ntcreatefile",
    }
    assert capability.ancestor_pin_mode in {
        "descriptor_survived_rename",
        "rename_blocked_while_pinned",
    }
    evidence = capability.to_mapping()
    assert evidence["contract"] == authority.DESCRIPTOR_RELATIVE_AUTHORITY_CONTRACT
    assert evidence["primitive_proven"] is True
    assert evidence["simulator_integration_proven"] is False
    assert evidence["allowed_root_bootstrap_proven"] is True
    expected_bootstrap = (
        "nt_absolute_obj_dont_reparse"
        if os.name == "nt"
        else "filesystem_root_descriptor_walk_no_symlinks"
    )
    assert evidence["absolute_bootstrap"] == expected_bootstrap
    assert evidence["absolute_bootstrap_mode"] == expected_bootstrap
    assert evidence["absolute_ancestor_resolution_no_reparse_proven"] is True
    assert evidence["authority_root_identity_pinned"] is True
    assert evidence["final_output_race_exclusion_simulator_proven"] is False
    assert evidence["hostile_same_user_name_races_excluded"] is False
    assert evidence["unopened_ancestors_pinned"] is False
    assert evidence["directory_listing_identity_binding"] == (
        "reopen_each_name_beneath_pinned_parent"
    )
    assert evidence["local_acceptance_blockers_closed"] == []
    assert tuple(tmp_path.iterdir()) == ()


def test_probe_close_failure_still_removes_owned_tree_and_closes_root(
    tmp_path: Path,
    monkeypatch,
):
    original_close = authority.DirectoryAuthority.close
    original_create = authority.DirectoryAuthority.create_directory
    injected = False
    closed_bootstrap = False
    probe_authority = None

    def record_probe(self, relative, *, mode=0o700):
        nonlocal probe_authority
        result = original_create(self, relative, mode=mode)
        if self._anchor_path == tmp_path and str(relative).startswith(
            ".stv3-authority-"
        ):
            probe_authority = result
        return result

    def fail_probe_close(self):
        nonlocal injected, closed_bootstrap
        is_bootstrap = self._anchor_path == tmp_path
        original_close(self)
        closed_bootstrap = closed_bootstrap or is_bootstrap
        if self is probe_authority and not injected:
            injected = True
            raise authority.AuthorityIOError("injected-probe-close")

    monkeypatch.setattr(authority.DirectoryAuthority, "create_directory", record_probe)
    monkeypatch.setattr(authority.DirectoryAuthority, "close", fail_probe_close)
    with pytest.raises(
        authority.AuthorityIOError,
        match="descriptor_authority_probe_cleanup_failed",
    ) as captured:
        authority.probe_descriptor_relative_authority(tmp_path)

    assert injected is True
    assert closed_bootstrap is True
    assert tuple(tmp_path.iterdir()) == ()
    assert isinstance(captured.value.__cause__, BaseExceptionGroup)


def test_probe_collision_never_removes_preexisting_entry(
    tmp_path: Path,
    monkeypatch,
):
    fixed = "fixed"
    collision = tmp_path / f".stv3-authority-{fixed}"
    collision.mkdir()
    sentinel = collision / "sentinel"
    sentinel.write_bytes(b"preexisting")
    monkeypatch.setattr(
        authority.uuid,
        "uuid4",
        lambda: type("FixedUuid", (), {"hex": fixed})(),
    )

    with pytest.raises(authority.AuthorityConflict):
        authority.probe_descriptor_relative_authority(tmp_path)

    assert sentinel.read_bytes() == b"preexisting"


def test_source_contains_no_path_based_mutation_fallbacks():
    source = Path(authority.__file__).read_text(encoding="utf-8")
    forbidden = (
        "_win_open_accessible_prefix",
        "_win_try_open_absolute_directory",
        "_win_open_anchor",
        "os.rename(",
        "os.replace(",
        "Path.rename(",
        "Path.replace(",
        "shutil.move(",
        "MoveFileExW",
    )
    for token in forbidden:
        assert token not in source
    assert "dir_fd=" in source
    assert "RENAME_NOREPLACE" in source
    assert "RootDirectory" in source
    assert "FILE_OPEN_REPARSE_POINT" in source
    assert "FILE_SHARE_DELETE" in source


@pytest.mark.skipif(os.name != "nt", reason="Windows native rename ABI")
def test_windows_native_rename_is_handle_relative_and_no_replace(monkeypatch):
    captured: dict[str, object] = {}

    def observe(
        source_handle,
        io_status,
        buffer,
        length,
        info_class,
    ):
        info = authority._FILE_RENAME_INFO.from_buffer(buffer)
        encoded_length = int(info.FileNameLength)
        offset = authority._FILE_RENAME_INFO.FileName.offset
        captured.update(
            source=authority._win_handle_value(source_handle),
            target=authority._win_handle_value(info.RootDirectory),
            replace=int(info.ReplaceIfExists),
            length=int(length),
            name_length=encoded_length,
            name=ctypes.string_at(ctypes.addressof(buffer) + offset, encoded_length),
            tail=ctypes.string_at(
                ctypes.addressof(buffer) + offset + encoded_length,
                int(length) - offset - encoded_length,
            ),
            info_class=int(info_class),
        )
        return 0

    import ctypes

    with monkeypatch.context() as patcher:
        patcher.setattr(authority, "_NtSetInformationFile", observe)
        backend = authority._win_publish(101, 202, "destination")

    assert backend == "windows_ntsetinformationfile_noreplace"
    assert captured == {
        "source": 101,
        "target": 202,
        "replace": 0,
        "length": ctypes.sizeof(authority._FILE_RENAME_INFO)
        + len("destination".encode("utf-16-le")),
        "name_length": len("destination".encode("utf-16-le")),
        "name": "destination".encode("utf-16-le"),
        "tail": b"\0" * (
            ctypes.sizeof(authority._FILE_RENAME_INFO)
            - authority._FILE_RENAME_INFO.FileName.offset
        ),
        "info_class": authority._NATIVE_FILE_RENAME_INFORMATION_CLASS,
    }


@pytest.mark.skipif(os.name != "nt", reason="Windows ownership transfer")
def test_windows_constructor_failure_closes_each_handle_exactly_once(
    monkeypatch,
):
    closed: list[int] = []

    with monkeypatch.context() as patcher:
        patcher.setattr(
            authority,
            "_win_open_absolute_no_reparse",
            lambda _lexical: 101,
        )
        patcher.setattr(
            authority,
            "_win_identity",
            lambda *_args: (_ for _ in ()).throw(
                authority.AuthorityIOError("injected_constructor_failure")
            ),
        )
        patcher.setattr(authority, "_win_close", lambda handle: closed.append(handle))
        with pytest.raises(
            authority.AuthorityIOError,
            match="injected_constructor_failure",
        ):
            authority.DirectoryAuthority.open_absolute(r"C:\target")

    assert closed == [101]


@pytest.mark.skipif(os.name == "nt", reason="POSIX backend assertion")
def test_posix_backend_uses_openat_or_openat2(tmp_path: Path):
    with authority.DirectoryAuthority.open_absolute(tmp_path) as root:
        assert root.backend in {"linux_openat2", "posix_openat"}
        assert (
            root.absolute_bootstrap_mode
            == "filesystem_root_descriptor_walk_no_symlinks"
        )
        assert root.absolute_ancestor_resolution_no_reparse_proven is True
        assert root.authority_root_identity_pinned is True
        with root.create_directory("proof-child") as child:
            assert child.absolute_bootstrap_mode == root.absolute_bootstrap_mode
            assert child.absolute_ancestor_resolution_no_reparse_proven is True
            assert child.authority_root_identity_pinned is True
        root.fsync()


@pytest.mark.skipif(os.name != "nt", reason="Windows backend assertion")
def test_windows_backend_uses_root_directory_handles(tmp_path: Path):
    with authority.DirectoryAuthority.open_absolute(tmp_path) as root:
        assert root.backend == "windows_ntcreatefile"
        assert root.absolute_bootstrap_mode == "nt_absolute_obj_dont_reparse"
        assert root.absolute_ancestor_resolution_no_reparse_proven is True
        assert root.authority_root_identity_pinned is True
        assert root.native_directory_handle > 0
        _write(root, "payload", b"windows")
        assert [entry.name for entry in root.list_entries()] == ["payload"]
