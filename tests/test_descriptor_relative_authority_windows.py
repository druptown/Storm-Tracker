"""Current-host Windows red-team coverage for descriptor authority.

The implementation lives only in ``descriptor_relative_authority``.  This
separate file adds Windows-native behavioral coverage without creating a
second backend or claiming installer integration.
"""
from __future__ import annotations

import ctypes
from ctypes import wintypes
from dataclasses import replace
import os
from pathlib import Path
import subprocess

import pytest

import descriptor_relative_authority as authority


pytestmark = pytest.mark.skipif(os.name != "nt", reason="Windows-only authority")


FILE_ATTRIBUTE_REPARSE_POINT = 0x00000400
FILE_FLAG_BACKUP_SEMANTICS = 0x02000000
FILE_FLAG_OPEN_REPARSE_POINT = 0x00200000
FILE_READ_ATTRIBUTES = 0x0080
FILE_SHARE_READ = 0x00000001
FILE_SHARE_WRITE = 0x00000002
OPEN_EXISTING = 3
ERROR_SHARING_VIOLATION = 32


def _kernel32():
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.CreateFileW.argtypes = (
        wintypes.LPCWSTR,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.LPVOID,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.HANDLE,
    )
    kernel32.CreateFileW.restype = wintypes.HANDLE
    kernel32.CloseHandle.argtypes = (wintypes.HANDLE,)
    kernel32.CloseHandle.restype = wintypes.BOOL
    return kernel32


def _attempt_delete_capable_open(path: Path) -> int:
    kernel32 = _kernel32()
    ctypes.set_last_error(0)
    handle = kernel32.CreateFileW(
        str(path),
        0x00010000 | FILE_READ_ATTRIBUTES,
        FILE_SHARE_READ | FILE_SHARE_WRITE,
        None,
        OPEN_EXISTING,
        FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT,
        None,
    )
    value = getattr(handle, "value", handle)
    if value == ctypes.c_void_p(-1).value:
        return 0
    return int(value or 0)


def test_windows_root_and_child_handles_are_identity_bound(
    tmp_path: Path,
    monkeypatch,
):
    object_attributes_seen: list[tuple[int, int]] = []
    native_create = authority._NtCreateFile

    def observe_ntcreatefile(*arguments):
        object_attributes = ctypes.cast(
            arguments[2],
            ctypes.POINTER(authority._OBJECT_ATTRIBUTES),
        ).contents
        root_handle = getattr(
            object_attributes.RootDirectory,
            "value",
            object_attributes.RootDirectory,
        )
        object_attributes_seen.append(
            (int(root_handle or 0), int(object_attributes.Attributes))
        )
        return native_create(*arguments)

    monkeypatch.setattr(authority, "_NtCreateFile", observe_ntcreatefile)
    with authority.DirectoryAuthority.open_absolute(tmp_path) as root:
        assert root.backend == "windows_ntcreatefile"
        root_identity = root.identity()
        with root.create_directory("child") as child:
            assert child.absolute_bootstrap_mode == root.absolute_bootstrap_mode
            assert child.absolute_ancestor_resolution_no_reparse_proven is True
            assert child.authority_root_identity_pinned is True
            child_identity = child.identity()
            with child.create_file("payload") as payload:
                payload.write_all(b"identity-bound")
                payload.flush()
                file_identity = payload.identity()

            assert child.reanchor(tmp_path / "child").same_object(child_identity)
            assert child.entry_identity("payload").same_object(file_identity)
        assert root.reanchor().same_object(root_identity)
    assert object_attributes_seen
    assert object_attributes_seen[0] == (
        0,
        authority._OBJ_CASE_INSENSITIVE | authority._OBJ_DONT_REPARSE,
    )
    assert any(root_handle for root_handle, _attributes in object_attributes_seen)


def test_windows_open_reparse_point_is_observed_then_rejected(tmp_path: Path):
    target = tmp_path / "target"
    nested = target / "nested"
    nested.mkdir(parents=True)
    indirect = tmp_path / "indirect"
    created = subprocess.run(
        ["cmd.exe", "/d", "/c", "mklink", "/J", str(indirect), str(target)],
        check=False,
        capture_output=True,
        text=True,
        creationflags=subprocess.CREATE_NO_WINDOW,
    )
    if created.returncode != 0:
        pytest.fail(
            "directory junction creation failed: "
            f"{created.stderr.strip() or created.stdout.strip()}"
        )
    try:
        assert indirect.lstat().st_file_attributes & FILE_ATTRIBUTE_REPARSE_POINT
        with pytest.raises(authority.AuthorityViolation, match="reparse"):
            authority.DirectoryAuthority.open_absolute(indirect)
        with pytest.raises(authority.AuthorityViolation, match="reparse"):
            authority.DirectoryAuthority.open_absolute(indirect / "nested")
        with authority.DirectoryAuthority.open_absolute(tmp_path) as root:
            with pytest.raises(authority.AuthorityViolation):
                root.open_directory("indirect")
            with pytest.raises(authority.AuthorityViolation):
                root.entry_identity("indirect")
    finally:
        indirect.rmdir()


def test_windows_pinned_authority_root_excludes_delete_share(tmp_path: Path):
    parent = tmp_path / "parent"
    child = parent / "child"
    child.mkdir(parents=True)

    with authority.DirectoryAuthority.open_absolute(child) as opened:
        assert opened.identity().kind == "directory"
        competing = _attempt_delete_capable_open(child)
        error_code = ctypes.get_last_error()
        if competing:
            _kernel32().CloseHandle(competing)
            pytest.fail("pinned ancestor unexpectedly admitted DELETE access")
        assert error_code == ERROR_SHARING_VIOLATION
        with pytest.raises(OSError) as rename_error:
            os.rename(child, parent / "child-moved")
        assert rename_error.value.winerror == ERROR_SHARING_VIOLATION
        assert child.is_dir()


def test_windows_absolute_ntcreatefile_abi_is_fail_closed(monkeypatch):
    captured: dict[str, object] = {}
    closed: list[int] = []

    def observe(*arguments):
        result_handle = ctypes.cast(
            arguments[0],
            ctypes.POINTER(authority._HANDLE),
        )
        result_handle.contents.value = 31337
        attributes = ctypes.cast(
            arguments[2],
            ctypes.POINTER(authority._OBJECT_ATTRIBUTES),
        ).contents
        name = attributes.ObjectName.contents
        captured.update(
            root=authority._win_handle_value(attributes.RootDirectory),
            attributes=int(attributes.Attributes),
            name=ctypes.wstring_at(name.Buffer, int(name.Length) // 2),
            desired_access=int(arguments[1]),
            file_attributes=int(arguments[5]),
            share=int(arguments[6]),
            disposition=int(arguments[7]),
            options=int(arguments[8]),
        )
        return 0

    identity = authority.NodeIdentity(
        backend="windows_ntcreatefile",
        volume=1,
        file_id="01" * 16,
        kind="directory",
        link_count=1,
        size=0,
    )
    with monkeypatch.context() as patcher:
        patcher.setattr(authority, "_NtCreateFile", observe)
        patcher.setattr(authority, "_win_identity", lambda *_args: identity)
        patcher.setattr(authority, "_win_close", lambda handle: closed.append(handle))
        handle = authority._win_open_absolute_no_reparse(r"C:\one\two")
        authority._win_close(handle)

    assert handle == 31337
    assert captured == {
        "root": 0,
        "attributes": authority._OBJ_CASE_INSENSITIVE
        | authority._OBJ_DONT_REPARSE,
        "name": "\\??\\C:\\one\\two",
        "desired_access": authority._win_directory_access(mutable=True),
        "file_attributes": authority._FILE_ATTRIBUTE_DIRECTORY,
        "share": authority._FILE_SHARE_READ | authority._FILE_SHARE_WRITE,
        "disposition": authority._FILE_OPEN,
        "options": authority._FILE_DIRECTORY_FILE
        | authority._FILE_SYNCHRONOUS_IO_NONALERT
        | authority._FILE_OPEN_FOR_BACKUP_INTENT
        | authority._FILE_OPEN_REPARSE_POINT,
    }
    assert closed == [31337]


@pytest.mark.parametrize(
    "status",
    (
        ctypes.c_int32(0xC000050B).value,
        0xC000050B,
    ),
)
def test_windows_signed_reparse_status_is_always_a_violation(
    monkeypatch,
    status: int,
):
    with monkeypatch.context() as patcher:
        patcher.setattr(authority, "_NtCreateFile", lambda *_args: status)
        patcher.setattr(
            authority,
            "_win_error_from_ntstatus",
            lambda _status: (_ for _ in ()).throw(
                AssertionError("fixed reparse status must not use DOS mapping")
            ),
        )
        with pytest.raises(
            authority.AuthorityViolation,
            match="reparse_point_encountered",
        ):
            authority._win_open_absolute_no_reparse(r"C:\target")


def test_windows_failed_status_closes_any_returned_handle(monkeypatch):
    closed: list[int] = []

    def fail_with_handle(*arguments):
        result_handle = ctypes.cast(
            arguments[0],
            ctypes.POINTER(authority._HANDLE),
        )
        result_handle.contents.value = 5150
        return ctypes.c_int32(0xC000050B).value

    with monkeypatch.context() as patcher:
        patcher.setattr(authority, "_NtCreateFile", fail_with_handle)
        patcher.setattr(authority, "_win_close", lambda handle: closed.append(handle))
        with pytest.raises(authority.AuthorityViolation, match="reparse"):
            authority._win_open_absolute_no_reparse(r"C:\target")

    assert closed == [5150]


def test_windows_relative_failed_status_closes_any_returned_handle(monkeypatch):
    closed: list[int] = []

    def fail_with_handle(*arguments):
        result_handle = ctypes.cast(
            arguments[0],
            ctypes.POINTER(authority._HANDLE),
        )
        result_handle.contents.value = 6161
        return ctypes.c_int32(0xC000050B).value

    with monkeypatch.context() as patcher:
        patcher.setattr(authority, "_NtCreateFile", fail_with_handle)
        patcher.setattr(authority, "_win_close", lambda handle: closed.append(handle))
        with pytest.raises(authority.AuthorityViolation, match="reparse"):
            authority._win_nt_open(
                111,
                "target",
                desired_access=authority._FILE_READ_ATTRIBUTES,
                disposition=authority._FILE_OPEN,
                directory=None,
            )

    assert closed == [6161]


def test_windows_absolute_open_closes_handle_after_identity_failure(monkeypatch):
    closed: list[int] = []

    def succeed(*arguments):
        result_handle = ctypes.cast(
            arguments[0],
            ctypes.POINTER(authority._HANDLE),
        )
        result_handle.contents.value = 4242
        return 0

    with monkeypatch.context() as patcher:
        patcher.setattr(authority, "_NtCreateFile", succeed)
        patcher.setattr(
            authority,
            "_win_identity",
            lambda *_args: (_ for _ in ()).throw(
                authority.AuthorityViolation("injected_identity_failure")
            ),
        )
        patcher.setattr(authority, "_win_close", lambda handle: closed.append(handle))
        with pytest.raises(
            authority.AuthorityViolation,
            match="injected_identity_failure",
        ):
            authority._win_open_absolute_no_reparse(r"C:\target")

    assert closed == [4242]


def test_windows_ctypes_layout_matches_sdk_boolean_fields():
    assert authority._FILE_STANDARD_INFO.DeletePending.offset == 20
    assert authority._FILE_STANDARD_INFO.Directory.offset == 21
    assert ctypes.sizeof(authority._FILE_STANDARD_INFO) == 24
    assert ctypes.sizeof(authority._FILE_DISPOSITION_INFO) == 1


def _create_publish_source(
    root: authority.DirectoryAuthority,
) -> authority.NodeIdentity:
    with root.create_file("source") as source:
        source.write_all(b"source")
        source.flush()
        return source.identity()


def test_windows_access_denied_publish_retries_once_with_same_pinned_handle(
    tmp_path: Path,
    monkeypatch,
):
    calls: list[tuple[int, int, str]] = []
    source_open_delete_shares: list[bool] = []
    native_nt_open = authority._win_nt_open

    def observe_source_opens(parent_handle: int, name: str, **options) -> int:
        if name == "source":
            source_open_delete_shares.append(bool(options.get("share_delete", False)))
        return native_nt_open(parent_handle, name, **options)

    def deny_once_then_succeed(
        source_handle: int,
        destination_parent_handle: int,
        destination_name: str,
    ) -> str:
        calls.append((source_handle, destination_parent_handle, destination_name))
        if len(calls) == 1:
            raise authority.AuthorityIOError(
                "windows_ntsetinformationfile_rename_failed_winerror_5"
            )
        return "windows_mock_noreplace"

    with authority.DirectoryAuthority.open_absolute(tmp_path) as root:
        expected = _create_publish_source(root)
        monkeypatch.setattr(authority, "_win_nt_open", observe_source_opens)
        monkeypatch.setattr(authority, "_win_publish", deny_once_then_succeed)

        backend = root.publish_noreplace(
            "source", "destination", expected_source=expected
        )

    assert backend == "windows_mock_noreplace"
    assert len(calls) == 2
    assert calls[0][0] == calls[1][0]
    assert calls[0][1:] == calls[1][1:]
    assert source_open_delete_shares == [True, True, True]


def test_windows_repeated_access_denied_stops_after_second_attempt(
    tmp_path: Path,
    monkeypatch,
):
    source_handles: list[int] = []
    closed_handles: list[int] = []
    native_close = authority._win_close

    def observe_close(handle: int) -> None:
        closed_handles.append(handle)
        native_close(handle)

    def always_deny(
        source_handle: int,
        _destination_parent_handle: int,
        _destination_name: str,
    ) -> str:
        source_handles.append(source_handle)
        raise authority.AuthorityIOError(
            "windows_ntsetinformationfile_rename_failed_winerror_5"
        )

    with authority.DirectoryAuthority.open_absolute(tmp_path) as root:
        expected = _create_publish_source(root)
        monkeypatch.setattr(authority, "_win_close", observe_close)
        monkeypatch.setattr(authority, "_win_publish", always_deny)

        with pytest.raises(
            authority.AuthorityIOError,
            match="rename_failed_winerror_5",
        ):
            root.publish_noreplace(
                "source", "destination", expected_source=expected
            )

    assert len(source_handles) == 2
    assert source_handles[0] == source_handles[1]
    assert closed_handles.count(source_handles[0]) == 1


def test_windows_publish_conflict_is_not_retried(
    tmp_path: Path,
    monkeypatch,
):
    publish_calls = 0

    def conflict(
        _source_handle: int,
        _destination_parent_handle: int,
        _destination_name: str,
    ) -> str:
        nonlocal publish_calls
        publish_calls += 1
        raise authority.AuthorityConflict("authority_destination_exists")

    with authority.DirectoryAuthority.open_absolute(tmp_path) as root:
        _create_publish_source(root)
        monkeypatch.setattr(authority, "_win_publish", conflict)

        with pytest.raises(
            authority.AuthorityConflict,
            match="authority_destination_exists",
        ):
            root.publish_noreplace("source", "destination")

    assert publish_calls == 1


def test_windows_access_denied_destination_appearance_aborts_retry(
    tmp_path: Path,
    monkeypatch,
):
    native_nt_open = authority._win_nt_open
    denied = False
    publish_calls = 0

    def deny(
        _source_handle: int,
        _destination_parent_handle: int,
        _destination_name: str,
    ) -> str:
        nonlocal denied, publish_calls
        publish_calls += 1
        denied = True
        raise authority.AuthorityIOError(
            "windows_ntsetinformationfile_rename_failed_winerror_5"
        )

    def destination_appears(parent_handle: int, name: str, **options) -> int:
        if denied and name == "destination":
            return native_nt_open(parent_handle, "source", **options)
        return native_nt_open(parent_handle, name, **options)

    with authority.DirectoryAuthority.open_absolute(tmp_path) as root:
        _create_publish_source(root)
        monkeypatch.setattr(authority, "_win_publish", deny)
        monkeypatch.setattr(authority, "_win_nt_open", destination_appears)

        with pytest.raises(
            authority.AuthorityConflict,
            match="authority_destination_exists",
        ):
            root.publish_noreplace("source", "destination")

    assert publish_calls == 1


@pytest.mark.parametrize("changed_proof", ("handle", "name"))
def test_windows_access_denied_source_identity_change_aborts_retry(
    tmp_path: Path,
    monkeypatch,
    changed_proof: str,
):
    native_identity = authority._win_identity
    denied = False
    publish_calls = 0
    published_source_handle: int | None = None

    def deny(
        source_handle: int,
        _destination_parent_handle: int,
        _destination_name: str,
    ) -> str:
        nonlocal denied, publish_calls, published_source_handle
        publish_calls += 1
        published_source_handle = source_handle
        denied = True
        raise authority.AuthorityIOError(
            "windows_ntsetinformationfile_rename_failed_winerror_5"
        )

    def changing_handle_identity(handle: int, backend: str) -> authority.NodeIdentity:
        actual = native_identity(handle, backend)
        if denied and changed_proof == "handle" and handle == published_source_handle:
            return replace(actual, file_id="ff" * 16)
        if denied and changed_proof == "name" and handle != published_source_handle:
            return replace(actual, file_id="ee" * 16)
        return actual

    with authority.DirectoryAuthority.open_absolute(tmp_path) as root:
        _create_publish_source(root)
        monkeypatch.setattr(authority, "_win_publish", deny)
        monkeypatch.setattr(authority, "_win_identity", changing_handle_identity)

        with pytest.raises(
            authority.AuthorityViolation,
            match=rf"publication_source_{changed_proof}_identity_changed",
        ):
            root.publish_noreplace("source", "destination")

    assert publish_calls == 1


def test_windows_non_access_denied_publish_error_is_not_retried(
    tmp_path: Path,
    monkeypatch,
):
    publish_calls = 0

    def fail_once(
        _source_handle: int,
        _destination_parent_handle: int,
        _destination_name: str,
    ) -> str:
        nonlocal publish_calls
        publish_calls += 1
        raise authority.AuthorityIOError(
            "windows_ntsetinformationfile_rename_failed_winerror_50"
        )

    with authority.DirectoryAuthority.open_absolute(tmp_path) as root:
        _create_publish_source(root)
        monkeypatch.setattr(authority, "_win_publish", fail_once)

        with pytest.raises(
            authority.AuthorityIOError,
            match="rename_failed_winerror_50",
        ):
            root.publish_noreplace("source", "destination")

    assert publish_calls == 1


def test_windows_file_and_nonempty_directory_noreplace(tmp_path: Path):
    with authority.DirectoryAuthority.open_absolute(tmp_path) as root:
        with root.create_file("file-source") as source:
            source.write_all(b"source")
            source.flush()
        root.publish_noreplace("file-source", "file-destination")
        with root.create_file("file-conflict") as conflict:
            conflict.write_all(b"conflict")
            conflict.flush()
        with pytest.raises(authority.AuthorityConflict):
            root.publish_noreplace("file-conflict", "file-destination")
        assert root.read_file("file-conflict", maximum_bytes=16) == b"conflict"
        assert root.read_file("file-destination", maximum_bytes=16) == b"source"

        with root.create_directory("directory-source") as source_directory:
            with source_directory.create_file("sentinel") as sentinel:
                sentinel.write_all(b"source-directory")
                sentinel.flush()
        root.publish_noreplace("directory-source", "directory-destination")
        with root.create_directory("directory-conflict-source") as conflict_source:
            with conflict_source.create_file("source") as marker:
                marker.write_all(b"source")
                marker.flush()
        with root.create_directory("directory-conflict-destination") as conflict_dest:
            with conflict_dest.create_file("destination") as marker:
                marker.write_all(b"destination")
                marker.flush()
        with pytest.raises(authority.AuthorityConflict):
            root.publish_noreplace(
                "directory-conflict-source",
                "directory-conflict-destination",
            )
        with root.open_directory("directory-destination") as destination:
            assert destination.read_file("sentinel", maximum_bytes=32) == (
                b"source-directory"
            )
        with root.open_directory("directory-conflict-source") as source:
            assert source.read_file("source", maximum_bytes=16) == b"source"
        with root.open_directory("directory-conflict-destination") as destination:
            assert destination.read_file("destination", maximum_bytes=16) == (
                b"destination"
            )


def test_windows_probe_is_clean_and_claims_no_installer_blocker(tmp_path: Path):
    capability = authority.probe_descriptor_relative_authority(tmp_path)

    assert capability.backend == "windows_ntcreatefile"
    assert capability.primitive_proven is True
    assert capability.ancestor_pin_mode == "rename_blocked_while_pinned"
    evidence = capability.to_mapping()
    assert evidence["absolute_bootstrap_mode"] == "nt_absolute_obj_dont_reparse"
    assert evidence["absolute_ancestor_resolution_no_reparse_proven"] is True
    assert evidence["authority_root_identity_pinned"] is True
    assert evidence["allowed_root_bootstrap_proven"] is True
    assert evidence["simulator_integration_proven"] is False
    assert evidence["local_acceptance_blockers_closed"] == []
    assert tuple(tmp_path.iterdir()) == ()
