from __future__ import annotations

import ast
import hashlib
import io
from pathlib import Path
from types import SimpleNamespace

import pytest

import descriptor_relative_authority as descriptor
import release_install_output_authority as output


def _path(value: str = "") -> descriptor.RelativePath:
    return descriptor.RelativePath.parse(value)


def _names(authority: output.ReleaseInstallOutputAuthority) -> list[str]:
    return [entry.name for entry in authority.list(maximum_entries=64)]


class _RecordingReader(io.BytesIO):
    def __init__(self, value: bytes) -> None:
        super().__init__(value)
        self.requests: list[int] = []

    def read(self, size: int = -1) -> bytes:
        self.requests.append(size)
        return super().read(size)


@pytest.mark.parametrize("failure_point", ("mode", "fsync", "constructor"))
def test_create_run_root_failure_removes_owned_directory_and_closes_handles(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure_point: str,
):
    tracked: list[descriptor.DirectoryAuthority] = []
    original_duplicate = descriptor.DirectoryAuthority.duplicate
    original_create = descriptor.DirectoryAuthority.create_directory

    def recording_duplicate(self):
        duplicate = original_duplicate(self)
        tracked.append(duplicate)
        return duplicate

    def recording_create(self, relative, *, mode=0o700):
        created = original_create(self, relative, mode=mode)
        tracked.append(created)
        return created

    monkeypatch.setattr(descriptor.DirectoryAuthority, "duplicate", recording_duplicate)
    monkeypatch.setattr(
        descriptor.DirectoryAuthority,
        "create_directory",
        recording_create,
    )
    if failure_point == "mode":
        monkeypatch.setattr(
            output,
            "_set_bound_directory_mode",
            lambda *_args: (_ for _ in ()).throw(
                output.OutputAuthorityIOError("injected-mode")
            ),
        )
    elif failure_point == "fsync":
        monkeypatch.setattr(
            output,
            "_flush_directory",
            lambda *_args: (_ for _ in ()).throw(
                output.OutputAuthorityIOError("injected-fsync")
            ),
        )
    else:
        monkeypatch.setattr(
            output.ReleaseInstallOutputAuthority,
            "__init__",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(
                RuntimeError("injected-constructor")
            ),
        )

    with descriptor.DirectoryAuthority.open_absolute(tmp_path) as allowed:
        expected = RuntimeError if failure_point == "constructor" else output.OutputAuthorityError
        with pytest.raises(expected):
            output.ReleaseInstallOutputAuthority.create_run_root(
                allowed,
                _path("run"),
            )

        assert allowed.list_entries(maximum_entries=1) == ()
        assert tracked
        assert all(authority.closed for authority in tracked)


def test_create_open_mkdir_write_read_list_fsync_and_reproof(tmp_path: Path):
    payload = b'{"status":"ready"}\n'
    with descriptor.DirectoryAuthority.open_absolute(tmp_path) as allowed:
        with output.ReleaseInstallOutputAuthority.create_run_root(
            allowed,
            _path("run"),
        ) as run:
            directory = run.mkdir(_path("journal"), mode=0o700)
            publication = run.write_bytes_noreplace(
                _path("journal/state.json"),
                payload,
                mode=0o600,
            )

            assert directory.identity.kind == "directory"
            assert publication.identity.size == len(payload)
            assert publication.stream.sha256 == hashlib.sha256(payload).hexdigest()
            assert publication.stream.read_calls >= 2
            assert run.read(_path("journal/state.json"), maximum_bytes=64) == payload
            assert _names(run) == ["journal"]
            assert run.fsync(_path("journal")) in {True, False}
            assert run.reprove_run_root().same_object(run.root_identity)
            identity = run.root_identity

        with output.ReleaseInstallOutputAuthority.open_run_root(
            allowed,
            _path("run"),
        ) as reopened:
            assert reopened.root_identity.same_object(identity)
            assert reopened.read(
                _path("journal/state.json"),
                maximum_bytes=64,
            ) == payload


@pytest.mark.parametrize("failure_point", ("mode", "fsync"))
def test_mkdir_postcreate_failure_cleans_exact_owned_directory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure_point: str,
):
    with descriptor.DirectoryAuthority.open_absolute(tmp_path) as allowed:
        with output.ReleaseInstallOutputAuthority.create_run_root(
            allowed, _path("run")
        ) as run:
            if failure_point == "mode":
                original = output._set_bound_directory_mode
                calls = 0

                def fail_once(*args):
                    nonlocal calls
                    calls += 1
                    if calls == 1:
                        raise output.OutputAuthorityIOError("injected-mode")
                    return original(*args)

                monkeypatch.setattr(output, "_set_bound_directory_mode", fail_once)
            else:
                original = output._flush_directory
                calls = 0

                def fail_once(*args):
                    nonlocal calls
                    calls += 1
                    if calls == 1:
                        raise output.OutputAuthorityIOError("injected-fsync")
                    return original(*args)

                monkeypatch.setattr(output, "_flush_directory", fail_once)

            with pytest.raises(
                output.OutputAuthorityIOError, match="injected"
            ) as captured:
                run.mkdir(_path("partial"))

            assert not isinstance(
                captured.value, output.OutputAuthorityPostCreateError
            )
            assert _names(run) == []


def test_mkdir_cleanup_never_removes_replacement(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    original_remove = descriptor.DirectoryAuthority.remove_owned_tree

    def replace_before_cleanup(self, relative, *, expected_root, maximum_entries=256):
        if str(relative) == "partial":
            original_remove(
                self,
                relative,
                expected_root=expected_root,
                maximum_entries=maximum_entries,
            )
            replacement = self.create_directory(relative)
            replacement.close()
        return original_remove(
            self,
            relative,
            expected_root=expected_root,
            maximum_entries=maximum_entries,
        )

    with descriptor.DirectoryAuthority.open_absolute(tmp_path) as allowed:
        with output.ReleaseInstallOutputAuthority.create_run_root(
            allowed, _path("run")
        ) as run:
            monkeypatch.setattr(
                output,
                "_set_bound_directory_mode",
                lambda *_args: (_ for _ in ()).throw(
                    output.OutputAuthorityIOError("injected-mode")
                ),
            )
            monkeypatch.setattr(
                descriptor.DirectoryAuthority,
                "remove_owned_tree",
                replace_before_cleanup,
            )
            with pytest.raises(
                output.OutputAuthorityPostCreateError,
                match="output_directory_created_but_cleanup_failed",
            ) as captured:
                run.mkdir(_path("partial"))

            replacement = run.root_identity
            assert captured.value.created_identity is not None
            assert captured.value.relative == _path("partial")
            assert run.list(maximum_entries=8)[0].name == "partial"
            assert replacement.kind == "directory"


def test_all_public_mutation_paths_require_relative_path_objects(tmp_path: Path):
    with descriptor.DirectoryAuthority.open_absolute(tmp_path) as allowed:
        with pytest.raises(TypeError, match="RelativePath"):
            output.ReleaseInstallOutputAuthority.create_run_root(allowed, "run")

        with output.ReleaseInstallOutputAuthority.create_run_root(
            allowed,
            _path("run"),
        ) as run:
            calls = (
                lambda: run.mkdir("directory"),
                lambda: run.write_bytes_noreplace("file", b"value"),
                lambda: run.stream_noreplace(
                    "stream",
                    io.BytesIO(b"value"),
                    expected_size=5,
                    maximum_size=5,
                ),
                lambda: run.read("file", maximum_bytes=5),
                lambda: run.list("", maximum_entries=1),
                lambda: run.fsync(""),
            )
            for call in calls:
                with pytest.raises(TypeError, match="RelativePath"):
                    call()
            assert _names(run) == []


def test_streaming_publication_is_bounded_hashed_and_identity_pinned(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    payload = b"abcdefghijklmnopqrstuvwxyz"
    reader = _RecordingReader(payload)
    publications: list[tuple[object, object, descriptor.NodeIdentity]] = []
    original_publish = descriptor.DirectoryAuthority.publish_noreplace

    def recording_publish(self, source, destination, *, expected_source=None):
        assert isinstance(source, descriptor.RelativePath)
        assert isinstance(destination, descriptor.RelativePath)
        assert isinstance(expected_source, descriptor.NodeIdentity)
        assert self.entry_identity(source).same_object(expected_source)
        publications.append((source, destination, expected_source))
        return original_publish(
            self,
            source,
            destination,
            expected_source=expected_source,
        )

    monkeypatch.setattr(
        descriptor.DirectoryAuthority,
        "publish_noreplace",
        recording_publish,
    )
    with descriptor.DirectoryAuthority.open_absolute(tmp_path) as allowed:
        with output.ReleaseInstallOutputAuthority.create_run_root(
            allowed,
            _path("run"),
        ) as run:
            result = run.stream_noreplace(
                _path("payload.bin"),
                reader,
                expected_size=len(payload),
                maximum_size=len(payload),
                expected_sha256=hashlib.sha256(payload).hexdigest(),
                forbidden_markers=(),
                chunk_bytes=5,
            )

            assert run.read(_path("payload.bin"), maximum_bytes=len(payload)) == payload
            assert result.stream.size == len(payload)
            assert result.stream.write_calls == 6
            assert max(reader.requests) <= 5
            assert reader.requests[-1] == 1
            assert len(publications) == 1
            assert publications[0][1] == _path("payload.bin")
            assert publications[0][2].same_object(result.identity)
            assert publications[0][0].leaf not in _names(run)
            assert _names(run) == ["payload.bin"]


def test_private_marker_split_across_chunks_fails_and_cleans_stage(tmp_path: Path):
    marker = b"-----BEGIN PRIVATE KEY-----"
    payload = b"prefix-" + marker + b"-suffix"
    with descriptor.DirectoryAuthority.open_absolute(tmp_path) as allowed:
        with output.ReleaseInstallOutputAuthority.create_run_root(
            allowed,
            _path("run"),
        ) as run:
            with pytest.raises(
                output.OutputAuthorityValidationError,
                match="output_forbidden_bytes",
            ):
                run.stream_noreplace(
                    _path("secret.pem"),
                    io.BytesIO(payload),
                    expected_size=len(payload),
                    maximum_size=len(payload),
                    chunk_bytes=11,
                )

            assert _names(run) == []


@pytest.mark.parametrize(
    ("source", "expected_size", "message"),
    (
        (io.BytesIO(b"short"), 6, "output_body_truncated"),
        (io.BytesIO(b"overrun"), 6, "output_body_exceeds_bound"),
    ),
)
def test_body_length_failures_leave_no_final_or_stage(
    tmp_path: Path,
    source: io.BytesIO,
    expected_size: int,
    message: str,
):
    with descriptor.DirectoryAuthority.open_absolute(tmp_path) as allowed:
        with output.ReleaseInstallOutputAuthority.create_run_root(
            allowed,
            _path("run"),
        ) as run:
            with pytest.raises(output.OutputAuthorityValidationError, match=message):
                run.stream_noreplace(
                    _path("payload"),
                    source,
                    expected_size=expected_size,
                    maximum_size=expected_size,
                    forbidden_markers=(),
                )
            assert _names(run) == []


@pytest.mark.parametrize("bad_value", (bytearray(b"x"), memoryview(b"x"), None))
def test_nonbytes_reader_is_rejected_and_stage_is_cleaned(
    tmp_path: Path,
    bad_value,
):
    class Reader:
        def read(self, _size):
            return bad_value

    with descriptor.DirectoryAuthority.open_absolute(tmp_path) as allowed:
        with output.ReleaseInstallOutputAuthority.create_run_root(
            allowed,
            _path("run"),
        ) as run:
            with pytest.raises(
                output.OutputAuthorityValidationError,
                match="output_source_returned_nonbytes",
            ):
                run.stream_noreplace(
                    _path("payload"),
                    Reader(),
                    expected_size=1,
                    maximum_size=1,
                    forbidden_markers=(),
                )
            assert _names(run) == []


def test_overreturning_reader_is_rejected_before_write(tmp_path: Path):
    class Reader:
        def read(self, size):
            return b"x" * (size + 1)

    with descriptor.DirectoryAuthority.open_absolute(tmp_path) as allowed:
        with output.ReleaseInstallOutputAuthority.create_run_root(
            allowed,
            _path("run"),
        ) as run:
            with pytest.raises(
                output.OutputAuthorityValidationError,
                match="output_read_bound_exceeded",
            ):
                run.stream_noreplace(
                    _path("payload"),
                    Reader(),
                    expected_size=2,
                    maximum_size=2,
                    forbidden_markers=(),
                    chunk_bytes=1,
                )
            assert _names(run) == []


def test_hash_mismatch_and_reader_exception_clean_the_private_stage(tmp_path: Path):
    class FailingReader:
        def read(self, _size):
            raise OSError("injected")

    with descriptor.DirectoryAuthority.open_absolute(tmp_path) as allowed:
        with output.ReleaseInstallOutputAuthority.create_run_root(
            allowed,
            _path("run"),
        ) as run:
            with pytest.raises(
                output.OutputAuthorityValidationError,
                match="output_sha256_mismatch",
            ):
                run.stream_noreplace(
                    _path("hash"),
                    io.BytesIO(b"payload"),
                    expected_size=7,
                    maximum_size=7,
                    expected_sha256="0" * 64,
                    forbidden_markers=(),
                )
            with pytest.raises(
                output.OutputAuthorityValidationError,
                match="output_source_read_failed",
            ):
                run.stream_noreplace(
                    _path("read"),
                    FailingReader(),
                    expected_size=1,
                    maximum_size=1,
                    forbidden_markers=(),
                )
            assert _names(run) == []


def test_existing_final_is_preserved_and_new_stage_is_cleaned(tmp_path: Path):
    with descriptor.DirectoryAuthority.open_absolute(tmp_path) as allowed:
        with output.ReleaseInstallOutputAuthority.create_run_root(
            allowed,
            _path("run"),
        ) as run:
            run.write_bytes_noreplace(_path("final"), b"original")

            with pytest.raises(
                output.OutputAuthorityConflict,
                match="output_destination_exists",
            ):
                run.write_bytes_noreplace(_path("final"), b"replacement")

            assert run.read(_path("final"), maximum_bytes=16) == b"original"
            assert _names(run) == ["final"]


def test_postpublication_reproof_failure_removes_exact_sealed_final(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    original_entry_identity = descriptor.DirectoryAuthority.entry_identity
    publication_completed = False
    reproof_failed = False

    def failing_postpublication_reproof(self, relative):
        nonlocal reproof_failed
        if publication_completed and not reproof_failed and str(relative) == "final":
            reproof_failed = True
            raise descriptor.AuthorityIOError("injected-reproof")
        return original_entry_identity(self, relative)

    original_publish = descriptor.DirectoryAuthority.publish_noreplace

    def recording_publish(self, source, destination, *, expected_source=None):
        nonlocal publication_completed
        backend = original_publish(
            self,
            source,
            destination,
            expected_source=expected_source,
        )
        publication_completed = True
        return backend

    monkeypatch.setattr(
        descriptor.DirectoryAuthority,
        "publish_noreplace",
        recording_publish,
    )
    monkeypatch.setattr(
        descriptor.DirectoryAuthority,
        "entry_identity",
        failing_postpublication_reproof,
    )
    with descriptor.DirectoryAuthority.open_absolute(tmp_path) as allowed:
        with output.ReleaseInstallOutputAuthority.create_run_root(
            allowed,
            _path("run"),
        ) as run:
            with pytest.raises(
                output.OutputAuthorityPostPublicationError,
                match="output_published_but_postproof_failed",
            ) as captured:
                run.write_bytes_noreplace(_path("final"), b"sealed")

            assert captured.value.published_identity.size == 6
            assert captured.value.stream.sha256 == hashlib.sha256(b"sealed").hexdigest()
            assert captured.value.destination == _path("final")
            assert captured.value.cleanup_succeeded is True
            assert (
                captured.value.cleanup_status
                == output.POSTPUBLICATION_CLEANUP_SUCCEEDED
            )
            assert _names(run) == []


def test_postpublication_directory_fsync_failure_removes_and_resyncs_final(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    original_publish = descriptor.DirectoryAuthority.publish_noreplace
    original_flush = output._flush_directory
    publication_completed = False
    injected = False
    postpublication_flushes = 0

    def recording_publish(self, source, destination, *, expected_source=None):
        nonlocal publication_completed
        backend = original_publish(
            self,
            source,
            destination,
            expected_source=expected_source,
        )
        publication_completed = True
        return backend

    def fail_first_postpublication_flush(authority):
        nonlocal injected, postpublication_flushes
        if publication_completed:
            postpublication_flushes += 1
            if not injected:
                injected = True
                raise output.OutputAuthorityIOError("injected-directory-fsync")
        return original_flush(authority)

    with descriptor.DirectoryAuthority.open_absolute(tmp_path) as allowed:
        with output.ReleaseInstallOutputAuthority.create_run_root(
            allowed, _path("run")
        ) as run:
            monkeypatch.setattr(
                descriptor.DirectoryAuthority,
                "publish_noreplace",
                recording_publish,
            )
            monkeypatch.setattr(
                output,
                "_flush_directory",
                fail_first_postpublication_flush,
            )

            with pytest.raises(
                output.OutputAuthorityPostPublicationError,
                match="output_published_but_postproof_failed",
            ) as captured:
                run.write_bytes_noreplace(_path("final"), b"sealed")

            assert injected is True
            assert postpublication_flushes == 2
            assert captured.value.destination == _path("final")
            assert captured.value.cleanup_succeeded is True
            assert _names(run) == []


@pytest.mark.parametrize("postproof_fails", (False, True))
def test_parent_close_after_publication_preserves_postpublication_classification(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    postproof_fails: bool,
):
    original_close = descriptor.DirectoryAuthority.close
    original_identity = descriptor.DirectoryAuthority.entry_identity
    publication_completed = False
    close_failed = False
    identity_failed = False

    def record_publish(self, source, destination, *, expected_source=None):
        nonlocal publication_completed
        backend = original_publish(
            self, source, destination, expected_source=expected_source
        )
        publication_completed = True
        return backend

    def fail_identity(self, relative):
        nonlocal identity_failed
        if (
            postproof_fails
            and publication_completed
            and not identity_failed
            and str(relative) == "final"
        ):
            identity_failed = True
            raise descriptor.AuthorityIOError("injected-postproof")
        return original_identity(self, relative)

    def fail_close(self):
        nonlocal close_failed
        if publication_completed and not close_failed and self is not allowed:
            close_failed = True
            raise descriptor.AuthorityIOError("injected-parent-close")
        original_close(self)

    original_publish = descriptor.DirectoryAuthority.publish_noreplace
    with descriptor.DirectoryAuthority.open_absolute(tmp_path) as allowed:
        with output.ReleaseInstallOutputAuthority.create_run_root(
            allowed, _path("run")
        ) as run:
            monkeypatch.setattr(
                descriptor.DirectoryAuthority, "publish_noreplace", record_publish
            )
            monkeypatch.setattr(
                descriptor.DirectoryAuthority, "entry_identity", fail_identity
            )
            monkeypatch.setattr(descriptor.DirectoryAuthority, "close", fail_close)

            with pytest.raises(
                output.OutputAuthorityPostPublicationError,
                match="output_published_but_postproof_failed",
            ) as captured:
                run.write_bytes_noreplace(_path("final"), b"sealed")

            assert captured.value.published_identity.size == 6
            assert captured.value.destination == _path("final")
            assert captured.value.cleanup_succeeded is True
            assert captured.value.cleanup_status == "succeeded"
            assert _names(run) == []


def test_postpublication_cleanup_never_removes_replacement(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    original_entry_identity = descriptor.DirectoryAuthority.entry_identity
    original_publish = descriptor.DirectoryAuthority.publish_noreplace
    original_remove = descriptor.DirectoryAuthority.remove_owned_tree
    publication_completed = False
    proof_failed = False
    replacement_written = False

    def recording_publish(self, source, destination, *, expected_source=None):
        nonlocal publication_completed
        backend = original_publish(
            self,
            source,
            destination,
            expected_source=expected_source,
        )
        publication_completed = True
        return backend

    def fail_final_proof_once(self, relative):
        nonlocal proof_failed
        if publication_completed and not proof_failed and str(relative) == "final":
            proof_failed = True
            raise descriptor.AuthorityIOError("injected-final-proof")
        return original_entry_identity(self, relative)

    def replace_before_cleanup(
        self,
        relative,
        *,
        expected_root,
        maximum_entries=256,
    ):
        nonlocal replacement_written
        if (
            not replacement_written
            and isinstance(relative, descriptor.RelativePath)
            and str(relative) == "final"
        ):
            original_publish(
                self,
                relative,
                _path("displaced"),
                expected_source=expected_root,
            )
            with self.create_file(relative) as replacement:
                replacement.write_all(b"contender")
                replacement.flush()
            replacement_written = True
        return original_remove(
            self,
            relative,
            expected_root=expected_root,
            maximum_entries=maximum_entries,
        )

    with descriptor.DirectoryAuthority.open_absolute(tmp_path) as allowed:
        with output.ReleaseInstallOutputAuthority.create_run_root(
            allowed, _path("run")
        ) as run:
            monkeypatch.setattr(
                descriptor.DirectoryAuthority,
                "publish_noreplace",
                recording_publish,
            )
            monkeypatch.setattr(
                descriptor.DirectoryAuthority,
                "entry_identity",
                fail_final_proof_once,
            )
            monkeypatch.setattr(
                descriptor.DirectoryAuthority,
                "remove_owned_tree",
                replace_before_cleanup,
            )

            with pytest.raises(
                output.OutputAuthorityPostPublicationError,
                match="output_published_but_postproof_failed",
            ) as captured:
                run.write_bytes_noreplace(_path("final"), b"sealed")

            assert replacement_written is True
            assert captured.value.destination == _path("final")
            assert captured.value.cleanup_succeeded is False
            assert (
                captured.value.cleanup_status
                == output.POSTPUBLICATION_CLEANUP_FAILED
            )
            assert run.read(_path("final"), maximum_bytes=9) == b"contender"
            assert run.read(_path("displaced"), maximum_bytes=6) == b"sealed"
            assert _names(run) == ["displaced", "final"]


def test_stage_collision_budget_never_removes_preexisting_collision(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    fixed = "a" * 32
    stage_name = f".stv3-output-{fixed}.pending"
    with descriptor.DirectoryAuthority.open_absolute(tmp_path) as allowed:
        with output.ReleaseInstallOutputAuthority.create_run_root(
            allowed,
            _path("run"),
        ) as run:
            run.write_bytes_noreplace(_path(stage_name), b"owned-elsewhere")
            monkeypatch.setattr(
                output.uuid,
                "uuid4",
                lambda: SimpleNamespace(hex=fixed),
            )

            with pytest.raises(
                output.OutputAuthorityConflict,
                match="output_stage_names_exhausted",
            ):
                run.write_bytes_noreplace(_path("final"), b"payload")

            assert run.read(_path(stage_name), maximum_bytes=32) == b"owned-elsewhere"
            assert _names(run) == [stage_name]


def test_cleanup_failure_is_fixed_error_and_never_publishes_final(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    original_remove = descriptor.DirectoryAuthority.remove_owned_tree

    def failing_remove(self, relative, *, expected_root, maximum_entries=256):
        if isinstance(relative, descriptor.RelativePath) and relative.leaf.endswith(
            ".pending"
        ):
            raise descriptor.AuthorityIOError("injected-cleanup")
        return original_remove(
            self,
            relative,
            expected_root=expected_root,
            maximum_entries=maximum_entries,
        )

    monkeypatch.setattr(
        descriptor.DirectoryAuthority,
        "remove_owned_tree",
        failing_remove,
    )
    with descriptor.DirectoryAuthority.open_absolute(tmp_path) as allowed:
        with output.ReleaseInstallOutputAuthority.create_run_root(
            allowed,
            _path("run"),
        ) as run:
            with pytest.raises(
                output.OutputAuthorityIOError,
                match="output_stage_cleanup_failed",
            ):
                run.stream_noreplace(
                    _path("final"),
                    io.BytesIO(b"short"),
                    expected_size=6,
                    maximum_size=6,
                    forbidden_markers=(),
                )

            assert "final" not in _names(run)
            assert any(name.endswith(".pending") for name in _names(run))


def test_owned_run_root_cleanup_is_identity_pinned(tmp_path: Path):
    with descriptor.DirectoryAuthority.open_absolute(tmp_path) as allowed:
        run = output.ReleaseInstallOutputAuthority.create_run_root(
            allowed,
            _path("run"),
        )
        run.write_bytes_noreplace(_path("payload"), b"value")

        assert run.remove_created_run_root(maximum_entries=2) in {True, False}
        assert run.closed is True
        assert allowed.list_entries(maximum_entries=1) == ()

        with allowed.create_directory(_path("existing")):
            pass
        with output.ReleaseInstallOutputAuthority.open_run_root(
            allowed,
            _path("existing"),
        ) as reopened:
            with pytest.raises(
                output.OutputAuthorityValidationError,
                match="output_run_root_not_owned",
            ):
                reopened.remove_created_run_root(maximum_entries=1)


def test_module_has_no_absolute_reopen_and_makes_no_blocker_claim():
    source = Path(output.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)
    called_attributes = {
        node.func.attr
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
    }
    imported_names = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, (ast.Import, ast.ImportFrom))
        for alias in node.names
    }

    assert "open_absolute" not in called_attributes
    assert "pathlib" not in imported_names
    evidence = output.output_authority_evidence()
    assert evidence["absolute_path_reopen"] is False
    assert evidence["expected_source_identity_required"] is True
    assert evidence["process_lease_integration_proven"] is False
    assert evidence["atomic_publication_adapter_integrated"] is False
    assert evidence["postcreate_failure_outcomes_classified"] is True
    assert evidence["postpublication_failure_outcomes_classified"] is True
    assert evidence[
        "postcreate_and_postpublication_outcomes_classified"
    ] is (
        evidence["postcreate_failure_outcomes_classified"]
        and evidence["postpublication_failure_outcomes_classified"]
    )
    assert evidence["postpublication_final_cleanup_proven"] is True
    assert evidence["publication_parent_identity_pinned"] is True
    assert evidence[
        "source_consumption_and_destination_identity_reproved"
    ] is True
    assert evidence["simulator_integration_proven"] is False
    assert evidence["local_acceptance_blockers_closed"] == []
