from __future__ import annotations

import ast
from collections.abc import Iterator, Mapping
import hashlib
import json
from pathlib import Path

import pytest

import release_install_bound_inputs as bound_inputs
from descriptor_relative_authority import DirectoryAuthority, RelativePath


ROOT = Path(__file__).resolve().parents[1]


class _SnapshotOnlyMapping(
    Mapping[str, bound_inputs.BoundInputDeclaration]
):
    """Mapping that permits exactly one bounded items() snapshot."""

    def __init__(
        self,
        values: dict[str, bound_inputs.BoundInputDeclaration],
    ) -> None:
        self._values = values
        self.items_calls = 0

    def __getitem__(self, key: str) -> bound_inputs.BoundInputDeclaration:
        raise AssertionError("registry must read its declaration snapshot")

    def __iter__(self) -> Iterator[str]:
        raise AssertionError("registry must not reiterate adversarial keys")

    def __len__(self) -> int:
        raise AssertionError("registry must not trust adversarial length")

    def items(self):
        self.items_calls += 1
        if self.items_calls != 1:
            raise AssertionError("declaration mapping was materialized twice")
        return tuple(self._values.items())


def _sha(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _create(
    authority: DirectoryAuthority,
    name: str,
    value: bytes,
    *,
    maximum_bytes: int | None = None,
) -> bound_inputs.BoundInputDeclaration:
    relative = RelativePath.parse(name)
    with authority.create_file(relative) as target:
        target.write_all(value)
        target.flush()
        identity = target.identity()
    return bound_inputs.BoundInputDeclaration(
        relative=relative,
        identity=identity,
        size=len(value),
        sha256=_sha(value),
        maximum_bytes=(len(value) if maximum_bytes is None else maximum_bytes),
    )


def _registry(
    tmp_path: Path,
    values: dict[str, bytes],
) -> tuple[DirectoryAuthority, bound_inputs.BoundInputRegistry]:
    authority = DirectoryAuthority.open_absolute(tmp_path)
    declarations = {
        key: _create(authority, f"{key}.bin", value)
        for key, value in values.items()
    }
    registry = bound_inputs.BoundInputRegistry.seal(
        authority,
        declarations,
        expected_keys=values,
    )
    return authority, registry


def test_registry_is_path_free_exact_and_protocol_compatible(tmp_path: Path):
    authority, registry = _registry(
        tmp_path,
        {"evidence": b'{"contract":"v1"}', "integration": b"archive"},
    )
    authority.close()
    try:
        assert isinstance(registry, bound_inputs.BoundInputSource)
        assert registry.keys == ("evidence", "integration")
        registry.require_exact_keys(["integration", "evidence"])
        with pytest.raises(bound_inputs.BoundInputContractError, match="keys differ"):
            registry.require_exact_keys(["evidence"])

        metadata = registry.metadata("integration")
        assert isinstance(metadata.relative, RelativePath)
        assert metadata.size == len(b"archive")
        assert metadata.sha256 == _sha(b"archive")
        assert "absolute" not in metadata.to_mapping()
        assert registry.evidence()["simulator_integration_proven"] is False
        assert registry.evidence()["path_api_used"] is False
    finally:
        registry.close()


def test_duplicate_stream_and_bounded_read_reprove_exact_bytes(tmp_path: Path):
    authority, registry = _registry(tmp_path, {"worker": b"worker-archive"})
    authority.close()
    try:
        with registry.open_stream("worker") as first:
            assert first.read(6) == b"worker"
        with registry.open_stream("worker") as second:
            assert second.read() == b"worker-archive"
        assert registry.read_bounded("worker", maximum_bytes=14) == b"worker-archive"
        with pytest.raises(bound_inputs.BoundInputContractError, match="read limit"):
            registry.read_bounded("worker", maximum_bytes=13)
    finally:
        registry.close()


def test_seal_rejects_wrong_identity_size_hash_and_keyset(tmp_path: Path):
    with DirectoryAuthority.open_absolute(tmp_path) as authority:
        declaration = _create(authority, "payload", b"payload", maximum_bytes=32)
        other = _create(authority, "other", b"payload", maximum_bytes=32)

        wrong_identity = bound_inputs.BoundInputDeclaration(
            relative=declaration.relative,
            identity=other.identity,
            size=other.size,
            sha256=other.sha256,
            maximum_bytes=other.maximum_bytes,
        )
        with pytest.raises(bound_inputs.BoundInputIntegrityError, match="identity"):
            bound_inputs.BoundInputRegistry.seal(
                authority,
                {"payload": wrong_identity},
                expected_keys={"payload"},
            )

        wrong_hash = bound_inputs.BoundInputDeclaration(
            relative=declaration.relative,
            identity=declaration.identity,
            size=declaration.size,
            sha256="0" * 64,
            maximum_bytes=declaration.maximum_bytes,
        )
        with pytest.raises(bound_inputs.BoundInputIntegrityError, match="size or SHA"):
            bound_inputs.BoundInputRegistry.seal(
                authority,
                {"payload": wrong_hash},
                expected_keys={"payload"},
            )

        with pytest.raises(bound_inputs.BoundInputContractError, match="keys differ"):
            bound_inputs.BoundInputRegistry.seal(
                authority,
                {"payload": declaration},
                expected_keys={"payload", "missing"},
            )


def test_seal_snapshots_an_adversarial_mapping_once(tmp_path: Path):
    with DirectoryAuthority.open_absolute(tmp_path) as authority:
        declaration = _create(authority, "payload", b"payload")
        declarations = _SnapshotOnlyMapping({"payload": declaration})
        registry = bound_inputs.BoundInputRegistry.seal(
            authority,
            declarations,
            expected_keys=("payload",),
        )
        try:
            assert declarations.items_calls == 1
            assert registry.read_bounded("payload", maximum_bytes=7) == b"payload"
        finally:
            registry.close()

def test_replaced_final_name_fails_sealed_identity(tmp_path: Path):
    authority, registry = _registry(tmp_path, {"evidence": b"same-bytes"})
    authority.close()
    original = tmp_path / "evidence.bin"
    moved = tmp_path / "old.bin"
    original.rename(moved)
    original.write_bytes(b"same-bytes")
    try:
        with pytest.raises(bound_inputs.BoundInputIntegrityError, match="identity"):
            with registry.open_stream("evidence"):
                pass
    finally:
        registry.close()


def test_in_place_change_during_use_fails_post_hash(tmp_path: Path):
    authority, registry = _registry(tmp_path, {"archive": b"before"})
    authority.close()
    try:
        with pytest.raises(bound_inputs.BoundInputIntegrityError, match="size or SHA"):
            with registry.open_stream("archive") as stream:
                assert stream.read() == b"before"
                (tmp_path / "archive.bin").write_bytes(b"after!")
    finally:
        registry.close()


def test_consumer_cannot_close_registry_owned_stream(tmp_path: Path):
    authority, registry = _registry(tmp_path, {"archive": b"value"})
    authority.close()
    try:
        with pytest.raises(bound_inputs.BoundInputIntegrityError, match="consumer closed"):
            with registry.open_stream("archive") as stream:
                stream.close()
    finally:
        registry.close()


def test_consumer_exception_does_not_bypass_post_use_reproof(tmp_path: Path):
    authority, registry = _registry(tmp_path, {"archive": b"before"})
    authority.close()
    try:
        sentinel = RuntimeError("consumer rejected input")
        with pytest.raises(
            bound_inputs.BoundInputIntegrityError, match="size or SHA"
        ) as raised:
            with registry.open_stream("archive") as stream:
                assert stream.read() == b"before"
                (tmp_path / "archive.bin").write_bytes(b"after!")
                raise sentinel
        assert raised.value.__cause__ is sentinel
    finally:
        registry.close()


def test_unchanged_consumer_exception_is_preserved(tmp_path: Path):
    authority, registry = _registry(tmp_path, {"archive": b"stable"})
    authority.close()
    sentinel = RuntimeError("consumer rejected stable input")
    try:
        with pytest.raises(RuntimeError) as raised:
            with registry.open_stream("archive") as stream:
                assert stream.read() == b"stable"
                raise sentinel
        assert raised.value is sentinel
    finally:
        registry.close()


def test_bounded_strict_json_requires_exact_top_level_keys(tmp_path: Path):
    value = json.dumps(
        {"archives": {"worker": "x"}, "contract": "v1"},
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    authority, registry = _registry(tmp_path, {"evidence": value})
    authority.close()
    try:
        document = registry.read_json_object(
            "evidence",
            maximum_bytes=len(value),
            expected_keys={"archives", "contract"},
        )
        assert document == {"archives": {"worker": "x"}, "contract": "v1"}
        with pytest.raises(bound_inputs.BoundInputContractError, match="keys differ"):
            registry.read_json_object(
                "evidence",
                maximum_bytes=len(value),
                expected_keys={"contract"},
            )
    finally:
        registry.close()


def test_strict_json_rejects_duplicates_floats_and_excess_depth(tmp_path: Path):
    values = {
        "duplicate": b'{"a":1,"a":2}',
        "floating": b'{"a":1.5}',
        "deep": (b'{"a":' + b"[" * 65 + b"0" + b"]" * 65 + b"}"),
    }
    authority, registry = _registry(tmp_path, values)
    authority.close()
    try:
        for key in ("duplicate", "floating", "deep"):
            with pytest.raises(bound_inputs.BoundInputContractError):
                registry.read_json_object(
                    key,
                    maximum_bytes=len(values[key]),
                    expected_keys={"a"},
                )
    finally:
        registry.close()


def test_registry_close_is_idempotent_and_streams_fail_closed(tmp_path: Path):
    authority, registry = _registry(tmp_path, {"input": b"value"})
    authority.close()
    registry.close()
    registry.close()
    assert registry.closed is True
    with pytest.raises(bound_inputs.BoundInputClosedError):
        with registry.open_stream("input"):
            pass


def test_module_has_no_pathname_open_or_shared_integration_claim():
    source_path = ROOT / "release_install_bound_inputs.py"
    source = source_path.read_text(encoding="utf-8")
    parsed = ast.parse(source, filename=str(source_path))

    assert "pathlib" not in source
    assert "simulator_integration_proven\": False" in source
    assert "release_install_dry_run" not in source
    forbidden_attributes = {
        ("os", "open"),
        ("os", "stat"),
        ("os", "lstat"),
        ("Path", "open"),
        ("Path", "read_bytes"),
        ("Path", "read_text"),
    }
    observed = {
        (node.value.id, node.attr)
        for node in ast.walk(parsed)
        if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name)
    }
    assert observed.isdisjoint(forbidden_attributes)
    assert not any(
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "open"
        for node in ast.walk(parsed)
    )
