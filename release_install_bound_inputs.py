"""Descriptor-bound input registry for a later release-verifier adapter.

This module deliberately has no pathname API.  A caller supplies an already
opened :class:`DirectoryAuthority`, relative names, exact identities, sizes and
hashes.  Every use reopens the entry beneath that authority, proves the sealed
identity and bytes before use, and repeats those proofs before releasing the
handle.

The registry is standalone preparation.  It is not integrated into the release
installer or release-artifact verifier and therefore closes no acceptance
blocker by itself.
"""
from __future__ import annotations

from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass
import hashlib
from itertools import islice
import json
import re
import threading
from types import MappingProxyType
from typing import BinaryIO, Final, Iterable, Iterator, Mapping, Protocol, runtime_checkable

from descriptor_relative_authority import (
    AuthorityError,
    BoundFile,
    DirectoryAuthority,
    NodeIdentity,
    RelativePath,
)


BOUND_INPUT_REGISTRY_CONTRACT: Final = "stv3-release-install-bound-inputs-v1"
MAX_BOUND_INPUTS: Final = 64
MAX_BOUND_INPUT_BYTES: Final = 2 * 1024 * 1024 * 1024
MAX_JSON_DEPTH: Final = 64
MAX_JSON_NODES: Final = 50_000
MAX_JSON_STRING_BYTES: Final = 1024 * 1024
MAX_JSON_KEY_BYTES: Final = 256
MAX_JSON_INTEGER_ABSOLUTE: Final = MAX_BOUND_INPUT_BYTES
STREAM_CHUNK_BYTES: Final = 1024 * 1024

_KEY = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


class BoundInputError(RuntimeError):
    """Base error for the fail-closed bound-input contract."""


class BoundInputContractError(BoundInputError):
    """A declaration or caller request is outside the bounded contract."""


class BoundInputIntegrityError(BoundInputError):
    """A descriptor, identity, byte count or digest differs from its seal."""


class BoundInputClosedError(BoundInputError):
    """The registry can no longer issue streams."""


@dataclass(frozen=True, slots=True)
class BoundInputDeclaration:
    """The exact expected state of one authority-relative regular file."""

    relative: RelativePath
    identity: NodeIdentity
    size: int
    sha256: str
    maximum_bytes: int

    def __post_init__(self) -> None:
        if not isinstance(self.relative, RelativePath) or not self.relative.parts:
            raise BoundInputContractError(
                "bound input needs a non-empty RelativePath"
            )
        if not isinstance(self.identity, NodeIdentity):
            raise BoundInputContractError("bound input identity is invalid")
        if (
            self.identity.kind != "regular_file"
            or self.identity.link_count != 1
        ):
            raise BoundInputContractError(
                "bound input must seal one single-link regular file"
            )
        if type(self.size) is not int or self.size < 0:
            raise BoundInputContractError("bound input size is invalid")
        if (
            type(self.maximum_bytes) is not int
            or self.maximum_bytes < 0
            or self.maximum_bytes > MAX_BOUND_INPUT_BYTES
            or self.size > self.maximum_bytes
        ):
            raise BoundInputContractError("bound input byte bound is invalid")
        if self.identity.size != self.size:
            raise BoundInputContractError(
                "bound input identity size differs from its declaration"
            )
        if not isinstance(self.sha256, str) or _SHA256.fullmatch(self.sha256) is None:
            raise BoundInputContractError("bound input SHA-256 is invalid")


@dataclass(frozen=True, slots=True)
class BoundInputMetadata:
    """Path-sanitized immutable evidence for one registered input."""

    key: str
    relative: RelativePath
    identity: NodeIdentity
    size: int
    sha256: str
    maximum_bytes: int

    def to_mapping(self) -> dict[str, object]:
        return {
            "identity": self.identity.to_mapping(),
            "key": self.key,
            "maximum_bytes": self.maximum_bytes,
            "relative_name": str(self.relative),
            "sha256": self.sha256,
            "size": self.size,
        }


@runtime_checkable
class BoundInputSource(Protocol):
    """Minimal path-free protocol a stream verifier can depend on."""

    @property
    def keys(self) -> tuple[str, ...]: ...

    def require_exact_keys(self, expected_keys: Iterable[str]) -> None: ...

    def metadata(self, key: str) -> BoundInputMetadata: ...

    def open_stream(self, key: str) -> AbstractContextManager[BinaryIO]: ...

    def read_bounded(self, key: str, *, maximum_bytes: int) -> bytes: ...

    def read_json_object(
        self,
        key: str,
        *,
        maximum_bytes: int,
        expected_keys: Iterable[str],
    ) -> dict[str, object]: ...


def _validated_key(value: object) -> str:
    if not isinstance(value, str) or _KEY.fullmatch(value) is None:
        raise BoundInputContractError("bound input key is invalid")
    return value


def _exact_key_tuple(values: Iterable[str]) -> tuple[str, ...]:
    try:
        materialized = tuple(islice(iter(values), MAX_BOUND_INPUTS + 1))
    except TypeError as error:
        raise BoundInputContractError("bound input keys are not iterable") from error
    if len(materialized) > MAX_BOUND_INPUTS:
        raise BoundInputContractError("bound input key count exceeds its bound")
    checked = tuple(_validated_key(item) for item in materialized)
    if len(set(checked)) != len(checked):
        raise BoundInputContractError("bound input keys are duplicated")
    return tuple(sorted(checked))


def _snapshot_declarations(
    declarations: Mapping[str, BoundInputDeclaration],
) -> dict[str, BoundInputDeclaration]:
    """Materialize an adversarial Mapping once, within the registry bound."""

    try:
        materialized = tuple(
            islice(iter(declarations.items()), MAX_BOUND_INPUTS + 1)
        )
    except Exception as error:
        raise BoundInputContractError(
            "bound input declarations cannot be materialized"
        ) from error
    if len(materialized) > MAX_BOUND_INPUTS:
        raise BoundInputContractError(
            "bound input declaration count exceeds its bound"
        )
    snapshot: dict[str, BoundInputDeclaration] = {}
    for item in materialized:
        try:
            raw_key, declaration = item
        except (TypeError, ValueError) as error:
            raise BoundInputContractError(
                "bound input declaration item is invalid"
            ) from error
        key = _validated_key(raw_key)
        if key in snapshot:
            raise BoundInputContractError("bound input declaration keys are duplicated")
        if not isinstance(declaration, BoundInputDeclaration):
            raise BoundInputContractError(
                "bound input declaration value is invalid"
            )
        snapshot[key] = declaration
    return snapshot


def _assert_identity(
    actual: NodeIdentity,
    expected: NodeIdentity,
    *,
    size: int,
) -> None:
    if (
        not actual.same_object(expected)
        or actual.kind != "regular_file"
        or actual.link_count != 1
        or actual.size != size
    ):
        raise BoundInputIntegrityError("bound input identity differs from its seal")


def _measure_stream(stream: BinaryIO, maximum_bytes: int) -> tuple[int, str]:
    digest = hashlib.sha256()
    total = 0
    try:
        stream.seek(0)
        while True:
            remaining = maximum_bytes - total
            request = min(STREAM_CHUNK_BYTES, remaining + 1)
            if request <= 0:
                raise BoundInputIntegrityError("bound input exceeds its byte bound")
            chunk = stream.read(request)
            if not chunk:
                break
            if not isinstance(chunk, bytes):
                raise BoundInputIntegrityError("bound input stream is not binary")
            total += len(chunk)
            if total > maximum_bytes:
                raise BoundInputIntegrityError("bound input exceeds its byte bound")
            digest.update(chunk)
        stream.seek(0)
    except BoundInputError:
        raise
    except (OSError, ValueError) as error:
        raise BoundInputIntegrityError("bound input stream could not be measured") from error
    return total, digest.hexdigest()


def _assert_stream(
    stream: BinaryIO,
    *,
    maximum_bytes: int,
    size: int,
    sha256: str,
) -> None:
    observed_size, observed_sha256 = _measure_stream(stream, maximum_bytes)
    if observed_size != size or observed_sha256 != sha256:
        raise BoundInputIntegrityError("bound input size or SHA-256 differs")


def _strict_json_object(
    value: bytes,
    *,
    expected_keys: tuple[str, ...],
) -> dict[str, object]:
    def object_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, nested in pairs:
            if key in result:
                raise ValueError("duplicate JSON key")
            result[key] = nested
        return result

    def reject_constant(token: str) -> object:
        raise ValueError(f"non-finite JSON constant: {token}")

    def reject_float(token: str) -> object:
        raise ValueError(f"JSON float is forbidden: {token}")

    def bounded_integer(token: str) -> int:
        number = int(token)
        if abs(number) > MAX_JSON_INTEGER_ABSOLUTE:
            raise ValueError("JSON integer exceeds its bound")
        return number

    try:
        document = json.loads(
            value.decode("utf-8"),
            object_pairs_hook=object_pairs,
            parse_constant=reject_constant,
            parse_float=reject_float,
            parse_int=bounded_integer,
        )
    except (UnicodeError, json.JSONDecodeError, RecursionError, ValueError) as error:
        raise BoundInputContractError("bound input is not strict UTF-8 JSON") from error
    if not isinstance(document, dict) or tuple(sorted(document)) != expected_keys:
        raise BoundInputContractError("bound JSON object keys differ")

    pending: list[tuple[object, int]] = [(document, 1)]
    nodes = 0
    while pending:
        item, depth = pending.pop()
        nodes += 1
        if nodes > MAX_JSON_NODES or depth > MAX_JSON_DEPTH:
            raise BoundInputContractError("bound JSON structure exceeds its bound")
        if isinstance(item, dict):
            for key, nested in item.items():
                if (
                    not isinstance(key, str)
                    or len(key.encode("utf-8")) > MAX_JSON_KEY_BYTES
                ):
                    raise BoundInputContractError("bound JSON key exceeds its bound")
                pending.append((nested, depth + 1))
        elif isinstance(item, list):
            pending.extend((nested, depth + 1) for nested in item)
        elif isinstance(item, str):
            if len(item.encode("utf-8")) > MAX_JSON_STRING_BYTES:
                raise BoundInputContractError("bound JSON string exceeds its bound")
        elif item is None or isinstance(item, (bool, int)):
            continue
        else:
            raise BoundInputContractError("bound JSON value type is forbidden")
    return document


class BoundInputRegistry:
    """Own an authority and issue independently verified read-only streams."""

    __slots__ = ("_authority", "_closed", "_entries", "_lock")

    def __init__(
        self,
        *,
        authority: DirectoryAuthority,
        entries: Mapping[str, BoundInputMetadata],
    ) -> None:
        self._authority = authority
        self._entries = MappingProxyType(dict(entries))
        self._lock = threading.RLock()
        self._closed = False

    @classmethod
    def seal(
        cls,
        authority: DirectoryAuthority,
        declarations: Mapping[str, BoundInputDeclaration],
        *,
        expected_keys: Iterable[str],
    ) -> "BoundInputRegistry":
        if not isinstance(authority, DirectoryAuthority) or authority.closed:
            raise BoundInputContractError("bound input authority is unavailable")
        if not isinstance(declarations, Mapping):
            raise BoundInputContractError("bound input declarations are not a mapping")
        exact_keys = _exact_key_tuple(expected_keys)
        snapshot = _snapshot_declarations(declarations)
        declared_keys = tuple(sorted(snapshot))
        if declared_keys != exact_keys:
            raise BoundInputContractError("bound input declaration keys differ")

        try:
            owned_authority = authority.duplicate()
        except AuthorityError as error:
            raise BoundInputIntegrityError(
                "bound input authority could not be duplicated"
            ) from error
        entries: dict[str, BoundInputMetadata] = {}
        try:
            for key in exact_keys:
                declaration = snapshot[key]
                entries[key] = cls._seal_one(
                    owned_authority,
                    key,
                    declaration,
                )
            return cls(authority=owned_authority, entries=entries)
        except BaseException:
            try:
                owned_authority.close()
            except AuthorityError:
                pass
            raise

    @staticmethod
    def _seal_one(
        authority: DirectoryAuthority,
        key: str,
        declaration: BoundInputDeclaration,
    ) -> BoundInputMetadata:
        try:
            with authority.open_file(declaration.relative) as bound:
                before = bound.identity()
                _assert_identity(
                    before,
                    declaration.identity,
                    size=declaration.size,
                )
                with bound.duplicate_binary("rb") as stream:
                    _assert_stream(
                        stream,
                        maximum_bytes=declaration.maximum_bytes,
                        size=declaration.size,
                        sha256=declaration.sha256,
                    )
                after = bound.identity()
                _assert_identity(
                    after,
                    declaration.identity,
                    size=declaration.size,
                )
        except BoundInputError:
            raise
        except AuthorityError as error:
            raise BoundInputIntegrityError(
                "bound input could not be sealed beneath its authority"
            ) from error
        return BoundInputMetadata(
            key=key,
            relative=declaration.relative,
            identity=before,
            size=declaration.size,
            sha256=declaration.sha256,
            maximum_bytes=declaration.maximum_bytes,
        )

    @property
    def keys(self) -> tuple[str, ...]:
        return tuple(self._entries)

    @property
    def closed(self) -> bool:
        with self._lock:
            return self._closed

    def require_exact_keys(self, expected_keys: Iterable[str]) -> None:
        if _exact_key_tuple(expected_keys) != self.keys:
            raise BoundInputContractError("bound input registry keys differ")

    def metadata(self, key: str) -> BoundInputMetadata:
        checked = _validated_key(key)
        try:
            return self._entries[checked]
        except KeyError as error:
            raise BoundInputContractError("bound input key is not registered") from error

    def _clone_authority(self) -> DirectoryAuthority:
        with self._lock:
            if self._closed:
                raise BoundInputClosedError("bound input registry is closed")
            try:
                return self._authority.duplicate()
            except AuthorityError as error:
                raise BoundInputIntegrityError(
                    "bound input authority could not be duplicated"
                ) from error

    @contextmanager
    def open_stream(self, key: str) -> Iterator[BinaryIO]:
        metadata = self.metadata(key)
        authority = self._clone_authority()
        bound: BoundFile | None = None
        stream: BinaryIO | None = None
        try:
            try:
                bound = authority.open_file(metadata.relative)
                _assert_identity(
                    bound.identity(),
                    metadata.identity,
                    size=metadata.size,
                )
                stream = bound.duplicate_binary("rb")
                _assert_stream(
                    stream,
                    maximum_bytes=metadata.maximum_bytes,
                    size=metadata.size,
                    sha256=metadata.sha256,
                )
                _assert_identity(
                    bound.identity(),
                    metadata.identity,
                    size=metadata.size,
                )
            except BoundInputError:
                raise
            except AuthorityError as error:
                raise BoundInputIntegrityError(
                    "bound input stream could not be opened"
                ) from error

            # Capture a consumer failure so post-use proof always runs.  A
            # successful proof preserves that exact failure; a failed proof is
            # fail-closed and explicitly chains the consumer failure.
            consumer_error: BaseException | None = None
            consumer_traceback = None
            try:
                yield stream
            except BaseException as error:
                consumer_error = error
                consumer_traceback = error.__traceback__
            try:
                if stream.closed:
                    raise BoundInputIntegrityError(
                        "bound input consumer closed the registry stream"
                    )
                _assert_identity(
                    bound.identity(),
                    metadata.identity,
                    size=metadata.size,
                )
                _assert_stream(
                    stream,
                    maximum_bytes=metadata.maximum_bytes,
                    size=metadata.size,
                    sha256=metadata.sha256,
                )
                _assert_identity(
                    bound.identity(),
                    metadata.identity,
                    size=metadata.size,
                )
            except (BoundInputError, AuthorityError) as proof_error:
                if isinstance(proof_error, AuthorityError):
                    integrity_error = BoundInputIntegrityError(
                        "bound input changed while its stream was in use"
                    )
                else:
                    integrity_error = proof_error
                if consumer_error is not None:
                    raise integrity_error from consumer_error
                if integrity_error is proof_error:
                    raise
                raise integrity_error from proof_error
            if consumer_error is not None:
                raise consumer_error.with_traceback(consumer_traceback)
        except BoundInputError:
            raise
        except AuthorityError as error:
            raise BoundInputIntegrityError(
                "bound input changed while its stream was in use"
            ) from error
        finally:
            if stream is not None and not stream.closed:
                try:
                    stream.close()
                except OSError:
                    pass
            if bound is not None and not bound.closed:
                try:
                    bound.close()
                except AuthorityError:
                    pass
            if not authority.closed:
                try:
                    authority.close()
                except AuthorityError:
                    pass

    def read_bounded(self, key: str, *, maximum_bytes: int) -> bytes:
        metadata = self.metadata(key)
        if (
            type(maximum_bytes) is not int
            or maximum_bytes < 0
            or maximum_bytes > metadata.maximum_bytes
        ):
            raise BoundInputContractError("bound input read limit is invalid")
        if metadata.size > maximum_bytes:
            raise BoundInputContractError("bound input exceeds the requested read limit")
        chunks: list[bytes] = []
        total = 0
        with self.open_stream(key) as stream:
            while True:
                request = min(STREAM_CHUNK_BYTES, maximum_bytes - total + 1)
                if request <= 0:
                    raise BoundInputIntegrityError(
                        "bound input exceeds the requested read limit"
                    )
                try:
                    chunk = stream.read(request)
                except OSError as error:
                    raise BoundInputIntegrityError(
                        "bound input bounded read failed"
                    ) from error
                if not chunk:
                    break
                if not isinstance(chunk, bytes):
                    raise BoundInputIntegrityError("bound input stream is not binary")
                total += len(chunk)
                if total > maximum_bytes:
                    raise BoundInputIntegrityError(
                        "bound input exceeds the requested read limit"
                    )
                chunks.append(chunk)
        value = b"".join(chunks)
        if len(value) != metadata.size or hashlib.sha256(value).hexdigest() != metadata.sha256:
            raise BoundInputIntegrityError("bound input bounded read bytes differ")
        return value

    def read_json_object(
        self,
        key: str,
        *,
        maximum_bytes: int,
        expected_keys: Iterable[str],
    ) -> dict[str, object]:
        exact_keys = _exact_key_tuple(expected_keys)
        return _strict_json_object(
            self.read_bounded(key, maximum_bytes=maximum_bytes),
            expected_keys=exact_keys,
        )

    def evidence(self) -> dict[str, object]:
        return {
            "contract": BOUND_INPUT_REGISTRY_CONTRACT,
            "descriptor_relative": True,
            "exact_keyset": list(self.keys),
            "inputs": {
                key: self._entries[key].to_mapping()
                for key in self.keys
            },
            "path_api_used": False,
            "pre_and_post_identity_proven": True,
            "pre_and_post_size_sha256_proven": True,
            "simulator_integration_proven": False,
        }

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            authority = self._authority
        try:
            authority.close()
        except AuthorityError as error:
            raise BoundInputIntegrityError(
                "bound input registry authority close failed"
            ) from error

    def __enter__(self) -> "BoundInputRegistry":
        if self.closed:
            raise BoundInputClosedError("bound input registry is closed")
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.close()


__all__ = [
    "BOUND_INPUT_REGISTRY_CONTRACT",
    "MAX_BOUND_INPUTS",
    "MAX_BOUND_INPUT_BYTES",
    "BoundInputClosedError",
    "BoundInputContractError",
    "BoundInputDeclaration",
    "BoundInputError",
    "BoundInputIntegrityError",
    "BoundInputMetadata",
    "BoundInputRegistry",
    "BoundInputSource",
]
