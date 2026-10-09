"""Veilige overdracht van grotere workerresultaten via de gedeelde map."""
from __future__ import annotations

import gzip
import hashlib
import json
import os
from pathlib import Path
import stat
import tempfile
import time
from typing import Any

from .tracking_core.engine.bounded_payload import (
    decompress_limited,
    read_limited_stream,
)

ARTIFACT_ROOT = Path(
    os.getenv("STV3_ARTIFACT_ROOT", "/share/storm_tracker_worker")
)
MAX_ARTIFACT_BYTES = 32 * 1024 * 1024
ARTIFACT_RETENTION_SECONDS = 2 * 60 * 60


def _cleanup_expired_artifacts(now: float | None = None) -> None:
    """Remove only expired worker JSON artifacts from the direct share root."""
    reference = time.time() if now is None else float(now)
    if not ARTIFACT_ROOT.is_dir():
        return
    for path in ARTIFACT_ROOT.iterdir():
        if (
            not path.is_file()
            or path.suffixes[-2:] != [".json", ".gz"]
            or path.name.startswith(".")
        ):
            continue
        try:
            if reference - path.stat().st_mtime > ARTIFACT_RETENTION_SECONDS:
                path.unlink()
        except FileNotFoundError:
            continue


def _verify_published_artifact(
    path: Path, *, expected_bytes: int, expected_sha256: str,
) -> None:
    """Verify the immutable bytes visible at a published artifact path."""
    published_stat = path.lstat()
    if not stat.S_ISREG(published_stat.st_mode):
        raise ValueError("workerartifact-pad is geen regulier bestand")
    if published_stat.st_size != expected_bytes:
        raise ValueError("bestaand workerartifact heeft andere bytes")
    with path.open("rb") as stream:
        published = read_limited_stream(
            stream,
            maximum_bytes=MAX_ARTIFACT_BYTES,
            declared_bytes=expected_bytes,
            require_declared_match=True,
        )
    if hashlib.sha256(published).hexdigest() != expected_sha256:
        raise ValueError("bestaand workerartifact heeft andere bytes")


def _fsync_directory(path: Path) -> None:
    """Make a newly published directory entry durable where supported."""
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError:
        # Windows cannot open directories this way; publication is still
        # atomic there, but directory fsync is unavailable.
        return
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _publish_immutable_artifact(
    temporary_path: Path,
    target: Path,
    *,
    expected_bytes: int,
    expected_sha256: str,
) -> None:
    """Atomically create ``target`` or reuse its already identical bytes."""
    # The hard link publishes a fully written inode without replace semantics:
    # an existing content-addressed path can therefore never be overwritten.
    # A concurrent retention cleanup can remove an existing path just before
    # verification, so retry that harmless create/reuse race a few times.
    for _attempt in range(3):
        created = False
        try:
            os.link(temporary_path, target)
            created = True
        except FileExistsError:
            pass
        try:
            _verify_published_artifact(
                target,
                expected_bytes=expected_bytes,
                expected_sha256=expected_sha256,
            )
        except FileNotFoundError:
            continue
        if created:
            _fsync_directory(target.parent)
        else:
            # Reuse renews the same retention lease as an ordinary write;
            # only metadata changes, never the content-addressed bytes.
            try:
                os.utime(target, None)
            except FileNotFoundError:
                continue
        return
    raise OSError("workerartifact kon niet atomair worden gepubliceerd")


def write_json_artifact(job_id: str, value: dict[str, Any]) -> dict[str, Any]:
    """Schrijf inhoudsgebonden gzip-JSON en geef alleen kleine metadata terug."""
    safe_job_id = "".join(
        character for character in job_id
        if character.isalnum() or character in {"-", "_"}
    )
    if not safe_job_id or safe_job_id != job_id:
        raise ValueError("Ongeldig artifact-taak-ID")
    ARTIFACT_ROOT.mkdir(parents=True, exist_ok=True)
    _cleanup_expired_artifacts()
    encoded = json.dumps(
        value, ensure_ascii=False, separators=(",", ":"), sort_keys=True
    ).encode("utf-8")
    if len(encoded) > MAX_ARTIFACT_BYTES:
        raise ValueError("Workerartifact overschrijdt veiligheidslimiet")
    compressed = gzip.compress(encoded, compresslevel=5, mtime=0)
    # Hash the final transfer bytes before deriving their immutable path.
    digest = hashlib.sha256(compressed).hexdigest()
    target = ARTIFACT_ROOT / f"{safe_job_id}-{digest}.json.gz"
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            dir=ARTIFACT_ROOT, prefix=f".{safe_job_id}.", delete=False
        ) as temporary:
            temporary.write(compressed)
            temporary.flush()
            os.fsync(temporary.fileno())
            temporary_path = Path(temporary.name)
        _publish_immutable_artifact(
            temporary_path,
            target,
            expected_bytes=len(compressed),
            expected_sha256=digest,
        )
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)
    return {
        "artifact_path": str(target),
        "artifact_sha256": digest,
        "artifact_bytes": len(compressed),
        "content_type": "application/json+gzip",
    }


def read_json_artifact(metadata: dict[str, Any]) -> dict[str, Any]:
    """Read one direct-child worker artifact under size and hash contracts."""
    raw_path = metadata.get("artifact_path")
    if not isinstance(raw_path, str):
        raise ValueError("workerartifact mist pad")
    root = ARTIFACT_ROOT.resolve()
    path = Path(raw_path).resolve()
    if path.parent != root or path.suffixes[-2:] != [".json", ".gz"]:
        raise ValueError("workerartifact staat buiten de gedeelde map")
    expected = int(metadata.get("artifact_bytes", -1))
    if expected <= 0 or expected > MAX_ARTIFACT_BYTES or path.stat().st_size != expected:
        raise ValueError("workerartifact heeft een ongeldige grootte")
    with path.open("rb") as stream:
        compressed = read_limited_stream(
            stream, maximum_bytes=MAX_ARTIFACT_BYTES,
            declared_bytes=expected, require_declared_match=True,
        )
    if hashlib.sha256(compressed).hexdigest() != metadata.get("artifact_sha256"):
        raise ValueError("workerartifact-controlesom klopt niet")
    decoded = decompress_limited(
        compressed, codec="gzip",
        maximum_compressed_bytes=MAX_ARTIFACT_BYTES,
        maximum_uncompressed_bytes=MAX_ARTIFACT_BYTES,
    )
    value = json.loads(decoded)
    if not isinstance(value, dict):
        raise ValueError("workerartifact bevat geen object")
    return value
