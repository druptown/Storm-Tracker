"""Build a local, non-admitting offline replay preflight document.

The complete acceptance API lives in
``storm_tracker_worker.offline_replay_acceptance`` and requires in-memory,
validated external-truth and scientific-manifest objects.  This CLI only
binds a portable input contract to the exact local component sources.  It
always reports every operational admission flag as false.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from storm_tracker_worker.offline_replay_acceptance import (  # noqa: E402
    audit_local_science_component_sources,
    build_preflight_mapping,
    load_offline_replay_input,
)


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a fail-closed local offline-replay preflight.",
    )
    parser.add_argument("--input-contract", type=Path, required=True)
    parser.add_argument(
        "--worker-package",
        type=Path,
        default=REPOSITORY_ROOT / "storm_tracker_worker",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="New output file; stdout is used when omitted.",
    )
    return parser.parse_args()


def _exclusive_write(path: Path, body: bytes) -> None:
    unresolved_parent = Path(os.path.abspath(os.fspath(path.parent)))
    if unresolved_parent.is_symlink():
        raise RuntimeError("output parent may not be a symlink")
    parent = unresolved_parent.resolve(strict=True)
    if not parent.is_dir():
        raise RuntimeError("output parent must be an existing regular directory")
    target = parent / path.name
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_BINARY", 0)
    descriptor = os.open(target, flags, 0o600)
    try:
        written = 0
        while written < len(body):
            count = os.write(descriptor, body[written:])
            if count <= 0:
                raise OSError("short output write")
            written += count
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def main() -> int:
    arguments = _arguments()
    replay_input = load_offline_replay_input(arguments.input_contract)
    component_sources = audit_local_science_component_sources(
        arguments.worker_package,
    )
    document = build_preflight_mapping(
        replay_input,
        component_sources=component_sources,
    )
    body = (
        json.dumps(
            document,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        )
        + "\n"
    ).encode("ascii")
    if arguments.output is None:
        sys.stdout.buffer.write(body)
        sys.stdout.buffer.flush()
    else:
        _exclusive_write(arguments.output, body)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
