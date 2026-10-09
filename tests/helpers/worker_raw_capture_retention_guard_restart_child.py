"""Hard-exit driver for every read-only retention-guard boundary."""
from __future__ import annotations

import os
from pathlib import Path
import sys


REPOSITORY = Path(__file__).resolve().parents[2]
TESTS = REPOSITORY / "tests"
for path in (str(REPOSITORY), str(TESTS)):
    if path not in sys.path:
        sys.path.insert(0, path)

from test_worker_raw_capture_retention_guard import (  # noqa: E402
    _config,
    _guard,
    _state,
)


CHECKPOINTS = frozenset({
    "after_ownership",
    "after_ledger",
    "after_raw_store",
    "after_capacity_probe",
    "after_identity_reproof",
    "after_ownership_reproof",
})


def main() -> int:
    if len(sys.argv) != 3 or sys.argv[2] not in CHECKPOINTS:
        return 64
    root = Path(sys.argv[1]).resolve()
    checkpoint = sys.argv[2]

    def crash(value: str) -> None:
        if value == checkpoint:
            os._exit(73)

    guard = _guard(
        _config(root, create_cadence=False),
        _state(),
        fault_injector=crash,
    )
    guard.inspect(full_audit=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
