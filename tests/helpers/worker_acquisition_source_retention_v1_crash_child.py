"""Closed hard-exit driver for all v1 source-retention export boundaries."""
from __future__ import annotations

import json
import os
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[2]
TESTS = ROOT / "tests"
for entry in (str(TESTS), str(ROOT)):
    if entry not in sys.path:
        sys.path.insert(0, entry)

from storm_tracker_worker.acquisition_source_retention import (  # noqa: E402
    AcquisitionSourceCandidate,
)
from test_worker_acquisition_source_retention_v1_os_exit73 import (  # noqa: E402
    _reopen_v1_retention_case,
)


CRASH_POINTS = frozenset(
    f"{label}.{suffix}"
    for label in ("segment", "manifest", "anchor")
    for suffix in (
        "before_stage",
        "after_stage_fsync",
        "after_publish",
        "after_directory_fsync",
    )
)


def main() -> None:
    if len(sys.argv) != 3:
        raise SystemExit(
            "usage: worker_acquisition_source_retention_v1_crash_child.py "
            "CASE_ROOT CHECKPOINT"
        )
    case_root = Path(sys.argv[1]).resolve()
    checkpoint = sys.argv[2]
    if checkpoint not in CRASH_POINTS:
        raise SystemExit("v1 retention crash checkpoint is not allowlisted")
    candidate_value = json.loads(
        (case_root / "candidate.json").read_text(encoding="ascii")
    )
    candidate = AcquisitionSourceCandidate(**candidate_value)

    def crash(point: str) -> None:
        if point == checkpoint:
            os._exit(73)

    _authorities, archive = _reopen_v1_retention_case(
        case_root,
        candidate,
        fault=crash,
    )
    plan = archive.plan((candidate,), now=1_800_000_100.0)
    archive.export(plan)
    raise SystemExit("allowlisted v1 retention checkpoint was not reached")


if __name__ == "__main__":
    main()
