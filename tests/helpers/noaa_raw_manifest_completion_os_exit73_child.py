"""Closed real-process crash harness for NOAA and raw-manifest consumers.

The parent test modules build every input from hermetic local fixtures.  This
entry point only validates a closed scenario/checkpoint pair, invokes the
matching test-owned runner, and converts the first matching production fault
callback into a real ``os._exit(73)``.
"""
from __future__ import annotations

import os
from pathlib import Path
import sys
from typing import Callable, Sequence


REPOSITORY = Path(__file__).resolve().parents[2]
TESTS = REPOSITORY / "tests"
for entry in (str(TESTS), str(REPOSITORY)):
    if entry not in sys.path:
        sys.path.insert(0, entry)


CRASH_EXIT_CODE = 73
_CHECKPOINTS = {
    "noaa": frozenset(
        {
            "migration.after_metadata",
            "migration.after_triggers",
            "migration.before_commit",
            "migration.after_commit",
            "output.before_commit",
            "output.after_commit",
            "consumer.after_output_commit",
            "consumer.before_final_reproof",
            "consumer.after_final_reproof_before_ack",
            "consumer.after_ack",
            "consumer.after_claim",
        }
    ),
    "raw-manifest": frozenset(
        {
            "migration.after_metadata",
            "migration.after_triggers",
            "migration.before_commit",
            "migration.after_commit",
            "consumer.after_output_commit",
            "consumer.before_final_reproof",
            "consumer.after_completion_reproof_before_ack",
            "consumer.after_lifecycle_ack_before_output_reproof_release",
        }
    ),
}


def _runner(
    scenario: str,
) -> Callable[[Path, str, Callable[[str], None]], None]:
    if scenario == "noaa":
        from test_worker_noaa_err_completion_consumer import (
            run_noaa_completion_os_exit73_child,
        )

        return run_noaa_completion_os_exit73_child
    if scenario == "raw-manifest":
        from test_worker_raw_manifest_audit_completion_consumer import (
            run_raw_manifest_completion_os_exit73_child,
        )

        return run_raw_manifest_completion_os_exit73_child
    raise SystemExit("completion crash scenario is not allowlisted")


def main(arguments: Sequence[str]) -> None:
    if len(arguments) != 3:
        raise SystemExit(
            "usage: noaa_raw_manifest_completion_os_exit73_child.py "
            "SCENARIO ROOT CHECKPOINT"
        )
    scenario, root_value, checkpoint = arguments
    allowed = _CHECKPOINTS.get(scenario)
    if allowed is None or checkpoint not in allowed:
        raise SystemExit("completion crash checkpoint is not allowlisted")
    root = Path(root_value)
    if not root.is_absolute() or not root.is_dir() or root.is_symlink():
        raise SystemExit("completion crash root must be an existing real directory")

    hits = 0

    def crash(point: str) -> None:
        nonlocal hits
        if point != checkpoint:
            return
        hits += 1
        if hits == 1:
            os._exit(CRASH_EXIT_CODE)

    _runner(scenario)(root.resolve(), checkpoint, crash)
    raise RuntimeError("allowlisted completion checkpoint was not reached")


if __name__ == "__main__":
    main(sys.argv[1:])
