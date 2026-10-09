"""Closed real-process crash driver for the DWD and KNMI consumers.

The two parent test modules own the hermetic fixture construction.  This
entrypoint only validates a closed scenario/checkpoint pair, reopens that
fixture code in a fresh interpreter and turns the selected production fault
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
    "dwd": frozenset(
        {
            "migration.after_metadata",
            "migration.after_triggers",
            "migration.before_commit",
            "migration.after_commit",
            "output.before_commit",
            "output.after_commit",
            "consumer.after_decode",
            "consumer.after_cells",
            "consumer.after_output_commit",
            "consumer.after_claim",
            "consumer.after_final_reproof_before_ack",
            "consumer.after_ack",
        }
    ),
    "knmi": frozenset(
        {
            "migration.after_metadata",
            "migration.after_triggers",
            "migration.before_commit",
            "migration.after_commit",
            "output.before_commit",
            "output.after_commit",
            "consumer.after_output_commit",
            "consumer.before_lifecycle_ack",
            "consumer.after_lifecycle_ack",
        }
    ),
}


def _runner(
    scenario: str,
) -> Callable[[Path, str, Callable[[str], None]], None]:
    if scenario == "dwd":
        from test_worker_dwd_rv_completion_consumer import (
            run_dwd_completion_consumer_os_exit73_child,
        )

        return run_dwd_completion_consumer_os_exit73_child
    if scenario == "knmi":
        from test_worker_knmi_rtcor_completion_consumer import (
            run_knmi_completion_consumer_os_exit73_child,
        )

        return run_knmi_completion_consumer_os_exit73_child
    raise SystemExit("completion-consumer crash scenario is not allowlisted")


def main(arguments: Sequence[str]) -> None:
    if len(arguments) != 3:
        raise SystemExit(
            "usage: completion_consumer_os_exit73_child.py "
            "SCENARIO ROOT CHECKPOINT"
        )
    scenario, root_value, checkpoint = arguments
    allowed = _CHECKPOINTS.get(scenario)
    if allowed is None or checkpoint not in allowed:
        raise SystemExit("completion-consumer crash checkpoint is not allowlisted")
    root = Path(root_value)
    if not root.is_absolute() or not root.is_dir() or root.is_symlink():
        raise SystemExit("completion-consumer root must be an existing real directory")

    hits = 0

    def crash(point: str) -> None:
        nonlocal hits
        if point != checkpoint:
            return
        hits += 1
        if hits == 1:
            os._exit(CRASH_EXIT_CODE)

    _runner(scenario)(root.resolve(), checkpoint, crash)
    raise RuntimeError("allowlisted completion-consumer checkpoint was not reached")


if __name__ == "__main__":
    main(sys.argv[1:])
