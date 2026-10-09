"""Closed real-process crash harness for two completion consumers.

The parent tests create every durable input before launching this helper.  The
child only reopens those local authorities, arms one allowlisted production
fault point, and terminates through ``os._exit(73)`` when that point is hit.
"""
from __future__ import annotations

import os
from pathlib import Path
import re
import sys
from typing import Callable, Sequence


REPOSITORY = Path(__file__).resolve().parents[2]
TESTS = REPOSITORY / "tests"
for entry in (str(TESTS), str(REPOSITORY)):
    if entry not in sys.path:
        sys.path.insert(0, entry)


CRASH_EXIT_CODE = 73
_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$")
_TOKEN = re.compile(r"^[0-9a-f]{64}$")
_MIGRATION_POINTS = frozenset(
    {
        "migration.after_metadata",
        "migration.after_triggers",
        "migration.before_commit",
        "migration.after_commit",
    }
)
_OUTPUT_POINTS = frozenset(
    {
        "output.before_commit",
        "output.after_commit",
    }
)
_CONSUMER_POINTS = frozenset(
    {
        "consumer.after_output_commit",
        "consumer.before_lifecycle_ack",
        "consumer.after_lifecycle_ack",
    }
)
_CHECKPOINTS = {
    "migration": _MIGRATION_POINTS,
    "output": _OUTPUT_POINTS,
    "consumer": _CONSUMER_POINTS,
}
_SCENARIOS = frozenset({"metar-ground", "met-office-rate"})


def _runner(
    scenario: str,
) -> Callable[[Path, str, str, str | None, str | None, Callable[[str], None]], None]:
    if scenario == "metar-ground":
        from test_worker_metar_ground_completion_consumer import (
            run_metar_ground_completion_crash_child,
        )

        return run_metar_ground_completion_crash_child
    if scenario == "met-office-rate":
        from test_worker_met_office_rate_completion_consumer import (
            run_met_office_rate_completion_crash_child,
        )

        return run_met_office_rate_completion_crash_child
    raise SystemExit("completion-consumer crash scenario is not allowlisted")


def main(arguments: Sequence[str]) -> None:
    if len(arguments) not in {4, 6}:
        raise SystemExit(
            "usage: completion_consumers_os_exit73_child.py "
            "SCENARIO ROOT PHASE CHECKPOINT [EVENT_ID LEASE_TOKEN]"
        )
    scenario, root_value, phase, checkpoint = arguments[:4]
    allowed = _CHECKPOINTS.get(phase)
    if (
        scenario not in _SCENARIOS
        or allowed is None
        or checkpoint not in allowed
        or (phase == "migration") != (len(arguments) == 4)
    ):
        raise SystemExit("completion-consumer crash route is not allowlisted")
    root = Path(root_value)
    if not root.is_absolute() or not root.is_dir() or root.is_symlink():
        raise SystemExit(
            "completion-consumer crash root must be an existing real directory"
        )
    event_id: str | None = None
    lease_token: str | None = None
    if phase != "migration":
        event_id, lease_token = arguments[4:]
        if _IDENTIFIER.fullmatch(event_id) is None or _TOKEN.fullmatch(
            lease_token
        ) is None:
            raise SystemExit("completion-consumer lease arguments are invalid")

    def crash(point: str) -> None:
        if point == checkpoint:
            os._exit(CRASH_EXIT_CODE)

    _runner(scenario)(
        root.resolve(),
        phase,
        checkpoint,
        event_id,
        lease_token,
        crash,
    )
    raise RuntimeError("allowlisted completion-consumer checkpoint was not reached")


if __name__ == "__main__":
    main(sys.argv[1:])
