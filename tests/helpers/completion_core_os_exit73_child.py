"""Closed real-process crash harness for the completion-core fault matrix."""
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


_CHECKPOINTS = {
    "archive": frozenset({"archive.before_insert"}),
    "route-adapter": frozenset({
        "adapter.before_final_output_verify",
        "adapter.after_final_output_verify_before_barrier",
    }),
    "terminal-router": frozenset({
        "completion.after_authorities_verified",
        "completion.before_lifecycle_publish",
        "completion.after_lifecycle_publish",
    }),
}


def _runner(scenario: str) -> Callable[[Path, str, Callable[[str], None]], None]:
    if scenario == "archive":
        from test_worker_completion_output_archive import (
            run_completion_archive_insert_crash_child,
        )

        return run_completion_archive_insert_crash_child
    if scenario == "terminal-router":
        from test_worker_terminal_raw_completion_router import (
            run_terminal_router_crash_child,
        )

        return run_terminal_router_crash_child
    if scenario == "route-adapter":
        from test_worker_completion_route_adapters import (
            run_dwd_route_adapter_crash_child,
        )

        return run_dwd_route_adapter_crash_child
    raise SystemExit("completion-core crash scenario is not allowlisted")


def main(arguments: Sequence[str]) -> None:
    if len(arguments) != 3:
        raise SystemExit(
            "usage: completion_core_os_exit73_child.py "
            "SCENARIO ROOT CHECKPOINT"
        )
    scenario, root_value, checkpoint = arguments
    allowed = _CHECKPOINTS.get(scenario)
    if allowed is None or checkpoint not in allowed:
        raise SystemExit("completion-core crash checkpoint is not allowlisted")
    root = Path(root_value)
    if not root.is_absolute() or not root.is_dir() or root.is_symlink():
        raise SystemExit("completion-core crash root must be an existing real directory")

    def crash(point: str) -> None:
        if point == checkpoint:
            os._exit(73)

    _runner(scenario)(root.resolve(), checkpoint, crash)
    raise RuntimeError("allowlisted completion-core checkpoint was not reached")


if __name__ == "__main__":
    main(sys.argv[1:])
