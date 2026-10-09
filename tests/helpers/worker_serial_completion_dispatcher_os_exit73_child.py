"""Closed real-process crash harness for the serial completion dispatcher.

The parent test owns the fixtures and the exact checkpoint inventory.  This
small entry point only makes that inventory reachable from a fresh interpreter
whose abrupt termination is a real ``os._exit(73)``.
"""
from __future__ import annotations

from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[2]
TESTS = ROOT / "tests"
for entry in (str(ROOT), str(TESTS)):
    if entry not in sys.path:
        sys.path.insert(0, entry)

from test_worker_serial_completion_dispatcher import (  # noqa: E402
    run_serial_completion_dispatcher_os_exit73_child,
)


def main(arguments: list[str]) -> None:
    if len(arguments) != 2:
        raise SystemExit(
            "usage: worker_serial_completion_dispatcher_os_exit73_child.py "
            "ROOT CHECKPOINT"
        )
    root_value, checkpoint = arguments
    root = Path(root_value)
    if not root.is_absolute() or not root.is_dir() or root.is_symlink():
        raise SystemExit("dispatcher crash root must be an absolute real directory")
    run_serial_completion_dispatcher_os_exit73_child(
        root.resolve(), checkpoint=checkpoint
    )


if __name__ == "__main__":
    main(sys.argv[1:])
