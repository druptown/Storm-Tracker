"""Native hard-exit harness for the v1→current dispatcher migration."""

from __future__ import annotations

from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[2]
TESTS = ROOT / "tests"
for entry in (str(ROOT), str(TESTS)):
    if entry not in sys.path:
        sys.path.insert(0, entry)

from test_worker_serial_completion_dispatcher import (  # noqa: E402
    run_serial_completion_dispatcher_migration_os_exit73_child,
)


def main(arguments: list[str]) -> None:
    if len(arguments) != 1:
        raise SystemExit("usage: migration-child.py ROOT")
    root = Path(arguments[0])
    if not root.is_absolute() or not root.is_dir() or root.is_symlink():
        raise SystemExit("dispatcher migration root must be absolute and real")
    run_serial_completion_dispatcher_migration_os_exit73_child(root.resolve())


if __name__ == "__main__":
    main(sys.argv[1:])
