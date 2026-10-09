"""Closed subprocess driver for retention-v2 restart consistency."""
from __future__ import annotations

from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[2]
TESTS = ROOT / "tests"
for entry in (str(TESTS), str(ROOT)):
    if entry not in sys.path:
        sys.path.insert(0, entry)

from test_worker_acquisition_source_retention_v2_restart_consistency import (  # noqa: E402
    run_retention_v2_restart_child,
)


def main() -> None:
    if len(sys.argv) != 5:
        raise SystemExit(
            "usage: worker_acquisition_source_retention_v2_restart_child.py "
            "CASE_ROOT MODE OPERATION CHECKPOINT"
        )
    run_retention_v2_restart_child(
        Path(sys.argv[1]).resolve(),
        mode=sys.argv[2],
        operation=sys.argv[3],
        checkpoint=sys.argv[4],
    )


if __name__ == "__main__":
    main()
