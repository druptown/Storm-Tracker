"""Abrupt child driver for terminal APPLIED replay reproof tests."""
from __future__ import annotations

from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[2]
TESTS = ROOT / "tests"
for entry in (str(TESTS), str(ROOT)):
    if entry not in sys.path:
        sys.path.insert(0, entry)

from test_worker_acquisition_source_reclamation_native_applied_reproof_crash import (  # noqa: E402,E501
    run_applied_reproof_crash_child,
)


def main() -> None:
    if len(sys.argv) != 4:
        raise SystemExit(
            "usage: worker_reclamation_applied_reproof_crash_child.py "
            "ROOT PUBLIC_API CHECKPOINT"
        )
    run_applied_reproof_crash_child(
        Path(sys.argv[1]).resolve(),
        public_api=sys.argv[2],
        checkpoint=sys.argv[3],
    )


if __name__ == "__main__":
    main()
