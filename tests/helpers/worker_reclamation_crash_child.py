"""Abrupt subprocess driver for native five-role source reclamation."""
from __future__ import annotations

from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[2]
TESTS = ROOT / "tests"
for entry in (str(TESTS), str(ROOT)):
    if entry not in sys.path:
        sys.path.insert(0, entry)

from test_worker_acquisition_source_reclamation_native_five_role_crash import (  # noqa: E402
    run_crash_child,
)


def main() -> None:
    if len(sys.argv) != 6:
        raise SystemExit(
            "usage: worker_reclamation_crash_child.py "
            "ROOT MODE TARGET CHECKPOINT OCCURRENCE"
        )
    run_crash_child(
        Path(sys.argv[1]).resolve(),
        mode=sys.argv[2],
        target=sys.argv[3],
        checkpoint=sys.argv[4],
        occurrence=int(sys.argv[5]),
    )


if __name__ == "__main__":
    main()
